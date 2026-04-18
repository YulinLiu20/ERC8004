import json
import os
import random
import time
import threading
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import psycopg2
import requests
from psycopg2.extras import execute_batch
from requests import HTTPError
from web3 import Web3


# =========================================================
# User Configuration
# Edit these values in code before running the workflow.
# For production backfills with a fixed observation_block, the RPC must support
# historical eth_call at that block height. In GitHub Actions we wire the
# Ethereum_archive_RPC secret into the ETHEREUM_ARCHIVE_RPC environment variable.
# =========================================================

DEFAULT_RPC_URL = "https://ethereum-rpc.publicnode.com"
RPC_URL = os.environ.get("ETHEREUM_ARCHIVE_RPC") or DEFAULT_RPC_URL
NEON_DATABASE_URL = os.environ["NEON_DATABASE_URL"]

CHAIN_ID = 1
NETWORK = "Ethereum"

IDENTITY_REGISTRY = "0x8004A169FB4a3325136EB29fA0ceB6D2e539a432"
REPUTATION_REGISTRY = "0x8004BAa17C55a88189AE136b182e5fdA19dE9b63"

CONTRACT_NAME = "AgentIdentity"
CONTRACT_SYMBOL = "AGENT"

START_BLOCK = 24339925
OBSERVATION_BLOCK = START_BLOCK + 500000

TARGET_AGENT_ID_MIN = 0
TARGET_AGENT_ID_MAX = 9999
TARGET_AGENT_COUNT = 10000

SCAN_BLOCK_WINDOW = 500
PIPELINE_BATCH_SIZE = 50
MAX_WORKERS = 4
REPUTATION_MAX_WORKERS = 2
HTTP_TIMEOUT = 20
RPC_MAX_RETRIES = 6
RPC_RETRY_BASE_DELAY_SECONDS = 0.5
RPC_BACKOFF_JITTER_SECONDS = 0.25
RPC_MAX_IN_FLIGHT = 2
RPC_MIN_INTERVAL_SECONDS = 0.05
FAIL_ON_INCOMPLETE_SNAPSHOT = True
SECOND_PASS_RETRY_ENABLED = True
SECOND_PASS_RETRY_DELAY_SECONDS = 0.3

# -----------------------------
# Manual rerun controls
# -----------------------------
RERUN_AGENT_IDS: List[int] = [2434]
RERUN_ONLY_STAGES = ["identity", "metadata", "reputation"]
RERUN_ONLY = False

RUN_IDENTITY = False
RUN_METADATA = False
RUN_REPUTATION = True


@dataclass(frozen=True)
class PipelineConfig:
    start_block: int = START_BLOCK
    observation_block: int = OBSERVATION_BLOCK
    target_agent_id_min: int = TARGET_AGENT_ID_MIN
    target_agent_id_max: int = TARGET_AGENT_ID_MAX
    target_agent_count: int = TARGET_AGENT_COUNT
    scan_block_window: int = SCAN_BLOCK_WINDOW
    pipeline_batch_size: int = PIPELINE_BATCH_SIZE
    max_workers: int = MAX_WORKERS
    reputation_max_workers: int = REPUTATION_MAX_WORKERS
    http_timeout: int = HTTP_TIMEOUT
    rpc_max_retries: int = RPC_MAX_RETRIES
    rpc_retry_base_delay_seconds: float = RPC_RETRY_BASE_DELAY_SECONDS
    rpc_backoff_jitter_seconds: float = RPC_BACKOFF_JITTER_SECONDS
    fail_on_incomplete_snapshot: bool = FAIL_ON_INCOMPLETE_SNAPSHOT
    second_pass_retry_enabled: bool = SECOND_PASS_RETRY_ENABLED
    second_pass_retry_delay_seconds: float = SECOND_PASS_RETRY_DELAY_SECONDS
    rpc_max_in_flight: int = RPC_MAX_IN_FLIGHT
    rpc_min_interval_seconds: float = RPC_MIN_INTERVAL_SECONDS
    rerun_agent_ids: Tuple[int, ...] = tuple(RERUN_AGENT_IDS)
    rerun_only_stages: Tuple[str, ...] = tuple(RERUN_ONLY_STAGES)
    rerun_only: bool = RERUN_ONLY
    run_identity: bool = RUN_IDENTITY
    run_metadata: bool = RUN_METADATA
    run_reputation: bool = RUN_REPUTATION


w3 = Web3(Web3.HTTPProvider(RPC_URL))
RPC_SEMAPHORE = threading.BoundedSemaphore(max(1, RPC_MAX_IN_FLIGHT))
RPC_THROTTLE_LOCK = threading.Lock()
RPC_NEXT_ALLOWED_AT = 0.0


# =========================================================
# Helper Functions
# =========================================================

TRANSFER_TOPIC = w3.keccak(text="Transfer(address,address,uint256)")
ZERO_TOPIC_BYTES32 = b"\x00" * 32


def norm_addr(addr: Optional[str]) -> Optional[str]:
    return str(addr).lower() if addr else None


def norm_tx_hash(txh: Optional[str]) -> Optional[str]:
    return str(txh).lower() if txh else None


def checksum(addr: str) -> str:
    return Web3.to_checksum_address(addr)


def parse_unix_ts(ts: Optional[object]) -> Optional[datetime]:
    if ts is None:
        return None
    return datetime.fromtimestamp(int(ts), tz=timezone.utc)


def detect_hosting_type(token_uri: Optional[str]) -> Optional[str]:
    if not token_uri:
        return None
    value = token_uri.lower()
    if value.startswith("ipfs://"):
        return "ipfs"
    if value.startswith("http://") or value.startswith("https://"):
        return "https"
    return "other"


def resolve_token_uri(token_uri: Optional[str]) -> Optional[str]:
    if not token_uri:
        return None
    if token_uri.startswith("ipfs://"):
        cid = token_uri.replace("ipfs://", "", 1)
        return f"https://ipfs.io/ipfs/{cid}"
    return token_uri


def parse_data_uri_json(url: str) -> Optional[Dict[str, object]]:
    if not url.startswith("data:"):
        return None

    header, sep, payload = url.partition(",")
    if not sep:
        return None

    header_lower = header.lower()
    if "application/json" not in header_lower:
        return None
    if ";base64" not in header_lower:
        return None

    decoded_bytes = base64.b64decode(payload)
    decoded_text = decoded_bytes.decode("utf-8")
    return json.loads(decoded_text)


def get_db_conn():
    return psycopg2.connect(NEON_DATABASE_URL)


def chunked(items: Sequence[object], size: int) -> Iterable[List[object]]:
    for index in range(0, len(items), size):
        yield list(items[index:index + size])


def _is_retryable_rpc_exception(exc: Exception) -> bool:
    if isinstance(exc, HTTPError) and exc.response is not None:
        return int(exc.response.status_code) in {429, 500, 502, 503, 504}
    return False


def rpc_call_with_retry(callable_fn, config: PipelineConfig, context: str):
    global RPC_NEXT_ALLOWED_AT

    attempts = max(1, int(config.rpc_max_retries))
    delay_seconds = max(0.0, float(config.rpc_retry_base_delay_seconds))
    jitter_seconds = max(0.0, float(config.rpc_backoff_jitter_seconds))

    for attempt in range(1, attempts + 1):
        try:
            with RPC_SEMAPHORE:
                with RPC_THROTTLE_LOCK:
                    now = time.monotonic()
                    wait_for = RPC_NEXT_ALLOWED_AT - now
                    if wait_for > 0:
                        time.sleep(wait_for)
                    RPC_NEXT_ALLOWED_AT = time.monotonic() + max(0.0, config.rpc_min_interval_seconds)
                return callable_fn()
        except Exception as exc:
            if attempt >= attempts or not _is_retryable_rpc_exception(exc):
                raise
            backoff = delay_seconds * (2 ** (attempt - 1)) + random.uniform(0.0, jitter_seconds)
            print(
                f"[rpc] {context} failed with retryable error on attempt "
                f"{attempt}/{attempts}: {repr(exc)}. Sleeping {backoff:.2f}s."
            )
            time.sleep(backoff)


# =========================================================
# ABI Definitions
# =========================================================

IDENTITY_ABI = [
    {
        "inputs": [{"internalType": "uint256", "name": "tokenId", "type": "uint256"}],
        "name": "ownerOf",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "uint256", "name": "tokenId", "type": "uint256"}],
        "name": "tokenURI",
        "outputs": [{"internalType": "string", "name": "", "type": "string"}],
        "stateMutability": "view",
        "type": "function",
    },
]

REPUTATION_ABI = [
    {
        "inputs": [{"internalType": "uint256", "name": "agentId", "type": "uint256"}],
        "name": "getClients",
        "outputs": [{"internalType": "address[]", "name": "", "type": "address[]"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"internalType": "uint256", "name": "agentId", "type": "uint256"},
            {"internalType": "address", "name": "clientAddress", "type": "address"},
        ],
        "name": "getLastIndex",
        "outputs": [{"internalType": "uint64", "name": "", "type": "uint64"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"internalType": "uint256", "name": "agentId", "type": "uint256"},
            {"internalType": "address", "name": "clientAddress", "type": "address"},
            {"internalType": "uint64", "name": "feedbackIndex", "type": "uint64"},
        ],
        "name": "readFeedback",
        "outputs": [
            {"internalType": "int128", "name": "value", "type": "int128"},
            {"internalType": "uint8", "name": "valueDecimals", "type": "uint8"},
            {"internalType": "string", "name": "tag1", "type": "string"},
            {"internalType": "string", "name": "tag2", "type": "string"},
            {"internalType": "bool", "name": "isRevoked", "type": "bool"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"internalType": "uint256", "name": "agentId", "type": "uint256"},
            {"internalType": "address[]", "name": "clientAddresses", "type": "address[]"},
            {"internalType": "string", "name": "tag1", "type": "string"},
            {"internalType": "string", "name": "tag2", "type": "string"},
        ],
        "name": "getSummary",
        "outputs": [
            {"internalType": "uint64", "name": "count", "type": "uint64"},
            {"internalType": "int128", "name": "summaryValue", "type": "int128"},
            {"internalType": "uint8", "name": "summaryValueDecimals", "type": "uint8"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
]

identity_contract = w3.eth.contract(address=checksum(IDENTITY_REGISTRY), abi=IDENTITY_ABI)
rep_contract = w3.eth.contract(address=checksum(REPUTATION_REGISTRY), abi=REPUTATION_ABI)


# =========================================================
# Discovery
# =========================================================

def fetch_identity_logs(start_block: int, end_block: int) -> List[dict]:
    return w3.eth.get_logs(
        {
            "fromBlock": int(start_block),
            "toBlock": int(end_block),
            "address": checksum(IDENTITY_REGISTRY),
            "topics": [TRANSFER_TOPIC],
        }
    )


def discover_target_agents(config: PipelineConfig) -> List[Dict[str, object]]:
    current_block = config.start_block
    discovered: Dict[int, Dict[str, object]] = {}
    current_window = config.scan_block_window

    print(
        f"[discovery] scanning from block {config.start_block} "
        f"to observation_block {config.observation_block}"
    )

    while current_block <= config.observation_block:
        chunk_end = min(current_block + current_window - 1, config.observation_block)

        try:
            logs = fetch_identity_logs(current_block, chunk_end)
        except Exception as exc:
            if current_window > 1:
                next_window = max(1, current_window // 2)
                print(
                    f"[discovery] get_logs failed for blocks {current_block}-{chunk_end}: {repr(exc)}. "
                    f"Retrying with smaller window {next_window}."
                )
                current_window = next_window
                continue
            raise

        for log in logs:
            topics = log.get("topics", [])
            if len(topics) != 4 or topics[0] != TRANSFER_TOPIC:
                continue
            if bytes(topics[1]) != ZERO_TOPIC_BYTES32:
                continue

            agent_id = int.from_bytes(bytes(topics[3]), byteorder="big")
            if agent_id < config.target_agent_id_min or agent_id > config.target_agent_id_max:
                continue

            if agent_id in discovered:
                continue

            discovered[agent_id] = {
                "agent_id": agent_id,
                "mint_block": int(log["blockNumber"]),
                "mint_tx_hash": norm_tx_hash(log["transactionHash"].hex()),
            }

        print(
            f"[discovery] blocks {current_block}-{chunk_end} "
            f"discovered={len(discovered)} window={current_window}"
        )

        if len(discovered) >= config.target_agent_count:
            break

        if current_window < config.scan_block_window:
            current_window = min(config.scan_block_window, current_window * 2)
        current_block = chunk_end + 1

    ordered = [discovered[agent_id] for agent_id in sorted(discovered)]
    trimmed = ordered[:config.target_agent_count]

    print(
        f"[discovery] selected {len(trimmed)} agents "
        f"for id range {config.target_agent_id_min}-{config.target_agent_id_max}"
    )
    return trimmed


# =========================================================
# Identity
# =========================================================

def fetch_identity_state(
    agent_seed: Dict[str, object], observation_block: int, config: PipelineConfig
) -> Dict[str, object]:
    agent_id = int(agent_seed["agent_id"])
    mint_block = int(agent_seed["mint_block"])
    mint_tx_hash = norm_tx_hash(agent_seed["mint_tx_hash"])

    owner = norm_addr(
        rpc_call_with_retry(
            lambda: identity_contract.functions.ownerOf(agent_id).call(
                block_identifier=observation_block
            ),
            config,
            f"ownerOf(agent_id={agent_id})",
        )
    )
    token_uri = rpc_call_with_retry(
        lambda: identity_contract.functions.tokenURI(agent_id).call(
            block_identifier=observation_block
        ),
        config,
        f"tokenURI(agent_id={agent_id})",
    )
    mint_block_data = rpc_call_with_retry(
        lambda: w3.eth.get_block(mint_block),
        config,
        f"get_block(mint_block={mint_block})",
    )

    return {
        "agent_id": agent_id,
        "mint_block": mint_block,
        "mint_timestamp": datetime.fromtimestamp(mint_block_data["timestamp"], tz=timezone.utc),
        "mint_tx_hash": mint_tx_hash,
        "owner_wallet": owner,
        "agent_wallet": None,
        "token_uri": token_uri,
        "metadata_hosting_type": detect_hosting_type(token_uri),
        "lifecycle_status": "metadata_linked" if token_uri else "minted_only",
        "observation_block": observation_block,
        "observation_timestamp": datetime.now(timezone.utc),
    }


def upsert_agents_core(records: Sequence[Dict[str, object]]) -> None:
    if not records:
        return

    rows = [
        (
            record["agent_id"],
            record["mint_block"],
            record["mint_timestamp"],
            record["mint_tx_hash"],
            record["owner_wallet"],
            record["agent_wallet"],
            record["token_uri"],
            record["metadata_hosting_type"],
            record["lifecycle_status"],
            record["observation_block"],
            record["observation_timestamp"],
        )
        for record in records
    ]

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            execute_batch(
                cur,
                """
                INSERT INTO agents_core (
                    agent_id,
                    mint_block,
                    mint_timestamp,
                    mint_tx_hash,
                    owner_wallet,
                    agent_wallet,
                    token_uri,
                    metadata_hosting_type,
                    lifecycle_status,
                    observation_block,
                    observation_timestamp
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (agent_id) DO UPDATE SET
                    mint_block = EXCLUDED.mint_block,
                    mint_timestamp = EXCLUDED.mint_timestamp,
                    mint_tx_hash = EXCLUDED.mint_tx_hash,
                    owner_wallet = EXCLUDED.owner_wallet,
                    agent_wallet = EXCLUDED.agent_wallet,
                    token_uri = EXCLUDED.token_uri,
                    metadata_hosting_type = EXCLUDED.metadata_hosting_type,
                    lifecycle_status = EXCLUDED.lifecycle_status,
                    observation_block = EXCLUDED.observation_block,
                    observation_timestamp = EXCLUDED.observation_timestamp
                """,
                rows,
                page_size=500,
            )


def upsert_mint_economics(records: Sequence[Dict[str, object]], config: PipelineConfig) -> None:
    if not records:
        return

    unique_txs: Dict[str, int] = {}
    for record in records:
        tx_hash = norm_tx_hash(record["mint_tx_hash"])
        if tx_hash and tx_hash not in unique_txs:
            unique_txs[tx_hash] = int(record["mint_block"])

    rows = []
    for tx_hash, mint_block in unique_txs.items():
        try:
            tx = rpc_call_with_retry(
                lambda: w3.eth.get_transaction(tx_hash),
                config,
                f"get_transaction(tx_hash={tx_hash})",
            )
            receipt = rpc_call_with_retry(
                lambda: w3.eth.get_transaction_receipt(tx_hash),
                config,
                f"get_transaction_receipt(tx_hash={tx_hash})",
            )
            block = rpc_call_with_retry(
                lambda: w3.eth.get_block(mint_block),
                config,
                f"get_block(mint_block={mint_block})",
            )
        except Exception as exc:
            print(f"[identity] failed mint economics tx_hash={tx_hash}: {repr(exc)}")
            continue

        gas_used = int(receipt["gasUsed"])
        gas_price_value = tx.get("gasPrice")
        if gas_price_value is None:
            gas_price_value = receipt.get("effectiveGasPrice")
        gas_price_wei = int(gas_price_value)
        mint_cost_eth = gas_used * gas_price_wei / 1e18

        rows.append(
            (
                tx_hash,
                gas_used,
                gas_price_wei,
                mint_cost_eth,
                int(tx["value"]),
                int(tx["nonce"]),
                int(block["gasLimit"]),
                int(block["gasUsed"]),
            )
        )

    if not rows:
        return

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            execute_batch(
                cur,
                """
                INSERT INTO mint_economics (
                    mint_tx_hash,
                    gas_used,
                    gas_price_wei,
                    mint_cost_eth,
                    tx_value_wei,
                    nonce,
                    block_gas_limit,
                    block_gas_used
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (mint_tx_hash) DO UPDATE SET
                    gas_used = EXCLUDED.gas_used,
                    gas_price_wei = EXCLUDED.gas_price_wei,
                    mint_cost_eth = EXCLUDED.mint_cost_eth,
                    tx_value_wei = EXCLUDED.tx_value_wei,
                    nonce = EXCLUDED.nonce,
                    block_gas_limit = EXCLUDED.block_gas_limit,
                    block_gas_used = EXCLUDED.block_gas_used
                """,
                rows,
                page_size=500,
            )


def run_identity_stage(
    agent_seeds: Sequence[Dict[str, object]], config: PipelineConfig
) -> Tuple[List[Dict[str, object]], Dict[str, int]]:
    successes: List[Dict[str, object]] = []
    failed = 0
    skipped = 0
    failed_seeds: List[Dict[str, object]] = []

    with ThreadPoolExecutor(max_workers=config.reputation_max_workers) as executor:
        future_map = {
            executor.submit(fetch_identity_state, seed, config.observation_block, config): seed
            for seed in agent_seeds
        }

        for future in as_completed(future_map):
            seed = future_map[future]
            agent_id = int(seed["agent_id"])

            try:
                record = future.result()
                successes.append(record)
            except Exception as exc:
                print(f"[identity] failed agent_id={agent_id}: {repr(exc)}")
                failed += 1
                failed_seeds.append(seed)

    if config.second_pass_retry_enabled and failed_seeds:
        print(f"[identity] second-pass retry for {len(failed_seeds)} failed agents (serial mode)")
        recovered: List[Dict[str, object]] = []
        still_failed: List[Dict[str, object]] = []

        for seed in failed_seeds:
            agent_id = int(seed["agent_id"])
            try:
                time.sleep(config.second_pass_retry_delay_seconds)
                record = fetch_identity_state(seed, config.observation_block, config)
                recovered.append(record)
            except Exception as exc:
                print(f"[identity] second-pass failed agent_id={agent_id}: {repr(exc)}")
                still_failed.append(seed)

        if recovered:
            successes.extend(recovered)
            failed -= len(recovered)
            print(
                f"[identity] second-pass recovered={len(recovered)} "
                f"remaining_failed={len(still_failed)}"
            )

    if successes:
        upsert_agents_core(successes)
        upsert_mint_economics(successes, config)

    stats = {
        "success": len(successes),
        "failed": failed,
        "skipped": skipped,
    }
    return successes, stats


# =========================================================
# Metadata
# =========================================================

def parse_registration_entry(agent_registry_str: Optional[str]) -> Optional[Dict[str, object]]:
    if not agent_registry_str:
        return None

    try:
        parts = agent_registry_str.split(":")
        if len(parts) < 3:
            return None

        return {
            "target_chain_namespace": parts[0],
            "target_chain_id": int(parts[1]),
            "target_identity_registry": norm_addr(parts[2]),
        }
    except Exception:
        return None


def fetch_metadata(identity_record: Dict[str, object], config: PipelineConfig) -> Optional[Dict[str, object]]:
    token_uri = identity_record.get("token_uri")
    if not token_uri:
        return None

    resolved_url = resolve_token_uri(token_uri)
    if not resolved_url:
        return None

    metadata_from_data_uri = parse_data_uri_json(resolved_url)
    if metadata_from_data_uri is not None:
        metadata = metadata_from_data_uri
        content_type = "application/json"
    else:
        response = requests.get(resolved_url, timeout=config.http_timeout)
        response.raise_for_status()
        if response.text.strip() == "":
            raise ValueError("metadata response body is empty")
        try:
            metadata = response.json()
        except Exception as exc:
            raise ValueError(f"metadata response is not valid JSON: {repr(exc)}") from exc
        content_type = response.headers.get("Content-Type", "")

    services = metadata.get("services", [])
    supported_trust = metadata.get("supportedTrust", [])
    registrations = metadata.get("registrations", [])
    x402_support = metadata.get("x402Support")
    if x402_support is None:
        x402_support = metadata.get("x402support")

    return {
        "agent_id": identity_record["agent_id"],
        "resolved_url": resolved_url,
        "content_type": content_type,
        "metadata_updated_at": parse_unix_ts(metadata.get("updatedAt")),
        "name": metadata.get("name"),
        "description": metadata.get("description"),
        "image_url": metadata.get("image"),
        "x402_support": x402_support,
        "active": metadata.get("active"),
        "service_count": len(services),
        "trust_count": len(supported_trust),
        "registration_count": len(registrations),
        "services": services,
        "registrations": registrations,
    }


def write_metadata_record(record: Dict[str, object], _observation_block: int) -> None:
    # token_uri is read at observation_block, but the fetched JSON content may be
    # newer when the URI points to mutable HTTPS-hosted metadata.
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO agent_metadata (
                    agent_id,
                    metadata_url_resolved,
                    metadata_content_type,
                    metadata_updated_at,
                    name,
                    description,
                    image_url,
                    x402_support,
                    active,
                    service_count,
                    trust_count,
                    registration_count
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (agent_id) DO UPDATE SET
                    metadata_url_resolved = EXCLUDED.metadata_url_resolved,
                    metadata_content_type = EXCLUDED.metadata_content_type,
                    metadata_updated_at = EXCLUDED.metadata_updated_at,
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    image_url = EXCLUDED.image_url,
                    x402_support = EXCLUDED.x402_support,
                    active = EXCLUDED.active,
                    service_count = EXCLUDED.service_count,
                    trust_count = EXCLUDED.trust_count,
                    registration_count = EXCLUDED.registration_count
                """,
                (
                    record["agent_id"],
                    record["resolved_url"],
                    record["content_type"],
                    record["metadata_updated_at"],
                    record["name"],
                    record["description"],
                    record["image_url"],
                    record["x402_support"],
                    record["active"],
                    record["service_count"],
                    record["trust_count"],
                    record["registration_count"],
                ),
            )

            cur.execute("DELETE FROM agent_services WHERE agent_id = %s", (record["agent_id"],))
            for index, service in enumerate(record["services"], start=1):
                cur.execute(
                    """
                    INSERT INTO agent_services (
                        agent_id,
                        service_order,
                        service_name,
                        endpoint,
                        version,
                        skills_json,
                        domains_json
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        record["agent_id"],
                        index,
                        service.get("name"),
                        service.get("endpoint"),
                        service.get("version"),
                        json.dumps(service.get("skills") or service.get("a2aSkills")),
                        json.dumps(service.get("domains")),
                    ),
                )

            cur.execute(
                "DELETE FROM crosschain_registrations WHERE source_agent_id = %s",
                (record["agent_id"],),
            )
            for registration in record["registrations"]:
                parsed = parse_registration_entry(registration.get("agentRegistry"))
                if not parsed:
                    continue
                cur.execute(
                    """
                    INSERT INTO crosschain_registrations (
                        source_agent_id,
                        target_chain_namespace,
                        target_chain_id,
                        target_identity_registry,
                        target_agent_id
                    )
                    VALUES (%s,%s,%s,%s,%s)
                    """,
                    (
                        record["agent_id"],
                        parsed["target_chain_namespace"],
                        parsed["target_chain_id"],
                        parsed["target_identity_registry"],
                        registration.get("agentId"),
                    ),
                )


def run_metadata_stage(
    identity_records: Sequence[Dict[str, object]], config: PipelineConfig
) -> Tuple[Dict[int, Dict[str, object]], Dict[str, int]]:
    successful_records: Dict[int, Dict[str, object]] = {}
    failed = 0
    skipped = 0
    failed_agent_ids: List[int] = []

    with ThreadPoolExecutor(max_workers=config.reputation_max_workers) as executor:
        future_map = {
            executor.submit(fetch_metadata, record, config): record
            for record in identity_records
        }

        for future in as_completed(future_map):
            identity_record = future_map[future]
            agent_id = int(identity_record["agent_id"])

            try:
                record = future.result()
                if record is None:
                    skipped += 1
                    continue

                write_metadata_record(record, config.observation_block)
                successful_records[agent_id] = record
            except Exception as exc:
                print(f"[metadata] failed agent_id={agent_id}: {repr(exc)}")
                failed += 1
                failed_agent_ids.append(agent_id)

    stats = {
        "success": len(successful_records),
        "failed": failed,
        "skipped": skipped,
        "failed_agent_ids": failed_agent_ids,
    }
    return successful_records, stats


# =========================================================
# Reputation
# =========================================================

def fetch_reputation(
    identity_record: Dict[str, object], observation_block: int, config: PipelineConfig
) -> Dict[str, object]:
    agent_id = int(identity_record["agent_id"])

    clients = rpc_call_with_retry(
        lambda: rep_contract.functions.getClients(agent_id).call(
            block_identifier=observation_block
        ),
        config,
        f"getClients(agent_id={agent_id})",
    )
    clients = [norm_addr(client) for client in clients]

    feedback_count_total = 0
    reputation_score_raw = 0
    reputation_score_decimals = 0

    if clients:
        checksum_clients = [checksum(client) for client in clients if client]
        count, raw_value, raw_decimals = rpc_call_with_retry(
            lambda: rep_contract.functions.getSummary(
                agent_id,
                checksum_clients,
                "",
                "",
            ).call(block_identifier=observation_block),
            config,
            f"getSummary(agent_id={agent_id}, clients={len(checksum_clients)})",
        )

        feedback_count_total = int(count)
        reputation_score_raw = int(raw_value)
        reputation_score_decimals = int(raw_decimals)

    feedback_rows = []
    for client in clients:
        if not client:
            continue

        client_checksum = checksum(client)
        last_index = int(
            rpc_call_with_retry(
                lambda: rep_contract.functions.getLastIndex(agent_id, client_checksum).call(
                    block_identifier=observation_block
                ),
                config,
                f"getLastIndex(agent_id={agent_id}, client={client})",
            )
        )

        for index in range(1, last_index + 1):
            value_raw, value_decimals, tag1, tag2, revoked = rpc_call_with_retry(
                lambda: rep_contract.functions.readFeedback(
                    agent_id,
                    client_checksum,
                    index,
                ).call(block_identifier=observation_block),
                config,
                f"readFeedback(agent_id={agent_id}, client={client}, index={index})",
            )

            feedback_rows.append(
                (
                    agent_id,
                    client,
                    int(index),
                    int(value_raw),
                    int(value_decimals),
                    tag1,
                    tag2,
                    bool(revoked),
                )
            )

    return {
        "summary": {
            "agent_id": agent_id,
            "client_count": len(clients),
            "feedback_count_total": feedback_count_total,
            "reputation_score_raw": reputation_score_raw,
            "reputation_score_decimals": reputation_score_decimals,
            "observation_block": observation_block,
            "observation_timestamp": datetime.now(timezone.utc),
        },
        "feedback_rows": feedback_rows,
    }


def write_reputation_record(summary: Dict[str, object], feedback_rows: Sequence[Tuple]) -> None:
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO agent_reputation_summary (
                    agent_id,
                    feedback_count_total,
                    reputation_score_raw,
                    reputation_score_decimals,
                    client_count,
                    observation_block,
                    observation_timestamp
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (agent_id) DO UPDATE SET
                    feedback_count_total = EXCLUDED.feedback_count_total,
                    reputation_score_raw = EXCLUDED.reputation_score_raw,
                    reputation_score_decimals = EXCLUDED.reputation_score_decimals,
                    client_count = EXCLUDED.client_count,
                    observation_block = EXCLUDED.observation_block,
                    observation_timestamp = EXCLUDED.observation_timestamp
                """,
                (
                    summary["agent_id"],
                    summary["feedback_count_total"],
                    summary["reputation_score_raw"],
                    summary["reputation_score_decimals"],
                    summary["client_count"],
                    summary["observation_block"],
                    summary["observation_timestamp"],
                ),
            )

            cur.execute(
                "DELETE FROM agent_feedback_records WHERE agent_id = %s",
                (summary["agent_id"],),
            )

            if feedback_rows:
                execute_batch(
                    cur,
                    """
                    INSERT INTO agent_feedback_records (
                        agent_id,
                        client_address,
                        feedback_index,
                        value_raw,
                        value_decimals,
                        tag1,
                        tag2,
                        is_revoked
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    list(feedback_rows),
                    page_size=500,
                )


def run_reputation_stage(
    identity_records: Sequence[Dict[str, object]], config: PipelineConfig
) -> Tuple[Dict[int, Dict[str, object]], Dict[str, int]]:
    successful_records: Dict[int, Dict[str, object]] = {}
    failed = 0
    skipped = 0
    failed_agent_ids: List[int] = []
    failed_identity_records: List[Dict[str, object]] = []

    with ThreadPoolExecutor(max_workers=config.reputation_max_workers) as executor:
        future_map = {
            executor.submit(fetch_reputation, record, config.observation_block, config): record
            for record in identity_records
        }

        for future in as_completed(future_map):
            identity_record = future_map[future]
            agent_id = int(identity_record["agent_id"])

            try:
                result = future.result()
                write_reputation_record(result["summary"], result["feedback_rows"])
                successful_records[agent_id] = result
            except Exception as exc:
                print(f"[reputation] failed agent_id={agent_id}: {repr(exc)}")
                failed += 1
                failed_agent_ids.append(agent_id)
                failed_identity_records.append(identity_record)

    if config.second_pass_retry_enabled and failed_identity_records:
        print(
            f"[reputation] second-pass retry for {len(failed_identity_records)} "
            "failed agents (serial mode)"
        )
        recovered_agent_ids: List[int] = []

        for identity_record in failed_identity_records:
            agent_id = int(identity_record["agent_id"])
            try:
                time.sleep(config.second_pass_retry_delay_seconds)
                result = fetch_reputation(identity_record, config.observation_block, config)
                write_reputation_record(result["summary"], result["feedback_rows"])
                successful_records[agent_id] = result
                recovered_agent_ids.append(agent_id)
            except Exception as exc:
                print(f"[reputation] second-pass failed agent_id={agent_id}: {repr(exc)}")

        if recovered_agent_ids:
            recovered_set = set(recovered_agent_ids)
            failed_agent_ids = [agent_id for agent_id in failed_agent_ids if agent_id not in recovered_set]
            failed = len(failed_agent_ids)
            print(
                f"[reputation] second-pass recovered={len(recovered_set)} "
                f"remaining_failed={failed}"
            )

    stats = {
        "success": len(successful_records),
        "failed": failed,
        "skipped": skipped,
        "failed_agent_ids": failed_agent_ids,
    }
    return successful_records, stats


# =========================================================
# Pipeline
# =========================================================

def load_identity_seeds_from_db(agent_ids: Sequence[int]) -> Tuple[List[Dict[str, object]], List[int]]:
    if not agent_ids:
        return [], []

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT agent_id, mint_block, mint_tx_hash
                FROM agents_core
                WHERE agent_id = ANY(%s)
                """,
                (list(agent_ids),),
            )
            rows = cur.fetchall()

    seeds = []
    found_ids = set()
    for agent_id, mint_block, mint_tx_hash in rows:
        found_ids.add(int(agent_id))
        if mint_block is None or mint_tx_hash is None:
            continue
        seeds.append(
            {
                "agent_id": int(agent_id),
                "mint_block": int(mint_block),
                "mint_tx_hash": norm_tx_hash(mint_tx_hash),
            }
        )
    missing_ids = sorted(set(agent_ids) - {int(seed["agent_id"]) for seed in seeds})
    return sorted(seeds, key=lambda x: int(x["agent_id"])), missing_ids


def load_identity_records_from_db(agent_ids: Sequence[int]) -> Tuple[List[Dict[str, object]], List[int]]:
    if not agent_ids:
        return [], []

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    agent_id,
                    mint_block,
                    mint_timestamp,
                    mint_tx_hash,
                    owner_wallet,
                    agent_wallet,
                    token_uri,
                    metadata_hosting_type,
                    lifecycle_status,
                    observation_block,
                    observation_timestamp
                FROM agents_core
                WHERE agent_id = ANY(%s)
                """,
                (list(agent_ids),),
            )
            rows = cur.fetchall()

    records = []
    for row in rows:
        records.append(
            {
                "agent_id": int(row[0]),
                "mint_block": int(row[1]) if row[1] is not None else None,
                "mint_timestamp": row[2],
                "mint_tx_hash": norm_tx_hash(row[3]),
                "owner_wallet": norm_addr(row[4]),
                "agent_wallet": norm_addr(row[5]) if row[5] else None,
                "token_uri": row[6],
                "metadata_hosting_type": row[7],
                "lifecycle_status": row[8],
                "observation_block": int(row[9]) if row[9] is not None else None,
                "observation_timestamp": row[10],
            }
        )
    missing_ids = sorted(set(agent_ids) - {int(record["agent_id"]) for record in records})
    return sorted(records, key=lambda x: int(x["agent_id"])), missing_ids

def print_batch_stats(stage_name: str, batch_number: int, stats: Dict[str, int]) -> None:
    print(
        f"[batch {batch_number}] {stage_name}: "
        f"success={stats['success']} failed={stats['failed']} skipped={stats['skipped']}"
    )


def run_pipeline(config: PipelineConfig) -> None:
    print("Connected:", w3.is_connected())
    print("Latest block:", w3.eth.block_number)
    print("Start block:", config.start_block)
    print("Observation block:", config.observation_block)
    print(
        "Target agent ids:",
        f"{config.target_agent_id_min}-{config.target_agent_id_max}",
    )
    print("Target agent count:", config.target_agent_count)
    print("Pipeline batch size:", config.pipeline_batch_size)
    print("Run stages:", {
        "identity": config.run_identity,
        "metadata": config.run_metadata,
        "reputation": config.run_reputation,
    })
    print("Rerun only:", config.rerun_only)
    print("Rerun agent ids:", list(config.rerun_agent_ids))
    print("Rerun-only stages:", list(config.rerun_only_stages))

    failed_identity_agents: List[int] = []
    failed_metadata_agents: List[int] = []
    failed_reputation_agents: List[int] = []

    if config.rerun_only and config.rerun_agent_ids:
        target_ids = sorted(set(int(agent_id) for agent_id in config.rerun_agent_ids))
        print(f"[rerun] running only specified agent ids: {target_ids}")

        if config.run_identity and "identity" in config.rerun_only_stages:
            discovered_agents, missing_identity_seed_ids = load_identity_seeds_from_db(target_ids)
            failed_identity_agents.extend(missing_identity_seed_ids)
            identity_records, identity_stats = run_identity_stage(discovered_agents, config)
            print_batch_stats("identity", 1, identity_stats)
            succeeded_ids = {int(record["agent_id"]) for record in identity_records}
            failed_identity_agents.extend(
                [agent_id for agent_id in target_ids if agent_id not in succeeded_ids]
            )
        else:
            identity_records, missing_identity_record_ids = load_identity_records_from_db(target_ids)
            failed_identity_agents.extend(missing_identity_record_ids)
            if missing_identity_record_ids:
                print(
                    "[rerun] missing prerequisite identity records in DB for agent_ids="
                    f"{missing_identity_record_ids}"
                )
    else:
        discovered_agents = discover_target_agents(config)
        if not discovered_agents:
            print("No agents discovered for the current configuration.")
            return

        identity_records = []
        for batch_number, agent_batch in enumerate(
            chunked(discovered_agents, config.pipeline_batch_size),
            start=1,
        ):
            batch_agent_ids = [int(agent["agent_id"]) for agent in agent_batch]
            print(f"[batch {batch_number}] agent_ids={batch_agent_ids[0]}-{batch_agent_ids[-1]}")

            if config.run_identity:
                batch_identity_records, identity_stats = run_identity_stage(agent_batch, config)
                print_batch_stats("identity", batch_number, identity_stats)
                succeeded_ids = {int(record["agent_id"]) for record in batch_identity_records}
                failed_identity_agents.extend(
                    [agent_id for agent_id in batch_agent_ids if agent_id not in succeeded_ids]
                )
            else:
                batch_identity_records, missing_ids = load_identity_records_from_db(batch_agent_ids)
                failed_identity_agents.extend(missing_ids)

            if not batch_identity_records:
                print(f"[batch {batch_number}] no identity records available, skipping downstream stages")
                continue

            if config.run_metadata:
                _, metadata_stats = run_metadata_stage(batch_identity_records, config)
                print_batch_stats("metadata", batch_number, metadata_stats)
                failed_metadata_agents.extend(metadata_stats["failed_agent_ids"])

            if config.run_reputation:
                _, reputation_stats = run_reputation_stage(batch_identity_records, config)
                print_batch_stats("reputation", batch_number, reputation_stats)
                failed_reputation_agents.extend(reputation_stats["failed_agent_ids"])

            identity_records.extend(batch_identity_records)

    if config.rerun_only and config.rerun_agent_ids:
        target_ids = sorted(set(int(agent_id) for agent_id in config.rerun_agent_ids))

        if config.run_metadata and "metadata" in config.rerun_only_stages and identity_records:
            _, metadata_stats = run_metadata_stage(identity_records, config)
            print_batch_stats("metadata", 1, metadata_stats)
            failed_metadata_agents.extend(metadata_stats["failed_agent_ids"])
        elif config.run_metadata and "metadata" in config.rerun_only_stages:
            failed_metadata_agents.extend(target_ids)

        if config.run_reputation and "reputation" in config.rerun_only_stages and identity_records:
            _, reputation_stats = run_reputation_stage(identity_records, config)
            print_batch_stats("reputation", 1, reputation_stats)
            failed_reputation_agents.extend(reputation_stats["failed_agent_ids"])
        elif config.run_reputation and "reputation" in config.rerun_only_stages:
            failed_reputation_agents.extend(target_ids)

    final_failed_identity = sorted(set(failed_identity_agents))
    final_failed_metadata = sorted(set(failed_metadata_agents))
    final_failed_reputation = sorted(set(failed_reputation_agents))
    final_failed_all = sorted(
        set(final_failed_identity + final_failed_metadata + final_failed_reputation)
    )

    print("FINAL_FAILED_IDENTITY =", final_failed_identity)
    print("FINAL_FAILED_METADATA =", final_failed_metadata)
    print("FINAL_FAILED_REPUTATION =", final_failed_reputation)
    print("FINAL_FAILED_ALL =", final_failed_all)
    print("RERUN_AGENT_IDS =", final_failed_all)

    failed_agents_summary = {
        "identity": final_failed_identity,
        "metadata": final_failed_metadata,
        "reputation": final_failed_reputation,
        "all_failed": final_failed_all,
    }
    with open("failed_agents_last_run.json", "w", encoding="utf-8") as f:
        json.dump(failed_agents_summary, f, ensure_ascii=False, indent=2)

    if config.fail_on_incomplete_snapshot and final_failed_all:
        raise RuntimeError(
            "Pipeline finished with incomplete snapshot. "
            f"identity_failed={len(final_failed_identity)} "
            f"metadata_failed={len(final_failed_metadata)} "
            f"reputation_failed={len(final_failed_reputation)}"
        )

    print("Pipeline completed.")


def main() -> None:
    run_pipeline(PipelineConfig())


if __name__ == "__main__":
    main()
