import os
import json
import time
import argparse
import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from web3 import Web3

# =========================================================
# User Configuration
# Only NEON_DATABASE_URL is secret.
# Everything else can remain visible in the repo.
# =========================================================

RPC_URL = "https://ethereum-rpc.publicnode.com"
NEON_DATABASE_URL = os.environ["NEON_DATABASE_URL"]

CHAIN_ID = 1
NETWORK = "Ethereum"

IDENTITY_REGISTRY = "0x8004A169FB4a3325136EB29fA0ceB6D2e539a432"
REPUTATION_REGISTRY = "0x8004BAa17C55a88189AE136b182e5fdA19dE9b63"

CONTRACT_NAME = "AgentIdentity"
CONTRACT_SYMBOL = "AGENT"

SCAN_BLOCK_WINDOW = 500
AGENT_BATCH_SIZE = 100
MAX_WORKERS = 10
HTTP_TIMEOUT = 20

w3 = Web3(Web3.HTTPProvider(RPC_URL))
http = requests.Session()

# =========================================================
# Helper Functions
# =========================================================

def norm_addr(addr):
    return str(addr).lower() if addr else None

def norm_tx_hash(txh):
    return str(txh).lower() if txh else None

def checksum(addr):
    return Web3.to_checksum_address(addr)

def parse_unix_ts(ts):
    if ts is None:
        return None
    return datetime.fromtimestamp(int(ts), tz=timezone.utc)

def detect_hosting_type(token_uri):
    if not token_uri:
        return None
    s = token_uri.lower()
    if s.startswith("ipfs://"):
        return "ipfs"
    if s.startswith("http://") or s.startswith("https://"):
        return "https"
    return "other"

def resolve_token_uri(token_uri):
    if not token_uri:
        return None
    if token_uri.startswith("ipfs://"):
        cid = token_uri.replace("ipfs://", "")
        return f"https://ipfs.io/ipfs/{cid}"
    return token_uri

def get_db_conn():
    return psycopg2.connect(NEON_DATABASE_URL)

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

REPUTATION_ABI_FULL = [
    {
        "inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"}],
        "name":"getClients",
        "outputs":[{"internalType":"address[]","name":"","type":"address[]"}],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[
            {"internalType":"uint256","name":"agentId","type":"uint256"},
            {"internalType":"address","name":"clientAddress","type":"address"}
        ],
        "name":"getLastIndex",
        "outputs":[{"internalType":"uint64","name":"","type":"uint64"}],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[
            {"internalType":"uint256","name":"agentId","type":"uint256"},
            {"internalType":"address","name":"clientAddress","type":"address"},
            {"internalType":"uint64","name":"feedbackIndex","type":"uint64"}
        ],
        "name":"readFeedback",
        "outputs":[
            {"internalType":"int128","name":"value","type":"int128"},
            {"internalType":"uint8","name":"valueDecimals","type":"uint8"},
            {"internalType":"string","name":"tag1","type":"string"},
            {"internalType":"string","name":"tag2","type":"string"},
            {"internalType":"bool","name":"isRevoked","type":"bool"}
        ],
        "stateMutability":"view",
        "type":"function"
    },
    {
        "inputs":[
            {"internalType":"uint256","name":"agentId","type":"uint256"},
            {"internalType":"address[]","name":"clientAddresses","type":"address[]"},
            {"internalType":"string","name":"tag1","type":"string"},
            {"internalType":"string","name":"tag2","type":"string"}
        ],
        "name":"getSummary",
        "outputs":[
            {"internalType":"uint64","name":"count","type":"uint64"},
            {"internalType":"int128","name":"summaryValue","type":"int128"},
            {"internalType":"uint8","name":"summaryValueDecimals","type":"uint8"}
        ],
        "stateMutability":"view",
        "type":"function"
    }
]

identity_contract = w3.eth.contract(
    address=checksum(IDENTITY_REGISTRY),
    abi=IDENTITY_ABI
)

rep_contract = w3.eth.contract(
    address=checksum(REPUTATION_REGISTRY),
    abi=REPUTATION_ABI_FULL
)

# =========================================================
# Stage A — Fast Mint Discovery
# =========================================================

TRANSFER_TOPIC = w3.keccak(text="Transfer(address,address,uint256)")
ZERO_TOPIC_BYTES32 = b"\x00" * 32

def fetch_logs(start_block, end_block):
    return w3.eth.get_logs({
        "fromBlock": int(start_block),
        "toBlock": int(end_block),
        "address": checksum(IDENTITY_REGISTRY),
    })

def extract_mint_candidates(logs):
    candidates = []
    for log in logs:
        topics = log["topics"]
        if len(topics) == 4 and topics[0] == TRANSFER_TOPIC:
            if bytes(topics[1]) == ZERO_TOPIC_BYTES32:
                agent_id = int.from_bytes(bytes(topics[3]), byteorder="big")
                owner_wallet = norm_addr("0x" + bytes(topics[2])[-20:].hex())
                candidates.append({
                    "agent_id": agent_id,
                    "owner_wallet": owner_wallet,
                    "mint_tx_hash": norm_tx_hash(log["transactionHash"].hex()),
                    "mint_block": int(log["blockNumber"]),
                })
    return candidates

def validate_one_agent(cand):
    try:
        agent_id = cand["agent_id"]
        owner = norm_addr(identity_contract.functions.ownerOf(agent_id).call())
        token_uri = identity_contract.functions.tokenURI(agent_id).call()
        block = w3.eth.get_block(cand["mint_block"])

        return {
            "agent_id": agent_id,
            "mint_block": cand["mint_block"],
            "mint_timestamp": datetime.fromtimestamp(block["timestamp"], tz=timezone.utc),
            "mint_tx_hash": cand["mint_tx_hash"],
            "owner_wallet": owner,
            "agent_wallet": None,
            "token_uri": token_uri,
            "metadata_hosting_type": detect_hosting_type(token_uri),
            "lifecycle_status": "metadata_linked" if token_uri else "minted_only",
            "observation_block": cand["mint_block"],
            "observation_timestamp": datetime.now(timezone.utc),
        }
    except Exception as e:
        print(f"validate_one_agent failed for agent {cand.get('agent_id')}: {repr(e)}")
        return None

def parallel_validate_agents(candidates):
    validated = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(validate_one_agent, c) for c in candidates]

        for future in as_completed(futures):
            result = future.result()
            if result:
                validated.append(result)

    return validated

def upsert_agents_core(validated_agents):
    if not validated_agents:
        return

    rows = [
        (
            a["agent_id"],
            a["mint_block"],
            a["mint_timestamp"],
            a["mint_tx_hash"],
            a["owner_wallet"],
            a["agent_wallet"],
            a["token_uri"],
            a["metadata_hosting_type"],
            a["lifecycle_status"],
            a["observation_block"],
            a["observation_timestamp"],
        )
        for a in validated_agents
    ]

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        execute_batch(cur, """
            INSERT INTO agents_core (
                agent_id, mint_block, mint_timestamp, mint_tx_hash,
                owner_wallet, agent_wallet, token_uri,
                metadata_hosting_type, lifecycle_status,
                observation_block, observation_timestamp
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (agent_id) DO NOTHING
        """, rows, page_size=500)
        conn.commit()
    finally:
        cur.close()
        conn.close()

def upsert_mint_economics(validated_agents):
    if not validated_agents:
        print("mint_economics inserted: 0")
        return

    unique_txs = {}

    for a in validated_agents:
        txh = norm_tx_hash(a["mint_tx_hash"])
        if txh not in unique_txs:
            unique_txs[txh] = int(a["mint_block"])

    rows = []

    for tx_hash, mint_block in unique_txs.items():
        try:
            tx = w3.eth.get_transaction(tx_hash)
            receipt = w3.eth.get_transaction_receipt(tx_hash)
            block = w3.eth.get_block(mint_block)

            gas_used = int(receipt["gasUsed"])
            gas_price_wei = int(tx["gasPrice"])
            mint_cost_eth = gas_used * gas_price_wei / 1e18

            rows.append((
                tx_hash,
                gas_used,
                gas_price_wei,
                mint_cost_eth,
                int(tx["value"]),
                int(tx["nonce"]),
                int(block["gasLimit"]),
                int(block["gasUsed"]),
            ))

        except Exception as e:
            print(f"Skip mint tx {tx_hash}: {repr(e)}")

    if not rows:
        print("mint_economics inserted: 0")
        return

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        execute_batch(cur, """
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
        """, rows, page_size=500)
        conn.commit()
    finally:
        cur.close()
        conn.close()

    print(f"mint_economics inserted: {len(rows)}")

def run_stage_a(start_block, end_block):
    current = start_block
    mint_buffer = []

    while current <= end_block:
        chunk_end = min(current + SCAN_BLOCK_WINDOW - 1, end_block)

        print(f"Scanning blocks {current} -> {chunk_end}")

        logs = fetch_logs(current, chunk_end)
        candidates = extract_mint_candidates(logs)
        mint_buffer.extend(candidates)

        while len(mint_buffer) >= AGENT_BATCH_SIZE:
            batch = mint_buffer[:AGENT_BATCH_SIZE]
            mint_buffer = mint_buffer[AGENT_BATCH_SIZE:]

            validated = parallel_validate_agents(batch)
            upsert_agents_core(validated)
            upsert_mint_economics(validated)

            print(f"Inserted batch: {len(validated)} agents")

        current = chunk_end + 1

    if mint_buffer:
        validated = parallel_validate_agents(mint_buffer)
        upsert_agents_core(validated)
        upsert_mint_economics(validated)
        print(f"Inserted final batch: {len(validated)} agents")

# =========================================================
# Stage B — Metadata Enrichment
# =========================================================

def load_agents_for_metadata(batch_size=500):
    conn = get_db_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT agent_id, token_uri
            FROM agents_core
            WHERE agent_id NOT IN (
                SELECT agent_id FROM agent_metadata
            )
            ORDER BY agent_id
            LIMIT %s
        """, (batch_size,))
        rows = cur.fetchall()
        return rows
    finally:
        cur.close()
        conn.close()

def parse_registration_entry(agent_registry_str):
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

def process_one_metadata(agent_id, token_uri):
    if not token_uri:
        return None

    resolved_url = resolve_token_uri(token_uri)
    if not resolved_url:
        return None

    try:
        resp = http.get(resolved_url, timeout=HTTP_TIMEOUT)
        if resp.status_code != 200:
            return None

        meta = resp.json()

        services = meta.get("services", [])
        supported_trust = meta.get("supportedTrust", [])
        registrations = meta.get("registrations", [])

        return {
            "agent_id": agent_id,
            "resolved_url": resolved_url,
            "content_type": resp.headers.get("Content-Type", ""),
            "metadata_updated_at": parse_unix_ts(meta.get("updatedAt")),
            "name": meta.get("name"),
            "description": meta.get("description"),
            "image_url": meta.get("image"),
            "x402_support": meta.get("x402support"),
            "active": meta.get("active"),
            "service_count": len(services),
            "trust_count": len(supported_trust),
            "registration_count": len(registrations),
            "services": services,
            "registrations": registrations,
        }

    except Exception as e:
        print(f"process_one_metadata failed for agent {agent_id}: {repr(e)}")
        return None

def write_metadata_record(record):
    conn = get_db_conn()
    cur = conn.cursor()

    agent_id = record["agent_id"]

    try:
        cur.execute("""
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
        """, (
            agent_id,
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
        ))

        cur.execute("DELETE FROM agent_services WHERE agent_id=%s", (agent_id,))
        for idx, svc in enumerate(record["services"], start=1):
            cur.execute("""
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
            """, (
                agent_id,
                idx,
                svc.get("name"),
                svc.get("endpoint"),
                svc.get("version"),
                json.dumps(svc.get("skills") or svc.get("a2aSkills")),
                json.dumps(svc.get("domains")),
            ))

        cur.execute("DELETE FROM crosschain_registrations WHERE source_agent_id=%s", (agent_id,))
        for reg in record["registrations"]:
            parsed = parse_registration_entry(reg.get("agentRegistry"))
            if parsed:
                cur.execute("""
                    INSERT INTO crosschain_registrations (
                        source_agent_id,
                        target_chain_namespace,
                        target_chain_id,
                        target_identity_registry,
                        target_agent_id
                    )
                    VALUES (%s,%s,%s,%s,%s)
                """, (
                    agent_id,
                    parsed["target_chain_namespace"],
                    parsed["target_chain_id"],
                    parsed["target_identity_registry"],
                    reg.get("agentId"),
                ))

        conn.commit()
    finally:
        cur.close()
        conn.close()

def run_stage_b(batch_size=500, max_workers=8):
    rows = load_agents_for_metadata(batch_size)
    print("Agents to enrich:", len(rows))

    processed = 0
    skipped = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(process_one_metadata, agent_id, token_uri): (agent_id, token_uri)
            for agent_id, token_uri in rows
        }

        for future in as_completed(future_map):
            agent_id, token_uri = future_map[future]

            try:
                record = future.result()

                if record is None:
                    skipped += 1
                    continue

                write_metadata_record(record)
                processed += 1

            except Exception as e:
                print(f"Parallel metadata skip agent {agent_id}: {repr(e)}")
                skipped += 1

    print({
        "metadata_processed": processed,
        "metadata_skipped": skipped
    })

# =========================================================
# Stage C — Reputation Enrichment
# =========================================================

def load_agents_for_reputation(batch_size=200):
    conn = get_db_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT agent_id
            FROM agents_core
            WHERE agent_id NOT IN (
                SELECT agent_id FROM agent_reputation_summary
            )
            ORDER BY agent_id
            LIMIT %s
        """, (batch_size,))
        rows = [r[0] for r in cur.fetchall()]
        return rows
    finally:
        cur.close()
        conn.close()

def fetch_one_agent_reputation(agent_id, observation_block):
    try:
        clients = rep_contract.functions.getClients(int(agent_id)).call()
        clients = [norm_addr(c) for c in clients]

        feedback_count_total = 0
        reputation_score_raw = 0
        reputation_score_decimals = 0

        if len(clients) > 0:
            checksum_clients = [checksum(c) for c in clients]
            count, raw_value, raw_decimals = rep_contract.functions.getSummary(
                int(agent_id),
                checksum_clients,
                "",
                ""
            ).call()

            feedback_count_total = int(count)
            reputation_score_raw = int(raw_value)
            reputation_score_decimals = int(raw_decimals)

        feedback_rows = []

        for client in clients:
            client_cs = checksum(client)

            try:
                last_index = int(
                    rep_contract.functions.getLastIndex(int(agent_id), client_cs).call()
                )
            except Exception:
                continue

            for idx in range(1, last_index + 1):
                try:
                    value_raw, value_decimals, tag1, tag2, revoked = rep_contract.functions.readFeedback(
                        int(agent_id),
                        client_cs,
                        idx
                    ).call()

                    feedback_rows.append((
                        int(agent_id),
                        client,
                        int(idx),
                        int(value_raw),
                        int(value_decimals),
                        tag1,
                        tag2,
                        bool(revoked)
                    ))

                except Exception:
                    continue

        summary = {
            "agent_id": int(agent_id),
            "client_count": len(clients),
            "feedback_count_total": feedback_count_total,
            "reputation_score_raw": reputation_score_raw,
            "reputation_score_decimals": reputation_score_decimals,
            "observation_block": observation_block,
            "observation_timestamp": datetime.now(timezone.utc),
        }

        return summary, feedback_rows

    except Exception as e:
        print(f"Skip reputation agent {agent_id}: {repr(e)}")
        return None, []

def write_reputation_record(summary, feedback_rows):
    conn = get_db_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
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
        """, (
            summary["agent_id"],
            summary["feedback_count_total"],
            summary["reputation_score_raw"],
            summary["reputation_score_decimals"],
            summary["client_count"],
            summary["observation_block"],
            summary["observation_timestamp"],
        ))

        cur.execute("""
            DELETE FROM agent_feedback_records
            WHERE agent_id = %s
        """, (summary["agent_id"],))

        if feedback_rows:
            execute_batch(cur, """
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
            """, feedback_rows, page_size=500)

        conn.commit()

    finally:
        cur.close()
        conn.close()

def run_stage_c(batch_size=200, max_workers=8):
    agent_ids = load_agents_for_reputation(batch_size)
    observation_block = w3.eth.block_number

    print("Reputation observation block:", observation_block)
    print("Agents to enrich reputation:", len(agent_ids))

    processed = 0
    skipped = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(fetch_one_agent_reputation, agent_id, observation_block): agent_id
            for agent_id in agent_ids
        }

        for future in as_completed(future_map):
            agent_id = future_map[future]

            try:
                summary, feedback_rows = future.result()

                if summary is None:
                    skipped += 1
                    continue

                write_reputation_record(summary, feedback_rows)
                processed += 1

            except Exception as e:
                print(f"Parallel skip agent {agent_id}: {repr(e)}")
                skipped += 1

    print({
        "reputation_processed": processed,
        "reputation_skipped": skipped
    })

def run_stage_c_until_done(batch_size=200):
    total_processed = 0
    observation_block = w3.eth.block_number

    print("Reputation observation block:", observation_block)

    while True:
        agent_ids = load_agents_for_reputation(batch_size)

        if len(agent_ids) == 0:
            break

        print(f"Processing batch of {len(agent_ids)} agents...")

        processed = 0
        skipped = 0

        for agent_id in agent_ids:
            summary, feedback_rows = fetch_one_agent_reputation(agent_id, observation_block)

            if summary is None:
                skipped += 1
                continue

            write_reputation_record(summary, feedback_rows)
            processed += 1

        total_processed += processed

        print({
            "batch_processed": processed,
            "batch_skipped": skipped,
            "total_processed_so_far": total_processed
        })

    print("All remaining reputation agents completed.")

# =========================================================
# CLI
# =========================================================

def main():
    parser = argparse.ArgumentParser(description="ERC-8004 V3 pipeline runner")
    parser.add_argument("--stage", required=True, choices=["a", "b", "c", "c_all"])
    parser.add_argument("--start-block", type=int, default=None)
    parser.add_argument("--end-block", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=None)
    parser.add_argument("--max-workers", type=int, default=None)

    args = parser.parse_args()

    global MAX_WORKERS
    if args.max_workers is not None:
        MAX_WORKERS = args.max_workers

    print("Connected:", w3.is_connected())
    print("Latest block:", w3.eth.block_number)
    print("Stage:", args.stage)

    if args.stage == "a":
        if args.start_block is None or args.end_block is None:
            raise ValueError("Stage A requires --start-block and --end-block")
        run_stage_a(args.start_block, args.end_block)

    elif args.stage == "b":
        batch_size = args.batch_size if args.batch_size is not None else 500
        run_stage_b(batch_size=batch_size, max_workers=MAX_WORKERS)

    elif args.stage == "c":
        batch_size = args.batch_size if args.batch_size is not None else 200
        run_stage_c(batch_size=batch_size, max_workers=MAX_WORKERS)

    elif args.stage == "c_all":
        batch_size = args.batch_size if args.batch_size is not None else 200
        run_stage_c_until_done(batch_size=batch_size)

if __name__ == "__main__":
    main()