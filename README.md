# ERC-8004 Agent Dataset — Collection Pipeline

This repository contains the code used to collect, process, and validate the dataset described in:

> **A dataset of early blockchain-registered AI agents on Ethereum**
> Yulin Liu, *Scientific Data* (2026, in press)

The dataset itself (nine CSV files, one JSONL snapshot, and a data dictionary) is archived at Harvard Dataverse: <https://doi.org/10.7910/DVN/HJZW8Q> (CC0 1.0).

## Overview

The pipeline backfills ERC-8004 agent data from Ethereum mainnet into a Postgres database in three fixed stages:

1. `identity` — owner, token URI, and mint transaction data for each agent
2. `metadata` — resolution and parsing of token-linked off-chain metadata (`agent_metadata`, `agent_services`, `crosschain_registrations`)
3. `reputation` — per-agent reputation summaries and individual feedback records (`agent_reputation_summary`, `agent_feedback_records`)

All dynamic contract state is read at a single fixed `observation_block` via historical `eth_call`, so every agent is described at one consistent block height and results are exactly reproducible against any archive node.

## Repository structure

- `scripts/main.py` — main pipeline (discovery, identity, metadata, reputation) and run configuration
- `.github/workflows/backfill.yml` — GitHub Actions workflow for manual runs
- `requirements.txt` — Python dependencies (web3, psycopg2-binary, pandas, requests, python-dateutil)

## Configuration used for the released dataset

Configuration lives at the top of `scripts/main.py`:

- `START_BLOCK = 24339925` (first mint, 2026-01-29)
- `OBSERVATION_BLOCK = START_BLOCK + 500000` (= 24,839,925, the fixed observation block)
- `TARGET_AGENT_ID_MIN = 0`, `TARGET_AGENT_ID_MAX = 9999`, `TARGET_AGENT_COUNT = 10000`
- `PIPELINE_BATCH_SIZE = 100`, `MAX_WORKERS = 8`

Contract addresses (Ethereum mainnet):

- Identity registry: `0x8004A169FB4a3325136EB29fA0ceB6D2e539a432`
- Reputation registry: `0x8004BAa17C55a88189AE136b182e5fdA19dE9b63`

## Requirements

- Python 3.10+; install dependencies with `pip install -r requirements.txt`
- An **archive-capable** Ethereum RPC endpoint. The pipeline performs historical `eth_call` (`ownerOf`, `tokenURI`, and reputation reads pinned to `observation_block`); ordinary public RPCs may support `eth_getLogs` but fail historical state queries with errors such as `historical state ... is not available`. Set it via the environment variable `ETHEREUM_ARCHIVE_RPC` (falls back to `https://ethereum-rpc.publicnode.com`, which is suitable only for quick local experiments).
- A Postgres connection string via `NEON_DATABASE_URL`.

For GitHub Actions runs, both values are provided as repository secrets (`NEON_DATABASE_URL`, `Ethereum_archive_RPC`).

## Pipeline behavior

### Discovery

The script scans identity-registry `Transfer` logs from `START_BLOCK` to `OBSERVATION_BLOCK` and keeps mint events (`from == 0x0…0`), filtered to the configured agent-ID range. Logs are requested in fixed-size chunks with adaptive chunk-size reduction on RPC errors, bounded concurrency, rate limiting, and exponential-backoff retries.

### Identity, metadata, reputation

Each stage reads contract state at `observation_block`. Note one documented nuance: the token URI itself is read at the fixed block, but the content behind an HTTPS URI is mutable and may be newer than the block timestamp. The released dataset therefore includes a frozen snapshot of all retrieved metadata documents (`metadata_raw_snapshot.jsonl`) with SHA-256 hashes.

Agents are processed in batches of `PIPELINE_BATCH_SIZE`; single-agent failures do not stop a batch. `agents_core` rows are upserted (`ON CONFLICT DO UPDATE`).

### Transfer history

`transfer_history` is an auxiliary table built from a scanned window of the first 100,000 blocks after the first mint (`24339925`–`24439925`), matching the window reported in the paper.

## Failure handling and reruns

Partial failures (HTTP 429, invalid or non-JSON metadata, unreachable URLs) are expected in large runs. Transient RPC failures are retried automatically, including a serial second-pass retry. At the end of each run the script prints final per-stage failure lists (`FINAL_FAILED_IDENTITY/METADATA/REPUTATION/ALL`, `RERUN_AGENT_IDS`) and writes `failed_agents_last_run.json`. Only agents that remain failed after all retries need rerunning.

To rerun specific agents, set in `scripts/main.py`:

```python
RERUN_AGENT_IDS = [...]
RERUN_ONLY = True
# optional stage control:
RUN_IDENTITY = False
RUN_METADATA = True
RUN_REPUTATION = False
```

## Running via GitHub Actions

1. Open the **Actions** tab and select `ERC8004 Manual Run`.
2. Click **Run workflow**. There are no runtime form inputs; parameters are edited directly in `scripts/main.py` before running.

## Citation

If you use this dataset or code, please cite:

```
Liu, Y. A dataset of early blockchain-registered AI agents on Ethereum.
Sci Data (2026). https://doi.org/10.7910/DVN/HJZW8Q
```


## License

Code: MIT. Dataset: CC0 1.0 at Harvard Dataverse.
