# ERC8004 Backfill Pipeline

This repository backfills ERC-8004 agent data into Neon/Postgres in three fixed steps:

1. `identity`
2. `metadata`
3. `reputation`

The pipeline is designed around a single `observation_block`. For one run, all on-chain reads try to use the same block height so the dataset is internally consistent.

## Current Run Configuration

The main configuration lives at the top of [scripts/main.py](C:\Users\yulin\OneDrive\Documents\GitHub\ERC8004\scripts\main.py:19).

Important values:

- `START_BLOCK = 24339925`
- `OBSERVATION_BLOCK = START_BLOCK + 500000`
- `TARGET_AGENT_ID_MIN = 0`
- `TARGET_AGENT_ID_MAX = 999`
- `TARGET_AGENT_COUNT = 1000`
- `PIPELINE_BATCH_SIZE = 100`
- `MAX_WORKERS = 8`

These values are intentionally kept in code so manual adjustments are easy before each GitHub Actions run.

## Why Archive RPC Is Required

The pipeline does not only scan logs. It also performs historical contract calls such as:

- `ownerOf(agentId)` at `observation_block`
- `tokenURI(agentId)` at `observation_block`
- reputation contract reads at `observation_block`

A normal public RPC may support `get_logs`, but still fail historical `eth_call` with errors like:

- `historical state ... is not available`

Because of that, this pipeline must use an archive-capable Ethereum RPC whenever `observation_block` is in the past.

## Secrets And Environment Variables

GitHub Actions uses these secrets:

- `NEON_DATABASE_URL`
- `Ethereum_archive_RPC`

In the workflow, `Ethereum_archive_RPC` is mapped to the runtime environment variable `ETHEREUM_ARCHIVE_RPC`.

In code:

- `scripts/main.py` first reads `ETHEREUM_ARCHIVE_RPC`
- if it is missing, it falls back to `https://ethereum-rpc.publicnode.com`

The fallback is useful for quick local experiments, but it is not reliable for historical backfills because it may not support archive state.

## Pipeline Behavior

### Discovery

The script scans identity contract `Transfer` logs from `START_BLOCK` up to `OBSERVATION_BLOCK`.

It keeps only mint events:

- `from == 0x0000000000000000000000000000000000000000`

Then it filters to the configured agent ID range and keeps the first `TARGET_AGENT_COUNT` agents in ascending `agent_id` order.

Some RPC providers are stricter about `eth_getLogs` requests than others. The script now:

- filters discovery logs to the `Transfer` topic immediately
- automatically retries with a smaller block window if a provider rejects a larger log query

### Identity

For each agent in the batch, the script reads:

- current owner at `observation_block`
- token URI at `observation_block`
- mint block / mint tx information

`agents_core` is updated with `ON CONFLICT DO UPDATE`, so existing rows are refreshed. This is important because:

- previous runs may have used different `observation_block` values
- `owner_wallet` can change after transfers

### Metadata

Metadata uses the `tokenURI` read at `observation_block`.

Important nuance:

- the token URI itself is read at the fixed block
- the content behind that URI may still be a newer version if the URI points to mutable HTTPS content

This is expected and documented in code comments.

The script refreshes:

- `agent_metadata`
- `agent_services`
- `crosschain_registrations`

### Reputation

Reputation reads are also executed at `observation_block`.

The script refreshes:

- `agent_reputation_summary`
- `agent_feedback_records`

## Transfer History (Exploratory Auxiliary Table)

`transfer_history` is treated as an exploratory auxiliary table under a sampled observation window.

Current sampled window in `scripts/main.py`:

- `TRANSFER_HISTORY_START_BLOCK = 24339925`
- `TRANSFER_HISTORY_END_BLOCK = 24439925`

This transfer-history run is intentionally scoped to the first 100k blocks of the larger backfill range.

## Batch Strategy

The pipeline processes agents in batches of `PIPELINE_BATCH_SIZE`.

For each batch, it runs:

1. `identity`
2. `metadata`
3. `reputation`

Each stage prints:

- `success`
- `failed`
- `skipped`

Single-agent failures do not stop the whole batch. Single-agent failures do not stop the whole batch. Failed agents are collected and can be rerun using the rerun mode described below.

## GitHub Actions Usage

Workflow file:

- [.github/workflows/backfill.yml](C:\Users\yulin\OneDrive\Documents\GitHub\ERC8004\.github\workflows\backfill.yml:1)

How to run:

1. Open the GitHub Actions tab.
2. Select `ERC8004 Manual Run`.
3. Click `Run workflow`.

There are no runtime form inputs. Change parameters directly in `scripts/main.py` before running.

## Recommended Rollout Plan

Current test plan:

1. Run `agent_id 0-999`
2. If successful, run `agent_id 1000-2999`
3. If successful, run `agent_id 3000-10000`

To move to the next range, update these values in `scripts/main.py`:

- `TARGET_AGENT_ID_MIN`
- `TARGET_AGENT_ID_MAX`
- `TARGET_AGENT_COUNT`

After each run, always check FINAL_FAILED_ALL and rerun failed agents before proceeding to the next range.

## Notes

- Existing rows are intentionally refreshed, not preserved.
- The pipeline reads chain state at a configured `observation_block`, but table schemas may not persist that field directly.
- If historical calls fail again, the first thing to check is whether the archive RPC secret is valid and whether that provider really supports archive `eth_call` on Ethereum mainnet.

## Failure Handling & Rerun Strategy

Because the pipeline depends on external RPC providers and off-chain metadata endpoints, partial failures are expected in large runs (e.g. HTTP 429, invalid JSON, unreachable URLs).

The pipeline is designed to:

* continue processing even if individual agents fail
* retry transient RPC failures automatically
* record **final failed agent IDs per stage**

### Final Failure Output

At the end of each run, the script prints:

```python
FINAL_FAILED_IDENTITY = [...]
FINAL_FAILED_METADATA = [...]
FINAL_FAILED_REPUTATION = [...]
FINAL_FAILED_ALL = [...]
RERUN_AGENT_IDS = [...]
```

It also writes a file:

```
failed_agents_last_run.json
```

This file contains:

```json
{
  "identity": [...],
  "metadata": [...],
  "reputation": [...],
  "all_failed": [...]
}
```

### Important Rule

Only **final failures** are included.

* Agents that failed initially but succeeded during second-pass retry are **NOT included**
* Only agents that remain failed after all retries need to be rerun

---

### Manual Rerun Mode

The pipeline supports rerunning only specific agents.

In `scripts/main.py`:

```python
RERUN_AGENT_IDS = [2434, 2365, 2433]
RERUN_ONLY = True
```

Optional stage control:

```python
RUN_IDENTITY = False
RUN_METADATA = True
RUN_REPUTATION = False
```

This allows targeted reruns such as:

* only rerun metadata for failed agents
* only rerun reputation for specific IDs

---

### Typical Workflow

#### Step 1 — Run full pipeline

Run the pipeline normally:

```python
RERUN_ONLY = False
```

---

#### Step 2 — Collect failed agents

After the run:

* copy `RERUN_AGENT_IDS` from logs
* or read `failed_agents_last_run.json`

---

#### Step 3 — Rerun only failed agents

Paste into config:

```python
RERUN_AGENT_IDS = [...]
RERUN_ONLY = True
```

Optionally restrict stages:

```python
RUN_METADATA = True
RUN_REPUTATION = False
```

---

#### Step 4 — Repeat if necessary

Continue rerunning until:

```python
FINAL_FAILED_ALL = []
```

---

### Common Failure Types

#### 1. RPC Rate Limiting (HTTP 429)

* caused by high concurrency or public RPC endpoints
* usually resolved by retry or rerun

#### 2. Metadata JSON Errors

* empty response
* invalid JSON
* incorrect content-type

These agents should be rerun, but some may be permanently invalid.

#### 3. Data URI Metadata

Some agents use:

```
data:application/json;base64,...
```

These are now supported and should not fail after fixes.

#### 4. Missing Identity Records (rerun mode)

If rerunning metadata/reputation without identity:

* the script loads identity records from database
* if missing, the agent will be marked as failed

---

### Best Practices

* Always run with an archive RPC for historical consistency
* Expect a small number of failures per batch
* Use rerun mode instead of rerunning full ranges
* Treat metadata failures as partially unreliable data sources

---
