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

Single-agent failures do not stop the whole batch.

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

## Notes

- Existing rows are intentionally refreshed, not preserved.
- `agents_core.observation_block` is the canonical block-height field name used by the pipeline.
- If historical calls fail again, the first thing to check is whether the archive RPC secret is valid and whether that provider really supports archive `eth_call` on Ethereum mainnet.
