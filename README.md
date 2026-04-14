# YouTube Topic Radar (MVP)

A local, metadata-only pipeline for monitoring selected YouTube channels and surfacing topic candidates for editorial review.

## What this project does

- Monitors a fixed channel list from config (`configs/channels.yaml`).
- Fetches **video metadata only** via YouTube Data API.
- Stores data in a local DuckDB database.
- Computes deterministic feature scores.
- Builds simple deterministic topic clusters.
- Exports practical reports (console + CSV, optional Markdown).

## What this project does **not** do

- Does **not** download video/audio.
- Does **not** re-upload source content.
- Does **not** use LLMs.
- Does **not** include a web UI.
- Does **not** deploy to cloud infra.

---

## Setup

### 1) Requirements

- Python 3.11 recommended
- YouTube Data API key

Install dependencies:

```bash
pip install -r requirements.txt
```

### 2) Configure channels

Edit `configs/channels.yaml`:

- Set each `youtube_channel_id`
- Set `active: true` for channels you want to monitor

### 3) (Optional) tune scoring/cluster configs

- `configs/features.yaml`
- `configs/clustering.yaml`

---

## Environment variables

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `YOUTUBE_API_KEY` | Yes (for video collection) | - | YouTube Data API key |
| `YTRADAR_DB_PATH` | No | `data/radar.duckdb` | Local DuckDB path |
| `YTRADAR_CHANNELS_CONFIG` | No | `configs/channels.yaml` | Channel config file |
| `YTRADAR_FEATURES_CONFIG` | No | `configs/features.yaml` | Feature/scoring config |
| `YTRADAR_CLUSTERING_CONFIG` | No | `configs/clustering.yaml` | Clustering config |

---

## CLI usage

Unified entrypoint:

```bash
PYTHONPATH=src python scripts/run_cli.py --help
```

### Commands

Initialize DB schema:

```bash
PYTHONPATH=src python scripts/run_cli.py init-db
```

Sync configured channels into DB:

```bash
PYTHONPATH=src python scripts/run_cli.py sync-channels
```

Collect recent video metadata:

```bash
PYTHONPATH=src python scripts/run_cli.py collect-videos --days 7
```

Compute deterministic features:

```bash
PYTHONPATH=src python scripts/run_cli.py compute-features
```

Build deterministic topic candidates:

```bash
PYTHONPATH=src python scripts/run_cli.py build-topics
```

Export report (console + CSV, optional Markdown):

```bash
PYTHONPATH=src python scripts/run_cli.py export-report \
  --top-n 20 \
  --csv-path data/exports/topic_candidates.csv \
  --md-path data/exports/topic_report.md
```

Run end-to-end pipeline:

```bash
PYTHONPATH=src python scripts/run_cli.py run-all --days 7 --top-n 20
```

---

## Expected outputs

Database:

- `data/radar.duckdb` (or `YTRADAR_DB_PATH`)

Exports:

- CSV (default): `data/exports/topic_candidates.csv`
- Markdown (if requested): custom `--md-path`
- Ranked console output with separate shortform/longform sections

---

## Project structure (high level)

```text
configs/                  # editable YAML configs
scripts/                  # thin command entry scripts
src/ytradar/
  cli.py                  # unified CLI
  collectors/             # YouTube metadata collection
  features/               # deterministic feature engineering
  clustering/             # deterministic topic clustering
  reporting/              # console/CSV/Markdown output
  db/                     # schema init + repository
  config/                 # config loaders
```

---

## Important limitations

- Clustering is heuristic (token/keyword overlap), not semantic.
- No transcript or speech analysis (metadata-only).
- Quality depends on channel config and keyword tuning.
- Current workflow is batch-oriented, not real-time.
- Existing DB schema init is create-if-not-exists; no migration system yet.

---

## Future improvement ideas

- Add DB migrations (schema evolution safety).
- Add unit/integration tests for pipeline stages.
- Improve clustering with better lexical normalization and dedup rules.
- Add richer risk/rule filters and editorial status workflows.
- Add scheduling helpers (cron templates) and run summaries.
- Add lightweight dashboard (optional, still local-first).
