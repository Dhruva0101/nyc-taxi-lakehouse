# NYC taxi lakehouse: bronze → silver → gold with PySpark

[![CI](https://github.com/Dhruva0101/nyc-taxi-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/Dhruva0101/nyc-taxi-lakehouse/actions/workflows/ci.yml)
![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Tech:** Python · PySpark · Delta Lake · dbt · DuckDB · Apache Airflow · pytest · GitHub Actions

> **Status:** the PySpark pipeline runs end to end on real NYC taxi data and writes Delta Lake tables. Cloud deployment (S3, Databricks) is a next step.

A medallion-architecture batch pipeline over public NYC TLC yellow-taxi trip records.

```
NYC TLC parquet ──▶ BRONZE (raw, + ingest metadata)
                       │  quality: schema check, row count
                       ▼
                    SILVER (typed, cleaned, deduplicated, partitioned by trip_date)
                       │  quality: nulls, duplicates, value ranges
                       ▼
                    GOLD   (daily metrics by pickup zone: trips, revenue, tip %, distance)
                       │
                       ▼
                    dbt + DuckDB star schema (reads the silver Delta table)
                    fct_trips ── dim_zone, dim_date  →  borough_daily_revenue
```

## What works today

- **Bronze:** ingests raw parquet files unchanged, adds `_ingested_at` and `_source_file`.
- **Silver:** renames and casts columns, drops invalid rows (non-positive fares, impossible distances, drop-off before pick-up), removes exact duplicate trips, adds `trip_date`, writes partitioned by `trip_date`.
- **Gold:** daily metrics per pickup zone, ready for a star schema.
- **Data quality checks** after each layer (expected columns, required non-null fields, no duplicate keys, value ranges). The pipeline fails loudly if a check fails.
- **Row counts per stage** are logged so you can see how much each step filters.
- **Delta Lake** tables by default (ACID writes, schema enforcement, time travel); `--format parquet` also works.
- `--month YYYY-MM` keeps only trips picked up in that month (the raw files contain a few rows with bad device clocks, e.g. 2002 and 2009).
- **dbt star schema** (`dbt/`): `fct_trips` (one row per trip) with `dim_zone` (from the official TLC zone lookup, loaded as a seed) and `dim_date`, plus a `borough_daily_revenue` report. 13 dbt tests: `unique`, `not_null`, `relationships` and `accepted_values`. DuckDB reads the Delta table directly, so no warehouse account is needed.
- **Airflow DAG** with retries and failure alerts.
- **pytest** tests on a small synthetic dataset, run in GitHub Actions.

## Run it locally

Requires Python 3.10+ and Java 11 or 17 (for Spark).

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pytest -q                                          # tests (synthetic data)

# One real month of data (~50 MB) from the NYC TLC website:
mkdir -p data/landing
curl -o data/landing/yellow_tripdata_2024-01.parquet \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet

python -m lakehouse.run_pipeline --landing data/landing --lake data/lake --month 2024-01
```

Then build the star schema:

```bash
cd dbt
dbt build --profiles-dir .      # seeds, models and tests
dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .   # lineage graph
```

The first Delta run downloads the Delta Lake jars, so it needs internet.

## Results (January 2024, run locally)

| Stage | Rows | Time |
|---|---|---|
| Bronze (raw) | 2,964,624 | 12.0 s |
| Silver (clean) | 2,859,135 (96.4% kept) | 17.7 s |
| Gold (day × pickup zone) | 6,803 | 2.6 s |

dbt on the silver table: all 19 models, seeds and tests pass. Trips and revenue by pickup borough:

| Borough | Trips | Revenue |
|---|---|---|
| Manhattan | 2,566,006 | $58.4M |
| Queens (incl. JFK/LaGuardia) | 255,338 | $18.5M |
| Brooklyn | 22,051 | $0.7M |
| Bronx | 5,719 | $0.2M |

Silver dropped 105,489 rows: non-positive fares, zero or impossible distances, trips under 1 minute or over 5 hours, duplicates, and 17 trips dated outside January.

## Roadmap

- [x] **Step 0:** PySpark bronze/silver/gold, quality checks, row counts, Airflow DAG, tests, CI
- [x] **Step 1:** switch storage to **Delta Lake** and run on a real month of data
- [ ] **Step 1b:** land raw files in **AWS S3**; run on **Databricks**
- [x] **Step 2:** **dbt** star schema (`fct_trips`, `dim_zone`, `dim_date`) with dbt tests
- [ ] **Step 3:** run the DAG in Airflow (Docker), with email/Slack alerts on failure

## Design notes / interview prep

- **Why medallion layers?** Bronze keeps the raw truth so you can always reprocess; silver is the trusted, cleaned version; gold is shaped for one business use. A bug in silver logic means rerunning from bronze, not re-downloading.
- **Partitioning by `trip_date`:** most queries filter by date, so Spark reads only the partitions it needs (partition pruning).
- **Why Delta Lake?** Parquet alone has no transactions. Delta adds ACID writes, `MERGE` for upserts, schema enforcement, and time travel.
- **Idempotency:** each stage overwrites its output for the run, so reruns don't double-count.

## License

[MIT](LICENSE)
