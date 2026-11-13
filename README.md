# Cascade Mobility pipeline (AD 323 Data Engineering)

Over 11 weeks you build a data pipeline for **Cascade Mobility**, a bike-share
company, using real Chicago Divvy trips and Open-Meteo weather:

```text
Divvy CSVs + weather JSON ──ingest──▶ bronze ──clean──▶ silver ──SQL / dbt──▶ gold ──▶ dashboard
                                      (raw)           (Parquet)              (star schema)
                    Prefect runs the spine · CI checks it · Spark and streaming labs hang off it
```

This repository is a **template**. Everything is here from Week 1: the
infrastructure is finished, and every piece of pipeline logic is a stub marked
`TODO(weekNN)` for you to write. Unfinished weeks never break anything: their
tests are **skipped**, not failed, until you start them.

## Setup

1. Click **Use this template → Create a new repository**. Make your copy
   **public** (see [Why public?](#why-your-repo-should-be-public)).
2. Install [uv](https://docs.astral.sh/uv/) and Git. Windows: do everything
   inside **WSL2 (Ubuntu)**, in your Linux home folder (`cd ~`), not under `/mnt/c`.
3. Clone your copy and set it up:

```bash
git clone https://github.com/<you>/<your-repo>.git
cd <your-repo>
uv sync                  # installs Python 3.12 and every pinned package
uv run pytest            # Week 1 passes; later weeks show as "skipped"
```

Everything graded runs on the frozen sample in `data/sample/` (50,000 trips
for each of 2026-09 and 2026-10), so no test needs the internet.

## How the TODOs, tests and CI work

- **TODO markers.** Python stubs raise `NotImplementedError("TODO(week03): ...")`.
  SQL stubs start with a `-- TODO(week04): ...` line. Delete the raise (or the
  marker line) once that piece is done.
- **One week's tests:** `uv run pytest -m week03`. While a TODO is still there,
  that week's tests are skipped and say why.
- **Prove a week is done:** `uv run pytest -m week03 --require-implemented`.
  Now a TODO counts as a failure.
- **CI** (`.github/workflows/ci.yml`) runs on every push: all tests, then
  `uv run python -m cascade.check`, which runs the sample pipeline as far as
  you have written it. Unfinished weeks keep CI green; real errors turn it red.

## Grading evidence: a green run for the week

With each assignment you submit a link to a green CI run for that week:

1. Push your work.
2. On GitHub: **Actions → ci → Run workflow**, pick the week (for example `03`),
   click **Run workflow**.
3. That run (named "Week 03 grading evidence") runs only that week's tests,
   with `--require-implemented`, so a leftover TODO fails it.
4. When it's green, open it and copy its URL into your submission.

## Week by week

| Week | Topic | Files you work in | Tests |
|---|---|---|---|
| 1 | Setup, first SQL | `docs/week01_sql_questions.md` → your answers in `docs/WEEK_01.md` | `week01` |
| 2 | Ingestion to bronze | `src/cascade/ingest.py` | `week02` |
| 3 | Cleaning to silver | `src/cascade/clean.py` | `week03` |
| 4 | Star schema in SQL | `src/cascade/gold_sql/02-04_*.sql`, ERD and answers to `docs/week04_business_questions.md` in `docs/WEEK_04.md` | `week04` |
| 5 | dbt | `dbt_project/models/` (staging + marts), tests in the `.yml` files, `dbt_project/tests/` | `week05` |
| 6 | Prefect | `src/cascade/pipeline.py` | `week06` |
| 7 | Spark vs. DuckDB (Docker) | `spark/week07_spark_lab.ipynb`, `spark/week07_duckdb.py`, `docs/week07_comparison_template.md` | `week07` |
| 8 | Streaming (Docker) | `src/cascade/stream/consumer.py`, `dbt_project/models/stream/` | `week08` |
| 9 | Secrets and PII | `.env`, `dbt_project/models/restricted/`, `dbt_project/models/marts/dim_rider.sql` | `week09` |
| 10 | DataOps | `dbt_project/models/staging/_sources.yml` (freshness), CI, the incident (`docs/week10_incident_report_template.md`) | `week10` |
| 11 | Dashboard | `dashboard/app.py`, your architecture write-up | `week11` |

Each week's write-up goes in `docs/WEEK_NN.md`. Commands for each stage:

```bash
uv run python -m cascade.ingest --batch 2026-09 2026-10 --source data/sample   # Week 2
uv run python -m cascade.clean  --batch 2026-09 2026-10                        # Week 3
uv run python -m cascade.gold                                                  # Week 4
uv run python -m cascade.transform                                             # Week 5 (dbt build + export)
uv run python -m cascade.pipeline backfill --batch 2026-09 2026-10             # Week 6
uv run python -m cascade.transform --freshness                                 # Week 10
uv run streamlit run dashboard/app.py                                          # Week 11
uv run python -m cascade.demo          # once everything works: the whole pipeline + dashboard
```

**Week 6 practice failure:** `CASCADE_FAIL_CLEAN=1` (or `CASCADE_FAIL_DBT=1`)
makes that task fail on purpose, so you can watch a failed run in the Prefect
UI (`uv run prefect server start`, then http://127.0.0.1:4200), unset it, and
rerun the batch.

**Week 9 salt:** copy `.env.example` to `.env` and follow the instructions in
it. `.env` is gitignored; never commit it.

**Week 10 incident:** the broken data for the exercise is released at the
start of Week 10, with its own instructions.

## If you fall behind: checkpoints

No week requires your earlier weeks to be correct. After each due date, the
course publishes a **checkpoint** you can start from. See
[`checkpoints/README.md`](checkpoints/README.md). The tests pick checkpoint
data up automatically. Note in your `WEEK_NN.md` which checkpoint you used;
there is no penalty.

## Prepare for Week 7 (do this in Week 6)

Weeks 7 and 8 run Spark and Redpanda in Docker. The downloads are big, so get
them on a good connection **before** Week 7:

1. Make sure you have **about 15 GB of free disk**: the Spark image is 7 GB,
   Redpanda 0.5 GB, and the full-year data about 0.3 GB, plus room to work.
2. Install **Docker Desktop**.
   - Windows: turn on "Use the WSL 2 based engine" and, under Settings →
     Resources → WSL integration, enable your Ubuntu distribution.
   - Mac: Settings → Resources → Memory: at least 4 GB.
3. Pull both images:

```bash
docker pull quay.io/jupyter/pyspark-notebook:2026-10-26
docker pull docker.redpanda.com/redpandadata/redpanda:v26.2.6
```

4. Check that Docker works: `docker run --rm hello-world`.

Both labs fit on an 8 GB laptop: the Spark container is capped at 3 GB and
Redpanda at 1 GB. Close other big apps during the labs.

**Week 7 commands:**

```bash
uv run python spark/get_full_year.py           # downloads a year of cleaned trips (~0.3 GB)
uv run python spark/week07_duckdb.py
docker compose -f docker/spark.yml up          # open http://127.0.0.1:8888, run the notebook
docker compose -f docker/spark.yml down
```

**Week 8 commands:**

```bash
docker compose -f docker/redpanda.yml up -d
uv run python -m cascade.stream.producer --speedup 40     # replays 2 hours of the live feed
uv run python -m cascade.stream.consumer --exit-when-idle
docker compose -f docker/redpanda.yml down -v
```

## Codespaces (fallback)

If your own computer can't run the course, you can work in GitHub Codespaces:
**Code → Codespaces → Create codespace on main**. Setup runs `uv sync` for you.
Measured memory, disk and core-hours for the Docker labs in Codespaces will be added here.

## Why your repo should be public

Public repositories get GitHub's **secret scanning** and **push protection**
for free. Push protection blocks a push that contains a known kind of secret
(an API key, a token) before it reaches GitHub, and secret scanning alerts you
if one is already there. Week 9 is about keeping secrets out of git; this is
your safety net. Your work is also your portfolio. Never put real personal
data in the repo: the rider data in Week 9 is fake on purpose.

## Credits and licenses

- Trip data: Divvy / Lyft Bikes and Scooters, LLC, under the
  [Divvy Data License Agreement](https://divvybikes.com/data-license-agreement).
- Weather data by [Open-Meteo.com](https://open-meteo.com/) (CC BY 4.0).
- Station feed (Week 8): Divvy's public GBFS feed, recorded 2026-11-12.

Keep these credits in your dashboard (Week 11); both licenses require them.
