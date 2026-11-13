# WEEK 07: Spark vs. DuckDB on a full year of trips

Copy this file to `docs/WEEK_07.md` and fill in every `___`.

## 1. The data

| | Value |
|---|---|
| Months | ___ (from ___ to ___) |
| Silver rows (all trips) | ___ |
| Valid trips | ___ |
| Size of silver on disk (Parquet) | ___ MB |
| Time to download the year (`get_full_year.py`) | ___ min |

Hint: `du -sh data/runs/full-year/silver` (macOS/Linux/WSL).

## 2. My machine

| | Value |
|---|---|
| Computer / operating system | ___ |
| RAM | ___ GB |
| CPU cores | ___ |
| Docker memory limit for the Spark container | 3 GB (from `docker/spark.yml`) |

## 3. Timings (seconds)

Run each engine once on the same data. Spark numbers come from the last cell of
`spark/week07_spark_lab.ipynb`; DuckDB numbers from `uv run python spark/week07_duckdb.py`.

| Step | Spark | DuckDB | Faster engine |
|---|---:|---:|---|
| Count all rows | ___ | ___ | ___ |
| Q1: trips per month by member type | ___ | ___ | ___ |
| Q2: top 10 start stations | ___ | ___ | ___ |
| Q3: avg minutes by hour and bike type | ___ | ___ | ___ |
| Write station_daily (partitioned) | ___ | ___ | ___ |

Do the answers match? Q2's top station is ___ with ___ trips in both engines: yes / no.

## 4. Explain it (3–5 sentences each)

1. **Which engine was faster here, and why?** Think about where each one runs,
   how much setup it needs, and how big this data really is.
2. **What is a shuffle?** Use Q2 and the `Exchange` line from `q2.explain()`.
3. **Partitioning:** what is the difference between Spark's in-memory partitions
   and the `year_month=...` folders on disk? When does each one help?
4. **At what point would Cascade Mobility actually need Spark?** Give a rough
   data size or situation, and say what would change.

## 5. Limitations of this comparison

List at least two reasons these timings are not a perfectly fair contest
(for example: one engine runs in a container and one doesn't, warm-up, caching,
different memory limits).
