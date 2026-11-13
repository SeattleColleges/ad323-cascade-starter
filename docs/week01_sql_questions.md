# Week 1: SQL questions on the sample trips

These questions use the frozen sample in `data/sample/` (50,000 trips each for
September and October 2026) and DuckDB.

## Setup

DuckDB runs inside Python, so there is nothing extra to install. Start a Python
prompt from the repo root:

```bash
uv run python
```

Then create a view over both sample files:

```python
import duckdb
con = duckdb.connect()
con.sql("""
    CREATE VIEW trips AS
    SELECT * FROM read_csv('data/sample/*/*-divvy-tripdata.csv',
                           types = {'ride_id': 'VARCHAR'})
""")
con.sql("SELECT * FROM trips LIMIT 5").show()
```

Run each answer with `con.sql("""...""").show()`.

> **Why `types = {'ride_id': 'VARCHAR'}`?** DuckDB guesses column types by
> looking at the first rows. In September `ride_id` looks like
> `B3C076B2D6BAC911` (hex text). In October Divvy switched to IDs like
> `438793225098011695`, which DuckDB guesses are integers. An ID is a label,
> not a number: never do math on it, and declare it as text. This is a good
> discussion question for class.

> **Naming tip.** Don't name a result column `trips` when the view is also
> called `trips`. In `ORDER BY trips`, DuckDB may read `trips` as the whole
> table row rather than your count. Use a name like `trip_count`.

## Q1. How many trips are in each month, by bike type?

**Skills:** `GROUP BY`, `count(*)`, date formatting.

**Your SQL and answer:**

```sql

```

## Q2. How do members and casual riders differ in trip length?

**Skills:** timestamp arithmetic, `avg` vs `median`, rounding.

**Your SQL and answer:**

```sql

```

## Q3. What are the 5 most popular start stations?

**Skills:** filtering `NULL`, `ORDER BY` with a tie-breaker, `LIMIT`.

**Your SQL and answer:**

```sql

```

## Q4. What is the busiest start hour on weekdays vs. weekends?

**Skills:** `CASE`, date parts, CTEs, window functions with `QUALIFY`.

**Your SQL and answer:**

```sql

```

## Q5. How many trips are missing a start station, by bike type?

**Skills:** conditional aggregation with `FILTER`, percentages.

**Your SQL and answer:**

```sql

```
