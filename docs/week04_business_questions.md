# Week 4: business questions on your star schema


## Setup

```bash
uv run python -m cascade.ingest --batch 2026-09 2026-10 --source data/sample --data-root data/runs/sample
uv run python -m cascade.clean  --batch 2026-09 2026-10 --data-root data/runs/sample
uv run python -m cascade.gold   --data-root data/runs/sample
uv run python
```

```python
import duckdb
con = duckdb.connect("data/runs/sample/warehouse.duckdb", read_only=True)
con.sql("SET schema = 'gold_sql'")
con.sql("""...""").show()
```

`read_only=True` lets you query while another program has the file open for
reading, and it guarantees you can't break gold by accident.

The point of the star schema: every question below is the fact table joined
to one or two dimensions, then grouped. Nobody has to re-derive weekdays,
holidays, or weather categories from raw timestamps.

## Q1. Which days do casual riders use the system most?

*Scenario: marketing wants to know when to run casual-rider promotions.*

**Your SQL and answer:**

```sql

```

## Q2. How much does rain reduce riding?

*Scenario: operations wants to know how many bikes to rebalance on a wet day.*

Counting trips alone would be misleading: there are many more dry hours than
rainy hours. Divide by the number of hours in each category. We only count
daytime hours (7:00–21:59), so quiet night hours don't drag the averages down.

**Your SQL and answer:**

```sql

```

## Q3. Which stations are busiest, and how much riding is dockless?

*Scenario: the planning team wants to know where to add docks.*

**Your SQL and answer:**

```sql

```

## Q4. Do holidays change how people ride?

*Scenario: should the holiday staffing plan look like a weekday or a weekend?*

**Your SQL and answer:**

```sql

```

## Q5. How does temperature affect ridership?

*Scenario: the forecasting team wants a simple rule of thumb.*

**Your SQL and answer:**

```sql

```
