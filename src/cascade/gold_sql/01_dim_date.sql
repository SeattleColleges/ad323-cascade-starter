-- dim_date: one row per calendar day, from the first trip start to the last trip end.
-- Grain: one day. Key: date_key, an integer like 20260907 (readable, sorts correctly).

CREATE OR REPLACE TABLE gold_sql.dim_date AS
WITH
-- Holidays are typed in by hand for now. In Week 5 they move to a dbt seed
-- (a small CSV), which is the better home for reference data like this.
holidays (holiday_date, holiday_name) AS (
    VALUES
        (DATE '2026-09-07', 'Labor Day'),
        (DATE '2026-10-12', 'Columbus Day / Indigenous Peoples'' Day')
),
-- Every day from the first start to the last end of a VALID trip (a trip can
-- end the next day). Invalid trips are left out: one bad end time could
-- otherwise stretch the calendar over years.
days AS (
    SELECT CAST(day AS DATE) AS full_date
    FROM generate_series(
        (SELECT min(started_at)::DATE FROM silver_trips WHERE is_valid),
        (SELECT max(ended_at)::DATE   FROM silver_trips WHERE is_valid),
        INTERVAL 1 DAY
    ) AS t(day)
)
SELECT
    CAST(strftime(d.full_date, '%Y%m%d') AS INTEGER) AS date_key,
    d.full_date,
    year(d.full_date)          AS year,
    month(d.full_date)         AS month,
    monthname(d.full_date)     AS month_name,
    day(d.full_date)           AS day_of_month,
    isodow(d.full_date)        AS day_of_week,   -- 1 = Monday ... 7 = Sunday
    dayname(d.full_date)       AS day_name,
    isodow(d.full_date) IN (6, 7) AS is_weekend,
    h.holiday_date IS NOT NULL AS is_holiday,
    h.holiday_name
FROM days AS d
LEFT JOIN holidays AS h ON h.holiday_date = d.full_date
ORDER BY d.full_date;
