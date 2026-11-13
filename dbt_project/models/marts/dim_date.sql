-- TODO(week05): port your Week 4 dim_date to dbt. Delete this TODO line when the file is done.
--
-- Same columns as the Week 4 version (see src/cascade/gold_sql/). Read from
-- {{ ref('stg_trips') }} / {{ ref('stg_weather') }} instead of the silver views.
-- One row per day from the first start to the last end of a valid trip.
-- Holidays: {{ ref('holidays') }} (seeds/holidays.csv).

select 1 as todo
