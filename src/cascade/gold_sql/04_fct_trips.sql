-- TODO(week04): write fct_trips. Delete this TODO line when the file is done.
--
-- fct_trips: one row per VALID trip (is_valid = true). Grain: one trip. Key: ride_id.
-- Trips with a missing station ARE valid: they point at the unknown station.
--
-- Read from silver_trips; look up keys in the dimensions you built above.
--
-- Columns, in this order:
--   ride_id            VARCHAR
--   start_station_key  VARCHAR    md5 of the start station ID, or md5('UNKNOWN')
--   end_station_key    VARCHAR    md5 of the end station ID, or md5('UNKNOWN')
--   date_key           INTEGER    yyyymmdd of started_at (dim_date)
--   start_hour         BIGINT     hour of started_at
--   weather_key        BIGINT     the hour the trip started, or -1 if there is no weather for it
--   started_at, ended_at   TIMESTAMP
--   duration_min       DOUBLE
--   distance_km        DOUBLE     straight-line distance, start to end, 3 decimals
--                                 (haversine formula, Earth radius 6371 km)
--   rideable_type, member_casual  VARCHAR   (plain columns, no tiny dimensions)
--   year_month         VARCHAR    which batch the trip came from
--
-- gold.py checks afterwards that fct_trips has exactly the valid trips from
-- the DQ reports and that every key has a dimension row.

CREATE OR REPLACE TABLE gold_sql.fct_trips AS
SELECT 'TODO' AS ride_id;
