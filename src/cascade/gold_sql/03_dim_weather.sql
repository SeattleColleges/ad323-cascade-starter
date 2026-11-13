-- TODO(week04): write dim_weather. Delete this TODO line when the file is done.
--
-- dim_weather: one row per date and hour (Chicago local time).
-- Grain: one hour. Key: weather_key, an integer like 2026090717 (date + hour).
--
-- Read from the view silver_weather.
--
-- Columns, in this order:
--   weather_key       BIGINT     yyyymmddhh of observed_at
--   date_key          INTEGER    yyyymmdd
--   hour              BIGINT     0-23
--   observed_at       TIMESTAMP
--   temp_c, precipitation_mm, wind_kmh   DOUBLE
--   weather_code      INTEGER    WMO code from Open-Meteo
--   weather_category  VARCHAR    from weather_code:
--       0-1 clear | 2-3 cloudy | 45, 48 fog | 51-57 drizzle
--       61-67 and 80-82 rain | 71-77 and 85-86 snow | 95+ thunderstorm | else unknown
--
-- Plus one "unknown weather" row (weather_key = -1, weather_category = 'unknown')
-- for trips with no matching hour, for example trips in the September file
-- that started on Aug 31. A fact row should never have a NULL key.

CREATE OR REPLACE TABLE gold_sql.dim_weather AS
SELECT 'TODO' AS weather_key;
