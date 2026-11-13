-- TODO(week04): write dim_station. Delete this TODO line when the file is done.
--
-- dim_station: one row per station ID, built from the trips themselves.
-- Grain: one station ID. Key: station_key = md5(station_id).
--
-- Why md5 and not 1, 2, 3...? A numbered key would change whenever a new
-- station appears and the numbering shifts. A hash of the ID gives the same
-- key on every run, which the incremental fact table in Week 5 depends on.
--
-- Read from the view silver_trips (all of silver; gold.py creates it).
-- Use both ends of each trip: a station can appear only as an end station.
--
-- Columns, in this order:
--   station_key   VARCHAR    md5(station_id)
--   station_id    VARCHAR
--   station_name  VARCHAR    one name per station: decide which, and say why in WEEK_04.md
--   name_count    BIGINT     how many different names the ID appeared with
--   lat, lng      DOUBLE     one location per station (trip coordinates vary; pick a robust summary)
--   first_seen    TIMESTAMP  earliest trip time at the station
--   last_seen     TIMESTAMP  latest trip time at the station
--   is_unknown    BOOLEAN    false for real stations
--
-- Plus ONE extra row for dockless e-bike trips that have no station:
--   station_key = md5('UNKNOWN'), station_id = 'UNKNOWN',
--   station_name = 'Unknown station (dockless)', is_unknown = true
-- so every trip has a station key and totals still reconcile.

CREATE OR REPLACE TABLE gold_sql.dim_station AS
SELECT 'TODO' AS station_key;
