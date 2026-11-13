-- TODO(week05): write agg_station_daily. Delete this TODO line when the file is done.
--
-- One row per station per day, from fct_trips:
--   station_key    VARCHAR
--   date_key       INTEGER
--   trips_started  BIGINT   trips that STARTED at the station that day
--   trips_ended    BIGINT   trips that ENDED there that day (by end date)
--   net_flow       BIGINT   trips_ended - trips_started
-- A station with only starts (or only ends) on a day still gets a row.

select 1 as todo
