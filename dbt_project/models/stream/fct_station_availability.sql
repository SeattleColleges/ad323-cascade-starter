-- TODO(week08): write fct_station_availability. Delete this TODO line when the file is done.
--
-- One row per GBFS station per 5-minute window, from your consumer's files
-- ({{ source('stream', 'station_availability') }}), joined to dim_station
-- through stg_gbfs_stations. Columns: see _stream.yml. Stations that never
-- appear in the trip data stay in (station_key NULL, in_trip_data false).

{{ config(schema='gold') }}

select 1 as todo
