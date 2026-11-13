-- TODO(week08): write stg_gbfs_stations. Delete this TODO line when the file is done.
--
-- One row per GBFS station, unpacked from {{ source('gbfs', 'station_information') }}
-- (one JSON object with a `stations` list). Columns: see _stream.yml.
-- Explore the file first: the station_id in station_status messages is NOT
-- the station ID in the trip files. Find the field that links them.

select 1 as todo
