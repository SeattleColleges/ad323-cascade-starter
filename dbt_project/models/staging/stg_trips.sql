-- TODO(week05): write stg_trips. Delete this TODO line when the file is done.
--
-- Staging: one row per silver trip, with the silver columns you need downstream
-- (including is_valid and year_month). No filtering here; the marts decide which
-- trips to keep. Read the source with {{ source('silver', 'trips') }}.
-- stg_weather.sql next to this file is a finished example.

select 1 as todo
