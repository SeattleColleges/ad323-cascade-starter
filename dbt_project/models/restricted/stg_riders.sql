-- TODO(week09): write stg_riders. Delete this TODO line when the file is done.
-- RESTRICTED: raw rider accounts with direct identifiers (fake, but treat as real).
--
-- Type the columns of {{ source('restricted', 'riders') }}: dates as DATE,
-- home_zip as text, email lower-cased and trimmed. This view lives in the
-- `restricted` schema and is never exported.

{{ config(schema='restricted', materialized='view') }}

select 1 as todo
