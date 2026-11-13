-- TODO(week09): write dim_rider. Delete this TODO line when the file is done.
--
-- One row per rider, with every direct identifier removed or masked. This is
-- what analysts and the dashboard may see. Columns (the Week 9 tests check them):
--   rider_id      as in stg_riders
--   email_hash    sha256(salt || email), salt = env_var('CASCADE_PII_SALT', '')
--                 (keep the '' default: dbt reads this file even when Week 9 is skipped)
--   name_masked   first initial + ***, e.g. "Maria Lopez" -> "M*** L***"
--   age_band      under 18 | 18-24 | 25-34 | 35-44 | 45-54 | 55-64 | 65+,
--                 age in full years as of var('pii_as_of_date', '2026-10-31')
--   home_zip3     first 3 digits of home_zip
--   member_casual, signup_year
-- macros/redact.sql shows how a masking macro works.

{{ config(tags=['pii']) }}

select 1 as todo
