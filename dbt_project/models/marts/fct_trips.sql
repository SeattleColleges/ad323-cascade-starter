-- TODO(week05): write the INCREMENTAL fct_trips. Delete this TODO line when the file is done.
--
-- Same columns as your Week 4 fct_trips, plus one more at the end:
--   loaded_at  TIMESTAMP  when this row was written (current_timestamp)
--
-- Incremental rules (the Week 5 tests check each one):
--   * First run, or --full-refresh: build from every batch in silver.
--   * Later runs load only batches (year_month) NEWER than the newest one
--     already in the table. A run with no new batch changes nothing.
--   * With --vars '{batch_id: 2026-10}' (cascade.transform --batch 2026-10),
--     load exactly that batch again: it is REPLACED, never duplicated, and
--     other batches are untouched.
-- Hints: materialized='incremental', incremental_strategy='delete+insert',
-- unique_key='year_month', is_incremental(), var('batch_id', none), {{ this }}.

select 1 as todo
