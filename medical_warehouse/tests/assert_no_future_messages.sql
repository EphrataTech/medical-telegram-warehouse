-- assert_no_future_messages.sql
-- ──────────────────────────────────────────────────────────────────────────────
-- Business rule: No message can have a posted_at timestamp in the future.
-- A future timestamp indicates a data quality error in the source or scraper.
--
-- This query must return 0 rows to pass.
-- ──────────────────────────────────────────────────────────────────────────────

select
    message_id,
    channel,
    posted_at,
    now() as current_ts,
    posted_at - now() as how_far_in_future
from {{ ref('fct_messages') }}
where posted_at > now()
