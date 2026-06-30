-- assert_positive_views.sql
-- ──────────────────────────────────────────────────────────────────────────────
-- Business rule: view_count and forward_count must never be negative.
-- Telegram does not report negative engagement; a negative value is a
-- sign of a parsing error or bad data in the source.
--
-- This query must return 0 rows to pass.
-- ──────────────────────────────────────────────────────────────────────────────

select
    message_id,
    channel,
    view_count,
    forward_count,
    posted_at
from {{ ref('fct_messages') }}
where
    view_count    < 0
    or forward_count < 0
