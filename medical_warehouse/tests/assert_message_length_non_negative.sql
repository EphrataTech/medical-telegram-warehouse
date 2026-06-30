-- assert_message_length_non_negative.sql
-- ──────────────────────────────────────────────────────────────────────────────
-- Business rule: message_length must be >= 0 and must equal the actual
-- character length of message_text to ensure the derived field is consistent.
--
-- This query must return 0 rows to pass.
-- ──────────────────────────────────────────────────────────────────────────────

select
    message_id,
    channel,
    message_length,
    length(message_text) as actual_length,
    message_text
from {{ ref('fct_messages') }}
where
    message_length < 0
    or message_length != length(message_text)
