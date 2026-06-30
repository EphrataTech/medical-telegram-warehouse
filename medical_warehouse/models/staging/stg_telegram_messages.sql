{{
    config(
        materialized = 'view',
        schema       = 'staging'
    )
}}

/*
  stg_telegram_messages
  ─────────────────────
  Cleans and standardises raw.telegram_messages:
    • Casts every column to its correct type
    • Drops rows with no message_id or channel
    • Filters out rows with future timestamps (data quality guard)
    • Adds calculated fields: message_length, has_image
    • Normalises channel name to lower-case
*/

with source as (

    select * from {{ source('raw', 'telegram_messages') }}

),

cleaned as (

    select
        -- ── Identifiers ──────────────────────────────────────────────────────
        cast(message_id      as bigint)          as message_id,
        lower(trim(channel))                     as channel,

        -- ── Timestamps ───────────────────────────────────────────────────────
        cast(date            as timestamptz)     as posted_at,
        cast(scraped_at      as timestamptz)     as scraped_at,
        cast(edit_date       as timestamptz)     as edit_date,

        -- ── Message content ──────────────────────────────────────────────────
        coalesce(trim(text), '')                 as message_text,
        length(coalesce(trim(text), ''))         as message_length,

        -- ── Engagement metrics ───────────────────────────────────────────────
        coalesce(cast(views    as integer), 0)   as view_count,
        coalesce(cast(forwards as integer), 0)   as forward_count,

        -- ── Media ────────────────────────────────────────────────────────────
        coalesce(cast(has_photo as boolean), false) as has_image,
        cast(photo_id          as bigint)           as photo_id,
        trim(media_type)                            as media_type,

        -- ── Thread / authorship ──────────────────────────────────────────────
        cast(reply_to_msg_id as bigint)          as reply_to_msg_id,
        trim(post_author)                        as post_author,
        cast(grouped_id      as bigint)          as grouped_id,

        -- ── Flags ────────────────────────────────────────────────────────────
        coalesce(cast(out       as boolean), false) as is_outgoing,
        coalesce(cast(mentioned as boolean), false) as is_mentioned,
        coalesce(cast(pinned    as boolean), false) as is_pinned

    from source

    where
        -- Drop rows missing primary identifiers
        message_id is not null
        and channel   is not null
        and trim(channel) != ''
        -- Drop rows with clearly invalid timestamps (future dates are data errors)
        and cast(date as timestamptz) <= now()
        -- Drop rows where the date is impossibly old (Telegram launched 2013)
        and cast(date as timestamptz) >= '2013-01-01'::timestamptz

)

select * from cleaned
