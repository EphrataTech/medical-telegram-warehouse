{{
    config(
        materialized = 'table',
        schema       = 'marts'
    )
}}

/*
  fct_messages
  ────────────
  Central fact table of the star schema.
  Grain: one row per Telegram message.

  Foreign keys
  ────────────
  channel_key  → dim_channels.channel_key
  date_key     → dim_dates.date_key
*/

with messages as (

    select * from {{ ref('stg_telegram_messages') }}

),

channels as (

    select channel_name, channel_key
    from {{ ref('dim_channels') }}

),

dates as (

    select date_key, full_date
    from {{ ref('dim_dates') }}

),

final as (

    select
        -- ── Grain identifier ─────────────────────────────────────────────────
        m.message_id,

        -- ── Foreign keys ─────────────────────────────────────────────────────
        c.channel_key,
        d.date_key,

        -- ── Descriptive attributes ───────────────────────────────────────────
        m.channel,
        m.message_text,
        m.message_length,
        m.post_author,
        m.reply_to_msg_id,
        m.grouped_id,

        -- ── Measures (facts) ─────────────────────────────────────────────────
        m.view_count,
        m.forward_count,

        -- ── Flags ────────────────────────────────────────────────────────────
        m.has_image,
        m.is_pinned,
        m.is_mentioned,

        -- ── Media ────────────────────────────────────────────────────────────
        m.media_type,
        m.photo_id,

        -- ── Timestamps ───────────────────────────────────────────────────────
        m.posted_at,
        m.edit_date,
        m.scraped_at

    from messages m

    left join channels c
        on m.channel = c.channel_name

    left join dates d
        on m.posted_at::date = d.full_date

)

select * from final
