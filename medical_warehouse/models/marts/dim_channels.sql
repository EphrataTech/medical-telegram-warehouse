{{
    config(
        materialized = 'table',
        schema       = 'marts'
    )
}}

/*
  dim_channels
  ────────────
  One row per Telegram channel with descriptive attributes and
  pre-aggregated statistics derived from the staging layer.

  channel_key is a surrogate integer key used as the FK in fct_messages.
  channel_type is inferred from the channel name.
*/

with base as (

    select
        channel,
        min(posted_at)                          as first_post_date,
        max(posted_at)                          as last_post_date,
        count(*)                                as total_posts,
        round(avg(view_count)::numeric, 2)      as avg_views,
        round(avg(forward_count)::numeric, 2)   as avg_forwards,
        sum(case when has_image then 1 else 0 end) as total_images,
        round(
            100.0 * sum(case when has_image then 1 else 0 end)
            / nullif(count(*), 0),
            2
        )                                       as pct_messages_with_image

    from {{ ref('stg_telegram_messages') }}
    group by channel

),

with_type as (

    select
        -- Surrogate key: stable hash-based integer derived from channel name
        abs(hashtext(channel))                  as channel_key,
        channel                                 as channel_name,

        -- Classify channel by name heuristics
        case
            when channel ilike '%pharma%'
              or channel ilike '%med%'
              or channel ilike '%drug%'
              or channel ilike '%tikvah%'
              or channel ilike '%chemd%'        then 'Pharmaceutical'
            when channel ilike '%cosmetic%'
              or channel ilike '%lobelia%'
              or channel ilike '%beauty%'       then 'Cosmetics'
            else                                     'Medical'
        end                                     as channel_type,

        first_post_date,
        last_post_date,
        total_posts,
        avg_views,
        avg_forwards,
        total_images,
        pct_messages_with_image

    from base

)

select * from with_type
