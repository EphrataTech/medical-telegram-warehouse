{{
    config(
        materialized = 'table',
        schema       = 'marts'
    )
}}

/*
  fct_image_detections
  ─────────────────────
  Fact table combining YOLO object detection results with the core
  message fact table.

  Grain: one row per detected object per image.
         (Images with no detections have one row with detected_class = NULL)

  Foreign keys
  ────────────
  channel_key  → dim_channels.channel_key
  date_key     → dim_dates.date_key
  message_id   → fct_messages.message_id  (informational, not enforced in PG)
*/

with detections as (

    select
        id                                          as detection_id,
        lower(trim(channel))                        as channel,
        cast(message_id      as bigint)             as message_id,
        lower(trim(detected_class))                 as detected_class,
        cast(confidence      as numeric(6,4))       as confidence_score,
        lower(trim(image_category))                 as image_category,
        cast(bbox_x1         as numeric(10,2))      as bbox_x1,
        cast(bbox_y1         as numeric(10,2))      as bbox_y1,
        cast(bbox_x2         as numeric(10,2))      as bbox_x2,
        cast(bbox_y2         as numeric(10,2))      as bbox_y2,
        cast(detected_at     as timestamptz)        as detected_at,
        image_path
    from {{ source('raw', 'yolo_detections') }}
    where channel  is not null
      and image_path is not null

),

messages as (

    select
        message_id,
        channel,
        channel_key,
        date_key,
        posted_at,
        view_count,
        forward_count
    from {{ ref('fct_messages') }}

),

final as (

    select
        -- ── Identifiers ──────────────────────────────────────────────────────
        d.detection_id,
        d.message_id,

        -- ── Foreign keys ─────────────────────────────────────────────────────
        m.channel_key,
        m.date_key,

        -- ── Detection attributes ─────────────────────────────────────────────
        d.channel,
        d.detected_class,
        d.confidence_score,
        d.image_category,

        -- ── Bounding box ─────────────────────────────────────────────────────
        d.bbox_x1,
        d.bbox_y1,
        d.bbox_x2,
        d.bbox_y2,

        -- ── Derived bounding box metrics ─────────────────────────────────────
        round((d.bbox_x2 - d.bbox_x1) * (d.bbox_y2 - d.bbox_y1), 2)
                                                    as bbox_area_px,

        -- ── Context from the message ─────────────────────────────────────────
        m.posted_at,
        m.view_count,
        m.forward_count,

        -- ── Timestamp ────────────────────────────────────────────────────────
        d.image_path,
        d.detected_at

    from detections d
    left join messages m
        on  d.message_id = m.message_id
        and d.channel    = m.channel

)

select * from final
