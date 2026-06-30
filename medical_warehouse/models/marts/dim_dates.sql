{{
    config(
        materialized = 'table',
        schema       = 'marts'
    )
}}

/*
  dim_dates
  ─────────
  Standard calendar dimension covering every date between the earliest
  and latest message in the warehouse (plus a 1-year buffer on each end).
  One row per calendar date.

  date_key format: YYYYMMDD integer (e.g. 20240615) — compact FK for fct_messages.
*/

with date_spine as (

    select
        generate_series(
            (select min(posted_at::date) - interval '1 year'
             from {{ ref('stg_telegram_messages') }}),
            (select max(posted_at::date) + interval '1 year'
             from {{ ref('stg_telegram_messages') }}),
            interval '1 day'
        )::date as full_date

),

enriched as (

    select
        -- Surrogate key: YYYYMMDD integer
        cast(to_char(full_date, 'YYYYMMDD') as integer)     as date_key,

        full_date,

        -- Day-level attributes
        extract(day   from full_date)::integer               as day_of_month,
        extract(dow   from full_date)::integer               as day_of_week,   -- 0=Sun … 6=Sat
        to_char(full_date, 'Day')                            as day_name,
        case
            when extract(dow from full_date) in (0, 6) then true
            else false
        end                                                  as is_weekend,

        -- Week-level attributes
        extract(week  from full_date)::integer               as week_of_year,

        -- Month-level attributes
        extract(month from full_date)::integer               as month,
        to_char(full_date, 'Month')                          as month_name,
        to_char(full_date, 'YYYY-MM')                        as year_month,

        -- Quarter-level attributes
        extract(quarter from full_date)::integer             as quarter,
        to_char(full_date, '"Q"Q YYYY')                      as quarter_name,

        -- Year-level attributes
        extract(year  from full_date)::integer               as year

    from date_spine

)

select * from enriched
