-- ============================================================================
-- INTERMEDIATE: Event-Derived Behaviour Features (7d, 14d, 30d windows)
-- Nguồn: stg_app_events (streaming events lake)
-- ============================================================================
{{ config(materialized='table') }}

with events as (

    select *
    from {{ ref('stg_app_events') }}
    where event_ts >= now() - interval '{{ var("history_days", 30) }} days'

),

agg as (

    select
        user_id,
        min(event_ts) as first_seen_ts,
        max(event_ts) as last_seen_ts,

        -- Cua so 7 ngay
        count(*) filter (
            where event_type = 'order'
              and event_ts >= now() - interval '7 days'
        ) as order_cnt_7d,
        coalesce(sum(gmv) filter (
            where event_type = 'order'
              and event_ts >= now() - interval '7 days'
        ), 0.0) as gmv_7d,

        -- Cua so 14 ngay
        count(*) filter (
            where event_type = 'order'
              and event_ts >= now() - interval '14 days'
        ) as order_cnt_14d,
        count(*) filter (
            where event_type = 'voucher_claim'
              and event_ts >= now() - interval '14 days'
        ) as voucher_used_14d,

        -- Cua so 30 ngay
        count(*) filter (where event_type = 'order')         as order_cnt_30d,
        coalesce(sum(gmv) filter (where event_type = 'order'), 0.0) as gmv_30d,
        count(*) filter (where event_type = 'voucher_claim') as voucher_used_30d,
        count(distinct session_id)                           as session_cnt_30d,
        count(distinct cast(event_ts as date))               as active_days_30d,

        -- Recency
        max(event_ts) filter (where event_type = 'order')    as last_order_ts

    from events
    group by user_id

)

select
    user_id,
    cast(coalesce(order_cnt_7d, 0) as integer)  as order_cnt_7d,
    cast(coalesce(order_cnt_14d, 0) as integer) as order_cnt_14d,
    cast(coalesce(order_cnt_30d, 0) as integer) as order_cnt_30d,
    cast(coalesce(gmv_7d, 0.0) as double)       as gmv_7d,
    cast(coalesce(gmv_30d, 0.0) as double)      as gmv_30d,

    -- AOV (Average Order Value)
    case
        when order_cnt_30d > 0 then cast(gmv_30d / order_cnt_30d as double)
        else 0.0
    end as avg_order_value_30d,

    cast(coalesce(voucher_used_14d, 0) as integer) as voucher_used_14d,

    -- Voucher claim rate 30d
    case
        when order_cnt_30d > 0 then cast(voucher_used_30d * 1.0 / order_cnt_30d as double)
        else 0.0
    end as voucher_claim_rate_30d,

    cast(coalesce(active_days_30d, 0) as integer) as active_days_30d,
    cast(coalesce(session_cnt_30d, 0) as integer) as session_cnt_30d,

    -- Recency last order (days)
    case
        when last_order_ts is not null then cast(date_diff('day', last_order_ts, now()) as integer)
        else 999
    end as recency_last_order_days,

    -- User tenure (days)
    case
        when first_seen_ts is not null then cast(date_diff('day', first_seen_ts, now()) as integer)
        else 0
    end as user_tenure_days

from agg
