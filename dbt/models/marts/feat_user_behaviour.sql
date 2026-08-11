-- Feature hanh vi tinh tu lich su event (cua so {{ var('history_days') }} ngay).
-- Day la phan "batch" - tinh 1 lan/ngay, sync len Redis.
{{ config(materialized='table') }}

with events as (

    select *
    from {{ ref('stg_app_events') }}
    where event_ts >= now() - interval '{{ var("history_days") }} days'

),

agg as (

    select
        user_id,
        min(event_ts)                                                   as first_seen_ts,
        max(event_ts)                                                   as last_seen_ts,
        count(*)                                                        as event_cnt_30d,
        count(*) filter (where event_type = 'order')                    as hist_order_cnt_30d,
        sum(gmv)  filter (where event_type = 'order')                   as hist_gmv_30d,
        count(*) filter (where event_type = 'voucher_claim')            as voucher_used_30d,
        count(distinct session_id)                                      as session_cnt_30d
    from events
    group by user_id

)

select
    user_id,
    -- Tuoi tai khoan: so ngay tu lan dau nhin thay user
    cast(date_diff('day', first_seen_ts, now()) as integer) as user_tenure_days,
    cast(hist_order_cnt_30d as integer)                     as hist_order_cnt_30d,
    cast(coalesce(hist_gmv_30d, 0.0) as double)             as hist_gmv_30d,
    cast(voucher_used_30d as integer)                       as voucher_used_30d,
    cast(event_cnt_30d as integer)                          as event_cnt_30d,
    cast(session_cnt_30d as integer)                        as session_cnt_30d,
    first_seen_ts,
    last_seen_ts
from agg
