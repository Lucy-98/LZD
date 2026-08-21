-- Point-in-time training view matching the complete online feature contract.
-- Realtime fields are reconstructed strictly before feature_ts.
{{ config(materialized='view') }}

with features as (
    select * from {{ ref('serving_features') }}
),

realtime as (
    select * from {{ ref('feat_user_realtime_pit') }}
),

labels as (
    select user_id, dt, split, label, is_treat
    from {{ ref('stg_user_snapshot') }}
    where split = 'train'
    {% if var('run_date') != '1970-01-01' %}
      and dt = date '{{ var("run_date") }}'
    {% endif %}
)

select
    f.*,
    coalesce(r.rt_page_view_5m,    0)   as rt_page_view_5m,
    coalesce(r.rt_add_to_cart_5m,  0)   as rt_add_to_cart_5m,
    coalesce(r.rt_cart_gmv_5m,     0.0) as rt_cart_gmv_5m,
    coalesce(r.rt_search_cnt_5m,   0)   as rt_search_cnt_5m,
    coalesce(r.rt_events_1h,       0)   as rt_events_1h,
    coalesce(r.rt_page_view_1h,    0)   as rt_page_view_1h,
    coalesce(r.rt_add_to_cart_1h,  0)   as rt_add_to_cart_1h,
    coalesce(r.rt_order_1h,        0)   as rt_order_1h,
    coalesce(r.rt_search_cnt_1h,   0)   as rt_search_cnt_1h,
    coalesce(r.rt_gmv_1h,          0.0) as rt_gmv_1h,
    coalesce(r.rt_session_len_sec, 0.0) as rt_session_len_sec,
    coalesce(r.rt_last_event_ts,   0)   as rt_last_event_ts,
    l.split,
    l.label,
    l.is_treat
from features f
left join realtime r
       on r.user_id = f.user_id and r.dt = f.dt and r.feature_ts = f.feature_ts
inner join labels l
        on l.user_id = f.user_id
       and l.dt = f.dt
