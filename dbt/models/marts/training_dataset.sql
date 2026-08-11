-- ============================================================================
-- TRAINING DATASET - dau ra cuoi cung cua nhanh offline.
--
-- = feature batch (y het bang duoc sync len Redis)
-- + feature realtime tinh point-in-time
-- + label + is_treat + split
--
-- Vi dung CHUNG nguon feature voi serving, model train tren dung nhung con so
-- ma no se nhin thay luc chay that.
-- ============================================================================
{{ config(materialized='table') }}

with serving as (

    select * from {{ ref('feat_user_serving') }}

),

realtime as (

    select * from {{ ref('feat_user_realtime_pit') }}

),

labels as (

    select user_id, dt, split, label, is_treat
    from {{ ref('stg_user_snapshot') }}
    {% if var('run_date') != '1970-01-01' %}
    where dt = date '{{ var("run_date") }}'
    {% endif %}

)

select
    s.*,

    -- Feature realtime (thieu -> default trong feature_spec.yml)
    coalesce(r.rt_events_1h,       0)   as rt_events_1h,
    coalesce(r.rt_page_view_1h,    0)   as rt_page_view_1h,
    coalesce(r.rt_add_to_cart_1h,  0)   as rt_add_to_cart_1h,
    coalesce(r.rt_order_1h,        0)   as rt_order_1h,
    coalesce(r.rt_gmv_1h,          0.0) as rt_gmv_1h,
    coalesce(r.rt_session_len_sec, 0.0) as rt_session_len_sec,
    coalesce(r.rt_last_event_ts,   0)   as rt_last_event_ts,

    l.split,
    l.label,
    l.is_treat

from serving s
left join realtime r
       on r.user_id = s.user_id and r.dt = s.dt
inner join labels l
       on l.user_id = s.user_id and l.dt = s.dt
