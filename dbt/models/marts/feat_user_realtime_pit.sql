-- ============================================================================
-- POINT-IN-TIME realtime features (chong offline/online feature skew).
--
-- Van de: luc SERVE, model nhan rt_events_1h lay tu Redis - tuc la "1 gio
-- truoc THOI DIEM request". Neu luc TRAIN ta tinh rt_events_1h tren toan bo
-- lich su, model hoc tren phan bo khac han -> skew + ro ri nhan (leakage).
--
-- Giai phap: tinh lai dung cong thuc do, nhung tinh "tinh den moc feature_ts
-- cua snapshot", tuc la chi dung event trong cua so
-- [feature_ts - {{ var('realtime_window_seconds') }}s, feature_ts).
--
-- Cong thuc o day PHAI khop voi lzd_pipeline/ingestion/stream_consumer.py
-- (EVENT_TO_COUNTER) va realtime_features trong feature_spec.yml.
--
-- CUA SO PHAI KHOP TUNG GIAY VOI REDIS:
--   Online chia gio thanh 12 o 5 phut (online_store.RT_BUCKET_SECONDS) va cong
--   cac o co bucket_start >= bucket_start(now) - 11*300.
--   Neu o day dung `feature_ts - interval '1 hour'` thi hai ben lech nhau toi
--   5 phut du lieu -> model hoc tren con so khong bao gio gap luc serve.
--   Vi vay ta lam tron feature_ts ve dau o 5 phut y het ben Redis.
-- ============================================================================
{{ config(materialized='table') }}

{%- set bucket_seconds = 300 -%}
{%- set n_buckets = (var('realtime_window_seconds') | int) // bucket_seconds -%}

with snapshot as (

    select
        user_id,
        dt,
        feature_ts,
        -- Dau o 5 phut chua feature_ts - giong het rt_bucket_start() ben Python
        floor(epoch(feature_ts) / {{ bucket_seconds }}) * {{ bucket_seconds }}
            as bucket_start_epoch
    from {{ ref('stg_user_snapshot') }}
    {% if var('run_date') != '1970-01-01' %}
    where dt = date '{{ var("run_date") }}'
    {% endif %}

),

events as (

    select * from {{ ref('stg_app_events') }}

),

joined as (

    select
        s.user_id,
        s.dt,
        s.feature_ts,
        e.event_type,
        e.gmv,
        e.event_ts,
        e.session_id
    from snapshot s
    left join events e
           on e.user_id = s.user_id
          -- CHI lay event xay ra TRUOC moc snapshot...
          and e.event_ts <  s.feature_ts
          -- ...va tu dau o cu nhat con nam trong cua so:
          --     bucket_start(feature_ts) - (12 - 1) * 300
          -- Doi chieu: online_store.rt_cutoff() ben Python.
          and e.event_ts >= to_timestamp(
                  s.bucket_start_epoch - {{ (n_buckets - 1) * bucket_seconds }}
              )

)

select
    user_id,
    dt,
    feature_ts,
    cast(count(event_type) as integer)                                   as rt_events_1h,
    cast(count(*) filter (where event_type = 'page_view')   as integer)  as rt_page_view_1h,
    cast(count(*) filter (where event_type = 'add_to_cart') as integer)  as rt_add_to_cart_1h,
    cast(count(*) filter (where event_type = 'order')       as integer)  as rt_order_1h,
    cast(coalesce(sum(gmv) filter (where event_type = 'order'), 0.0) as double) as rt_gmv_1h,
    -- Do dai phien gan nhat (giay)
    cast(coalesce(date_diff('second', min(event_ts), max(event_ts)), 0) as double) as rt_session_len_sec,
    cast(coalesce(epoch(max(event_ts)), 0) as bigint)                    as rt_last_event_ts
from joined
group by user_id, dt, feature_ts
