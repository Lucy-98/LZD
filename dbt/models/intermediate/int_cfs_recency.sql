-- ============================================================================
-- INTERMEDIATE: CFS Recency Features (T1) — f1, f2
-- Nguồn: raw/events_v2 + biz.reconstruction_boundary
-- ============================================================================
{{ config(materialized='table') }}

with boundary as (

    select target_id, customer_id_hint as user_id, reference_ts
    from {{ source('biz', 'reconstruction_boundary') }}

),

events as (

    select * from {{ ref('stg_events_v2') }}

),

joined as (

    select
        b.target_id,
        b.user_id,
        b.reference_ts,
        e.event_ts
    from boundary b
    left join events e
           on e.target_id = b.target_id
          and e.event_type = 'EVT_ORDER_PAID'
          and e.event_ts < b.reference_ts
          and e.observation_ts <= b.reference_ts

),

agg as (

    select
        target_id,
        user_id,
        reference_ts,
        min(event_ts) as first_ts,
        max(event_ts) as last_ts
    from joined
    group by target_id, user_id, reference_ts

)

select
    target_id,
    user_id,
    reference_ts,
    first_ts,
    last_ts,

    -- Cột f* gốc
    cast(date_diff('day', first_ts, reference_ts) as double) as f1,
    cast(date_diff('day', last_ts, reference_ts) as double)  as f2,

    -- Tên nghiệp vụ
    coalesce(cast(date_diff('day', first_ts, reference_ts) as double), 0.0) as days_since_first_signal,
    coalesce(cast(date_diff('day', last_ts, reference_ts) as double), 0.0)  as days_since_last_signal

from agg
