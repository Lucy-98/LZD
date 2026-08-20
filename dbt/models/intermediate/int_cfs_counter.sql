-- ============================================================================
-- INTERMEDIATE: CFS Counter Features (T1) — f5, f18
-- Nguồn: raw/events_v2 + biz.reconstruction_boundary
-- ============================================================================
{{ config(materialized='table') }}

{%- set counter_window = var('counter_window_days', 365) -%}

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
        e.event_type,
        e.event_ts
    from boundary b
    left join events e
           on e.target_id = b.target_id
          and e.event_ts < b.reference_ts
          and e.observation_ts <= b.reference_ts

),

counted as (

    select
        target_id,
        user_id,
        reference_ts,
        count(*) filter (
            where event_type = 'EVT_F5'
              and event_ts >= reference_ts - interval '{{ counter_window }} days'
        ) as n5,
        count(*) filter (
            where event_type = 'EVT_F18'
              and event_ts >= reference_ts - interval '{{ counter_window }} days'
        ) as n18
    from joined
    group by target_id, user_id, reference_ts

)

select
    target_id,
    user_id,
    reference_ts,
    n5,
    n18,

    -- Cột f* gốc
    case when n5  >= 1 then ln(n5)  end as f5,
    case when n18 >= 1 then round(log10(n18), 6) end as f18,

    -- Tên nghiệp vụ
    coalesce(case when n5  >= 1 then ln(n5) end, 0.0) as browse_intensity_365d,
    coalesce(case when n18 >= 1 then round(log10(n18), 6) end, 0.0) as promo_touch_intensity_365d

from counted
