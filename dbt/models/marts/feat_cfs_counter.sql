-- Reconstruction forward engine for the active 30-feature contract.
-- T1 counters in fs_2026_08_v3 are only f5 and f18.
{{ config(materialized='table') }}

{%- set counter_window = var('counter_window_days', 365) -%}

with boundary as (
    select target_id, reference_ts
    from {{ source('biz', 'reconstruction_boundary') }}
),

events as (
    select * from {{ ref('stg_events_v2') }}
),

counted as (
    select
        b.target_id,
        b.reference_ts,
        count(*) filter (
            where e.event_type = 'EVT_F5'
              and e.event_ts >= b.reference_ts - interval '{{ counter_window }} days'
        ) as n5,
        count(*) filter (
            where e.event_type = 'EVT_F18'
              and e.event_ts >= b.reference_ts - interval '{{ counter_window }} days'
        ) as n18
    from boundary b
    left join events e
           on e.target_id = b.target_id
          and e.event_ts < b.reference_ts
          and e.observation_ts <= b.reference_ts
    group by b.target_id, b.reference_ts
)

select
    target_id,
    reference_ts,
    n5,
    n18,
    case when n5 >= 1 then ln(n5) end as f5,
    case when n18 >= 1 then round(log10(n18), 6) end as f18
from counted
