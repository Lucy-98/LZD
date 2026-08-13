-- ============================================================================
-- FORWARD FEATURE ENGINE — recency (T1): f1 · f2
--
-- docs/RECONSTRUCTION_SPEC.md §7 regime REC
--
-- Bat bien DO DUOC tren 100% dong:  f1 >= f2
--   [ASSUMPTION] S-01/S-02: f1 = days_since_first_*, f2 = days_since_last_*
--   Quan he "lan dau luon xa hon lan cuoi" TU THOA rang buoc — day la ly do
--   chon semantic nay thay vi mot semantic phai bẻ cong de vua.
--
-- 🚫 KHONG doc payload target. 🚫 KHONG co reconstruction_mode.
-- 🚫 KHONG dem theo gen_reason.
-- ============================================================================
{{ config(materialized='table') }}

with boundary as (

    select target_id, reference_ts
    from {{ source('biz', 'reconstruction_boundary') }}

),

events as (

    select * from {{ ref('stg_events_v2') }}

),

joined as (

    select
        b.target_id,
        b.reference_ts,
        e.event_ts
    from boundary b
    left join events e
           on e.target_id = b.target_id
          and e.event_type = 'EVT_ORDER_PAID'   -- CFS recency witness, khong phai gen_reason
          and e.event_ts < b.reference_ts        -- bien PIT NGHIEM NGAT
          and e.observation_ts <= b.reference_ts

),

agg as (

    select
        target_id,
        reference_ts,
        min(event_ts) as first_ts,
        max(event_ts) as last_ts
    from joined
    group by target_id, reference_ts

)

select
    target_id,
    reference_ts,

    -- Regime REC: so nguyen, so sanh CHINH XAC (khong dung sai).
    -- `date_diff('day', ...)` dem ranh gioi ngay, khop voi cach solver dat
    -- event tai `reference_ts - d ngay`.
    case when first_ts is not null
         then cast(date_diff('day', first_ts, reference_ts) as integer) end as f1,
    case when last_ts is not null
         then cast(date_diff('day', last_ts,  reference_ts) as integer) end as f2

from agg

-- ⚠️ f1 = f2 khi user chi co MOT event — do la truong hop BINH THUONG,
--    do duoc o 97.42% dong. Solver phai sinh MOT event, khong phai hai
--    event trung timestamp (§7.2 E-3).
