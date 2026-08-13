-- ============================================================================
-- FORWARD FEATURE ENGINE — counter (T1): f5 · f11 · f18 · f30
--
-- docs/RECONSTRUCTION_SPEC.md §7 (regime) · §4.2 (H1/H2)
--
-- ★ Model nay la mat xich bien Gate A tu "hop dong" thanh "chay duoc".
--   Track A:  E*  ->  MODEL NAY  ->  F'  ->  so voi target
--
-- 🚫 KHONG doc payload cua target. Chi doc `reference_ts` qua boundary view.
-- 🚫 KHONG co nhanh `if reconstruction_mode` (TA-2).
-- 🚫 KHONG dem theo `gen_reason` — stg_events_v2 da drop no (TA-6).
--    Model chi biet `event_type`, dung nhu production.
--
-- HAI REGIME MA HOA KHAC NHAU — do duoc tren 926,669 dong:
--   LOG10  f18, f30   luu 6 chu so   round-trip khop CHINH XAC 100.0000%
--   LN     f5,  f11   float64 day du round-trip exact chi 93.05% / 93.42%
--                                     => phai so bang dung sai tuong doi 1e-15
-- ============================================================================
{{ config(materialized='table') }}

{%- set active_window = var('history_days') -%}          {# 30 ngay #}
{%- set counter_window = var('counter_window_days') -%}  {# 365 - [ASSUMPTION] S-06 #}
{%- set f30_branch = var('f30_semantic_branch', 'H1') -%}

with boundary as (

    -- CHI target_id + reference_ts. View nay khong phoi bay payload (§6b SQL).
    select target_id, customer_id_hint as customer_id, reference_ts
    from {{ source('biz', 'reconstruction_boundary') }}

),

events as (

    select * from {{ ref('stg_events_v2') }}

),

joined as (

    select
        b.target_id,
        b.reference_ts,
        e.event_type,
        e.event_ts
    from boundary b
    left join events e
           on e.target_id = b.target_id
          -- Bien PIT: NGHIEM NGAT `<`, khop voi feat_user_realtime_pit.
          -- Dung `<=` se lech mot event o bien — loi rat kho tim vi chi hien
          -- o mot ti le nho user.
          and e.event_ts < b.reference_ts
          -- availability: event chua duoc quan sat thi chua ton tai
          and e.observation_ts <= b.reference_ts

),

counted as (

    select
        target_id,
        reference_ts,

        -- H1: f30 la SO NGAY PHAN BIET trong cua so 30 ngay.
        count(distinct cast(event_ts as date)) filter (
            where event_ts >= reference_ts - interval '{{ active_window }} days'
        )                                                        as n30_h1,

        -- H2: f30 la counter cua lop event rieng. Hai hypothesis dung chung
        -- event contract, chi khac semantic duoc chon o runtime config.
        count(*) filter (
            where event_type = 'EVT_F30'
              and event_ts >= reference_ts - interval '{{ counter_window }} days'
        )                                                        as n30_h2,

        -- f5 / f11 / f18: dem CFS witness event theo LOAI, cua so {{ counter_window }} ngay.
        -- Loai event la contract noi bo CFS; solver va model khai bao DOC LAP
        -- cung mot gia dinh => lech nhau thi Gate A do.
        count(*) filter (
            where event_type = 'EVT_F5'
              and event_ts >= reference_ts - interval '{{ counter_window }} days'
        )                                                        as n5,
        count(*) filter (
            where event_type = 'EVT_F11'
              and event_ts >= reference_ts - interval '{{ counter_window }} days'
        )                                                        as n11,
        count(*) filter (
            where event_type = 'EVT_F18'
              and event_ts >= reference_ts - interval '{{ counter_window }} days'
        )                                                        as n18

    from joined
    group by target_id, reference_ts

),

resolved as (

    select
        target_id,
        reference_ts,
        n5,
        n11,
        n18,
        case
            when '{{ f30_branch }}' = 'H2' then n30_h2
            else n30_h1
        end as n30
    from counted

)

select
    target_id,
    reference_ts,

    -- Gia tri da giai ma — giu lai de audit va de assertion o tang event (§14.1)
    n5, n11, n18, n30,

    -- ── REGIME LN — float64 DAY DU, KHONG lam tron ──────────────────────
    -- 🚫 CAM `round(ln(n), k)`: lam tron se pha round-trip 1e-15.
    case when n5  >= 1 then ln(n5)  end                          as f5,
    case when n11 >= 1 then ln(n11) end                          as f11,

    -- ── REGIME LOG10 — lam tron DUNG 6 chu so ───────────────────────────
    -- 6 chu so la quy uoc luu tru do duoc; lam tron o day cho round-trip
    -- khop CHINH XAC, khong can dung sai.
    case when n18 >= 1 then round(log10(n18), 6) end             as f18,
    case when n30 >= 1 then round(log10(n30), 6) end             as f30

from resolved

-- ⚠️ KHONG coalesce n=0 thanh 1.
--    Mien do duoc la [1, N] cho ca bon counter. Neu engine thay 0, do la
--    KHONG KHOP THAT — phai de NULL cho Gate A bat, khong duoc che di.
