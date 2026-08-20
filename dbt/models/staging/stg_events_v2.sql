-- ============================================================================
-- CLEANED layer cho event witness cua Track A (raw/events_v2).
--
-- Track A event staging: strip reconstruction-only witness metadata.
--
-- ★ TRACH NHIEM QUAN TRONG NHAT: DROP metadata cua solver.
--
--   Solver gan `gen_reason` len tung event — "event nay sinh ra de phuc vu f5".
--   Neu feature engine dem theo `gen_reason`, no dang dem NHUNG EVENT MA SOLVER
--   DA NOI la thuoc ve f5 => vong tron hoan hao, Gate A khong kiem gi ca.
--
--   Model nay chi cho di qua:  event_id · customer_id · event_type · event_ts
--                              observation_ts
--   🚫 DROP: gen_reason · day_offset · sub_index · occurrence · _gen_*
--
--   `day_offset` bi drop CO CHU Y du no tien: feature engine phai TU TINH lai
--   tu (event_ts, reference_ts) — dung nhu production lam.
-- ============================================================================
{{ config(materialized='view') }}

with source as (

    select * from {{ source('raw', 'events_v2') }}

),

typed as (

    select
        event_id,
        customer_id,
        target_id,
        event_type,
        -- 🚫 KHONG select gen_reason / day_offset / sub_index / occurrence:
        --    do la noi bo cua solver. Feature engine phai mu voi chung.
        cast(event_ts as double)                        as event_ts_epoch,
        to_timestamp(cast(event_ts as double))          as event_ts,
        -- availability semantics §12.1: event xay ra truoc reference_ts nhung
        -- he thong chi nhin thay sau do thi KHONG co mat luc tinh feature
        to_timestamp(cast(observation_ts as double))    as observation_ts,
        source_type,
        generation_run_id
    from source
    where event_id is not null
      and customer_id is not null

),

deduped as (

    -- Consumer at-least-once nen co the trung; giu ban ghi dau tien vao lake
    select
        *,
        row_number() over (partition by event_id order by observation_ts asc) as _rn
    from typed

)

select * exclude (_rn)
from deduped
where _rn = 1
