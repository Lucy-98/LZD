-- ============================================================================
-- SELECTED FEATURE SERVING - nguon sync DuckDB -> Redis cho reconstruction.
--
-- Scope: DUNG 36 cot trong config/features/fs_2026_08_v1.yaml.
-- Khong sync f0..f82 day du. Khong select label/is_treat.
-- ============================================================================
{{ config(
    materialized='table',
    post_hook="CREATE INDEX IF NOT EXISTS idx_feat_selected_serving_user ON {{ this }} (user_id)"
) }}

with snapshot as (

    select * from {{ ref('stg_user_snapshot') }}
    {% if var('run_date') != '1970-01-01' %}
    where dt = date '{{ var("run_date") }}'
    {% endif %}

)

select
    user_id,
    dt,
    feature_ts,

    -- T1 - event-level
    f1, f2, f5, f11, f18, f30,

    -- T2 - attribute-level
    f37, f38, f79, f80, f81, f82, f40, f43, f44, f45, f64, f68,

    -- T3 - pass-through
    f3, f4, f8, f9, f10, f12, f13, f16, f20, f21, f22, f23,
    f25, f26, f28, f29, f31, f35

from snapshot
