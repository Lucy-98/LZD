-- ============================================================================
-- SELECTED FEATURE SERVING - nguon sync DuckDB -> Redis cho reconstruction.
--
-- Scope: DUNG 55 cot trong config/features/fs_2026_08_v2.yaml.
-- Khong sync f0..f82 day du. Khong select label/is_treat.
-- ============================================================================
{{ config(
    materialized='table',
    pre_hook=[
        "DROP INDEX IF EXISTS marts.idx_feat_selected_serving_user",
        "DROP INDEX IF EXISTS idx_feat_selected_serving_user"
    ],
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

    -- T1 - event-level (7)
    f1, f2, f5, f11, f18, f19, f30,

    -- T2 - attribute-level (24)
    f37, f38, f79, f80, f81, f82,
    f40, f41, f42,
    f43, f44, f45, f46, f47, f52,
    f53, f54, f57, f58, f59, f62,
    f64, f65,
    f68,

    -- T3 - pass-through (24)
    f0, f3, f4, f6, f8, f9, f10, f12, f13, f16, f17, f20,
    f21, f22, f23, f24, f25, f26, f27, f28, f29, f31, f34, f35

from snapshot
