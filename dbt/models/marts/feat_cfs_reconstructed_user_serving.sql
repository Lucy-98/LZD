-- ============================================================================
-- TRACK A SERVING MART — dbt output duy nhat duoc sync len Redis cho demo.
--
-- `feat_cfs_reconstructed_selected` giu khoa audit `target_id`. Mart nay doi
-- sang `user_id` bang boundary da duoc control plane chap nhan, them dt /
-- feature_ts va giu DUNG 55 cot theo feature_spec.yml.
-- ============================================================================
-- Khong tao secondary index o day: DuckDB coi index la dependency cua table,
-- nen dbt khong the atomic replace table khi demo/backfill duoc chay lai.
-- Cohort demo chi co vai dong va sync doc full shard, index cung khong co loi.
{{ config(materialized='table') }}

with reconstructed as (

    select * from {{ ref('feat_cfs_reconstructed_selected') }}

),

boundary as (

    select target_id, customer_id_hint, reference_ts
    from {{ source('biz', 'reconstruction_boundary') }}

)

select
    b.customer_id_hint                                               as user_id,
    cast(r.reference_ts as date)                                    as dt,
    r.reference_ts                                                  as feature_ts,

    -- Thu tu canonical cua fs_2026_08_v2 / feature_spec.yml.
    r.f1, r.f2, r.f5, r.f11, r.f18, r.f19, r.f30,
    r.f37, r.f38, r.f79, r.f80, r.f81, r.f82,
    r.f40, r.f41, r.f42,
    r.f43, r.f44, r.f45, r.f46, r.f47, r.f52,
    r.f53, r.f54, r.f57, r.f58, r.f59, r.f62,
    r.f64, r.f65, r.f68,
    r.f0, r.f3, r.f4, r.f6, r.f8, r.f9, r.f10, r.f12, r.f13,
    r.f16, r.f17, r.f20, r.f21, r.f22, r.f23, r.f24, r.f25,
    r.f26, r.f27, r.f28, r.f29, r.f31, r.f34, r.f35

from reconstructed r
join boundary b using (target_id)
