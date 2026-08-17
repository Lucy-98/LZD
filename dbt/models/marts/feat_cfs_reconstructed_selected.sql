-- ============================================================================
-- RECONSTRUCTED SELECTED FEATURE MART — 55 cot cua fs_2026_08_v2.
--
-- Day la output hop nhat de audit/consume sau khi bon nhanh forward engine da
-- tinh xong. Model KHONG doc payload cua reconstruction_target; boundary chi
-- cap target_id + reference_ts. Vi vay join nay khong co duong "doc dap an".
--
--   7 T1  = recency (2) + counter (5)
--  24 T2  = categorical/attribute
--  24 T3  = pass-through, van phai danh gia rieng bang Gate A-T3
-- ============================================================================
{{ config(materialized='table') }}

with boundary as (

    select target_id, reference_ts
    from {{ source('biz', 'reconstruction_boundary') }}

),

counter as (
    select * from {{ ref('feat_cfs_counter') }}
),

recency as (
    select * from {{ ref('feat_cfs_recency') }}
),

categorical as (
    select * from {{ ref('feat_cfs_categorical') }}
),

passthrough as (
    select * from {{ ref('feat_passthrough') }}
)

select
    b.target_id,
    b.reference_ts,

    -- Thu tu canonical cua SelectedFeatureSet.columns. Giu thu tu nay de
    -- consumer positional khong bi skew (lexicographic theo ten f*).
    p.f0,
    r.f1,
    p.f10,
    c.f11,
    p.f12,
    p.f13,
    p.f16,
    p.f17,
    c.f18,
    c.f19,
    r.f2,
    p.f20,
    p.f21,
    p.f22,
    p.f23,
    p.f24,
    p.f25,
    p.f26,
    p.f27,
    p.f28,
    p.f29,
    p.f3,
    c.f30,
    p.f31,
    p.f34,
    p.f35,
    a.f37,
    a.f38,
    p.f4,
    a.f40,
    a.f41,
    a.f42,
    a.f43,
    a.f44,
    a.f45,
    a.f46,
    a.f47,
    c.f5,
    a.f52,
    a.f53,
    a.f54,
    a.f57,
    a.f58,
    a.f59,
    p.f6,
    a.f62,
    a.f64,
    a.f65,
    a.f68,
    a.f79,
    p.f8,
    a.f80,
    a.f81,
    a.f82,
    p.f9

from boundary b
left join counter c
       on c.target_id = b.target_id
      and c.reference_ts = b.reference_ts
left join recency r
       on r.target_id = b.target_id
      and r.reference_ts = b.reference_ts
left join categorical a
       on a.target_id = b.target_id
left join passthrough p
       on p.target_id = b.target_id

-- LEFT JOIN co y: target van phai hien ra neu mot component bi thieu; gia tri
-- NULL se lam dbt test/Gate A do, thay vi INNER JOIN lam mat target im lang.
