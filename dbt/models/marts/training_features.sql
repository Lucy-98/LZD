-- ============================================================================
-- MARTS (GOLD): Training Features (30 CỘT f*)
-- Nguồn phục vụ trực tiếp cho Jupyter Notebook để huấn luyện Uplift Model.
-- T1 (4) tính từ events; T2 (5) & T3 (21) decode/lấy từ biz.*
-- ============================================================================
{{ config(
    materialized='table',
    pre_hook=[
        "DROP INDEX IF EXISTS marts.idx_training_features_user",
        "DROP INDEX IF EXISTS idx_training_features_user"
    ],
    post_hook="CREATE INDEX IF NOT EXISTS idx_training_features_user ON {{ this }} (user_id)"
) }}

with boundary as (

    select
        target_id,
        customer_id_hint as user_id,
        reference_ts,
        cast(reference_ts as date) as dt
    from {{ source('biz', 'reconstruction_boundary') }}

),

c_rec as (
    select * from {{ ref('int_cfs_recency') }}
),

c_cnt as (
    select * from {{ ref('int_cfs_counter') }}
),

c_cat as (
    select * from {{ ref('int_cfs_categorical') }}
),

c_pass as (
    select * from {{ ref('int_passthrough') }}
)

select
    b.user_id,
    b.dt,
    b.reference_ts as feature_ts,

    -- T1 (2 cột - Event-Derived)
    coalesce(r.f1, 0.0)  as f1,
    coalesce(c.f5, 0.0)  as f5,

    -- T2 (7 cột - Categorical Segment)
    coalesce(cat.f38, 0.0) as f38,
    coalesce(cat.f42, 0.0) as f42,
    coalesce(cat.f52, 0.0) as f52,
    coalesce(cat.f60, 0.0) as f60,
    coalesce(cat.f68, 0.0) as f68,
    coalesce(cat.f80, 0.0) as f80,
    coalesce(cat.f82, 0.0) as f82,

    -- T3 (21 cột - Latent Scores)
    coalesce(p.f0, 0.0)  as f0,
    coalesce(p.f3, 0.0)  as f3,
    coalesce(p.f4, 0.0)  as f4,
    coalesce(p.f6, 0.0)  as f6,
    coalesce(p.f7, 0.0)  as f7,
    coalesce(p.f8, 0.0)  as f8,
    coalesce(p.f9, 0.0)  as f9,
    coalesce(p.f10, 0.0) as f10,
    coalesce(p.f13, 0.0) as f13,
    coalesce(p.f16, 0.0) as f16,
    coalesce(p.f17, 0.0) as f17,
    coalesce(p.f20, 0.0) as f20,
    coalesce(p.f21, 0.0) as f21,
    coalesce(p.f22, 0.0) as f22,
    coalesce(p.f23, 0.0) as f23,
    coalesce(p.f25, 0.0) as f25,
    coalesce(p.f26, 0.0) as f26,
    coalesce(p.f27, 0.0) as f27,
    coalesce(p.f28, 0.0) as f28,
    coalesce(p.f29, 0.0) as f29,
    coalesce(p.f35, 0.0) as f35

from boundary b
left join c_rec r  on r.target_id = b.target_id
left join c_cnt c  on c.target_id = b.target_id
left join c_cat cat on cat.target_id = b.target_id
left join c_pass p on p.target_id = b.target_id
