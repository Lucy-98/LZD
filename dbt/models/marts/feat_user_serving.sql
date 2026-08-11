-- ============================================================================
-- BUSINESS READY - day CHINH LA bang duoc sync len Redis.
--
-- Danh sach cot phai khop 1-1 voi `batch_features` trong
-- config/features/feature_spec.yml. Sync job kiem tra dieu do truoc khi chay
-- (prepare_sync -> validate_columns) va se fail neu lech.
--
-- Khong dua feature realtime vao day: chung do stream-consumer ghi thang vao
-- Redis (rt:u:*), API merge luc doc.
-- ============================================================================
{{ config(
    materialized='table',
    post_hook="CREATE INDEX IF NOT EXISTS idx_feat_serving_user ON {{ this }} (user_id)"
) }}

with snapshot as (

    select * from {{ ref('stg_user_snapshot') }}
    {% if var('run_date') != '1970-01-01' %}
    where dt = date '{{ var("run_date") }}'
    {% endif %}

),

behaviour as (

    select * from {{ ref('feat_user_behaviour') }}

)

select
    s.user_id,
    s.dt,
    s.feature_ts,

    -- f0..f82: feature goc tu dataset DESCN
    {% for i in range(0, 83) -%}
    s.f{{ i }},
    {% endfor %}

    -- Feature ke thua tu lich su event (thieu -> 0, dung default trong spec)
    coalesce(b.user_tenure_days,   0)   as user_tenure_days,
    coalesce(b.hist_order_cnt_30d, 0)   as hist_order_cnt_30d,
    coalesce(b.hist_gmv_30d,       0.0) as hist_gmv_30d,
    coalesce(b.voucher_used_30d,   0)   as voucher_used_30d

from snapshot s
left join behaviour b using (user_id)
