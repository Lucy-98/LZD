-- ============================================================================
-- BUSINESS READY BASELINE - full mart, KHONG phai selected Redis sync source.
--
-- Bang duoc sync len Redis la `serving_features`: 71 batch fields.
-- Mart nay giu full f0..f82 + legacy hist_* de doi chieu/phan tich; no khong
-- nam tren duong Gold/Redis hien tai.
--
-- Khong dua feature realtime vao day: stream-consumer persist raw event xuong
-- lake/MinIO truoc, roi moi cap nhat Redis (rt:u:*). API merge luc doc.
-- ============================================================================
{{ config(
    materialized='view'
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
