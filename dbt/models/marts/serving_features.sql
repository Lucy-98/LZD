-- ============================================================================
-- MARTS (GOLD): Serving Features (TÊN NGHIỆP VỤ)
-- Nguồn phục vụ trực tiếp cho DAG 40 để sync lên Redis Feature Store (fs:*).
-- ============================================================================
{{ config(
    materialized='table',
    pre_hook=[
        "DROP INDEX IF EXISTS marts.idx_serving_features_user",
        "DROP INDEX IF EXISTS idx_serving_features_user"
    ],
    post_hook="CREATE INDEX IF NOT EXISTS idx_serving_features_user ON {{ this }} (user_id)"
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
),

behaviour as (
    select * from {{ ref('int_event_behaviour') }}
)

select
    b.user_id,
    b.dt,
    b.reference_ts as feature_ts,

    -- ═══ Profile: Customer Segmentation (T2) ═══
    coalesce(cat.price_sensitivity_segment, 0.0) as price_sensitivity_segment,
    coalesce(cat.promo_affinity_segment, 0.0)    as promo_affinity_segment,
    coalesce(cat.platform_preference_flag, 0.0)  as platform_preference_flag,
    coalesce(cat.preferred_category_enc_a, 0.0)  as preferred_category_enc_a,
    coalesce(cat.preferred_category_enc_b, 0.0)  as preferred_category_enc_b,

    -- ═══ Profile: Latent Scores (T3) ═══
    coalesce(p.latent_ordinal_segment, 0.0)          as latent_ordinal_segment,
    coalesce(p.customer_value_score, 0.0)            as customer_value_score,
    coalesce(p.purchase_power_score, 0.0)            as purchase_power_score,
    coalesce(p.baseline_conversion_propensity, 0.0)  as baseline_conversion_propensity,
    coalesce(p.incremental_response_score, 0.0)      as incremental_response_score,
    coalesce(p.discount_sensitivity_score, 0.0)      as discount_sensitivity_score,
    coalesce(p.category_affinity_score, 0.0)         as category_affinity_score,
    coalesce(p.brand_affinity_score, 0.0)            as brand_affinity_score,
    coalesce(p.churn_risk_score, 0.0)                as churn_risk_score,
    coalesce(p.assortment_breadth_score, 0.0)        as assortment_breadth_score,
    coalesce(p.abuse_risk_score, 0.0)                as abuse_risk_score,
    coalesce(p.ltv_score_a, 0.0)                     as ltv_score_a,
    coalesce(p.basket_stability_score, 0.0)          as basket_stability_score,
    coalesce(p.ltv_score_b, 0.0)                     as ltv_score_b,
    coalesce(p.shipping_sensitivity_score, 0.0)      as shipping_sensitivity_score,
    coalesce(p.quality_index_a, 0.0)                 as quality_index_a,
    coalesce(p.payment_friction_score, 0.0)          as payment_friction_score,
    coalesce(p.search_intent_score, 0.0)             as search_intent_score,
    coalesce(p.campaign_fatigue_score, 0.0)          as campaign_fatigue_score,
    coalesce(p.quality_index_b, 0.0)                 as quality_index_b,
    coalesce(p.seller_quality_pref_score, 0.0)       as seller_quality_pref_score,

    -- ═══ Event-Derived: T1 CFS Features ═══
    coalesce(r.days_since_first_signal, 0.0)         as days_since_first_signal,
    coalesce(r.days_since_last_signal, 0.0)          as days_since_last_signal,
    coalesce(c.browse_intensity_365d, 0.0)           as browse_intensity_365d,
    coalesce(c.promo_touch_intensity_365d, 0.0)      as promo_touch_intensity_365d,

    -- ═══ Event-Derived: Behaviour Features ═══
    coalesce(bh.order_cnt_7d, 0)                     as order_cnt_7d,
    coalesce(bh.order_cnt_14d, 0)                    as order_cnt_14d,
    coalesce(bh.order_cnt_30d, 0)                    as order_cnt_30d,
    coalesce(bh.gmv_7d, 0.0)                         as gmv_7d,
    coalesce(bh.gmv_30d, 0.0)                        as gmv_30d,
    coalesce(bh.avg_order_value_30d, 0.0)            as avg_order_value_30d,
    coalesce(bh.voucher_used_14d, 0)                 as voucher_used_14d,
    coalesce(bh.voucher_claim_rate_30d, 0.0)         as voucher_claim_rate_30d,
    coalesce(bh.active_days_30d, 0)                  as active_days_30d,
    coalesce(bh.recency_last_order_days, 999)        as recency_last_order_days,
    coalesce(bh.user_tenure_days, 0)                 as user_tenure_days

from boundary b
left join c_rec r  on r.target_id = b.target_id
left join c_cnt c  on c.target_id = b.target_id
left join c_cat cat on cat.target_id = b.target_id
left join c_pass p on p.target_id = b.target_id
left join behaviour bh on bh.user_id = b.user_id
