-- ============================================================================
-- INTERMEDIATE: Pass-Through Latent Features (T3) — 21 cột
-- Nguồn: biz.passthrough_source
-- ============================================================================
{{ config(materialized='table') }}

select
    target_id,

    -- Cột f* gốc (21 cột)
    f0, f3, f4, f6, f7, f8, f9, f10, f13, f16, f17,
    f20, f21, f22, f23, f25, f26, f27, f28, f29, f35,

    -- Tên nghiệp vụ (21 cột)
    coalesce(f0, 0.0)  as latent_ordinal_segment,
    coalesce(f3, 0.0)  as customer_value_score,
    coalesce(f4, 0.0)  as purchase_power_score,
    coalesce(f6, 0.0)  as category_affinity_score,
    coalesce(f7, 0.0)  as basket_stability_score,
    coalesce(f8, 0.0)  as baseline_conversion_propensity,
    coalesce(f9, 0.0)  as incremental_response_score,
    coalesce(f10, 0.0) as discount_sensitivity_score,
    coalesce(f13, 0.0) as brand_affinity_score,
    coalesce(f16, 0.0) as churn_risk_score,
    coalesce(f17, 0.0) as assortment_breadth_score,
    coalesce(f20, 0.0) as campaign_fatigue_score,
    coalesce(f21, 0.0) as quality_index_b,
    coalesce(f22, 0.0) as abuse_risk_score,
    coalesce(f23, 0.0) as ltv_score_a,
    coalesce(f25, 0.0) as ltv_score_b,
    coalesce(f26, 0.0) as shipping_sensitivity_score,
    coalesce(f27, 0.0) as quality_index_a,
    coalesce(f28, 0.0) as payment_friction_score,
    coalesce(f29, 0.0) as search_intent_score,
    coalesce(f35, 0.0) as seller_quality_pref_score

from {{ source('biz', 'passthrough_source') }}
