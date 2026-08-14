# Business Alias Map For Selected Features

Tai lieu nay giai thich lop ten nghiep vu synthetic cho 55 feature selected.

Muc dich: lam bai toan Lazada voucher uplift doc co logic hon. Reviewer co the
nhin `f30` va hieu no dang dong vai `active_days_30d_log10` trong synthetic
business story.

Ranh gioi quan trong:

- `f1..f82` van la ten cot that trong dataset/Redis/dbt.
- Alias khong phai semantic that cua Lazada/DESCN.
- Alias chi la `SYNTHETIC_ASSUMPTION` de ke chuyen business hop ly.
- Track A van sinh `CFS_WITNESS` events, khong sinh raw Lazada domain events.
- Track B moi la future synthetic business-v2 event tu `CustomerState(T0)`.

Source of truth cho mapping:

```text
config/features/business_aliases.yml
```

## Why These Names Are Reasonable

Voucher uplift decision thuong phu thuoc cac nhom tin hieu sau:

| Business signal | Anh huong toi voucher decision |
|---|---|
| Recent activity | User vua active co kha nang phan hoi cao hon user ngu nguoi |
| Purchase intent | Browse/cart/checkout cho thay user dang gan quyet dinh mua |
| Promo sensitivity | User hay cham promo co the nhay voi voucher hon |
| Category preference | Voucher hieu qua khac nhau theo nganh hang |
| Price sensitivity | User nhay gia de bi tac dong boi discount |
| Lifecycle segment | New/active/dormant/high-intent can policy khac nhau |
| Geography/platform | Logistics, payment habit, app/web surface khac nhau |
| Latent value/risk | Gia tri khach hang, bien loi nhuan, abuse risk, fatigue |

Dataset LZD khong cong bo ten semantic cua `f*`, nhung nhung nhom tren la hop
ly cho mot ecommerce voucher decision system. Vi vay repo gan alias theo vai tro
business, khong gan theo fact.

## T1 - Event-Like CFS Surface

| Feature | Synthetic alias | Business role | Reconstruction meaning |
|---|---|---|---|
| `f1` | `days_since_first_commerce_signal` | long-term lifecycle recency | CFS recency marker |
| `f2` | `days_since_last_commerce_signal` | recent purchase intent | CFS recency marker |
| `f5` | `product_browse_intensity_365d_ln` | browse intensity | `round(exp(f5))` witness count |
| `f11` | `cart_checkout_intent_365d_ln` | cart/checkout intent | `round(exp(f11))` witness count |
| `f18` | `promo_touch_intensity_365d_log10` | promo exposure/sensitivity | `round(10 ** f18)` witness count |
| `f19` | `promo_redemption_intensity_365d_log10` | repeat redemption, tach promo looker vs taker | `round(10 ** f19)` witness count |
| `f30` | `active_days_30d_log10` | recent activity frequency | H1 distinct active days or H2 marker count |

Important: `EVT_ORDER_PAID` in Track A is not a proven Lazada `ORDER_PAID`.
It is aliased as `CFS_RECENCY_MARKER` in reports.

## T2 - Synthetic Customer Attributes

| Feature | Synthetic alias | Why it fits voucher uplift |
|---|---|---|
| `f37` | `price_sensitivity_segment_64_encoded` | discounts affect price-sensitive users differently |
| `f38` | `promo_affinity_segment_241_encoded` | customers have heterogeneous promo affinity |
| `f79` | `preferred_leaf_category_515_enc_a` | category changes voucher response and basket value |
| `f80` | `preferred_leaf_category_515_enc_b` | same 515-level category, alternate encoding |
| `f81` | `preferred_leaf_category_515_enc_c` | same 515-level category, alternate encoding |
| `f82` | `preferred_leaf_category_515_enc_d` | same 515-level category, alternate encoding |
| `f40` | `preferred_platform_segment_flag` | app/web placement and behavior differ |
| `f43` | `lifecycle_new_or_cold_flag` | cold/new users need different voucher pressure |
| `f44` | `lifecycle_active_flag` | active users may buy without subsidy |
| `f45` | `lifecycle_high_intent_flag` | high-intent users are close to conversion |
| `f64` | `city_tier_major_metro_flag` | region affects purchase power and logistics |
| `f68` | `discount_hunter_flag` | deal hunters react strongly to voucher offers |

`f79..f82` are not four independent business attributes. They are four encodings
of one latent 515-level attribute.

## T3 - Latent Pass-Through Signals

T3 features are high-cardinality/continuous selected fields. They stay frozen
and are not converted to Track A events.

| Feature | Synthetic alias |
|---|---|
| `f3` | `latent_customer_value_score` |
| `f4` | `latent_purchase_power_score` |
| `f8` | `latent_baseline_conversion_propensity` |
| `f9` | `latent_incremental_response_score` |
| `f10` | `latent_discount_sensitivity_score` |
| `f12` | `latent_category_affinity_score` |
| `f13` | `latent_brand_affinity_score` |
| `f16` | `latent_churn_risk_score` |
| `f20` | `latent_budget_fit_score` |
| `f21` | `latent_margin_risk_score` |
| `f22` | `latent_abuse_risk_score` |
| `f23` | `latent_ltv_signal_a` |
| `f25` | `latent_ltv_signal_b` |
| `f26` | `latent_shipping_sensitivity_score` |
| `f28` | `latent_payment_friction_score` |
| `f29` | `latent_search_intent_score` |
| `f31` | `latent_campaign_fatigue_score` |
| `f35` | `latent_seller_quality_preference_score` |

## Track A Event Alias

| Current event_type | Report alias | Meaning |
|---|---|---|
| `EVT_F5` | `CFS_PRODUCT_BROWSE_INTENSITY` | witness event for `f5` |
| `EVT_F11` | `CFS_CART_CHECKOUT_INTENT` | witness event for `f11` |
| `EVT_F18` | `CFS_PROMO_TOUCH` | witness event for `f18` |
| `EVT_F19` | `CFS_PROMO_REDEMPTION` | witness event for `f19` |
| `EVT_F30` | `CFS_ACTIVE_DAY_MARKER` | H2 witness event for `f30` |
| `EVT_ORDER_PAID` | `CFS_RECENCY_MARKER` | witness event for `f1/f2`, not real order proof |
| `EVT_SESSION_STARTED` | `CFS_ACTIVE_DAY_FILLER` | H1 active-day filler |

## How To Read A Report Row

Example:

```text
column=f30
business_alias=active_days_30d_log10
tier=T1
method=event_reconstructed
semantic_confidence=SYNTHETIC_ASSUMPTION
```

Correct reading:

> In this project, `f30` is used as a synthetic active-days signal for the
> voucher decision story, and the CFS reconstruction can reproduce it exactly.

Incorrect reading:

> Lazada's real `f30` is active days.

That second statement is not supported by the public DESCN dataset.
