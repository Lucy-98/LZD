-- ============================================================================
-- INTERMEDIATE: CFS Categorical Features (T2) — f37, f38, f40, f79, f80
-- Nguồn: biz.customer_attribute, biz.encoding_map, biz.onehot_layout
-- ============================================================================
{{ config(materialized='table') }}

{%- set encoding_version = var('reconstruction_encoding_version') -%}

with attrs as (

    select target_id, attr_name, level_id
    from {{ source('biz', 'customer_attribute') }}

),

enc as (

    select attr_name, level_id, column_name, value
    from {{ source('biz', 'encoding_map') }}
    where encoding_version = '{{ encoding_version }}'

),

encoded as (

    select
        a.target_id,
        e.column_name,
        e.value
    from attrs a
    join enc e
      on e.attr_name = a.attr_name
     and e.level_id  = a.level_id

),

onehot as (

    select
        a.target_id,
        g.column_name,
        case when g.level_index = a.level_id then 1.0 else 0.0 end as value
    from attrs a
    join {{ source('biz', 'onehot_layout') }} g
      on g.attr_name = a.attr_name
     and g.encoding_version = '{{ encoding_version }}'

),

unioned as (

    select target_id, column_name, value from encoded
    union all
    select target_id, column_name, value from onehot

),

pivoted as (

    select
        target_id,
        max(value) filter (where column_name = 'f37') as f37,
        max(value) filter (where column_name = 'f38') as f38,
        max(value) filter (where column_name = 'f40') as f40,
        max(value) filter (where column_name = 'f79') as f79,
        max(value) filter (where column_name = 'f80') as f80
    from unioned
    group by target_id

)

select
    target_id,

    -- Cột f* gốc
    f37,
    f38,
    f40,
    f79,
    f80,

    -- Tên nghiệp vụ
    coalesce(f37, 0.0) as price_sensitivity_segment,
    coalesce(f38, 0.0) as promo_affinity_segment,
    coalesce(f40, 0.0) as platform_preference_flag,
    coalesce(f79, 0.0) as preferred_category_enc_a,
    coalesce(f80, 0.0) as preferred_category_enc_b

from pivoted
