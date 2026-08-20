-- Reconstruction forward engine for the five active T2 features.
-- f41/f42/f81/f82 remain encoding intermediates and never enter the target.
-- synthetic_category_515 is MOT bien latent with four encodings; only f79/f80
-- are selected by fs_2026_08_v3.
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
    select a.target_id, e.column_name, e.value
    from attrs a
    join enc e
      on e.attr_name = a.attr_name
     and e.level_id = a.level_id
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
    select * from encoded
    union all
    select * from onehot
)

select
    target_id,
    max(value) filter (where column_name = 'f37') as f37,
    max(value) filter (where column_name = 'f38') as f38,
    max(value) filter (where column_name = 'f40') as f40,
    max(value) filter (where column_name = 'f79') as f79,
    max(value) filter (where column_name = 'f80') as f80,
    sum(value) filter (where column_name in ('f40', 'f41', 'f42')) as _g1_onehot_sum
from unioned
group by target_id
