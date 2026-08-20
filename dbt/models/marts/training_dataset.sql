-- Training export view: exactly 30 model features plus non-feature columns.
-- label, is_treat and split are outcomes/metadata, not model inputs.
{{ config(materialized='view') }}

with features as (
    select * from {{ ref('training_features') }}
),

labels as (
    select user_id, dt, split, label, is_treat
    from {{ ref('stg_user_snapshot') }}
    where split = 'train'
    {% if var('run_date') != '1970-01-01' %}
      and dt = date '{{ var("run_date") }}'
    {% endif %}
)

select
    f.*,
    l.split,
    l.label,
    l.is_treat
from features f
inner join labels l
        on l.user_id = f.user_id
       and l.dt = f.dt
