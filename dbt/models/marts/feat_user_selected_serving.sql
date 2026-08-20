-- Legacy relation kept only for callers that still reference the old name.
-- The active Redis contract is marts.serving_features (41 business keys).
{{ config(materialized='view') }}

select * from {{ ref('serving_features') }}
