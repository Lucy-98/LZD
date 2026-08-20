-- Controlled copy for the 21 active T3 features in fs_2026_08_v3.
-- KHONG PHAI FEATURE ENGINEERING; Gate A-T3 validates this copy separately.
-- Active columns: 'f0','f3','f4','f8','f9','f10','f12','f13','f16','f17',
-- 'f22','f23','f24','f25','f26','f27','f28','f29','f31','f34','f35'.
{{ config(materialized='table') }}

select
    target_id,
    f0, f3, f4, f8, f9, f10, f12, f13, f16, f17, f22,
    f23, f24, f25, f26, f27, f28, f29, f31, f34, f35
from {{ source('biz', 'passthrough_source') }}
