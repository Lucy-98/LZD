-- CLEANED layer cho snapshot user.
-- Trach nhiem: chuan hoa kieu, khu trung, KHONG them logic nghiep vu.
{{ config(materialized='view') }}

with source as (

    select * from {{ source('raw', 'user_snapshot') }}

),

deduped as (

    -- 1 user co the xuat hien nhieu lan (nap lai file, backfill...).
    -- Giu ban ghi moi nhat theo feature_ts -> ket qua on dinh khi rerun.
    select
        *,
        row_number() over (
            partition by user_id, dt
            order by feature_ts desc
        ) as _rn
    from source

)

select
    user_id,
    data_id,
    split,
    cast(dt as date)         as dt,
    cast(feature_ts as timestamp) as feature_ts,
    cast(label    as integer) as label,
    cast(is_treat as integer) as is_treat,
    -- f0..f82 giu nguyen ten, chi ep ve double cho dong nhat
    {% for i in range(0, 83) -%}
    cast(f{{ i }} as double) as f{{ i }}{{ "," if not loop.last }}
    {% endfor %}
from deduped
where _rn = 1
