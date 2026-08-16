-- CLEANED layer cho event stream.
-- Trach nhiem chinh: KHU TRUNG theo event_id.
-- Consumer commit offset SAU khi ghi (at-least-once) nen khi no restart giua
-- chung, mot so event se xuat hien 2 lan trong lake. Dedup o day de moi tang
-- phia sau coi nhu exactly-once.
{{ config(materialized='view') }}

-- Lake co the con rong (stream chua chay lan nao). Xem
-- `macros/external_source.sql`: DuckDB nem IO Error chu khong tra ve 0 dong,
-- va no keo sap ca duong feature batch — thu khong can event nao.
with source as (

{% if external_source_is_empty('raw', 'app_events') %}
    {{ typed_empty_relation({
        'event_id':        'varchar',
        'user_id':         'varchar',
        'event_type':      'varchar',
        'event_ts':        'double',
        'ingested_at':     'double',
        'session_id':      'varchar',
        'platform':        'varchar',
        'item_id':         'varchar',
        'category_id':     'varchar',
        'price':           'double',
        'quantity':        'integer',
        'kafka_partition': 'integer',
        'kafka_offset':    'bigint'
    }) }}
{% else %}
    select * from {{ source('raw', 'app_events') }}
{% endif %}

),

typed as (

    select
        event_id,
        user_id,
        event_type,
        cast(event_ts as double)                       as event_ts_epoch,
        to_timestamp(cast(event_ts as double))         as event_ts,
        cast(ingested_at as double)                    as ingested_at_epoch,
        to_timestamp(cast(ingested_at as double))      as ingested_at,
        session_id,
        platform,
        item_id,
        category_id,
        coalesce(cast(price as double), 0.0)           as price,
        coalesce(cast(quantity as integer), 0)         as quantity,
        coalesce(cast(price as double), 0.0) * coalesce(cast(quantity as integer), 0) as gmv,
        cast(kafka_partition as integer)               as kafka_partition,
        cast(kafka_offset as bigint)                   as kafka_offset,
        cast(date_trunc('day', to_timestamp(cast(event_ts as double))) as date) as dt
    from source
    where user_id is not null
      and event_id is not null

),

deduped as (

    select
        *,
        row_number() over (
            partition by event_id
            order by ingested_at_epoch asc   -- giu ban ghi vao lake dau tien
        ) as _rn
    from typed

)

select * exclude (_rn)
from deduped
where _rn = 1
