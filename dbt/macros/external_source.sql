{# ==========================================================================
   Doc source ngoai (parquet tren MinIO) khi lake CO THE con rong.

   ★ VAN DE
   -------------------------------------------------------------------------
   `source('raw','app_events')` duoc dbt-duckdb dich thanh
   `read_parquet('s3://lakehouse/raw/app_events/**/*.parquet')`. Khi chua co
   file nao khop, DuckDB KHONG tra ve 0 dong ma nem loi:

       IO Error: No files found that match the pattern

   Hau qua da gap: `stg_app_events` do -> `feat_user_realtime_pit` do, va ca
   DAG 20 fail. Tuc la duong feature BATCH (55 cot, khong can event nao) chet
   chi vi duong STREAM chua chay lan nao. Do la loi ghep noi, khong phai loi
   du lieu.

   ★ CACH LAM
   -------------------------------------------------------------------------
   `glob()` cua DuckDB dem file ma KHONG nem loi khi khong co gi (da do:
   tra ve 0). Dung no lam cong kiem luc bien dich, roi chon mot trong hai
   nhanh. Nhanh rong phai tu khai kieu — `select null as x` cho ra kieu
   "UNKNOWN"/INTEGER va se lam vo cac phep cast phia sau.

   🚫 KHONG dung cach "ghi mot file parquet rong lam moi" vao lake: no bo mot
      ban ghi gia vao tang raw, ma tang raw la thu duy nhat duoc coi la su
      that goc. Cong kiem nam o SQL thi lake van sach.
   ========================================================================== #}


{# Dem file khop `external_location` cua mot source. Tra ve true khi rong. #}
{% macro external_source_is_empty(source_name, table_name) %}

    {#- Luc dbt parse (execute=false) khong duoc chay query. Tra ve false de
        nhanh "co du lieu" duoc bien dich - no la nhanh tham chieu toi source
        that, nen do thi phu thuoc cua dbt van dung. -#}
    {%- if not execute -%}
        {{ return(false) }}
    {%- endif -%}

    {%- set ns = namespace(location=none) -%}
    {%- for node in graph.sources.values() -%}
        {%- if node.source_name == source_name and node.name == table_name -%}
            {%- set ns.location = node.meta.get('external_location') -%}
        {%- endif -%}
    {%- endfor -%}

    {#- Source khong khai `external_location` => khong phai external, cu doc
        binh thuong. -#}
    {%- if ns.location is none -%}
        {{ return(false) }}
    {%- endif -%}

    {%- set result = run_query(
        "select count(*) as n from glob('" ~ ns.location ~ "')"
    ) -%}
    {%- set n = result.columns[0].values()[0] | int -%}

    {%- if n == 0 -%}
        {{ log(
            "[external_source] " ~ source_name ~ "." ~ table_name ~
            " rong (" ~ ns.location ~ ") -> dung relation rong co kieu",
            info=true
        ) }}
    {%- endif -%}

    {{ return(n == 0) }}

{% endmacro %}


{# Relation rong nhung DA KHAI KIEU, dung `where false` de chac chan 0 dong.
   `columns` la dict {ten_cot: kieu_sql}. #}
{% macro typed_empty_relation(columns) %}
    select
        {%- for name, dtype in columns.items() %}
        cast(null as {{ dtype }}) as {{ name }}{{ "," if not loop.last }}
        {%- endfor %}
    where false
{% endmacro %}
