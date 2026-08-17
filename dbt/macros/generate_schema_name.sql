{# ==========================================================================
   Ten schema = DUNG cai khai trong `dbt_project.yml`, khong ghep tien to.

   ★ VAN DE
   -------------------------------------------------------------------------
   Mac dinh dbt ghep `<target.schema>_<custom_schema>`. `profiles.yml` khong
   dat `schema:` nen dbt-duckdb lay `main`, va `+schema: marts` bien thanh
   `main_marts`. Nhung phia Python doc theo ten khai trong
   `config/features/feature_spec.yml`:

       serving_table: marts.feat_user_selected_serving

   Hai ben goi hai ten khac nhau cho cung mot bang. Hau qua da gap: DAG 20
   build xong 7/7 model, DAG 40 van do ngay o buoc `prepare`:

       RuntimeError: Bang offline 'marts.feat_user_selected_serving'
                     chua ton tai. Chay DAG 20_build_features_dbt truoc.

   — mot thong bao chi thang vao viec da lam roi, nen rat de dan nguoi doc di
   sai huong.

   ★ VI SAO SUA O DAY CHU KHONG SUA `feature_spec.yml`
   -------------------------------------------------------------------------
   `feature_spec.yml` la hop dong cua feature store va serving, va ten `marts.*`
   cung la ten dung trong tai lieu, SQL viet tay va cac DAG. Doi mot
   dong macro re hon doi hop dong o nhieu noi.

   Tien to `<target>_` sinh ra de nhieu nguoi chia nhau mot warehouse ma khong
   dam vao nhau. O day warehouse la mot file DuckDB rieng cho tung moi truong
   (duong dan lay tu `DUCKDB_PATH`), nen su cach ly da co san — tien to chi
   con lam ten dai ra.
   ========================================================================== #}
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
