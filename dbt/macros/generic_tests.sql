{# ==========================================================================
   Generic test tu viet (khong can cai dbt_utils - de build offline nhanh)
   ========================================================================== #}

{% test dbt_utils_at_least_zero(model, column_name) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} < 0
{% endtest %}


{# Kiem tra do tuoi du lieu: cot thoi gian moi nhat khong duoc cu hon N gio #}
{% test freshness_hours(model, column_name, max_hours=26) %}
select
    max({{ column_name }}) as latest,
    date_diff('hour', max({{ column_name }}), now()) as age_hours
from {{ model }}
having date_diff('hour', max({{ column_name }}), now()) > {{ max_hours }}
{% endtest %}


{# Ty le null cua 1 cot khong duoc vuot nguong #}
{% test null_rate_below(model, column_name, max_rate=0.02) %}
select
    sum(case when {{ column_name }} is null then 1 else 0 end)::double / nullif(count(*), 0) as null_rate
from {{ model }}
having sum(case when {{ column_name }} is null then 1 else 0 end)::double / nullif(count(*), 0) > {{ max_rate }}
{% endtest %}


{# So dong toi thieu - bat truong hop pipeline chay nhung ra bang rong #}
{% test min_rows(model, n=1000) %}
select count(*) as row_count
from {{ model }}
having count(*) < {{ n }}
{% endtest %}
