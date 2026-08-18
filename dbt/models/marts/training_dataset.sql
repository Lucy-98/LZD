-- ============================================================================
-- TRAINING DATASET - dau ra cuoi cung cua nhanh offline.
--
-- = feature batch (y het bang duoc sync len Redis)
-- + feature realtime tinh point-in-time
-- + label + is_treat + split
--
-- Vi dung CHUNG nguon feature voi serving, model train tren dung nhung con so
-- ma no se nhin thay luc chay that.
--
-- ★ CHI `split='train'`. Tap danh gia nam o `eval_holdout` va DONG BANG —
--   xem docstring cua model do de biet vi sao.
--
-- ★ INCREMENTAL, KHONG PHAI TABLE
-- ---------------------------------------------------------------------------
-- Truoc day: `materialized='table'` + loc `dt = run_date`. Nghia la moi lan
-- dbt chay la THAY SACH bang, va bang chi con dung MOT ngay. DAG 30 thi doc
-- ca bang, nen "train hang tuan tren toan bo du lieu" thuc te la train tren
-- mot ngay. Lich su van con trong lake (partition theo `dt`), nhung bang
-- training thi khong giu.
--
-- Te hon nua: noi dung bang phu thuoc AM THAM vao viec dbt duoc goi the nao
-- lan cuoi. Hai lan train co the thay hai luong du lieu khac han ma khong co
-- gi ghi lai su khac biet do.
--
-- Gio bang cong don theo `dt`, va `train.py` phai NOI RO cua so no doc
-- (`dt_from`/`dt_to`, log vao MLflow params).
--
-- ★ CUA SO TRUOT, KHONG PHAI TOAN BO LICH SU
-- ---------------------------------------------------------------------------
-- `training_window_weeks` trong `dbt_project.yml`. Hanh vi mua sam troi theo
-- mua — du lieu hai nam truoc khong mo ta khach hang thang nay, ma con lam
-- bang phinh vo han. Dong qua han bi xoa o cuoi file.
-- ============================================================================
{{ config(
    materialized='view'
) }}

{%- set window_weeks = var('training_window_weeks') | int -%}

with serving as (

    select * from {{ ref('feat_user_serving') }}

),

realtime as (

    select * from {{ ref('feat_user_realtime_pit') }}

),

labels as (

    select user_id, dt, split, label, is_treat
    from {{ ref('stg_user_snapshot') }}
    -- 🚫 Tap danh gia KHONG duoc lan vao day.
    where split = 'train'
    {% if var('run_date') != '1970-01-01' %}
      and dt = date '{{ var("run_date") }}'
    {% endif %}

)

select
    s.*,

    -- Feature realtime (thieu -> default trong feature_spec.yml)
    coalesce(r.rt_events_1h,       0)   as rt_events_1h,
    coalesce(r.rt_page_view_1h,    0)   as rt_page_view_1h,
    coalesce(r.rt_add_to_cart_1h,  0)   as rt_add_to_cart_1h,
    coalesce(r.rt_order_1h,        0)   as rt_order_1h,
    coalesce(r.rt_gmv_1h,          0.0) as rt_gmv_1h,
    coalesce(r.rt_session_len_sec, 0.0) as rt_session_len_sec,
    coalesce(r.rt_last_event_ts,   0)   as rt_last_event_ts,

    l.split,
    l.label,
    l.is_treat

from serving s
left join realtime r
       on r.user_id = s.user_id and r.dt = s.dt
inner join labels l
       on l.user_id = s.user_id and l.dt = s.dt

{% if is_incremental() %}
-- Cua so truot: bo dong cu hon `training_window_weeks` tinh tu ngay moi nhat
-- CO TRONG BANG (khong phai `now()`) — de ket qua tai lap duoc khi chay lai
-- tren du lieu cu.
where s.dt > (
    select coalesce(max(dt), date '1970-01-01') - interval '{{ window_weeks }} weeks'
    from {{ this }}
)
{% endif %}
