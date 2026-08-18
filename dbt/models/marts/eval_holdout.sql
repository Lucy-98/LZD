-- ============================================================================
-- EVAL HOLDOUT - tap danh gia DONG BANG.
--
-- = y het `training_dataset` ve mat cot, nhung chi `split='test'`, va CHOT o
--   mot moc thoi gian, khong tu cap nhat.
--
-- ★ VI SAO PHAI DONG BANG
-- ---------------------------------------------------------------------------
-- `training/promote.py` quyet dinh co thay model dang phuc vu hay khong bang
-- cach so `qini` cua run moi voi run dang giu alias Production.
--
--     Phep so do CHI CO NGHIA neu hai run do tren CUNG mot tap danh gia.
--
-- Neu tap test cung lon len theo tung tuan (user moi den se duoc gan
-- `split='test'` tu CSV nguon), thi tuan sau do tren mot tap khac tuan truoc.
-- Hai con so khong so duoc nua, ma cong promote van cu so va van cu ra quyet
-- dinh. Hong kieu do khong co trieu chung: metric van dep, alias van doi,
-- khong ai biet cai thuoc do da co gian.
--
-- ★ `holdout_version` LA MOT PHAN CUA DU LIEU
-- ---------------------------------------------------------------------------
-- Cot nay di theo tung dong va duoc log vao MLflow params. `promote.py` TU
-- CHOI so hai run co `holdout_version` khac nhau. Nho vay viec doi tap danh
-- gia khong the xay ra am tham — doi thi phep so tu dung lai, chu khong ra
-- mot con so sai.
--
-- 🚫 KHONG bump `holdout_version` chi de "cho co du lieu moi". Bump nghia la
--    moi so lieu lich su truoc do khong con so sanh duoc voi so lieu sau do.
--    Do la mot quyet dinh co chu dich, khong phai mot buoc bao tri.
--
-- ★ MATERIALIZED='TABLE' NHUNG KHONG TU LAM MOI
-- ---------------------------------------------------------------------------
-- Bang duoc dung lai moi lan `dbt run`, nhung noi dung KHONG doi vi no loc
-- theo `holdout_dt` — mot hang so trong `dbt_project.yml`, khong phai
-- `run_date`. Chay dbt bao nhieu lan cung ra dung bay nhieu dong.
-- ============================================================================
{{ config(materialized='view') }}

{%- set holdout_dt = var('holdout_dt') -%}
{%- set holdout_version = var('holdout_version') -%}

with serving as (

    select * from {{ ref('feat_user_serving') }}
    where dt = date '{{ holdout_dt }}'

),

realtime as (

    select * from {{ ref('feat_user_realtime_pit') }}
    where dt = date '{{ holdout_dt }}'

),

labels as (

    -- 🚫 KHONG loc theo `run_date`. Moc thoi gian cua tap danh gia la
    --    `holdout_dt`, doc lap voi ngay chay dbt.
    select user_id, dt, split, label, is_treat
    from {{ ref('stg_user_snapshot') }}
    where dt = date '{{ holdout_dt }}'
      and split = 'test'

)

select
    '{{ holdout_version }}'             as holdout_version,
    s.*,

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
