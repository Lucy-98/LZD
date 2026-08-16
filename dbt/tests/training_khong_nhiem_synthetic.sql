-- ============================================================================
-- BAT BIEN: `training_dataset` KHONG duoc chua event do Track B sinh ra.
--
-- Track B la hanh vi `RuleBasedBehaviour` BIA RA tu CustomerState(T0). Train
-- tren no la day model hoc lai luat cua chinh no — mot vong lap tu khang dinh
-- ma metric KHONG he to ra: no van dep, co khi con dep hon, vi model duoc
-- cham tren chinh hanh vi ma luat cua no sinh ra.
--
-- Hien tai Track B ghi sang prefix rieng (`raw/track_b_future/`) va khong
-- model dbt production nao doc no, nen test nay le ra luon xanh. Do CHINH LA
-- muc dich: no bien mot su that ve kien truc thanh mot thu duoc kiem lai sau
-- MOI lan doi SQL. Ngay ai do them mot `union` hay doi mot `source()`, test
-- nay do truoc khi model kip hoc.
--
-- Test dbt PASS khi tra ve 0 dong.
-- ============================================================================

{{ config(severity='error') }}

with nguon_realtime as (

    -- `training_dataset` lay cot rt_* tu day; day la duong duy nhat event co
    -- the lot vao tap huan luyen.
    select distinct user_id
    from {{ ref('stg_app_events') }}
    where lower(coalesce(cast(event_type as varchar), '')) like '%synthetic%'

),

nhiem as (

    select t.user_id, t.dt
    from {{ ref('training_dataset') }} t
    inner join nguon_realtime n on n.user_id = t.user_id

)

select * from nhiem
