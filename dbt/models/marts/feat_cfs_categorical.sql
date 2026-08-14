-- ============================================================================
-- FORWARD FEATURE ENGINE — categorical (T2): 24 cot (scope fs_2026_08_v2)
--   value map : f37 · f38 · f79 · f80 · f81 · f82
--   one-hot g1: f40 · f41 · f42                       (3/3 — DU CA GROUP)
--   one-hot g2: f43 · f44 · f45 · f46 · f47 · f52     (6/10)
--   one-hot g3: f53 · f54 · f57 · f58 · f59 · f62     (6/10)  ★ MOI o v2
--   one-hot g4: f64 · f65                             (2/3)
--   one-hot g6: f68                                   (1/2)
--
-- docs/RECONSTRUCTION_SPEC.md §5 (group-level) · §8 (encoding contract)
--
-- ★ GROUP-LEVEL RECONSTRUCTION (§5, G-1)
--   `synthetic_segment_g2` co 10 MUC du model chi doc 6 cot.
--   Dung du 10 muc la BAT BUOC: chi dung 6 se lam vo bat bien
--   `sum(f43..f52) = 1`, va luc do cac cot da chon mat nghia "loai tru lan nhau".
--   Dieu nay dung y het cho g3 (10 muc, chon 6) va g4 (3 muc, chon 2).
--
-- ★ BAY §8.3 — f37 va f38 DUNG CHUNG ALPHABET
--   alphabet(f37) la tap con hoan toan cua alphabet(f38) (64/64 gia tri),
--   nhung card(tuple) = 1133 > 241 => HAI bien doc lap.
--   => encoding map PHAI duoc tra theo KHOA THUOC TINH, khong phai theo gia tri.
--   Trong SQL: join theo (attr, level_id), khong join theo value.
--
-- ★ f79-f82 la MOT bien latent 515 muc, BON encoding
--   card(tuple f79..f82) = card(moi cot) = 515 (song anh, dung o CA train lan test)
--   => MOT thuoc tinh `synthetic_category_515`, KHONG phai bon.
--
-- 🚫 KHONG doc payload target. 🚫 KHONG reconstruction_mode.
-- ============================================================================
{{ config(materialized='table') }}

{%- set encoding_version = var('reconstruction_encoding_version') -%}

with attrs as (

    -- Thuoc tinh latent cua customer, do decode step ghi ra.
    -- Day la NGUON, khong phai target.
    select target_id, attr_name, level_id
    from {{ source('biz', 'customer_attribute') }}

),

-- ── Encoding map: tra theo (attr_name, level_id) — KHONG tra theo value ──
enc as (

    select attr_name, level_id, column_name, value
    from {{ source('biz', 'encoding_map') }}
    where encoding_version = '{{ encoding_version }}'

),

encoded as (

    select
        a.target_id,
        e.column_name,
        e.value
    from attrs a
    join enc e
      on e.attr_name = a.attr_name        -- ★ khoa thuoc tinh — chan mo ho §8.3
     and e.level_id  = a.level_id

),

-- ── One-hot: dung DU muc cua group, roi moi chieu ra cot ────────────────
onehot as (

    select
        a.target_id,
        g.column_name,
        case when g.level_index = a.level_id then 1.0 else 0.0 end as value
    from attrs a
    join {{ source('biz', 'onehot_layout') }} g
      on g.attr_name = a.attr_name
     and g.encoding_version = '{{ encoding_version }}'

),

unioned as (
    select target_id, column_name, value from encoded
    union all
    select target_id, column_name, value from onehot
)

select
    target_id,
    -- 24 cot T2. Cac cot anh em (f48..f51, f55, f56, f60, f61, f63, f78) VAN
    -- duoc dung o `unioned` de giu bat bien one-hot, chi khong xuat ra day (§1.2).
    {% for col in [
        'f37','f38','f79','f80','f81','f82',
        'f40','f41','f42',
        'f43','f44','f45','f46','f47','f52',
        'f53','f54','f57','f58','f59','f62',
        'f64','f65',
        'f68'
    ] -%}
    max(value) filter (where column_name = '{{ col }}') as {{ col }},
    {% endfor %}

    -- Bat bien one-hot cua group DAY DU — dbt test se assert = 1.
    -- Neu chi dung 6/10 muc thi tong nay khong bao gio bang 1 => lo ngay.
    sum(value) filter (where column_name in ('f40','f41','f42'))  as _g1_onehot_sum,
    sum(value) filter (where column_name in
        ('f43','f44','f45','f46','f47','f48','f49','f50','f51','f52')
    )                                             as _g2_onehot_sum,
    sum(value) filter (where column_name in
        ('f53','f54','f55','f56','f57','f58','f59','f60','f61','f62')
    )                                             as _g3_onehot_sum,
    sum(value) filter (where column_name in ('f63','f64','f65'))  as _g4_onehot_sum,
    sum(value) filter (where column_name in ('f68','f78'))        as _g6_onehot_sum

from unioned
group by target_id

-- ⚠️ g4: bat bien nay dung 100% tren train nhung VO tren test —
--    sum(f63,f64,f65) = 0 o 3/181,669 dong test. Attribute that co mot muc
--    baseline toan-0 khong xuat hien trong train. `split_allowed: train`
--    nen chua no ra; 🚫 KHONG mo scope sang test truoc khi xu ly muc thu tu.
