-- ============================================================================
-- FORWARD FEATURE ENGINE — categorical (T2): 12 cot
--   f37 · f38 · f79 · f80 · f81 · f82 · f40 · f43 · f44 · f45 · f64 · f68
--
-- docs/RECONSTRUCTION_SPEC.md §5 (group-level) · §8 (encoding contract)
--
-- ★ GROUP-LEVEL RECONSTRUCTION (§5, G-1)
--   `synthetic_segment_g2` co 10 MUC du model chi doc f43/f44/f45.
--   Dung du 10 muc la BAT BUOC: chi dung 3 se lam vo bat bien
--   `sum(f43..f52) = 1`, va luc do f43/f44/f45 mat nghia "loai tru lan nhau".
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
    -- 12 cot T2. Cac cot anh em (f41,f42,f46..f52,f63,f65,f78) VAN duoc dung
    -- o `unioned` de giu bat bien one-hot, chi khong xuat ra day (§1.2).
    max(value) filter (where column_name = 'f37') as f37,
    max(value) filter (where column_name = 'f38') as f38,
    max(value) filter (where column_name = 'f79') as f79,
    max(value) filter (where column_name = 'f80') as f80,
    max(value) filter (where column_name = 'f81') as f81,
    max(value) filter (where column_name = 'f82') as f82,
    max(value) filter (where column_name = 'f40') as f40,
    max(value) filter (where column_name = 'f43') as f43,
    max(value) filter (where column_name = 'f44') as f44,
    max(value) filter (where column_name = 'f45') as f45,
    max(value) filter (where column_name = 'f64') as f64,
    max(value) filter (where column_name = 'f68') as f68,

    -- Bat bien one-hot cua group DAY DU — dbt test se assert = 1.
    -- Neu chi dung 3/10 muc thi tong nay khong bao gio bang 1 => lo ngay.
    sum(value) filter (where column_name in
        ('f43','f44','f45','f46','f47','f48','f49','f50','f51','f52')
    )                                             as _g2_onehot_sum,
    sum(value) filter (where column_name in ('f40','f41','f42'))  as _g1_onehot_sum,
    sum(value) filter (where column_name in ('f63','f64','f65'))  as _g4_onehot_sum,
    sum(value) filter (where column_name in ('f68','f78'))        as _g6_onehot_sum

from unioned
group by target_id
