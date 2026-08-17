-- ============================================================================
-- PASS-THROUGH (T3): 24 cot (scope fs_2026_08_v2)
--   f0 f3 f4 f6 f8 f9 f10 f12 f13 f16 f17 f20 f21 f22
--   f23 f24 f25 f26 f27 f28 f29 f31 f34 f35
--
-- docs/RECONSTRUCTION_SPEC.md §7 regime PASS · §12 Gate A-T3
--
-- ⚠️ MODEL NAY KHONG PHAI FEATURE ENGINEERING.
--    No la COPY co kiem soat. Ghi ro dieu do de khong ai nham.
--
-- 🚫 Ket qua cua no KHONG duoc tinh vao ti le pass cua Gate A.
--    Gop hai ti le se lam Gate A luon >= 50% nho copy — con so vo nghia.
--    => Gate A (31 cot T1+T2) va Gate A-T3 (24 cot nay) bao cao RIENG.
--
-- Vi sao van can model nay: tong reconstruction target la 55 cot. Neu khong
-- kiem T3, 24 cot co the hong tren duong truyen ma khong ai biet.
-- ============================================================================
{{ config(materialized='table') }}

select
    target_id,

    -- 24 cot T3 — copy nguyen trang tu snapshot goc.
    -- KHONG bien doi, KHONG dien giai, KHONG gan semantic ([UNKNOWN]).
    {% for col in [
        'f0','f3','f4','f6','f8','f9','f10','f12','f13','f16','f17','f20',
        'f21','f22','f23','f24','f25','f26','f27','f28','f29','f31','f34','f35'
    ] -%}
    {{ col }}{{ "," if not loop.last }}
    {% endfor %}

from {{ source('biz', 'passthrough_source') }}

-- ⚠️ `f23` va `f25` trung nhau 99.78% (corr 0.999957) va `f9`/`f16` bang nhau
--    73.84%. Ca hai cap deu CHUA duoc giai quyet o muc semantic
--    (feature_semantics_2026_08.yaml). Chung van duoc giu NGUYEN VEN o day —
--    🚫 khong duoc gop/bo cot nao khi chua co ablation evidence.
--
-- ⚠️ `f0` (6 muc), `f27`/`f34` (so nguyen [0,100]) duoc dat o T3 mot cach CO Y.
--    ERD.md xep chung la "T1|T2" — tuc la CHINH TIER cung la assumption.
--    Pass-through la lua chon KHONG cam ket semantic gi ca; nang chung len
--    T1 hay T2 doi hoi bang chung ma hien tai chua co.
