-- ============================================================================
-- PASS-THROUGH (T3): 18 cot
--   f3 f4 f8 f9 f10 f12 f13 f16 f20 f21 f22 f23 f25 f26 f28 f29 f31 f35
--
-- docs/RECONSTRUCTION_SPEC.md §7 regime PASS · §12 Gate A-T3
--
-- ⚠️ MODEL NAY KHONG PHAI FEATURE ENGINEERING.
--    No la COPY co kiem soat. Ghi ro dieu do de khong ai nham.
--
-- 🚫 Ket qua cua no KHONG duoc tinh vao ti le pass cua Gate A.
--    Gop hai ti le se lam Gate A luon >= 50% nho copy — con so vo nghia.
--    => Gate A (18 cot T1+T2) va Gate A-T3 (18 cot nay) bao cao RIENG.
--
-- Vi sao van can model nay: tong reconstruction target la 36 cot. Neu khong
-- kiem T3, 18 cot co the hong tren duong truyen ma khong ai biet.
-- ============================================================================
{{ config(materialized='table') }}

select
    target_id,

    -- 18 cot T3 — copy nguyen trang tu snapshot goc.
    -- KHONG bien doi, KHONG dien giai, KHONG gan semantic ([UNKNOWN]).
    {% for col in [
        'f3','f4','f8','f9','f10','f12','f13','f16','f20',
        'f21','f22','f23','f25','f26','f28','f29','f31','f35'
    ] -%}
    {{ col }}{{ "," if not loop.last }}
    {% endfor %}

from {{ source('biz', 'passthrough_source') }}

-- ⚠️ `f23` va `f25` trung nhau 99.78% (corr 0.999957) va `f9`/`f16` bang nhau
--    73.84%. Ca hai cap deu CHUA duoc giai quyet o muc semantic
--    (feature_semantics_2026_08.yaml). Chung van duoc giu NGUYEN VEN o day —
--    🚫 khong duoc gop/bo cot nao khi chua co ablation evidence.
