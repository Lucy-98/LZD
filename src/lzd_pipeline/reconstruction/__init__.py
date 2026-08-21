"""Feature-consistent Event Reconstruction - runnable E2E prototype.

    ⚠️  PROTOTYPE. KHONG phai production.

Muc dich: chay Track A -> dbt feature SQL -> gates -> CustomerState -> Track B
trong dry-run co lap theo feature-set artifact dang active.

Gioi han tuyet doi:

    KHONG ghi production data       KHONG chay full dataset
    KHONG ghi Redis/Kafka/MinIO     KHONG dang ky model alias Production

Ba invariant khong duoc vi pham (§14.1):

    1. 30 columns = hard reconstruction scope. KHONG tu mo len 83.
    2. UNIDENTIFIED la trang thai NHAN THUC, khong phai semantic truth.
       semantic_branch = kich ban.  semantic_status = nhan thuc.
    3. Seed CHI tac dong SAU khi da co optimal pool.
       objective quyet dinh CHAT LUONG - seed chon TRONG SO DA NGANG NHAU -
       canonical ordering CHI sap thu tu witness da chon.
"""
from __future__ import annotations

PROTOTYPE = True
"""Co chan: moi duong ghi ra production phai assert `not PROTOTYPE`."""

SPEC_REF = "config/features/fs_2026_08_v4.yaml"
