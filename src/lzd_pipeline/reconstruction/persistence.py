"""Persistence layer cho provenance + lineage.

RECONSTRUCTION_SPEC.md §9 · sql/postgres/02_biz_reconstruction.sql

NGUYEN TAC: module nay KHONG duoc lam thay doi semantics cua `Provenance` da
pass test. No chi:

    Provenance  <-->  row (dict)          round-trip khong mat mat
    danh sach row  ->  kiem P-1 / P-2 / P-5

P-3 / P-4 da duoc `Provenance.__post_init__` thuc thi, va duoc LAP LAI thanh
CHECK constraint trong SQL => hai lop cung ma hoa MOT luat. Test parity o
`test_persistence.py` canh cho chung khong troi khoi nhau.

P-1 / P-2 / P-5 la tinh chat cua DO THI, khong kiem duoc bang CHECK tren mot
dong => phai co ham rieng (SQL co view tuong duong).

🚫 Module nay nam PHIA TRACK A. `live.py` khong duoc reach toi day.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime
from typing import Any

from lzd_pipeline.reconstruction.state import Provenance

#: Cot cua `biz.provenance` — thu tu nay la contract voi SQL.
PROVENANCE_COLUMNS = (
    "source_type",
    "generation_run_id",
    "root_generation_id",
    "parent_run_id",
    "parent_target_id",
    "parent_entity_id",
    "parent_feature_version",
    "ancestor_model_ids",
    "behaviour_policy_version",
    "created_at",
)


class LineageViolation(ValueError):
    """P-1 / P-2 / P-5 — tinh chat cua DO THI bi vi pham."""


# ===========================================================================
# Round-trip mapping
# ===========================================================================
def provenance_to_row(p: Provenance) -> dict[str, Any]:
    """`ancestor_model_ids` -> list de khop TEXT[] cua Postgres."""
    return {
        "source_type": p.source_type,
        "generation_run_id": p.generation_run_id,
        "root_generation_id": p.root_generation_id,
        "parent_run_id": p.parent_run_id,
        "parent_target_id": p.parent_target_id,
        "parent_entity_id": p.parent_entity_id,
        "parent_feature_version": p.parent_feature_version,
        "ancestor_model_ids": list(p.ancestor_model_ids),
        "behaviour_policy_version": p.behaviour_policy_version,
        "created_at": p.created_at,
    }


def provenance_from_row(row: Mapping[str, Any]) -> Provenance:
    """Dung lai `Provenance` tu row.

    Di qua `__post_init__` => P-3/P-4 duoc kiem LAI luc doc. Neu DB co dong
    vi pham (vd CHECK bi drop tay), no se lo o day chu khong am tham chay tiep.
    """
    return Provenance(
        source_type=row["source_type"],
        generation_run_id=row["generation_run_id"],
        root_generation_id=row["root_generation_id"],
        parent_run_id=row.get("parent_run_id"),
        parent_target_id=row.get("parent_target_id"),
        parent_entity_id=row.get("parent_entity_id"),
        parent_feature_version=row.get("parent_feature_version"),
        ancestor_model_ids=tuple(row.get("ancestor_model_ids") or ()),
        behaviour_policy_version=row.get("behaviour_policy_version"),
        created_at=row.get("created_at"),
    )


# ===========================================================================
# P-1 · do thi parent_run_id khong co chu trinh
# ===========================================================================
def assert_no_cycle(edges: Mapping[str, str | None]) -> None:
    """`edges[run_id] = parent_run_id`. Tuong duong `biz.v_lineage_cycles`."""
    WHITE, GREY, BLACK = 0, 1, 2
    color: dict[str, int] = {k: WHITE for k in edges}

    for start in edges:
        if color[start] != WHITE:
            continue
        path: list[str] = []
        node: str | None = start
        while node is not None and color.get(node, BLACK) == WHITE:
            color[node] = GREY
            path.append(node)
            node = edges.get(node)
        if node is not None and color.get(node) == GREY:
            cut = path.index(node)
            raise LineageViolation(
                "P-1 vi pham: do thi parent_run_id co CHU TRINH: "
                + " -> ".join(path[cut:] + [node])
            )
        for n in path:
            color[n] = BLACK


# ===========================================================================
# P-2 · moi ban ghi trong cung cay co cung root_generation_id
# ===========================================================================
def assert_single_root(rows: Iterable[Mapping[str, Any]]) -> None:
    edges = {r["generation_run_id"]: r.get("parent_run_id") for r in rows}
    roots = {r["generation_run_id"]: r["root_generation_id"] for r in rows}

    for run_id in edges:
        node, seen = run_id, set()
        while (parent := edges.get(node)) is not None and node not in seen:
            seen.add(node)
            node = parent
        actual_root = node
        if roots[run_id] != roots.get(actual_root, roots[run_id]):
            raise LineageViolation(
                f"P-2 vi pham: {run_id} khai root={roots[run_id]!r} nhung goc "
                f"that cua cay la {actual_root!r} (root={roots.get(actual_root)!r})"
            )


# ===========================================================================
# P-5 · tap train production — kiem tren DO THI, khong tren tung dong
# ===========================================================================
def real_lineage_roots(rows: Sequence[Mapping[str, Any]]) -> frozenset[str]:
    """Tuong duong `biz.v_real_lineage_roots`.

    Mot root du dieu kien khi MOI ban ghi trong cay deu `REAL` va khong ban ghi
    nao co to tien model. Kiem muc dong (`source_type = REAL`) KHONG DU: mot
    dong REAL van co the co to tien SYNTHETIC qua nhieu doi.
    """
    by_root: dict[str, list[Mapping[str, Any]]] = {}
    for r in rows:
        by_root.setdefault(r["root_generation_id"], []).append(r)

    return frozenset(
        root
        for root, group in by_root.items()
        if all(g["source_type"] == "REAL" for g in group)
        and all(not (g.get("ancestor_model_ids") or ()) for g in group)
    )


def assert_production_train_eligible(rows: Sequence[Mapping[str, Any]]) -> None:
    """🚫 Model production KHONG duoc train tren du lieu co to tien synthetic —
    ke ca GIAN TIEP qua nhieu doi (§9.3)."""
    ok = real_lineage_roots(rows)
    bad = sorted({r["root_generation_id"] for r in rows} - ok)
    if bad:
        raise LineageViolation(
            "P-5 vi pham: lineage sau KHONG du dieu kien cho tap train production "
            f"(co to tien khong phai REAL): {bad}"
        )


def check_all(rows: Sequence[Mapping[str, Any]]) -> None:
    """Gate D — chay ca ba kiem do thi."""
    assert_no_cycle({r["generation_run_id"]: r.get("parent_run_id") for r in rows})
    assert_single_root(rows)
