"""Capability boundary — architectural fitness function.

RECONSTRUCTION_SPEC.md INVARIANT 4 / TEST-10b.

    Chu ky ham  =  API boundary
    Do thi import = CAPABILITY boundary

`CustomerState.source_target_id` la lineage identifier hop le, NHUNG no tro
thanh lo hong neu Track B co the giai no:

    source_target_id -> target repository -> reconstruction_target -> 36 features

Luc do DU chu ky ham sach, Track B van "nhin trom" duoc target.

Module nay quet AST va di BAC CAU qua moi import noi bo, tra ve tap module
that su REACHABLE. No fail ngay khi ai do them mot dong import — ke ca import
gian tiep qua ba lop module.
"""
from __future__ import annotations

import ast
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

#: Goc package duoc coi la "noi bo" — chi di bac cau trong pham vi nay.
INTERNAL_ROOT = "lzd_pipeline"


@dataclass(frozen=True)
class ImportEdge:
    """Mot canh trong do thi import — giu lai de bao loi CO DUONG DI."""

    source: str
    target: str
    lineno: int


def _module_name(path: Path, src_root: Path) -> str:
    rel = path.relative_to(src_root).with_suffix("")
    parts = list(rel.parts)
    if parts and parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _module_path(module: str, src_root: Path) -> Path | None:
    base = src_root.joinpath(*module.split("."))
    for cand in (base.with_suffix(".py"), base / "__init__.py"):
        if cand.is_file():
            return cand
    return None


def _resolve_relative(node: ast.ImportFrom, current: str, is_package: bool) -> str:
    """`from ..x import y` -> ten module tuyet doi.

    `level=1` phan giai theo PACKAGE CHUA module, khong theo chinh module:

        module  a.b.c          `from .x` -> a.b.x     (bo 'c')
        package a.b/__init__   `from .x` -> a.b.x     (GIU 'b')

    `_module_name()` da bo hau to `__init__`, nen package `a/b/__init__.py`
    co ten "a.b" — phai phan biet hai truong hop nay, neu khong se lech mot cap.
    """
    if not node.level:
        return node.module or ""
    parts = current.split(".")
    drop = node.level - 1 if is_package else node.level
    base = parts[: len(parts) - drop] if drop else parts
    if node.module:
        base = base + node.module.split(".")
    return ".".join(base)


def direct_imports(path: Path, src_root: Path) -> list[ImportEdge]:
    """Import truc tiep cua MOT file, chi giu phan noi bo."""
    current = _module_name(path, src_root)
    is_package = path.name == "__init__.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    edges: list[ImportEdge] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith(INTERNAL_ROOT):
                    edges.append(ImportEdge(current, alias.name, node.lineno))
        elif isinstance(node, ast.ImportFrom):
            mod = _resolve_relative(node, current, is_package)
            if not mod.startswith(INTERNAL_ROOT):
                continue
            edges.append(ImportEdge(current, mod, node.lineno))
            # `from pkg import submodule` — submodule cung la mot canh
            for alias in node.names:
                child = f"{mod}.{alias.name}"
                if _module_path(child, src_root) is not None:
                    edges.append(ImportEdge(current, child, node.lineno))
    return edges


def reachable_modules(
    entry: Path | Iterable[Path], src_root: Path
) -> tuple[set[str], list[ImportEdge]]:
    """Tap module REACHABLE tu `entry`, di BAC CAU.

    Tra ve ca danh sach canh de khi fail con truy duoc DUONG DI cu the.
    """
    entries = [entry] if isinstance(entry, Path) else list(entry)
    files: list[Path] = []
    for e in entries:
        files.extend(sorted(e.rglob("*.py")) if e.is_dir() else [e])

    seen: set[str] = set()
    all_edges: list[ImportEdge] = []
    queue = list(files)

    while queue:
        path = queue.pop()
        mod = _module_name(path, src_root)
        if mod in seen:
            continue
        seen.add(mod)

        for edge in direct_imports(path, src_root):
            all_edges.append(edge)
            if edge.target in seen:
                continue
            nxt = _module_path(edge.target, src_root)
            if nxt is not None:
                queue.append(nxt)

    return seen, all_edges


def _trace(edges: list[ImportEdge], forbidden: str, roots: set[str]) -> list[str]:
    """Dung lai duong di tu mot root toi module bi cam — de bao loi co ich."""
    parents: dict[str, ImportEdge] = {}
    for e in edges:
        parents.setdefault(e.target, e)

    chain: list[str] = [forbidden]
    cur = forbidden
    while cur in parents and cur not in roots:
        e = parents[cur]
        chain.append(f"{e.source}:{e.lineno}")
        if e.source == cur:
            break
        cur = e.source
    return list(reversed(chain))


def assert_cannot_reach(
    *,
    entry: Path | Iterable[Path],
    forbidden: Iterable[str],
    src_root: Path,
    why: str = "",
) -> None:
    """INVARIANT 4 — fail neu `entry` co ĐƯỜNG NÀO toi module bi cam.

    `forbidden` khop theo tien to: cam `a.b` cung cam `a.b.c`.
    """
    seen, edges = reachable_modules(entry, src_root)
    roots = {
        _module_name(p, src_root)
        for p in (
            [entry] if isinstance(entry, Path) else list(entry)
        )
        for p in ([p] if p.is_file() else sorted(p.rglob("*.py")))
    }

    violations: list[str] = []
    for bad in forbidden:
        hits = sorted(m for m in seen if m == bad or m.startswith(bad + "."))
        for hit in hits:
            path = " -> ".join(_trace(edges, hit, roots))
            violations.append(f"  {hit}\n      duong di: {path}")

    if violations:
        raise AssertionError(
            "CAPABILITY BOUNDARY VI PHAM (INVARIANT 4)\n"
            + (f"{why}\n" if why else "")
            + "Module bi cam nhung VAN REACHABLE:\n"
            + "\n".join(violations)
            + "\n\nChu ky ham sach la CHUA DU — day la do thi import."
        )
