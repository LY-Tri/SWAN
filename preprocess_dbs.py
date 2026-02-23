#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import pickle
import re
import shutil
import sqlite3
import sys
from dataclasses import dataclass
from typing import Any, Iterable, Optional


PAIR_RE = re.compile(
    r"""
    (?:
        (?P<table_bt>[A-Za-z0-9_]+)\.\`(?P<col_bt>[^`]+)\`
      |
        (?P<table>[A-Za-z0-9_]+)\.(?P<col>[A-Za-z0-9_]+)
    )
    """,
    re.VERBOSE,
)


@dataclass
class ColInfo:
    name: str
    decl_type: str
    notnull: bool
    pk: bool


def _iter_pairs(expr: str) -> Iterable[tuple[str, str]]:
    # Handles normal "table.col" and "table.`col with spaces`" cases, and also
    # malformed concatenations in the pickle by extracting all recognizable pairs.
    matches = list(PAIR_RE.finditer(expr))
    if not matches:
        if "." in expr:
            table, col = expr.split(".", 1)
            yield table.strip(), col.strip().strip("`")
        return

    for m in matches:
        table = (m.group("table_bt") or m.group("table") or "").strip()
        col = (m.group("col_bt") or m.group("col") or "").strip()
        if table and col:
            yield table, col


def _list_tables(conn: sqlite3.Connection) -> dict[str, str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    names = [r[0] for r in rows]
    return {n.lower(): n for n in names}


def _table_info(conn: sqlite3.Connection, table: str) -> dict[str, ColInfo]:
    # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
    rows = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    out: dict[str, ColInfo] = {}
    for _, name, typ, notnull, _dflt, pk in rows:
        out[name.lower()] = ColInfo(
            name=name, decl_type=(typ or ""), notnull=bool(notnull), pk=bool(pk)
        )
    return out


def _empty_value(ci: ColInfo) -> Any:
    t = (ci.decl_type or "").upper()
    if any(x in t for x in ("INT", "REAL", "FLOA", "DOUB", "NUM", "DEC", "BOOL")):
        return 0
    return ""


def _unique_cols(conn: sqlite3.Connection, table: str) -> set[str]:
    # Returns lowercased column names that participate in any UNIQUE index.
    out: set[str] = set()
    for _seq, name, unique, *_ in conn.execute(f'PRAGMA index_list("{table}")').fetchall():
        if int(unique) != 1:
            continue
        for _seqno, _cid, colname in conn.execute(f'PRAGMA index_info("{name}")').fetchall():
            if colname:
                out.add(colname.lower())
    return out


def _can_use_rowid(conn: sqlite3.Connection, table: str) -> bool:
    try:
        conn.execute(f'SELECT rowid FROM "{table}" LIMIT 1').fetchone()
        return True
    except Exception:
        return False


def _apply_unique_dummy(
    conn: sqlite3.Connection, table: str, col_info: ColInfo, *, verbose: bool
) -> Optional[str]:
    # For NOT NULL + UNIQUE columns, we can't set a single empty value across all rows.
    # Replace with per-row dummy values derived from rowid.
    if not _can_use_rowid(conn, table):
        return "no rowid available for per-row dummy update"

    t = (col_info.decl_type or "").upper()
    try:
        if any(x in t for x in ("INT", "REAL", "FLOA", "DOUB", "NUM", "DEC", "BOOL")):
            # If it's also an INTEGER PRIMARY KEY aliasing rowid, make it negative to destroy ids.
            expr = "-rowid" if col_info.pk and "INT" in t else "rowid"
            conn.execute(f'UPDATE "{table}" SET "{col_info.name}" = {expr}')
        else:
            conn.execute(
                f'UPDATE "{table}" SET "{col_info.name}" = printf(?, rowid)',
                ("__DROPPED__%d",),
            )
        if verbose:
            print(
                f"[warn] {table}.{col_info.name} is NOT NULL + UNIQUE; set to per-row dummy values",
                file=sys.stderr,
            )
        return None
    except Exception as e:
        return f"{type(e).__name__}: {e}"


def _resolve_table_name(
    table_raw: str, table_map: dict[str, str], aliases: dict[str, str]
) -> Optional[str]:
    t = table_raw.strip().strip('"').strip("`")
    t_norm = aliases.get(t.lower(), t.lower())
    return table_map.get(t_norm)


def _apply_nullify(
    conn: sqlite3.Connection,
    table: str,
    col: str,
    col_info: ColInfo,
    *,
    unique_cols: set[str],
    verbose: bool,
) -> Optional[str]:
    try:
        conn.execute(f'UPDATE "{table}" SET "{col_info.name}" = NULL')
        return None
    except Exception as e:
        if col_info.notnull and (col_info.pk or col_info.name.lower() in unique_cols):
            return _apply_unique_dummy(conn, table, col_info, verbose=verbose)

        # Fall back for NOT NULL / CHECK constraints by using an "empty" value.
        try:
            empty = _empty_value(col_info)
            conn.execute(f'UPDATE "{table}" SET "{col_info.name}" = ?', (empty,))
            if verbose:
                print(
                    f"[warn] {table}.{col} could not be NULL ({type(e).__name__}: {e}); "
                    f"set to {empty!r} instead",
                    file=sys.stderr,
                )
            return None
        except Exception as e2:
            return f"{type(e2).__name__}: {e2}"


def process_db(
    *,
    db_id: str,
    src_db_path: str,
    dst_db_path: str,
    columns: list[str],
    verbose: bool,
) -> int:
    os.makedirs(os.path.dirname(dst_db_path), exist_ok=True)
    shutil.copy2(src_db_path, dst_db_path)

    conn = sqlite3.connect(dst_db_path)
    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        table_map = _list_tables(conn)
        table_aliases = {
            # observed typo in columns_to_drop.pickle
            "fprm": "frpm",
        }

        applied = 0
        failed = 0
        missing = 0

        for ref in columns:
            for table_raw, col_raw in _iter_pairs(ref):
                table = _resolve_table_name(table_raw, table_map, table_aliases)
                if not table:
                    missing += 1
                    if verbose:
                        print(f"[missing-table] {db_id}: {table_raw} (from {ref!r})", file=sys.stderr)
                    continue

                col_map = _table_info(conn, table)
                ci = col_map.get(col_raw.lower())
                if not ci:
                    missing += 1
                    if verbose:
                        print(
                            f"[missing-col] {db_id}: {table}.{col_raw} (from {ref!r})",
                            file=sys.stderr,
                        )
                    continue

                uniq = _unique_cols(conn, table)
                err = _apply_nullify(conn, table, col_raw, ci, unique_cols=uniq, verbose=verbose)
                if err is None:
                    applied += 1
                else:
                    failed += 1
                    print(
                        f"[fail] {db_id}: could not empty {table}.{ci.name} ({ref!r}): {err}",
                        file=sys.stderr,
                    )

        conn.commit()
        if verbose:
            print(
                f"[ok] {db_id}: wrote {dst_db_path} | applied={applied} missing={missing} failed={failed}",
                file=sys.stderr,
            )
        return 0 if failed == 0 else 2
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Create 'beyond-db' SQLite copies by nulling out columns listed in "
            "SWAN/databases/columns_to_drop.pickle."
        )
    )
    parser.add_argument(
        "--pickle",
        default=os.path.join(os.path.dirname(__file__), "databases", "columns_to_drop.pickle"),
        help="Path to columns_to_drop.pickle (default: SWAN/databases/columns_to_drop.pickle).",
    )
    parser.add_argument(
        "--src-root",
        default=os.path.join(os.path.dirname(__file__), "databases", "dev_databases"),
        help="Root containing original DBs (default: SWAN/databases/dev_databases).",
    )
    parser.add_argument(
        "--dst-root",
        default=os.path.join(os.path.dirname(__file__), "databases", "beyond_databases"),
        help="Root to write modified DBs (default: SWAN/databases/beyond_databases).",
    )
    parser.add_argument(
        "--db",
        default="",
        help="If set, process only this DB id (e.g. california_schools).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print missing/failed columns details.",
    )
    args = parser.parse_args()

    cols_to_drop = pickle.load(open(args.pickle, "rb"))
    if not isinstance(cols_to_drop, dict):
        raise SystemExit(f"Unexpected pickle format: {type(cols_to_drop)}")

    db_ids = [args.db] if args.db else list(cols_to_drop.keys())
    exit_code = 0

    for db_id in db_ids:
        if db_id not in cols_to_drop:
            raise SystemExit(f"DB id not in pickle: {db_id}")

        src_db_path = os.path.join(args.src_root, db_id, f"{db_id}.sqlite")
        dst_db_path = os.path.join(args.dst_root, db_id, f"{db_id}.sqlite")

        if not os.path.isfile(src_db_path):
            print(f"[missing-src] {db_id}: {src_db_path}", file=sys.stderr)
            exit_code = max(exit_code, 2)
            continue

        columns = cols_to_drop[db_id]
        if not isinstance(columns, list):
            print(f"[skip] {db_id}: unexpected columns list type {type(columns)}", file=sys.stderr)
            exit_code = max(exit_code, 2)
            continue

        rc = process_db(
            db_id=db_id,
            src_db_path=src_db_path,
            dst_db_path=dst_db_path,
            columns=columns,
            verbose=bool(args.verbose),
        )
        exit_code = max(exit_code, rc)

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

