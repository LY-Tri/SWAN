#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

import duckdb


def _jsonable_scalar(x: Any) -> Any:
    # Keep it simple; most SWAN DBs are numeric/text.
    if x is None:
        return None
    if isinstance(x, (bool, int, float, str)):
        return x
    # fall back to string repr for timestamps/decimals/etc.
    return str(x)


def _simple_type(duck_type: str) -> str:
    t = (duck_type or "").upper()
    if "BOOL" in t:
        return "BOOLEAN"
    if any(
        k in t
        for k in (
            "INT",
            "HUGEINT",
            "UBIGINT",
            "UINTEGER",
            "USMALLINT",
            "UTINYINT",
            "SMALLINT",
            "TINYINT",
            "BIGINT",
        )
    ):
        return "NUMBER"
    if "DECIMAL" in t or "NUMERIC" in t:
        return "NUMBER"
    if any(k in t for k in ("DOUBLE", "REAL", "FLOAT")):
        return "FLOAT"
    return "TEXT"


def _ddl_type(simple: str) -> str:
    if simple == "BOOLEAN":
        return "BOOLEAN"
    if simple == "FLOAT":
        return "FLOAT"
    if simple == "NUMBER":
        return "NUMBER(38,0)"
    return "VARCHAR(16777216)"


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def export_one(db_id: str, duckdb_path: Path, out_root: Path, *, sample_rows: int) -> None:
    out_dir = out_root / db_id / db_id
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]

        ddl_rows: list[tuple[str, str, str]] = []
        for table in tables:
            desc_rows = con.execute(f"DESCRIBE {_quote_ident(table)}").fetchall()
            col_names = [str(r[0]) for r in desc_rows]
            duck_types = [str(r[1]) for r in desc_rows]
            simple_types = [_simple_type(t) for t in duck_types]

            # DDL.csv row
            ddl_lines = [f"create or replace TABLE {table} ("]
            for i, (cn, st) in enumerate(zip(col_names, simple_types)):
                comma = "," if i < len(col_names) - 1 else ""
                ddl_lines.append(f'\t{_quote_ident(cn)} {_ddl_type(st)}{comma}')
            ddl_lines.append(");")
            ddl = "\n".join(ddl_lines)
            ddl_rows.append((table, "", ddl))

            # Per-table JSON schema
            sample: list[dict[str, Any]] = []
            if sample_rows > 0:
                rows = con.execute(f"SELECT * FROM {_quote_ident(table)} LIMIT {int(sample_rows)}").fetchall()
                for row in rows:
                    sample.append({cn: _jsonable_scalar(v) for cn, v in zip(col_names, row)})

            obj = {
                "table_name": f"{db_id}.{table}",
                "table_fullname": f"{db_id}.{db_id}.{table}",
                "column_names": col_names,
                "column_types": simple_types,
                "description": [None for _ in col_names],
                "sample_rows": sample,
            }
            (out_dir / f"{table}.json").write_text(
                json.dumps(obj, ensure_ascii=False, indent=4) + "\n", encoding="utf-8"
            )

        # Write DDL.csv
        with (out_dir / "DDL.csv").open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["table_name", "description", "DDL"])
            for row in ddl_rows:
                w.writerow(row)
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export schema artifacts (DDL.csv + per-table JSON) from DuckDBs in SWAN/db_eval."
    )
    parser.add_argument(
        "--db-eval-dir",
        default=str(Path(__file__).resolve().parent / "db_eval"),
        help="Directory containing <db_id>.duckdb files (default: SWAN/db_eval).",
    )
    parser.add_argument(
        "--out-root",
        default=str(Path(__file__).resolve().parent / "db_eval"),
        help="Root directory to write schema artifacts under (default: SWAN/db_eval).",
    )
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=5,
        help="Number of sample rows to include per table (default: 5).",
    )
    args = parser.parse_args()

    db_eval_dir = Path(args.db_eval_dir).resolve()
    out_root = Path(args.out_root).resolve()

    duckdb_files = sorted(db_eval_dir.glob("*.duckdb"))
    if not duckdb_files:
        raise SystemExit(f"No .duckdb files found in: {db_eval_dir}")

    for p in duckdb_files:
        db_id = p.stem
        export_one(db_id, p, out_root, sample_rows=int(args.sample_rows))
        print(f"[ok] {db_id} -> {out_root / db_id / db_id}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

