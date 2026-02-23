#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import os
from decimal import Decimal
from typing import Any


def _require_import(name: str):
    try:
        return __import__(name)
    except Exception as e:
        raise SystemExit(
            f"Missing dependency '{name}'. Install it (e.g. pip install {name}). "
            f"Import error: {type(e).__name__}: {e}"
        )


duckdb = _require_import("duckdb")


def _stable_key(x: Any) -> str:
    return json.dumps(x, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _jsonable_scalar(x: Any) -> Any:
    if x is None:
        return None
    if isinstance(x, (bool, int, float, str)):
        return x
    if isinstance(x, Decimal):
        return float(x)
    if isinstance(x, (bytes, bytearray, memoryview)):
        raw = bytes(x)
        return {"__type__": "bytes", "base64": base64.b64encode(raw).decode("ascii")}
    iso = getattr(x, "isoformat", None)
    if callable(iso):
        try:
            return iso()
        except Exception:
            pass
    return {"__type__": "py_repr", "py_type": type(x).__name__, "repr": repr(x)}


def _jsonable_rows(rows: list[tuple[Any, ...]]) -> list[list[Any]]:
    return [[_jsonable_scalar(v) for v in row] for row in rows]


def _canonicalize_rows(rows: list[list[Any]]) -> list[list[Any]]:
    return sorted(rows, key=_stable_key)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Execute duck_sql from swan/out.jsonl against baseline and dropped-column DuckDBs, "
            "and compare results to the stored answer."
        )
    )
    parser.add_argument(
        "--out-jsonl",
        default=os.path.join("swan", "out.jsonl"),
        help="Path to out.jsonl (default: swan/out.jsonl).",
    )
    parser.add_argument(
        "--duckdb-dir",
        default=os.path.join("swan", "duckdb"),
        help="Directory containing baseline DuckDBs (default: swan/duckdb).",
    )
    parser.add_argument(
        "--duckdb-dropped-dir",
        default=os.path.join("swan", "duckdb_dropped"),
        help="Directory containing dropped-column DuckDBs (default: swan/duckdb_dropped).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="If >0, only process first N JSONL records.",
    )
    args = parser.parse_args()

    out_path = args.out_jsonl
    base_dir = args.duckdb_dir
    drop_dir = args.duckdb_dropped_dir

    base_conns: dict[str, Any] = {}
    drop_conns: dict[str, Any] = {}

    total = 0
    base_ok = 0
    drop_ok = 0
    base_err = 0
    drop_err = 0

    def get_conn(cache: dict[str, Any], path: str):
        c = cache.get(path)
        if c is None:
            c = duckdb.connect(path, read_only=True)
            cache[path] = c
        return c

    try:
        with open(out_path, "r", encoding="utf-8") as f:
            for i, line in enumerate(f, start=1):
                if args.limit and i > args.limit:
                    break
                line = line.strip()
                if not line:
                    continue

                rec = json.loads(line)
                db_id = rec["db"]
                duck_sql = rec["duck_sql"]
                expected = rec.get("answer", [])
                expected_c = _canonicalize_rows(expected if isinstance(expected, list) else [])

                total += 1

                base_path = os.path.join(base_dir, f"{db_id}.duckdb")
                drop_path = os.path.join(drop_dir, f"{db_id}.duckdb")

                # Baseline
                try:
                    c = get_conn(base_conns, base_path)
                    got = c.execute(duck_sql).fetchall()
                    got_c = _canonicalize_rows(_jsonable_rows(got))
                    if got_c == expected_c:
                        base_ok += 1
                    else:
                        # mismatch
                        pass
                except Exception:
                    base_err += 1

                # Dropped-column
                try:
                    c2 = get_conn(drop_conns, drop_path)
                    got2 = c2.execute(duck_sql).fetchall()
                    got2_c = _canonicalize_rows(_jsonable_rows(got2))
                    if got2_c == expected_c:
                        drop_ok += 1
                    else:
                        pass
                except Exception:
                    drop_err += 1

    finally:
        for c in list(base_conns.values()):
            try:
                c.close()
            except Exception:
                pass
        for c in list(drop_conns.values()):
            try:
                c.close()
            except Exception:
                pass

    def pct(x: int, n: int) -> float:
        return (x / n * 100.0) if n else 0.0

    print(f"records: {total}")
    print(f"baseline: {base_ok}/{total} = {pct(base_ok, total):.1f}% (exec_errors={base_err})")
    print(f"dropped:  {drop_ok}/{total} = {pct(drop_ok, total):.1f}% (exec_errors={drop_err})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

