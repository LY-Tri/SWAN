#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import csv
import glob
import json
import os
import sqlite3
import sys
from dataclasses import dataclass
from typing import Any, Iterable, Optional


@dataclass(frozen=True)
class ExecOutcome:
    ok: bool
    result: Optional[list[list[Any]]] = None
    error: Optional[str] = None


def _jsonable_scalar(x: Any) -> Any:
    if x is None:
        return None
    if isinstance(x, (bool, int, float, str)):
        return x
    if isinstance(x, (bytes, bytearray, memoryview)):
        raw = bytes(x)
        return {"__type__": "bytes", "base64": base64.b64encode(raw).decode("ascii")}
    return {"__type__": "py_repr", "py_type": type(x).__name__, "repr": repr(x)}


def _jsonable_rows(rows: Iterable[tuple[Any, ...]]) -> list[list[Any]]:
    return [[_jsonable_scalar(v) for v in row] for row in rows]


def execute_sqlite(db_path: str, sql: str, timeout_s: float) -> ExecOutcome:
    try:
        conn = sqlite3.connect(db_path, timeout=timeout_s, uri=True)
        cur = conn.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            return ExecOutcome(ok=True, result=_jsonable_rows(rows))
        finally:
            try:
                cur.close()
            finally:
                conn.close()
    except Exception as e:
        return ExecOutcome(ok=False, error=f"{type(e).__name__}: {e}")


def iter_csv_rows(csv_path: str) -> Iterable[list[str]]:
    # These CSVs include quoted multi-line SQL; csv.reader handles that correctly
    # when opened with newline="".
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            yield row


def default_db_path(db_root: str, db_id: str) -> str:
    return os.path.join(db_root, db_id, f"{db_id}.sqlite")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Execute SWAN gold SQLs against their corresponding SQLite databases and save outputs."
        )
    )
    parser.add_argument(
        "--questions-dir",
        default=os.path.join(os.path.dirname(__file__), "beyond-database-questions"),
        help="Directory containing *Queries.csv files (default: SWAN/beyond-database-questions).",
    )
    parser.add_argument(
        "--db-root",
        default=os.path.join(os.path.dirname(__file__), "databases", "dev_databases"),
        help="Root directory containing per-db folders (default: SWAN/databases/dev_databases).",
    )
    parser.add_argument(
        "--pattern",
        default="*Queries.csv",
        help='Glob pattern under questions-dir (default: "*Queries.csv").',
    )
    parser.add_argument(
        "--out-dir",
        default=os.path.join(os.path.dirname(__file__), "gold_answers"),
        help="Output directory for JSONL files (default: SWAN/gold_answers).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="If >0, only process first N rows per CSV.",
    )
    parser.add_argument(
        "--timeout-s",
        type=float,
        default=30.0,
        help="SQLite connection timeout in seconds (default: 30).",
    )
    parser.add_argument(
        "--skip-missing-db",
        action="store_true",
        help="If set, rows with missing DB files are written as errors (and processing continues).",
    )
    parser.add_argument(
        "--allow-errors",
        action="store_true",
        help=(
            "If set, SQL execution errors are written into the answer field; otherwise the script exits."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse inputs and report counts without executing SQL.",
    )
    args = parser.parse_args()

    questions_dir = os.path.abspath(args.questions_dir)
    db_root = os.path.abspath(args.db_root)
    out_dir = os.path.abspath(args.out_dir)

    csv_paths = sorted(glob.glob(os.path.join(questions_dir, args.pattern)))
    if not csv_paths:
        raise SystemExit(f"No files matched: {os.path.join(questions_dir, args.pattern)}")

    # If *Queries.csv and *_HybridQueries.csv are fully overlapping w.r.t. the gold SQLs
    # we execute (db_id + row[3]), keep only *Queries.csv to avoid duplication.
    by_name = {os.path.basename(p): p for p in csv_paths}
    filtered: list[str] = []
    for p in csv_paths:
        name = os.path.basename(p)
        if not name.endswith("_HybridQueries.csv"):
            filtered.append(p)
            continue
        counterpart = name.replace("_HybridQueries.csv", "Queries.csv")
        if counterpart not in by_name:
            filtered.append(p)
            continue

        def gold_rows(path: str) -> list[tuple[str, str]]:
            rows: list[tuple[str, str]] = []
            for row in iter_csv_rows(path):
                db_id = row[0].strip() if len(row) > 0 else ""
                gold_sql = row[3] if len(row) > 3 else ""
                rows.append((db_id, gold_sql))
            return rows

        if gold_rows(p) == gold_rows(by_name[counterpart]):
            # skip hybrid: fully overlaps on executed gold SQLs
            continue
        filtered.append(p)

    csv_paths = filtered

    os.makedirs(out_dir, exist_ok=True)

    # Cache gold SQL outputs across all CSVs in this run so we only execute each
    # (db_path, gold_sql) once (e.g., appears in both *Queries and *HybridQueries).
    gold_cache: dict[tuple[str, str], ExecOutcome] = {}

    for csv_path in csv_paths:
        base = os.path.splitext(os.path.basename(csv_path))[0]
        stem = base
        if stem.endswith("_HybridQueries"):
            stem = stem[: -len("_HybridQueries")]
        if stem.endswith("Queries"):
            stem = stem[: -len("Queries")]
        out_path = os.path.join(out_dir, f"{stem}_gold.jsonl")

        processed = 0
        written = 0
        missing_db = 0
        with open(out_path, "w", encoding="utf-8", newline="\n") as out:
            for row_idx, row in enumerate(iter_csv_rows(csv_path), start=1):
                if args.limit and row_idx > args.limit:
                    break
                processed = row_idx

                # Convention in these files:
                # row[0]=db_id, row[1]=question, row[3]=gold_sql
                db_id = row[0].strip() if len(row) > 0 else ""
                question = row[1] if len(row) > 1 else ""
                hint = row[2] if len(row) > 2 else ""
                gold_sql = row[3] if len(row) > 3 else ""

                if not db_id or not gold_sql:
                    raise SystemExit(
                        f"Malformed row in {os.path.basename(csv_path)} at line {row_idx}: "
                        "missing db_id and/or gold_sql (expected columns 0 and 3)"
                    )

                db_path = default_db_path(db_root, db_id)

                if not os.path.isfile(db_path):
                    msg = f"missing database file: {db_path}"
                    if args.skip_missing_db or args.dry_run:
                        missing_db += 1
                        print(
                            f"[missing-db] {db_id} {base}:{row_idx} -> {db_path}",
                            file=sys.stderr,
                        )
                        out.write(
                            json.dumps(
                                {
                                    "db": db_id,
                                    "question_id": f"{base}:{row_idx}",
                                    "question": question,
                                    "hint": hint,
                                    "answer": {"__error__": msg},
                                },
                                ensure_ascii=False,
                            )
                            + "\n"
                        )
                        written += 1
                        continue
                    raise SystemExit(msg)

                if args.dry_run:
                    out.write(
                        json.dumps(
                            {
                                "db": db_id,
                                "question_id": f"{base}:{row_idx}",
                                "question": question,
                                "hint": hint,
                                "answer": None,
                            },
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
                    written += 1
                    continue

                cache_key = (db_path, gold_sql)
                if cache_key in gold_cache:
                    outcome = gold_cache[cache_key]
                else:
                    outcome = execute_sqlite(db_path=db_path, sql=gold_sql, timeout_s=args.timeout_s)
                    gold_cache[cache_key] = outcome
                if not outcome.ok:
                    if not args.allow_errors:
                        raise SystemExit(
                            f"SQL execution failed for {db_id} ({os.path.basename(csv_path)}:{row_idx}): "
                            f"{outcome.error}"
                        )
                    answer: Any = {"__error__": outcome.error}
                else:
                    answer = outcome.result

                out.write(
                    json.dumps(
                        {
                            "db": db_id,
                            "question_id": f"{base}:{row_idx}",
                            "question": question,
                            "hint": hint,
                            "answer": answer,
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
                written += 1

        print(f"{os.path.basename(csv_path)}: processed={processed}, wrote={written}, missing_db={missing_db} -> {out_path}")

    print(f"Done. Outputs in: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

