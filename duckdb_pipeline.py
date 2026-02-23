#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import csv
import json
import math
import os
import pickle
import re
import shutil
import sqlite3
import subprocess
import sys
import time
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable


def _require_import(name: str):
    try:
        return __import__(name)
    except Exception as e:
        raise SystemExit(
            f"Missing dependency '{name}'. Install it (e.g. pip install {name}). "
            f"Import error: {type(e).__name__}: {e}"
        )


duckdb = _require_import("duckdb")
sqlglot = _require_import("sqlglot")
sqlite2duckdb = _require_import("sqlite2duckdb")


def _is_sqlite_file(p: Path) -> bool:
    try:
        with p.open("rb") as f:
            return f.read(16) == b"SQLite format 3\x00"
    except Exception:
        return False


def _iter_csv_rows(csv_path: Path) -> Iterable[list[str]]:
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if row:
                yield row


def _safe_unlink(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return


def _convert_sqlite_to_duckdb_with_all_varchar(sqlite_db: Path, duck_db: Path) -> None:
    conn = duckdb.connect(str(duck_db))
    try:
        db_name = conn.sql("SELECT current_database()").fetchone()[0]
        conn.sql("INSTALL sqlite;")
        conn.sql("LOAD sqlite;")
        conn.sql("SET sqlite_all_varchar=true;")

        # Ensure the output database starts clean, even if a previous attempt created tables.
        conn.sql(f"USE {db_name};")
        for (t_existing,) in conn.sql("SHOW TABLES").fetchall():
            t = '"' + str(t_existing).replace('"', '""') + '"'
            conn.sql(f"DROP TABLE IF EXISTS {t};")

        # If a previous run crashed before DETACH, DuckDB can persist the attachment
        # name in the output file. Make this step idempotent.
        try:
            conn.sql("DETACH __other;")
        except Exception:
            pass

        sqlite_lit = str(sqlite_db).replace("'", "''")
        conn.sql(f"ATTACH '{sqlite_lit}' AS __other (TYPE sqlite);")

        conn.sql("USE __other;")
        tables = [i[0] for i in conn.sql("SHOW tables").fetchall()]
        conn.sql(f"USE {db_name};")

        for table in tables:
            t = '"' + str(table).replace('"', '""') + '"'
            conn.sql(f"DROP TABLE IF EXISTS {t};")
            conn.sql(f"CREATE TABLE {t} AS SELECT * FROM __other.{t};")
        conn.sql("DETACH __other;")
    finally:
        conn.close()


def _run_sqlite2duckdb_subprocess(sqlite_db: Path, duck_db: Path) -> None:
    # Running sqlite2duckdb in-process can leave an open/partial DuckDB handle after
    # exceptions, which makes subsequent fallback conversion write to an unlinked DB.
    code = (
        "import sqlite2duckdb\n"
        f"sqlite2duckdb.sqlite_to_duckdb({str(sqlite_db)!r}, {str(duck_db)!r})\n"
    )
    # Capture output to avoid noisy tracebacks on known type-mismatch failures.
    subprocess.run([sys.executable, "-c", code], check=True, capture_output=True, text=True)


def convert_sqlite_to_duckdb(sqlite_db: Path, duck_db: Path, *, force: bool) -> None:
    duck_db.parent.mkdir(parents=True, exist_ok=True)
    if duck_db.exists():
        if not force:
            return
        _safe_unlink(duck_db)

    t0 = time.time()
    try:
        # Known dataset issue: european_football_2 has float-like values in Player.height
        # that can break sqlite2duckdb's type assumptions. Use the robust all-varchar
        # sqlite-extension import path instead.
        if sqlite_db.stem == "european_football_2":
            raise RuntimeError("skip sqlite2duckdb for european_football_2")

        _run_sqlite2duckdb_subprocess(sqlite_db, duck_db)
        # sqlite2duckdb can still abort mid-import; ensure we actually got tables.
        c = duckdb.connect(str(duck_db), read_only=True)
        try:
            n_tables = len(c.execute("SHOW TABLES").fetchall())
        finally:
            c.close()
        if n_tables == 0:
            raise RuntimeError("sqlite2duckdb produced 0 tables")
    except Exception as e:
        # sqlite2duckdb may leave a partially created output.
        _safe_unlink(duck_db)
        _convert_sqlite_to_duckdb_with_all_varchar(sqlite_db, duck_db)
    finally:
        _ = time.time() - t0


def apply_db_fixes(db_id: str, duck_db: Path) -> None:
    # Keep fixes minimal + targeted to remove exec-time type errors.
    conn = duckdb.connect(str(duck_db))
    try:
        if db_id == "california_schools":
            # gold uses BETWEEN 2014 AND 2015, but original values are like '2014-2015'
            try:
                info = conn.execute("PRAGMA table_info('frpm')").fetchall()
                typ = ""
                for _cid, name, t, *_rest in info:
                    if str(name) == "Academic Year":
                        typ = str(t or "")
                        break
                if not typ or "INT" not in typ.upper():
                    conn.execute(
                        'ALTER TABLE frpm ALTER COLUMN "Academic Year" SET DATA TYPE INTEGER '
                        'USING TRY_CAST(SUBSTRING(CAST("Academic Year" AS VARCHAR), 1, 4) AS INTEGER)'
                    )
            except Exception:
                # Best-effort; if schema differs, don't fail the whole pipeline here.
                pass

        if db_id == "european_football_2":
            # common numeric columns end up as VARCHAR if imported with sqlite_all_varchar
            conn.execute(
                'ALTER TABLE "Player" ALTER COLUMN height SET DATA TYPE DOUBLE '
                "USING TRY_CAST(height AS DOUBLE)"
            )
            conn.execute(
                'ALTER TABLE "Player" ALTER COLUMN weight SET DATA TYPE DOUBLE '
                "USING TRY_CAST(weight AS DOUBLE)"
            )
            conn.execute(
                'ALTER TABLE "Match" ALTER COLUMN home_team_goal SET DATA TYPE INTEGER '
                "USING TRY_CAST(home_team_goal AS INTEGER)"
            )
            conn.execute(
                'ALTER TABLE "Match" ALTER COLUMN away_team_goal SET DATA TYPE INTEGER '
                "USING TRY_CAST(away_team_goal AS INTEGER)"
            )
            conn.execute(
                'ALTER TABLE "Player_Attributes" ALTER COLUMN sprint_speed SET DATA TYPE INTEGER '
                "USING TRY_CAST(sprint_speed AS INTEGER)"
            )
            conn.execute(
                'ALTER TABLE "Player_Attributes" ALTER COLUMN heading_accuracy SET DATA TYPE DOUBLE '
                "USING TRY_CAST(heading_accuracy AS DOUBLE)"
            )
            conn.execute(
                'ALTER TABLE "Player_Attributes" ALTER COLUMN finishing SET DATA TYPE DOUBLE '
                "USING TRY_CAST(finishing AS DOUBLE)"
            )
            conn.execute(
                'ALTER TABLE "Player_Attributes" ALTER COLUMN overall_rating SET DATA TYPE DOUBLE '
                "USING TRY_CAST(overall_rating AS DOUBLE)"
            )
            conn.execute(
                'ALTER TABLE "Team_Attributes" ALTER COLUMN buildUpPlayPassing SET DATA TYPE DOUBLE '
                "USING TRY_CAST(buildUpPlayPassing AS DOUBLE)"
            )
    finally:
        conn.close()


def transpile_sql(sqlite_sql: str) -> str:
    out = sqlglot.transpile(sqlite_sql, read="sqlite", write="duckdb", pretty=False)
    return out[0] if out else sqlite_sql


_RE_STRFTIME_YEAR = re.compile(r"STRFTIME\(([^)]*?),\s*'%Y'\)")


def _filter_query_csvs(csv_paths: list[Path]) -> list[Path]:
    # Mirror SWAN/export_gold_answers.py behavior:
    # if *_HybridQueries.csv fully overlaps on executed gold SQLs with *Queries.csv,
    # skip hybrid to avoid duplicated work.
    by_name = {p.name: p for p in csv_paths}
    out: list[Path] = []
    for p in csv_paths:
        name = p.name
        if not name.endswith("_HybridQueries.csv"):
            out.append(p)
            continue
        counterpart = name.replace("_HybridQueries.csv", "Queries.csv")
        if counterpart not in by_name:
            out.append(p)
            continue

        def gold_rows(path: Path) -> list[tuple[str, str]]:
            rows: list[tuple[str, str]] = []
            for row in _iter_csv_rows(path):
                db_id = row[0].strip() if len(row) > 0 else ""
                gold_sql = row[3] if len(row) > 3 else ""
                rows.append((db_id, gold_sql))
            return rows

        if gold_rows(p) == gold_rows(by_name[counterpart]):
            continue
        out.append(p)
    return out


def _dq(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


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


def _canonicalize_jsonable_rows(rows: list[list[Any]]) -> list[list[Any]]:
    # Deterministic ordering for unordered comparisons / stable output.
    return sorted(rows, key=_stable_key)


def _stable_key(x: Any) -> str:
    return json.dumps(x, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _rows_as_counter(rows: list[tuple[Any, ...]]) -> Counter[str]:
    return Counter(_stable_key([_jsonable_scalar(v) for v in row]) for row in rows)


def rewrite_for_duckdb(sql: str) -> str:
    # Keep rewrites minimal and conservative. These cover remaining exec errors we observed.
    sql = sql.replace("DATETIME()", "CURRENT_TIMESTAMP")

    # If query subtracts year strings like STRFTIME(...,'%Y') - STRFTIME(...,'%Y'),
    # cast each year to INT.
    if "STRFTIME" in sql and "%Y" in sql and "-" in sql:
        sql = _RE_STRFTIME_YEAR.sub(r"CAST(STRFTIME(\1, '%Y') AS INTEGER)", sql)

    # If query subtracts current_timestamp - some .birthday (stored as VARCHAR),
    # cast birthday to TIMESTAMP.
    sql = re.sub(
        r"(CURRENT_TIMESTAMP)\s*-\s*([A-Za-z_][A-Za-z0-9_]*\.)?birthday\b",
        r"\\1 - CAST(\\2birthday AS TIMESTAMP)",
        sql,
        flags=re.IGNORECASE,
    )

    # DuckDB is strict about GROUP BY. For two known patterns, wrap the projected
    # non-grouped column with ANY_VALUE to match SQLite's loose grouping behavior.
    sql = re.sub(
        r"\bSELECT\s+(t1\.)player_name\b",
        r"SELECT ANY_VALUE(\\1player_name) AS player_name",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"\bSELECT\s+(teamInfo\.)team_long_name\b",
        r"SELECT ANY_VALUE(\\1team_long_name) AS team_long_name",
        sql,
        flags=re.IGNORECASE,
    )

    return sql


def _num(x: Any) -> float | None:
    if isinstance(x, bool):
        return None
    if isinstance(x, (int, float, Decimal)):
        return float(x)
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        try:
            if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
                return float(int(s))
            return float(s)
        except Exception:
            return None
    return None


def scalar_equal(a: Any, b: Any) -> bool:
    if a is None and b is None:
        return True

    # bytes are encoded in gold answers
    if isinstance(a, dict) and a.get("__type__") == "bytes":
        a = ("bytes", a.get("base64"))
    if isinstance(b, dict) and b.get("__type__") == "bytes":
        b = ("bytes", b.get("base64"))
    if isinstance(a, tuple) or isinstance(b, tuple):
        return a == b

    na = _num(a)
    nb = _num(b)
    if na is not None and nb is not None:
        return math.isclose(na, nb, rel_tol=1e-9, abs_tol=1e-9)

    return a == b


def rows_equal(expected: Any, got: list[tuple[Any, ...]]) -> bool:
    if not isinstance(expected, list):
        return False
    if len(expected) != len(got):
        return False
    for erow, grow in zip(expected, got):
        if not isinstance(erow, list):
            return False
        if len(erow) != len(grow):
            return False
        for ea, ga in zip(erow, list(grow)):
            if not scalar_equal(ea, ga):
                return False
    return True


def rows_equal_unordered(expected: Any, got: list[tuple[Any, ...]]) -> bool:
    if not isinstance(expected, list):
        return False
    exp_rows: list[tuple[Any, ...]] = []
    for r in expected:
        if not isinstance(r, list):
            return False
        exp_rows.append(tuple(r))
    return _rows_as_counter(exp_rows) == _rows_as_counter(got)


def rows_equal_sqlite_duckdb(
    sqlite_rows: list[tuple[Any, ...]],
    duck_rows: list[tuple[Any, ...]],
    *,
    unordered: bool,
) -> bool:
    if not unordered:
        if len(sqlite_rows) != len(duck_rows):
            return False
        for sr, dr in zip(sqlite_rows, duck_rows):
            if len(sr) != len(dr):
                return False
            for a, b in zip(sr, dr):
                if not scalar_equal(a, b):
                    return False
        return True
    return _rows_as_counter(sqlite_rows) == _rows_as_counter(duck_rows)


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


def _iter_pairs(expr: str) -> Iterable[tuple[str, str]]:
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


def _duck_list_tables(conn: duckdb.DuckDBPyConnection) -> dict[str, str]:
    rows = conn.execute("SHOW TABLES").fetchall()
    names = [r[0] for r in rows]
    return {n.lower(): n for n in names}


def _duck_table_info(conn: duckdb.DuckDBPyConnection, table: str) -> dict[str, tuple[str, str, bool, bool]]:
    # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
    rows = conn.execute(f"PRAGMA table_info({_dq(table)})").fetchall()
    out: dict[str, tuple[str, str, bool, bool]] = {}
    for _cid, name, typ, notnull, _dflt, pk in rows:
        out[str(name).lower()] = (str(name), str(typ or ""), bool(notnull), bool(pk))
    return out


def _duck_empty_value(type_s: str) -> Any:
    t = (type_s or "").upper()
    if any(x in t for x in ("INT", "REAL", "FLOA", "DOUB", "NUM", "DEC", "BOOL", "HUGEINT")):
        return 0
    if "DATE" in t:
        return "1970-01-01"
    if "TIME" in t:
        return "1970-01-01 00:00:00"
    return ""


def nullify_columns_in_duckdb(
    *,
    db_id: str,
    src_duckdb: Path,
    dst_duckdb: Path,
    columns: list[str],
    verbose: bool,
) -> int:
    dst_duckdb.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src_duckdb, dst_duckdb)

    conn = duckdb.connect(str(dst_duckdb))
    try:
        table_map = _duck_list_tables(conn)
        table_aliases = {"fprm": "frpm"}  # observed typo in columns_to_drop.pickle

        applied = 0
        failed = 0
        missing = 0

        for ref in columns:
            for table_raw, col_raw in _iter_pairs(ref):
                t = table_raw.strip().strip('"').strip("`")
                t_norm = table_aliases.get(t.lower(), t.lower())
                table = table_map.get(t_norm)
                if not table:
                    missing += 1
                    if verbose:
                        print(f"[missing-table] {db_id}: {table_raw} (from {ref!r})", file=sys.stderr)
                    continue

                col_map = _duck_table_info(conn, table)
                ci = col_map.get(col_raw.lower())
                if not ci:
                    missing += 1
                    if verbose:
                        print(
                            f"[missing-col] {db_id}: {table}.{col_raw} (from {ref!r})",
                            file=sys.stderr,
                        )
                    continue

                col_name, col_type, _notnull, _pk = ci
                try:
                    conn.execute(f"UPDATE {_dq(table)} SET {_dq(col_name)} = NULL")
                    applied += 1
                except Exception as e:
                    try:
                        empty = _duck_empty_value(col_type)
                        conn.execute(f"UPDATE {_dq(table)} SET {_dq(col_name)} = ?", [empty])
                        applied += 1
                        if verbose:
                            print(
                                f"[warn] {table}.{col_name} could not be NULL ({type(e).__name__}: {e}); "
                                f"set to {empty!r} instead",
                                file=sys.stderr,
                            )
                    except Exception as e2:
                        failed += 1
                        print(
                            f"[fail] {db_id}: could not empty {table}.{col_name} ({ref!r}): "
                            f"{type(e2).__name__}: {e2}",
                            file=sys.stderr,
                        )

        if verbose:
            print(
                f"[ok] {db_id}: wrote {dst_duckdb} | applied={applied} missing={missing} failed={failed}",
                file=sys.stderr,
            )
        return 0 if failed == 0 else 2
    finally:
        conn.close()


@dataclass
class EvalStats:
    total: int = 0
    correct: int = 0
    exec_errors: int = 0
    transpile_errors: int = 0
    mismatches: int = 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert SWAN SQLite DBs to DuckDB, transpile gold SQLs, execute, and compute accuracy."
    )
    parser.add_argument(
        "--db-root",
        default=str(Path(__file__).resolve().parent / "databases" / "dev_databases"),
        help="Root directory containing per-db folders (default: SWAN/databases/dev_databases).",
    )
    parser.add_argument(
        "--questions-dir",
        default=str(Path(__file__).resolve().parent / "beyond-database-questions"),
        help="Directory containing *Queries.csv files (default: SWAN/beyond-database-questions).",
    )
    parser.add_argument(
        "--gold-dir",
        default=str(Path(__file__).resolve().parent / "gold_answers"),
        help="Directory containing *_gold.jsonl (default: SWAN/gold_answers).",
    )
    parser.add_argument(
        "--db",
        default="",
        help="If set, only process this DB id (e.g. formula_1).",
    )
    parser.add_argument(
        "--duckdb-out-root",
        default="",
        help=(
            "If set, write/read converted DuckDBs under this root (flat files: <db_id>.duckdb) "
            "instead of next to SQLite under --db-root."
        ),
    )
    parser.add_argument(
        "--rewrite",
        action="store_true",
        help="Apply a few conservative DuckDB-compat rewrites after transpilation.",
    )
    parser.add_argument(
        "--compare-to",
        choices=("gold", "sqlite"),
        default="gold",
        help='Compare DuckDB outputs to "gold" JSONL answers or to live "sqlite" execution (default: gold).',
    )
    parser.add_argument(
        "--unordered",
        action="store_true",
        help="Compare result rows as an unordered multiset (useful when queries lack ORDER BY).",
    )
    parser.add_argument(
        "--out-compatible",
        default="",
        help="If set, write JSONL of question_ids whose DuckDB results match the comparison source.",
    )
    parser.add_argument(
        "--out-broken",
        action="store_true",
        help=(
            "Only meaningful with --drop-columns and --out-compatible. "
            "If set, write only queries that match the original results in baseline DuckDB, "
            "but do NOT match anymore after dropping columns."
        ),
    )
    parser.add_argument(
        "--drop-columns",
        action="store_true",
        help="Create DuckDB copies with columns-to-drop nulled out (per columns_to_drop.pickle).",
    )
    parser.add_argument(
        "--drop-pickle",
        default=str(Path(__file__).resolve().parent / "databases" / "columns_to_drop.pickle"),
        help="Path to columns_to_drop.pickle (default: SWAN/databases/columns_to_drop.pickle).",
    )
    parser.add_argument(
        "--drop-dst-root",
        default=str(Path(__file__).resolve().parent / "databases" / "beyond_databases_duckdb"),
        help="Root to write modified DuckDBs (default: SWAN/databases/beyond_databases_duckdb).",
    )
    parser.add_argument(
        "--verbose-drop",
        action="store_true",
        help="Print missing/failed column details when dropping columns.",
    )
    args = parser.parse_args()

    db_root = Path(args.db_root).resolve()
    questions_dir = Path(args.questions_dir).resolve()
    gold_dir = Path(args.gold_dir).resolve()
    only_db = (args.db or "").strip()
    duckdb_out_root = Path(args.duckdb_out_root).resolve() if args.duckdb_out_root else db_root
    drop_dst_root = Path(args.drop_dst_root).resolve()

    # Load expected answers
    expected: dict[str, Any] = {}
    if args.compare_to == "gold":
        for p in sorted(gold_dir.glob("*_gold.jsonl")):
            with p.open("r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    obj = json.loads(line)
                    expected[obj["question_id"]] = obj["answer"]

    # Collect query CSVs (prefer non-hybrid; match export_gold_answers behavior minimally)
    csv_paths = sorted(questions_dir.glob("*Queries.csv"))
    csv_paths = _filter_query_csvs(list(csv_paths))
    if not csv_paths:
        raise SystemExit(f"No query CSVs found under: {questions_dir}")

    # Default behavior: always convert SQLite -> DuckDB (overwriting outputs),
    # then apply minimal per-DB fixes to reduce exec-time failures.
    sqlite_paths = sorted(
        p
        for p in db_root.rglob("*")
        if p.is_file() and p.suffix.lower() in {".sqlite", ".sqlite3", ".db"} and _is_sqlite_file(p)
    )
    for sqlite_db in sqlite_paths:
        db_id = sqlite_db.parent.name
        if only_db and db_id != only_db:
            continue
        duck_db = duckdb_out_root / f"{db_id}.duckdb"
        convert_sqlite_to_duckdb(sqlite_db, duck_db, force=True)
        apply_db_fixes(db_id, duck_db)

    out_compatible = None
    if args.out_compatible:
        out_compatible = open(args.out_compatible, "w", encoding="utf-8", newline="\n")
    if args.out_broken:
        if not args.drop_columns:
            raise SystemExit("--out-broken requires --drop-columns")
        if not out_compatible:
            raise SystemExit("--out-broken requires --out-compatible")

    def _report(label: str, duckdb_root: Path, by_db: dict[str, EvalStats]) -> None:
        db_ids = sorted(by_db.keys())
        all_total = sum(by_db[k].total for k in db_ids)
        all_correct = sum(by_db[k].correct for k in db_ids)
        all_exec = sum(by_db[k].exec_errors for k in db_ids)
        all_mismatch = sum(by_db[k].mismatches for k in db_ids)
        all_transpile = sum(by_db[k].transpile_errors for k in db_ids)

        print(f"\n{label}")
        print(f"Evaluated DuckDB root: {duckdb_root}")
        print("Per-DB results:")
        for db_id in db_ids:
            s = by_db[db_id]
            acc = (s.correct / s.total * 100.0) if s.total else 0.0
            print(
                f"- {db_id}: {s.correct}/{s.total} = {acc:.1f}% "
                f"(exec_errors={s.exec_errors}, mismatches={s.mismatches}, transpile_errors={s.transpile_errors})"
            )

        if all_total:
            print("\nOverall:")
            print(f"- accuracy: {all_correct}/{all_total} = {all_correct / all_total * 100.0:.1f}%")
            print(f"- exec_errors: {all_exec}")
            print(f"- mismatches: {all_mismatch}")
            print(f"- transpile_errors: {all_transpile}")
        else:
            print("\nNo evaluated questions.")

    def _evaluate(
        duckdb_root: Path,
        *,
        restrict_to: set[tuple[str, str]] | None,
        record_compatible: bool,
        capture_records: bool,
    ) -> tuple[dict[str, EvalStats], set[tuple[str, str]], dict[tuple[str, str], dict[str, Any]]]:
        by_db: dict[str, EvalStats] = {}
        compatible: set[tuple[str, str]] = set()
        compatible_records: dict[tuple[str, str], dict[str, Any]] = {}

        for csv_path in csv_paths:
            base = csv_path.stem
            for row_idx, row in enumerate(_iter_csv_rows(csv_path), start=1):
                if len(row) < 4:
                    continue
                db_id = row[0].strip()
                gold_sql = row[3]
                if not db_id or not gold_sql:
                    continue
                if only_db and db_id != only_db:
                    continue

                qid = f"{base}:{row_idx}"
                key = (db_id, qid)
                if restrict_to is not None and key not in restrict_to:
                    continue

                sqlite_rows: list[tuple[Any, ...]] | None = None
                exp = None
                if args.compare_to == "gold":
                    exp = expected.get(qid)
                    if exp is None:
                        # Skip unknown qids (don't count toward totals).
                        continue
                else:
                    sqlite_db_path = db_root / db_id / f"{db_id}.sqlite"
                    stats = by_db.setdefault(db_id, EvalStats())
                    stats.total += 1
                    if not sqlite_db_path.is_file():
                        stats.exec_errors += 1
                        continue
                    try:
                        sconn = sqlite3.connect(str(sqlite_db_path))
                        try:
                            sqlite_rows = sconn.execute(gold_sql).fetchall()
                        finally:
                            sconn.close()
                    except Exception:
                        stats.exec_errors += 1
                        continue

                stats = by_db.setdefault(db_id, EvalStats())
                if args.compare_to == "gold":
                    stats.total += 1

                try:
                    duck_sql = transpile_sql(gold_sql)
                except Exception:
                    stats.transpile_errors += 1
                    stats.exec_errors += 1
                    continue

                if args.rewrite:
                    duck_sql = rewrite_for_duckdb(duck_sql)

                duck_db_path = duckdb_root / f"{db_id}.duckdb"
                if not duck_db_path.is_file():
                    stats.exec_errors += 1
                    continue

                conn = duckdb.connect(str(duck_db_path), read_only=True)
                try:
                    got = conn.execute(duck_sql).fetchall()
                except Exception:
                    stats.exec_errors += 1
                    continue
                finally:
                    conn.close()

                ok = False
                if args.compare_to == "gold":
                    ok = rows_equal_unordered(exp, got) if args.unordered else rows_equal(exp, got)
                else:
                    ok = rows_equal_sqlite_duckdb(sqlite_rows or [], got, unordered=bool(args.unordered))

                if ok:
                    stats.correct += 1
                    if record_compatible:
                        compatible.add(key)
                        if out_compatible is not None:
                            rec = {
                                "question_id": qid,
                                "db": db_id,
                                "gold_sql": gold_sql,
                                "duck_sql": duck_sql,
                                "answer": _canonicalize_jsonable_rows(_jsonable_rows(got)),
                            }
                            if args.out_broken:
                                if capture_records:
                                    compatible_records[key] = rec
                            else:
                                out_compatible.write(json.dumps(rec, ensure_ascii=False) + "\n")
                else:
                    stats.mismatches += 1

        return by_db, compatible, compatible_records

    # Phase 1: baseline DuckDB evaluation (also collects compatible subset)
    baseline_by_db, compatible, baseline_records = _evaluate(
        duckdb_out_root,
        restrict_to=None,
        record_compatible=bool(args.drop_columns or out_compatible is not None),
        capture_records=bool(args.out_broken),
    )
    _report("Baseline (no dropped columns)", duckdb_out_root, baseline_by_db)

    # Phase 2: drop columns and re-evaluate only on the baseline-compatible subset
    if args.drop_columns:
        cols_to_drop = pickle.load(open(args.drop_pickle, "rb"))
        if not isinstance(cols_to_drop, dict):
            raise SystemExit(f"Unexpected pickle format: {type(cols_to_drop)}")

        db_ids = [only_db] if only_db else sorted(cols_to_drop.keys())
        for db_id in db_ids:
            if db_id not in cols_to_drop:
                raise SystemExit(f"DB id not in pickle: {db_id}")
            cols = cols_to_drop[db_id]
            if not isinstance(cols, list):
                raise SystemExit(f"Unexpected pickle columns type for {db_id}: {type(cols)}")

            src_duck = duckdb_out_root / f"{db_id}.duckdb"
            dst_duck = drop_dst_root / f"{db_id}.duckdb"
            if not src_duck.is_file():
                print(f"[missing-src] {db_id}: {src_duck}", file=sys.stderr)
                continue

            _ = nullify_columns_in_duckdb(
                db_id=db_id,
                src_duckdb=src_duck,
                dst_duckdb=dst_duck,
                columns=cols,
                verbose=bool(args.verbose_drop),
            )

        dropped_by_db, _still_compatible, _ = _evaluate(
            drop_dst_root,
            restrict_to=compatible,
            record_compatible=False,
            capture_records=False,
        )
        print(f"\nBaseline-compatible subset size: {len(compatible)}")
        _report("After dropping columns (evaluated only on baseline-compatible subset)", drop_dst_root, dropped_by_db)

        if args.out_broken and out_compatible is not None:
            # Re-evaluate on dropped DBs to find which compatible keys remain correct.
            _, still_ok, _ = _evaluate(
                drop_dst_root,
                restrict_to=compatible,
                record_compatible=True,
                capture_records=False,
            )
            broken = compatible - still_ok
            for key in sorted(broken):
                rec = baseline_records.get(key)
                if rec is not None:
                    out_compatible.write(json.dumps(rec, ensure_ascii=False) + "\n")

    if out_compatible is not None:
        out_compatible.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
