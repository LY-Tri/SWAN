# DuckDB Conversion Pipeline

This document explains what `SWAN/duckdb_pipeline.py` currently does and how to run it.

Before running the script, make sure to unzip the `SWAN/databases/dev_databases.zip` file. This will create the `SWAN/databases/dev_databases` directory.

## CLI arguments (all)

These are the current CLI flags (from `python SWAN/duckdb_pipeline.py --help`):

- **`-h, --help`**: show usage/help and exit.
- **`--db-root`**: root directory containing per-db folders with SQLite files (default: `SWAN/databases/dev_databases`).
- **`--questions-dir`**: directory containing `*Queries.csv` files (default: `SWAN/beyond-database-questions`).
- **`--gold-dir`**: directory containing `*_gold.jsonl` reference answers (default: `SWAN/gold_answers`). Only used when `--compare-to gold`.
- **`--db`**: if set, only process this database id (e.g. `formula_1`).
- **`--duckdb-out-root`**: directory where converted DuckDB files are written/read (flat files: `<db_id>.duckdb`). If not set, defaults to `--db-root`.
- **`--rewrite`**: apply a few conservative DuckDB-compat rewrites after transpilation.
- **`--compare-to {gold,sqlite}`**:
  - `sqlite`: compare DuckDB results to live SQLite execution of the gold SQL.
  - `gold`: compare DuckDB results to JSONL answers under `--gold-dir`.
- **`--unordered`**: compare result rows as an unordered multiset (useful when queries lack `ORDER BY`).
- **`--out-compatible`**: if set, write JSONL records for baseline-compatible queries (see “Output file” below).
- **`--out-broken`**: only meaningful with `--drop-columns` and `--out-compatible`. If set, write only baseline-compatible queries that **break** after dropping columns.
- **`--drop-columns`**: create dropped-column DuckDB copies and run post-drop evaluation.
- **`--drop-pickle`**: path to `columns_to_drop.pickle` (default: `SWAN/databases/columns_to_drop.pickle`).
- **`--drop-dst-root`**: directory where dropped-column DuckDB files are written (flat files: `<db_id>.duckdb`).
- **`--verbose-drop`**: print missing/failed column details while nulling columns during `--drop-columns`.

## What it’s for

The script supports evaluating “LLM + DB query” settings where:

- You start with **SQLite** databases under `SWAN/databases/dev_databases/<db_id>/<db_id>.sqlite`
- You want to run the **gold SQL** queries from `SWAN/beyond-database-questions/*Queries.csv`
- Your agent/runtime only supports **DuckDB**, so you need **SQLite → DuckDB** conversion
- You optionally create “beyond” databases by **nulling out** specific columns listed in `SWAN/databases/columns_to_drop.pickle`
- You measure how many queries’ execution results still match the original results after conversion and/or after dropping columns

## High-level pipeline

### 1) Load query set (CSV)

- Reads `*Queries.csv` under `--questions-dir` (default: `SWAN/beyond-database-questions`)
- De-duplicates: if `*_HybridQueries.csv` is fully overlapping (same `(db_id, gold_sql)` pairs) with `*Queries.csv`, the hybrid file is skipped (mirrors `export_gold_answers.py` behavior).

### 2) Convert SQLite → DuckDB (always runs)

For each SQLite DB under `--db-root` (default: `SWAN/databases/dev_databases`):

- Writes a DuckDB file named **`<db_id>.duckdb`** under `--duckdb-out-root` (flat directory).
  - If `--duckdb-out-root` is not set, it defaults to `--db-root`.
- Conversion always **overwrites** the output DuckDB.
- Conversion strategy:
  - First tries `sqlite2duckdb` in a **subprocess**.
  - If it fails or produces an unusable output, falls back to DuckDB’s sqlite extension:
    - `INSTALL sqlite; LOAD sqlite; SET sqlite_all_varchar=true;`
    - `ATTACH ... (TYPE sqlite)` and `CREATE TABLE ... AS SELECT ...`
  - `european_football_2` is forced onto the sqlite-extension fallback to avoid known type mismatch issues (e.g. `Player.height` with values like `182.88`).

### 3) Apply per-DB “fixups” (always runs)

After conversion, `apply_db_fixes(db_id, duck_db_path)` runs best-effort, targeted fixes to reduce execution-time errors:

- `california_schools`: makes `frpm."Academic Year"` consistently usable as an integer year (type-safe; skips if already integer).
- `european_football_2`: casts common numeric columns to numeric types using `TRY_CAST` (e.g. `Player.height/weight`, goals, etc.).

These fixups are intended to be minimal and practical (avoid execution errors), not to be a perfect semantic reimplementation of SQLite behavior.

### 4) Evaluate baseline DuckDB vs reference results

For each query row:

- Transpiles gold SQL from SQLite dialect to DuckDB using `sqlglot`.
- Optionally applies conservative string rewrites (`--rewrite`).
- Executes the transpiled SQL on the baseline DuckDB (`--duckdb-out-root/<db_id>.duckdb`).
- Compares to a reference, controlled by `--compare-to`:
  - `sqlite`: run the **original** gold SQL on the SQLite DB and compare results
  - `gold`: compare against precomputed JSONL answers under `--gold-dir`
- If `--unordered` is set: compares results as an **unordered multiset** of rows (useful for queries without `ORDER BY`).

The script reports per-DB and overall accuracy, plus counts for `exec_errors`, `mismatches`, and `transpile_errors`.

### 5) (Optional) Drop columns in DuckDB and re-evaluate only on compatible subset

If `--drop-columns` is set:

1) The script builds a **baseline-compatible subset**: queries that matched the reference in the baseline run.
2) It creates dropped-column DuckDB copies:
   - Reads the drop spec from `--drop-pickle` (default: `SWAN/databases/columns_to_drop.pickle`)
   - Writes modified DuckDBs to `--drop-dst-root/<db_id>.duckdb` (flat directory)
   - “Dropping” here means **nulling out** the specified columns (or falling back to an “empty” value when `NULL` is not allowed).
3) It re-runs evaluation against the dropped-column DuckDBs, but **only** on the baseline-compatible subset.

The “after drop” accuracy is therefore computed as:

- denominator = `#(baseline compatible queries)`
- numerator = `#(still match after drop)`

This matches the goal “only judge post-drop behavior on queries that were correct pre-drop”.

## Output file: `--out-compatible`

If `--out-compatible path.jsonl` is set, the script can write JSONL records for baseline-compatible queries.

Each line currently includes:

- `question_id`
- `db`
- `gold_sql`
- `duck_sql`
- `answer`: DuckDB execution output (JSON-serializable) with a deterministic ordering (canonically sorted)

### Only save “broken after drop”

If you set **both**:

- `--drop-columns`
- `--out-compatible ...`
- `--out-broken`

…then the output file contains only queries that:

- matched the reference in the baseline run **but**
- no longer match after dropping columns

The saved `answer` is the **baseline DuckDB answer**.

## Common commands

### Baseline conversion + evaluation (vs SQLite)

```bash
python SWAN/duckdb_pipeline.py \
  --db-root SWAN/databases/dev_databases \
  --duckdb-out-root ./swan/duckdb \
  --compare-to sqlite --rewrite --unordered
```

### Drop columns and evaluate on baseline-compatible subset

```bash
python SWAN/duckdb_pipeline.py \
  --db-root SWAN/databases/dev_databases \
  --duckdb-out-root ./swan/duckdb \
  --drop-columns --drop-dst-root ./swan/duckdb_dropped \
  --compare-to sqlite --rewrite --unordered
```

### Save only queries that “break” after dropping columns

```bash
python SWAN/duckdb_pipeline.py \
  --db-root SWAN/databases/dev_databases \
  --duckdb-out-root ./swan/duckdb \
  --drop-columns --drop-dst-root ./swan/duckdb_dropped \
  --compare-to sqlite --rewrite --unordered \
  --out-compatible ./swan/out.jsonl \
  --out-broken
```

## Notes / gotchas

- Conversion always overwrites `--duckdb-out-root/<db_id>.duckdb`.
- `--unordered` changes the correctness criterion (order-insensitive). The saved `answer` is sorted deterministically for stability.
- DuckDB is stricter than SQLite for some SQL semantics (e.g. GROUP BY). `--rewrite` includes a couple of targeted rewrites to reduce common failures.

