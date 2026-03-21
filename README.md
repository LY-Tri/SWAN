# SWAN
## databases Folder
It contains a zip file of the four curated databases. 

It also contains the keys for each table and the columns that should be not be used.

## beyond-database-questions Folder
It contains the 120 beyond database questions used in SWAN.

To answer beyond-database questions, we include two baseline solutions in SWAN, HQDL and Hybrid Query (based on BlendSQL).

## Hybrid Query over Database and LLM
In HQDL, if a question cannot be answered based on the current database schema, then one may modify the database schema (add new columns/tables) to answer the questions. 
Hence, after modifying the schema (currently a manual step), one can use LLMs to fills in the missing values.
This approach is simple and requiers no modification to the query optimizer.
- **HQDL** - This folder contains HQDL.ipynb, the pipeline code used to evaluate HQDL on SWAN. It also contains the inputs for and the outputs from the OpemAI models (via the Batch API).

## Hybrid Query (via User Defined Function)
Many companies and opensource communties have supported LLM calls directly within the SQL query.
We evaluate [BlendSQL](https://github.com/parkervg/blendsql) on our SWAN benchmark.
- **HybridQuery** - This folder contains code for running BlendSQL on SWAN. It also contains the logs for our experiments on hybrid query.

---

## DuckDB Pipeline (`duckdb_pipeline.py`)

Converts the SWAN SQLite databases to DuckDB, transpiles gold SQL queries from SQLite dialect to DuckDB dialect, executes them, and reports per-DB and overall accuracy.

**Dependencies:** `duckdb`, `sqlglot`, `sqlite2duckdb`

### Basic usage

```bash
python duckdb_pipeline.py
```

By default, this looks for SQLite databases under `databases/dev_databases/`, query CSVs under `beyond-database-questions/`, and gold answers under `gold_answers/`.

### Key options

| Flag | Default | Description |
|---|---|---|
| `--db-root DIR` | `databases/dev_databases` | Root containing per-DB folders with `.sqlite` files |
| `--questions-dir DIR` | `beyond-database-questions` | Directory containing `*Queries.csv` files |
| `--gold-dir DIR` | `gold_answers` | Directory containing `*_gold.jsonl` expected answers |
| `--db DB_ID` | *(all)* | Process only a single database (e.g. `formula_1`) |
| `--duckdb-out-root DIR` | same as `--db-root` | Write/read converted `.duckdb` files to a separate directory |
| `--compare-to {gold,sqlite}` | `gold` | Compare DuckDB output against gold JSONL or live SQLite execution |
| `--rewrite` | off | Apply conservative DuckDB-compatibility rewrites after transpilation |
| `--unordered` | off | Compare result rows as an unordered multiset |
| `--out-compatible FILE` | *(none)* | Write JSONL of questions whose DuckDB results match the comparison source |
| `--print-baseline-errors` | off | Print transpile/exec errors to stderr for inspection |

### Column-dropping options

Used to create modified database copies where certain columns are hidden (nullified or dropped), in order to measure how well queries depend on those columns.

| Flag | Default | Description |
|---|---|---|
| `--drop-columns` | off | Enable the column-dropping phase |
| `--drop-behavior {nullify,drop}` | `nullify` | `nullify` sets values to NULL; `drop` removes the column entirely |
| `--drop-pickle FILE` | `databases/columns_to_drop.pickle` | Pickle file mapping `db_id -> [column_refs]` |
| `--drop-dst-root DIR` | `databases/beyond_databases_duckdb` | Directory to write modified DuckDB files |
| `--out-broken` | off | With `--drop-columns --out-compatible`, write only questions that break after dropping columns |
| `--verbose-drop` | off | Print missing/failed column details during the drop phase |

### Examples

```bash
# Evaluate a single database against gold answers
python duckdb_pipeline.py --db california_schools

# Evaluate all DBs, comparing to live SQLite execution
python duckdb_pipeline.py --compare-to sqlite

# Measure query breakage after nullifying hidden columns
python duckdb_pipeline.py \
  --drop-columns \
  --drop-behavior nullify \
  --out-compatible broken.jsonl \
  --out-broken

# Write converted DuckDBs to a separate directory
python duckdb_pipeline.py --duckdb-out-root /tmp/swan_duckdbs
```

---

## DuckDB Schema Exporter (`duckdb_export_schema.py`)

Exports schema artifacts from a directory of `.duckdb` files. For each database it writes:
- **`DDL.csv`** — a CSV with columns `table_name`, `description`, `DDL` containing a `CREATE OR REPLACE TABLE` statement for each table.
- **`<table>.json`** — a JSON file per table with column names, simplified types, and optional sample rows.

Output is written under `<out>/<db_id>/<db_id>/`.

**Dependencies:** `duckdb`

### Usage

```bash
python duckdb_export_schema.py --db <duckdb-dir> --out <output-dir> [--sample-rows N]
```

| Flag | Required | Default | Description |
|---|---|---|---|
| `--db DIR` | yes | — | Directory containing `.duckdb` files (searched recursively) |
| `--out DIR` | yes | — | Root directory to write schema artifacts under |
| `--sample-rows N` | no | `5` | Number of sample rows to include per table in the JSON files; set to `0` to omit samples |

### Examples

```bash
# Export schema for all DuckDB files in a directory
python duckdb_export_schema.py \
  --db databases/duckdb \
  --out schema_export

# Export without sample rows
python duckdb_export_schema.py \
  --db databases/duckdb \
  --out schema_export \
  --sample-rows 0
```

