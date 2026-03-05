"""
demo_run.py
===========
Self-contained ETL demo — no internet or SQL Server required.

Generates a realistic Sales Excel dataset locally, then runs the full
Extract → Schema Inference → DDL Generation → Transform → Load pipeline
against a local SQLite database, printing a formatted summary at each stage.

Usage:
    python demo_run.py

GitHub  : https://github.com/kage77/python-etl-pipeline
License : MIT
"""

import sqlite3
import re
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import numpy as np

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)-8s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

BANNER = "━" * 64


# ─────────────────────────────────────────────────────────────────────────────
# STEP 0 — Generate sample dataset
# ─────────────────────────────────────────────────────────────────────────────
def generate_sample_excel(path: Path, n_rows: int = 250) -> Path:
    """
    Create a realistic Sales Excel file with mixed column types:
    dates, integers, floats, strings, booleans, and intentional nulls.
    """
    np.random.seed(42)

    regions    = ["West", "East", "Central", "South"]
    categories = ["Technology", "Furniture", "Office Supplies"]
    sub_cats   = {
        "Technology":      ["Phones", "Laptops", "Accessories", "Copiers"],
        "Furniture":       ["Chairs", "Tables", "Bookcases", "Furnishings"],
        "Office Supplies": ["Paper", "Binders", "Pens", "Labels"],
    }
    segments   = ["Consumer", "Corporate", "Home Office"]
    ship_modes = ["Standard Class", "Second Class", "First Class", "Same Day"]
    states     = ["California", "Texas", "New York", "Florida", "Washington",
                  "Illinois", "Ohio", "Pennsylvania", "Georgia", "Arizona"]

    base_date   = datetime(2023, 1, 1)
    order_dates = [base_date + timedelta(days=int(d)) for d in np.random.randint(0, 365, n_rows)]
    ship_dates  = [od + timedelta(days=int(d)) for od, d in
                   zip(order_dates, np.random.randint(1, 8, n_rows))]

    categories_col = np.random.choice(categories, n_rows)
    sub_cat_col    = [np.random.choice(sub_cats[c]) for c in categories_col]
    sales          = np.round(np.random.uniform(10, 5000, n_rows), 2)
    quantity       = np.random.randint(1, 15, n_rows)
    discount       = np.round(np.random.choice([0.0, 0.1, 0.2, 0.3, 0.4], n_rows), 2)
    profit         = np.round(sales * (0.15 - discount + np.random.normal(0, 0.05, n_rows)), 2)

    df = pd.DataFrame({
        "Order ID":      [f"CA-2023-{100000 + i}" for i in range(n_rows)],
        "Order Date":    order_dates,
        "Ship Date":     ship_dates,
        "Ship Mode":     np.random.choice(ship_modes, n_rows),
        "Customer Name": [f"Customer {i:04d}" for i in np.random.randint(1, 80, n_rows)],
        "Segment":       np.random.choice(segments, n_rows),
        "State":         np.random.choice(states, n_rows),
        "Region":        np.random.choice(regions, n_rows),
        "Product ID":    [f"TEC-{np.random.randint(1000,9999)}-{np.random.randint(10000,99999)}"
                          for _ in range(n_rows)],
        "Category":      categories_col,
        "Sub-Category":  sub_cat_col,
        "Sales":         sales,
        "Quantity":      quantity,
        "Discount":      discount,
        "Profit":        profit,
        "Returned?":     np.random.choice([True, False], n_rows, p=[0.05, 0.95]),
    })

    # Introduce realistic nulls
    df.loc[np.random.random(n_rows) < 0.03, "Profit"]        = np.nan
    df.loc[np.random.random(n_rows) < 0.01, "Customer Name"] = np.nan

    df.to_excel(path, index=False, engine="openpyxl")
    log.info("Sample dataset generated: %s  (%d rows × %d cols)", path.name, *df.shape)
    return path


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — EXTRACT
# ─────────────────────────────────────────────────────────────────────────────
def extract(path: Path) -> pd.DataFrame:
    log.info("Reading Excel: %s", path)
    df = pd.read_excel(path, engine="openpyxl")
    log.info("Extracted  →  %d rows × %d columns", *df.shape)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — SCHEMA INFERENCE
# ─────────────────────────────────────────────────────────────────────────────
def _clean_col(name: str) -> str:
    name = str(name).strip()
    name = re.sub(r"[\s\-\/\(\)\[\]\.\?]+", "_", name)
    name = re.sub(r"[^a-zA-Z0-9_]", "", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name.lower()


def _infer_sql_type(series: pd.Series) -> str:
    dtype = series.dtype
    if pd.api.types.is_bool_dtype(dtype):           return "BIT"
    if pd.api.types.is_integer_dtype(dtype):
        mx = series.abs().max() if len(series) else 0
        return "BIGINT" if mx > 2_147_483_647 else "INT"
    if pd.api.types.is_float_dtype(dtype):          return "FLOAT"
    if pd.api.types.is_datetime64_any_dtype(dtype): return "DATETIME"

    sample = series.dropna().head(20).astype(str)
    hits = 0
    for v in sample:
        try:
            pd.to_datetime(v, infer_datetime_format=True)
            hits += 1
        except Exception:
            pass
    if hits >= max(1, len(sample) * 0.8):
        return "DATETIME"

    max_len = int(series.dropna().astype(str).str.len().max() or 50)
    if max_len <= 50:  return "NVARCHAR(50)"
    if max_len <= 255: return "NVARCHAR(255)"
    return "NVARCHAR(MAX)"


def infer_schema(df: pd.DataFrame) -> pd.DataFrame:
    total = len(df)
    records = []
    for col in df.columns:
        s = df[col]
        records.append({
            "original_name": col,
            "clean_name":    _clean_col(col),
            "pandas_dtype":  str(s.dtype),
            "sql_type":      _infer_sql_type(s),
            "null_count":    int(s.isna().sum()),
            "null_pct":      f"{s.isna().sum()/total*100:.1f}%",
            "unique_values": int(s.nunique()),
        })
    return pd.DataFrame(records)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — DDL GENERATION
# ─────────────────────────────────────────────────────────────────────────────
def generate_ddl(schema: pd.DataFrame, table: str) -> str:
    lines = [
        f"-- Auto-generated  {datetime.now():%Y-%m-%d %H:%M:%S}",
        f"CREATE TABLE IF NOT EXISTS [{table}] (",
    ]
    for _, r in schema.iterrows():
        nullable = "NULL" if r["null_count"] > 0 else "NOT NULL"
        lines.append(f"    [{r['clean_name']}]  {r['sql_type']}  {nullable},")
    lines[-1] = lines[-1].rstrip(",")
    lines.append(");")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — TRANSFORM
# ─────────────────────────────────────────────────────────────────────────────
def transform(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df.dropna(how="all")
    if len(df) < before:
        log.info("  Dropped %d fully-empty rows", before - len(df))

    df.columns = [_clean_col(c) for c in df.columns]
    log.info("  Column names → snake_case")

    str_cols = df.select_dtypes(include=["object", "str"]).columns
    df[str_cols] = df[str_cols].apply(lambda s: s.str.strip() if hasattr(s, "str") else s)
    log.info("  Strings trimmed")

    log.info("Transform complete  →  %d rows × %d columns", *df.shape)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — LOAD (SQLite, batched)
# ─────────────────────────────────────────────────────────────────────────────
def load(df: pd.DataFrame, db_path: str, table: str, batch_size: int = 100) -> dict:
    conn = sqlite3.connect(db_path)

    type_map = {
        "int64": "INTEGER", "float64": "REAL",
        "bool": "INTEGER", "object": "TEXT", "datetime64[ns]": "TEXT",
    }
    col_defs = ", ".join(
        f'"{c}" {type_map.get(str(df[c].dtype), "TEXT")}'
        for c in df.columns
    )
    conn.execute(f'DROP TABLE IF EXISTS "{table}"')
    conn.execute(f'CREATE TABLE "{table}" ({col_defs})')
    conn.commit()

    chunks = [df.iloc[i:i+batch_size] for i in range(0, len(df), batch_size)]
    total_loaded = 0
    for i, chunk in enumerate(chunks, 1):
        chunk.to_sql(table, conn, if_exists="append", index=False)
        total_loaded += len(chunk)
        log.info("  Batch %d/%d  →  %d rows inserted", i, len(chunks), len(chunk))

    count = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    conn.close()
    return {"rows_loaded": total_loaded, "db_count": count, "db_path": db_path}


# ─────────────────────────────────────────────────────────────────────────────
# DISPLAY HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def print_schema_table(schema_df: pd.DataFrame):
    cols   = ["original_name", "clean_name", "sql_type", "null_count", "null_pct", "unique_values"]
    widths = {c: max(len(c), schema_df[c].astype(str).str.len().max()) for c in cols}
    header = "  ".join(c.upper().ljust(widths[c]) for c in cols)
    sep    = "  ".join("─" * widths[c] for c in cols)
    print(f"\n  {header}\n  {sep}")
    for _, row in schema_df.iterrows():
        print("  " + "  ".join(str(row[c]).ljust(widths[c]) for c in cols))
    print()


def print_sample_rows(df: pd.DataFrame, n: int = 5):
    show_cols = list(df.columns[:6])
    sub = df.head(n)[show_cols].copy()
    sub = sub.astype(str).map(lambda x: x[:22] + "…" if len(x) > 22 else x)
    widths = {c: max(len(c), sub[c].str.len().max()) for c in show_cols}
    header = "  ".join(c.ljust(widths[c]) for c in show_cols)
    sep    = "  ".join("─" * widths[c] for c in show_cols)
    print(f"  {header}\n  {sep}")
    for _, row in sub.iterrows():
        print("  " + "  ".join(str(row[c]).ljust(widths[c]) for c in show_cols))
    print()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"\n{BANNER}")
    print("  🔄  ETL PIPELINE DEMO  —  Python as an ETL engine")
    print(f"{BANNER}\n")

    EXCEL_PATH = Path("demo_sales_data.xlsx")
    DB_PATH    = "etl_demo.db"
    TABLE_NAME = "sales_data"
    BATCH_SIZE = 100

    print(f"{'─'*64}\n  STEP 0  │  Generating sample dataset\n{'─'*64}")
    generate_sample_excel(EXCEL_PATH, n_rows=250)

    print(f"\n{'─'*64}\n  STEP 1  │  EXTRACT — Reading Excel file\n{'─'*64}")
    raw_df = extract(EXCEL_PATH)

    print(f"\n{'─'*64}\n  STEP 2  │  SCHEMA INFERENCE — Dynamic column type detection\n{'─'*64}")
    schema = infer_schema(raw_df)
    print_schema_table(schema)

    print(f"{'─'*64}\n  STEP 3  │  DDL GENERATION — Auto-generated CREATE TABLE\n{'─'*64}\n")
    ddl = generate_ddl(schema, TABLE_NAME)
    for line in ddl.split("\n"):
        print(f"  {line}")
    Path("generated_ddl.sql").write_text(ddl)
    print(f"\n  ✅  DDL saved to: generated_ddl.sql\n")

    print(f"{'─'*64}\n  STEP 4  │  TRANSFORM — Clean, normalize, coerce\n{'─'*64}")
    clean_df = transform(raw_df)
    print(f"\n  Sample rows after transform (first 6 columns):")
    print_sample_rows(clean_df, n=5)

    print(f"{'─'*64}\n  STEP 5  │  LOAD — Batched INSERT → SQLite [{DB_PATH}]\n{'─'*64}")
    result = load(clean_df, DB_PATH, TABLE_NAME, BATCH_SIZE)

    print(f"\n{BANNER}")
    print("  ✅  PIPELINE COMPLETE — SUMMARY")
    print(BANNER)
    print(f"  Source file     : {EXCEL_PATH}")
    print(f"  Rows extracted  : {len(raw_df)}")
    print(f"  Columns inferred: {len(schema)}")
    print(f"  Rows loaded     : {result['rows_loaded']}")
    print(f"  DB row count    : {result['db_count']}  ← verified with SELECT COUNT(*)")
    print(f"  Target DB       : {result['db_path']}  (SQLite, demo mode)")
    print(f"  DDL file        : generated_ddl.sql")
    print(f"  Batch size      : {BATCH_SIZE} rows/batch")
    print(BANNER)
    print()
    print("  💡  To use SQL Server: set destination.mode: sqlserver in config.yaml")
    print()
