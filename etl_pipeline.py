"""
etl_pipeline.py
===============
A lightweight, configurable ETL pipeline that:
  - Loads any Excel file (local path or sample dataset auto-downloaded from the internet)
  - Dynamically infers column names, data types, nullability, and row count
  - Auto-generates a CREATE TABLE DDL statement
  - Inserts data in configurable batches into Microsoft SQL Server or SQLite (demo mode)

GitHub  : https://github.com/kage77/python-etl-pipeline
License : MIT
"""

import os
import sys
import logging
import argparse
import textwrap
from pathlib import Path
from datetime import datetime

import yaml
import pandas as pd
import numpy as np
import requests

# ── optional SQL Server driver ──────────────────────────────────────────────
try:
    import pyodbc
    import sqlalchemy
    from sqlalchemy import create_engine, text
    SQL_SERVER_AVAILABLE = True
except ImportError:
    SQL_SERVER_AVAILABLE = False

import sqlite3  # always available – used for demo / local mode

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)-8s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("etl_pipeline.log", mode="a"),
    ],
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG LOADER
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_CONFIG_PATH = Path(__file__).parent / "config.yaml"


def load_config(config_path: Path = DEFAULT_CONFIG_PATH) -> dict:
    """Load YAML config file; return merged dict with safe defaults."""
    defaults = {
        "source": {
            "use_sample_dataset": True,
            "sample_dataset_url": (
                "https://raw.githubusercontent.com/bharathirajatut/sample-excel-dataset"
                "/master/sales.xls"
            ),
            "local_file_path": None,
            "sheet_name": 0,
            "header_row": 0,
            "skip_rows": 0,
        },
        "destination": {
            "mode": "sqlite",           # "sqlite" | "sqlserver"
            "sqlite_db_path": "etl_demo.db",
            "table_name": "etl_load",
            "schema": "dbo",
            "if_table_exists": "replace",   # "replace" | "append" | "fail"
            "batch_size": 500,
        },
        "sql_server": {
            "server": "YOUR_SERVER",
            "database": "YOUR_DATABASE",
            "username": "",             # leave blank for Windows Auth
            "password": "",
            "driver": "ODBC Driver 17 for SQL Server",
            "trusted_connection": True,
        },
        "pipeline": {
            "log_sample_rows": 5,
            "infer_types": True,
            "clean_column_names": True,
            "trim_strings": True,
            "drop_fully_empty_rows": True,
        },
    }

    if config_path.exists():
        with open(config_path, "r") as f:
            user_cfg = yaml.safe_load(f) or {}
        for section, values in user_cfg.items():
            if section in defaults and isinstance(values, dict):
                defaults[section].update(values)
            else:
                defaults[section] = values
        log.info("Config loaded from %s", config_path)
    else:
        log.warning("config.yaml not found – using built-in defaults.")

    return defaults


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACT
# ─────────────────────────────────────────────────────────────────────────────
def download_sample_dataset(url: str, dest_dir: Path = Path(".")) -> Path:
    """Download a remote Excel/XLS file if not already cached locally."""
    filename = dest_dir / Path(url.split("/")[-1])
    if filename.exists():
        log.info("Sample dataset already cached at %s", filename)
        return filename

    log.info("Downloading sample dataset from:\n  %s", url)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    filename.write_bytes(resp.content)
    log.info("Saved to %s  (%d bytes)", filename, len(resp.content))
    return filename


def extract(cfg: dict) -> pd.DataFrame:
    """
    Load an Excel file into a DataFrame.
    Chooses between sample dataset (auto-download) or a local file path.
    """
    src = cfg["source"]

    if src["use_sample_dataset"] or not src.get("local_file_path"):
        file_path = download_sample_dataset(src["sample_dataset_url"])
    else:
        file_path = Path(src["local_file_path"])
        if not file_path.exists():
            raise FileNotFoundError(f"Local file not found: {file_path}")

    log.info("Reading Excel file: %s", file_path)

    engine_hint = "xlrd" if str(file_path).lower().endswith(".xls") else "openpyxl"

    df = pd.read_excel(
        file_path,
        sheet_name=src["sheet_name"],
        header=src["header_row"],
        skiprows=src["skip_rows"] if src["skip_rows"] else None,
        engine=engine_hint,
    )

    log.info("Extracted  →  %d rows × %d columns", *df.shape)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# TRANSFORM
# ─────────────────────────────────────────────────────────────────────────────
def _clean_col_name(name: str) -> str:
    """Normalize a column name to snake_case, SQL-safe."""
    import re
    name = str(name).strip()
    name = re.sub(r"[\s\-\/\(\)\[\]\.]+", "_", name)
    name = re.sub(r"[^a-zA-Z0-9_]", "", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name.lower()


def _infer_sql_type(series: pd.Series) -> str:
    """
    Map a pandas Series dtype to a SQL Server / SQLite type string.
    Uses a sample of non-null values for heuristic date detection.
    """
    dtype = series.dtype

    if pd.api.types.is_bool_dtype(dtype):
        return "BIT"
    if pd.api.types.is_integer_dtype(dtype):
        max_val = series.abs().max() if len(series) else 0
        return "BIGINT" if max_val > 2_147_483_647 else "INT"
    if pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATETIME"

    sample = series.dropna().head(20).astype(str)
    date_count = 0
    for val in sample:
        try:
            pd.to_datetime(val, infer_datetime_format=True)
            date_count += 1
        except Exception:
            pass
    if date_count >= max(1, len(sample) * 0.8):
        return "DATETIME"

    max_len = series.dropna().astype(str).str.len().max() if len(series) else 50
    max_len = int(max_len) if not np.isnan(max_len) else 50
    if max_len <= 50:
        return "NVARCHAR(50)"
    if max_len <= 255:
        return "NVARCHAR(255)"
    return "NVARCHAR(MAX)"


def build_schema_report(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate a schema inference report DataFrame with columns:
      original_name | clean_name | pandas_dtype | sql_type | null_count | null_pct | sample_values
    """
    records = []
    total = len(df)
    for col in df.columns:
        series = df[col]
        clean = _clean_col_name(col)
        sql_t = _infer_sql_type(series)
        nulls = int(series.isna().sum())
        pct = round(nulls / total * 100, 1) if total else 0
        sample = series.dropna().head(3).tolist()
        records.append({
            "original_name": col,
            "clean_name": clean,
            "pandas_dtype": str(series.dtype),
            "sql_type": sql_t,
            "null_count": nulls,
            "null_pct": pct,
            "sample_values": str(sample),
        })
    return pd.DataFrame(records)


def transform(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """
    Apply all configured transformations:
      - Clean column names to snake_case
      - Trim string values
      - Drop fully empty rows
      - Coerce date-like object columns to datetime
    """
    pipe = cfg["pipeline"]

    if pipe["drop_fully_empty_rows"]:
        before = len(df)
        df = df.dropna(how="all")
        dropped = before - len(df)
        if dropped:
            log.info("Dropped %d fully-empty rows", dropped)

    if pipe["clean_column_names"]:
        df.columns = [_clean_col_name(c) for c in df.columns]
        log.info("Column names normalized to snake_case")

    if pipe["trim_strings"]:
        str_cols = df.select_dtypes(include=["object", "str"]).columns
        df[str_cols] = df[str_cols].apply(
            lambda s: s.str.strip() if hasattr(s, "str") else s
        )

    for col in df.select_dtypes(include=["object", "str"]).columns:
        sample = df[col].dropna().head(20).astype(str)
        hits = sum(1 for v in sample if _looks_like_date(v))
        if hits >= max(1, len(sample) * 0.8):
            try:
                df[col] = pd.to_datetime(df[col], infer_datetime_format=True, errors="coerce")
                log.info("  Auto-coerced column '%s' to datetime", col)
            except Exception:
                pass

    log.info("Transform complete  →  %d rows × %d columns", *df.shape)
    return df


def _looks_like_date(val: str) -> bool:
    try:
        pd.to_datetime(val, infer_datetime_format=True)
        return True
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# DDL GENERATION
# ─────────────────────────────────────────────────────────────────────────────
def generate_ddl(schema_report: pd.DataFrame, table_name: str, schema: str = "dbo") -> str:
    """Auto-generate a CREATE TABLE DDL statement from the schema inference report."""
    full_table = f"[{schema}].[{table_name}]"
    col_defs = []
    for _, row in schema_report.iterrows():
        nullable = "NULL" if row["null_count"] > 0 else "NOT NULL"
        col_defs.append(f"    [{row['clean_name']}]  {row['sql_type']}  {nullable}")

    ddl = (
        f"-- Auto-generated by etl_pipeline.py  |  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"IF OBJECT_ID(N'{full_table}', N'U') IS NOT NULL DROP TABLE {full_table};\n\n"
        f"CREATE TABLE {full_table} (\n"
        + ",\n".join(col_defs)
        + "\n);\n"
    )
    return ddl


# ─────────────────────────────────────────────────────────────────────────────
# LOAD
# ─────────────────────────────────────────────────────────────────────────────
def _get_sqlite_engine(db_path: str):
    from sqlalchemy import create_engine
    return create_engine(f"sqlite:///{db_path}")


def _get_sqlserver_engine(ss_cfg: dict):
    if not SQL_SERVER_AVAILABLE:
        raise RuntimeError(
            "pyodbc / sqlalchemy not installed. Run: pip install pyodbc sqlalchemy"
        )
    from sqlalchemy import create_engine
    if ss_cfg["trusted_connection"] and not ss_cfg.get("username"):
        conn_str = (
            f"mssql+pyodbc://{ss_cfg['server']}/{ss_cfg['database']}"
            f"?driver={ss_cfg['driver'].replace(' ', '+')}&trusted_connection=yes"
        )
    else:
        conn_str = (
            f"mssql+pyodbc://{ss_cfg['username']}:{ss_cfg['password']}"
            f"@{ss_cfg['server']}/{ss_cfg['database']}"
            f"?driver={ss_cfg['driver'].replace(' ', '+')}"
        )
    return create_engine(conn_str, fast_executemany=True)


def load(df: pd.DataFrame, cfg: dict, ddl: str) -> None:
    """Load DataFrame into target database in configurable batches."""
    dest = cfg["destination"]
    mode = dest["mode"].lower()
    table = dest["table_name"]
    batch_size = dest["batch_size"]
    if_exists = dest["if_table_exists"]

    if mode == "sqlite":
        engine = _get_sqlite_engine(dest["sqlite_db_path"])
        log.info("Target  →  SQLite  (%s)", dest["sqlite_db_path"])
    elif mode == "sqlserver":
        engine = _get_sqlserver_engine(cfg["sql_server"])
        log.info(
            "Target  →  SQL Server  [%s].[%s]",
            cfg["sql_server"]["server"],
            cfg["sql_server"]["database"],
        )
    else:
        raise ValueError(f"Unknown destination mode: '{mode}'")

    total_rows = len(df)
    loaded = 0
    chunks = [df.iloc[i: i + batch_size] for i in range(0, total_rows, batch_size)]

    log.info(
        "Loading %d rows in %d batch(es) of up to %d ...",
        total_rows, len(chunks), batch_size,
    )

    with engine.begin() as conn:
        for i, chunk in enumerate(chunks, start=1):
            chunk.to_sql(
                name=table,
                con=conn,
                if_exists=if_exists if i == 1 else "append",
                index=False,
                schema=dest["schema"] if mode == "sqlserver" else None,
            )
            loaded += len(chunk)
            log.info("  Batch %d/%d  →  %d rows committed", i, len(chunks), len(chunk))

    log.info("=" * 60)
    log.info("LOAD COMPLETE")
    log.info("  Table        : %s", table)
    log.info("  Rows loaded  : %d / %d", loaded, total_rows)
    log.info("  Mode         : %s", mode.upper())
    log.info("=" * 60)

    ddl_path = Path("generated_ddl.sql")
    ddl_path.write_text(ddl)
    log.info("DDL saved to: %s", ddl_path)


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────
def run_pipeline(config_path: Path = DEFAULT_CONFIG_PATH) -> None:
    log.info("━" * 60)
    log.info("  ETL PIPELINE STARTED  —  %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("━" * 60)

    cfg = load_config(config_path)

    raw_df = extract(cfg)
    schema_report = build_schema_report(raw_df)

    log.info("\n%s\nSCHEMA INFERENCE REPORT\n%s", "─" * 60, "─" * 60)
    log.info("\n" + schema_report.to_string(index=False))

    ddl = generate_ddl(
        schema_report,
        table_name=cfg["destination"]["table_name"],
        schema=cfg["destination"].get("schema", "dbo"),
    )
    log.info("\nGenerated DDL:\n%s", ddl)

    clean_df = transform(raw_df, cfg)

    n = cfg["pipeline"]["log_sample_rows"]
    log.info("\nSample output (%d rows):\n%s", n, clean_df.head(n).to_string(index=False))

    load(clean_df, cfg, ddl)

    log.info("Pipeline finished successfully.")


# ─────────────────────────────────────────────────────────────────────────────
# CLI ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=textwrap.dedent(
            """
            Python ETL Pipeline — Excel to SQL
            ───────────────────────────────────
            Extracts any Excel file, infers schema dynamically,
            and loads data into SQL Server or SQLite.
            """
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH,
                   help="Path to config.yaml (default: ./config.yaml)")
    p.add_argument("--file", type=str, default=None,
                   help="Override: path to a local Excel file")
    p.add_argument("--table", type=str, default=None,
                   help="Override: target table name")
    p.add_argument("--mode", choices=["sqlite", "sqlserver"], default=None,
                   help="Override: destination mode")
    return p


if __name__ == "__main__":
    parser = _build_parser()
    args = parser.parse_args()

    cfg = load_config(args.config)
    if args.file:
        cfg["source"]["use_sample_dataset"] = False
        cfg["source"]["local_file_path"] = args.file
    if args.table:
        cfg["destination"]["table_name"] = args.table
    if args.mode:
        cfg["destination"]["mode"] = args.mode

    import tempfile
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp:
        yaml.dump(cfg, tmp)
        tmp_path = Path(tmp.name)

    try:
        run_pipeline(tmp_path)
    finally:
        tmp_path.unlink(missing_ok=True)
