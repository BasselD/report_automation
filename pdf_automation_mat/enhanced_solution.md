Yes. That is the production shape I’d recommend.

## Recommended pipeline

```text
Teradata SQL
   ↓
extract_td_to_parquet.py
   ↓
provider_snapshot.parquet
   ↓
main.py
   ↓
HTML per provider
   ↓
PDF per provider
```

Why this is strong:

* the Teradata SQL Driver for Python is a DB-API 2.0 driver built to connect Python apps to Teradata, so it fits this extraction step cleanly ([PyPI][1])
* pandas supports writing DataFrames to Parquet with `to_parquet()` and loading them back with `read_parquet()` ([Pandas][2])

---

# What I would change

Split the workflow into **two scripts**:

## 1. `extract_td_to_parquet.py`

Responsible for:

* connect to Teradata
* run SQL
* load results into a DataFrame
* do light validation
* save a Parquet snapshot

## 2. `main.py`

Responsible for:

* read the Parquet snapshot
* group by provider
* build HTML
* generate PDFs

That separation gives you cleaner reruns, easier QA, and less load on Teradata.

---

# Suggested folder structure

```text
project/
├── main.py
├── extract_td_to_parquet.py
├── report_builder.py
├── pdf_converter.py
├── report.html
├── sql/
│   └── provider_report_query.sql
├── output/
│   ├── snapshots/
│   │   └── provider_report_2026-03-24.parquet
│   └── pdfs/
└── configs/
    └── td_config.json
```

---

# Script 1. `extract_td_to_parquet.py`

This script queries Teradata and writes the Parquet file.

```python
import argparse
import json
from pathlib import Path
from datetime import datetime

import pandas as pd
import teradatasql


REQUIRED_COLUMNS = [
    "Market",
    "SubMarket",
    "ManagingEntity",
    "ReportingPod",
    "PCPName",
    "PCPNPI",
]


def load_sql(sql_path: str) -> str:
    return Path(sql_path).read_text(encoding="utf-8")


def load_config(config_path: str) -> dict:
    return json.loads(Path(config_path).read_text(encoding="utf-8"))


def query_teradata(sql: str, config: dict) -> pd.DataFrame:
    host = config["host"]
    user = config["user"]
    password = config["password"]

    # Optional extras if your environment needs them
    logmech = config.get("logmech")
    database = config.get("database")

    con_parts = [f'host={host}', f'user={user}', f'password={password}']
    if logmech:
        con_parts.append(f'logmech={logmech}')
    if database:
        con_parts.append(f'database={database}')

    con_str = ",".join(con_parts)

    with teradatasql.connect(con_str) as conn:
        df = pd.read_sql(sql, conn)

    return df


def validate_dataframe(df: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    if df.empty:
        raise ValueError("Query returned zero rows")

    provider_count = (
        df[["Market", "SubMarket", "ManagingEntity", "ReportingPod", "PCPName", "PCPNPI"]]
        .fillna("UNKNOWN")
        .drop_duplicates()
        .shape[0]
    )

    print(f"Rows returned: {len(df):,}")
    print(f"Distinct providers: {provider_count:,}")


def normalize_types(df: pd.DataFrame) -> pd.DataFrame:
    # Add only the conversions you actually trust
    date_cols = [
        "DOB",
        "LastFillDate",
        "MedicationRunoutDate",
        "ReportAnchorDate",
    ]
    numeric_cols = [
        "CurrentPDC",
        "Prior1YearPDC",
        "Prior2YearPDC",
        "DaysLatetoRefill",
        "MedicationRunoutDays",
        "RecoverableDays",
    ]

    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def build_output_path(output_dir: str, prefix: str) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"{prefix}_{ts}.parquet"


def main():
    parser = argparse.ArgumentParser(description="Extract Teradata query results to Parquet")
    parser.add_argument("--sql", required=True, help="Path to SQL file")
    parser.add_argument("--config", required=True, help="Path to Teradata config JSON")
    parser.add_argument("--output-dir", required=True, help="Directory to save parquet")
    parser.add_argument("--prefix", default="provider_report", help="Parquet filename prefix")
    args = parser.parse_args()

    sql = load_sql(args.sql)
    config = load_config(args.config)

    print("Running Teradata query...")
    df = query_teradata(sql, config)

    print("Validating result set...")
    validate_dataframe(df)

    print("Normalizing types...")
    df = normalize_types(df)

    output_path = build_output_path(args.output_dir, args.prefix)

    print(f"Writing parquet: {output_path}")
    df.to_parquet(output_path, index=False)

    print("Done.")
    print(f"Parquet saved to: {output_path}")


if __name__ == "__main__":
    main()
```

---

# Example `td_config.json`

Do **not** hardcode credentials into the script. Put them in config or, even better, environment variables.

```json
{
  "host": "your-teradata-server.company.com",
  "user": "your_username",
  "password": "your_password",
  "logmech": "LDAP",
  "database": "your_database"
}
```

Better version for production:

* keep password in environment variable
* load it at runtime
* never commit config with secrets

---

# Example SQL file `sql/provider_report_query.sql`

Use a plain SQL file so your extraction logic stays readable.

```sql
SELECT
    Market,
    SubMarket,
    ManagingEntity,
    ReportingPod,
    PCPName,
    PCPNPI,
    MemberID,
    MemberName,
    DOB,
    DrugName,
    PharmacyName,
    MeasureCode,
    NonAdherenceRiskTier,
    CurrentPDC,
    Prior1YearPDC,
    Prior2YearPDC,
    DaysLatetoRefill,
    MedicationRunoutDate,
    MedicationRunoutDays,
    RecoverableDays,
    ReportAnchorDate,
    RecommendedAction,
    RecommendationReason
FROM your_schema.your_source_table
WHERE ReportMonth = DATE '2026-03-01';
```

---

# Script 2. keep `main.py` focused on Parquet

Your current `main.py` already supports Parquet input, which is exactly what we want. pandas `read_parquet()` is built for this purpose. ([Pandas][3])

So the run would look like this:

```bash
python extract_td_to_parquet.py \
  --sql sql/provider_report_query.sql \
  --config configs/td_config.json \
  --output-dir output/snapshots \
  --prefix provider_report

python main.py \
  --input output/snapshots/provider_report_20260324_083000.parquet \
  --output output/pdfs \
  --concurrency 6 \
  --qr-code assets/qr_code.png
```

---

# Slight improvement to `main.py`

You can make the pipeline smoother by allowing `main.py` to accept a Parquet path and an optional `--latest-snapshot` pattern, but honestly, keep it simple first.

If you want convenience, add a tiny helper script instead.

---

# Optional wrapper script. run both in one shot

If you want one command that still preserves the 2-step design, add a wrapper like `run_pipeline.py`.

```python
import argparse
import subprocess
from pathlib import Path


def latest_parquet(snapshot_dir: str, prefix: str) -> str:
    files = sorted(Path(snapshot_dir).glob(f"{prefix}_*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found in {snapshot_dir}")
    return str(files[-1])


def main():
    parser = argparse.ArgumentParser(description="Run TD extract then PDF pipeline")
    parser.add_argument("--sql", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--snapshot-dir", required=True)
    parser.add_argument("--prefix", default="provider_report")
    parser.add_argument("--pdf-output", required=True)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--qr-code")
    args = parser.parse_args()

    subprocess.run(
        [
            "python",
            "extract_td_to_parquet.py",
            "--sql", args.sql,
            "--config", args.config,
            "--output-dir", args.snapshot_dir,
            "--prefix", args.prefix,
        ],
        check=True,
    )

    parquet_path = latest_parquet(args.snapshot_dir, args.prefix)

    cmd = [
        "python",
        "main.py",
        "--input", parquet_path,
        "--output", args.pdf_output,
        "--concurrency", str(args.concurrency),
    ]

    if args.qr_code:
        cmd.extend(["--qr-code", args.qr_code])

    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
```

That way you still get:

* extraction snapshot
* reproducibility
* one command for convenience

Best of both worlds. No nonsense.

---

# Best practice additions I strongly recommend

## 1. Save metadata next to the Parquet

Create a small JSON audit file with:

* query timestamp
* row count
* provider count
* SQL file name
* maybe a report month parameter

Example:

```json
{
  "created_at": "2026-03-24T08:30:00",
  "rows": 58432,
  "providers": 1197,
  "sql_file": "sql/provider_report_query.sql"
}
```

That saves future-you from detective work.

## 2. Validate required columns before writing Parquet

Do not let bad extracts move downstream quietly.

## 3. Normalize datatypes once

Parquet preserves structure much better than CSV, so this is the right place to lock dates and numerics into sane types. pandas supports writing DataFrames directly to Parquet for exactly this snapshot use case. ([Pandas][2])

## 4. Keep SQL outside Python

Much easier to maintain and review.

## 5. Parameterize SQL

If you run this monthly, add report month as a parameter.

---

# My recommendation on libraries

For this specific pattern:

* **Teradata connectivity**: `teradatasql`
* **Data handling**: `pandas`
* **Parquet backend**: `pyarrow`

Why:

* the Teradata SQL Driver for Python is the official DB-API style driver for Python connectivity to Teradata ([PyPI][1])
* pandas natively supports `to_parquet()` and `read_parquet()` for this snapshot-and-reload workflow ([Pandas][2])

---

# What I would not do

I would **not** make `main.py` query Teradata directly and build PDFs in the same run for production.

Why:

* if PDF rendering fails, you have to hit Teradata again
* if the data changes, your rerun is not apples-to-apples
* debugging becomes messy

That design is fine for quick tests, but not for your main pipeline.

---

# Final recommendation

Use this:

## Extraction step

```bash
python extract_td_to_parquet.py --sql ... --config ... --output-dir ...
```

## Rendering step

```bash
python main.py --input snapshot.parquet --output output/pdfs ...
```

That is the cleaner architecture.

If you want, I can turn this into a **full ready-to-run version** tailored to your current report codebase, including:

* environment-variable based credentials
* SQL parameterization for report month
* snapshot metadata JSON
* updated `main.py` argument patterns

[1]: https://pypi.org/project/teradatasql/?utm_source=chatgpt.com "teradatasql"
[2]: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_parquet.html?utm_source=chatgpt.com "pandas.DataFrame.to_parquet — pandas 3.0.1 documentation"
[3]: https://pandas.pydata.org/docs/reference/api/pandas.read_parquet.html?utm_source=chatgpt.com "pandas.read_parquet — pandas 3.0.1 documentation - PyData |"
