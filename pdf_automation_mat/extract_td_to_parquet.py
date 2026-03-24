import argparse
import configparser as cp
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


def load_td_config(config_path: str, section: str = "HealthSpring") -> dict:
    config = cp.ConfigParser()
    read_files = config.read(config_path)

    if not read_files:
        raise FileNotFoundError(f"Could not read config file: {config_path}")

    if not config.has_section(section):
        raise KeyError(f"Section [{section}] not found in config file: {config_path}")

    required_keys = ["STDID", "STDPW", "TDHOST"]
    missing_keys = [key for key in required_keys if not config.has_option(section, key)]
    if missing_keys:
        raise KeyError(f"Missing required config keys in [{section}]: {missing_keys}")

    td_config = {
        "user": config.get(section, "STDID"),
        "password": config.get(section, "STDPW"),
        "host": config.get(section, "TDHOST"),
        "database": config.get(section, "DATABASE", fallback=""),
        "logmech": config.get(section, "LOGMECH", fallback=""),
    }

    return td_config


def query_teradata(sql: str, td_config: dict) -> pd.DataFrame:
    con_parts = [
        f'host={td_config["host"]}',
        f'user={td_config["user"]}',
        f'password={td_config["password"]}',
    ]

    if td_config.get("logmech"):
        con_parts.append(f'logmech={td_config["logmech"]}')

    if td_config.get("database"):
        con_parts.append(f'database={td_config["database"]}')

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
    parser.add_argument("--config", required=True, help="Path to external .ini config file")
    parser.add_argument("--config-section", default="HealthSpring", help="Section name in config file")
    parser.add_argument("--output-dir", required=True, help="Directory to save parquet")
    parser.add_argument("--prefix", default="provider_report", help="Parquet filename prefix")
    args = parser.parse_args()

    print("Loading SQL...")
    sql = load_sql(args.sql)

    print("Loading Teradata config...")
    td_config = load_td_config(args.config, args.config_section)

    print("Running Teradata query...")
    df = query_teradata(sql, td_config)

    print("Validating result set...")
    validate_dataframe(df)

    print("Normalizing datatypes...")
    df = normalize_types(df)

    output_path = build_output_path(args.output_dir, args.prefix)

    print(f"Writing parquet to: {output_path}")
    df.to_parquet(output_path, index=False)

    print("Done.")
    print(f"Saved parquet: {output_path}")


if __name__ == "__main__":
    main()
