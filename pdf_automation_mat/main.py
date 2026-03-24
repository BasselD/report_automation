"""
main.py  ── v3
Entry point for batch PDF generation.

Usage:
    python main.py --input data/adherence_data.csv --output output/pdfs
    python main.py --input data/adherence_data.csv --output output/pdfs \
                   --concurrency 8 --qr-code assets/qr_code.png
"""

import argparse
import asyncio
import sys
import time
from pathlib import Path

import pandas as pd

from report_builder import build_report_html
from pdf_converter   import build_filename, convert_batch

PROVIDER_KEY = ["Market", "SubMarket", "ManagingEntity", "ReportingPod", "PCPName", "PCPNPI"]


def load_data(path):
    p = Path(path)
    if p.suffix.lower() == ".parquet":
        return pd.read_parquet(p)
    return pd.read_csv(p, dtype=str, low_memory=False)


def prepare_jobs(df, output_dir, qr_code):
    for col in PROVIDER_KEY:
        if col not in df.columns:
            df[col] = "UNKNOWN"

    jobs  = []
    total = df.groupby(PROVIDER_KEY, dropna=False).ngroups
    print(f"  Found {total} provider groups.")

    qr_abs = str(Path(qr_code).resolve()) if qr_code else None

    for keys, provider_df in df.groupby(PROVIDER_KEY, dropna=False):
        meta     = dict(zip(PROVIDER_KEY, keys))
        out_path = build_filename(meta, output_dir)

        if out_path.exists():
            print(f"  ↷  Skipping (exists): {out_path.name}")
            continue

        try:
            html = build_report_html(provider_df, qr_code_path=qr_abs)
            jobs.append((html, out_path))
        except Exception as e:
            print(f"  ✗  HTML render failed — {meta.get('PCPName', '?')}: {e}")

    return jobs


def main():
    parser = argparse.ArgumentParser(description="Medication Adherence PDF Generator v3")
    parser.add_argument("--input",       required=True)
    parser.add_argument("--output",      default="output/pdfs")
    parser.add_argument("--concurrency", type=int, default=6)
    parser.add_argument("--qr-code",     default=None, help="Path to QR code PNG")
    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*55}")
    print("  Medication Adherence Report Generator  v3")
    print(f"{'='*55}")
    print(f"  Input       : {args.input}")
    print(f"  Output      : {output_dir}")
    print(f"  Concurrency : {args.concurrency} workers")
    print(f"  QR code     : {args.qr_code or '(placeholder)'}")
    print(f"{'='*55}\n")

    print("► Loading data...")
    try:
        df = load_data(args.input)
    except FileNotFoundError:
        print(f"  ERROR: File not found — {args.input}")
        sys.exit(1)
    npi_count = df["PCPNPI"].nunique() if "PCPNPI" in df.columns else "?"
    print(f"  {len(df):,} rows | {npi_count} unique providers")

    print("\n► Building HTML reports...")
    t0   = time.time()
    jobs = prepare_jobs(df, output_dir, args.qr_code)
    print(f"  {len(jobs)} reports queued  ({time.time()-t0:.1f}s)")

    if not jobs:
        print("\n  Nothing to generate. Exiting.")
        return

    print(f"\n► Converting to PDF ({args.concurrency} workers)...")
    t1      = time.time()
    results = asyncio.run(convert_batch(jobs, concurrency=args.concurrency))
    elapsed = time.time() - t1

    ok  = sum(1 for v in results.values() if v == "ok")
    err = len(results) - ok

    print(f"\n{'='*55}")
    print(f"  Done in {elapsed:.1f}s  ({ok/max(elapsed,1):.1f} PDFs/sec)")
    print(f"  ✓  {ok} PDFs generated")
    if err:
        print(f"  ✗  {err} errors")
        for path, msg in results.items():
            if msg != "ok": print(f"     {Path(path).name}: {msg}")
    print(f"  Output: {output_dir.resolve()}")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    main()
