"""
pdf_converter.py
Async Playwright batch PDF conversion with configurable concurrency.
"""

import asyncio
import re
from pathlib import Path

from playwright.async_api import async_playwright


def _sanitize(val):
    return re.sub(r"[^\w\-]", "_", str(val).strip())


def build_filename(row_data: dict, output_dir: Path) -> Path:
    """
    Market_SubMarket_ManagingEntity_ReportingPod_PCPName_PCPNPI.pdf
    """
    parts = [
        _sanitize(row_data.get("Market",        "UNKNOWN")),
        _sanitize(row_data.get("SubMarket",      "UNKNOWN")),
        _sanitize(row_data.get("ManagingEntity", "UNKNOWN")),
        _sanitize(row_data.get("ReportingPod",   "UNKNOWN")),
        _sanitize(row_data.get("PCPName",        "UNKNOWN")),
        _sanitize(row_data.get("PCPNPI",         "UNKNOWN")),
    ]
    return output_dir / ("_".join(parts) + ".pdf")


async def _render(page, html, out_path):
    await page.set_content(html, wait_until="domcontentloaded")
    await page.pdf(
        path=str(out_path),
        format="Letter",
        print_background=True,
        margin={"top": "12mm", "bottom": "12mm", "left": "10mm", "right": "10mm"},
    )


async def convert_batch(jobs: list, concurrency: int = 6) -> dict:
    """
    jobs: list of (html_string, output_pdf_path)
    Returns: {str(path): "ok" | "error: <msg>"}
    """
    results   = {}
    semaphore = asyncio.Semaphore(concurrency)

    async with async_playwright() as p:
        browser = await p.chromium.launch()

        async def process(html, out_path):
            async with semaphore:
                page = await browser.new_page()
                try:
                    await _render(page, html, out_path)
                    results[str(out_path)] = "ok"
                    print(f"  ✓  {out_path.name}")
                except Exception as e:
                    results[str(out_path)] = f"error: {e}"
                    print(f"  ✗  {out_path.name} — {e}")
                finally:
                    await page.close()

        await asyncio.gather(*[process(h, p) for h, p in jobs])
        await browser.close()

    return results
