import os
import pandas as pd
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import LETTER
from reportlab.lib import colors
from reportlab.lib.units import inch
import re
import math
from datetime import datetime

from reportlab.graphics.shapes import Drawing, Rect, String
from reportlab.graphics import colors
from reportlab.platypus import Paragraph

#heatmap color
def pdc_color(pdc):
    """
    Returns None for missing data (blank cell)
    """
    if pdc is None:
        return None

    try:
        if math.isnan(pdc):
            return None
    except TypeError:
        pass

    if pdc < 60:
        return colors.HexColor("#DC2626")   # red
    elif pdc < 80:
        return colors.HexColor("#F59E0B")   # amber
    else:
        return colors.HexColor("#16A34A")   # green

#heatmap drawing
def pdc_heatmap(measure_rows, width=120, height_per_row=18):
    """
    measure_rows = [
        ("Statin",   [pdc_yy2, pdc_yy1, pdc_yy]),
        ("Diabetes", [...]),
        ...
    ]
    """
    if not measure_rows:
        return Drawing(width, height_per_row)

    rows = len(measure_rows)
    cols = 3
    height = rows * height_per_row

    cell_w = width / cols
    cell_h = height_per_row

    d = Drawing(width, height)

    for r, (_, values) in enumerate(measure_rows):
        padded = (values + [None] * cols)[:cols]

        for c, val in enumerate(padded):
            color = pdc_color(val)
            if color is None:
                continue  # BLANK cell

            d.add(Rect(
                c * cell_w,
                height - (r + 1) * cell_h,
                cell_w,
                cell_h,
                fillColor=color,
                strokeColor=colors.white,
                strokeWidth=0.5
            ))

    return d
# Dynamic year headers (2-digit format)
def heatmap_year_headers(report_period_year):
    """
    Returns ['YY-2', 'YY-1', 'YY']
    """
    y = int(report_period_year)
    return [
        f"{str(y-2)[-2:]}",
        f"{str(y-1)[-2:]}",
        f"{str(y)[-2:]}"
    ]
#Heatmap legend (top-right of column header)
def heatmap_legend(width=120, height=24):
    d = Drawing(width, height)

    items = [
        ("< 60", colors.HexColor("#DC2626")),
        ("60–79", colors.HexColor("#F59E0B")),
        ("≥ 80", colors.HexColor("#16A34A")),
    ]

    x = 0
    for label, color in items:
        d.add(Rect(x, 8, 10, 10, fillColor=color, strokeColor=color))
        d.add(String(x + 14, 10, label, fontSize=7))
        x += 40

    return d
#Heatmap column header (labels + legend)
def heatmap_header(report_period_year, styles):
    years = heatmap_year_headers(report_period_year)

    header = [
        Paragraph(
            f"<b>PDC History</b><br/>{years[0]}&nbsp;&nbsp;{years[1]}&nbsp;&nbsp;{years[2]}",
            styles["TableHeader"]
        ),
        heatmap_legend()
    ]

    return header
#Build measure rows safely (2 or 3 measures supported)
def build_measure_rows(row):
    measure_defs = [
        ("Statin", "Statin"),
        ("Diabetes", "Diabetes"),
        ("RAS", "RAS")
    ]

    rows = []

    for label, prefix in measure_defs:
        values = [
            row.get(f"{prefix}_PDC_Prior_2"),
            row.get(f"{prefix}_PDC_Prior_1"),
            row.get(f"{prefix}_PDC_Current"),
        ]

        if any(v is not None and not (isinstance(v, float) and math.isnan(v)) for v in values):
            rows.append((label, values))

    return rows


# cleanup names in the file
def safe_filename(value):
    if value is None:
        return "UNKNOWN"
    value = str(value).strip()
    value = re.sub(r"[^\w\-]+", "_", value)
    return value

def first_page_header(
    canvas,
    doc,
    provider,
    reporting_pod,
    report_period,
    logo_path=None
):
    canvas.saveState()

    banner_height = 1.4 * inch
    page_width, page_height = LETTER

    # Banner background
    canvas.setFillColorRGB(0/255, 40/255, 80/255)
    canvas.rect(
        0,
        page_height - banner_height,
        page_width,
        banner_height,
        stroke=0,
        fill=1
    )

    left_x = 0.75 * inch
    top_y = page_height - 0.55 * inch

    # Main title
    canvas.setFillColor(colors.white)
    canvas.setFont("Helvetica-Bold", 20)
    canvas.drawString(
        left_x,
        top_y,
        "2026 High Risk of Non Adherence Report"
    )

    # Subtitle (provider | pod | date)
    canvas.setFont("Helvetica", 12)
    canvas.drawString(
        left_x,
        top_y - 0.45 * inch,
        f"{provider} | {reporting_pod} | {report_period}"
    )

    # Logo (large, padded, vertically centered)
    if logo_path and os.path.exists(logo_path):
        logo_height = banner_height - 0.35 * inch
        logo_width = logo_height * 2.0  # assumes wide logo

        canvas.drawImage(
            logo_path,
            page_width - logo_width - 0.6 * inch,
            page_height - banner_height + 0.175 * inch,
            width=logo_width,
            height=logo_height,
            preserveAspectRatio=True,
            mask="auto"
        )

    canvas.restoreState()


def build_provider_pdf(
    provider_df,
    operational_market,
    operational_submarket,
    managing_entity,
    reporting_pod,
    provider,
    npi,
    header_text,
    report_period,
    logo_path=None,
    output_dir="output_pdfs"
    ):
    os.makedirs(output_dir, exist_ok=True)

    filename_parts = [
        operational_market,
        operational_submarket,
        managing_entity,
        reporting_pod,
        provider,
        npi
    ]

    filename = "_".join(safe_filename(p) for p in filename_parts) + ".pdf"
    filepath = os.path.join(output_dir, filename)

    doc = SimpleDocTemplate(
        filepath,
        pagesize=LETTER,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=1.9 * inch,   # space for banner
        bottomMargin=0.75 * inch
    )

    styles = getSampleStyleSheet()

    styles.add(ParagraphStyle(
        name="HeaderText",
        fontSize=12,
        leading=16,
        spaceAfter=14
    ))

    styles.add(ParagraphStyle(
        name="TableCell",
        fontSize=11,
        leading=15
    ))

    styles.add(ParagraphStyle(
        name="TableHeader",
        fontSize=12,
        leading=14,
        fontName="Helvetica-Bold"
    ))

    story = []

    # Header paragraph (page 1 only)
    story.append(Paragraph(header_text, styles["HeaderText"]))
    story.append(Spacer(1, 12))

    # --- Table headers (WITH heatmap header + legend) ---

    heatmap_header_cell = heatmap_header(
        report_period_year=report_period[-4:],  # e.g. "2026"
        styles=styles
    )

    table_data = [[
        Paragraph("Patient Details", styles["TableHeader"]),
        Paragraph("Adherence Breakdown - Failed Measure(s)", styles["TableHeader"]),
        Paragraph("PDC History<br/>24&nbsp;&nbsp;25&nbsp;&nbsp;26", styles["TableHeader"]),
    ]]

    for _, row in provider_df.iterrows():
        table_data.append([
            Paragraph(str(row["member detail"]), styles["TableCell"]),
            Paragraph(str(row["adherence breakdown"]), styles["TableCell"])
        ])

    usable_width = doc.width
    col_widths = [
        usable_width * 0.3,
        usable_width * 0.52
    ]

    table = Table(
        table_data,
        colWidths=col_widths,
        repeatRows=1
    )
    HEADER_BG = colors.HexColor("#F4F6F9")
    ROW_LINE = colors.HexColor("#E6E9EF")
    TEXT_DARK = colors.HexColor("#1F2933")

    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), HEADER_BG),
        ("TEXTCOLOR", (0, 0), (-1, 0), TEXT_DARK),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("LINEBELOW", (0, 0), (-1, 0), 1, colors.HexColor("#DADDE2")),
        ("LINEBELOW", (0, 1), (-1, -1), 0.25, colors.HexColor("#E6E9EF")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 10),
    ]))
    # --- Heatmap legend ABOVE the table, right-aligned ---

    legend_table = Table(
        [[ "", heatmap_legend() ]],
        colWidths=[doc.width - 130, 130]  # pushes legend to the right
    )
    
    legend_table.setStyle(TableStyle([
        ("ALIGN", (1, 0), (1, 0), "RIGHT"),
        ("VALIGN", (1, 0), (1, 0), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    
    story.append(legend_table)
    story.append(Spacer(1, 6))

    story.append(table)

    doc.build(
            story,
            onFirstPage=lambda c, d: first_page_header(
                c,
                d,
                provider=provider,
                reporting_pod=reporting_pod,
                report_period=REPORT_PERIOD,
                logo_path=logo_path
            )
        )


    print(f"Created: {filepath}")

data = {
    "OperationalMarket": ["East", "East"],
    "OperationalSubmarket": ["SubmarketA", "SubmarketA"],
    "ManagingEntity": ["EntityA", "EntityA"],
    "ReportingPod": ["PodA", "PodA"],
    "Provider": ["Dr. Jane Smith", "Dr. Jane Smith"],
    "NPI": ["1234567890", "7234547810"],
    "member detail": [
        "Member: Alice Smith<br/>DOB: 01/01/1980<br/>Risk Level: High",
        "Member: Bob Jones<br/>DOB: 06/12/1975<br/>Risk Level: Medium"
    ],
    "adherence breakdown": [
        "PDC: 72%<br/>Missed refills in last 60 days.<br/>Intervention recommended.",
        "PDC: 89%<br/>Consistent fills.<br/>Continue monitoring."
    ]
}

df = pd.DataFrame(data)

HEADER_TEXT = """
<b>Medication Adherence Review</b><br/>
<i>Confidential. For provider use only.</i><br/><br/>
This section summarizes member-level adherence information and highlights
opportunities for intervention.
"""

LOGO_PATH = "logo.png"  # set to None if not available
REPORT_PERIOD = "Jan 2026"

for (
    op_market,
    op_submarket,
    managing_entity,
    pod,
    provider,
    npi
), group_df in df.groupby([
    "OperationalMarket",
    "OperationalSubmarket",
    "ManagingEntity",
    "ReportingPod",
    "Provider",
    "NPI"
]):
    build_provider_pdf(
        provider_df=group_df,
        operational_market=op_market,
        operational_submarket=op_submarket,
        managing_entity=managing_entity,
        reporting_pod=pod,
        provider=provider,
        npi=npi,
        header_text=HEADER_TEXT,
        report_period=REPORT_PERIOD,
        logo_path=LOGO_PATH
    )

