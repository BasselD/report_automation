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

    # Two wide columns
    columns = ["Member Detail", "Adherence Breakdown"]

    table_data = [[
        Paragraph(col, styles["TableHeader"]) for col in columns
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

