from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import LETTER
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas
import os

data = {
    "OperationalMarket": [
        "East", "West", "Midwest", "South", "East",
        "West", "South", "Midwest", "East", "West"
    ],
    "ReportingPod": [
        "Primary Care", "Cardiology", "Endocrinology", "Family Medicine", "Primary Care",
        "Cardiology", "Family Medicine", "Endocrinology", "Primary Care", "Cardiology"
    ],
    "Provider": [
        "Dr. Smith", "Dr. Lee", "Dr. Patel", "Dr. Johnson", "Dr. Smith",
        "Dr. Lee", "Dr. Johnson", "Dr. Patel", "Dr. Smith", "Dr. Lee"
    ],
    "MemberID": [
        "Member_001", "Member_002", "Member_003", "Member_004", "Member_005",
        "Member_006", "Member_007", "Member_008", "Member_009", "Member_010"
    ],
    "MemberName": [
        "Member_001", "Member_002", "Member_003", "Member_004", "Member_005",
        "Member_006", "Member_007", "Member_008", "Member_009", "Member_010"
    ],
    "DOB": [
        "1980-01-01", "1981-02-02", "1982-03-03", "1983-04-04", "1984-05-05",
        "1985-06-06", "1986-07-07", "1987-08-08", "1988-09-09", "1989-10-10"
    ],
    "RiskScore": [
        0.92, 0.76, 0.88, 0.64, 0.95,
        0.81, 0.69, 0.90, 0.73, 0.85
    ],
    "Notes": [
        "Note 1", "Note 2", "Note 3", "Note 4", "Note 5",
        "Note 6", "Note 7", "Note 8", "Note 9", "Note 10"
    ]
}

HEADER_TEXT = """
<b>Provider Performance Summary</b><br/>
<i>Confidential. For internal use only.</i><br/><br/>
This report contains a list of attributed members for the provider listed below.
Please review carefully and contact Analytics if discrepancies are identified.
"""

def first_page_header(canvas, doc, title, logo_path=None):
    canvas.saveState()

    banner_height = 1.2 * inch
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

    # Title
    canvas.setFillColor(colors.white)
    canvas.setFont("Helvetica-Bold", 20)
    canvas.drawString(
        0.75 * inch,
        page_height - 0.75 * inch,
        title
    )

    # Logo (right aligned)
    if logo_path and os.path.exists(logo_path):
        canvas.drawImage(
            logo_path,
            page_width - 1.75 * inch,
            page_height - 1.0 * inch,
            width=1.0 * inch,
            height=0.6 * inch,
            preserveAspectRatio=True,
            mask="auto"
        )

    canvas.restoreState()

def build_provider_pdf(
    provider_df,
    operational_market,
    reporting_pod,
    provider,
    header_text,
    logo_path=None,
    output_dir="output_pdfs"
):
    os.makedirs(output_dir, exist_ok=True)

    filename = f"{operational_market}_{reporting_pod}_{provider}.pdf"
    filepath = os.path.join(output_dir, filename)

    doc = SimpleDocTemplate(
        filepath,
        pagesize=LETTER,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=1.75 * inch,  # leave room for banner
        bottomMargin=0.75 * inch
    )

    styles = getSampleStyleSheet()

    styles.add(ParagraphStyle(
        name="HeaderText",
        fontSize=12,
        leading=16,
        spaceAfter=12
    ))

    styles.add(ParagraphStyle(
        name="TableCell",
        fontSize=10,
        leading=14
    ))

    story = []

    # Header text (only appears once)
    story.append(Paragraph(header_text, styles["HeaderText"]))
    story.append(Spacer(1, 12))

    # Table columns
    columns = [
        "MemberID",
        "MemberName",
        "DOB",
        "RiskScore",
        "Notes"
    ]

    # Prepare table data with wrapped paragraphs
    table_data = [[
        Paragraph(col, styles["TableCell"]) for col in columns
    ]]

    for _, row in provider_df.iterrows():
        table_data.append([
            Paragraph(str(row[col]), styles["TableCell"]) for col in columns
        ])

    # Column widths sum to usable page width
    usable_width = doc.width
    col_widths = [
        1.1 * inch,   # MemberID
        1.6 * inch,   # Name
        1.0 * inch,   # DOB
        0.9 * inch,   # Risk
        usable_width - (1.1 + 1.6 + 1.0 + 0.9) * inch  # Notes
    ]

    table = Table(
        table_data,
        colWidths=col_widths,
        repeatRows=1
    )

    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 10),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
    ]))

    story.append(table)

    doc.build(
        story,
        onFirstPage=lambda c, d: first_page_header(
            c,
            d,
            title=f"{provider} – Member Roster",
            logo_path=logo_path
        )
    )

    return filepath

LOGO_PATH = "careallies_logo.png"  # optional. set to None if you don’t have one

for (op_market, pod, provider), group_df in df.groupby(
    ["OperationalMarket", "ReportingPod", "Provider"]
):
    build_provider_pdf(
        provider_df=group_df,
        operational_market=op_market,
        reporting_pod=pod,
        provider=provider,
        header_text=HEADER_TEXT,
        logo_path=LOGO_PATH
    )
