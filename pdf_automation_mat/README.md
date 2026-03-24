# Medication Adherence Report Generator  v3

Batch PDF generator — one report per provider — driven entirely by your SQL dataframe.
**Stack:** Python · Jinja2 · Playwright (Chromium)

---

## Project Structure

```
med_adherence_report_v3/
├── main.py               # Entry point
├── report_builder.py     # DataFrame → HTML (minimal Python logic; trusts SQL fields)
├── pdf_converter.py      # Async Playwright batch engine
├── templates/
│   └── report.html       # Visual layout — edit CSS here
├── assets/
│   └── qr_code.png       # ← Place your QR code PNG here
├── data/
│   └── adherence_data.csv  # ← Place your SQL export here
├── requirements.txt
└── README.md
```

---

## Setup

```bash
python -m venv .venv
source .venv/bin/activate        # Mac/Linux
.venv\Scripts\activate          # Windows

pip install -r requirements.txt
playwright install chromium
```

---

## Run

```bash
python main.py \
  --input     data/adherence_data.csv \
  --output    output/pdfs \
  --concurrency 8 \
  --qr-code   assets/qr_code.png
```

### Output filenames
```
Market_SubMarket_ManagingEntity_ReportingPod_PCPName_PCPNPI.pdf
```

---

## Fields consumed directly from SQL (no Python logic needed)

| Column                    | Used For                                         |
|--------------------------|--------------------------------------------------|
| `AdherenceMeasureTag`    | RASA / DIAB / STAT badge color                   |
| `RecommendedActionBullet1` | First action bullet                            |
| `RecommendedActionBullet2` | Second action bullet                           |
| `RecommendedActionTags`  | Tier driver line (pipe-delimited tags)           |
| `MedicationAdherenceInbox` | Contact email in report header               |
| `MedicationAdherenceFax` | Fax number in report header                      |
| `DaysLatetoRefill`       | "Late: Xd" in Status col + Overdue pill count    |
| `DaysUntilUnrecoverable` | "Recoverable in Xd" in Status column             |
| `NonAdherenceRiskTier`   | HIGH / MED / LOW tier assignment                 |
| `CurrentPDC`             | PDC % bar and color coding                       |
| `Prior1YearPDC`          | Prior year adherence bar                         |
| `Prior2YearPDC`          | Two years ago adherence bar                      |
| `MemberName`, `MemberID`, `MemberDOB` | Patient row                       |
| `HealthPlanName`         | Patient row                                      |
| `LastFillMedicationName` | Medication name                                  |
| `LastPharmacyName`       | Pharmacy                                         |
| `LastPharmacyNPI`        | Pharmacy NPI in parens                           |
| `TotalUniqueFilICount`   | Fill count (note capital I in column name)       |
| `PCPName`, `PCPNPI`      | Header + filename                                |
| `Market`, `SubMarket`, `ManagingEntity`, `ReportingPod`, `Podname` | Filename + header |
| `ReportAnchorDate`       | Report date; Return-by = this date + 12 days     |

---

## ⚠ Missing Fields — Add to Your SQL Query

| Missing Field         | Impact                                      | Suggested SQL                                      |
|----------------------|---------------------------------------------|---------------------------------------------------|
| `MedicationRunoutDate` | "Due: MM/DD" shows "—" for all rows       | `DATEADD(day, LastDaysSupply, LastFillDate)`      |
| `MedicationRunoutDays` | "Due ≤7d" pill count shows 0              | `DATEDIFF(day, GETDATE(), MedicationRunoutDate)`  |
| `Prior2YearPDC` = 0  | Verify: real 0% PDC vs NULL coded as 0     | `NULLIF(Prior2YearPDC, 0)` if these are missing  |

---

## Customization

| What                    | Where                                     |
|------------------------|------------------------------------------|
| Return-by day offset   | `RETURN_BY_OFFSET_DAYS` in `report_builder.py` |
| Layout / styling       | `templates/report.html` (pure CSS)      |
| Tier driver formatting | `_tier_driver_text()` in `report_builder.py` |

---

## Performance (1,200 providers)

| `--concurrency` | Approx. Time |
|----------------|-------------|
| 4              | ~12 min     |
| 6              | ~8 min      |
| 8              | ~5 min      |

Reruns skip already-existing PDFs automatically.
