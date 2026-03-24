"""
report_builder.py  ── v3
Transforms a provider-level DataFrame into the Jinja2 template context.

Fields consumed directly from the SQL dataframe (no Python derivation):
  AdherenceMeasureTag       → RASA / DIAB / STAT badge
  RecommendedActionBullet1  → First action bullet
  RecommendedActionBullet2  → Second action bullet
  RecommendedActionTags     → Tier driver line (pipe-delimited tags from SQL)
  MedicationAdherenceInbox  → Contact email in header
  MedicationAdherenceFax    → Fax in header
  DaysLatetoRefill          → "Late: Xd" in Status column + Overdue pill count
  DaysUntilUnrecoverable    → "Recoverable in Xd" in Status column

Fields still derived in Python (SQL does not provide them):
  return_by_date            → ReportAnchorDate + RETURN_BY_OFFSET_DAYS
  due_date display          → MedicationRunoutDate if present, else "—"
  due_7d_count pill         → MedicationRunoutDays 0–7 if present, else 0 (see MISSING FIELDS note)
  adherence history bars    → Prior2YearPDC, Prior1YearPDC, CurrentPDC

MISSING FIELDS (add to SQL query for full functionality):
  MedicationRunoutDate  → needed for "Due: MM/DD" in Status column
  MedicationRunoutDays  → needed for "Due ≤7d" pill count in metrics bar
                          Suggested SQL: DATEDIFF(day, GETDATE(), MedicationRunoutDate)
  Prior2YearPDC         → confirm 0 = true zero vs NULL; cast NULLs explicitly in SQL
"""

from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from jinja2 import Environment, FileSystemLoader

TEMPLATE_DIR          = Path(__file__).parent / "templates"
RETURN_BY_OFFSET_DAYS = 12   # report_date + N days → "Return report by" header field


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe(val, default="—"):
    if pd.isna(val): return default
    s = str(val).strip()
    return s if s else default

def _fmt_date(val, fmt="%m/%d"):
    if pd.isna(val): return "—"
    if isinstance(val, (datetime, date)): return val.strftime(fmt)
    try:    return pd.to_datetime(val).strftime(fmt)
    except: return str(val)

def _pdc_class(pdc):
    try:    v = float(pdc)
    except: return "pdc-low"
    if v >= 0.80: return "pdc-high"
    if v >= 0.60: return "pdc-med"
    return "pdc-low"

def _bar_class(pct):
    if pct is None: return "bar-missing"
    if pct >= 80:   return "bar-80plus"
    if pct >= 60:   return "bar-60to79"
    return "bar-below60"

def _to_int(val, default=0):
    try:    return int(float(val))
    except: return default

def _to_float(val, default=None):
    try:    return float(val)
    except: return default

def _tier_label(tier):
    return {
        "HIGH": "Borderline Compliance",
        "MED":  "Approaching Borderline",
        "LOW":  "Emerging Non-Compliance",
    }.get(tier, "")

def _build_history(row):
    """3-year adherence bars from Prior2YearPDC, Prior1YearPDC, CurrentPDC."""
    cur_year = date.today().year
    entries  = [
        (cur_year - 2, row.get("Prior2YearPDC")),
        (cur_year - 1, row.get("Prior1YearPDC")),
        (cur_year,     row.get("CurrentPDC")),
    ]
    hist = []
    for yr, raw in entries:
        f = _to_float(raw)
        # Treat 0.0 as valid zero PDC; treat NaN/None as missing
        pct = None if f is None else round(f * 100)
        hist.append({"year": yr, "pct": pct, "bar_class": _bar_class(pct)})
    return hist

def _recoverable_days(row):
    """Read DaysUntilUnrecoverable directly from SQL field."""
    val = row.get("DaysUntilUnrecoverable")
    if pd.isna(val): return None
    return max(0, _to_int(val))

def _days_late(row):
    """
    Use DaysLatetoRefill (SQL field) as primary source.
    Positive value = member is overdue by that many days.
    """
    val = row.get("DaysLatetoRefill")
    if pd.isna(val): return 0
    return max(0, _to_int(val))

def _tier_driver_text(row):
    """
    RecommendedActionTags is pipe-delimited: "Reversals>2 | Avg PDC < 80% | Comorbidity"
    Display as-is; replace pipes with bullet separators for readability.
    """
    raw = _safe(row.get("RecommendedActionTags"), default="")
    if not raw or raw == "—": return ""
    # Replace " | " with "  ·  " for a cleaner inline display
    return raw.replace(" | ", "  ·  ")


# ── Row transformer ───────────────────────────────────────────────────────────

def _transform_row(row):
    pdc_raw  = row.get("CurrentPDC")
    pdc_f    = _to_float(pdc_raw, default=0.0)
    pdc_pct  = round(pdc_f * 100)
    tier     = _safe(row.get("NonAdherenceRiskTier"), default="LOW").upper()

    return {
        "tier":            tier,
        "tier_label":      _tier_label(tier),
        "member_name":     _safe(row.get("MemberName")),
        "member_id":       _safe(row.get("MemberID")),
        "dob":             _fmt_date(row.get("MemberDOB")),
        "health_plan":     _safe(row.get("HealthPlanName")),
        # AdherenceMeasureTag comes directly from SQL
        "measure_tag":     _safe(row.get("AdherenceMeasureTag"), default="RASA").upper(),
        "medication":      _safe(row.get("LastFillMedicationName")),
        "pharmacy":        _safe(row.get("LastPharmacyName")),
        "pharmacy_npi":    _safe(row.get("LastPharmacyNPI"), default=""),
        "pdc_pct":         pdc_pct,
        "pdc_class":       _pdc_class(pdc_raw),
        "fill_count":      _to_int(row.get("TotalUniqueFilICount")),
        "days_late":       _days_late(row),
        # MedicationRunoutDate from SQL (may be null — shows "—" gracefully)
        "due_date":        _fmt_date(row.get("MedicationRunoutDate")),
        "recoverable_days": _recoverable_days(row),
        # Action bullets come directly from SQL
        "action1":         _safe(row.get("RecommendedActionBullet1"), default=""),
        "action2":         _safe(row.get("RecommendedActionBullet2"), default=""),
        # Tier driver tags come directly from SQL
        "tier_driver":     _tier_driver_text(row),
        "history":         _build_history(row),
    }


# ── Provider-level metrics ────────────────────────────────────────────────────

def _compute_metrics(df):
    tiers     = df["NonAdherenceRiskTier"].str.upper() if "NonAdherenceRiskTier" in df.columns else pd.Series(dtype=str)
    late_days = pd.to_numeric(df.get("DaysLatetoRefill",  pd.Series(dtype=float)), errors="coerce").fillna(0)
    # MedicationRunoutDays: negative=overdue, 0-7=due soon — use if present, else fall back to DaysLatetoRefill
    runout    = pd.to_numeric(df.get("MedicationRunoutDays", pd.Series(dtype=float)), errors="coerce")
    has_runout = runout.notna().any()

    if has_runout:
        overdue_count = int((runout < 0).sum())
        due_7d_count  = int(runout.between(0, 7).sum())
    else:
        # Fallback: DaysLatetoRefill > 0 = overdue; due_7d not computable without RunoutDays
        overdue_count = int((late_days > 0).sum())
        due_7d_count  = 0  # Cannot determine without MedicationRunoutDays

    return {
        "total_patients": len(df),
        "high_count":     int((tiers == "HIGH").sum()),
        "med_count":      int((tiers == "MED").sum()),
        "low_count":      int((tiers == "LOW").sum()),
        "overdue_count":  overdue_count,
        "due_7d_count":   due_7d_count,
    }


# ── Main entry point ──────────────────────────────────────────────────────────

def build_report_html(
    provider_df,
    qr_code_path = None,
):
    """
    Render a complete HTML report for one provider group.

    Args:
        provider_df:   DataFrame slice for a single PCPName/PCPNPI group.
        qr_code_path:  Absolute path to QR code PNG, or None for placeholder.

    Returns:
        HTML string ready for Playwright PDF conversion.
    """
    TIER_ORDER = {"HIGH": 0, "MED": 1, "LOW": 2}
    df = provider_df.copy()
    df["_ts"] = df["NonAdherenceRiskTier"].str.upper().map(TIER_ORDER).fillna(9)
    df["_ps"] = pd.to_numeric(df["CurrentPDC"], errors="coerce").fillna(1.0)
    df = df.sort_values(["_ts", "_ps"]).drop(columns=["_ts", "_ps"])

    first = df.iloc[0]

    # Report date from ReportAnchorDate
    anchor     = first.get("ReportAnchorDate")
    report_dt  = pd.to_datetime(anchor, errors="coerce") if not pd.isna(anchor) else datetime.today()
    report_date    = report_dt.strftime("%m/%d/%Y")
    return_by_date = (report_dt + timedelta(days=RETURN_BY_OFFSET_DAYS)).strftime("%m/%d/%Y")

    # Contact info from SQL fields (per-provider row)
    contact_email = _safe(first.get("MedicationAdherenceInbox"), default="Pharmacy@CareAllies.com")
    contact_fax   = _safe(first.get("MedicationAdherenceFax"),   default="1-855-201-2678")

    context = {
        "reporting_pod":   _safe(first.get("Podname")),
        "pcp_name":        _safe(first.get("PCPName")),
        "pcp_npi":         _safe(first.get("PCPNPI")),
        "report_date":     report_date,
        "return_by_date":  return_by_date,
        "contact_email":   contact_email,
        "contact_fax":     contact_fax,
        "qr_code_path":    qr_code_path,
        **_compute_metrics(df),
        "patients": [_transform_row(row) for _, row in df.iterrows()],
    }

    env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))
    return env.get_template("report.html").render(**context)
