from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable

import duckdb
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# -----------------------------
# Configuration
# -----------------------------
DB_PATH = "hospital_utilization.duckdb"   # change to your DuckDB file
TABLE_NAME = "auth_events"                # change to your table/view name
APP_TITLE = "Hospital Observation vs Acute Utilization"
APP_SUBTITLE = (
    "Authorization-backed event view with claims support indicators and inferred attending-provider utilization patterns."
)

# Brand / UI colors
BG = "#F7F9FC"
CARD = "#FFFFFF"
TEXT = "#0F172A"
MUTED = "#64748B"
BORDER = "#E2E8F0"
ACCENT = "#0EA5A5"
ACCENT_2 = "#2563EB"
OBS_COLOR = "#F59E0B"
ACUTE_COLOR = "#0EA5A5"
SUPPORT_COLOR = "#2563EB"
NO_SUPPORT_COLOR = "#CBD5E1"
READMIT_COLOR = "#DC2626"

st.set_page_config(
    page_title=APP_TITLE,
    layout="wide",
    initial_sidebar_state="expanded",
)


# -----------------------------
# Styling
# -----------------------------
def inject_css() -> None:
    st.markdown(
        f"""
        <style>
            .stApp {{
                background: {BG};
                color: {TEXT};
            }}
            .block-container {{
                padding-top: 1.2rem;
                padding-bottom: 1.5rem;
                max-width: 1500px;
            }}
            h1, h2, h3 {{
                color: {TEXT};
                letter-spacing: -0.02em;
            }}
            .app-shell {{
                padding: 0.25rem 0 0.5rem 0;
            }}
            .hero-card {{
                background: linear-gradient(135deg, #ffffff 0%, #f8fbff 100%);
                border: 1px solid {BORDER};
                border-radius: 22px;
                padding: 1.2rem 1.35rem 1rem 1.35rem;
                box-shadow: 0 10px 30px rgba(15, 23, 42, 0.05);
                margin-bottom: 1rem;
            }}
            .hero-kicker {{
                color: {ACCENT_2};
                font-size: 0.78rem;
                font-weight: 700;
                letter-spacing: 0.08em;
                text-transform: uppercase;
                margin-bottom: 0.35rem;
            }}
            .hero-title {{
                font-size: 2rem;
                font-weight: 800;
                line-height: 1.05;
                margin-bottom: 0.45rem;
            }}
            .hero-subtitle {{
                color: {MUTED};
                font-size: 0.96rem;
                max-width: 1000px;
                line-height: 1.5;
            }}
            .metric-row {{
                display: grid;
                grid-template-columns: repeat(6, minmax(0, 1fr));
                gap: 0.8rem;
                margin-top: 1rem;
            }}
            .metric-card {{
                background: {CARD};
                border: 1px solid {BORDER};
                border-radius: 18px;
                padding: 0.95rem 1rem;
                box-shadow: 0 8px 22px rgba(15, 23, 42, 0.04);
            }}
            .metric-label {{
                color: {MUTED};
                font-size: 0.75rem;
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: 0.04em;
                margin-bottom: 0.3rem;
            }}
            .metric-value {{
                color: {TEXT};
                font-size: 1.55rem;
                font-weight: 800;
                line-height: 1.1;
                margin-bottom: 0.2rem;
            }}
            .metric-sub {{
                color: {MUTED};
                font-size: 0.8rem;
            }}
            .section-card {{
                background: {CARD};
                border: 1px solid {BORDER};
                border-radius: 22px;
                padding: 1rem 1rem 0.6rem 1rem;
                box-shadow: 0 10px 30px rgba(15, 23, 42, 0.05);
                margin-bottom: 1rem;
            }}
            .section-header {{
                display: flex;
                justify-content: space-between;
                align-items: flex-start;
                gap: 1rem;
                margin-bottom: 0.25rem;
            }}
            .section-title {{
                font-size: 1.1rem;
                font-weight: 800;
                color: {TEXT};
                margin-bottom: 0.15rem;
            }}
            .section-desc {{
                color: {MUTED};
                font-size: 0.87rem;
                line-height: 1.45;
                margin-bottom: 0.5rem;
            }}
            .note-chip {{
                display: inline-block;
                border: 1px solid {BORDER};
                color: {MUTED};
                background: #FAFCFF;
                border-radius: 999px;
                padding: 0.35rem 0.7rem;
                font-size: 0.75rem;
                font-weight: 600;
                white-space: nowrap;
            }}
            section[data-testid="stSidebar"] {{
                background: #FBFCFE;
                border-right: 1px solid {BORDER};
            }}
            .small-footnote {{
                color: {MUTED};
                font-size: 0.78rem;
                margin-top: 0.2rem;
                margin-bottom: 0.4rem;
            }}
            .stDataFrame, .stPlotlyChart {{
                border-radius: 14px;
            }}
            @media (max-width: 1200px) {{
                .metric-row {{
                    grid-template-columns: repeat(3, minmax(0, 1fr));
                }}
            }}
            @media (max-width: 740px) {{
                .metric-row {{
                    grid-template-columns: repeat(2, minmax(0, 1fr));
                }}
            }}
        </style>
        """,
        unsafe_allow_html=True,
    )


# -----------------------------
# Helpers
# -----------------------------
EXPECTED_COLUMNS = [
    "BedType",
    "OperationalMarket",
    "OperationalSubMarket",
    "ReportingPod",
    "ManagingEntity",
    "ManagingProviderName",
    "PodCd",
    "PodName",
    "PCPName",
    "PCPNPI",
    "AuthznKey",
    "MemberID",
    "Admit",
    "Obs_Match_Method",
    "Obs_Claim_Indicator",
    "Obs_Date",
    "Active",
    "Discharge",
    "AdmittingProviderID",
    "AdmittingProviderName",
    "AdmittingProviderNPI",
    "AdmissionReason",
    "Expired",
    "MemberDOB",
    "HCODE",
    "PlanType",
    "AdmitsDischargeError",
    "PrimaryDiagnosisCode",
    "PrimaryDiagnosis",
    "LACE",
    "AdmissionType",
    "AdmitFrom",
    "DischargeStatusInd",
    "DischargeStatusCode",
    "DischargeStatusDescription",
    "Readmit",
    "ReadmitDenominator",
    "ReadmitDenominatorExclusionReason",
    "LengthofStay",
    "AttendingProvider_Level1Bucket",
    "AttendingProvider_Level2Bucket",
    "AttendingProvider_Level3Bucket",
    "AttendingProvider_DOSbegin",
    "AttendingProvider_ProviderID",
    "AttendingProvider_ProviderName",
    "AttendingProvider_ProviderNPI",
    "AttendingProvider_ProviderTIN",
    "AttendingProvider_ProviderSpecialtyCode",
    "AttendingProvider_ProviderSpecialtyDesc",
]

DISPLAY_NAMES = {
    "AdmittingProviderName": "Facility",
    "AttendingProvider_ProviderName": "Attending Provider",
    "PrimaryDiagnosis": "Primary Diagnosis",
    "PCPName": "PCP",
    "ReportingPod": "Reporting Pod",
}


def format_int(value: float | int | None) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "-"
    return f"{int(round(value)):,}"


def format_pct(value: float | int | None) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "-"
    return f"{value:.1%}"


def format_num(value: float | int | None, digits: int = 1) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "-"
    return f"{value:,.{digits}f}"


def standardize_bedtype(series: pd.Series) -> pd.Series:
    values = series.fillna("Unknown").astype(str).str.strip().str.lower()
    mapped = np.select(
        [
            values.str.contains("obs"),
            values.str.contains("acute|inpatient|ip"),
        ],
        ["Observation", "Acute"],
        default="Other",
    )
    return pd.Series(mapped, index=series.index)


def standardize_claim_support(df: pd.DataFrame) -> pd.Series:
    raw = df.get("Obs_Claim_Indicator", pd.Series(index=df.index, dtype="object")).fillna("").astype(str).str.strip().str.lower()
    method = df.get("Obs_Match_Method", pd.Series(index=df.index, dtype="object")).fillna("").astype(str).str.strip().str.lower()

    supported = (
        raw.isin(["1", "y", "yes", "true", "supported", "match", "matched"]) |
        raw.str.contains("found|match|claim") |
        method.str.contains("exact|fuzzy|match|claim found|supported")
    )
    not_supported = (
        raw.isin(["0", "n", "no", "false", "not found", "none"]) |
        raw.str.contains("not found|no obs found|no claim") |
        method.str.contains("no obs found|no claim|not found")
    )

    return pd.Series(
        np.select([supported, not_supported], ["Supported in Claims", "Not Found in Claims"], default="Unknown"),
        index=df.index,
    )


def standardize_readmit(series: pd.Series) -> pd.Series:
    raw = series.fillna(0)
    return pd.Series(np.where(raw.astype(str).isin(["1", "Y", "y", "True", "true"]), 1, 0), index=series.index)


def ensure_columns(df: pd.DataFrame, required: Iterable[str]) -> list[str]:
    return [c for c in required if c not in df.columns]


@st.cache_data(show_spinner=False)
def load_data(db_path: str, table_name: str) -> pd.DataFrame:
    query = f"SELECT * FROM {table_name}"
    with duckdb.connect(db_path, read_only=True) as con:
        df = con.execute(query).df()
    return df


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    for date_col in ["Admit", "Discharge", "Obs_Date", "AttendingProvider_DOSbegin", "MemberDOB"]:
        if date_col in out.columns:
            out[date_col] = pd.to_datetime(out[date_col], errors="coerce")

    if "LengthofStay" in out.columns:
        out["LengthofStay"] = pd.to_numeric(out["LengthofStay"], errors="coerce")
    else:
        out["LengthofStay"] = np.nan

    if "LACE" in out.columns:
        out["LACE"] = pd.to_numeric(out["LACE"], errors="coerce")
    else:
        out["LACE"] = np.nan

    out["EventType"] = standardize_bedtype(out["BedType"]) if "BedType" in out.columns else "Unknown"
    out["ClaimSupportStatus"] = standardize_claim_support(out)
    out["ReadmitFlag"] = standardize_readmit(out.get("Readmit", pd.Series(index=out.index, dtype="object")))

    if "Admit" in out.columns:
        out["AdmitMonth"] = out["Admit"].dt.to_period("M").dt.to_timestamp()
    else:
        out["AdmitMonth"] = pd.NaT

    out["LOSBand"] = pd.cut(
        out["LengthofStay"],
        bins=[-np.inf, 1, 3, 6, np.inf],
        labels=["0–1 days", "2–3 days", "4–6 days", "7+ days"],
    )
    out["LACEBand"] = pd.cut(
        out["LACE"],
        bins=[-np.inf, 4, 9, 14, np.inf],
        labels=["0–4", "5–9", "10–14", "15+"],
    )

    out["Facility"] = out.get("AdmittingProviderName", pd.Series(index=out.index, dtype="object")).fillna("Unknown")
    out["AttendingProvider"] = out.get("AttendingProvider_ProviderName", pd.Series(index=out.index, dtype="object")).fillna("Unknown")
    out["Diagnosis"] = out.get("PrimaryDiagnosis", pd.Series(index=out.index, dtype="object")).fillna("Unknown")
    out["PCP"] = out.get("PCPName", pd.Series(index=out.index, dtype="object")).fillna("Unknown")
    out["Pod"] = out.get("ReportingPod", pd.Series(index=out.index, dtype="object")).fillna("Unknown")

    return out


def filter_frame(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.markdown("### Filters")

    def multiselect_with_all(label: str, column: str) -> list[str]:
        if column not in df.columns:
            return []
        opts = sorted([str(v) for v in df[column].dropna().astype(str).unique() if str(v).strip()])
        return st.sidebar.multiselect(label, options=opts, default=[])

    min_dt = pd.to_datetime(df["Admit"], errors="coerce").min() if "Admit" in df.columns else None
    max_dt = pd.to_datetime(df["Admit"], errors="coerce").max() if "Admit" in df.columns else None
    if pd.notna(min_dt) and pd.notna(max_dt):
        date_range = st.sidebar.date_input(
            "Admit date range",
            value=(min_dt.date(), max_dt.date()),
            min_value=min_dt.date(),
            max_value=max_dt.date(),
        )
    else:
        date_range = None

    market = multiselect_with_all("Operational Market", "OperationalMarket")
    submarket = multiselect_with_all("Operational SubMarket", "OperationalSubMarket")
    pod = multiselect_with_all("Reporting Pod", "ReportingPod")
    plan = multiselect_with_all("Plan Type", "PlanType")
    event_type = st.sidebar.multiselect(
        "Event Type",
        options=sorted(df["EventType"].dropna().unique().tolist()),
        default=[],
    )
    facility = multiselect_with_all("Facility", "Facility")
    provider = multiselect_with_all("Attending Provider", "AttendingProvider")

    out = df.copy()

    if date_range and len(date_range) == 2 and "Admit" in out.columns:
        start, end = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
        out = out[out["Admit"].between(start, end)]
    for col, selected in [
        ("OperationalMarket", market),
        ("OperationalSubMarket", submarket),
        ("ReportingPod", pod),
        ("PlanType", plan),
        ("EventType", event_type),
        ("Facility", facility),
        ("AttendingProvider", provider),
    ]:
        if selected and col in out.columns:
            out = out[out[col].astype(str).isin(selected)]

    st.sidebar.markdown("---")
    st.sidebar.caption("Event counts are based on authorization records. Claims fields indicate whether supporting claim activity was identified.")
    return out


def add_section_header(title: str, description: str, chip: str) -> None:
    st.markdown(
        f"""
        <div class='section-header'>
            <div>
                <div class='section-title'>{title}</div>
                <div class='section-desc'>{description}</div>
            </div>
            <div class='note-chip'>{chip}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def metric_cards(df: pd.DataFrame) -> None:
    total_events = len(df)
    obs_events = int((df["EventType"] == "Observation").sum())
    acute_events = int((df["EventType"] == "Acute").sum())
    supported = int((df["ClaimSupportStatus"] == "Supported in Claims").sum())
    avg_los = df["LengthofStay"].mean()
    readmit_rate = df["ReadmitFlag"].mean() if len(df) else np.nan

    cards = [
        ("Total Events", format_int(total_events), "Authorization-backed denominator"),
        ("Observation", format_int(obs_events), format_pct(obs_events / total_events) if total_events else "-"),
        ("Acute", format_int(acute_events), format_pct(acute_events / total_events) if total_events else "-"),
        ("Claims Supported", format_int(supported), format_pct(supported / total_events) if total_events else "-"),
        ("Avg LOS", format_num(avg_los, 1), "Days"),
        ("Readmit Rate", format_pct(readmit_rate), "From event table"),
    ]

    cards_html = "".join(
        [
            f"""
            <div class='metric-card'>
                <div class='metric-label'>{label}</div>
                <div class='metric-value'>{value}</div>
                <div class='metric-sub'>{sub}</div>
            </div>
            """
            for label, value, sub in cards
        ]
    )
    st.markdown(f"<div class='metric-row'>{cards_html}</div>", unsafe_allow_html=True)


def blank_fig(message: str) -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(
        x=0.5,
        y=0.5,
        text=message,
        showarrow=False,
        font=dict(size=16, color=MUTED),
        xref="paper",
        yref="paper",
    )
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    fig.update_layout(height=320, paper_bgcolor=CARD, plot_bgcolor=CARD, margin=dict(l=10, r=10, t=10, b=10))
    return fig


def apply_fig_style(fig: go.Figure, height: int = 360) -> go.Figure:
    fig.update_layout(
        height=height,
        paper_bgcolor=CARD,
        plot_bgcolor=CARD,
        margin=dict(l=18, r=18, t=32, b=18),
        font=dict(color=TEXT, family="Arial"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_yaxes(gridcolor="#E5E7EB", zeroline=False)
    return fig


def build_trend_chart(df: pd.DataFrame) -> go.Figure:
    trend = (
        df.dropna(subset=["AdmitMonth"])
        .groupby(["AdmitMonth", "EventType"], as_index=False)
        .size()
        .rename(columns={"size": "Events"})
    )
    if trend.empty:
        return blank_fig("No monthly trend data available")

    fig = px.line(
        trend,
        x="AdmitMonth",
        y="Events",
        color="EventType",
        markers=True,
        color_discrete_map={"Observation": OBS_COLOR, "Acute": ACUTE_COLOR, "Other": ACCENT_2},
    )
    fig.update_traces(line=dict(width=3), marker=dict(size=7))
    fig.update_layout(title="Monthly Event Trend")
    return apply_fig_style(fig)


def build_claim_support_chart(df: pd.DataFrame) -> go.Figure:
    support = (
        df.groupby(["EventType", "ClaimSupportStatus"], as_index=False)
        .size()
        .rename(columns={"size": "Events"})
    )
    if support.empty:
        return blank_fig("No claims support data available")

    totals = support.groupby("EventType")["Events"].transform("sum")
    support["Pct"] = support["Events"] / totals

    fig = px.bar(
        support,
        x="EventType",
        y="Pct",
        color="ClaimSupportStatus",
        text=support["Pct"].map(lambda x: f"{x:.0%}"),
        barmode="stack",
        color_discrete_map={
            "Supported in Claims": SUPPORT_COLOR,
            "Not Found in Claims": NO_SUPPORT_COLOR,
            "Unknown": "#94A3B8",
        },
    )
    fig.update_layout(title="Claims Support by Event Type", yaxis_tickformat=".0%")
    return apply_fig_style(fig)


def build_facility_chart(df: pd.DataFrame, top_n: int = 10) -> go.Figure:
    summary = (
        df.groupby(["Facility", "EventType"], as_index=False)
        .size()
        .rename(columns={"size": "Events"})
    )
    if summary.empty:
        return blank_fig("No facility data available")

    top_facilities = (
        df.groupby("Facility", as_index=False)
        .size()
        .rename(columns={"size": "Events"})
        .sort_values("Events", ascending=False)
        .head(top_n)["Facility"]
        .tolist()
    )
    summary = summary[summary["Facility"].isin(top_facilities)]

    fig = px.bar(
        summary,
        y="Facility",
        x="Events",
        color="EventType",
        orientation="h",
        barmode="stack",
        color_discrete_map={"Observation": OBS_COLOR, "Acute": ACUTE_COLOR, "Other": ACCENT_2},
    )
    fig.update_layout(title="Top Facilities by Event Volume", yaxis=dict(categoryorder="total ascending"))
    return apply_fig_style(fig)


def build_provider_scatter(df: pd.DataFrame, min_events: int = 5, top_n: int = 60) -> go.Figure:
    provider = (
        df.groupby("AttendingProvider", as_index=False)
        .agg(
            Events=("MemberID", "size"),
            ObsRate=("EventType", lambda s: (s == "Observation").mean()),
            AvgLOS=("LengthofStay", "mean"),
            ReadmitRate=("ReadmitFlag", "mean"),
            ClaimsSupportRate=("ClaimSupportStatus", lambda s: (s == "Supported in Claims").mean()),
        )
        .query("Events >= @min_events")
        .sort_values("Events", ascending=False)
        .head(top_n)
    )
    if provider.empty:
        return blank_fig("Not enough provider volume for scatter plot")

    fig = px.scatter(
        provider,
        x="Events",
        y="ObsRate",
        size="AvgLOS",
        color="ClaimsSupportRate",
        hover_name="AttendingProvider",
        hover_data={
            "AvgLOS": ':.1f',
            "ReadmitRate": ':.1%',
            "ClaimsSupportRate": ':.1%',
            "Events": True,
            "ObsRate": ':.1%',
        },
        color_continuous_scale=[[0, "#CBD5E1"], [0.5, "#60A5FA"], [1, "#1D4ED8"]],
    )
    fig.update_layout(title="Attending Provider Variation", yaxis_tickformat=".0%", coloraxis_colorbar_title="Claims support")
    fig.update_yaxes(title="Observation Rate")
    fig.update_xaxes(title="Event Volume")
    return apply_fig_style(fig)


def build_diagnosis_chart(df: pd.DataFrame, top_n: int = 10) -> go.Figure:
    diag = (
        df.groupby(["Diagnosis", "EventType"], as_index=False)
        .size()
        .rename(columns={"size": "Events"})
    )
    if diag.empty:
        return blank_fig("No diagnosis data available")

    top_dx = (
        df.groupby("Diagnosis", as_index=False)
        .size()
        .rename(columns={"size": "Events"})
        .sort_values("Events", ascending=False)
        .head(top_n)["Diagnosis"]
        .tolist()
    )
    diag = diag[diag["Diagnosis"].isin(top_dx)]

    fig = px.bar(
        diag,
        y="Diagnosis",
        x="Events",
        color="EventType",
        orientation="h",
        barmode="stack",
        color_discrete_map={"Observation": OBS_COLOR, "Acute": ACUTE_COLOR, "Other": ACCENT_2},
    )
    fig.update_layout(title="Top Diagnoses Driving Utilization", yaxis=dict(categoryorder="total ascending"))
    return apply_fig_style(fig)


def build_los_chart(df: pd.DataFrame) -> go.Figure:
    los = (
        df.dropna(subset=["LOSBand"])
        .groupby(["LOSBand", "EventType"], observed=False, as_index=False)
        .size()
        .rename(columns={"size": "Events"})
    )
    if los.empty:
        return blank_fig("No length-of-stay data available")
    fig = px.bar(
        los,
        x="LOSBand",
        y="Events",
        color="EventType",
        barmode="group",
        color_discrete_map={"Observation": OBS_COLOR, "Acute": ACUTE_COLOR, "Other": ACCENT_2},
    )
    fig.update_layout(title="Length of Stay Profile")
    return apply_fig_style(fig)


def build_detail_table(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "Admit",
        "Discharge",
        "EventType",
        "ClaimSupportStatus",
        "OperationalMarket",
        "OperationalSubMarket",
        "ReportingPod",
        "PCPName",
        "Facility",
        "AttendingProvider",
        "AttendingProvider_ProviderSpecialtyDesc",
        "PrimaryDiagnosisCode",
        "Diagnosis",
        "LengthofStay",
        "LACE",
        "Readmit",
        "Obs_Match_Method",
        "Obs_Date",
        "AuthznKey",
        "MemberID",
    ]
    existing = [c for c in cols if c in df.columns]
    table = df[existing].copy()
    rename_map = {
        "EventType": "Event Type",
        "ClaimSupportStatus": "Claim Support",
        "OperationalMarket": "Market",
        "OperationalSubMarket": "SubMarket",
        "ReportingPod": "Reporting Pod",
        "PCPName": "PCP",
        "LengthofStay": "LOS",
        "Diagnosis": "Primary Diagnosis",
        "AuthznKey": "Auth Key",
    }
    table = table.rename(columns=rename_map)
    if "Admit" in table.columns:
        table["Admit"] = pd.to_datetime(table["Admit"], errors="coerce").dt.date
    if "Discharge" in table.columns:
        table["Discharge"] = pd.to_datetime(table["Discharge"], errors="coerce").dt.date
    if "Obs_Date" in table.columns:
        table["Obs_Date"] = pd.to_datetime(table["Obs_Date"], errors="coerce").dt.date
    return table.sort_values(by=[c for c in ["Admit", "Facility"] if c in table.columns], ascending=[False, True][:len([c for c in ["Admit", "Facility"] if c in table.columns])])


# -----------------------------
# Main app
# -----------------------------
def main() -> None:
    inject_css()

    st.markdown("<div class='app-shell'>", unsafe_allow_html=True)
    st.markdown(
        f"""
        <div class='hero-card'>
            <div class='hero-kicker'>One-page Streamlit Command Center</div>
            <div class='hero-title'>{APP_TITLE}</div>
            <div class='hero-subtitle'>{APP_SUBTITLE}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.expander("Data source settings", expanded=False):
        st.code(
            f"DB_PATH = '{DB_PATH}'\nTABLE_NAME = '{TABLE_NAME}'",
            language="python",
        )
        st.caption("Update those two constants at the top of the script to point at your DuckDB extract and event table or view.")

    try:
        raw = load_data(DB_PATH, TABLE_NAME)
    except Exception as exc:
        st.error(
            "Could not load the DuckDB extract. Update DB_PATH and TABLE_NAME at the top of the script.\n\n"
            f"Error: {exc}"
        )
        st.stop()

    missing = ensure_columns(raw, ["BedType", "Admit", "LengthofStay", "Readmit", "AdmittingProviderName", "AttendingProvider_ProviderName", "PrimaryDiagnosis"])
    if missing:
        st.warning("Some expected columns are missing. The app will still run, but a few visuals may be simplified. Missing columns: " + ", ".join(missing))

    df = prepare_data(raw)
    df = filter_frame(df)

    if df.empty:
        st.warning("No rows remain after filtering.")
        st.stop()

    metric_cards(df)
    st.markdown("<div class='small-footnote'>Event counts are authorization-based. Claims support and provider attribution are enrichment layers.</div>", unsafe_allow_html=True)

    # Section 1
    st.markdown("<div class='section-card'>", unsafe_allow_html=True)
    add_section_header(
        "Section 1. Utilization Overview",
        "Start with overall mix, movement over time, and how much of the event universe is supported by claims evidence.",
        "Executive lens",
    )
    c1, c2 = st.columns([1.2, 1.0], gap="large")
    with c1:
        st.plotly_chart(build_trend_chart(df), use_container_width=True)
    with c2:
        st.plotly_chart(build_claim_support_chart(df), use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

    # Section 2
    st.markdown("<div class='section-card'>", unsafe_allow_html=True)
    add_section_header(
        "Section 2. Operational Variation",
        "Show where the volume sits and who is associated with event management patterns. This is where high-volume outliers start waving at you.",
        "Facility + provider lens",
    )
    c3, c4 = st.columns([1.0, 1.15], gap="large")
    with c3:
        st.plotly_chart(build_facility_chart(df), use_container_width=True)
    with c4:
        st.plotly_chart(build_provider_scatter(df), use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

    # Section 3
    st.markdown("<div class='section-card'>", unsafe_allow_html=True)
    add_section_header(
        "Section 3. Clinical Profile + Detail",
        "Explain the utilization with diagnosis and LOS context, then let users inspect the supporting event-level records below.",
        "Clinical lens",
    )
    c5, c6 = st.columns([1.1, 0.9], gap="large")
    with c5:
        st.plotly_chart(build_diagnosis_chart(df), use_container_width=True)
    with c6:
        st.plotly_chart(build_los_chart(df), use_container_width=True)

    detail = build_detail_table(df)
    st.dataframe(detail, use_container_width=True, height=320, hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)


if __name__ == "__main__":
    main()
