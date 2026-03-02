# professional_spend_report.py
# ─────────────────────────────────────────────────────────────────────────────
# Professional Spend Analytics — DuckDB + Streamlit Single-File App
# Data Source  : Parquet file on shared drive (CSV also supported)
# Run          : streamlit run professional_spend_report.py
# Requirements : pip install streamlit duckdb plotly pandas pyarrow
# ─────────────────────────────────────────────────────────────────────────────

import streamlit as st
import duckdb
import plotly.express as px
import pandas as pd
from pathlib import Path
import io

# ══════════════════════════════════════════════════════════════════
#  CONFIGURATION  ── update DATA_PATH to your shared drive location
# ══════════════════════════════════════════════════════════════════
#DATA_PATH = r"\\shared_drive\analytics\professional_spend.parquet"
DATA_PATH = r"/Users/bassel_instructor/Documents/datasets/medicare_synthetic_12k.parquet"
# Tip: also accepts CSV → DATA_PATH = r"\\shared_drive\...\file.csv"

C = {
    "paid":      "TotalPaid",
    "allowed":   "TotalAllowed",
    "member":    "MemberID",
    "claim":     "ClaimID",
    "procedure": "ProcedureCode",
    "proc_desc": "ProcedureDesc",
    "network":   "NetworkStatus", # values: INN / OON
    "market":    "OperationalMarket",
    "submarket": "OperationalSubMarket",
    "plan":      "PlanType",
    "entity":    "ManagingEntity",
    "pod":       "PodName",
    "rep_pod":   "ReportingPod",
    "specialty": "Specialty",
    "date":      "DOSbegin",
    "pcp":       "PCPName",
    "provider":  "ProviderName",
    "risk":      "RiskScore",
}

GRANULARITY_OPTIONS = {
    "Market":          C["market"],
    "SubMarket":       C["submarket"],
    "Managing Entity": C["entity"],
    "Pod":             C["pod"],
    "PCP":             C["pcp"],
    "Provider":        C["provider"],
    "Procedure Code":  C["procedure"],
}

# ══════════════════════════════════════════════════════════════════
#  PAGE CONFIG
# ══════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Professional Spend Analytics",
    layout="wide",
    #page_icon="🏥",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    /* Global font */
    html, body, [class*="css"] { font-family: "Segoe UI", sans-serif; }

    /* Remove default top padding */
    .block-container { padding-top: 1.5rem; padding-bottom: 2rem; }

    /* KPI Cards — white background, fixed height, consistent layout */
    .kpi-card {
        background-color: #ffffff;
        border-radius: 8px;
        padding: 20px 16px 16px;
        text-align: center;
        border-top: 3px solid #1f5ea8;
        box-shadow: 0 1px 4px rgba(0,0,0,0.08);
        height: 100px;
        display: flex;
        flex-direction: column;
        justify-content: center;
    }
    .kpi-card-warn { border-top-color: #c0392b; }
    .kpi-label {
        font-size: 0.70rem;
        color: #6b7280;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        line-height: 1.4;
        font-weight: 600;
    }
    .kpi-value {
        font-size: 1.55rem;
        font-weight: 700;
        color: #111827;
        margin-top: 4px;
        white-space: nowrap;
    }

    /* Section labels */
    .section-label {
        font-size: 0.70rem;
        font-weight: 600;
        color: #6b7280;
        text-transform: uppercase;
        letter-spacing: 0.07em;
        margin: 10px 0 4px;
    }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        background-color: #f9fafb;
        border-right: 1px solid #e5e7eb;
    }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════
#  FILE TYPE HELPER
# ══════════════════════════════════════════════════════════════════
def read_expr(path: str) -> str:
    ext = Path(path).suffix.lower()
    if ext == ".csv":
        return f"read_csv_auto(\'{path}\')"
    return f"read_parquet(\'{path}\')"


# ══════════════════════════════════════════════════════════════════
#  FILTER OPTIONS LOADER
# ══════════════════════════════════════════════════════════════════
@st.cache_data(ttl=3600, show_spinner="Connecting to data source...")
def load_filter_options(data_path: str):
    if not Path(data_path).exists():
        return None, f"File not found:\n{data_path}"
    expr = read_expr(data_path)
    conn = duckdb.connect()

    def distinct(col):
        rows = conn.execute(
            f"SELECT DISTINCT {col} FROM {expr} WHERE {col} IS NOT NULL ORDER BY 1"
        ).fetchall()
        return [r[0] for r in rows]

    try:
        date_range = conn.execute(
            f"SELECT MIN({C['date']}), MAX({C['date']}) FROM {expr}"
        ).fetchone()
        opts = {
            "markets":  distinct(C["market"]),
            "plans":    distinct(C["plan"]),
            "entities": distinct(C["entity"]),
            "pods":     distinct(C["pod"]),
            "specs":    distinct(C["specialty"]),
            "date_min": pd.to_datetime(date_range[0]),
            "date_max": pd.to_datetime(date_range[1]),
        }
        conn.close()
        return opts, None
    except Exception as e:
        return None, str(e)


# ══════════════════════════════════════════════════════════════════
#  WHERE CLAUSE BUILDER
# ══════════════════════════════════════════════════════════════════
def where_clause(markets, plans, entities, pods, specs, d_start, d_end) -> str:
    parts = []
    if markets:
        vals = ", ".join(f"\'{v}\'" for v in markets)
        parts.append(f"{C['market']} IN ({vals})")
    if plans:
        vals = ", ".join(f"\'{v}\'" for v in plans)
        parts.append(f"{C['plan']} IN ({vals})")
    if entities:
        vals = ", ".join(f"\'{v}\'" for v in entities)
        parts.append(f"{C['entity']} IN ({vals})")
    if pods:
        vals = ", ".join(f"\'{v}\'" for v in pods)
        parts.append(f"{C['pod']} IN ({vals})")
    if specs:
        vals = ", ".join(f"\'{v}\'" for v in specs)
        parts.append(f"{C['specialty']} IN ({vals})")
    if d_start and d_end:
        parts.append(
            f"CAST({C['date']} AS DATE) BETWEEN \'{d_start}\' AND \'{d_end}\'"
        )
    return ("WHERE " + " AND ".join(parts)) if parts else ""


# ══════════════════════════════════════════════════════════════════
#  KPI QUERY
# ══════════════════════════════════════════════════════════════════
@st.cache_data(ttl=300, show_spinner=False)
def get_kpis(data_path: str, wc: str) -> pd.DataFrame:
    expr = read_expr(data_path)
    q = f"""
    SELECT
        SUM({C['paid']})                                                AS total_paid,
        SUM({C['allowed']})                                             AS total_allowed,
        COUNT(DISTINCT {C['member']})                                   AS unique_members,
        COUNT(DISTINCT {C['procedure']})                                AS unique_procedures,
        ROUND(SUM({C['paid']}) / NULLIF(COUNT({C['claim']}), 0), 2)  AS avg_paid_per_claim,
        ROUND(
            SUM(CASE WHEN {C['network']} = 'OON' THEN {C['paid']} ELSE 0 END)
            / NULLIF(SUM({C['paid']}), 0) * 100, 1
        )                                                                 AS pct_oon
    FROM {expr}
    {wc}
    """
    return duckdb.execute(q).fetchdf()


# ══════════════════════════════════════════════════════════════════
#  SCATTER QUERY
# ══════════════════════════════════════════════════════════════════
@st.cache_data(ttl=300, show_spinner=False)
def get_scatter(data_path: str, wc: str, gran_col: str) -> pd.DataFrame:
    expr = read_expr(data_path)
    q = f"""
    WITH filtered AS (
        SELECT * FROM {expr} {wc}
    ),
    grand_avg AS (
        SELECT AVG({C['paid']}) AS grand_mean FROM filtered
    ),
    grouped AS (
        SELECT
            {gran_col}                                                                 AS dimension,
            ROUND(SUM({C['paid']}) / NULLIF(COUNT({C['claim']}), 0), 2)           AS avg_cost_per_claim,
            COUNT({C['claim']})                                                      AS claim_count,
            ROUND(SUM({C['paid']}) / NULLIF(COUNT(DISTINCT {C['member']}), 0), 2) AS paid_per_member,
            MODE({C['specialty']})                                                   AS specialty_mode,
            MODE({C['network']})                                                     AS network_mode,
            ROUND(AVG({C['risk']}), 3)                                               AS avg_risk
        FROM filtered
        GROUP BY {gran_col}
        HAVING COUNT({C['claim']}) > 0
    )
    SELECT
        g.*,
        ROUND(g.avg_cost_per_claim - ga.grand_mean, 2) AS cost_deviation
    FROM grouped g
    CROSS JOIN grand_avg ga
    ORDER BY g.claim_count DESC
    LIMIT 500
    """
    return duckdb.execute(q).fetchdf()


# ══════════════════════════════════════════════════════════════════
#  DETAIL DOWNLOAD QUERY
# ══════════════════════════════════════════════════════════════════
@st.cache_data(ttl=300, show_spinner="Preparing data...")
def get_detail(data_path: str, wc: str, limit: int) -> pd.DataFrame:
    expr = read_expr(data_path)
    return duckdb.execute(f"SELECT * FROM {expr} {wc} LIMIT {limit}").fetchdf()


# ══════════════════════════════════════════════════════════════════
#  FORMAT HELPERS
# ══════════════════════════════════════════════════════════════════
def fmt_usd(v):
    if pd.isna(v): return "—"
    if v >= 1_000_000: return f"${v/1_000_000:.2f}M"
    if v >= 1_000:     return f"${v/1_000:.1f}K"
    return f"${v:,.2f}"

def fmt_int(v):
    return "—" if pd.isna(v) else f"{int(v):,}"


# ════════════════════════════════════════════════════════════════
#  APP LAYOUT
# ════════════════════════════════════════════════════════════════

st.title("Professional Spend Analytics")
st.caption("Powered by DuckDB · Single-file · Local Browser")

# Data source config
with st.expander("Data Source Configuration", expanded=not Path(DATA_PATH).exists()):
    DATA_PATH = st.text_input(
        "File Path (parquet or CSV on shared drive)",
        value=DATA_PATH,
        help="UNC path, e.g. \\\\server\\share\\file.parquet  or  C:\\data\\file.csv",
    )

opts, err = load_filter_options(DATA_PATH)
if err:
    st.error(err)
    st.stop()


# ── SIDEBAR ─────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filters")

    sel_markets  = st.multiselect("Market",           options=opts["markets"],  placeholder="All")
    sel_plans    = st.multiselect("Plan Type",         options=opts["plans"],    placeholder="All")
    sel_entities = st.multiselect("Managing Entity",   options=opts["entities"], placeholder="All")
    sel_pods     = st.multiselect("Pod",               options=opts["pods"],     placeholder="All")
    sel_specs    = st.multiselect("Specialty",         options=opts["specs"],    placeholder="All")

    st.divider()
    st.markdown('<p class="section-label">Date Range</p>', unsafe_allow_html=True)
    d_start = st.date_input("From", value=opts["date_min"],
                             min_value=opts["date_min"], max_value=opts["date_max"])
    d_end   = st.date_input("To",   value=opts["date_max"],
                             min_value=opts["date_min"], max_value=opts["date_max"])

    st.divider()
    st.markdown('<p class="section-label">Scatter Granularity</p>', unsafe_allow_html=True)
    gran_label = st.selectbox("Group By", list(GRANULARITY_OPTIONS.keys()))
    gran_col   = GRANULARITY_OPTIONS[gran_label]

    st.markdown('<p class="section-label">Color Bubbles By</p>', unsafe_allow_html=True)
    color_by = st.radio("", ["Specialty", "Network Status"], horizontal=True,
                        label_visibility="collapsed")

    st.divider()
    if st.button("Reset All Filters", use_container_width=True):
        st.cache_data.clear()
        st.rerun()


wc = where_clause(sel_markets, sel_plans, sel_entities, sel_pods, sel_specs, d_start, d_end)


# ── KPI ROW ─────────────────────────────────────────────────────
kpi = get_kpis(DATA_PATH, wc).iloc[0]

kpi_items = [
    ("Total Professional Spend", fmt_usd(kpi["total_paid"]),        ""),
    ("Total Allowed",             fmt_usd(kpi["total_allowed"]),     ""),
    ("Unique Members",            fmt_int(kpi["unique_members"]),    ""),
    ("Unique Procedures",         fmt_int(kpi["unique_procedures"]), ""),
    ("Avg Paid per Claim",        fmt_usd(kpi["avg_paid_per_claim"]),""),
    ("% Out-of-Network Spend",
     f"{kpi['pct_oon']:.1f}%" if not pd.isna(kpi["pct_oon"]) else "—",
     "kpi-card-warn"),
]

cols = st.columns(6)
for col, (label, value, extra_cls) in zip(cols, kpi_items):
    col.markdown(
        f'''<div class="kpi-card {extra_cls}">
               <div class="kpi-label">{label}</div>
               <div class="kpi-value">{value}</div>
           </div>''',
        unsafe_allow_html=True,
    )

st.markdown("<br>", unsafe_allow_html=True)


# ── SCATTER PLOT ────────────────────────────────────────────────
st.subheader(f"Cost Efficiency Scatter — Grouped by {gran_label}")
st.caption(
    "X-axis: Avg Paid per Claim  |  Y-axis: Deviation from Benchmark (dataset mean)  |  "
    "Bubble size: Claim Volume  |  Dashed line = benchmark"
)

scatter_df = get_scatter(DATA_PATH, wc, gran_col)

if scatter_df.empty:
    st.info("No data matches the current filters.")
else:
    color_col   = "specialty_mode" if color_by == "Specialty" else "network_mode"
    color_label = "Specialty"      if color_by == "Specialty" else "Network Status"

    fig = px.scatter(
        scatter_df,
        x="avg_cost_per_claim",
        y="cost_deviation",
        size="claim_count",
        color=color_col,
        hover_name="dimension",
        hover_data={
            "avg_cost_per_claim": ":$,.2f",
            "cost_deviation":     ":$,.2f",
            "claim_count":        ":,",
            "paid_per_member":    ":$,.2f",
            "avg_risk":           ":.3f",
            color_col:            True,
        },
        labels={
            "avg_cost_per_claim": "Avg Paid per Claim ($)",
            "cost_deviation":     "Deviation from Benchmark ($)",
            "claim_count":        "Claim Volume",
            color_col:            color_label,
        },
        size_max=60,
        template="plotly_white",
        color_discrete_sequence=px.colors.qualitative.Safe,
    )
    fig.add_hline(
        y=0,
        line_dash="dot",
        line_color="#9ca3af",
        annotation_text="Benchmark (dataset avg)",
        annotation_position="bottom right",
        annotation_font_color="#6b7280",
        annotation_font_size=11,
    )
    fig.update_layout(
        height=540,
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
        font=dict(color="#374151", family="Segoe UI, sans-serif", size=12),
        legend=dict(
            orientation="v",
            x=1.01,
            bgcolor="rgba(255,255,255,0.9)",
            bordercolor="#e5e7eb",
            borderwidth=1,
        ),
        xaxis=dict(
            showgrid=True,
            gridcolor="#f3f4f6",
            linecolor="#d1d5db",
            zeroline=False,
            title_font=dict(size=12, color="#374151"),
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor="#f3f4f6",
            linecolor="#d1d5db",
            zeroline=False,
            title_font=dict(size=12, color="#374151"),
        ),
        margin=dict(l=60, r=20, t=40, b=60),
    )
    st.plotly_chart(fig, use_container_width=True)
    st.caption(f"Showing top 500 {gran_label} groups by claim volume.")


# ── DATA DOWNLOAD ───────────────────────────────────────────────
st.divider()
st.subheader("Download Filtered Data")

dl_col1, dl_col2, dl_col3 = st.columns([3, 1, 1])
with dl_col1:
    row_limit = st.slider("Row limit", 1_000, 200_000, 50_000, 1_000)
with dl_col2:
    dl_fmt = st.radio("Format", ["CSV", "Parquet"], horizontal=True)
with dl_col3:
    st.markdown("<br>", unsafe_allow_html=True)
    run_dl = st.button("Prepare Download", use_container_width=True)

if run_dl:
    dl_df = get_detail(DATA_PATH, wc, row_limit)
    st.info(f"{len(dl_df):,} rows ready for download.")

    if dl_fmt == "CSV":
        data_bytes = dl_df.to_csv(index=False).encode("utf-8")
        mime, fname = "text/csv", "professional_spend_filtered.csv"
    else:
        buf = io.BytesIO()
        dl_df.to_parquet(buf, index=False)
        data_bytes = buf.getvalue()
        mime, fname = "application/octet-stream", "professional_spend_filtered.parquet"

    st.download_button(
        f"Download {dl_fmt}  ({len(dl_df):,} rows)",
        data=data_bytes, file_name=fname, mime=mime,
        use_container_width=True,
    )
    st.dataframe(dl_df.head(300), use_container_width=True, height=380)


# ── FOOTER ──────────────────────────────────────────────────────
st.divider()
st.caption("Professional Spend Analytics  |  DuckDB + Streamlit  |  Local Browser")