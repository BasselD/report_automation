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
LOGO_PATH = r"logo.png"   # Set to your logo file path; supports PNG/JPG
BANNER_COLOR        = "rgb(0, 40, 80)"
GRANULARITY_DEFAULT = "Managing Entity"

C = {
    "paid":      "TotalPaid",
    "allowed":   "TotalAllowed",
    "member":    "MemberID",
    "claim":     "ClaimID",
    "procedure": "ProcedureCode",
    "proc_desc": "ProcedureDesc",
    "network":   "NetworkStatus",
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
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    html, body, [class*="css"] { font-family: "Segoe UI", sans-serif; }

    /* Push content below Streamlit's fixed toolbar (~48px) */
    .block-container { padding-top: 3.5rem !important; padding-bottom: 2rem; }

    /* Banner */
    .app-banner {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 14px 28px;
        margin-bottom: 1.4rem;
        border-radius: 6px;
    }
    .banner-title {
        color: #ffffff;
        font-size: 1.45rem;
        font-weight: 700;
        letter-spacing: 0.02em;
        margin: 0;
    }
    .banner-subtitle {
        color: rgba(255,255,255,0.60);
        font-size: 0.78rem;
        margin: 3px 0 0;
    }
    .banner-logo { height: 44px; object-fit: contain; }
    .banner-logo-placeholder {
        color: rgba(255,255,255,0.4);
        font-size: 0.72rem;
        font-style: italic;
    }

    /* KPI Cards */
    .kpi-card {
        background-color: #ffffff;
        border-radius: 8px;
        padding: 0 16px;
        text-align: center;
        border-top: 3px solid #1f5ea8;
        box-shadow: 0 1px 4px rgba(0,0,0,0.08);
        height: 90px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
    }
    .kpi-card-warn { border-top-color: #c0392b; }
    .kpi-label {
        font-size: 0.68rem;
        color: #6b7280;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        line-height: 1.4;
        font-weight: 600;
    }
    .kpi-value {
        font-size: 1.5rem;
        font-weight: 700;
        color: #111827;
        margin-top: 4px;
        white-space: nowrap;
    }

    /* Selection badge */
    .selection-badge {
        display: inline-block;
        background-color: #eff6ff;
        border: 1px solid #bfdbfe;
        color: #1d4ed8;
        border-radius: 6px;
        padding: 6px 14px;
        font-size: 0.82rem;
        font-weight: 500;
        margin-bottom: 10px;
    }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        background-color: #f9fafb;
        border-right: 1px solid #e5e7eb;
    }
    .section-label {
        font-size: 0.70rem;
        font-weight: 600;
        color: #6b7280;
        text-transform: uppercase;
        letter-spacing: 0.07em;
        margin: 10px 0 4px;
    }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════
#  BANNER
# ══════════════════════════════════════════════════════════════════
def render_banner(title, subtitle, color, logo_path):
    if Path(logo_path).exists():
        with open(logo_path, "rb") as f:
            ext  = Path(logo_path).suffix.lower().replace(".", "")
            mime = "jpeg" if ext == "jpg" else ext
            b64  = base64.b64encode(f.read()).decode()
        logo_html = f'<img src="data:image/{mime};base64,{b64}" class="banner-logo" alt="logo">'
    else:
        logo_html = '<span class="banner-logo-placeholder">[ logo.png ]</span>'

    st.markdown(
        f'<div class="app-banner" style="background-color:{color};">'
        f'<div><p class="banner-title">{title}</p>'
        f'<p class="banner-subtitle">{subtitle}</p></div>'
        f'{logo_html}</div>',
        unsafe_allow_html=True,
    )

render_banner(
    title     = "Professional Spend Analytics",
    subtitle  = "Powered by DuckDB  |  Local Browser",
    color     = BANNER_COLOR,
    logo_path = LOGO_PATH,
)


# ══════════════════════════════════════════════════════════════════
#  FILE TYPE HELPER
# ══════════════════════════════════════════════════════════════════
def read_expr(path: str) -> str:
    ext = Path(path).suffix.lower()
    return f"read_csv_auto('{path}')" if ext == ".csv" else f"read_parquet('{path}')"


# ══════════════════════════════════════════════════════════════════
#  CASCADING FILTER QUERY
# ══════════════════════════════════════════════════════════════════
@st.cache_data(ttl=300, show_spinner=False)
def get_distinct_filtered(data_path: str, col: str, upstream: tuple) -> list:
    expr  = read_expr(data_path)
    parts = [f"{col} IS NOT NULL"]
    for fc, fv in upstream:
        if fv:
            vals = ", ".join(f"'{v}'" for v in fv)
            parts.append(f"{fc} IN ({vals})")
    wc   = "WHERE " + " AND ".join(parts)
    rows = duckdb.execute(
        f"SELECT DISTINCT {col} FROM {expr} {wc} ORDER BY 1"
    ).fetchall()
    return [r[0] for r in rows]


# ══════════════════════════════════════════════════════════════════
#  DATE RANGE LOADER
# ══════════════════════════════════════════════════════════════════
@st.cache_data(ttl=3600, show_spinner="Connecting to data source...")
def load_date_range(data_path: str):
    if not Path(data_path).exists():
        return None, f"File not found:\n{data_path}"
    try:
        expr = read_expr(data_path)
        r    = duckdb.execute(
            f"SELECT MIN({C['date']}), MAX({C['date']}) FROM {expr}"
        ).fetchone()
        return (pd.to_datetime(r[0]), pd.to_datetime(r[1])), None
    except Exception as e:
        return None, str(e)


# ══════════════════════════════════════════════════════════════════
#  WHERE CLAUSE BUILDER
# ══════════════════════════════════════════════════════════════════
def where_clause(markets, plans, entities, pods, specs, d_start, d_end) -> str:
    parts = []
    def add(col, vals):
        if vals:
            parts.append(f"{col} IN ({', '.join(f(v) for v in vals)})")
    def f(v): return f"'{v}'"
    add(C["market"],  markets)
    add(C["plan"],    plans)
    add(C["entity"],  entities)
    add(C["pod"],     pods)
    add(C["specialty"], specs)
    if d_start and d_end:
        parts.append(f"CAST({C['date']} AS DATE) BETWEEN '{d_start}' AND '{d_end}'")
    return ("WHERE " + " AND ".join(parts)) if parts else ""


def append_dim_filter(base_wc: str, gran_col: str, selected_dims: list) -> str:
    """Append a chart-selection dimension filter to an existing WHERE clause."""
    if not selected_dims:
        return base_wc
    vals   = ", ".join(f"'{v}'" for v in selected_dims)
    clause = f"{gran_col} IN ({vals})"
    return (base_wc + f" AND {clause}") if base_wc else f"WHERE {clause}"


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
        ROUND(SUM({C['paid']}) / NULLIF(COUNT({C['claim']}), 0), 2)    AS avg_paid_per_claim,
        ROUND(
            SUM(CASE WHEN {C['network']} = 'OON' THEN {C['paid']} ELSE 0 END)
            / NULLIF(SUM({C['paid']}), 0) * 100, 1
        )                                                               AS pct_oon
    FROM {expr} {wc}
    """
    return duckdb.execute(q).fetchdf()


# ══════════════════════════════════════════════════════════════════
#  SCATTER QUERY
# ══════════════════════════════════════════════════════════════════
@st.cache_data(ttl=300, show_spinner=False)
def get_scatter(data_path: str, wc: str, gran_col: str) -> pd.DataFrame:
    expr = read_expr(data_path)
    q = f"""
    WITH filtered AS (SELECT * FROM {expr} {wc}),
    grand_avg AS (SELECT AVG({C['paid']}) AS grand_mean FROM filtered),
    grouped AS (
        SELECT
            {gran_col}                                                                    AS dimension,
            ROUND(SUM({C['paid']}) / NULLIF(COUNT({C['claim']}), 0), 2)                  AS avg_cost_per_claim,
            COUNT({C['claim']})                                                           AS claim_count,
            ROUND(SUM({C['paid']}) / NULLIF(COUNT(DISTINCT {C['member']}), 0), 2)        AS paid_per_member,
            MODE({C['specialty']})                                                        AS specialty_mode,
            MODE({C['network']})                                                          AS network_mode
        FROM filtered
        GROUP BY {gran_col}
        HAVING COUNT({C['claim']}) > 0
    )
    SELECT g.*, ROUND(g.avg_cost_per_claim - ga.grand_mean, 2) AS cost_deviation
    FROM grouped g CROSS JOIN grand_avg ga
    ORDER BY g.claim_count DESC LIMIT 500
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
#  DATA SOURCE CONFIG
# ════════════════════════════════════════════════════════════════
with st.expander("Data Source Configuration", expanded=not Path(DATA_PATH).exists()):
    DATA_PATH = st.text_input(
        "File Path (parquet or CSV on shared drive)",
        value=DATA_PATH,
        help=r"UNC path, e.g. \\server\share\file.parquet  or  C:\data\file.csv",
    )

date_range, err = load_date_range(DATA_PATH)
if err:
    st.error(err)
    st.stop()

date_min, date_max = date_range


# ════════════════════════════════════════════════════════════════
#  SIDEBAR — CASCADING FILTERS
# ════════════════════════════════════════════════════════════════
with st.sidebar:
    st.header("Filters")

    market_opts = get_distinct_filtered(DATA_PATH, C["market"], ())
    sel_markets = st.multiselect("Market", options=market_opts, placeholder="All")

    plan_opts = get_distinct_filtered(
        DATA_PATH, C["plan"],
        ((C["market"], tuple(sel_markets)),)
    )
    sel_plans = st.multiselect("Plan Type", options=plan_opts, placeholder="All")

    entity_opts = get_distinct_filtered(
        DATA_PATH, C["entity"],
        ((C["market"], tuple(sel_markets)), (C["plan"], tuple(sel_plans)))
    )
    sel_entities = st.multiselect("Managing Entity", options=entity_opts, placeholder="All")

    pod_opts = get_distinct_filtered(
        DATA_PATH, C["pod"],
        (
            (C["market"],  tuple(sel_markets)),
            (C["plan"],    tuple(sel_plans)),
            (C["entity"],  tuple(sel_entities)),
        )
    )
    sel_pods = st.multiselect("Pod", options=pod_opts, placeholder="All")

    spec_opts = get_distinct_filtered(
        DATA_PATH, C["specialty"],
        (
            (C["market"],   tuple(sel_markets)),
            (C["plan"],     tuple(sel_plans)),
            (C["entity"],   tuple(sel_entities)),
            (C["pod"],      tuple(sel_pods)),
        )
    )
    sel_specs = st.multiselect("Specialty", options=spec_opts, placeholder="All")

    st.divider()
    st.markdown('<p class="section-label">Date Range</p>', unsafe_allow_html=True)
    d_start = st.date_input("From", value=date_min, min_value=date_min, max_value=date_max)
    d_end   = st.date_input("To",   value=date_max, min_value=date_min, max_value=date_max)

    st.divider()
    st.markdown('<p class="section-label">Scatter Granularity</p>', unsafe_allow_html=True)
    gran_keys   = list(GRANULARITY_OPTIONS.keys())
    default_idx = gran_keys.index(GRANULARITY_DEFAULT)
    gran_label  = st.selectbox("Group By", gran_keys, index=default_idx)
    gran_col    = GRANULARITY_OPTIONS[gran_label]

    st.markdown('<p class="section-label">Color Bubbles By</p>', unsafe_allow_html=True)
    color_by = st.radio("", ["Specialty", "Network Status"], horizontal=True,
                        label_visibility="collapsed")

    st.divider()
    if st.button("Reset All Filters", use_container_width=True):
        st.cache_data.clear()
        st.rerun()


wc = where_clause(sel_markets, sel_plans, sel_entities, sel_pods, sel_specs, d_start, d_end)


# ════════════════════════════════════════════════════════════════
#  KPI ROW
# ════════════════════════════════════════════════════════════════
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
        f'<div class="kpi-card {extra_cls}">'
        f'<div class="kpi-label">{label}</div>'
        f'<div class="kpi-value">{value}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

st.markdown("<br>", unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════
#  SCATTER PLOT  (on_select → feeds download filter)
# ════════════════════════════════════════════════════════════════
st.subheader(f"Cost Efficiency Scatter — Grouped by {gran_label}")
st.caption(
    "X-axis: Avg Paid per Claim  |  Y-axis: Deviation from Benchmark (dataset mean)  |  "
    "Bubble size: Claim Volume  |  Click or lasso-select bubbles to filter the download below"
)

scatter_df = get_scatter(DATA_PATH, wc, gran_col)
selected_dims = []   # will be populated from chart click/lasso events

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
        custom_data=["dimension"],        # ← dimension stored at customdata[0] for selection
        hover_name="dimension",
        hover_data={
            "avg_cost_per_claim": ":$,.2f",
            "cost_deviation":     ":$,.2f",
            "claim_count":        ":,",
            "paid_per_member":    ":$,.2f",
            color_col:            True,
            "dimension":          False,  # suppress duplicate; already in hover_name
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
        y=0, line_dash="dot", line_color="#9ca3af",
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
            orientation="v", x=1.01,
            bgcolor="rgba(255,255,255,0.9)",
            bordercolor="#e5e7eb", borderwidth=1,
        ),
        xaxis=dict(showgrid=True, gridcolor="#f3f4f6", linecolor="#d1d5db", zeroline=False),
        yaxis=dict(showgrid=True, gridcolor="#f3f4f6", linecolor="#d1d5db", zeroline=False),
        margin=dict(l=60, r=20, t=40, b=60),
        # Enable box + lasso selection tools
        dragmode="select",
        newselection_mode="gradual",
    )
    fig.update_traces(
        marker=dict(line=dict(width=0.8, color="#ffffff")),
        unselected=dict(marker=dict(opacity=0.25)),
    )

    # Render chart — on_select triggers a rerun and returns event data
    event = st.plotly_chart(
        fig,
        use_container_width=True,
        on_select="rerun",
        selection_mode=["points", "box", "lasso"],
        key=f"scatter_{gran_label}_{color_by}",  # key resets selection on granularity change
    )

    # Extract selected dimension values from event
    if event and event.selection and event.selection.points:
        selected_dims = [
            p["customdata"][0]
            for p in event.selection.points
            if p.get("customdata") and len(p["customdata"]) > 0
        ]

    st.caption(f"Showing top 500 {gran_label} groups by claim volume.")

# Show selection status badge
if selected_dims:
    st.markdown(
        f'<div class="selection-badge">'
        f'{len(selected_dims)} group(s) selected on chart — '
        f'download below reflects this selection. '
        f'Click empty chart area or reset filters to clear.'
        f'</div>',
        unsafe_allow_html=True,
    )


# ════════════════════════════════════════════════════════════════
#  DOWNLOAD  (respects both sidebar filters + chart selection)
# ════════════════════════════════════════════════════════════════
st.divider()
st.subheader("Download Filtered Data")

# Build the effective WHERE clause: sidebar filters + chart selection (if any)
download_wc = append_dim_filter(wc, gran_col, selected_dims)

# Show active filter summary
filter_parts = []
if any([sel_markets, sel_plans, sel_entities, sel_pods, sel_specs]):
    filter_parts.append("sidebar filters")
if selected_dims:
    filter_parts.append(f"{len(selected_dims)} chart-selected group(s)")
if filter_parts:
    st.caption(f"Active filters: {' + '.join(filter_parts)}")
else:
    st.caption("No filters active — full dataset will be returned.")

dl_col1, dl_col2, dl_col3 = st.columns([3, 1, 1])
with dl_col1:
    row_limit = st.slider("Row limit", 1_000, 200_000, 50_000, 1_000)
with dl_col2:
    dl_fmt = st.radio("Format", ["CSV", "Parquet"], horizontal=True)
with dl_col3:
    st.markdown("<br>", unsafe_allow_html=True)
    run_dl = st.button("Prepare Download", use_container_width=True)

if run_dl:
    dl_df = get_detail(DATA_PATH, download_wc, row_limit)
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


# ════════════════════════════════════════════════════════════════
#  FOOTER
# ════════════════════════════════════════════════════════════════
st.divider()
st.caption("Professional Spend Analytics  |  DuckDB + Streamlit  |  Local Browser")