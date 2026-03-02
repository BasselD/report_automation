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
    .block-container { padding-top: 3.5rem !important; padding-bottom: 2rem; }

    .app-banner {
        display: flex; align-items: center; justify-content: space-between;
        padding: 14px 28px; margin-bottom: 1.4rem; border-radius: 6px;
    }
    .banner-title  { color:#ffffff; font-size:1.45rem; font-weight:700; letter-spacing:.02em; margin:0; }
    .banner-subtitle { color:rgba(255,255,255,.60); font-size:.78rem; margin:3px 0 0; }
    .banner-logo   { height:44px; object-fit:contain; }
    .banner-logo-placeholder { color:rgba(255,255,255,.4); font-size:.72rem; font-style:italic; }

    .kpi-card {
        background-color:#ffffff; border-radius:8px; padding:0 16px; text-align:center;
        border-top:3px solid #1f5ea8; box-shadow:0 1px 4px rgba(0,0,0,.08);
        height:90px; display:flex; flex-direction:column;
        justify-content:center; align-items:center;
    }
    .kpi-card-warn { border-top-color:#c0392b; }
    .kpi-label {
        font-size:.68rem; color:#6b7280; text-transform:uppercase;
        letter-spacing:.06em; line-height:1.4; font-weight:600;
    }
    .kpi-value { font-size:1.5rem; font-weight:700; color:#111827; margin-top:4px; white-space:nowrap; }

    /* Scatter chart controls row */
    .chart-controls {
        display:flex; gap:24px; align-items:flex-end;
        background:#f9fafb; border:1px solid #e5e7eb;
        border-radius:8px; padding:12px 18px; margin-bottom:10px;
    }
    .ctrl-group { display:flex; flex-direction:column; gap:4px; }
    .ctrl-label { font-size:.68rem; font-weight:600; color:#6b7280;
                  text-transform:uppercase; letter-spacing:.06em; }

    /* Quadrant legend */
    .quad-legend {
        display:grid; grid-template-columns:1fr 1fr;
        gap:6px; margin-top:8px;
    }
    .quad-item {
        border-radius:6px; padding:8px 12px;
        font-size:.74rem; line-height:1.4; border-left:3px solid;
    }
    .quad-alert  { background:#fff5f5; border-color:#e53e3e; color:#742a2a; }
    .quad-good   { background:#f0fff4; border-color:#38a169; color:#1c4532; }
    .quad-watch  { background:#fffbeb; border-color:#d97706; color:#78350f; }
    .quad-ok     { background:#eff6ff; border-color:#3b82f6; color:#1e3a5f; }

    .selection-badge {
        display:inline-block; background:#eff6ff; border:1px solid #bfdbfe;
        color:#1d4ed8; border-radius:6px; padding:6px 14px;
        font-size:.82rem; font-weight:500; margin-bottom:10px;
    }

    section[data-testid="stSidebar"] {
        background-color:#f9fafb; border-right:1px solid #e5e7eb;
    }
    .section-label {
        font-size:.70rem; font-weight:600; color:#6b7280;
        text-transform:uppercase; letter-spacing:.07em; margin:10px 0 4px;
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
        f'{logo_html}</div>', unsafe_allow_html=True,
    )

render_banner("Professional Spend Analytics", "Powered by DuckDB  |  Local Browser",
              BANNER_COLOR, LOGO_PATH)


# ══════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════
def read_expr(path: str) -> str:
    ext = Path(path).suffix.lower()
    return f"read_csv_auto('{path}')" if ext == ".csv" else f"read_parquet('{path}')"

def fmt_usd(v):
    if pd.isna(v): return "—"
    if v >= 1_000_000: return f"${v/1_000_000:.2f}M"
    if v >= 1_000:     return f"${v/1_000:.1f}K"
    return f"${v:,.2f}"

def fmt_int(v):
    return "—" if pd.isna(v) else f"{int(v):,}"

def where_clause(markets, plans, entities, pods, specs, d_start, d_end) -> str:
    parts = []
    def f(v): return f"'{v}'"
    if markets:   parts.append(f"{C['market']}   IN ({', '.join(f(v) for v in markets)})")
    if plans:     parts.append(f"{C['plan']}     IN ({', '.join(f(v) for v in plans)})")
    if entities:  parts.append(f"{C['entity']}   IN ({', '.join(f(v) for v in entities)})")
    if pods:      parts.append(f"{C['pod']}      IN ({', '.join(f(v) for v in pods)})")
    if specs:     parts.append(f"{C['specialty']} IN ({', '.join(f(v) for v in specs)})")
    if d_start and d_end:
        parts.append(f"CAST({C['date']} AS DATE) BETWEEN '{d_start}' AND '{d_end}'")
    return ("WHERE " + " AND ".join(parts)) if parts else ""

def append_dim_filter(base_wc: str, gran_col: str, selected_dims: list) -> str:
    if not selected_dims:
        return base_wc
    vals   = ", ".join(f"'{v}'" for v in selected_dims)
    clause = f"{gran_col} IN ({vals})"
    return (base_wc + f" AND {clause}") if base_wc else f"WHERE {clause}"


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
    rows = duckdb.execute(
        f"SELECT DISTINCT {col} FROM {expr} WHERE {' AND '.join(parts)} ORDER BY 1"
    ).fetchall()
    return [r[0] for r in rows]


# ══════════════════════════════════════════════════════════════════
#  DATE RANGE
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
#  SCATTER QUERY — Cost Index + Financial Impact
#
#  CostIndex = AVG( claim_cost / benchmark_cost )
#    "Within Procedure" → benchmark = avg TotalPaid for that ProcedureCode
#    "Within Specialty" → benchmark = avg TotalPaid for that Specialty
#
#  Computing the ratio at the CLAIM level then averaging per group
#  ensures each procedure/specialty is fairly weighted regardless of
#  how many claims the group has per code.
# ══════════════════════════════════════════════════════════════════
@st.cache_data(ttl=300, show_spinner=False)
def get_scatter(data_path: str, wc: str, gran_col: str,
                benchmark_mode: str, min_claims: int) -> pd.DataFrame:
    expr = read_expr(data_path)

    if benchmark_mode == "Within Procedure":
        benchmark_join  = C["procedure"]
        benchmark_label = "Procedure Avg"
    else:
        benchmark_join  = C["specialty"]
        benchmark_label = "Specialty Avg"

    q = f"""
    WITH filtered AS (
        SELECT * FROM {expr} {wc}
    ),

    -- Step 1: compute benchmark cost per procedure or specialty
    benchmarks AS (
        SELECT
            {benchmark_join}                        AS bench_key,
            AVG({C['paid']})                        AS benchmark_cost
        FROM filtered
        GROUP BY {benchmark_join}
    ),

    -- Step 2: join back and compute per-claim cost ratio
    claim_indexed AS (
        SELECT
            f.*,
            ROUND(f.{C['paid']} / NULLIF(b.benchmark_cost, 0), 4) AS claim_cost_index
        FROM filtered f
        JOIN benchmarks b ON f.{benchmark_join} = b.bench_key
    ),

    -- Step 3: aggregate by the chosen granularity dimension
    grouped AS (
        SELECT
            {gran_col}                                                                   AS dimension,
            ROUND(AVG(claim_cost_index), 3)                                              AS cost_index,
            ROUND(SUM({C['paid']}), 2)                                                   AS total_paid,
            COUNT({C['claim']})                                                          AS claim_count,
            ROUND(SUM({C['paid']}) / NULLIF(COUNT(DISTINCT {C['member']}), 0), 2)       AS paid_per_member,
            COUNT(DISTINCT {C['procedure']})                                             AS proc_count,
            MODE({C['specialty']})                                                       AS specialty_mode,
            MODE({C['network']})                                                         AS network_mode
        FROM claim_indexed
        GROUP BY {gran_col}
        HAVING COUNT({C['claim']}) >= {min_claims}
    )

    SELECT
        *,
        -- Estimated excess spend vs benchmark: spend above what benchmark would predict
        ROUND(total_paid * (1.0 - 1.0 / NULLIF(cost_index, 0)), 2) AS excess_spend
    FROM grouped
    ORDER BY total_paid DESC
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


# ════════════════════════════════════════════════════════════════
#  DATA SOURCE CONFIG
# ════════════════════════════════════════════════════════════════
with st.expander("Data Source Configuration", expanded=not Path(DATA_PATH).exists()):
    DATA_PATH = st.text_input(
        "File Path (parquet or CSV on shared drive)", value=DATA_PATH,
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
        DATA_PATH, C["plan"], ((C["market"], tuple(sel_markets)),))
    sel_plans = st.multiselect("Plan Type", options=plan_opts, placeholder="All")

    entity_opts = get_distinct_filtered(
        DATA_PATH, C["entity"],
        ((C["market"], tuple(sel_markets)), (C["plan"], tuple(sel_plans))))
    sel_entities = st.multiselect("Managing Entity", options=entity_opts, placeholder="All")

    pod_opts = get_distinct_filtered(
        DATA_PATH, C["pod"],
        ((C["market"], tuple(sel_markets)), (C["plan"], tuple(sel_plans)),
         (C["entity"], tuple(sel_entities))))
    sel_pods = st.multiselect("Pod", options=pod_opts, placeholder="All")

    spec_opts = get_distinct_filtered(
        DATA_PATH, C["specialty"],
        ((C["market"], tuple(sel_markets)), (C["plan"], tuple(sel_plans)),
         (C["entity"], tuple(sel_entities)), (C["pod"], tuple(sel_pods))))
    sel_specs = st.multiselect("Specialty", options=spec_opts, placeholder="All")

    st.divider()
    st.markdown('<p class="section-label">Date Range</p>', unsafe_allow_html=True)
    d_start = st.date_input("From", value=date_min, min_value=date_min, max_value=date_max)
    d_end   = st.date_input("To",   value=date_max, min_value=date_min, max_value=date_max)

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
     f"{kpi['pct_oon']:.1f}%" if not pd.isna(kpi["pct_oon"]) else "—", "kpi-card-warn"),
]

cols = st.columns(6)
for col, (label, value, extra_cls) in zip(cols, kpi_items):
    col.markdown(
        f'<div class="kpi-card {extra_cls}">'
        f'<div class="kpi-label">{label}</div>'
        f'<div class="kpi-value">{value}</div>'
        f'</div>', unsafe_allow_html=True,
    )

st.markdown("<br>", unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════
#  SCATTER CHART CONTROLS (inline above chart)
# ════════════════════════════════════════════════════════════════
st.subheader("Cost Anomaly Explorer")

ctrl1, ctrl2, ctrl3, ctrl4 = st.columns([2, 2, 2, 2])
with ctrl1:
    gran_keys   = list(GRANULARITY_OPTIONS.keys())
    default_idx = gran_keys.index(GRANULARITY_DEFAULT)
    gran_label  = st.selectbox("Granularity (Group By)", gran_keys, index=default_idx)
    gran_col    = GRANULARITY_OPTIONS[gran_label]
with ctrl2:
    benchmark_mode = st.radio(
        "Benchmark Mode",
        ["Within Procedure", "Within Specialty"],
        horizontal=True,
        help=(
            "Within Procedure: each claim compared to avg cost for its procedure code. "
            "Within Specialty: each claim compared to avg cost across its specialty."
        ),
    )
with ctrl3:
    color_by  = st.radio("Color By", ["Specialty", "Network Status"], horizontal=True)
with ctrl4:
    min_claims = st.number_input(
        "Min Claims (noise filter)", min_value=1, max_value=500, value=5, step=1,
        help="Exclude groups with fewer than this many claims to suppress low-volume noise.",
    )

st.caption(
    f"X-axis: Cost Index vs {benchmark_mode.split()[1]} Benchmark  (1.0 = at benchmark, "
    f">1.0 = above, <1.0 = below)  |  Y-axis: Total Paid ($)  |  "
    f"Bubble size: Claim Volume  |  Click or lasso-select bubbles to filter the download below"
)


# ════════════════════════════════════════════════════════════════
#  SCATTER PLOT
# ════════════════════════════════════════════════════════════════
scatter_df    = get_scatter(DATA_PATH, wc, gran_col, benchmark_mode, int(min_claims))
selected_dims = []

if scatter_df.empty:
    st.info("No data matches the current filters or minimum claim threshold.")
else:
    color_col   = "specialty_mode" if color_by == "Specialty" else "network_mode"
    color_label = "Specialty"      if color_by == "Specialty" else "Network Status"

    # Median total_paid for quadrant reference line
    median_spend = float(scatter_df["total_paid"].median())

    fig = px.scatter(
        scatter_df,
        x="cost_index",
        y="total_paid",
        size="claim_count",
        color=color_col,
        custom_data=["dimension"],
        hover_name="dimension",
        hover_data={
            "cost_index":     ":.3f",
            "total_paid":     ":$,.0f",
            "excess_spend":   ":$,.0f",
            "claim_count":    ":,",
            "paid_per_member":":$,.0f",
            "proc_count":     ":,",
            color_col:        True,
            "dimension":      False,
        },
        labels={
            "cost_index":      f"Cost Index (vs {benchmark_mode.split()[1]} Benchmark)",
            "total_paid":      "Total Paid ($)",
            "claim_count":     "Claim Volume",
            "excess_spend":    "Est. Excess Spend ($)",
            "paid_per_member": "Paid per Member ($)",
            "proc_count":      "Distinct Procedures",
            color_col:         color_label,
        },
        size_max=60,
        template="plotly_white",
        color_discrete_sequence=px.colors.qualitative.Safe,
    )

    # ── Quadrant shading ──────────────────────────────────────
    # Upper-right: high cost index + high spend → priority anomaly zone (light red)
    fig.add_vrect(
        x0=1.0, x1=999,
        fillcolor="rgba(229, 62, 62, 0.04)",
        line_width=0,
        layer="below",
    )

    # ── Benchmark reference lines ─────────────────────────────
    # Vertical at x=1.0
    fig.add_vline(
        x=1.0, line_dash="dash", line_color="#6b7280", line_width=1.5,
        annotation_text="Benchmark = 1.0",
        annotation_position="top",
        annotation_font_color="#374151",
        annotation_font_size=11,
    )
    # Horizontal at median spend (separates high/low financial impact)
    fig.add_hline(
        y=median_spend, line_dash="dot", line_color="#9ca3af", line_width=1,
        annotation_text="Median Spend",
        annotation_position="right",
        annotation_font_color="#6b7280",
        annotation_font_size=10,
    )

    # ── Quadrant corner labels ────────────────────────────────
    x_range = scatter_df["cost_index"].agg(["min", "max"])
    x_pad   = (x_range["max"] - x_range["min"]) * 0.03
    y_range = scatter_df["total_paid"].agg(["min", "max"])
    y_top   = float(y_range["max"])
    y_bot   = float(y_range["min"])
    x_left  = float(x_range["min"])
    x_right = float(x_range["max"])

    quad_labels = [
        (x_left,  y_top,    "Efficient + High Spend",    "left",  "#15803d"),
        (x_right, y_top,    "Priority Anomaly",           "right", "#b91c1c"),
        (x_right, y_bot,    "Over Benchmark + Low Spend", "right", "#b45309"),
        (x_left,  y_bot,    "Performing Well",            "left",  "#1d4ed8"),
    ]
    for qx, qy, text, anchor, color in quad_labels:
        fig.add_annotation(
            x=qx, y=qy, text=text,
            showarrow=False,
            font=dict(size=9, color=color),
            xanchor=anchor, yanchor="top" if qy == y_top else "bottom",
            opacity=0.55,
        )

    fig.update_traces(
        marker=dict(line=dict(width=0.8, color="#ffffff")),
        unselected=dict(marker=dict(opacity=0.20)),
    )
    fig.update_layout(
        height=560,
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
        font=dict(color="#374151", family="Segoe UI, sans-serif", size=12),
        legend=dict(
            orientation="v", x=1.01,
            bgcolor="rgba(255,255,255,0.9)",
            bordercolor="#e5e7eb", borderwidth=1,
        ),
        xaxis=dict(showgrid=True, gridcolor="#f3f4f6", linecolor="#d1d5db",
                   zeroline=False, title_standoff=12),
        yaxis=dict(showgrid=True, gridcolor="#f3f4f6", linecolor="#d1d5db",
                   zeroline=False, title_standoff=12,
                   tickprefix="$", tickformat=",.0f"),
        margin=dict(l=80, r=20, t=50, b=70),
        dragmode="pan",
    )

    event = st.plotly_chart(
        fig,
        use_container_width=True,
        on_select="rerun",
        selection_mode=["points", "box", "lasso"],
        key=f"scatter_{gran_label}_{benchmark_mode}_{color_by}",
    )

    if event and event.selection and event.selection.points:
        selected_dims = [
            p["customdata"][0]
            for p in event.selection.points
            if p.get("customdata") and len(p["customdata"]) > 0
        ]

    st.caption(
        f"Showing up to 500 {gran_label} groups with >= {int(min_claims)} claims. "
        f"Bubble size = claim volume."
    )

# ── Quadrant guide ───────────────────────────────────────────────
with st.expander("Quadrant Interpretation Guide", expanded=False):
    st.markdown("""
<div class="quad-legend">
  <div class="quad-item quad-alert">
    <strong>Priority Anomaly</strong> (upper-right)<br>
    Above benchmark cost AND high total spend. Highest financial risk — investigate first.
  </div>
  <div class="quad-item quad-good">
    <strong>Efficient + High Spend</strong> (upper-left)<br>
    Below benchmark despite high volume. High value — consider as reference standard.
  </div>
  <div class="quad-item quad-watch">
    <strong>Over Benchmark + Low Spend</strong> (lower-right)<br>
    Above benchmark but limited financial impact today. Monitor for volume growth.
  </div>
  <div class="quad-item quad-ok">
    <strong>Performing Well</strong> (lower-left)<br>
    Below benchmark and low spend. Low priority — no action needed.
  </div>
</div>
""", unsafe_allow_html=True)

# Selection badge
if selected_dims:
    st.markdown(
        f'<div class="selection-badge">{len(selected_dims)} group(s) selected — '
        f'download below reflects this selection. '
        f'Click empty chart area or reset to clear.</div>',
        unsafe_allow_html=True,
    )


# ════════════════════════════════════════════════════════════════
#  DOWNLOAD
# ════════════════════════════════════════════════════════════════
st.divider()
st.subheader("Download Filtered Data")

download_wc = append_dim_filter(wc, gran_col, selected_dims)

filter_parts = []
if any([sel_markets, sel_plans, sel_entities, sel_pods, sel_specs]):
    filter_parts.append("sidebar filters")
if selected_dims:
    filter_parts.append(f"{len(selected_dims)} chart-selected group(s)")
st.caption(
    f"Active filters: {' + '.join(filter_parts)}"
    if filter_parts else "No filters active — full dataset will be returned."
)

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
        data=data_bytes, file_name=fname, mime=mime, use_container_width=True,
    )
    st.dataframe(dl_df.head(300), use_container_width=True, height=380)


# ════════════════════════════════════════════════════════════════
#  FOOTER
# ════════════════════════════════════════════════════════════════
st.divider()
st.caption("Professional Spend Analytics  |  DuckDB + Streamlit  |  Local Browser")