from __future__ import annotations

import base64
import io
import os
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, Input, Output, State, callback, dash_table, dcc, html, no_update
from dash.exceptions import PreventUpdate


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DATA_PATH = BASE_DIR / "data" / "provider_metrics.csv"

MEASURES = [
    {"group": "Risk", "label": "Risk Total", "column": "Risk Total", "direction": "higher", "target": 1.40},
    {"group": "Risk", "label": "Risk Delta", "column": "Risk Delta", "direction": "higher", "target": 0.35},
    {"group": "Risk", "label": "Risk Closure Rate", "column": "Risk Closure Rate", "direction": "higher", "target": 0.75},
    {"group": "Cost PMPM", "label": "Inpatient Expense PMPM", "column": "Inpatient Expense PMPM", "direction": "lower", "target": 350.0},
    {"group": "Cost PMPM", "label": "Outpatient Expense PMPM", "column": "Outpatient Expense PMPM", "direction": "lower", "target": 220.0},
    {"group": "Cost PMPM", "label": "Total Expense PMPM", "column": "Total Expense PMPM", "direction": "lower", "target": 1100.0},
    {"group": "Financials", "label": "MLR", "column": "MLR", "direction": "lower", "target": 0.85},
    {"group": "Financials", "label": "Surplus PMPM", "column": "Surplus PMPM", "direction": "higher", "target": 0.0},
    {"group": "Financials", "label": "Surplus", "column": "Surplus", "direction": "higher", "target": 0.0},
    {"group": "Attestation", "label": "Attest Completion Rate", "column": "Attest Completion Rate", "direction": "higher", "target": 0.80},
    {"group": "Attestation", "label": "Attest Rejected Rate", "column": "Attest Rejected Rate", "direction": "lower", "target": 0.05},
    {"group": "Attestation", "label": "Attest Not Addressed Rate", "column": "Attest Not Addressed Rate", "direction": "lower", "target": 0.20},
    {"group": "Stars", "label": "Star C", "column": "Stars C", "direction": "higher", "target": 4.0},
    {"group": "Stars", "label": "Star D", "column": "Stars D", "direction": "higher", "target": 4.0},
    {"group": "Stars", "label": "Stars Total", "column": "Stars Total", "direction": "higher", "target": 4.0},
    {"group": "Utilization", "label": "ADK YTD", "column": "ADK YTD", "direction": "lower", "target": 0.90},
    {"group": "Utilization", "label": "EDK YTD", "column": "EDK YTD", "direction": "lower", "target": 0.70},
    {"group": "Utilization", "label": "Readmit Rate YTD", "column": "Readmit Rate YTD", "direction": "lower", "target": 0.10},
]

SUM_COLUMNS = {"Surplus"}
ID_COLUMNS = [
    "Report Date", "Operational Market", "Managing Entity", "PCP Name", "PCP NPI",
    "Membership", "Member Months",
]
REQUIRED_COLUMNS = ID_COLUMNS + [item["column"] for item in MEASURES]


def load_file(path_or_buffer, suffix: str | None = None) -> pd.DataFrame:
    suffix = suffix or Path(str(path_or_buffer)).suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path_or_buffer)
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(path_or_buffer)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path_or_buffer)
    raise ValueError("Supported data formats are CSV, Parquet, and Excel.")


def prepare_data(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.drop(columns=[c for c in frame.columns if str(c).startswith("Unnamed:")], errors="ignore").copy()
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError("Missing required columns: " + ", ".join(missing))
    frame = frame[REQUIRED_COLUMNS]
    frame["Report Date"] = pd.to_datetime(frame["Report Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    frame = frame.dropna(subset=["Report Date", "Operational Market", "Managing Entity", "PCP Name"])
    for column in ["Membership", "Member Months"] + [item["column"] for item in MEASURES]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def load_data() -> pd.DataFrame:
    path = Path(os.getenv("DATA_PATH", str(DEFAULT_DATA_PATH))).expanduser()
    if not path.exists():
        raise FileNotFoundError(f"Data file not found: {path}")
    return prepare_data(load_file(path))


BASE_DATA = load_data()


def current_data(store_value: str | None) -> pd.DataFrame:
    if not store_value:
        return BASE_DATA
    return prepare_data(pd.read_json(io.StringIO(store_value), orient="split"))


def options(values) -> list[dict]:
    return [{"label": str(value), "value": str(value)} for value in sorted(set(values))]


def weighted_average(frame: pd.DataFrame, column: str) -> float | None:
    valid = frame[[column, "Member Months", "Membership"]].dropna(subset=[column])
    if valid.empty:
        return None
    weights = valid["Member Months"].fillna(0).clip(lower=0)
    if weights.sum() <= 0:
        weights = valid["Membership"].fillna(0).clip(lower=0)
    if weights.sum() <= 0:
        return float(valid[column].mean())
    return float((valid[column] * weights).sum() / weights.sum())


def aggregate(frame: pd.DataFrame) -> dict | None:
    if frame.empty:
        return None
    result = {
        "provider_count": int(frame["PCP NPI"].nunique()),
        "Membership": float(frame["Membership"].fillna(0).sum()),
        "Member Months": float(frame["Member Months"].fillna(0).sum()),
    }
    for item in MEASURES:
        column = item["column"]
        result[column] = float(frame[column].fillna(0).sum()) if column in SUM_COLUMNS else weighted_average(frame, column)
    return result


def format_value(column: str, value: float | None) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    if "Rate" in column or column == "MLR" or "%" in column:
        return f"{value:.1%}"
    if "PMPM" in column:
        return f"${value:,.0f}"
    if column == "Surplus":
        return f"${value:,.0f}"
    if column.startswith("Stars") or column.startswith("Risk"):
        return f"{value:.2f}"
    return f"{value:,.2f}"


def domain(frame: pd.DataFrame, column: str) -> tuple[float, float]:
    series = frame[column].dropna()
    if series.empty:
        return 0.0, 1.0
    low, high = float(series.quantile(0.01)), float(series.quantile(0.99))
    if low == high:
        high = low + 1.0
    return low, high


def normalize(value: float | None, item: dict, date_frame: pd.DataFrame) -> float | None:
    if value is None or pd.isna(value):
        return None
    low, high = domain(date_frame, item["column"])
    score = max(0.0, min(1.0, (float(value) - low) / (high - low)))
    return score if item["direction"] == "higher" else 1.0 - score


def status(value: float | None, reference: float | None, direction: str) -> str:
    if value is None or reference is None or pd.isna(value) or pd.isna(reference):
        return "neutral"
    delta = value - reference if direction == "higher" else reference - value
    if abs(delta) < 1e-12:
        return "neutral"
    return "good" if delta > 0 else "bad"


def target_rows() -> list[dict]:
    return [
        {"Group": item["group"], "Measure": item["label"], "Column": item["column"], "Target": item["target"]}
        for item in MEASURES
    ]


app = Dash(
    __name__,
    title="Provider Performance Hex",
    suppress_callback_exceptions=True,
)
server = app.server

app.layout = html.Div([
    dcc.Store(id="uploaded-data"),
    dcc.Store(id="hex-payload"),
    html.Div(id="hex-render-token", style={"display": "none"}),
    html.Div(id="hex-tooltip", className="tooltip"),
    html.Header([
        html.Div([
            html.H1("Provider Performance · Hex View", className="app-title"),
            html.Div("Risk, cost, financial, attestation, Stars and utilization measures", className="app-subtitle"),
        ]),
        html.Div("Dash · Plotly · D3", className="badge"),
    ], className="app-header"),
    html.Main([
        html.Section([
            html.Div([
                html.Div([html.Label("Report date", className="field-label"), dcc.Dropdown(id="date-filter", clearable=False)], className="field"),
                html.Div([html.Label("Operational market", className="field-label"), dcc.Dropdown(id="market-filter", placeholder="All markets")], className="field"),
                html.Div([html.Label("Managing entity", className="field-label"), dcc.Dropdown(id="entity-filter", placeholder="All managing entities")], className="field wide"),
                html.Div([html.Label("PCP name", className="field-label"), dcc.Dropdown(id="pcp-filter", placeholder="All PCPs")], className="field wide"),
                html.Div([
                    html.Label("Reference line", className="field-label"),
                    dcc.RadioItems(
                        id="reference-mode", value="target", inline=True,
                        options=[{"label": "Target threshold", "value": "target"}, {"label": "Comparison entity", "value": "compare"}],
                        className="radio-wrap",
                    ),
                ], className="field wide"),
                html.Div([
                    html.Label("Compare at", className="field-label"),
                    dcc.Dropdown(
                        id="compare-level", value="market", clearable=False,
                        options=[{"label": "Market", "value": "market"}, {"label": "Managing entity", "value": "entity"}, {"label": "PCP", "value": "pcp"}],
                    ),
                ], id="compare-level-wrap", className="field"),
                html.Div([html.Label("Comparison name", className="field-label"), dcc.Dropdown(id="compare-name")], id="compare-name-wrap", className="field wide"),
                html.Div([
                    html.Label("Data source", className="field-label"),
                    dcc.Upload(id="upload-data", children="Upload CSV / Parquet / Excel", className="upload", multiple=False),
                    html.Div("Bundled mock dataset", id="upload-status", className="upload-status"),
                ], className="field wide"),
                html.Div(id="scope-note", className="scope-note"),
            ], className="control-grid"),
        ], className="card controls"),
        html.Section(id="summary", className="card summary"),
        html.Section([
            html.Div([
                html.Div([
                    html.Div([
                        html.Div([html.H2("Hierarchical hex", className="panel-title"), html.Div("Labels follow the hex edges · hover for raw values", className="hint")]),
                        html.Div([
                            html.Span([html.I(className="dot green"), "Favorable"]),
                            html.Span([html.I(className="dot red"), "Unfavorable"]),
                            html.Span([html.I(className="line-key"), "Reference"]),
                        ], className="legend"),
                    ], className="panel-head"),
                    html.Div(id="hex-chart"),
                    html.Div(id="logic-note", className="logic-note"),
                ], className="card panel"),
                html.Div([
                    html.Div([html.H2("Measure detail", className="panel-title"), html.Div("Normalized performance with raw values in hover", className="hint")], className="panel-head"),
                    dcc.Graph(id="detail-chart", config={"displayModeBar": False, "responsive": True}, className="dash-graph"),
                    html.Div([
                        html.Div("Edit targets in original measure units", className="field-label"),
                        dash_table.DataTable(
                            id="target-table", data=target_rows(),
                            columns=[
                                {"name": "Group", "id": "Group", "editable": False},
                                {"name": "Measure", "id": "Measure", "editable": False},
                                {"name": "Target", "id": "Target", "type": "numeric", "editable": True},
                                {"name": "Column", "id": "Column", "editable": False},
                            ],
                            hidden_columns=["Column"], editable=True, page_size=6,
                            style_table={"overflowX": "auto"},
                            style_cell={"padding": "6px", "textAlign": "left", "border": "1px solid #e4ecee"},
                            style_header={"fontWeight": "700", "backgroundColor": "#eef4f3"},
                            style_data_conditional=[{"if": {"column_id": "Target"}, "backgroundColor": "#fffaf0"}],
                        ),
                    ], id="target-wrap", className="target-wrap"),
                ], className="card panel"),
            ], className="dashboard-grid"),
        ]),
        html.Div("Rollups use member-month-weighted averages except Surplus, which is summed. Normalization uses the selected report date's 1st–99th percentile range.", className="footer-note"),
    ], className="shell"),
])


@callback(
    Output("uploaded-data", "data"),
    Output("upload-status", "children"),
    Input("upload-data", "contents"),
    State("upload-data", "filename"),
    prevent_initial_call=True,
)
def ingest_upload(contents, filename):
    if not contents or not filename:
        raise PreventUpdate
    try:
        _, encoded = contents.split(",", 1)
        raw = base64.b64decode(encoded)
        suffix = Path(filename).suffix.lower()
        frame = prepare_data(load_file(io.BytesIO(raw), suffix))
        return frame.to_json(orient="split"), f"Loaded {filename} · {len(frame):,} rows"
    except Exception as exc:
        return no_update, html.Span(f"Upload failed: {exc}", className="error")


@callback(Output("date-filter", "options"), Output("date-filter", "value"), Input("uploaded-data", "data"))
def set_dates(store_value):
    frame = current_data(store_value)
    dates = sorted(frame["Report Date"].dropna().unique(), reverse=True)
    return options(dates), dates[0] if dates else None


@callback(Output("market-filter", "options"), Output("market-filter", "value"), Input("date-filter", "value"), Input("uploaded-data", "data"))
def set_markets(report_date, store_value):
    frame = current_data(store_value)
    frame = frame[frame["Report Date"] == report_date]
    return options(frame["Operational Market"].dropna()), None


@callback(Output("entity-filter", "options"), Output("entity-filter", "value"), Input("date-filter", "value"), Input("market-filter", "value"), Input("uploaded-data", "data"))
def set_entities(report_date, market, store_value):
    frame = current_data(store_value)
    frame = frame[frame["Report Date"] == report_date]
    if market:
        frame = frame[frame["Operational Market"] == market]
    return options(frame["Managing Entity"].dropna()), None


@callback(Output("pcp-filter", "options"), Output("pcp-filter", "value"), Input("date-filter", "value"), Input("market-filter", "value"), Input("entity-filter", "value"), Input("uploaded-data", "data"))
def set_pcps(report_date, market, entity, store_value):
    frame = current_data(store_value)
    frame = frame[frame["Report Date"] == report_date]
    if market:
        frame = frame[frame["Operational Market"] == market]
    if entity:
        frame = frame[frame["Managing Entity"] == entity]
    return options(frame["PCP Name"].dropna()), None


@callback(
    Output("compare-name", "options"), Output("compare-name", "value"),
    Input("date-filter", "value"), Input("compare-level", "value"),
    Input("market-filter", "value"), Input("entity-filter", "value"), Input("uploaded-data", "data"),
)
def set_comparisons(report_date, level, market, entity, store_value):
    frame = current_data(store_value)
    frame = frame[frame["Report Date"] == report_date]
    if level in {"entity", "pcp"} and market:
        frame = frame[frame["Operational Market"] == market]
    if level == "pcp" and entity:
        frame = frame[frame["Managing Entity"] == entity]
    column = {"market": "Operational Market", "entity": "Managing Entity", "pcp": "PCP Name"}[level]
    names = sorted(frame[column].dropna().unique())
    return options(names), names[0] if names else None


def make_kpi(label, value, detail):
    return html.Div([
        html.Div(label, className="kpi-label"),
        html.Div(value, className="kpi-value"),
        html.Div(detail, className="kpi-detail"),
    ], className="kpi")


@callback(
    Output("summary", "children"), Output("scope-note", "children"),
    Output("detail-chart", "figure"), Output("hex-payload", "data"), Output("logic-note", "children"),
    Output("compare-level-wrap", "style"), Output("compare-name-wrap", "style"), Output("target-wrap", "style"),
    Input("date-filter", "value"), Input("market-filter", "value"), Input("entity-filter", "value"), Input("pcp-filter", "value"),
    Input("reference-mode", "value"), Input("compare-level", "value"), Input("compare-name", "value"),
    Input("target-table", "data"), Input("uploaded-data", "data"),
)
def update_dashboard(report_date, market, entity, pcp, reference_mode, compare_level, compare_name, target_data, store_value):
    frame = current_data(store_value)
    date_frame = frame[frame["Report Date"] == report_date]
    selected = date_frame
    if market:
        selected = selected[selected["Operational Market"] == market]
    if entity:
        selected = selected[selected["Managing Entity"] == entity]
    if pcp:
        selected = selected[selected["PCP Name"] == pcp]

    current = aggregate(selected)
    if not current:
        empty = go.Figure().update_layout(annotations=[{"text": "No data for this selection", "showarrow": False}])
        return [], "No rows", empty, None, "", {}, {}, {}

    current_name = pcp or entity or market or "All markets"
    comparison = None
    reference_label = "Target threshold"
    if reference_mode == "compare" and compare_name:
        column = {"market": "Operational Market", "entity": "Managing Entity", "pcp": "PCP Name"}[compare_level]
        comparison = aggregate(date_frame[date_frame[column] == compare_name])
        reference_label = compare_name

    targets = {row["Column"]: float(row["Target"]) for row in (target_data or target_rows()) if row.get("Target") not in (None, "")}
    measure_payload = []
    normalized_values, normalized_refs, raw_labels, raw_ref_labels, statuses = [], [], [], [], []

    for item in MEASURES:
        column = item["column"]
        raw_value = current[column]
        reference = comparison[column] if reference_mode == "compare" and comparison else targets.get(column, item["target"])
        score = normalize(raw_value, item, date_frame)
        ref_score = normalize(reference, item, date_frame)
        item_status = status(raw_value, reference, item["direction"]) if reference is not None else "neutral"
        measure_payload.append({
            "group": item["group"], "label": item["label"], "column": column,
            "score": score or 0.0, "reference_score": ref_score,
            "raw_display": format_value(column, raw_value), "reference_display": format_value(column, reference),
            "status": item_status,
        })
        normalized_values.append(score or 0.0)
        normalized_refs.append(ref_score)
        raw_labels.append(format_value(column, raw_value))
        raw_ref_labels.append(format_value(column, reference))
        statuses.append("#17845c" if item_status == "good" else "#c94d53" if item_status == "bad" else "#81949b")

    figure = go.Figure()
    figure.add_bar(
        y=[item["label"] for item in MEASURES], x=normalized_values, orientation="h",
        marker_color=statuses, customdata=raw_labels, text=raw_labels, textposition="outside", cliponaxis=False,
        hovertemplate="%{y}<br>Selected: %{customdata}<br>Normalized: %{x:.2f}<extra></extra>",
    )
    figure.add_scatter(
        y=[item["label"] for item in MEASURES], x=normalized_refs, mode="markers",
        marker={"symbol": "line-ns-open", "size": 13, "color": "#d3a33d", "line": {"width": 2}},
        customdata=raw_ref_labels,
        hovertemplate=f"{reference_label}: %{{customdata}}<br>Normalized: %{{x:.2f}}<extra></extra>",
    )
    figure.update_layout(
        height=530, margin={"l": 165, "r": 52, "t": 20, "b": 45}, showlegend=False,
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font={"family": "Inter, Arial", "size": 10, "color": "#294657"},
        xaxis={"title": "Normalized performance score", "range": [0, 1.12], "gridcolor": "#e5edef", "zerolinecolor": "#d3e0e2"},
        yaxis={"autorange": "reversed", "tickfont": {"size": 9}},
        hoverlabel={"bgcolor": "#102f42", "font": {"color": "#fff"}},
    )

    summary = [
        make_kpi("Providers", f"{current['provider_count']:,}", current_name),
        make_kpi("Membership", f"{current['Membership']:,.0f}", f"{current['Member Months']:,.0f} member months"),
        make_kpi("Stars Total", format_value("Stars Total", current["Stars Total"]), "Member-month-weighted average"),
        make_kpi("MLR", format_value("MLR", current["MLR"]), "Member-month-weighted average"),
    ]
    payload = {
        "groups": list(dict.fromkeys(item["group"] for item in MEASURES)),
        "measures": measure_payload, "reference_label": reference_label, "reference_mode": reference_mode,
    }
    scope_note = f"{len(selected):,} provider rows · {current_name}"
    if reference_mode == "compare":
        logic = "Color compares the selected scope with the chosen entity. Green means better performance after applying each measure's direction."
    else:
        logic = "Color compares the selected scope with editable targets. Lower is favorable for cost, MLR, rejected/not-addressed attestations, ADK, EDK, and readmissions."
    compare_style = {} if reference_mode == "compare" else {"display": "none"}
    target_style = {} if reference_mode == "target" else {"display": "none"}
    return summary, scope_note, figure, payload, logic, compare_style, compare_style, target_style


app.clientside_callback(
    """
    function(payload) {
        return window.dash_clientside.hex.render(payload);
    }
    """,
    Output("hex-render-token", "children"),
    Input("hex-payload", "data"),
)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8050")), debug=os.getenv("DASH_DEBUG", "0") == "1")
