
from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dash import Dash, Input, Output, State, callback, dash_table, dcc, html, no_update


APP_TITLE = "VOP Inpatient Utilization & Hospitalist Performance"
DEFAULT_DATA_PATH = Path(__file__).parent / "data" / "sample_event_backbone.csv"

LOW_VOLUME_MAX = int(os.getenv("LOW_VOLUME_MAX", "9"))
MODERATE_VOLUME_MAX = int(os.getenv("MODERATE_VOLUME_MAX", "24"))
HEATMAP_MIN_VOLUME = int(os.getenv("HEATMAP_MIN_VOLUME", "10"))

PLOT_TEMPLATE = "plotly_white"
PLOT_CONFIG = {
    "displaylogo": False,
    "modeBarButtonsToRemove": ["lasso2d", "select2d"],
    "responsive": True,
}

GROUP_COL = "AttendingProviderHospitalGroupName_VOP"
PROVIDER_COL = "AttendingProviderName"
PROVIDER_NPI_COL = "AttendingProviderNPI"
FACILITY_COL = "AdmittingFacilityNameGroup"


def read_dataset(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Dashboard data file was not found: {path}")

    suffix = path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(path, low_memory=False)
    elif suffix in {".parquet", ".pq"}:
        df = pd.read_parquet(path)
    elif suffix in {".feather", ".ft"}:
        df = pd.read_feather(path)
    else:
        raise ValueError("Supported formats are CSV, Parquet, and Feather.")

    return prepare_dataset(df)


def first_existing(df: pd.DataFrame, columns: Iterable[str], default=None):
    for column in columns:
        if column in df.columns:
            return df[column]
    return pd.Series(default, index=df.index)


def prepare_dataset(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    date_columns = [
        "Admit", "Discharge", "AdmitMonth", "DischargeMonth",
        "ReadmissionIndexMonth", "MetricMonth", "ConsultServiceDate",
    ]
    for column in date_columns:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors="coerce")

    if "MetricMonth" not in df.columns:
        metric = first_existing(df, ["ReadmissionIndexMonth"])
        if "DischargeMonth" in df.columns:
            metric = metric.combine_first(df["DischargeMonth"])
        if "AdmitMonth" in df.columns:
            metric = metric.combine_first(df["AdmitMonth"])
        if "Admit" in df.columns:
            metric = metric.combine_first(df["Admit"].dt.to_period("M").dt.to_timestamp())
        df["MetricMonth"] = pd.to_datetime(metric, errors="coerce")

    if FACILITY_COL not in df.columns:
        df[FACILITY_COL] = first_existing(
            df,
            ["AdmittingFacilityNameClean", "AdmittingFacilityName"],
            "UNMAPPED FACILITY",
        )

    if GROUP_COL not in df.columns:
        df[GROUP_COL] = "UNMAPPED"

    if PROVIDER_COL not in df.columns:
        df[PROVIDER_COL] = "UNMAPPED PROVIDER"

    if PROVIDER_NPI_COL not in df.columns:
        df[PROVIDER_NPI_COL] = df[PROVIDER_COL]

    defaults = {
        "AuthznKey": np.arange(len(df)).astype(str),
        "BedType": "Unknown",
        "ReportingPod": "Unknown",
        "OperationalSubMarket": "Unknown",
        "LengthOfStay": np.nan,
        "ConsultFlag": 0,
        "ConsultServiceGroup": "No Consult",
        "ConsultProviderName": "No Consult",
        "ReadmitNumerator": 0,
        "ReadmitDenominator": 0,
        "TransferBufferFlag": 0,
        "OverlapOrDateIssueFlag": 0,
        "DaysToNextAcuteAdmit": np.nan,
        "ReadmissionClassification": "Not Applicable",
        "Discharge_Disposition_Group": "Unknown",
        "PrimaryDiagnosis": "Unknown",
        "Obs_Claim_Indicator": None,
        "AttendingProvider_VOP": 0,
    }
    for column, value in defaults.items():
        if column not in df.columns:
            df[column] = value

    numeric_columns = [
        "LengthOfStay", "ConsultFlag", "ReadmitNumerator",
        "ReadmitDenominator", "TransferBufferFlag",
        "OverlapOrDateIssueFlag", "DaysToNextAcuteAdmit",
        "AttendingProvider_VOP",
    ]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)

    string_columns = [
        FACILITY_COL, GROUP_COL, PROVIDER_COL, PROVIDER_NPI_COL,
        "BedType", "ReportingPod", "OperationalSubMarket",
        "ConsultServiceGroup", "ConsultProviderName",
        "ReadmissionClassification", "Discharge_Disposition_Group",
        "PrimaryDiagnosis",
    ]
    for column in string_columns:
        df[column] = (
            df[column]
            .fillna("UNMAPPED" if column in {FACILITY_COL, GROUP_COL} else "Unknown")
            .astype(str)
            .str.strip()
        )

    df["BedType"] = df["BedType"].str.upper().replace(
        {"OBSERVATION": "OBS", "INPATIENT": "ACUTE"}
    )
    df[GROUP_COL] = (
        df[GROUP_COL]
        .str.upper()
        .replace({
            "BEYOND": "BENCHMARK / BEYOND",
            "BEYOND PHYSICIANS": "BENCHMARK / BEYOND",
            "BENCHMARK": "BENCHMARK / BEYOND",
            "BENCHMARK IP": "BENCHMARK / BEYOND",
        })
    )

    df["ConsultFlag"] = (df["ConsultFlag"] > 0).astype(int)
    df["ReadmitNumerator"] = (df["ReadmitNumerator"] > 0).astype(int)
    df["ReadmitDenominator"] = (df["ReadmitDenominator"] > 0).astype(int)
    df["IsMappedGroup"] = (~df[GROUP_COL].isin(["UNMAPPED", "UNKNOWN", ""])).astype(int)
    df["IsAcute"] = df["BedType"].eq("ACUTE").astype(int)
    df["IsOBS"] = df["BedType"].eq("OBS").astype(int)
    df["IsLTAC"] = df["BedType"].eq("LTAC").astype(int)
    return df


DATA_PATH = Path(os.getenv("DASH_DATA_PATH", str(DEFAULT_DATA_PATH)))
DF = read_dataset(DATA_PATH)


def options_for(column: str):
    values = sorted(v for v in DF[column].dropna().unique() if str(v).strip())
    return [{"label": str(value), "value": value} for value in values]


def rate(numerator, denominator):
    denominator = float(denominator)
    return float(numerator) / denominator if denominator else np.nan


def pct(value):
    return "—" if pd.isna(value) else f"{value:.1%}"


def number(value):
    return f"{int(value):,}"


def selected_period_reliability(df: pd.DataFrame) -> pd.DataFrame:
    counts = (
        df.dropna(subset=[PROVIDER_NPI_COL])
        .groupby(PROVIDER_NPI_COL)["AuthznKey"]
        .nunique()
        .rename("SelectedPeriodStayCount")
    )
    if counts.empty:
        return pd.DataFrame(
            columns=[
                PROVIDER_NPI_COL, "SelectedPeriodStayCount",
                "SelectedPeriodReliability", "SelectedPeriodVolumePercentile",
                "SelectedPeriodHighVolumeFlag",
            ]
        )

    reliability = counts.reset_index()
    reliability["SelectedPeriodReliability"] = np.select(
        [
            reliability["SelectedPeriodStayCount"] <= LOW_VOLUME_MAX,
            reliability["SelectedPeriodStayCount"] <= MODERATE_VOLUME_MAX,
        ],
        ["Low Volume", "Moderate Volume"],
        default="Reliable Volume",
    )
    reliability["SelectedPeriodVolumePercentile"] = (
        reliability["SelectedPeriodStayCount"].rank(pct=True, method="max")
    )
    reliability["SelectedPeriodHighVolumeFlag"] = (
        reliability["SelectedPeriodVolumePercentile"] >= 0.95
    ).astype(int)
    return reliability


def apply_filters(
    start_date,
    end_date,
    pods,
    facilities,
    groups,
    providers,
    bed_types,
    reliability,
):
    dff = DF.copy()
    if start_date:
        dff = dff[dff["MetricMonth"] >= pd.Timestamp(start_date)]
    if end_date:
        dff = dff[dff["MetricMonth"] <= pd.Timestamp(end_date)]
    if pods:
        dff = dff[dff["ReportingPod"].isin(pods)]
    if facilities:
        dff = dff[dff[FACILITY_COL].isin(facilities)]
    if groups:
        dff = dff[dff[GROUP_COL].isin(groups)]
    if providers:
        dff = dff[dff[PROVIDER_COL].isin(providers)]
    if bed_types:
        dff = dff[dff["BedType"].isin(bed_types)]

    rel = selected_period_reliability(dff)
    dff = dff.merge(rel, on=PROVIDER_NPI_COL, how="left")

    if reliability:
        dff = dff[dff["SelectedPeriodReliability"].isin(reliability)]

    return dff


def card(title, value_id, note=None, class_name="metric-card"):
    children = [
        html.Div(title, className="metric-title"),
        html.Div(id=value_id, className="metric-value"),
    ]
    if note:
        children.append(html.Div(note, className="metric-note"))
    return html.Div(children, className=class_name)


def graph_card(title, graph_id, subtitle=None, controls=None, class_name="chart-card"):
    header = [
        html.Div(
            [
                html.Div(title, className="chart-title"),
                html.Div(subtitle, className="chart-subtitle") if subtitle else None,
            ]
        ),
        controls,
    ]
    return html.Div(
        [
            html.Div(header, className="chart-header"),
            dcc.Loading(dcc.Graph(id=graph_id, config=PLOT_CONFIG), type="circle"),
        ],
        className=class_name,
    )


def empty_figure(message="No records match the selected filters."):
    fig = go.Figure()
    fig.add_annotation(
        text=message, x=0.5, y=0.5, xref="paper", yref="paper",
        showarrow=False, font={"size": 15}
    )
    fig.update_layout(template=PLOT_TEMPLATE, xaxis={"visible": False}, yaxis={"visible": False})
    return fig


min_date = DF["MetricMonth"].min()
max_date = DF["MetricMonth"].max()

app = Dash(__name__, title=APP_TITLE, suppress_callback_exceptions=True)
server = app.server

app.layout = html.Div(
    [
        dcc.Download(id="download-filtered-summary"),
        html.Div(
            [
                html.Div(
                    [
                        html.Div("CAREALLIES • VOP", className="eyebrow"),
                        html.H1(APP_TITLE),
                        html.P(
                            "Acute, observation, LTAC, readmissions, inpatient consults, "
                            "facility routing, and provider reliability.",
                            className="subtitle",
                        ),
                    ]
                ),
                html.Div(
                    [
                        html.Button(
                            "Download filtered summary",
                            id="download-button",
                            className="primary-button",
                            n_clicks=0,
                        ),
                        html.Div(
                            f"Source: {DATA_PATH.name}",
                            className="source-label",
                        ),
                    ],
                    className="header-actions",
                ),
            ],
            className="app-header",
        ),

        html.Div(
            [
                html.Div(
                    [
                        html.Label("Metric month"),
                        dcc.DatePickerRange(
                            id="date-range",
                            min_date_allowed=min_date.date(),
                            max_date_allowed=max_date.date(),
                            start_date=min_date.date(),
                            end_date=max_date.date(),
                            display_format="MMM D, YYYY",
                        ),
                    ],
                    className="filter-block date-filter",
                ),
                html.Div(
                    [
                        html.Label("Reporting pod"),
                        dcc.Dropdown(
                            id="pod-filter",
                            options=options_for("ReportingPod"),
                            multi=True,
                            placeholder="All pods",
                        ),
                    ],
                    className="filter-block",
                ),
                html.Div(
                    [
                        html.Label("Facility"),
                        dcc.Dropdown(
                            id="facility-filter",
                            options=options_for(FACILITY_COL),
                            multi=True,
                            placeholder="All facilities",
                        ),
                    ],
                    className="filter-block wide-filter",
                ),
                html.Div(
                    [
                        html.Label("Hospitalist group"),
                        dcc.Dropdown(
                            id="group-filter",
                            options=options_for(GROUP_COL),
                            multi=True,
                            placeholder="All groups",
                        ),
                    ],
                    className="filter-block",
                ),
                html.Div(
                    [
                        html.Label("Attending provider"),
                        dcc.Dropdown(
                            id="provider-filter",
                            options=options_for(PROVIDER_COL),
                            multi=True,
                            placeholder="All providers",
                        ),
                    ],
                    className="filter-block wide-filter",
                ),
                html.Div(
                    [
                        html.Label("Bed type"),
                        dcc.Dropdown(
                            id="bed-filter",
                            options=options_for("BedType"),
                            multi=True,
                            placeholder="All bed types",
                        ),
                    ],
                    className="filter-block",
                ),
                html.Div(
                    [
                        html.Label("Provider reliability"),
                        dcc.Dropdown(
                            id="reliability-filter",
                            options=[
                                {"label": "Low Volume (1–9)", "value": "Low Volume"},
                                {"label": "Moderate Volume (10–24)", "value": "Moderate Volume"},
                                {"label": "Reliable Volume (25+)", "value": "Reliable Volume"},
                            ],
                            multi=True,
                            placeholder="All reliability tiers",
                        ),
                    ],
                    className="filter-block",
                ),
                html.Button("Reset", id="reset-filters", className="secondary-button"),
            ],
            className="filter-panel",
        ),

        html.Div(
            [
                card("Total stays", "kpi-total-stays", "Distinct authorization events"),
                card("Observation rate", "kpi-obs-rate", "OBS ÷ (OBS + Acute)"),
                card("30-day readmission", "kpi-readmit-rate", "Numerator ÷ eligible index stays"),
                card("Average LOS", "kpi-los", "Across selected stays"),
                card("Consult rate", "kpi-consult-rate", "Stays with first qualifying consult"),
                card("Mapped group coverage", "kpi-mapped-rate", "Excludes UNMAPPED group"),
            ],
            className="metric-grid",
        ),

        dcc.Tabs(
            id="dashboard-tabs",
            value="overview",
            className="tabs",
            children=[
                dcc.Tab(
                    label="Executive Overview",
                    value="overview",
                    children=[
                        html.Div(
                            [
                                graph_card(
                                    "Monthly utilization and readmissions",
                                    "monthly-utilization",
                                    "Bed-type volume with the 30-day readmission rate.",
                                    class_name="chart-card span-2",
                                ),
                                graph_card(
                                    "Share of identified business",
                                    "group-share",
                                    "Mapped hospitalist-group share. UNMAPPED is excluded from the denominator.",
                                ),
                                graph_card(
                                    "Facility volume and bed mix",
                                    "facility-volume",
                                    "Top facilities by selected-period event volume.",
                                ),
                                graph_card(
                                    "Discharge disposition",
                                    "discharge-disposition",
                                    "Distribution across home, home health, SNF/rehab, and other.",
                                ),
                            ],
                            className="chart-grid",
                        )
                    ],
                ),
                dcc.Tab(
                    label="Facility & Group",
                    value="facility-group",
                    children=[
                        html.Div(
                            [
                                graph_card(
                                    "Observation-rate heatmap",
                                    "obs-heatmap",
                                    f"Facility × group. Cells below {HEATMAP_MIN_VOLUME} Acute + OBS stays are masked.",
                                    class_name="chart-card span-2",
                                ),
                                html.Div(
                                    [
                                        html.Div(
                                            [
                                                html.Div("Facility spotlight", className="chart-title"),
                                                html.Div(
                                                    "Defaults to a Harlingen facility when available.",
                                                    className="chart-subtitle",
                                                ),
                                            ]
                                        ),
                                        dcc.Dropdown(
                                            id="spotlight-facility",
                                            options=options_for(FACILITY_COL),
                                            value=next(
                                                (
                                                    v for v in DF[FACILITY_COL].dropna().unique()
                                                    if "HARLINGEN" in str(v).upper()
                                                ),
                                                DF[FACILITY_COL].dropna().iloc[0],
                                            ),
                                            clearable=False,
                                            className="spotlight-dropdown",
                                        ),
                                        dcc.Loading(
                                            dcc.Graph(id="facility-spotlight", config=PLOT_CONFIG),
                                            type="circle",
                                        ),
                                    ],
                                    className="chart-card",
                                ),
                                html.Div(
                                    [
                                        html.Div("Facility and group performance", className="chart-title"),
                                        html.Div(
                                            "Rates are recomputed from additive numerators and denominators.",
                                            className="chart-subtitle",
                                        ),
                                        dash_table.DataTable(
                                            id="facility-group-table",
                                            page_size=15,
                                            sort_action="native",
                                            filter_action="native",
                                            fixed_rows={"headers": True},
                                            style_table={"overflowX": "auto", "maxHeight": "560px"},
                                            style_cell={
                                                "fontFamily": "Inter, Arial, sans-serif",
                                                "fontSize": 12,
                                                "padding": "9px",
                                                "textAlign": "left",
                                                "minWidth": "90px",
                                                "maxWidth": "220px",
                                                "whiteSpace": "normal",
                                            },
                                            style_header={
                                                "fontWeight": 700,
                                                "backgroundColor": "#EEF2F7",
                                                "border": "none",
                                            },
                                            style_data_conditional=[
                                                {
                                                    "if": {"filter_query": "{Reliability} = 'Low Volume'"},
                                                    "backgroundColor": "#FFF4E5",
                                                }
                                            ],
                                        ),
                                    ],
                                    className="table-card span-2",
                                ),
                            ],
                            className="chart-grid",
                        )
                    ],
                ),
                dcc.Tab(
                    label="Providers",
                    value="providers",
                    children=[
                        html.Div(
                            [
                                graph_card(
                                    "Provider performance and reliability",
                                    "provider-bubble",
                                    "Volume versus observation rate. Bubble size reflects eligible readmission volume.",
                                    class_name="chart-card span-2",
                                ),
                                graph_card(
                                    "Top attending providers by bed type",
                                    "provider-volume",
                                    "Selected-period event volume.",
                                ),
                                html.Div(
                                    [
                                        html.Div("Provider drilldown", className="chart-title"),
                                        html.Div(
                                            "Low-volume results are retained but highlighted for cautious interpretation.",
                                            className="chart-subtitle",
                                        ),
                                        dash_table.DataTable(
                                            id="provider-table",
                                            page_size=20,
                                            sort_action="native",
                                            filter_action="native",
                                            fixed_rows={"headers": True},
                                            style_table={"overflowX": "auto", "maxHeight": "620px"},
                                            style_cell={
                                                "fontFamily": "Inter, Arial, sans-serif",
                                                "fontSize": 12,
                                                "padding": "9px",
                                                "textAlign": "left",
                                                "minWidth": "90px",
                                                "maxWidth": "230px",
                                                "whiteSpace": "normal",
                                            },
                                            style_header={
                                                "fontWeight": 700,
                                                "backgroundColor": "#EEF2F7",
                                                "border": "none",
                                            },
                                            style_data_conditional=[
                                                {
                                                    "if": {"filter_query": "{Reliability} = 'Low Volume'"},
                                                    "backgroundColor": "#FFF4E5",
                                                },
                                                {
                                                    "if": {"filter_query": "{Top 5% Volume} = 'Yes'"},
                                                    "fontWeight": 700,
                                                },
                                            ],
                                        ),
                                    ],
                                    className="table-card span-3",
                                ),
                            ],
                            className="chart-grid provider-grid",
                        )
                    ],
                ),
                dcc.Tab(
                    label="Readmissions",
                    value="readmissions",
                    children=[
                        html.Div(
                            [
                                graph_card(
                                    "30-day readmission trend",
                                    "readmit-trend",
                                    "Calculated as SUM(ReadmitNumerator) ÷ SUM(ReadmitDenominator).",
                                    class_name="chart-card span-2",
                                ),
                                graph_card(
                                    "Readmission rate by hospitalist group",
                                    "readmit-group",
                                    "Minimum denominator threshold is displayed in the hover detail.",
                                ),
                                graph_card(
                                    "Readmission rate by facility",
                                    "readmit-facility",
                                    "Sorted by eligible index-stay volume.",
                                ),
                                graph_card(
                                    "Days to next acute admission",
                                    "days-to-readmit",
                                    "Distribution for eligible index stays with a subsequent acute admission.",
                                ),
                                graph_card(
                                    "Readmission classifications",
                                    "readmit-classification",
                                    "Includes transfer-buffer and data-quality classifications.",
                                ),
                            ],
                            className="chart-grid",
                        )
                    ],
                ),
                dcc.Tab(
                    label="Consults & LTAC",
                    value="consults-ltac",
                    children=[
                        html.Div(
                            [
                                graph_card(
                                    "First qualifying inpatient consult",
                                    "consult-services",
                                    "Earliest qualifying specialty consult during the stay.",
                                ),
                                graph_card(
                                    "Consult rate by facility",
                                    "consult-facility",
                                    "Stays with a consult ÷ total stays.",
                                ),
                                graph_card(
                                    "LTAC utilization by facility",
                                    "ltac-facility",
                                    "LTAC events and share of total facility volume.",
                                ),
                                graph_card(
                                    "Consult rate by hospitalist group",
                                    "consult-group",
                                    "Useful for comparing specialty-service patterns across groups.",
                                ),
                            ],
                            className="chart-grid",
                        )
                    ],
                ),
                dcc.Tab(
                    label="Methodology & Data Quality",
                    value="methodology",
                    children=[
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.H3("Metric definitions"),
                                        html.Div(
                                            [
                                                html.Div(
                                                    [
                                                        html.Strong("Observation rate"),
                                                        html.P("OBS stays ÷ (OBS stays + Acute stays). LTAC is excluded."),
                                                    ],
                                                    className="definition-item",
                                                ),
                                                html.Div(
                                                    [
                                                        html.Strong("Readmission rate"),
                                                        html.P(
                                                            "SUM(ReadmitNumerator) ÷ SUM(ReadmitDenominator). "
                                                            "Never average precomputed rates."
                                                        ),
                                                    ],
                                                    className="definition-item",
                                                ),
                                                html.Div(
                                                    [
                                                        html.Strong("Share of identified business"),
                                                        html.P(
                                                            "Mapped group stays ÷ mapped stays within the selected context. "
                                                            "UNMAPPED is shown separately in data quality."
                                                        ),
                                                    ],
                                                    className="definition-item",
                                                ),
                                                html.Div(
                                                    [
                                                        html.Strong("Provider reliability"),
                                                        html.P(
                                                            f"Selected-period volume: Low 1–{LOW_VOLUME_MAX}, "
                                                            f"Moderate {LOW_VOLUME_MAX + 1}–{MODERATE_VOLUME_MAX}, "
                                                            f"Reliable {MODERATE_VOLUME_MAX + 1}+."
                                                        ),
                                                    ],
                                                    className="definition-item",
                                                ),
                                            ]
                                        ),
                                    ],
                                    className="method-card",
                                ),
                                html.Div(
                                    [
                                        html.H3("Data quality"),
                                        html.Div(id="data-quality-cards", className="quality-grid"),
                                        dcc.Graph(id="mapping-quality", config=PLOT_CONFIG),
                                    ],
                                    className="method-card",
                                ),
                                html.Div(
                                    [
                                        html.H3("Interpretation guardrails"),
                                        html.Ul(
                                            [
                                                html.Li(
                                                    "The event table must remain one row per authorization key."
                                                ),
                                                html.Li(
                                                    "Provider attribution is a professional-claim proxy, not a formal attending designation."
                                                ),
                                                html.Li(
                                                    "The consult field captures the first qualifying mapped specialty consult only."
                                                ),
                                                html.Li(
                                                    "Low-volume provider rates remain visible but should not drive rankings or conclusions."
                                                ),
                                                html.Li(
                                                    "Production deployment should use SSO, row-level authorization, encrypted transport, and no raw member identifiers in browser exports."
                                                ),
                                            ]
                                        ),
                                    ],
                                    className="method-card span-2",
                                ),
                            ],
                            className="method-grid",
                        )
                    ],
                ),
            ],
        ),

        html.Div(
            [
                "Dashboard grain: one authorization event per row. ",
                "Provider reliability is recalculated for the current filters.",
            ],
            className="footer",
        ),
    ],
    className="app-shell",
)


FILTER_INPUTS = [
    Input("date-range", "start_date"),
    Input("date-range", "end_date"),
    Input("pod-filter", "value"),
    Input("facility-filter", "value"),
    Input("group-filter", "value"),
    Input("provider-filter", "value"),
    Input("bed-filter", "value"),
    Input("reliability-filter", "value"),
]


@app.callback(
    Output("kpi-total-stays", "children"),
    Output("kpi-obs-rate", "children"),
    Output("kpi-readmit-rate", "children"),
    Output("kpi-los", "children"),
    Output("kpi-consult-rate", "children"),
    Output("kpi-mapped-rate", "children"),
    Output("monthly-utilization", "figure"),
    Output("group-share", "figure"),
    Output("facility-volume", "figure"),
    Output("discharge-disposition", "figure"),
    Output("obs-heatmap", "figure"),
    Output("facility-group-table", "data"),
    Output("facility-group-table", "columns"),
    Output("provider-bubble", "figure"),
    Output("provider-volume", "figure"),
    Output("provider-table", "data"),
    Output("provider-table", "columns"),
    Output("readmit-trend", "figure"),
    Output("readmit-group", "figure"),
    Output("readmit-facility", "figure"),
    Output("days-to-readmit", "figure"),
    Output("readmit-classification", "figure"),
    Output("consult-services", "figure"),
    Output("consult-facility", "figure"),
    Output("ltac-facility", "figure"),
    Output("consult-group", "figure"),
    Output("data-quality-cards", "children"),
    Output("mapping-quality", "figure"),
    *FILTER_INPUTS,
)
def update_dashboard(
    start_date,
    end_date,
    pods,
    facilities,
    groups,
    providers,
    bed_types,
    reliability,
):
    dff = apply_filters(
        start_date, end_date, pods, facilities, groups,
        providers, bed_types, reliability
    )

    if dff.empty:
        empty = empty_figure()
        return (
            "0", "—", "—", "—", "—", "—",
            empty, empty, empty, empty, empty,
            [], [], empty, empty, [], [], empty, empty, empty, empty, empty,
            empty, empty, empty, empty,
            [html.Div("No records", className="quality-card")], empty,
        )

    total_stays = dff["AuthznKey"].nunique()
    acute = dff["IsAcute"].sum()
    obs = dff["IsOBS"].sum()
    readmit_num = dff["ReadmitNumerator"].sum()
    readmit_den = dff["ReadmitDenominator"].sum()
    avg_los = dff["LengthOfStay"].replace(0, np.nan).mean()
    consult_rate = rate(dff["ConsultFlag"].sum(), total_stays)
    mapped_rate = rate(dff["IsMappedGroup"].sum(), total_stays)

    # Monthly utilization and readmissions
    monthly = (
        dff.groupby(["MetricMonth", "BedType"], dropna=False)["AuthznKey"]
        .nunique()
        .rename("Stays")
        .reset_index()
    )
    monthly_readmit = (
        dff.groupby("MetricMonth")[["ReadmitNumerator", "ReadmitDenominator"]]
        .sum()
        .reset_index()
    )
    monthly_readmit["ReadmissionRate"] = (
        monthly_readmit["ReadmitNumerator"]
        / monthly_readmit["ReadmitDenominator"].replace(0, np.nan)
    )
    monthly_util = make_subplots(specs=[[{"secondary_y": True}]])
    bed_order = ["ACUTE", "OBS", "LTAC"]
    for bed in bed_order:
        subset = monthly[monthly["BedType"].eq(bed)]
        monthly_util.add_trace(
            go.Bar(
                x=subset["MetricMonth"], y=subset["Stays"],
                name=bed.title() if bed != "OBS" else "OBS",
                hovertemplate="%{x|%b %Y}<br>Stays: %{y:,}<extra></extra>",
            ),
            secondary_y=False,
        )
    monthly_util.add_trace(
        go.Scatter(
            x=monthly_readmit["MetricMonth"],
            y=monthly_readmit["ReadmissionRate"],
            name="Readmission rate",
            mode="lines+markers",
            hovertemplate="%{x|%b %Y}<br>Readmission: %{y:.1%}<extra></extra>",
        ),
        secondary_y=True,
    )
    monthly_util.update_layout(
        template=PLOT_TEMPLATE, barmode="stack", legend_orientation="h",
        legend_y=1.12, margin={"l": 45, "r": 50, "t": 40, "b": 40},
        hovermode="x unified",
    )
    monthly_util.update_yaxes(title_text="Stays", secondary_y=False)
    monthly_util.update_yaxes(title_text="Readmission rate", tickformat=".0%", secondary_y=True)

    # Identified group share
    mapped = dff[dff["IsMappedGroup"].eq(1)]
    if mapped.empty:
        group_share_fig = empty_figure("No mapped hospitalist groups in the selected data.")
    else:
        group_share = (
            mapped.groupby(GROUP_COL)["AuthznKey"].nunique()
            .sort_values(ascending=True)
            .rename("Stays")
            .reset_index()
        )
        group_share["Share"] = group_share["Stays"] / group_share["Stays"].sum()
        group_share_fig = px.bar(
            group_share, x="Share", y=GROUP_COL, orientation="h",
            text=group_share["Share"].map(lambda x: f"{x:.1%}"),
            hover_data={"Stays": ":,", "Share": ":.1%"},
        )
        group_share_fig.update_traces(textposition="outside")
        group_share_fig.update_layout(
            template=PLOT_TEMPLATE, xaxis_tickformat=".0%", xaxis_title="Share of mapped volume",
            yaxis_title="", margin={"l": 135, "r": 45, "t": 20, "b": 40},
        )

    # Facility volume
    fac_bed = (
        dff.groupby([FACILITY_COL, "BedType"])["AuthznKey"]
        .nunique()
        .rename("Stays")
        .reset_index()
    )
    top_facilities = (
        fac_bed.groupby(FACILITY_COL)["Stays"].sum()
        .nlargest(10).index
    )
    fac_bed = fac_bed[fac_bed[FACILITY_COL].isin(top_facilities)]
    facility_volume_fig = px.bar(
        fac_bed, x="Stays", y=FACILITY_COL, color="BedType",
        orientation="h", category_orders={"BedType": bed_order},
        hover_data={"Stays": ":,"},
    )
    facility_volume_fig.update_layout(
        template=PLOT_TEMPLATE, barmode="stack", yaxis={"categoryorder": "total ascending"},
        xaxis_title="Stays", yaxis_title="", legend_title="Bed type",
        margin={"l": 210, "r": 20, "t": 20, "b": 40},
    )

    # Disposition
    disposition = (
        dff.groupby("Discharge_Disposition_Group")["AuthznKey"]
        .nunique().sort_values(ascending=False).rename("Stays").reset_index()
    )
    discharge_fig = px.pie(
        disposition, names="Discharge_Disposition_Group", values="Stays",
        hole=0.55,
    )
    discharge_fig.update_traces(textposition="inside", textinfo="percent+label")
    discharge_fig.update_layout(
        template=PLOT_TEMPLATE, legend_title="", margin={"l": 10, "r": 10, "t": 20, "b": 10}
    )

    # Facility-group aggregate
    fg = (
        dff.groupby([FACILITY_COL, GROUP_COL], dropna=False)
        .agg(
            Total=("AuthznKey", "nunique"),
            Acute=("IsAcute", "sum"),
            OBS=("IsOBS", "sum"),
            LTAC=("IsLTAC", "sum"),
            ReadmitNumerator=("ReadmitNumerator", "sum"),
            ReadmitDenominator=("ReadmitDenominator", "sum"),
            AvgLOS=("LengthOfStay", "mean"),
            Consults=("ConsultFlag", "sum"),
        )
        .reset_index()
    )
    fg["ObservationRate"] = fg["OBS"] / (fg["OBS"] + fg["Acute"]).replace(0, np.nan)
    fg["ReadmissionRate"] = fg["ReadmitNumerator"] / fg["ReadmitDenominator"].replace(0, np.nan)
    fg["ConsultRate"] = fg["Consults"] / fg["Total"].replace(0, np.nan)
    fg["Reliability"] = np.select(
        [fg["Total"] <= LOW_VOLUME_MAX, fg["Total"] <= MODERATE_VOLUME_MAX],
        ["Low Volume", "Moderate Volume"], default="Reliable Volume"
    )

    heat = fg.copy()
    heat["HeatValue"] = heat["ObservationRate"].where(
        (heat["Acute"] + heat["OBS"]) >= HEATMAP_MIN_VOLUME
    )
    heat_pivot = heat.pivot(index=FACILITY_COL, columns=GROUP_COL, values="HeatValue")
    volume_pivot = heat.pivot(
        index=FACILITY_COL, columns=GROUP_COL, values="Total"
    ).reindex(index=heat_pivot.index, columns=heat_pivot.columns)
    if heat_pivot.empty:
        heatmap_fig = empty_figure()
    else:
        custom = np.dstack([
            volume_pivot.fillna(0).values,
            heat_pivot.fillna(np.nan).values,
        ])
        heatmap_fig = go.Figure(
            go.Heatmap(
                z=heat_pivot.values,
                x=heat_pivot.columns,
                y=heat_pivot.index,
                customdata=custom,
                zmin=0,
                zmax=max(0.55, np.nanmax(heat_pivot.values) if np.isfinite(heat_pivot.values).any() else 0.55),
                colorbar={"title": "OBS rate", "tickformat": ".0%"},
                hovertemplate=(
                    "Facility: %{y}<br>Group: %{x}<br>"
                    "OBS rate: %{z:.1%}<br>Total stays: %{customdata[0]:,.0f}"
                    "<extra></extra>"
                ),
            )
        )
        heatmap_fig.update_layout(
            template=PLOT_TEMPLATE, xaxis_title="", yaxis_title="",
            margin={"l": 220, "r": 40, "t": 30, "b": 90},
            height=max(420, 34 * len(heat_pivot.index) + 160),
        )

    fg_table = fg.rename(columns={
        FACILITY_COL: "Facility",
        GROUP_COL: "Hospitalist Group",
        "AvgLOS": "Avg LOS",
        "ObservationRate": "Observation Rate",
        "ReadmissionRate": "Readmission Rate",
        "ReadmitDenominator": "Readmit Denominator",
        "ConsultRate": "Consult Rate",
    })
    fg_table["Observation Rate"] = fg_table["Observation Rate"].map(pct)
    fg_table["Readmission Rate"] = fg_table["Readmission Rate"].map(pct)
    fg_table["Consult Rate"] = fg_table["Consult Rate"].map(pct)
    fg_table["Avg LOS"] = fg_table["Avg LOS"].round(1)
    fg_table = fg_table[
        [
            "Facility", "Hospitalist Group", "Total", "Acute", "OBS", "LTAC",
            "Observation Rate", "Readmit Numerator" if "Readmit Numerator" in fg_table else "ReadmitNumerator",
            "Readmit Denominator", "Readmission Rate", "Avg LOS",
            "Consult Rate", "Reliability",
        ]
    ].rename(columns={"ReadmitNumerator": "Readmit Numerator"})
    fg_table_data = fg_table.sort_values(["Facility", "Total"], ascending=[True, False]).to_dict("records")
    fg_table_columns = [{"name": c, "id": c} for c in fg_table.columns]

    # Provider aggregate
    provider = (
        dff.groupby(
            [PROVIDER_NPI_COL, PROVIDER_COL, GROUP_COL],
            dropna=False
        )
        .agg(
            Total=("AuthznKey", "nunique"),
            Acute=("IsAcute", "sum"),
            OBS=("IsOBS", "sum"),
            LTAC=("IsLTAC", "sum"),
            ReadmitNumerator=("ReadmitNumerator", "sum"),
            ReadmitDenominator=("ReadmitDenominator", "sum"),
            AvgLOS=("LengthOfStay", "mean"),
            Consults=("ConsultFlag", "sum"),
            FacilityCount=(FACILITY_COL, "nunique"),
        )
        .reset_index()
    )
    provider["ObservationRate"] = provider["OBS"] / (
        provider["OBS"] + provider["Acute"]
    ).replace(0, np.nan)
    provider["ReadmissionRate"] = provider["ReadmitNumerator"] / (
        provider["ReadmitDenominator"].replace(0, np.nan)
    )
    provider["ConsultRate"] = provider["Consults"] / provider["Total"].replace(0, np.nan)
    provider["Reliability"] = np.select(
        [
            provider["Total"] <= LOW_VOLUME_MAX,
            provider["Total"] <= MODERATE_VOLUME_MAX,
        ],
        ["Low Volume", "Moderate Volume"],
        default="Reliable Volume",
    )
    provider["VolumePercentile"] = provider["Total"].rank(pct=True, method="max")
    provider["Top5"] = np.where(provider["VolumePercentile"] >= 0.95, "Yes", "No")

    if provider.empty:
        provider_bubble_fig = empty_figure()
    else:
        bubble = provider.copy()
        bubble["BubbleSize"] = bubble["ReadmitDenominator"].clip(lower=1)
        provider_bubble_fig = px.scatter(
            bubble,
            x="Total",
            y="ObservationRate",
            color=GROUP_COL,
            size="BubbleSize",
            hover_name=PROVIDER_COL,
            hover_data={
                "Total": ":,",
                "ObservationRate": ":.1%",
                "ReadmissionRate": ":.1%",
                "ReadmitDenominator": ":,",
                "Reliability": True,
                "BubbleSize": False,
            },
            log_x=False,
        )
        weighted_obs = rate(provider["OBS"].sum(), provider["OBS"].sum() + provider["Acute"].sum())
        if not pd.isna(weighted_obs):
            provider_bubble_fig.add_hline(
                y=weighted_obs, line_dash="dot",
                annotation_text=f"Selected average {weighted_obs:.1%}",
                annotation_position="top left",
            )
        provider_bubble_fig.add_vrect(
            x0=-0.5, x1=LOW_VOLUME_MAX + 0.5,
            opacity=0.08, line_width=0,
            annotation_text="Low volume", annotation_position="top left",
        )
        provider_bubble_fig.update_layout(
            template=PLOT_TEMPLATE, xaxis_title="Selected-period stays",
            yaxis_title="Observation rate", yaxis_tickformat=".0%",
            legend_title="Hospitalist group",
            margin={"l": 55, "r": 20, "t": 35, "b": 45},
        )

    provider_bed = (
        dff.groupby([PROVIDER_COL, "BedType"])["AuthznKey"]
        .nunique().rename("Stays").reset_index()
    )
    top_providers = (
        provider_bed.groupby(PROVIDER_COL)["Stays"].sum()
        .nlargest(15).index
    )
    provider_bed = provider_bed[provider_bed[PROVIDER_COL].isin(top_providers)]
    provider_volume_fig = px.bar(
        provider_bed, x="Stays", y=PROVIDER_COL, color="BedType",
        orientation="h", category_orders={"BedType": bed_order}
    )
    provider_volume_fig.update_layout(
        template=PLOT_TEMPLATE, barmode="stack",
        yaxis={"categoryorder": "total ascending"},
        xaxis_title="Stays", yaxis_title="", legend_title="Bed type",
        margin={"l": 145, "r": 20, "t": 20, "b": 40},
    )

    provider_table = provider.rename(columns={
        PROVIDER_COL: "Provider",
        PROVIDER_NPI_COL: "NPI",
        GROUP_COL: "Hospitalist Group",
        "AvgLOS": "Avg LOS",
        "ObservationRate": "Observation Rate",
        "ReadmissionRate": "Readmission Rate",
        "ReadmitDenominator": "Readmit Denominator",
        "ConsultRate": "Consult Rate",
        "FacilityCount": "Facilities",
        "Top5": "Top 5% Volume",
    })
    provider_table["Observation Rate"] = provider_table["Observation Rate"].map(pct)
    provider_table["Readmission Rate"] = provider_table["Readmission Rate"].map(pct)
    provider_table["Consult Rate"] = provider_table["Consult Rate"].map(pct)
    provider_table["Avg LOS"] = provider_table["Avg LOS"].round(1)
    provider_table = provider_table[
        [
            "Provider", "NPI", "Hospitalist Group", "Total", "Acute", "OBS", "LTAC",
            "Observation Rate", "ReadmitNumerator", "Readmit Denominator",
            "Readmission Rate", "Avg LOS", "Consult Rate", "Facilities",
            "Reliability", "Top 5% Volume",
        ]
    ].rename(columns={"ReadmitNumerator": "Readmit Numerator"})
    provider_table_data = provider_table.sort_values("Total", ascending=False).to_dict("records")
    provider_table_columns = [{"name": c, "id": c} for c in provider_table.columns]

    # Readmission figures
    readmit_monthly = monthly_readmit.copy()
    readmit_trend_fig = px.line(
        readmit_monthly,
        x="MetricMonth",
        y="ReadmissionRate",
        markers=True,
        hover_data={
            "ReadmitNumerator": ":,",
            "ReadmitDenominator": ":,",
            "ReadmissionRate": ":.1%",
        },
    )
    overall_readmit = rate(readmit_num, readmit_den)
    if not pd.isna(overall_readmit):
        readmit_trend_fig.add_hline(
            y=overall_readmit, line_dash="dot",
            annotation_text=f"Selected average {overall_readmit:.1%}",
        )
    readmit_trend_fig.update_layout(
        template=PLOT_TEMPLATE, yaxis_tickformat=".0%",
        xaxis_title="", yaxis_title="Readmission rate",
        margin={"l": 55, "r": 20, "t": 30, "b": 40},
    )

    def readmit_dimension_figure(dimension, title_label):
        agg = (
            dff.groupby(dimension)
            .agg(
                Numerator=("ReadmitNumerator", "sum"),
                Denominator=("ReadmitDenominator", "sum"),
            )
            .reset_index()
        )
        agg = agg[agg["Denominator"] > 0]
        agg["Rate"] = agg["Numerator"] / agg["Denominator"]
        agg = agg.sort_values("Denominator", ascending=True).tail(15)
        if agg.empty:
            return empty_figure()
        fig = px.bar(
            agg, x="Rate", y=dimension, orientation="h",
            text=agg["Rate"].map(lambda x: f"{x:.1%}"),
            hover_data={"Numerator": ":,", "Denominator": ":,", "Rate": ":.1%"},
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(
            template=PLOT_TEMPLATE, xaxis_tickformat=".0%",
            xaxis_title="Readmission rate", yaxis_title="",
            margin={"l": 190 if dimension == FACILITY_COL else 130, "r": 45, "t": 20, "b": 40},
        )
        return fig

    readmit_group_fig = readmit_dimension_figure(GROUP_COL, "Group")
    readmit_facility_fig = readmit_dimension_figure(FACILITY_COL, "Facility")

    days = dff[
        dff["ReadmitDenominator"].eq(1)
        & dff["DaysToNextAcuteAdmit"].notna()
        & dff["DaysToNextAcuteAdmit"].ge(0)
        & dff["DaysToNextAcuteAdmit"].le(120)
    ]
    if days.empty:
        days_fig = empty_figure()
    else:
        days_fig = px.histogram(
            days, x="DaysToNextAcuteAdmit", nbins=40,
            color="ReadmitNumerator",
            labels={"ReadmitNumerator": "Readmission numerator"},
        )
        days_fig.add_vline(x=30, line_dash="dash", annotation_text="Day 30")
        days_fig.update_layout(
            template=PLOT_TEMPLATE, barmode="overlay",
            xaxis_title="Days from index discharge to next acute admit",
            yaxis_title="Index stays", legend_title="Numerator",
            margin={"l": 50, "r": 20, "t": 25, "b": 50},
        )

    classification = (
        dff[dff["ReadmitDenominator"].eq(1)]
        .groupby("ReadmissionClassification")["AuthznKey"]
        .nunique().sort_values(ascending=True).rename("Index Stays").reset_index()
    )
    if classification.empty:
        class_fig = empty_figure()
    else:
        class_fig = px.bar(
            classification, x="Index Stays", y="ReadmissionClassification",
            orientation="h", text="Index Stays"
        )
        class_fig.update_layout(
            template=PLOT_TEMPLATE, xaxis_title="Index stays", yaxis_title="",
            margin={"l": 210, "r": 20, "t": 20, "b": 40},
        )

    # Consult and LTAC
    consult = (
        dff[dff["ConsultFlag"].eq(1)]
        .groupby("ConsultServiceGroup")["AuthznKey"]
        .nunique().sort_values(ascending=True).rename("Stays").reset_index()
    )
    consult_services_fig = (
        empty_figure("No qualifying consults in the selected data.")
        if consult.empty else
        px.bar(consult, x="Stays", y="ConsultServiceGroup", orientation="h", text="Stays")
    )
    consult_services_fig.update_layout(
        template=PLOT_TEMPLATE, xaxis_title="Stays with first consult",
        yaxis_title="", margin={"l": 175, "r": 20, "t": 20, "b": 40},
    )

    def consult_rate_fig(dimension):
        agg = (
            dff.groupby(dimension)
            .agg(Total=("AuthznKey", "nunique"), Consults=("ConsultFlag", "sum"))
            .reset_index()
        )
        agg["Rate"] = agg["Consults"] / agg["Total"].replace(0, np.nan)
        agg = agg.sort_values("Total", ascending=True).tail(15)
        if agg.empty:
            return empty_figure()
        fig = px.bar(
            agg, x="Rate", y=dimension, orientation="h",
            text=agg["Rate"].map(lambda x: f"{x:.1%}"),
            hover_data={"Total": ":,", "Consults": ":,", "Rate": ":.1%"},
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(
            template=PLOT_TEMPLATE, xaxis_tickformat=".0%",
            xaxis_title="Consult rate", yaxis_title="",
            margin={"l": 190 if dimension == FACILITY_COL else 135, "r": 45, "t": 20, "b": 40},
        )
        return fig

    consult_facility_fig = consult_rate_fig(FACILITY_COL)
    consult_group_fig = consult_rate_fig(GROUP_COL)

    ltac = (
        dff.groupby(FACILITY_COL)
        .agg(Total=("AuthznKey", "nunique"), LTAC=("IsLTAC", "sum"))
        .reset_index()
    )
    ltac["LTACShare"] = ltac["LTAC"] / ltac["Total"].replace(0, np.nan)
    ltac = ltac.sort_values("Total", ascending=True).tail(15)
    if ltac.empty:
        ltac_fig = empty_figure()
    else:
        ltac_fig = px.bar(
            ltac, x="LTAC", y=FACILITY_COL, orientation="h",
            text="LTAC",
            hover_data={"Total": ":,", "LTACShare": ":.1%"},
        )
        ltac_fig.update_layout(
            template=PLOT_TEMPLATE, xaxis_title="LTAC stays", yaxis_title="",
            margin={"l": 205, "r": 20, "t": 20, "b": 40},
        )

    # Data quality
    duplicate_count = len(dff) - dff["AuthznKey"].nunique()
    missing_provider = dff[PROVIDER_NPI_COL].isin(["", "Unknown", "UNMAPPED"]).sum()
    unmapped_group = (~dff["IsMappedGroup"].eq(1)).sum()
    missing_facility = dff[FACILITY_COL].isin(["", "Unknown", "UNMAPPED", "UNMAPPED FACILITY"]).sum()
    overlap_issues = dff["OverlapOrDateIssueFlag"].sum()

    quality_cards = [
        html.Div([html.Span("Duplicate event rows"), html.Strong(number(duplicate_count))], className="quality-card"),
        html.Div([html.Span("Missing attending provider"), html.Strong(pct(rate(missing_provider, len(dff))))], className="quality-card"),
        html.Div([html.Span("Unmapped hospitalist group"), html.Strong(pct(rate(unmapped_group, len(dff))))], className="quality-card"),
        html.Div([html.Span("Unmapped facility"), html.Strong(pct(rate(missing_facility, len(dff))))], className="quality-card"),
        html.Div([html.Span("Overlap/date issues"), html.Strong(number(overlap_issues))], className="quality-card"),
        html.Div([html.Span("Transfer-buffer events"), html.Strong(number(dff["TransferBufferFlag"].sum()))], className="quality-card"),
    ]
    quality_df = pd.DataFrame({
        "Category": ["Mapped group", "Unmapped group", "Attending present", "Attending missing"],
        "Events": [
            int(dff["IsMappedGroup"].sum()),
            int((1 - dff["IsMappedGroup"]).sum()),
            int(len(dff) - missing_provider),
            int(missing_provider),
        ],
    })
    mapping_quality_fig = px.bar(
        quality_df, x="Category", y="Events", text="Events",
    )
    mapping_quality_fig.update_layout(
        template=PLOT_TEMPLATE, xaxis_title="", yaxis_title="Events",
        showlegend=False, margin={"l": 45, "r": 20, "t": 25, "b": 80},
    )

    return (
        number(total_stays),
        pct(rate(obs, obs + acute)),
        pct(rate(readmit_num, readmit_den)),
        "—" if pd.isna(avg_los) else f"{avg_los:.1f} days",
        pct(consult_rate),
        pct(mapped_rate),
        monthly_util,
        group_share_fig,
        facility_volume_fig,
        discharge_fig,
        heatmap_fig,
        fg_table_data,
        fg_table_columns,
        provider_bubble_fig,
        provider_volume_fig,
        provider_table_data,
        provider_table_columns,
        readmit_trend_fig,
        readmit_group_fig,
        readmit_facility_fig,
        days_fig,
        class_fig,
        consult_services_fig,
        consult_facility_fig,
        ltac_fig,
        consult_group_fig,
        quality_cards,
        mapping_quality_fig,
    )


@app.callback(
    Output("facility-spotlight", "figure"),
    Input("spotlight-facility", "value"),
    *FILTER_INPUTS,
)
def update_facility_spotlight(
    spotlight_facility,
    start_date,
    end_date,
    pods,
    facilities,
    groups,
    providers,
    bed_types,
    reliability,
):
    dff = apply_filters(
        start_date, end_date, pods, facilities, groups,
        providers, bed_types, reliability
    )
    dff = dff[dff[FACILITY_COL].eq(spotlight_facility)]
    if dff.empty:
        return empty_figure("No records for this facility under the current filters.")

    agg = (
        dff.groupby(GROUP_COL)
        .agg(
            Total=("AuthznKey", "nunique"),
            Acute=("IsAcute", "sum"),
            OBS=("IsOBS", "sum"),
            ReadmitNumerator=("ReadmitNumerator", "sum"),
            ReadmitDenominator=("ReadmitDenominator", "sum"),
        )
        .reset_index()
    )
    agg["ObservationRate"] = agg["OBS"] / (agg["OBS"] + agg["Acute"]).replace(0, np.nan)
    agg["ReadmissionRate"] = agg["ReadmitNumerator"] / agg["ReadmitDenominator"].replace(0, np.nan)
    agg = agg.sort_values("Total", ascending=False)

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Bar(
            x=agg[GROUP_COL], y=agg["Total"], name="Stays",
            hovertemplate="%{x}<br>Stays: %{y:,}<extra></extra>",
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=agg[GROUP_COL], y=agg["ObservationRate"],
            name="Observation rate", mode="lines+markers",
            hovertemplate="%{x}<br>OBS rate: %{y:.1%}<extra></extra>",
        ),
        secondary_y=True,
    )
    fig.add_trace(
        go.Scatter(
            x=agg[GROUP_COL], y=agg["ReadmissionRate"],
            name="Readmission rate", mode="lines+markers",
            hovertemplate="%{x}<br>Readmission: %{y:.1%}<extra></extra>",
        ),
        secondary_y=True,
    )
    fig.update_layout(
        template=PLOT_TEMPLATE, legend_orientation="h", legend_y=1.15,
        xaxis_title="", margin={"l": 50, "r": 55, "t": 45, "b": 95},
    )
    fig.update_yaxes(title_text="Stays", secondary_y=False)
    fig.update_yaxes(title_text="Rate", tickformat=".0%", secondary_y=True)
    return fig


@app.callback(
    Output("date-range", "start_date"),
    Output("date-range", "end_date"),
    Output("pod-filter", "value"),
    Output("facility-filter", "value"),
    Output("group-filter", "value"),
    Output("provider-filter", "value"),
    Output("bed-filter", "value"),
    Output("reliability-filter", "value"),
    Input("reset-filters", "n_clicks"),
    prevent_initial_call=True,
)
def reset_filters(_):
    return min_date.date(), max_date.date(), [], [], [], [], [], []


@app.callback(
    Output("download-filtered-summary", "data"),
    Input("download-button", "n_clicks"),
    State("date-range", "start_date"),
    State("date-range", "end_date"),
    State("pod-filter", "value"),
    State("facility-filter", "value"),
    State("group-filter", "value"),
    State("provider-filter", "value"),
    State("bed-filter", "value"),
    State("reliability-filter", "value"),
    prevent_initial_call=True,
)
def download_summary(
    n_clicks,
    start_date,
    end_date,
    pods,
    facilities,
    groups,
    providers,
    bed_types,
    reliability,
):
    if not n_clicks:
        return no_update
    dff = apply_filters(
        start_date, end_date, pods, facilities, groups,
        providers, bed_types, reliability
    )
    summary = (
        dff.groupby(
            ["MetricMonth", "ReportingPod", FACILITY_COL, GROUP_COL, PROVIDER_COL],
            dropna=False,
        )
        .agg(
            StayCount=("AuthznKey", "nunique"),
            AcuteStayCount=("IsAcute", "sum"),
            OBSStayCount=("IsOBS", "sum"),
            LTACStayCount=("IsLTAC", "sum"),
            ReadmitNumerator=("ReadmitNumerator", "sum"),
            ReadmitDenominator=("ReadmitDenominator", "sum"),
            AverageLOS=("LengthOfStay", "mean"),
            ConsultCount=("ConsultFlag", "sum"),
        )
        .reset_index()
    )
    summary["ObservationRate"] = summary["OBSStayCount"] / (
        summary["OBSStayCount"] + summary["AcuteStayCount"]
    ).replace(0, np.nan)
    summary["ReadmissionRate"] = summary["ReadmitNumerator"] / (
        summary["ReadmitDenominator"].replace(0, np.nan)
    )
    summary["ConsultRate"] = summary["ConsultCount"] / summary["StayCount"].replace(0, np.nan)
    return dcc.send_data_frame(
        summary.to_csv,
        "vop_inpatient_filtered_summary.csv",
        index=False,
    )


if __name__ == "__main__":
    app.run(
        host=os.getenv("DASH_HOST", "127.0.0.1"),
        port=int(os.getenv("DASH_PORT", "8050")),
        debug=os.getenv("DASH_DEBUG", "false").lower() == "true",
    )
