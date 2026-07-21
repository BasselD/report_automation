Use **Plotly** for this analysis. It supports interactive filtering, WebGL rendering for tens of thousands of events, hover-level QA, and standalone HTML exports.

Assumed dataframe names:

```python
df_match_detail      # analytics.notification_match_detail_2025
df_reconciliation    # analytics.notification_reconciliation_2025
df_coverage_gaps     # analytics.notification_coverage_gaps_2025
```

## Recommended visualization set

| Visualization                      | Primary table  | Purpose                                                                    |
| ---------------------------------- | -------------- | -------------------------------------------------------------------------- |
| Coverage by care type              | Reconciliation | Compare admission, discharge, complete, and unmatched rates                |
| Mutually exclusive coverage status | Reconciliation | Show complete, admission-only, discharge-only, and no matched notification |
| Notification source mix            | Match detail   | Show HIE, authorization, EMR, and other sources                            |
| Patient-class matching             | Match detail   | Quantify exact, missing-class, and OBS cross-class matches                 |
| Lag distribution                   | Match detail   | Compare timeliness across care types and notification targets              |
| Full-year lag timeline             | Match detail   | Display every event’s lag through 2025                                     |
| Source timeliness performance      | Match detail   | Compare median and 90th-percentile lag by source                           |
| Facility coverage gaps             | Coverage gaps  | Identify facilities with the weakest notification coverage                 |
| Monthly lag heatmap                | Match detail   | Identify temporal patterns and feed disruptions                            |

---

# 1. Imports and preparation

```python
from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd

import plotly.express as px
import plotly.graph_objects as go
```

```python
def prepare_adt_visual_data(
    df_match_detail: pd.DataFrame,
    df_reconciliation: pd.DataFrame,
    df_coverage_gaps: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Prepare the three analytics dataframes for Plotly visualizations.

    Returns
    -------
    match : pd.DataFrame
        Message/source-level selected matches.
    recon : pd.DataFrame
        One row per claims event.
    gaps : pd.DataFrame
        Aggregated facility/IPA coverage table.
    """

    match = df_match_detail.copy()
    recon = df_reconciliation.copy()
    gaps = df_coverage_gaps.copy()

    # ---------------------------------
    # Normalize text fields
    # ---------------------------------
    match_text_cols = [
        "care_type",
        "match_target",
        "source_category",
        "sending_source",
        "adt_patient_class",
        "patient_class_match_status",
        "message_type",
    ]

    for col in match_text_cols:
        if col in match.columns:
            match[col] = (
                match[col]
                .astype("string")
                .str.strip()
                .str.upper()
            )

    if "care_type" in recon.columns:
        recon["care_type"] = (
            recon["care_type"]
            .astype("string")
            .str.strip()
            .str.upper()
        )

    if "care_type" in gaps.columns:
        gaps["care_type"] = (
            gaps["care_type"]
            .astype("string")
            .str.strip()
            .str.upper()
        )

    # ---------------------------------
    # Parse date and timestamp fields
    # ---------------------------------
    match_datetime_cols = [
        "event_admit_date",
        "event_discharge_date",
        "adt_match_date",
        "message_timestamp",
        "insert_timestamp",
    ]

    for col in match_datetime_cols:
        if col in match.columns:
            match[col] = pd.to_datetime(
                match[col],
                errors="coerce",
            )

    recon_datetime_cols = [
        "event_admit_date",
        "event_discharge_date",
        "first_admission_message_timestamp",
        "first_admission_insert_timestamp",
        "first_discharge_message_timestamp",
        "first_discharge_insert_timestamp",
    ]

    for col in recon_datetime_cols:
        if col in recon.columns:
            recon[col] = pd.to_datetime(
                recon[col],
                errors="coerce",
            )

    # ---------------------------------
    # Event date based on match target
    # ---------------------------------
    if {
        "event_admit_date",
        "event_discharge_date",
        "match_target",
    }.issubset(match.columns):

        match["event_date"] = match["event_admit_date"].where(
            match["match_target"].eq("ADMISSION"),
            match["event_discharge_date"],
        )

    elif "adt_match_date" in match.columns:
        match["event_date"] = match["adt_match_date"]

    # ---------------------------------
    # Recalculate lag fields when needed
    # ---------------------------------
    if "notification_lag_days" not in match.columns:
        if {"insert_timestamp", "event_date"}.issubset(match.columns):
            match["notification_lag_days"] = (
                match["insert_timestamp"].dt.normalize()
                - match["event_date"].dt.normalize()
            ).dt.days

    if "pipeline_lag_minutes" not in match.columns:
        if {
            "insert_timestamp",
            "message_timestamp",
        }.issubset(match.columns):

            match["pipeline_lag_minutes"] = (
                match["insert_timestamp"]
                - match["message_timestamp"]
            ).dt.total_seconds().div(60)

    if "pipeline_lag_minutes" in match.columns:
        match["pipeline_lag_hours"] = (
            pd.to_numeric(
                match["pipeline_lag_minutes"],
                errors="coerce",
            )
            / 60
        )

    if "notification_lag_days" in match.columns:
        match["notification_lag_days"] = pd.to_numeric(
            match["notification_lag_days"],
            errors="coerce",
        )

    # ---------------------------------
    # Display-friendly source labels
    # ---------------------------------
    if "source_category" in match.columns:
        match["source_category_display"] = (
            match["source_category"]
            .fillna("UNCLASSIFIED")
            .replace(
                {
                    "HIE_ADT": "HIE / ADT",
                    "AUTHORIZATION": "Authorization",
                    "EMR": "EMR",
                    "OTHER": "Other",
                    "UNCLASSIFIED": "Unclassified",
                }
            )
        )

    if "sending_source" in match.columns:
        match["sending_source_display"] = (
            match["sending_source"]
            .fillna("Unknown Source")
            .replace({"<NA>": "Unknown Source"})
        )

    # ---------------------------------
    # Normalize patient-class status
    # ---------------------------------
    if "patient_class_match_status" in match.columns:
        match["patient_class_match_status_display"] = (
            match["patient_class_match_status"]
            .fillna("UNKNOWN")
            .replace(
                {
                    "EXACT PATIENT CLASS": "Exact patient class",
                    "MISSING PATIENT CLASS FALLBACK":
                        "Missing-class fallback",
                    "OBS CROSS-CLASS FALLBACK":
                        "OBS cross-class fallback",
                    "UNSUPPORTED CROSS-CLASS":
                        "Unsupported cross-class",
                }
            )
        )

    # ---------------------------------
    # Mutually exclusive event status
    # ---------------------------------
    if {
        "admission_notification_flag",
        "discharge_notification_flag",
    }.issubset(recon.columns):

        admission = (
            pd.to_numeric(
                recon["admission_notification_flag"],
                errors="coerce",
            )
            .fillna(0)
            .astype(int)
        )

        discharge = (
            pd.to_numeric(
                recon["discharge_notification_flag"],
                errors="coerce",
            )
            .fillna(0)
            .astype(int)
        )

        recon["notification_status"] = np.select(
            [
                admission.eq(1) & discharge.eq(1),
                admission.eq(1) & discharge.eq(0),
                admission.eq(0) & discharge.eq(1),
            ],
            [
                "Admission and discharge received",
                "Admission only",
                "Discharge only",
            ],
            default="No matched notification",
        )

    # ---------------------------------
    # Month fields
    # ---------------------------------
    if "event_date" in match.columns:
        match["event_month"] = (
            match["event_date"]
            .dt.to_period("M")
            .dt.to_timestamp()
        )

    if "event_discharge_date" in recon.columns:
        recon["event_month"] = (
            recon["event_discharge_date"]
            .dt.to_period("M")
            .dt.to_timestamp()
        )

    return match, recon, gaps
```

```python
match, recon, gaps = prepare_adt_visual_data(
    df_match_detail=df_match_detail,
    df_reconciliation=df_reconciliation,
    df_coverage_gaps=df_coverage_gaps,
)
```

---

# 2. Coverage by care type

This chart shows overlapping measures. Complete events are also included in admission-covered and discharge-covered totals.

```python
def plot_coverage_by_care_type(
    recon: pd.DataFrame,
) -> go.Figure:

    coverage = (
        recon.groupby("care_type", dropna=False)
        .agg(
            total_events=("claim_event_id", "nunique"),
            admission_covered=(
                "admission_notification_flag",
                "sum",
            ),
            discharge_covered=(
                "discharge_notification_flag",
                "sum",
            ),
            complete_covered=(
                "complete_admission_discharge_flag",
                "sum",
            ),
        )
        .reset_index()
    )

    coverage["admission_coverage_pct"] = (
        100
        * coverage["admission_covered"]
        / coverage["total_events"]
    )

    coverage["discharge_coverage_pct"] = (
        100
        * coverage["discharge_covered"]
        / coverage["total_events"]
    )

    coverage["complete_coverage_pct"] = (
        100
        * coverage["complete_covered"]
        / coverage["total_events"]
    )

    coverage_long = coverage.melt(
        id_vars=["care_type", "total_events"],
        value_vars=[
            "admission_coverage_pct",
            "discharge_coverage_pct",
            "complete_coverage_pct",
        ],
        var_name="coverage_measure",
        value_name="coverage_pct",
    )

    coverage_long["coverage_measure"] = (
        coverage_long["coverage_measure"]
        .replace(
            {
                "admission_coverage_pct": "Admission",
                "discharge_coverage_pct": "Discharge",
                "complete_coverage_pct": "Complete",
            }
        )
    )

    fig = px.bar(
        coverage_long,
        x="care_type",
        y="coverage_pct",
        color="coverage_measure",
        barmode="group",
        text_auto=".1f",
        labels={
            "care_type": "Care setting",
            "coverage_pct": "Coverage rate",
            "coverage_measure": "Measure",
        },
        title="Notification Coverage by Care Setting",
    )

    fig.update_yaxes(
        ticksuffix="%",
        range=[0, 100],
    )

    fig.update_layout(
        legend_title_text="Coverage measure",
        hovermode="x unified",
    )

    return fig
```

```python
fig_coverage = plot_coverage_by_care_type(recon)
fig_coverage.show()
```

---

# 3. Mutually exclusive notification status

This is the most appropriate executive coverage chart because each event appears in exactly one category.

```python
def plot_notification_status(
    recon: pd.DataFrame,
) -> go.Figure:

    status_order = [
        "Admission and discharge received",
        "Admission only",
        "Discharge only",
        "No matched notification",
    ]

    counts = (
        recon.groupby(
            ["care_type", "notification_status"],
            dropna=False,
        )
        .size()
        .reset_index(name="event_count")
    )

    totals = (
        counts.groupby("care_type")["event_count"]
        .transform("sum")
    )

    counts["event_pct"] = (
        100
        * counts["event_count"]
        / totals
    )

    counts["notification_status"] = pd.Categorical(
        counts["notification_status"],
        categories=status_order,
        ordered=True,
    )

    counts = counts.sort_values(
        ["care_type", "notification_status"]
    )

    fig = px.bar(
        counts,
        x="care_type",
        y="event_pct",
        color="notification_status",
        barmode="stack",
        text=counts["event_pct"].round(1).astype(str) + "%",
        custom_data=["event_count"],
        labels={
            "care_type": "Care setting",
            "event_pct": "Percent of events",
            "notification_status": "Coverage status",
        },
        title="Notification Completeness by Care Setting",
    )

    fig.update_traces(
        hovertemplate=(
            "<b>%{x}</b><br>"
            "%{fullData.name}<br>"
            "Events: %{customdata[0]:,.0f}<br>"
            "Share: %{y:.1f}%"
            "<extra></extra>"
        )
    )

    fig.update_yaxes(
        ticksuffix="%",
        range=[0, 100],
    )

    return fig
```

```python
fig_status = plot_notification_status(recon)
fig_status.show()
```

---

# 4. Notification source mix

This uses the detailed selected-match table.

```python
def plot_source_mix(
    match: pd.DataFrame,
) -> go.Figure:

    source_mix = (
        match.groupby(
            [
                "care_type",
                "match_target",
                "source_category_display",
            ],
            dropna=False,
        )
        .size()
        .reset_index(name="matched_rows")
    )

    fig = px.sunburst(
        source_mix,
        path=[
            "care_type",
            "match_target",
            "source_category_display",
        ],
        values="matched_rows",
        title="Selected Notification Matches by Care Setting, Target, and Source",
    )

    fig.update_traces(
        hovertemplate=(
            "<b>%{label}</b><br>"
            "Matched rows: %{value:,.0f}<br>"
            "Parent share: %{percentParent:.1%}"
            "<extra></extra>"
        )
    )

    return fig
```

```python
fig_sources = plot_source_mix(match)
fig_sources.show()
```

---

# 5. Patient-class match status

This is particularly important for explaining OBS results.

```python
def plot_patient_class_match_status(
    match: pd.DataFrame,
) -> go.Figure:

    qa = (
        match.groupby(
            [
                "care_type",
                "match_target",
                "patient_class_match_status_display",
            ],
            dropna=False,
        )
        .size()
        .reset_index(name="matched_rows")
    )

    qa["target_care"] = (
        qa["care_type"]
        + " — "
        + qa["match_target"].str.title()
    )

    totals = (
        qa.groupby("target_care")["matched_rows"]
        .transform("sum")
    )

    qa["match_pct"] = (
        100
        * qa["matched_rows"]
        / totals
    )

    fig = px.bar(
        qa,
        x="target_care",
        y="match_pct",
        color="patient_class_match_status_display",
        barmode="stack",
        text=qa["match_pct"].round(1).astype(str) + "%",
        custom_data=["matched_rows"],
        labels={
            "target_care": "Care setting and notification target",
            "match_pct": "Percent of selected matches",
            "patient_class_match_status_display":
                "Patient-class match",
        },
        title="Patient-Class Match Composition",
    )

    fig.update_traces(
        hovertemplate=(
            "<b>%{x}</b><br>"
            "%{fullData.name}<br>"
            "Selected matches: %{customdata[0]:,.0f}<br>"
            "Share: %{y:.1f}%"
            "<extra></extra>"
        )
    )

    fig.update_yaxes(
        ticksuffix="%",
        range=[0, 100],
    )

    return fig
```

```python
fig_patient_class = plot_patient_class_match_status(match)
fig_patient_class.show()
```

---

# 6. Lag-distribution visualization

Use a box plot for the core distribution and outliers.

```python
def plot_notification_lag_distribution(
    match: pd.DataFrame,
    minimum_lag: Optional[float] = -7,
    maximum_lag: Optional[float] = 30,
) -> go.Figure:

    data = match.dropna(
        subset=[
            "care_type",
            "match_target",
            "notification_lag_days",
        ]
    ).copy()

    if minimum_lag is not None:
        data = data[
            data["notification_lag_days"] >= minimum_lag
        ]

    if maximum_lag is not None:
        data = data[
            data["notification_lag_days"] <= maximum_lag
        ]

    fig = px.box(
        data,
        x="care_type",
        y="notification_lag_days",
        color="match_target",
        points="outliers",
        labels={
            "care_type": "Care setting",
            "notification_lag_days":
                "Notification lag in calendar days",
            "match_target": "Notification target",
        },
        title="Distribution of Notification Lag",
    )

    fig.add_hline(
        y=0,
        line_dash="dash",
        annotation_text="Same-day availability",
    )

    return fig
```

```python
fig_lag_box = plot_notification_lag_distribution(match)
fig_lag_box.show()
```

For pipeline processing lag:

```python
def plot_pipeline_lag_distribution(
    match: pd.DataFrame,
    minimum_hours: Optional[float] = -2,
    maximum_hours: Optional[float] = 48,
) -> go.Figure:

    data = match.dropna(
        subset=[
            "care_type",
            "match_target",
            "pipeline_lag_hours",
        ]
    ).copy()

    if minimum_hours is not None:
        data = data[
            data["pipeline_lag_hours"] >= minimum_hours
        ]

    if maximum_hours is not None:
        data = data[
            data["pipeline_lag_hours"] <= maximum_hours
        ]

    fig = px.violin(
        data,
        x="care_type",
        y="pipeline_lag_hours",
        color="match_target",
        box=True,
        points=False,
        labels={
            "care_type": "Care setting",
            "pipeline_lag_hours":
                "Message-to-insert pipeline lag in hours",
            "match_target": "Notification target",
        },
        title="ADT Message-to-Insert Processing Lag",
    )

    fig.add_hline(
        y=0,
        line_dash="dash",
        annotation_text="Zero pipeline delay",
    )

    return fig
```

---

# 7. Full-year lag timeline

This visualization places the clinical event date on the x-axis and lag on the y-axis.

It includes:

* Every available lag as a WebGL scatter point
* Vertical connectors from zero to a sample of event lags
* A daily median line
* A daily 90th-percentile line
* Negative lag identification

## Notification lag timeline

```python
def plot_full_year_lag_timeline(
    match: pd.DataFrame,
    metric: str = "notification",
    care_type: Optional[str] = None,
    match_target: Optional[str] = None,
    year: int = 2025,
    max_connector_points: int = 2500,
) -> go.Figure:
    """
    Full-year interactive lag timeline.

    Parameters
    ----------
    metric:
        "notification" for claims-event-date to insert-date lag in days.
        "pipeline" for message-timestamp to insert-timestamp lag in hours.
    care_type:
        Optional: "ED", "IP", or "OBS".
    match_target:
        Optional: "ADMISSION" or "DISCHARGE".
    """

    if metric == "notification":
        metric_column = "notification_lag_days"
        y_axis_title = "Notification lag in calendar days"
        chart_title = "Full-Year Notification Lag Timeline"
    elif metric == "pipeline":
        metric_column = "pipeline_lag_hours"
        y_axis_title = "Message-to-insert pipeline lag in hours"
        chart_title = "Full-Year ADT Pipeline Lag Timeline"
    else:
        raise ValueError(
            "metric must be 'notification' or 'pipeline'"
        )

    required = [
        "event_date",
        metric_column,
        "care_type",
        "match_target",
    ]

    missing = [
        col for col in required
        if col not in match.columns
    ]

    if missing:
        raise KeyError(
            f"Missing required columns: {missing}"
        )

    data = match.dropna(
        subset=[
            "event_date",
            metric_column,
            "care_type",
            "match_target",
        ]
    ).copy()

    data = data[
        data["event_date"].dt.year.eq(year)
    ]

    if care_type:
        data = data[
            data["care_type"].eq(care_type.upper())
        ]

    if match_target:
        data = data[
            data["match_target"].eq(
                match_target.upper()
            )
        ]

    if data.empty:
        raise ValueError(
            "No rows remain after applying the filters."
        )

    hover_columns = [
        col
        for col in [
            "care_type",
            "match_target",
            "source_category_display",
            "sending_source_display",
            "message_type",
            "patient_class_match_status_display",
            metric_column,
        ]
        if col in data.columns
    ]

    fig = px.scatter(
        data,
        x="event_date",
        y=metric_column,
        color="care_type",
        symbol="match_target",
        render_mode="webgl",
        opacity=0.35,
        hover_data=hover_columns,
        labels={
            "event_date": "Claims event date",
            metric_column: y_axis_title,
            "care_type": "Care setting",
            "match_target": "Notification target",
        },
        title=chart_title,
    )

    # ---------------------------------
    # Vertical lag connectors
    # ---------------------------------
    connector_data = data

    if len(connector_data) > max_connector_points:
        connector_data = connector_data.sample(
            max_connector_points,
            random_state=42,
        )

    connector_x = []
    connector_y = []

    for row in connector_data[
        ["event_date", metric_column]
    ].itertuples(index=False):

        event_date = row[0]
        lag_value = row[1]

        connector_x.extend(
            [event_date, event_date, None]
        )
        connector_y.extend(
            [0, lag_value, None]
        )

    fig.add_trace(
        go.Scattergl(
            x=connector_x,
            y=connector_y,
            mode="lines",
            line={
                "width": 0.5,
                "color": "rgba(100,100,100,0.15)",
            },
            hoverinfo="skip",
            showlegend=False,
            name="Lag connector",
        )
    )

    # ---------------------------------
    # Daily median and p90
    # ---------------------------------
    daily = (
        data.groupby("event_date")[metric_column]
        .agg(
            daily_median="median",
            daily_p90=lambda values:
                values.quantile(0.90),
            daily_count="count",
        )
        .reset_index()
        .sort_values("event_date")
    )

    fig.add_trace(
        go.Scatter(
            x=daily["event_date"],
            y=daily["daily_median"],
            mode="lines",
            name="Daily median",
            line={"width": 2.5},
            customdata=daily[["daily_count"]],
            hovertemplate=(
                "<b>%{x|%b %d, %Y}</b><br>"
                "Daily median: %{y:.2f}<br>"
                "Events: %{customdata[0]:,.0f}"
                "<extra></extra>"
            ),
        )
    )

    fig.add_trace(
        go.Scatter(
            x=daily["event_date"],
            y=daily["daily_p90"],
            mode="lines",
            name="Daily 90th percentile",
            line={
                "width": 2,
                "dash": "dot",
            },
            hovertemplate=(
                "<b>%{x|%b %d, %Y}</b><br>"
                "Daily p90: %{y:.2f}"
                "<extra></extra>"
            ),
        )
    )

    fig.add_hline(
        y=0,
        line_dash="dash",
        annotation_text="Zero lag",
    )

    fig.update_xaxes(
        range=[
            f"{year}-01-01",
            f"{year}-12-31",
        ],
        rangeslider_visible=True,
    )

    fig.update_layout(
        hovermode="closest",
        legend_title_text="Care setting / target",
    )

    return fig
```

### All notification lags

```python
fig_notification_timeline = plot_full_year_lag_timeline(
    match,
    metric="notification",
    year=2025,
)

fig_notification_timeline.show()
```

### Message-to-insert pipeline lag

```python
fig_pipeline_timeline = plot_full_year_lag_timeline(
    match,
    metric="pipeline",
    year=2025,
)

fig_pipeline_timeline.show()
```

### Observation discharge lag only

```python
fig_obs_discharge_timeline = plot_full_year_lag_timeline(
    match,
    metric="notification",
    care_type="OBS",
    match_target="DISCHARGE",
    year=2025,
)

fig_obs_discharge_timeline.show()
```

## How to interpret the timeline

For notification lag:

```text
Claims event date → insert timestamp
```

* `0` = notification available on the same calendar day
* Positive = notification available after the event date
* Negative = message appears to have been available before the claims event date and requires QA

For pipeline lag:

```text
message_timestamp → insert_timestamp
```

* Near zero = fast ingestion
* Larger values = source or internal processing delay
* Negative = timestamp sequencing issue

Because the claims event only contains a date, **notification lag should remain in calendar days**. Pipeline lag can be presented in minutes or hours because both fields are timestamps.

---

# 8. Monthly lag heatmap

```python
def plot_monthly_lag_heatmap(
    match: pd.DataFrame,
    match_target: str = "DISCHARGE",
    statistic: str = "median",
) -> go.Figure:

    data = match[
        match["match_target"].eq(
            match_target.upper()
        )
    ].dropna(
        subset=[
            "event_month",
            "care_type",
            "notification_lag_days",
        ]
    ).copy()

    if statistic == "median":
        monthly = (
            data.groupby(
                ["care_type", "event_month"]
            )["notification_lag_days"]
            .median()
            .reset_index(name="lag_value")
        )
        title_stat = "Median"

    elif statistic == "p90":
        monthly = (
            data.groupby(
                ["care_type", "event_month"]
            )["notification_lag_days"]
            .quantile(0.90)
            .reset_index(name="lag_value")
        )
        title_stat = "90th-Percentile"

    else:
        raise ValueError(
            "statistic must be 'median' or 'p90'"
        )

    pivot = monthly.pivot(
        index="care_type",
        columns="event_month",
        values="lag_value",
    )

    pivot.columns = [
        col.strftime("%b %Y")
        for col in pivot.columns
    ]

    fig = px.imshow(
        pivot,
        text_auto=".1f",
        aspect="auto",
        labels={
            "x": "Event month",
            "y": "Care setting",
            "color": "Lag days",
        },
        title=(
            f"{title_stat} {match_target.title()} "
            "Notification Lag by Month"
        ),
    )

    return fig
```

```python
fig_monthly_heatmap = plot_monthly_lag_heatmap(
    match,
    match_target="DISCHARGE",
    statistic="median",
)

fig_monthly_heatmap.show()
```

This is useful for identifying:

* Feed interruptions
* HIE onboarding changes
* Seasonal source delays
* Specific months with unusually slow reporting

---

# 9. Source timeliness performance

This bubble chart compares median and 90th-percentile lag by sending source.

```python
def plot_source_timeliness(
    match: pd.DataFrame,
    match_target: str = "DISCHARGE",
    minimum_matches: int = 100,
    top_sources: int = 30,
) -> go.Figure:

    data = match[
        match["match_target"].eq(
            match_target.upper()
        )
    ].dropna(
        subset=[
            "sending_source_display",
            "notification_lag_days",
        ]
    ).copy()

    summary = (
        data.groupby(
            [
                "sending_source_display",
                "source_category_display",
            ]
        )["notification_lag_days"]
        .agg(
            matched_rows="count",
            median_lag="median",
            p90_lag=lambda values:
                values.quantile(0.90),
        )
        .reset_index()
    )

    summary = summary[
        summary["matched_rows"] >= minimum_matches
    ]

    summary = (
        summary.sort_values(
            "matched_rows",
            ascending=False,
        )
        .head(top_sources)
    )

    fig = px.scatter(
        summary,
        x="median_lag",
        y="p90_lag",
        size="matched_rows",
        color="source_category_display",
        hover_name="sending_source_display",
        hover_data={
            "matched_rows": ":,.0f",
            "median_lag": ":.2f",
            "p90_lag": ":.2f",
            "source_category_display": True,
        },
        labels={
            "median_lag": "Median notification lag in days",
            "p90_lag": "90th-percentile lag in days",
            "matched_rows": "Selected matches",
            "source_category_display": "Source category",
        },
        title=(
            f"{match_target.title()} Timeliness "
            "by Sending Source"
        ),
    )

    fig.add_vline(
        x=0,
        line_dash="dash",
    )

    fig.add_hline(
        y=0,
        line_dash="dash",
    )

    return fig
```

---

# 10. Facility coverage gaps

Use the pre-aggregated coverage-gap table for facility and IPA comparisons.

```python
def plot_facility_coverage_gaps(
    gaps: pd.DataFrame,
    care_type: str = "IP",
    minimum_events: int = 100,
    top_n: int = 20,
) -> go.Figure:

    no_match_pct_column = next(
        (
            col
            for col in [
                "percent_no_matched_notification",
                "percent_no_notification",
            ]
            if col in gaps.columns
        ),
        None,
    )

    if no_match_pct_column is None:
        raise KeyError(
            "No unmatched-percentage column was found."
        )

    data = gaps[
        gaps["care_type"].eq(
            care_type.upper()
        )
    ].copy()

    data["total_events"] = pd.to_numeric(
        data["total_events"],
        errors="coerce",
    )

    data[no_match_pct_column] = pd.to_numeric(
        data[no_match_pct_column],
        errors="coerce",
    )

    data = data[
        data["total_events"] >= minimum_events
    ]

    # Aggregate in case facility has multiple IPA rows
    facility = (
        data.groupby(
            "servicefacility",
            dropna=False,
        )
        .apply(
            lambda part: pd.Series(
                {
                    "total_events":
                        part["total_events"].sum(),

                    "no_match_pct":
                        np.average(
                            part[no_match_pct_column],
                            weights=part["total_events"],
                        ),
                }
            ),
            include_groups=False,
        )
        .reset_index()
    )

    facility["servicefacility"] = (
        facility["servicefacility"]
        .fillna("Unknown Facility")
    )

    facility = (
        facility.sort_values(
            "no_match_pct",
            ascending=False,
        )
        .head(top_n)
        .sort_values(
            "no_match_pct",
            ascending=True,
        )
    )

    fig = px.bar(
        facility,
        x="no_match_pct",
        y="servicefacility",
        orientation="h",
        text_auto=".1f",
        custom_data=["total_events"],
        labels={
            "no_match_pct":
                "No matched notification rate",
            "servicefacility":
                "Claims service facility",
        },
        title=(
            f"Facilities with Highest No-Matched-"
            f"Notification Rate — {care_type.upper()}"
        ),
    )

    fig.update_traces(
        hovertemplate=(
            "<b>%{y}</b><br>"
            "No matched notification: %{x:.1f}%<br>"
            "Eligible events: %{customdata[0]:,.0f}"
            "<extra></extra>"
        )
    )

    fig.update_xaxes(
        ticksuffix="%",
    )

    return fig
```

```python
fig_ip_gaps = plot_facility_coverage_gaps(
    gaps,
    care_type="IP",
    minimum_events=100,
    top_n=20,
)

fig_ip_gaps.show()
```

---

# 11. Build and export all figures

```python
def build_adt_figures(
    match: pd.DataFrame,
    recon: pd.DataFrame,
    gaps: pd.DataFrame,
) -> Dict[str, go.Figure]:

    figures = {
        "01_coverage_by_care_type":
            plot_coverage_by_care_type(recon),

        "02_notification_status":
            plot_notification_status(recon),

        "03_source_mix":
            plot_source_mix(match),

        "04_patient_class_match":
            plot_patient_class_match_status(match),

        "05_notification_lag_distribution":
            plot_notification_lag_distribution(match),

        "06_pipeline_lag_distribution":
            plot_pipeline_lag_distribution(match),

        "07_full_year_notification_lag":
            plot_full_year_lag_timeline(
                match,
                metric="notification",
                year=2025,
            ),

        "08_full_year_pipeline_lag":
            plot_full_year_lag_timeline(
                match,
                metric="pipeline",
                year=2025,
            ),

        "09_monthly_discharge_lag_heatmap":
            plot_monthly_lag_heatmap(
                match,
                match_target="DISCHARGE",
                statistic="median",
            ),

        "10_discharge_source_timeliness":
            plot_source_timeliness(
                match,
                match_target="DISCHARGE",
                minimum_matches=100,
            ),

        "11_ip_facility_gaps":
            plot_facility_coverage_gaps(
                gaps,
                care_type="IP",
                minimum_events=100,
            ),

        "12_ed_facility_gaps":
            plot_facility_coverage_gaps(
                gaps,
                care_type="ED",
                minimum_events=100,
            ),

        "13_obs_facility_gaps":
            plot_facility_coverage_gaps(
                gaps,
                care_type="OBS",
                minimum_events=50,
            ),
    }

    return figures
```

```python
figures = build_adt_figures(
    match=match,
    recon=recon,
    gaps=gaps,
)
```

Export each visualization as a standalone interactive HTML file:

```python
def export_adt_figures(
    figures: Dict[str, go.Figure],
    output_directory: str = "adt_visualizations",
) -> None:

    output_path = Path(output_directory)
    output_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    for figure_name, figure in figures.items():
        figure.write_html(
            output_path / f"{figure_name}.html",
            include_plotlyjs=True,
            full_html=True,
        )
```

```python
export_adt_figures(
    figures,
    output_directory="adt_visualizations",
)
```

## Recommended dashboard order

1. Coverage KPI cards
2. Mutually exclusive notification status
3. Admission/discharge coverage bars
4. Patient-class matching composition
5. Full-year notification-lag timeline
6. Pipeline-lag timeline
7. Monthly lag heatmap
8. Source timeliness bubble chart
9. Facility coverage-gap ranking
10. Source and message-level QA table

Avoid including `person_id` or `memberno` in shared visualizations or hover labels. Use `claim_event_id` only in restricted QA views.
