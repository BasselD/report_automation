Yes. A **hybrid approach** is best:

* Use standard Excel PivotTables directly from `notification_reconciliation_2025` because it has one row per claim event.
* Use `notification_coverage_gaps_2025` for pivots that remain at its existing facility/IPA grain.
* Pre-aggregate `notification_match_detail_2025` in Python before Excel because one event can have multiple messages and sources. A normal PivotTable cannot perform a distinct event count without the Data Model.

# 1. Standard PivotTables from the reconciliation table

Convert the reconciliation extract to an Excel table named:

```text
tbl_reconciliation
```

## Add two helper columns

### Event month

```excel
=DATE(YEAR([@event_discharge_date]),MONTH([@event_discharge_date]),1)
```

### Source contribution segment

```excel
=IFS(
    AND([@authorization_flag]=1,
        SUM([@hie_adt_flag],[@emr_flag],[@other_flag],[@unclassified_flag])>0),
    "Authorization + Clinical/Other",

    SUM([@hie_adt_flag],[@emr_flag],[@other_flag],[@unclassified_flag])>0,
    "Clinical/Other Only",

    [@authorization_flag]=1,
    "Authorization Only",

    TRUE,
    "No Matched Notification"
)
```

---

## Pivot A — Completeness by care setting

**Source:** `tbl_reconciliation`

| Pivot area      | Field                                        |
| --------------- | -------------------------------------------- |
| Rows            | `care_type`                                  |
| Columns         | `notification_completeness_status`           |
| Values          | Count of `claim_event_id`                    |
| Filters/Slicers | `reporting_group`, `ipa_name`, `event_month` |

Use:

```text
Show Values As → % of Row Total
```

This is the strongest leadership pivot because the categories are mutually exclusive.

---

## Pivot B — Admission, discharge, and complete coverage

Because the fields are binary `0/1`, their **average equals the coverage rate**.

| Pivot area      | Field                                          |
| --------------- | ---------------------------------------------- |
| Rows            | `care_type`                                    |
| Values          | Average of `admission_notification_flag`       |
| Values          | Average of `discharge_notification_flag`       |
| Values          | Average of `complete_admission_discharge_flag` |
| Values          | Average of `no_matched_notification_flag`      |
| Filters/Slicers | `reporting_group`, `ipa_name`, `event_month`   |

Format the values as percentages.

This avoids calculated fields entirely.

---

## Pivot C — Authorization contribution

| Pivot area | Field                                        |
| ---------- | -------------------------------------------- |
| Rows       | `care_type`                                  |
| Columns    | `source_contribution_segment`                |
| Values     | Count of `claim_event_id`                    |
| Filters    | `reporting_group`, `ipa_name`, `event_month` |

Use `% of Row Total`.

This shows whether IP and OBS coverage comes from:

* Authorization only
* Clinical/other sources only
* Both
* No matched notification

---

## Pivot D — IPA performance

| Pivot area | Field                                          |
| ---------- | ---------------------------------------------- |
| Rows       | `reporting_group`, then `ipa_name`             |
| Columns    | `care_type`                                    |
| Values     | Count of `claim_event_id`                      |
| Values     | Average of `complete_admission_discharge_flag` |
| Values     | Average of `no_matched_notification_flag`      |
| Filters    | `event_month`, `servicefacility`               |

Use conditional formatting on the two percentage measures.

---

## Pivot E — Monthly coverage trend

| Pivot area | Field                                            |
| ---------- | ------------------------------------------------ |
| Rows       | `event_month`                                    |
| Columns    | `care_type`                                      |
| Values     | Average of `complete_admission_discharge_flag`   |
| Filters    | `reporting_group`, `ipa_name`, `servicefacility` |

Create a second pivot using:

```text
Average of no_matched_notification_flag
```

Use line charts for both.

---

## Pivot F — Timeliness categories

Create separate admission and discharge pivots.

| Pivot area | Field                                            |
| ---------- | ------------------------------------------------ |
| Rows       | `care_type`                                      |
| Columns    | `admission_timeliness_category`                  |
| Values     | Count of `claim_event_id`                        |
| Filters    | `reporting_group`, `ipa_name`, `servicefacility` |

Use `% of Row Total`.

Repeat using:

```text
discharge_timeliness_category
```

---

# 2. Coverage-gaps table

Standard pivots can sum the numerator and denominator fields, but do not average the percentage columns.

## Facility-volume pivot

**Source:** `tbl_coverage_gaps`

| Pivot area | Field                                        |
| ---------- | -------------------------------------------- |
| Rows       | `servicefacility`                            |
| Columns    | `care_type`                                  |
| Values     | Sum of `total_events`                        |
| Values     | Sum of `events_with_complete_notifications`  |
| Values     | Sum of `events_with_no_matched_notification` |
| Filters    | `reporting_group`, `ipa_name`                |

Do not use:

```text
Average of complete_notification_coverage_pct
Average of percent_no_matched_notification
```

Those produce unweighted averages.

For a leadership-ready facility ranking, Python aggregation is safer.

---

# 3. Python helper tables for complex analysis

The detailed table should be aggregated before loading into Excel.

```python
import numpy as np
import pandas as pd

match = df_match_detail.copy()
recon = df_reconciliation.copy()
gaps = df_coverage_gaps.copy()

# Dates
recon["event_discharge_date"] = pd.to_datetime(
    recon["event_discharge_date"],
    errors="coerce",
)

recon["event_month"] = (
    recon["event_discharge_date"]
    .dt.to_period("M")
    .dt.to_timestamp()
)

for column in [
    "event_admit_date",
    "event_discharge_date",
    "message_timestamp",
    "insert_timestamp",
]:
    if column in match.columns:
        match[column] = pd.to_datetime(
            match[column],
            errors="coerce",
        )
```

## A. Care-setting executive summary

```python
care_summary = (
    recon.groupby("care_type", dropna=False)
    .agg(
        total_events=("claim_event_id", "nunique"),
        admission_covered=("admission_notification_flag", "sum"),
        discharge_covered=("discharge_notification_flag", "sum"),
        complete_covered=("complete_admission_discharge_flag", "sum"),
        no_matched=("no_matched_notification_flag", "sum"),
        authorization_events=("authorization_flag", "sum"),
        hie_adt_events=("hie_adt_flag", "sum"),
    )
    .reset_index()
)

for numerator, output in [
    ("admission_covered", "admission_coverage_pct"),
    ("discharge_covered", "discharge_coverage_pct"),
    ("complete_covered", "complete_coverage_pct"),
    ("no_matched", "no_matched_pct"),
    ("authorization_events", "authorization_coverage_pct"),
]:
    care_summary[output] = (
        100
        * care_summary[numerator]
        / care_summary["total_events"]
    )
```

---

## B. Patient-class crosswalk

This avoids duplicate event counting.

```python
patient_class_crosswalk = (
    match.groupby(
        [
            "care_type",
            "match_target",
            "adt_patient_class",
            "patient_class_match_status",
        ],
        dropna=False,
    )
    .agg(
        matched_events=("claim_event_id", "nunique"),
        matched_rows=("claim_event_id", "size"),
    )
    .reset_index()
)
```

Use this sheet for standard PivotTables:

| Rows        | Columns             | Values                  |
| ----------- | ------------------- | ----------------------- |
| `care_type` | `adt_patient_class` | Sum of `matched_events` |

Filter to `OBS` for the methodology backup slide.

---

## C. MedHOK match-confidence summary

```python
medhok_summary = (
    match.loc[
        match["source_category"].eq("AUTHORIZATION")
    ]
    .groupby(
        [
            "care_type",
            "match_target",
            "authorization_match_status",
        ],
        dropna=False,
    )
    .agg(
        selected_events=("claim_event_id", "nunique"),
        selected_rows=("claim_event_id", "size"),
        authorization_messages=("adt_message_key", "nunique"),
    )
    .reset_index()
)
```

Recommended pivot:

| Rows                        | Columns                      | Values                   |
| --------------------------- | ---------------------------- | ------------------------ |
| `care_type`, `match_target` | `authorization_match_status` | Sum of `selected_events` |

---

## D. Source performance summary

```python
def percentile_90(values):
    return values.quantile(0.90)

source_summary = (
    match.groupby(
        [
            "source_category",
            "sending_source",
            "care_type",
            "match_target",
        ],
        dropna=False,
    )
    .agg(
        matched_events=("claim_event_id", "nunique"),
        matched_rows=("claim_event_id", "size"),
        message_count=("adt_message_key", "nunique"),
        median_notification_lag=("notification_lag_days", "median"),
        p90_notification_lag=("notification_lag_days", percentile_90),
        median_pipeline_lag=("pipeline_lag_minutes", "median"),
        p90_pipeline_lag=("pipeline_lag_minutes", percentile_90),
        facility_match_rate=("facility_match_flag", "mean"),
    )
    .reset_index()
)

source_summary["facility_match_rate"] *= 100
```

This is better than using PivotTable averages because it provides median and 90th percentile lag.

---

## E. Facility coverage summary

Build this from the event-level table so rates are always correctly weighted.

```python
facility_summary = (
    recon.groupby(
        [
            "reporting_group",
            "ipa_name",
            "servicefacility",
            "care_type",
        ],
        dropna=False,
    )
    .agg(
        total_events=("claim_event_id", "nunique"),
        admission_covered=("admission_notification_flag", "sum"),
        discharge_covered=("discharge_notification_flag", "sum"),
        complete_covered=("complete_admission_discharge_flag", "sum"),
        no_matched=("no_matched_notification_flag", "sum"),
        authorization_events=("authorization_flag", "sum"),
        hie_adt_events=("hie_adt_flag", "sum"),
    )
    .reset_index()
)

for numerator, output in [
    ("admission_covered", "admission_coverage_pct"),
    ("discharge_covered", "discharge_coverage_pct"),
    ("complete_covered", "complete_coverage_pct"),
    ("no_matched", "no_matched_pct"),
    ("authorization_events", "authorization_coverage_pct"),
]:
    facility_summary[output] = (
        100
        * facility_summary[numerator]
        / facility_summary["total_events"]
    )
```

Filter to a minimum event count before ranking:

```python
facility_leadership = facility_summary.loc[
    facility_summary["total_events"] >= 100
].copy()
```

---

## F. Monthly trend summary

```python
monthly_summary = (
    recon.groupby(
        ["event_month", "care_type"],
        dropna=False,
    )
    .agg(
        total_events=("claim_event_id", "nunique"),
        admission_covered=("admission_notification_flag", "sum"),
        discharge_covered=("discharge_notification_flag", "sum"),
        complete_covered=("complete_admission_discharge_flag", "sum"),
        no_matched=("no_matched_notification_flag", "sum"),
        authorization_events=("authorization_flag", "sum"),
    )
    .reset_index()
)

for numerator, output in [
    ("admission_covered", "admission_coverage_pct"),
    ("discharge_covered", "discharge_coverage_pct"),
    ("complete_covered", "complete_coverage_pct"),
    ("no_matched", "no_matched_pct"),
    ("authorization_events", "authorization_coverage_pct"),
]:
    monthly_summary[output] = (
        100
        * monthly_summary[numerator]
        / monthly_summary["total_events"]
    )
```

---

## G. Timeliness summary

```python
timeliness_summary = (
    match.groupby(
        [
            "care_type",
            "match_target",
            "source_category",
            "sending_source",
        ],
        dropna=False,
    )
    .agg(
        matched_events=("claim_event_id", "nunique"),
        median_notification_lag=("notification_lag_days", "median"),
        p90_notification_lag=(
            "notification_lag_days",
            lambda x: x.quantile(0.90),
        ),
        median_pipeline_lag=("pipeline_lag_minutes", "median"),
        p90_pipeline_lag=(
            "pipeline_lag_minutes",
            lambda x: x.quantile(0.90),
        ),
        negative_notification_lags=(
            "notification_lag_days",
            lambda x: (x < 0).sum(),
        ),
    )
    .reset_index()
)
```

---

# 4. Export the helper tables to Excel

```python
output_file = "ADT_Leadership_Pivot_Sources.xlsx"

with pd.ExcelWriter(
    output_file,
    engine="xlsxwriter",
    datetime_format="yyyy-mm-dd",
) as writer:

    # Original event-level table for standard pivots
    recon.to_excel(
        writer,
        sheet_name="Event Detail",
        index=False,
    )

    # Python-generated helper sheets
    care_summary.to_excel(
        writer,
        sheet_name="Care Summary",
        index=False,
    )

    monthly_summary.to_excel(
        writer,
        sheet_name="Monthly Summary",
        index=False,
    )

    facility_summary.to_excel(
        writer,
        sheet_name="Facility Summary",
        index=False,
    )

    patient_class_crosswalk.to_excel(
        writer,
        sheet_name="Patient Class QA",
        index=False,
    )

    medhok_summary.to_excel(
        writer,
        sheet_name="MedHOK QA",
        index=False,
    )

    source_summary.to_excel(
        writer,
        sheet_name="Source Summary",
        index=False,
    )

    timeliness_summary.to_excel(
        writer,
        sheet_name="Timeliness Summary",
        index=False,
    )
```

# Recommended division of work

## Build directly in standard Excel PivotTables

Use `Event Detail` for:

1. Notification completeness
2. Admission/discharge/complete coverage
3. Authorization contribution
4. IPA performance
5. Monthly event-level trends
6. Timeliness-category distribution

## Use Python-generated sheets

Use the helper sheets for:

1. Patient-class crosswalk
2. MedHOK match confidence
3. Distinct-event source coverage
4. Median and 90th-percentile lag
5. Weighted facility rankings
6. High-volume coverage-gap analysis

This gives you normal Excel slicers and drill-down for the core presentation while avoiding duplicated event counts in the complex source-level views.

---
Use the **event-level reconciliation table** as the source. It has one row per claim event, so standard Excel PivotTables can safely use `Count of claim_event_id` without duplicate-event inflation.

The extract below reduces the table to the fields needed for leadership reporting, raw-data review, pivoting, timeliness analysis, OBS methodology, and MedHOK QA.

```sql
/*==============================================================================
  LEADERSHIP / EXCEL EVENT-LEVEL EXTRACT

  Grain:
      One row per eligible claims event

  Primary uses:
      - Leadership raw-data tab
      - Standard Excel PivotTables
      - Python visualizations
      - Coverage, source contribution, timeliness, OBS, and MedHOK QA
==============================================================================*/

DROP TABLE IF EXISTS analytics.notification_leadership_extract_2025;

CREATE TABLE analytics.notification_leadership_extract_2025 AS

WITH event_base AS
(
    SELECT
        /*--------------------------------------------------------------
          1. EVENT IDENTITY AND ATTRIBUTION
        --------------------------------------------------------------*/
        claim_event_id,

        /* Retain only in a secured internal workbook */
        memberno,

        care_type,
        care_subtype,

        event_admit_date,
        event_discharge_date,

        CAST(
            DATE_TRUNC(
                'month',
                event_discharge_date
            ) AS DATE
        ) AS event_month,

        EXTRACT(
            quarter
            FROM event_discharge_date
        )::INTEGER AS event_quarter,

        length_of_stay_days,

        servicefacility,
        ipa_name,
        reporting_group,

        /*--------------------------------------------------------------
          2. COVERAGE FLAGS
        --------------------------------------------------------------*/
        COALESCE(admission_notification_flag, 0)
            AS admission_notification_flag,

        COALESCE(discharge_notification_flag, 0)
            AS discharge_notification_flag,

        COALESCE(complete_admission_discharge_flag, 0)
            AS complete_admission_discharge_flag,

        COALESCE(no_matched_notification_flag, 0)
            AS no_matched_notification_flag,

        notification_completeness_status,

        admission_coverage_category,
        discharge_coverage_category,

        /*--------------------------------------------------------------
          3. OVERALL SOURCE FLAGS
        --------------------------------------------------------------*/
        COALESCE(authorization_flag, 0)
            AS authorization_flag,

        COALESCE(hie_adt_flag, 0)
            AS hie_adt_flag,

        COALESCE(emr_flag, 0)
            AS emr_flag,

        COALESCE(other_flag, 0)
            AS other_flag,

        COALESCE(unclassified_flag, 0)
            AS unclassified_flag,

        CASE
            WHEN
                   COALESCE(hie_adt_flag, 0) = 1
                OR COALESCE(emr_flag, 0) = 1
                OR COALESCE(other_flag, 0) = 1
                OR COALESCE(unclassified_flag, 0) = 1
                THEN 1
            ELSE 0
        END AS non_authorization_flag,

        overall_source_count,

        /*--------------------------------------------------------------
          4. ADMISSION SOURCE FLAGS
        --------------------------------------------------------------*/
        COALESCE(admission_authorization_flag, 0)
            AS admission_authorization_flag,

        COALESCE(admission_hie_flag, 0)
            AS admission_hie_flag,

        COALESCE(admission_emr_flag, 0)
            AS admission_emr_flag,

        COALESCE(admission_other_flag, 0)
            AS admission_other_flag,

        COALESCE(admission_unclassified_flag, 0)
            AS admission_unclassified_flag,

        CASE
            WHEN
                   COALESCE(admission_hie_flag, 0) = 1
                OR COALESCE(admission_emr_flag, 0) = 1
                OR COALESCE(admission_other_flag, 0) = 1
                OR COALESCE(admission_unclassified_flag, 0) = 1
                THEN 1
            ELSE 0
        END AS admission_non_authorization_flag,

        /*--------------------------------------------------------------
          5. DISCHARGE SOURCE FLAGS
        --------------------------------------------------------------*/
        COALESCE(discharge_authorization_flag, 0)
            AS discharge_authorization_flag,

        COALESCE(discharge_hie_flag, 0)
            AS discharge_hie_flag,

        COALESCE(discharge_emr_flag, 0)
            AS discharge_emr_flag,

        COALESCE(discharge_other_flag, 0)
            AS discharge_other_flag,

        COALESCE(discharge_unclassified_flag, 0)
            AS discharge_unclassified_flag,

        CASE
            WHEN
                   COALESCE(discharge_hie_flag, 0) = 1
                OR COALESCE(discharge_emr_flag, 0) = 1
                OR COALESCE(discharge_other_flag, 0) = 1
                OR COALESCE(discharge_unclassified_flag, 0) = 1
                THEN 1
            ELSE 0
        END AS discharge_non_authorization_flag,

        /*--------------------------------------------------------------
          6. EARLIEST ADMISSION NOTIFICATION
        --------------------------------------------------------------*/
        first_admission_source_category,
        first_admission_sending_source,
        first_admission_message_type,
        first_admission_insert_timestamp,

        admission_notification_lag_days,
        admission_timeliness_category,

        admission_patient_class_match_status,
        admission_facility_match_flag,

        first_admission_authorization_match_status,

        /*--------------------------------------------------------------
          7. EARLIEST DISCHARGE NOTIFICATION
        --------------------------------------------------------------*/
        first_discharge_source_category,
        first_discharge_sending_source,
        first_discharge_message_type,
        first_discharge_insert_timestamp,

        discharge_notification_lag_days,
        discharge_timeliness_category,

        discharge_patient_class_match_status,
        discharge_facility_match_flag,

        first_discharge_authorization_match_status

    FROM analytics.notification_reconciliation_2025
)

SELECT
    /*------------------------------------------------------------------
      EVENT IDENTITY
    ------------------------------------------------------------------*/
    claim_event_id,
    memberno,

    care_type,
    care_subtype,

    event_admit_date,
    event_discharge_date,
    event_month,
    event_quarter,

    length_of_stay_days,

    servicefacility,
    ipa_name,
    reporting_group,

    /*------------------------------------------------------------------
      COMPLETENESS AND COVERAGE
    ------------------------------------------------------------------*/
    admission_notification_flag,
    discharge_notification_flag,
    complete_admission_discharge_flag,
    no_matched_notification_flag,

    notification_completeness_status,

    CASE
        WHEN notification_completeness_status =
             'Admission and Discharge Received'
            THEN 1

        WHEN notification_completeness_status =
             'Admission Only'
            THEN 2

        WHEN notification_completeness_status =
             'Discharge Only'
            THEN 3

        WHEN notification_completeness_status =
             'No Matched Notification'
            THEN 4

        ELSE 9
    END AS notification_status_sort,

    admission_coverage_category,
    discharge_coverage_category,

    /*------------------------------------------------------------------
      OVERALL SOURCE CONTRIBUTION
    ------------------------------------------------------------------*/
    authorization_flag,
    non_authorization_flag,

    hie_adt_flag,
    emr_flag,
    other_flag,
    unclassified_flag,

    overall_source_count,

    CASE
        WHEN authorization_flag = 1
         AND non_authorization_flag = 1
            THEN 'Authorization + Non-Authorization'

        WHEN authorization_flag = 0
         AND non_authorization_flag = 1
            THEN 'Non-Authorization Only'

        WHEN authorization_flag = 1
         AND non_authorization_flag = 0
            THEN 'Authorization Only'

        ELSE 'No Matched Notification'
    END AS source_contribution_segment,

    /*------------------------------------------------------------------
      ADMISSION SOURCE CONTRIBUTION
    ------------------------------------------------------------------*/
    admission_authorization_flag,
    admission_non_authorization_flag,

    CASE
        WHEN admission_authorization_flag = 1
         AND admission_non_authorization_flag = 1
            THEN 'Authorization + Non-Authorization'

        WHEN admission_authorization_flag = 0
         AND admission_non_authorization_flag = 1
            THEN 'Non-Authorization Only'

        WHEN admission_authorization_flag = 1
         AND admission_non_authorization_flag = 0
            THEN 'Authorization Only'

        ELSE 'No Matched Admission'
    END AS admission_source_contribution_segment,

    /*------------------------------------------------------------------
      DISCHARGE SOURCE CONTRIBUTION
    ------------------------------------------------------------------*/
    discharge_authorization_flag,
    discharge_non_authorization_flag,

    CASE
        WHEN discharge_authorization_flag = 1
         AND discharge_non_authorization_flag = 1
            THEN 'Authorization + Non-Authorization'

        WHEN discharge_authorization_flag = 0
         AND discharge_non_authorization_flag = 1
            THEN 'Non-Authorization Only'

        WHEN discharge_authorization_flag = 1
         AND discharge_non_authorization_flag = 0
            THEN 'Authorization Only'

        ELSE 'No Matched Discharge'
    END AS discharge_source_contribution_segment,

    /*------------------------------------------------------------------
      EARLIEST ADMISSION NOTIFICATION
    ------------------------------------------------------------------*/
    first_admission_source_category,
    first_admission_sending_source,
    first_admission_message_type,
    first_admission_insert_timestamp,

    admission_notification_lag_days,
    admission_timeliness_category,

    admission_patient_class_match_status,
    admission_facility_match_flag,

    first_admission_authorization_match_status,

    /*------------------------------------------------------------------
      EARLIEST DISCHARGE NOTIFICATION
    ------------------------------------------------------------------*/
    first_discharge_source_category,
    first_discharge_sending_source,
    first_discharge_message_type,
    first_discharge_insert_timestamp,

    discharge_notification_lag_days,
    discharge_timeliness_category,

    discharge_patient_class_match_status,
    discharge_facility_match_flag,

    first_discharge_authorization_match_status

FROM event_base;
```

## Resulting extract

This produces approximately **55 columns**, rather than more than 80, while retaining the fields required for:

* Notification completeness
* Admission versus discharge coverage
* Authorization contribution
* Non-authorization contribution
* IPA and facility comparisons
* Monthly trends
* Timeliness
* Observation methodology
* MedHOK match-confidence backup
* Event-level raw-data review

## Columns to show first in the Excel raw-data tab

Freeze these columns on the left:

```text
claim_event_id
memberno
care_type
event_admit_date
event_discharge_date
event_month
servicefacility
ipa_name
reporting_group
notification_completeness_status
source_contribution_segment
admission_notification_lag_days
discharge_notification_lag_days
```

The remaining columns can stay to the right for drill-down.

## Safest fields for standard PivotTables

Because this table remains one row per event, use:

| Pivot metric               | Excel aggregation                              |
| -------------------------- | ---------------------------------------------- |
| Total events               | Count of `claim_event_id`                      |
| Admission coverage rate    | Average of `admission_notification_flag`       |
| Discharge coverage rate    | Average of `discharge_notification_flag`       |
| Complete coverage rate     | Average of `complete_admission_discharge_flag` |
| No matched rate            | Average of `no_matched_notification_flag`      |
| Authorization coverage     | Average of `authorization_flag`                |
| Non-authorization coverage | Average of `non_authorization_flag`            |

Format averages of the binary flags as percentages.

## Best pivots from this single extract

### Completeness

```text
Rows: care_type
Columns: notification_completeness_status
Values: Count of claim_event_id
Show values as: % of Row Total
```

### Source contribution

```text
Rows: care_type
Columns: source_contribution_segment
Values: Count of claim_event_id
Show values as: % of Row Total
```

### Monthly coverage trend

```text
Rows: event_month
Columns: care_type
Values:
    Average of complete_admission_discharge_flag
    Average of no_matched_notification_flag
```

### Facility and IPA gaps

```text
Rows:
    reporting_group
    ipa_name
    servicefacility

Columns:
    care_type

Values:
    Count of claim_event_id
    Average of complete_admission_discharge_flag
    Average of no_matched_notification_flag
```

### Timeliness

```text
Rows: care_type
Columns: discharge_timeliness_category
Values: Count of claim_event_id
Show values as: % of Row Total
```

For a leadership-shared workbook, consider removing `memberno` from the distributed version while retaining it in a secured internal QA version.

