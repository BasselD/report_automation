Use Python to create an **event-level reconciliation table** between claims and ADT, without moving either source. The cleanest approach is:

1. Standardize member IDs and dates.
2. Deduplicate ADT messages to one hospitalization event.
3. Match exact member + admit date first.
4. Apply a controlled fuzzy match, such as ±1 day.
5. Compare discharge dates and produce QA summaries.

## 1. Prepare both dataframes

```python
import pandas as pd
import numpy as np

claims = df_claims.copy()
adt = df_adt.copy()

# Standardize column names
claims = claims.rename(
    columns={
        "MemberID": "member_id",
        "Admit": "claim_admit",
        "Discharge": "claim_discharge",
    }
)

adt = adt.rename(
    columns={
        "memberno": "member_id",
        "admit_date": "adt_admit",
        "discharge_date": "adt_discharge",
    }
)

# Standardize member IDs
claims["member_id"] = (
    claims["member_id"]
    .astype(str)
    .str.strip()
    .str.upper()
)

adt["member_id"] = (
    adt["member_id"]
    .astype(str)
    .str.strip()
    .str.upper()
)

# Convert dates/timestamps
claims["claim_admit"] = pd.to_datetime(
    claims["claim_admit"],
    errors="coerce"
).dt.normalize()

claims["claim_discharge"] = pd.to_datetime(
    claims["claim_discharge"],
    errors="coerce"
).dt.normalize()

adt["adt_admit"] = pd.to_datetime(
    adt["adt_admit"],
    errors="coerce"
).dt.normalize()

adt["adt_discharge"] = pd.to_datetime(
    adt["adt_discharge"],
    errors="coerce"
).dt.normalize()

adt["insert_timestamp"] = pd.to_datetime(
    adt["insert_timestamp"],
    errors="coerce"
)

adt["message_timestamp"] = pd.to_datetime(
    adt["message_timestamp"],
    errors="coerce"
)
```

---

## 2. Remove invalid rows

```python
claims = claims.dropna(
    subset=["member_id", "claim_admit"]
).copy()

adt = adt.dropna(
    subset=["member_id", "adt_admit"]
).copy()
```

---

## 3. Deduplicate claims and ADT events

ADT commonly contains multiple messages for the same hospitalization. Keep the latest available record for each member and admission date.

```python
# One claims row per member/admission event
claims = (
    claims
    .sort_values(
        ["member_id", "claim_admit", "claim_discharge"]
    )
    .drop_duplicates(
        subset=["member_id", "claim_admit"],
        keep="last"
    )
    .reset_index(drop=True)
)

# Keep latest ADT message for each member/admission
adt = (
    adt
    .sort_values(
        [
            "member_id",
            "adt_admit",
            "insert_timestamp",
            "message_timestamp",
        ]
    )
    .drop_duplicates(
        subset=["member_id", "adt_admit"],
        keep="last"
    )
    .reset_index(drop=True)
)
```

If a member can legitimately have two admissions on the same date, include facility or encounter ID in the deduplication key when available.

---

# 4. Exact admission-date matching

```python
exact_matches = claims.merge(
    adt,
    left_on=["member_id", "claim_admit"],
    right_on=["member_id", "adt_admit"],
    how="inner",
    suffixes=("_claim", "_adt")
)

exact_matches["match_method"] = "Exact Admit Date"
exact_matches["admit_date_difference"] = 0
```

---

# 5. Identify unmatched records

```python
exact_claim_keys = exact_matches[
    ["member_id", "claim_admit"]
].drop_duplicates()

unmatched_claims = claims.merge(
    exact_claim_keys.assign(exact_match=1),
    on=["member_id", "claim_admit"],
    how="left"
)

unmatched_claims = (
    unmatched_claims[
        unmatched_claims["exact_match"].isna()
    ]
    .drop(columns="exact_match")
)
```

```python
exact_adt_keys = exact_matches[
    ["member_id", "adt_admit"]
].drop_duplicates()

unmatched_adt = adt.merge(
    exact_adt_keys.assign(exact_match=1),
    on=["member_id", "adt_admit"],
    how="left"
)

unmatched_adt = (
    unmatched_adt[
        unmatched_adt["exact_match"].isna()
    ]
    .drop(columns="exact_match")
)
```

---

# 6. Fuzzy match within ±1 day

Join unmatched events by member, then calculate the admission-date difference.

```python
fuzzy_candidates = unmatched_claims.merge(
    unmatched_adt,
    on="member_id",
    how="inner",
    suffixes=("_claim", "_adt")
)

fuzzy_candidates["admit_date_difference"] = (
    fuzzy_candidates["adt_admit"]
    - fuzzy_candidates["claim_admit"]
).dt.days

fuzzy_candidates = fuzzy_candidates[
    fuzzy_candidates["admit_date_difference"].abs() <= 1
].copy()
```

Select the closest ADT event for each claims event.

```python
fuzzy_matches = (
    fuzzy_candidates
    .sort_values(
        [
            "member_id",
            "claim_admit",
            "admit_date_difference",
            "message_timestamp",
        ],
        key=lambda col: (
            col.abs()
            if col.name == "admit_date_difference"
            else col
        )
    )
    .drop_duplicates(
        subset=["member_id", "claim_admit"],
        keep="first"
    )
    .copy()
)

fuzzy_matches["match_method"] = "Fuzzy Admit Date ±1 Day"
```

To prevent the same ADT event from matching multiple claims:

```python
fuzzy_matches = (
    fuzzy_matches
    .sort_values(
        [
            "member_id",
            "adt_admit",
            "admit_date_difference",
        ],
        key=lambda col: (
            col.abs()
            if col.name == "admit_date_difference"
            else col
        )
    )
    .drop_duplicates(
        subset=["member_id", "adt_admit"],
        keep="first"
    )
)
```

---




## Bottom line

Do **not** use only:

```python
insert_timestamp - message_timestamp
```

That measures **ADT ingestion or pipeline lag**, not how quickly the ADT notified you after the clinical event.

You need three separate lag measures:

| Measure | Calculation | What it tells you |
|---|---|---|
| **Source notification lag** | `message_timestamp - clinical_event_timestamp` | How quickly the hospital generated the ADT |
| **Pipeline ingestion lag** | `insert_timestamp - message_timestamp` | How quickly your platform received and loaded it |
| **End-to-end lag** | `insert_timestamp - clinical_event_timestamp` | Total delay from the clinical event to data availability |

Your current process normalizes admission and discharge fields to dates and keeps the latest ADT row per hospitalization. That is appropriate for reconciliation, but it loses information needed to evaluate the first notification and the sequence of ADT messages. fileciteturn0file0

---

# How `message_type` should be used

Common HL7 interpretations are:

| Message type | Typical meaning | Recommended lag calculation |
|---|---|---|
| `A01` | Admission | Admission timestamp to first A01 message |
| `A03` | Discharge | Discharge timestamp to first A03 message |
| `A08` | Update to patient or encounter | Analyze separately as an update, not the original notification |
| `A02` | Transfer | Analyze transfer notification if relevant |
| `A04` | Registration | Sometimes used as an early encounter notification |
| `A06` | Outpatient to inpatient | May function as an admission message in some feeds |

Validate these against your local interface specifications because healthcare feeds occasionally use standard message types in nonstandard ways. A classic interoperability plot twist.

## Specifically for `A03`

Yes. `A03` is generally the best message for measuring discharge notification timeliness:

```text
A03 message_timestamp - actual discharge timestamp
```

Then measure the platform portion:

```text
A03 insert_timestamp - A03 message_timestamp
```

And the complete delay:

```text
A03 insert_timestamp - actual discharge timestamp
```

## Specifically for `A08`

Do **not** normally use `A08` as the primary admission or discharge notification.

Use it to measure:

- How often hospitalization information gets corrected
- How long updates continue after admission or discharge
- Whether an A08 arrives after the A03
- Whether discharge dates or other critical fields change
- A08 ingestion lag between message and insert timestamps

Without an explicit **update-effective timestamp**, you cannot reliably calculate clinical update lag for A08. You can only calculate its pipeline lag.

---

# Important change to your process

Maintain two versions of the ADT data:

```python
adt_messages = all original ADT rows
adt_events = one consolidated row per hospitalization
```

Use:

- `adt_events` for claims-to-ADT matching
- `adt_messages` for timeliness, sequencing, duplicates, and message-type analysis

Do not overwrite the full ADT message history when you deduplicate.

---

# 1. Prepare the complete ADT message history

Start again from `df_adt`, not the already-deduplicated `adt` dataframe.

```python
import pandas as pd
import numpy as np

adt_messages = df_adt.rename(
    columns={
        "memberno": "member_id",
    }
).copy()

adt_messages["member_id"] = (
    adt_messages["member_id"]
    .astype(str)
    .str.strip()
    .str.upper()
)

adt_messages["message_type"] = (
    adt_messages["message_type"]
    .astype("string")
    .str.strip()
    .str.upper()
)

# Preserve the complete timestamp.
# Do not normalize these fields yet.
adt_messages["adt_admit_ts"] = pd.to_datetime(
    adt_messages["admit_date"],
    errors="coerce"
)

adt_messages["adt_discharge_ts"] = pd.to_datetime(
    adt_messages["discharge_date"],
    errors="coerce"
)

adt_messages["message_timestamp"] = pd.to_datetime(
    adt_messages["message_timestamp"],
    errors="coerce"
)

adt_messages["insert_timestamp"] = pd.to_datetime(
    adt_messages["insert_timestamp"],
    errors="coerce"
)

# Date keys used for matching and grouping
adt_messages["adt_admit"] = (
    adt_messages["adt_admit_ts"]
    .dt.normalize()
)

adt_messages["adt_discharge"] = (
    adt_messages["adt_discharge_ts"]
    .dt.normalize()
)
```

## Time-zone warning

Confirm that all four timestamps use the same timezone.

A three-hour lag can be a real delay, or it can be Eastern Time arguing with UTC. Check this before interpreting negative or unusually large values.

---

# 2. Calculate message-level pipeline lag

This applies to every message type, including A03 and A08.

```python
adt_messages["pipeline_lag_hours"] = (
    adt_messages["insert_timestamp"]
    - adt_messages["message_timestamp"]
).dt.total_seconds() / 3600

adt_messages["pipeline_lag_minutes"] = (
    adt_messages["insert_timestamp"]
    - adt_messages["message_timestamp"]
).dt.total_seconds() / 60
```

Add QA flags:

```python
adt_messages["negative_pipeline_lag_flag"] = (
    adt_messages["pipeline_lag_hours"] < 0
)

adt_messages["missing_pipeline_timestamp_flag"] = (
    adt_messages["message_timestamp"].isna()
    | adt_messages["insert_timestamp"].isna()
)
```

Negative values usually indicate:

- Time-zone inconsistency
- Clock synchronization issues
- Incorrect field definitions
- Batch reprocessing
- A message timestamp that is not actually the send timestamp

Do not silently convert negative values to zero. They are QA findings.

---

# 3. Calculate clinical-event lag by message type

```python
ADMISSION_TYPES = ["A01", "A04", "A06"]
DISCHARGE_TYPES = ["A03"]

adt_messages["clinical_event_timestamp"] = pd.NaT
adt_messages["clinical_event_type"] = pd.NA

admission_mask = adt_messages["message_type"].isin(
    ADMISSION_TYPES
)

discharge_mask = adt_messages["message_type"].isin(
    DISCHARGE_TYPES
)

adt_messages.loc[
    admission_mask,
    "clinical_event_timestamp"
] = adt_messages.loc[
    admission_mask,
    "adt_admit_ts"
]

adt_messages.loc[
    admission_mask,
    "clinical_event_type"
] = "Admission"

adt_messages.loc[
    discharge_mask,
    "clinical_event_timestamp"
] = adt_messages.loc[
    discharge_mask,
    "adt_discharge_ts"
]

adt_messages.loc[
    discharge_mask,
    "clinical_event_type"
] = "Discharge"
```

Calculate source and end-to-end lag:

```python
adt_messages["source_notification_lag_hours"] = (
    adt_messages["message_timestamp"]
    - adt_messages["clinical_event_timestamp"]
).dt.total_seconds() / 3600

adt_messages["end_to_end_lag_hours"] = (
    adt_messages["insert_timestamp"]
    - adt_messages["clinical_event_timestamp"]
).dt.total_seconds() / 3600
```

QA flags:

```python
adt_messages["negative_source_lag_flag"] = (
    adt_messages["source_notification_lag_hours"] < 0
)

adt_messages["negative_end_to_end_lag_flag"] = (
    adt_messages["end_to_end_lag_hours"] < 0
)
```

---

# If admit and discharge are dates only

If `admit_date` and `discharge_date` do not contain the actual event time, do **not** interpret the hourly lag literally. Midnight will be inserted automatically, which can overstate the delay by many hours.

Use calendar-day lag instead:

```python
adt_messages["clinical_notification_lag_days"] = (
    adt_messages["message_timestamp"].dt.normalize()
    - adt_messages["clinical_event_timestamp"].dt.normalize()
).dt.days

adt_messages["clinical_to_insert_lag_days"] = (
    adt_messages["insert_timestamp"].dt.normalize()
    - adt_messages["clinical_event_timestamp"].dt.normalize()
).dt.days
```

Then report categories such as:

- Same calendar day
- One day later
- Two or more days later

For precise hourly timeliness, you need actual:

- Admission datetime
- Discharge datetime
- Message datetime
- Insert datetime

---

# 4. Select the first admission and discharge notification

For timeliness, use the **first relevant message**, not the latest message.

```python
event_keys = [
    "member_id",
    "adt_admit",
]
```

If available, add encounter ID and facility:

```python
# Preferred example:
# event_keys = [
#     "member_id",
#     "facility_id",
#     "encounter_id",
# ]
```

## First admission notification

```python
first_admission_message = (
    adt_messages[
        adt_messages["message_type"].isin(ADMISSION_TYPES)
    ]
    .sort_values(
        event_keys
        + ["message_timestamp", "insert_timestamp"]
    )
    .drop_duplicates(
        subset=event_keys,
        keep="first"
    )
    [
        event_keys
        + [
            "message_type",
            "adt_admit_ts",
            "message_timestamp",
            "insert_timestamp",
            "pipeline_lag_hours",
            "source_notification_lag_hours",
            "end_to_end_lag_hours",
        ]
    ]
    .rename(
        columns={
            "message_type": "admit_message_type",
            "adt_admit_ts": "admit_event_timestamp",
            "message_timestamp": "admit_message_timestamp",
            "insert_timestamp": "admit_insert_timestamp",
            "pipeline_lag_hours": "admit_pipeline_lag_hours",
            "source_notification_lag_hours":
                "admit_source_lag_hours",
            "end_to_end_lag_hours":
                "admit_end_to_end_lag_hours",
        }
    )
)
```

## First A03 discharge notification

```python
first_discharge_message = (
    adt_messages[
        adt_messages["message_type"].eq("A03")
    ]
    .sort_values(
        event_keys
        + ["message_timestamp", "insert_timestamp"]
    )
    .drop_duplicates(
        subset=event_keys,
        keep="first"
    )
    [
        event_keys
        + [
            "adt_discharge_ts",
            "message_timestamp",
            "insert_timestamp",
            "pipeline_lag_hours",
            "source_notification_lag_hours",
            "end_to_end_lag_hours",
        ]
    ]
    .rename(
        columns={
            "adt_discharge_ts": "discharge_event_timestamp",
            "message_timestamp": "a03_message_timestamp",
            "insert_timestamp": "a03_insert_timestamp",
            "pipeline_lag_hours": "a03_pipeline_lag_hours",
            "source_notification_lag_hours":
                "discharge_source_lag_hours",
            "end_to_end_lag_hours":
                "discharge_end_to_end_lag_hours",
        }
    )
)
```

---

# 5. Summarize A08 update activity

```python
a08_summary = (
    adt_messages[
        adt_messages["message_type"].eq("A08")
    ]
    .groupby(
        event_keys,
        dropna=False
    )
    .agg(
        a08_message_count=(
            "message_type",
            "size"
        ),
        first_a08_message_timestamp=(
            "message_timestamp",
            "min"
        ),
        last_a08_message_timestamp=(
            "message_timestamp",
            "max"
        ),
        first_a08_insert_timestamp=(
            "insert_timestamp",
            "min"
        ),
        last_a08_insert_timestamp=(
            "insert_timestamp",
            "max"
        ),
        median_a08_pipeline_lag_hours=(
            "pipeline_lag_hours",
            "median"
        ),
        max_a08_pipeline_lag_hours=(
            "pipeline_lag_hours",
            "max"
        ),
    )
    .reset_index()
)
```

A high A08 count may indicate:

- Normal encounter updates
- Repeated demographic updates
- Discharge-date corrections
- Duplicate messages
- A noisy source interface

It is not automatically bad, but it should be analyzed by facility and sender.

---

# 6. Build one timing record per ADT event

```python
event_message_summary = (
    adt_messages
    .groupby(
        event_keys,
        dropna=False
    )
    .agg(
        adt_message_count=(
            "message_type",
            "size"
        ),
        first_adt_message_timestamp=(
            "message_timestamp",
            "min"
        ),
        last_adt_message_timestamp=(
            "message_timestamp",
            "max"
        ),
        first_adt_insert_timestamp=(
            "insert_timestamp",
            "min"
        ),
        last_adt_insert_timestamp=(
            "insert_timestamp",
            "max"
        ),
        message_types_seen=(
            "message_type",
            lambda values: ",".join(
                sorted(
                    set(
                        values
                        .dropna()
                        .astype(str)
                    )
                )
            )
        ),
        negative_pipeline_lag_count=(
            "negative_pipeline_lag_flag",
            "sum"
        ),
    )
    .reset_index()
)
```

Combine the timing components:

```python
event_timing = (
    event_message_summary
    .merge(
        first_admission_message,
        on=event_keys,
        how="left",
        validate="1:1"
    )
    .merge(
        first_discharge_message,
        on=event_keys,
        how="left",
        validate="1:1"
    )
    .merge(
        a08_summary,
        on=event_keys,
        how="left",
        validate="1:1"
    )
)
```

---

# 7. Attach the fields to your existing final summary

Assuming your concatenated dataframe is called:

```python
final_summary
```

Standardize the merge fields:

```python
final_summary = final_summary.copy()

final_summary["member_id"] = (
    final_summary["member_id"]
    .astype(str)
    .str.strip()
    .str.upper()
)

final_summary["adt_admit"] = pd.to_datetime(
    final_summary["adt_admit"],
    errors="coerce"
).dt.normalize()
```

Merge the timing information:

```python
final_summary = final_summary.merge(
    event_timing,
    on=[
        "member_id",
        "adt_admit",
    ],
    how="left",
    validate="m:1"
)
```

Because your unmatched claims do not have an ADT event, their lag fields will correctly remain null.

Add an interpretable status:

```python
final_summary["adt_availability_status"] = np.select(
    [
        final_summary["adt_admit"].isna(),

        final_summary["a03_message_timestamp"].notna(),

        final_summary["admit_message_timestamp"].notna(),

        final_summary["first_adt_message_timestamp"].notna(),
    ],
    [
        "Claims Event Missing ADT",
        "ADT Matched with A03",
        "ADT Matched with Admission Message",
        "ADT Matched but No Admission/A03 Message",
    ],
    default="Unknown"
)
```

Add message-presence flags:

```python
final_summary["admission_message_flag"] = (
    final_summary["admit_message_timestamp"]
    .notna()
    .astype(int)
)

final_summary["a03_discharge_message_flag"] = (
    final_summary["a03_message_timestamp"]
    .notna()
    .astype(int)
)

final_summary["a08_update_flag"] = (
    final_summary["a08_message_count"]
    .fillna(0)
    .gt(0)
    .astype(int)
)
```

---

# Recommended final fields

```python
lag_columns = [
    "member_id",
    "claim_admit",
    "claim_discharge",
    "adt_admit",
    "adt_discharge",

    "adt_availability_status",
    "adt_message_count",
    "message_types_seen",

    "admit_message_type",
    "admit_event_timestamp",
    "admit_message_timestamp",
    "admit_insert_timestamp",
    "admit_source_lag_hours",
    "admit_pipeline_lag_hours",
    "admit_end_to_end_lag_hours",

    "discharge_event_timestamp",
    "a03_message_timestamp",
    "a03_insert_timestamp",
    "discharge_source_lag_hours",
    "a03_pipeline_lag_hours",
    "discharge_end_to_end_lag_hours",

    "a08_message_count",
    "first_a08_message_timestamp",
    "last_a08_message_timestamp",
    "median_a08_pipeline_lag_hours",

    "match_method",
    "overall_match_status",
]
```

Use only columns that exist:

```python
lag_columns = [
    column
    for column in lag_columns
    if column in final_summary.columns
]

final_output = (
    final_summary[lag_columns]
    .sort_values(
        ["member_id", "claim_admit"]
    )
)
```

---

# Yes, reconsider the analysis slightly

Your analysis should now have two distinct dimensions.

## 1. Reliability and completeness

Use claims as the reference:

```text
Claims hospitalization with matched ADT
÷
Mature claims hospitalizations
```

Report:

- Exact match rate
- Fuzzy match rate
- Unmatched claims rate
- Unmatched ADT rate
- A01 or admission-message presence
- A03 presence
- Coverage by facility, month, provider group, and source system

Use a mature claims period. Recent claims may not have completed runout, so they should not be used to declare an ADT event missing.

## 2. Timeliness

Among matched ADT events, report:

- Median and 90th or 95th percentile admission end-to-end lag
- Median and 90th or 95th percentile discharge end-to-end lag
- Percentage available within 1, 4, 8, and 24 hours
- Pipeline lag separately from hospital notification lag
- Negative lag rate
- Missing timestamp rate
- Results by message type and facility

The most defensible primary metrics are:

```text
Admission availability:
First valid admission insert timestamp
minus admission event timestamp

Discharge availability:
First A03 insert timestamp
minus discharge event timestamp
```

The distinction matters. An ADT feed can be highly complete but slow, or very fast but missing 20% of inpatient stays. One score would hide that problem. Keep **coverage** and **timeliness** as separate KPIs.
)

This is much more defensible than calculating one generic lag from the latest retained ADT row. A08 updates can otherwise make the data look late even when the original admission or discharge notification arrived on time.
