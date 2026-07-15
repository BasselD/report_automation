## What this version produces

The main `Discharge_QA` sheet contains **one row per claims inpatient event**. It retains unmatched claims and includes:

* Exact or fuzzy admission match
* Missing ADT encounters
* Missing ADT discharge messages
* A03 presence
* A08 counts
* Claims versus ADT discharge-date difference
* Source, pipeline, and end-to-end lag
* A readable quality status and lag bucket

The original exact-match, fuzzy-match, and unmatched-claims approach is preserved. The important change is that the code no longer keeps only the latest ADT row. It retains all messages, consolidates them into encounters, and uses the **first A03 message** to calculate discharge lag. 

```python
import pandas as pd
import numpy as np


def build_adt_discharge_qa(
    df_claims,
    df_adt,
    fuzzy_days=1
):
    """
    Build a claims-centered ADT discharge reconciliation table.

    Expected claims columns:
        MemberID
        Admit
        Discharge

    Expected ADT columns:
        memberno
        admit_date
        discharge_date
        message_timestamp
        insert_timestamp
        message_type

    Returns:
        qa_output: One row per claims inpatient event
        summary: QA metrics
        adt_unmatched: ADT encounters without a claims match
    """

    # =========================================================
    # 1. Copy and standardize source columns
    # =========================================================

    claims = df_claims.rename(
        columns={
            "MemberID": "member_id",
            "Admit": "claim_admit_ts",
            "Discharge": "claim_discharge_ts",
        }
    ).copy()

    adt = df_adt.rename(
        columns={
            "memberno": "member_id",
            "admit_date": "adt_admit_ts",
            "discharge_date": "adt_discharge_ts",
        }
    ).copy()

    for frame in (claims, adt):
        frame["member_id"] = (
            frame["member_id"]
            .astype("string")
            .str.strip()
            .str.upper()
        )

    # Claims dates
    claims["claim_admit_ts"] = pd.to_datetime(
        claims["claim_admit_ts"],
        errors="coerce"
    )

    claims["claim_discharge_ts"] = pd.to_datetime(
        claims["claim_discharge_ts"],
        errors="coerce"
    )

    claims["claim_admit_date"] = (
        claims["claim_admit_ts"]
        .dt.normalize()
    )

    claims["claim_discharge_date"] = (
        claims["claim_discharge_ts"]
        .dt.normalize()
    )

    # ADT dates and timestamps
    for column in [
        "adt_admit_ts",
        "adt_discharge_ts",
        "message_timestamp",
        "insert_timestamp",
    ]:
        adt[column] = pd.to_datetime(
            adt[column],
            errors="coerce"
        )

    adt["message_type"] = (
        adt["message_type"]
        .astype("string")
        .str.strip()
        .str.upper()
    )

    adt["adt_admit_date"] = (
        adt["adt_admit_ts"]
        .dt.normalize()
    )

    # =========================================================
    # 2. Create one claims hospitalization per admission
    # =========================================================

    claims = (
        claims
        .dropna(
            subset=[
                "member_id",
                "claim_admit_date",
            ]
        )
        .sort_values(
            [
                "member_id",
                "claim_admit_date",
                "claim_discharge_ts",
            ]
        )
        .drop_duplicates(
            subset=[
                "member_id",
                "claim_admit_date",
            ],
            keep="last"
        )
        .reset_index(drop=True)
    )

    claims["claim_event_id"] = np.arange(
        1,
        len(claims) + 1
    )

    # ADT messages require member and admission date
    adt = adt.dropna(
        subset=[
            "member_id",
            "adt_admit_date",
        ]
    ).copy()

    event_key = [
        "member_id",
        "adt_admit_date",
    ]

    # Used to sequence messages
    adt["message_sort_ts"] = (
        adt["message_timestamp"]
        .fillna(adt["insert_timestamp"])
    )

    # =========================================================
    # 3. Create one ADT encounter from all its messages
    # =========================================================

    event_summary = (
        adt
        .groupby(
            event_key,
            dropna=False
        )
        .agg(
            adt_admit_ts=(
                "adt_admit_ts",
                "min"
            ),

            adt_message_count=(
                "message_type",
                "size"
            ),

            a03_message_count=(
                "message_type",
                lambda values: values.eq("A03").sum()
            ),

            a08_message_count=(
                "message_type",
                lambda values: values.eq("A08").sum()
            ),

            first_adt_message_timestamp=(
                "message_timestamp",
                "min"
            ),

            first_adt_insert_timestamp=(
                "insert_timestamp",
                "min"
            ),

            last_adt_message_timestamp=(
                "message_timestamp",
                "max"
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
        )
        .reset_index()
    )

    # =========================================================
    # 4. Select the first A03 discharge notification
    # =========================================================

    first_a03 = (
        adt[
            adt["message_type"].eq("A03")
        ]
        .sort_values(
            event_key
            + [
                "message_sort_ts",
                "insert_timestamp",
            ],
            na_position="last"
        )
        .drop_duplicates(
            subset=event_key,
            keep="first"
        )
        [
            event_key
            + [
                "adt_discharge_ts",
                "message_timestamp",
                "insert_timestamp",
            ]
        ]
        .rename(
            columns={
                "adt_discharge_ts":
                    "a03_discharge_ts",

                "message_timestamp":
                    "a03_message_timestamp",

                "insert_timestamp":
                    "a03_insert_timestamp",
            }
        )
    )

    # =========================================================
    # 5. Find latest discharge value from any ADT message
    # =========================================================
    #
    # This helps identify cases where an A08 or another message
    # carries a discharge date, but no A03 was received.
    # =========================================================

    latest_discharge = (
        adt[
            adt["adt_discharge_ts"].notna()
        ]
        .sort_values(
            event_key
            + [
                "message_sort_ts",
                "insert_timestamp",
            ],
            na_position="last"
        )
        .drop_duplicates(
            subset=event_key,
            keep="last"
        )
        [
            event_key
            + [
                "adt_discharge_ts",
                "message_type",
            ]
        ]
        .rename(
            columns={
                "adt_discharge_ts":
                    "latest_discharge_ts_any",

                "message_type":
                    "latest_discharge_message_type",
            }
        )
    )

    # Combine the message-level results
    adt_events = (
        event_summary
        .merge(
            first_a03,
            on=event_key,
            how="left",
            validate="1:1"
        )
        .merge(
            latest_discharge,
            on=event_key,
            how="left",
            validate="1:1"
        )
    )

    # Prefer the A03 discharge date.
    # Fall back to a discharge date carried by another message.
    adt_events["adt_discharge_ts"] = (
        adt_events["a03_discharge_ts"]
        .combine_first(
            adt_events["latest_discharge_ts_any"]
        )
    )

    adt_events["adt_discharge_date"] = (
        adt_events["adt_discharge_ts"]
        .dt.normalize()
    )

    adt_events["discharge_source"] = np.select(
        [
            adt_events["a03_discharge_ts"].notna(),

            adt_events[
                "latest_discharge_ts_any"
            ].notna(),
        ],
        [
            "A03",
            "Non-A03 ADT message",
        ],
        default="Missing"
    )

    # =========================================================
    # 6. Calculate discharge lag
    # =========================================================

    # Hospital/source delay:
    # A03 message time minus actual ADT discharge time
    adt_events["a03_source_lag_hours"] = (
        adt_events["a03_message_timestamp"]
        - adt_events["a03_discharge_ts"]
    ).dt.total_seconds() / 3600

    # Internal pipeline delay:
    # Insert time minus message time
    adt_events["a03_pipeline_lag_minutes"] = (
        adt_events["a03_insert_timestamp"]
        - adt_events["a03_message_timestamp"]
    ).dt.total_seconds() / 60

    # Primary lag:
    # Database availability minus actual discharge time
    adt_events["discharge_lag_hours"] = (
        adt_events["a03_insert_timestamp"]
        - adt_events["a03_discharge_ts"]
    ).dt.total_seconds() / 3600

    # Calendar-day lag for sources without reliable discharge time
    adt_events["discharge_lag_days"] = (
        adt_events[
            "a03_insert_timestamp"
        ].dt.normalize()
        - adt_events[
            "a03_discharge_ts"
        ].dt.normalize()
    ).dt.days

    adt_events["adt_event_id"] = np.arange(
        1,
        len(adt_events) + 1
    )

    # =========================================================
    # 7. Exact match by member and admission date
    # =========================================================

    exact_map = (
        claims[
            [
                "claim_event_id",
                "member_id",
                "claim_admit_date",
            ]
        ]
        .merge(
            adt_events[
                [
                    "adt_event_id",
                    "member_id",
                    "adt_admit_date",
                ]
            ],
            left_on=[
                "member_id",
                "claim_admit_date",
            ],
            right_on=[
                "member_id",
                "adt_admit_date",
            ],
            how="inner",
            validate="1:1"
        )
        [
            [
                "claim_event_id",
                "adt_event_id",
            ]
        ]
    )

    exact_map["match_method"] = (
        "Exact Admit Date"
    )

    matched_claim_ids = set(
        exact_map["claim_event_id"]
    )

    matched_adt_ids = set(
        exact_map["adt_event_id"]
    )

    unmatched_claims = claims[
        ~claims["claim_event_id"].isin(
            matched_claim_ids
        )
    ][
        [
            "claim_event_id",
            "member_id",
            "claim_admit_date",
            "claim_discharge_date",
        ]
    ]

    unmatched_adt = adt_events[
        ~adt_events["adt_event_id"].isin(
            matched_adt_ids
        )
    ][
        [
            "adt_event_id",
            "member_id",
            "adt_admit_date",
            "adt_discharge_date",
            "a03_insert_timestamp",
        ]
    ]

    # =========================================================
    # 8. Fuzzy admission match within ±N days
    # =========================================================

    candidates = unmatched_claims.merge(
        unmatched_adt,
        on="member_id",
        how="inner"
    )

    if not candidates.empty:

        candidates["admit_date_difference"] = (
            candidates["adt_admit_date"]
            - candidates["claim_admit_date"]
        ).dt.days

        candidates = candidates[
            candidates[
                "admit_date_difference"
            ]
            .abs()
            .le(fuzzy_days)
        ].copy()

        candidates["abs_admit_diff"] = (
            candidates[
                "admit_date_difference"
            ].abs()
        )

        # Use discharge difference as a tie breaker
        candidates["abs_discharge_diff"] = (
            candidates["adt_discharge_date"]
            - candidates["claim_discharge_date"]
        ).dt.days.abs()

        candidates["abs_discharge_diff"] = (
            candidates["abs_discharge_diff"]
            .fillna(9999)
        )

        candidates = candidates.sort_values(
            [
                "abs_admit_diff",
                "abs_discharge_diff",
                "a03_insert_timestamp",
                "claim_event_id",
                "adt_event_id",
            ],
            na_position="last"
        )

        # Greedy one-to-one matching prevents one ADT
        # encounter from matching multiple claims events.
        fuzzy_rows = []
        used_claims = set()
        used_adt = set()

        for row in candidates.itertuples(
            index=False
        ):

            if (
                row.claim_event_id in used_claims
                or row.adt_event_id in used_adt
            ):
                continue

            fuzzy_rows.append(
                {
                    "claim_event_id":
                        row.claim_event_id,

                    "adt_event_id":
                        row.adt_event_id,

                    "match_method":
                        f"Fuzzy Admit Date ±{fuzzy_days} Day",
                }
            )

            used_claims.add(
                row.claim_event_id
            )

            used_adt.add(
                row.adt_event_id
            )

        fuzzy_map = pd.DataFrame(
            fuzzy_rows,
            columns=[
                "claim_event_id",
                "adt_event_id",
                "match_method",
            ]
        )

    else:

        fuzzy_map = pd.DataFrame(
            columns=[
                "claim_event_id",
                "adt_event_id",
                "match_method",
            ]
        )

    match_map = pd.concat(
        [
            exact_map,
            fuzzy_map,
        ],
        ignore_index=True
    )

    # =========================================================
    # 9. Create claims-centered reconciliation table
    # =========================================================

    qa = (
        claims
        .merge(
            match_map,
            on="claim_event_id",
            how="left",
            validate="1:1"
        )
        .merge(
            adt_events,
            on="adt_event_id",
            how="left",
            validate="m:1",
            suffixes=("", "_adt")
        )
    )

    qa["admit_date_difference"] = (
        qa["adt_admit_date"]
        - qa["claim_admit_date"]
    ).dt.days

    qa["discharge_date_difference"] = (
        qa["adt_discharge_date"]
        - qa["claim_discharge_date"]
    ).dt.days

    # =========================================================
    # 10. Add QA flags
    # =========================================================

    qa["matched_flag"] = (
        qa["adt_event_id"]
        .notna()
        .astype(int)
    )

    qa["fuzzy_match_flag"] = (
        qa["match_method"]
        .str.startswith(
            "Fuzzy",
            na=False
        )
        .astype(int)
    )

    qa["a03_present_flag"] = (
        qa["a03_message_count"]
        .fillna(0)
        .gt(0)
        .astype(int)
    )

    qa["missing_adt_discharge_flag"] = (
        qa["adt_event_id"].isna()
        | qa["a03_discharge_ts"].isna()
    ).astype(int)

    # =========================================================
    # 11. Create discharge-quality status
    # =========================================================

    qa["discharge_quality_status"] = np.select(
        [
            qa["claim_discharge_date"].isna(),

            qa["adt_event_id"].isna(),

            (
                qa["a03_message_count"]
                .fillna(0)
                .eq(0)
                & qa["adt_discharge_ts"].isna()
            ),

            (
                qa["a03_message_count"]
                .fillna(0)
                .eq(0)
                & qa["adt_discharge_ts"].notna()
            ),

            (
                qa["a03_message_count"]
                .fillna(0)
                .gt(0)
                & qa["a03_discharge_ts"].isna()
            ),

            qa[
                "discharge_date_difference"
            ].eq(0),

            qa[
                "discharge_date_difference"
            ].abs().le(1),
        ],
        [
            "Claims discharge date missing",

            "Missing ADT encounter/discharge",

            "ADT matched, discharge missing",

            "ADT matched, discharge found but no A03",

            "A03 present, discharge date missing",

            "Matched discharge date",

            "Discharge within ±1 day",
        ],
        default="Discharge mismatch >1 day"
    )

    # =========================================================
    # 12. Create lag bucket
    # =========================================================

    qa["lag_status"] = np.select(
        [
            qa["discharge_lag_hours"].isna(),

            qa["discharge_lag_hours"].lt(0),

            qa["discharge_lag_hours"].le(4),

            qa["discharge_lag_hours"].le(24),

            qa["discharge_lag_hours"].le(48),
        ],
        [
            "Lag unavailable",

            "Negative lag - investigate",

            "0-4 hours",

            "4-24 hours",

            "24-48 hours",
        ],
        default="Over 48 hours"
    )

    # =========================================================
    # 13. Find ADT encounters without a claims match
    # =========================================================

    matched_adt_ids = set(
        match_map["adt_event_id"].dropna()
    )

    adt_unmatched = adt_events[
        ~adt_events["adt_event_id"].isin(
            matched_adt_ids
        )
    ].copy()

    # =========================================================
    # 14. Create QA summary
    # =========================================================

    valid_lags = qa.loc[
        qa["discharge_lag_hours"].ge(0),
        "discharge_lag_hours",
    ].dropna()

    summary = pd.DataFrame(
        {
            "Metric": [
                "Claims inpatient events",

                "Exact admission matches",

                f"Fuzzy admission matches ±{fuzzy_days} day",

                "Claims events missing ADT",

                "Matched events with A03",

                "Matched discharge date",

                "Discharge within ±1 day",

                "Discharge mismatch >1 day",

                "Median discharge lag hours",

                "90th percentile discharge lag hours",

                "Percent of valid A03 lags within 24 hours",
            ],

            "Value": [
                len(qa),

                qa["match_method"]
                .eq("Exact Admit Date")
                .sum(),

                qa["fuzzy_match_flag"].sum(),

                qa["adt_event_id"]
                .isna()
                .sum(),

                qa["a03_present_flag"].sum(),

                qa["discharge_quality_status"]
                .eq("Matched discharge date")
                .sum(),

                qa["discharge_quality_status"]
                .eq("Discharge within ±1 day")
                .sum(),

                qa["discharge_quality_status"]
                .eq("Discharge mismatch >1 day")
                .sum(),

                valid_lags.median()
                if len(valid_lags)
                else np.nan,

                valid_lags.quantile(0.90)
                if len(valid_lags)
                else np.nan,

                valid_lags.le(24).mean()
                if len(valid_lags)
                else np.nan,
            ],
        }
    )

    # =========================================================
    # 15. Select final spreadsheet columns
    # =========================================================

    output_columns = [
        "claim_event_id",
        "member_id",

        "claim_admit_ts",
        "claim_discharge_ts",

        "match_method",
        "matched_flag",
        "fuzzy_match_flag",

        "adt_admit_ts",
        "adt_discharge_ts",
        "discharge_source",

        "admit_date_difference",
        "discharge_date_difference",

        "message_types_seen",
        "adt_message_count",
        "a03_message_count",
        "a08_message_count",

        "a03_message_timestamp",
        "a03_insert_timestamp",

        "a03_source_lag_hours",
        "a03_pipeline_lag_minutes",

        "discharge_lag_hours",
        "discharge_lag_days",
        "lag_status",

        "discharge_quality_status",
        "missing_adt_discharge_flag",
    ]

    qa_output = (
        qa[
            [
                column
                for column in output_columns
                if column in qa.columns
            ]
        ]
        .sort_values(
            [
                "claim_admit_ts",
                "member_id",
            ]
        )
        .reset_index(drop=True)
    )

    return (
        qa_output,
        summary,
        adt_unmatched
    )
```

## Run the process

```python
qa_output, summary, adt_unmatched = (
    build_adt_discharge_qa(
        df_claims=df_claims,
        df_adt=df_adt,
        fuzzy_days=1
    )
)
```

## Export to Excel

```python
output_file = "ADT_Discharge_Quality_QA.xlsx"

with pd.ExcelWriter(
    output_file,
    datetime_format="yyyy-mm-dd hh:mm:ss",
    date_format="yyyy-mm-dd"
) as writer:

    qa_output.to_excel(
        writer,
        sheet_name="Discharge_QA",
        index=False
    )

    summary.to_excel(
        writer,
        sheet_name="Summary",
        index=False
    )

    adt_unmatched.to_excel(
        writer,
        sheet_name="ADT_Unmatched",
        index=False
    )

print(f"Created: {output_file}")
```

## Most important output columns

| Column                       | Interpretation                                                      |
| ---------------------------- | ------------------------------------------------------------------- |
| `match_method`               | Exact admission match, fuzzy match, or blank when ADT is missing    |
| `discharge_quality_status`   | Main quality conclusion for the event                               |
| `discharge_date_difference`  | ADT discharge date minus claims discharge date                      |
| `discharge_lag_hours`        | **Primary lag**, A03 insert timestamp minus A03 discharge timestamp |
| `a03_source_lag_hours`       | Hospital delay between discharge and generating the A03             |
| `a03_pipeline_lag_minutes`   | Internal delay between receiving the message and inserting it       |
| `lag_status`                 | Human-readable lag category                                         |
| `message_types_seen`         | All message types received for that hospitalization                 |
| `a08_message_count`          | Number of updates received                                          |
| `missing_adt_discharge_flag` | `1` when a valid A03 discharge is unavailable                       |

### Important timestamp limitation

Use `discharge_lag_hours` only when `adt_discharge_ts` contains an actual time. When the field contains only a date, pandas assigns midnight, making hourly lag appear longer than it really was. In that situation, use `discharge_lag_days` as the defensible metric.
