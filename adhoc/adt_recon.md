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

# 7. Combine exact and fuzzy matches

```python
comparison = pd.concat(
    [
        exact_matches,
        fuzzy_matches,
    ],
    ignore_index=True,
    sort=False
)
```

Add discharge-date comparisons.

```python
comparison["discharge_date_difference"] = (
    comparison["adt_discharge"]
    - comparison["claim_discharge"]
).dt.days

comparison["discharge_match_flag"] = np.select(
    [
        comparison["claim_discharge"].isna()
        | comparison["adt_discharge"].isna(),

        comparison["discharge_date_difference"].eq(0),

        comparison["discharge_date_difference"].abs().le(1),
    ],
    [
        "Missing Discharge Date",
        "Exact Discharge Match",
        "Discharge Within ±1 Day",
    ],
    default="Discharge Difference >1 Day"
)
```

---

# 8. Create the final reconciliation status

```python
comparison["overall_match_status"] = np.select(
    [
        comparison["match_method"].eq("Exact Admit Date")
        & comparison["discharge_date_difference"].eq(0),

        comparison["match_method"].eq("Exact Admit Date")
        & comparison["discharge_date_difference"].abs().le(1),

        comparison["match_method"].eq("Fuzzy Admit Date ±1 Day")
        & comparison["discharge_date_difference"].abs().le(1),

        comparison["claim_discharge"].isna()
        | comparison["adt_discharge"].isna(),
    ],
    [
        "Exact Admit and Discharge",
        "Exact Admit, Discharge Within ±1 Day",
        "Fuzzy Admit and Discharge Within ±1 Day",
        "Matched Admit, Missing Discharge",
    ],
    default="Matched Admit, Discharge Mismatch"
)
```

---

# 9. Identify final unmatched claims and ADT events

```python
matched_claim_keys = comparison[
    ["member_id", "claim_admit"]
].drop_duplicates()

claims_unmatched_final = claims.merge(
    matched_claim_keys.assign(matched=1),
    on=["member_id", "claim_admit"],
    how="left"
)

claims_unmatched_final = claims_unmatched_final[
    claims_unmatched_final["matched"].isna()
].drop(columns="matched")
```

```python
matched_adt_keys = comparison[
    ["member_id", "adt_admit"]
].drop_duplicates()

adt_unmatched_final = adt.merge(
    matched_adt_keys.assign(matched=1),
    on=["member_id", "adt_admit"],
    how="left"
)

adt_unmatched_final = adt_unmatched_final[
    adt_unmatched_final["matched"].isna()
].drop(columns="matched")
```

---

# 10. Produce the comparison summary

```python
summary = pd.DataFrame(
    {
        "Metric": [
            "Distinct claims events",
            "Distinct ADT events",
            "Exact admission matches",
            "Fuzzy admission matches",
            "Total matched claims events",
            "Unmatched claims events",
            "Unmatched ADT events",
            "Claims match rate",
            "ADT match rate",
        ],
        "Value": [
            len(claims),
            len(adt),
            comparison["match_method"]
                .eq("Exact Admit Date")
                .sum(),
            comparison["match_method"]
                .eq("Fuzzy Admit Date ±1 Day")
                .sum(),
            comparison[
                ["member_id", "claim_admit"]
            ].drop_duplicates().shape[0],
            len(claims_unmatched_final),
            len(adt_unmatched_final),
            comparison[
                ["member_id", "claim_admit"]
            ].drop_duplicates().shape[0] / len(claims)
            if len(claims) else np.nan,
            comparison[
                ["member_id", "adt_admit"]
            ].drop_duplicates().shape[0] / len(adt)
            if len(adt) else np.nan,
        ],
    }
)

summary
```

---

## Monthly reconciliation view

This helps determine whether missing ADT coverage is concentrated in particular months.

```python
claims_monthly = (
    claims
    .assign(month=claims["claim_admit"].dt.to_period("M").dt.to_timestamp())
    .groupby("month")
    .agg(
        claim_events=("claim_admit", "size")
    )
    .reset_index()
)

adt_monthly = (
    adt
    .assign(month=adt["adt_admit"].dt.to_period("M").dt.to_timestamp())
    .groupby("month")
    .agg(
        adt_events=("adt_admit", "size")
    )
    .reset_index()
)

matched_monthly = (
    comparison
    .assign(month=comparison["claim_admit"].dt.to_period("M").dt.to_timestamp())
    .groupby("month")
    .agg(
        matched_events=("claim_admit", "size"),
        exact_matches=(
            "match_method",
            lambda x: x.eq("Exact Admit Date").sum()
        ),
        fuzzy_matches=(
            "match_method",
            lambda x: x.eq("Fuzzy Admit Date ±1 Day").sum()
        ),
    )
    .reset_index()
)

monthly_comparison = (
    claims_monthly
    .merge(adt_monthly, on="month", how="outer")
    .merge(matched_monthly, on="month", how="outer")
    .fillna(0)
)

monthly_comparison["claims_match_rate"] = (
    monthly_comparison["matched_events"]
    / monthly_comparison["claim_events"].replace(0, np.nan)
)

monthly_comparison["adt_match_rate"] = (
    monthly_comparison["matched_events"]
    / monthly_comparison["adt_events"].replace(0, np.nan)
)

monthly_comparison.sort_values("month")
```

---

# Same-timeframe comparison

Because ADT data may lag claims, derive the latest **complete overlapping date**.

```python
claims_max_date = claims["claim_admit"].max()
adt_max_date = adt["adt_admit"].max()

overlap_end_date = min(
    claims_max_date,
    adt_max_date
)

overlap_start_date = max(
    claims["claim_admit"].min(),
    adt["adt_admit"].min()
)

print("Overlapping period:")
print(overlap_start_date, "through", overlap_end_date)
```

Filter both datasets to the same range:

```python
claims_aligned = claims[
    claims["claim_admit"].between(
        overlap_start_date,
        overlap_end_date
    )
].copy()

adt_aligned = adt[
    adt["adt_admit"].between(
        overlap_start_date,
        overlap_end_date
    )
].copy()
```

For an apples-to-apples YTD comparison:

```python
comparison_year = 2026

current_end = min(
    overlap_end_date,
    pd.Timestamp(f"{comparison_year}-12-31")
)

current_start = pd.Timestamp(
    f"{comparison_year}-01-01"
)

prior_start = current_start - pd.DateOffset(years=1)
prior_end = current_end - pd.DateOffset(years=1)

claims_current_ytd = claims[
    claims["claim_admit"].between(
        current_start,
        current_end
    )
]

claims_prior_aligned = claims[
    claims["claim_admit"].between(
        prior_start,
        prior_end
    )
]
```

## Recommended output fields

Keep this reconciliation table:

```python
comparison_output = comparison[
    [
        "member_id",
        "claim_admit",
        "claim_discharge",
        "adt_admit",
        "adt_discharge",
        "insert_timestamp",
        "message_timestamp",
        "match_method",
        "admit_date_difference",
        "discharge_date_difference",
        "discharge_match_flag",
        "overall_match_status",
    ]
].sort_values(
    ["member_id", "claim_admit"]
)
```

This gives you a defensible way to answer:

* How many claims admissions have an ADT?
* How many match exactly?
* How many need a date tolerance?
* How much ADT coverage exists by month?
* Are discharge dates aligned?
* Is the apparent difference due to data latency or true missing events?

For production QA, I would use **exact admit date as the primary match**, ±1 day only as the documented fallback, and report both separately.
