The source classification is working. The problem is most likely **between `adt_events_candidate` and the selected-match tables**.

## Why `AUTHORIZATION` can exist but `authorization_flag` stays 0

`authorization_flag` is **not calculated directly from `adt_events_candidate`**. It is calculated only from rows that survive into:

```sql
analytics.notification_match_detail_2025
```

The flow is:

```text
adt_events_candidate
→ admission/discharge candidates
→ ranking
→ selected matches
→ notification_match_detail_2025
→ authorization_flag
```

This aggregation is correct:

```sql
MAX(
    CASE
        WHEN source_category = 'AUTHORIZATION' THEN 1
        ELSE 0
    END
) AS authorization_flag
```

Therefore, if every final flag is 0, there are no selected `AUTHORIZATION` rows in `notification_match_detail_2025`.

---

# First: identify where authorization disappears

Run this in the same session as the pipeline:

```sql
/*==============================================================
  AUTHORIZATION MATCH WATERFALL
==============================================================*/

SELECT
    '1 - ADT candidate rows' AS stage,
    COUNT(*) AS row_count,
    COUNT(DISTINCT personid) AS person_count,
    NULL::BIGINT AS claim_event_count
FROM adt_events_candidate
WHERE source_category = 'AUTHORIZATION'

UNION ALL

SELECT
    '2 - Admission-eligible ADT rows',
    COUNT(*),
    COUNT(DISTINCT personid),
    NULL::BIGINT
FROM adt_events_candidate
WHERE source_category = 'AUTHORIZATION'
  AND admission_candidate_flag = 1

UNION ALL

SELECT
    '3 - Discharge-eligible ADT rows',
    COUNT(*),
    COUNT(DISTINCT personid),
    NULL::BIGINT
FROM adt_events_candidate
WHERE source_category = 'AUTHORIZATION'
  AND discharge_candidate_flag = 1

UNION ALL

SELECT
    '4 - Admission match candidates',
    COUNT(*),
    COUNT(DISTINCT person_id),
    COUNT(DISTINCT claim_event_id)
FROM tmp_admission_match_candidates
WHERE source_category = 'AUTHORIZATION'

UNION ALL

SELECT
    '5 - Selected admission matches',
    COUNT(*),
    COUNT(DISTINCT person_id),
    COUNT(DISTINCT claim_event_id)
FROM tmp_admission_matches
WHERE source_category = 'AUTHORIZATION'

UNION ALL

SELECT
    '6 - Discharge match candidates',
    COUNT(*),
    COUNT(DISTINCT person_id),
    COUNT(DISTINCT claim_event_id)
FROM tmp_discharge_match_candidates
WHERE source_category = 'AUTHORIZATION'

UNION ALL

SELECT
    '7 - Selected discharge matches',
    COUNT(*),
    COUNT(DISTINCT person_id),
    COUNT(DISTINCT claim_event_id)
FROM tmp_discharge_matches
WHERE source_category = 'AUTHORIZATION'

UNION ALL

SELECT
    '8 - Persistent match detail',
    COUNT(*),
    COUNT(DISTINCT person_id),
    COUNT(DISTINCT claim_event_id)
FROM analytics.notification_match_detail_2025
WHERE source_category = 'AUTHORIZATION';
```

## Interpretation

| Authorization disappears at             | Likely cause                                                         |
| --------------------------------------- | -------------------------------------------------------------------- |
| Stages 2–3                              | Authorization rows do not have usable admission/discharge dates      |
| Stage 4 or 6                            | Person, date tolerance, or patient-class criteria prevented matching |
| Stage 5 or 7                            | Ranking logic removed the candidates                                 |
| Stage 8 only                            | Persistent detail table was not rebuilt from the current temp tables |
| Stage 8 has rows, but final flags are 0 | Downstream aggregation/final table is stale                          |

---

# Most likely problem: the dual ranking condition

The script currently calculates two independent ranks:

```sql
claim_source_rank
message_event_rank
```

Then requires both to equal 1:

```sql
WHERE claim_source_rank = 1
  AND message_event_rank = 1
```

This selects only candidates that are simultaneously:

1. The best authorization message for the claim, **and**
2. The best claim for that authorization message

That sounds reasonable, but independent rankings can conflict.

## Example

Suppose:

* Authorization message `M1` considers claim `C1` its best match.
* Claim `C1` considers authorization message `M2` its best message.
* `M2` considers claim `C2` its best match.

Then:

| Candidate | Claim rank | Message rank | Selected? |
| --------- | ---------: | -----------: | --------- |
| C1–M1     |          2 |            1 | No        |
| C1–M2     |          1 |            2 | No        |

Neither row survives, even though valid authorization candidates exist.

This is especially likely for authorization data because one authorization episode may generate multiple rows, updates, or nearby-date candidates.

## Confirm whether this happened

```sql
SELECT
    claim_source_rank,
    message_event_rank,
    COUNT(*) AS authorization_candidates

FROM tmp_admission_match_candidates

WHERE source_category = 'AUTHORIZATION'

GROUP BY
    claim_source_rank,
    message_event_rank

ORDER BY
    claim_source_rank,
    message_event_rank;
```

Run the same query for discharge:

```sql
SELECT
    claim_source_rank,
    message_event_rank,
    COUNT(*) AS authorization_candidates

FROM tmp_discharge_match_candidates

WHERE source_category = 'AUTHORIZATION'

GROUP BY
    claim_source_rank,
    message_event_rank

ORDER BY
    claim_source_rank,
    message_event_rank;
```

If you see substantial counts in:

```text
claim_source_rank = 1, message_event_rank > 1
```

and:

```text
claim_source_rank > 1, message_event_rank = 1
```

but few or no rows where both equal 1, the ranking design is the cause.

---

# Recommended fix: make ranking sequential

Assign each ADT message to its best claim first. Then, among the assigned messages, select the best message per claim and source.

## Admission fix

Replace the current `tmp_admission_matches` creation with:

```sql
/*==============================================================
  STEP 1: ASSIGN EACH ADMISSION MESSAGE TO ITS BEST CLAIM
==============================================================*/

DROP TABLE IF EXISTS tmp_admission_message_assignment;

CREATE TEMP TABLE tmp_admission_message_assignment AS

SELECT *
FROM tmp_admission_match_candidates

QUALIFY ROW_NUMBER() OVER
(
    PARTITION BY adt_message_key

    ORDER BY
        patient_class_penalty,
        ABS(date_difference),

        CASE
            WHEN facility_match_flag = 1 THEN 0
            ELSE 1
        END,

        message_type_priority,
        insert_timestamp ASC NULLS LAST,
        message_timestamp ASC NULLS LAST,
        claim_event_id
) = 1;
```

Then select the best assigned authorization/feed row for each claim:

```sql
/*==============================================================
  STEP 2: SELECT BEST ASSIGNED MESSAGE PER CLAIM + SOURCE/FEED
==============================================================*/

DROP TABLE IF EXISTS tmp_admission_matches;

CREATE TEMP TABLE tmp_admission_matches AS

SELECT
    claim_event_id,
    native_event_id,
    claims_source,
    person_id,
    memberno,
    care_type,
    care_subtype,
    event_admit_date,
    event_discharge_date,
    claim_servicefacility,
    ipa_name,
    reporting_group,

    adt_message_key,
    adt_message_id,
    native_adt_event_id,
    source_category,
    sending_source,
    adt_patient_class,
    adt_event_location,
    adt_city,
    adt_state,
    adt_zip,
    message_type,
    notification_role,
    admit_type,
    admit_source,
    service_type,
    adt_match_date,
    message_timestamp,
    insert_timestamp,

    'ADMISSION' AS match_target,
    match_method,
    date_difference,
    patient_class_match_status,
    facility_match_flag,
    notification_lag_days,
    pipeline_lag_minutes,
    source_feed_key,

    1 AS selected_match_flag

FROM tmp_admission_message_assignment

QUALIFY ROW_NUMBER() OVER
(
    PARTITION BY
        claim_event_id,
        source_category,
        source_feed_key

    ORDER BY
        patient_class_penalty,
        ABS(date_difference),

        CASE
            WHEN facility_match_flag = 1 THEN 0
            ELSE 1
        END,

        message_type_priority,
        insert_timestamp ASC NULLS LAST,
        message_timestamp ASC NULLS LAST,
        adt_message_key
) = 1;
```

---

## Discharge fix

```sql
/*==============================================================
  STEP 1: ASSIGN EACH DISCHARGE MESSAGE TO ITS BEST CLAIM
==============================================================*/

DROP TABLE IF EXISTS tmp_discharge_message_assignment;

CREATE TEMP TABLE tmp_discharge_message_assignment AS

SELECT *
FROM tmp_discharge_match_candidates

QUALIFY ROW_NUMBER() OVER
(
    PARTITION BY adt_message_key

    ORDER BY
        patient_class_penalty,
        ABS(date_difference),

        CASE
            WHEN facility_match_flag = 1 THEN 0
            ELSE 1
        END,

        message_type_priority,
        insert_timestamp ASC NULLS LAST,
        message_timestamp ASC NULLS LAST,
        claim_event_id
) = 1;
```

```sql
/*==============================================================
  STEP 2: SELECT BEST ASSIGNED MESSAGE PER CLAIM + SOURCE/FEED
==============================================================*/

DROP TABLE IF EXISTS tmp_discharge_matches;

CREATE TEMP TABLE tmp_discharge_matches AS

SELECT
    claim_event_id,
    native_event_id,
    claims_source,
    person_id,
    memberno,
    care_type,
    care_subtype,
    event_admit_date,
    event_discharge_date,
    claim_discharge_date_inferred_flag,
    claim_servicefacility,
    ipa_name,
    reporting_group,

    adt_message_key,
    adt_message_id,
    native_adt_event_id,
    source_category,
    sending_source,
    adt_patient_class,
    adt_event_location,
    adt_city,
    adt_state,
    adt_zip,
    message_type,
    notification_role,
    discharge_disposition,
    discharge_location,
    service_type,
    adt_match_date,
    adt_discharge_date_inferred_flag,
    message_timestamp,
    insert_timestamp,

    'DISCHARGE' AS match_target,
    match_method,
    date_difference,
    patient_class_match_status,
    facility_match_flag,
    notification_lag_days,
    pipeline_lag_minutes,
    source_feed_key,

    1 AS selected_match_flag

FROM tmp_discharge_message_assignment

QUALIFY ROW_NUMBER() OVER
(
    PARTITION BY
        claim_event_id,
        source_category,
        source_feed_key

    ORDER BY
        patient_class_penalty,
        ABS(date_difference),

        CASE
            WHEN facility_match_flag = 1 THEN 0
            ELSE 1
        END,

        message_type_priority,
        insert_timestamp ASC NULLS LAST,
        message_timestamp ASC NULLS LAST,
        adt_message_key
) = 1;
```

---

# Also verify authorization date availability

The new process changed authorization treatment compared with the earlier process.

An authorization row qualifies only when it has the relevant date:

```sql
-- Admission
source_category = 'AUTHORIZATION'
AND admit_date IS NOT NULL
```

```sql
-- Discharge
source_category = 'AUTHORIZATION'
AND discharge_date IS NOT NULL
```

Run:

```sql
SELECT
    COUNT(*) AS authorization_rows,

    SUM(
        CASE WHEN admit_date IS NOT NULL
             THEN 1 ELSE 0 END
    ) AS authorization_with_admit_date,

    SUM(
        CASE WHEN discharge_date IS NOT NULL
             THEN 1 ELSE 0 END
    ) AS authorization_with_discharge_date,

    SUM(admission_candidate_flag)
        AS admission_candidate_rows,

    SUM(discharge_candidate_flag)
        AS discharge_candidate_rows

FROM adt_events_candidate

WHERE source_category = 'AUTHORIZATION';
```

If `authorization_with_admit_date` or `authorization_with_discharge_date` is very low, the disappearance is caused by the role-specific date requirements rather than ranking.

---

# Check the matching identifier

The new code matches on:

```sql
e.person_id = a.personid
```

The older process also used person ID, but confirm that MedHOK authorization rows have a usable person ID:

```sql
SELECT
    COUNT(*) AS authorization_rows,

    SUM(
        CASE WHEN personid IS NULL
             THEN 1 ELSE 0 END
    ) AS missing_personid,

    COUNT(DISTINCT personid)
        AS distinct_personids

FROM adt_events_candidate

WHERE source_category = 'AUTHORIZATION';
```

Then determine whether authorization people exist in the claims population:

```sql
SELECT
    COUNT(DISTINCT a.personid) AS authorization_people,

    COUNT(
        DISTINCT CASE
            WHEN e.person_id IS NOT NULL
                THEN a.personid
        END
    ) AS authorization_people_in_claims

FROM adt_events_candidate a

LEFT JOIN tmp_events_2025 e
    ON a.personid = e.person_id

WHERE a.source_category = 'AUTHORIZATION';
```

---

## Bottom line

The final flag calculation is not the likely problem. The authorization rows are being lost before aggregation.

The two leading explanations are:

1. **Authorization rows do not have the required admission/discharge dates**, so their candidate flags are zero.
2. **The simultaneous `claim_source_rank = 1 AND message_event_rank = 1` rule rejects otherwise valid candidates.**

Run the waterfall first. If authorization reaches the candidate tables but disappears from `tmp_admission_matches` or `tmp_discharge_matches`, replace the simultaneous ranking with the sequential two-stage process above.
