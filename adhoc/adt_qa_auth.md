# Solution
Yes. If MedHOK authorization records have a blank `patient_class`, they cannot independently tell you whether the authorization represents **ED, IP, or OBS**.

You should not force an ADT patient class onto those records. Instead:

> Match authorization using member and event dates, then inherit the care setting from the matched claims event.

However, **member-date alone is not always sufficient**, especially when a member has ED, OBS, and IP events on the same date.

# Recommended source-specific matching

## HIE / EMR notifications

Continue using:

* Person/member
* Event date
* ADT patient class
* Facility
* Message type
* Ranking

## MedHOK authorization

Use:

1. Same `person_id`
2. Admission-date proximity
3. Discharge-date proximity, when available
4. Facility agreement, when available
5. One-to-one ranking
6. Ambiguity control for multiple same-day claims

Do **not** require `patient_class`.

---

# Recommended authorization match hierarchy

## High-confidence match

Accept when:

```text
Same person
+ exact admission date
+ exact discharge date, when both are available
```

or:

```text
Same person
+ exact admission date
+ same facility
```

## Medium-confidence match

Accept when:

```text
Same person
+ admission date within ±1 day
+ only one eligible claims event in the window
```

## Ambiguous match

Do not automatically accept when:

```text
Same person
+ same date
+ multiple ED/OBS/IP claims events
+ no facility or discharge-date evidence
```

Those records should be labeled:

```text
Authorization Match Ambiguous
```

rather than assigned to every event.

---

# Candidate join adjustment

For admission matching:

```sql
INNER JOIN adt_events_candidate a
    ON e.person_id = a.personid
   AND a.admission_candidate_flag = 1
   AND a.admit_date IS NOT NULL

WHERE ABS
(
    DATEDIFF
    (
        day,
        e.event_admit_date,
        a.admit_date
    )
) <=
CASE
    WHEN a.source_category = 'AUTHORIZATION'
        THEN 1
    ELSE p.fuzzy_days
END

AND
(
       a.source_category = 'AUTHORIZATION'

    OR a.patient_class = e.care_type

    OR a.patient_class IS NULL

    OR
    (
        e.care_type = 'OBS'
        AND a.patient_class IN ('ED', 'IP')
    )
)
```

Use the equivalent logic for discharge matching:

```sql
AND
(
       a.source_category = 'AUTHORIZATION'

    OR a.patient_class = e.care_type

    OR a.patient_class IS NULL

    OR
    (
        e.care_type = 'OBS'
        AND a.patient_class IN ('ED', 'IP')
    )
)
```

---

# Do not call it a patient-class match

Update the status logic:

```sql
CASE
    WHEN a.source_category = 'AUTHORIZATION'
        THEN 'Authorization - Patient Class Unavailable'

    WHEN a.patient_class = e.care_type
        THEN 'Exact Patient Class'

    WHEN a.patient_class IS NULL
        THEN 'Missing Patient Class Fallback'

    WHEN e.care_type = 'OBS'
     AND a.patient_class IN ('ED', 'IP')
        THEN 'OBS Cross-Class Fallback'

    ELSE 'Unsupported Cross-Class'
END AS patient_class_match_status
```

This makes it clear that the authorization match is not evidence that MedHOK classified the encounter as IP, ED, or OBS.

---

# Add an authorization match score

In the authorization candidate table, add:

```sql
CASE
    WHEN a.source_category <> 'AUTHORIZATION'
        THEN NULL

    WHEN e.event_admit_date = a.admit_date
     AND e.event_discharge_date = a.discharge_date
        THEN 1

    WHEN e.event_admit_date = a.admit_date
     AND facility_match_flag = 1
        THEN 2

    WHEN e.event_admit_date = a.admit_date
        THEN 3

    WHEN ABS(
            DATEDIFF(
                day,
                e.event_admit_date,
                a.admit_date
            )
         ) = 1
        THEN 4

    ELSE 5
END AS authorization_match_score
```

Interpretation:

| Score | Confidence                              |
| ----: | --------------------------------------- |
|     1 | Exact admit and discharge dates         |
|     2 | Exact admit date and facility agreement |
|     3 | Exact admit date                        |
|     4 | Admit date within one day               |
|     5 | Weak candidate                          |

Rank lower scores first.

---

# Protect against same-day ED/OBS/IP duplication

Calculate how many claim events each authorization message can match:

```sql
COUNT(*) OVER
(
    PARTITION BY a.adt_message_key
) AS authorization_candidate_count
```

Also calculate how many care settings are represented:

```sql
COUNT(DISTINCT e.care_type) OVER
(
    PARTITION BY a.adt_message_key
) AS authorization_care_type_count
```

If Redshift does not support `COUNT(DISTINCT ...) OVER`, calculate it in a separate grouped CTE.

Then create:

```sql
CASE
    WHEN a.source_category = 'AUTHORIZATION'
     AND authorization_candidate_count = 1
        THEN 'Unique Authorization Match'

    WHEN a.source_category = 'AUTHORIZATION'
     AND authorization_candidate_count > 1
     AND facility_match_flag = 1
        THEN 'Ranked Authorization Match'

    WHEN a.source_category = 'AUTHORIZATION'
     AND authorization_candidate_count > 1
        THEN 'Ambiguous Authorization Match'

    ELSE NULL
END AS authorization_match_status
```

I would exclude ambiguous authorization matches from the main coverage flags until reviewed.

---

# Should authorization match ED?

This should depend on the MedHOK business process.

If MedHOK authorizations primarily represent inpatient and observation services, restrict authorization matching to:

```sql
AND
(
    a.source_category <> 'AUTHORIZATION'
    OR e.care_type IN ('IP', 'OBS')
)
```

Do not automatically match MedHOK authorization records to ED claims unless you confirm that ED encounters are represented meaningfully in the authorization workflow.

# Recommended final interpretation

Use authorization as a **supplemental notification source**, not as ADT patient-class evidence.

Your final table should distinguish:

```text
HIE/EMR matched with confirmed patient class
Authorization matched by member and date
Authorization match ambiguous
No matched notification
```

The cleanest approach is:

> **Member + exact/near event date for authorization, strengthened by discharge date and facility, with one-to-one ranking and an ambiguity flag.**

----
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
---
# qa2
The syntax error is fixable, but the **ranking change is not yet the solution to the authorization problem**.

Because authorization drops at:

> **Stage 4 — Admission match candidates**

the rows are disappearing **before ranking occurs**. The problem is in the candidate join criteria:

```sql
e.person_id = a.personid
AND a.admission_candidate_flag = 1
AND a.admit_date IS NOT NULL
AND patient-class rule
AND date difference <= fuzzy_days
```

## 1. Why the `ROW_NUMBER()` query failed

In Redshift, `QUALIFY` immediately after a `FROM` clause can require an alias. The safest approach is to use a subquery.

Also verify the table name is plural:

```sql
tmp_admission_match_candidates
```

not:

```sql
tmp_admission_match_candidate
```

### Corrected assignment query

Since `message_event_rank` is already calculated in your candidate table, you do not need to calculate another `ROW_NUMBER()` yet:

```sql
DROP TABLE IF EXISTS tmp_admission_message_assignment;

CREATE TEMP TABLE tmp_admission_message_assignment AS

SELECT *
FROM tmp_admission_match_candidates
WHERE message_event_rank = 1;
```

That is functionally equivalent to:

> Assign each ADT message to its highest-ranked claims event.

Alternatively, the explicit subquery syntax is:

```sql
DROP TABLE IF EXISTS tmp_admission_message_assignment;

CREATE TEMP TABLE tmp_admission_message_assignment AS

SELECT *
FROM
(
    SELECT
        c.*,

        ROW_NUMBER() OVER
        (
            PARTITION BY c.adt_message_key

            ORDER BY
                c.patient_class_penalty,
                ABS(c.date_difference),

                CASE
                    WHEN c.facility_match_flag = 1 THEN 0
                    ELSE 1
                END,

                c.message_type_priority,
                c.insert_timestamp ASC NULLS LAST,
                c.message_timestamp ASC NULLS LAST,
                c.claim_event_id
        ) AS assignment_rn

    FROM tmp_admission_match_candidates c
) ranked

WHERE assignment_rn = 1;
```

But this will not recover authorization rows when none reached `tmp_admission_match_candidates`.

---

# 2. Find which candidate condition removes authorization

Run this authorization-specific waterfall:

```sql
/*==============================================================================
  AUTHORIZATION ADMISSION CANDIDATE DIAGNOSTIC

  Each stage adds one production matching condition.
==============================================================================*/

SELECT
    '1 - Authorization rows' AS stage,
    COUNT(*) AS authorization_rows

FROM adt_events_candidate a

WHERE a.source_category = 'AUTHORIZATION'


UNION ALL


SELECT
    '2 - Has admission candidate flag',
    COUNT(*)

FROM adt_events_candidate a

WHERE a.source_category = 'AUTHORIZATION'
  AND a.admission_candidate_flag = 1


UNION ALL


SELECT
    '3 - Has usable admit date',
    COUNT(*)

FROM adt_events_candidate a

WHERE a.source_category = 'AUTHORIZATION'
  AND a.admission_candidate_flag = 1
  AND a.admit_date IS NOT NULL


UNION ALL


SELECT
    '4 - Person exists in eligible claims',
    COUNT(*)

FROM adt_events_candidate a

WHERE a.source_category = 'AUTHORIZATION'
  AND a.admission_candidate_flag = 1
  AND a.admit_date IS NOT NULL

  AND EXISTS
  (
      SELECT 1
      FROM tmp_events_2025 e
      WHERE e.person_id = a.personid
  )


UNION ALL


SELECT
    '5 - Claim within +/-7 days',
    COUNT(*)

FROM adt_events_candidate a

WHERE a.source_category = 'AUTHORIZATION'
  AND a.admission_candidate_flag = 1
  AND a.admit_date IS NOT NULL

  AND EXISTS
  (
      SELECT 1
      FROM tmp_events_2025 e

      WHERE e.person_id = a.personid

        AND ABS
        (
            DATEDIFF
            (
                day,
                e.event_admit_date,
                a.admit_date
            )
        ) <= 7
  )


UNION ALL


SELECT
    '6 - Claim within production tolerance',
    COUNT(*)

FROM adt_events_candidate a

CROSS JOIN tmp_params p

WHERE a.source_category = 'AUTHORIZATION'
  AND a.admission_candidate_flag = 1
  AND a.admit_date IS NOT NULL

  AND EXISTS
  (
      SELECT 1
      FROM tmp_events_2025 e

      WHERE e.person_id = a.personid

        AND ABS
        (
            DATEDIFF
            (
                day,
                e.event_admit_date,
                a.admit_date
            )
        ) <= p.fuzzy_days
  )


UNION ALL


SELECT
    '7 - Passes patient-class rule',
    COUNT(*)

FROM adt_events_candidate a

CROSS JOIN tmp_params p

WHERE a.source_category = 'AUTHORIZATION'
  AND a.admission_candidate_flag = 1
  AND a.admit_date IS NOT NULL

  AND EXISTS
  (
      SELECT 1
      FROM tmp_events_2025 e

      WHERE e.person_id = a.personid

        AND ABS
        (
            DATEDIFF
            (
                day,
                e.event_admit_date,
                a.admit_date
            )
        ) <= p.fuzzy_days

        AND
        (
               a.patient_class = e.care_type
            OR a.patient_class IS NULL

            OR
            (
                e.care_type = 'OBS'
                AND a.patient_class IN ('ED', 'IP')
            )
        )
  )


UNION ALL


SELECT
    '8 - Actual admission match candidates',
    COUNT(*)

FROM tmp_admission_match_candidates

WHERE source_category = 'AUTHORIZATION';
```

## How to read the result

| Drop occurs between | Likely problem                                                  |
| ------------------- | --------------------------------------------------------------- |
| Stage 3 → 4         | Authorization `personid` does not match the claims population   |
| Stage 4 → 5         | Authorization and claims dates are materially different         |
| Stage 5 → 6         | The ±1-day production tolerance is too narrow for authorization |
| Stage 6 → 7         | Authorization patient class conflicts with claims care type     |
| Stage 7 → 8         | Candidate SQL does not match the QA rules or table is stale     |

---

# 3. Most likely cause: authorization date behavior

Authorization data frequently has a requested, approved, or entered date that does not equal the actual admission date. Your current production match requires:

```sql
ABS(
    DATEDIFF(
        day,
        e.event_admit_date,
        a.admit_date
    )
) <= 1
```

That rule may be appropriate for real-time HIE messages but too strict for authorization records.

Check the closest authorization-to-claims date differences:

```sql
WITH authorization_nearest_claim AS
(
    SELECT
        a.adt_message_key,
        a.personid,
        a.admit_date AS authorization_admit_date,

        MIN
        (
            ABS
            (
                DATEDIFF
                (
                    day,
                    e.event_admit_date,
                    a.admit_date
                )
            )
        ) AS nearest_claim_admit_difference

    FROM adt_events_candidate a

    INNER JOIN tmp_events_2025 e
        ON a.personid = e.person_id

    WHERE a.source_category = 'AUTHORIZATION'
      AND a.admission_candidate_flag = 1
      AND a.admit_date IS NOT NULL

    GROUP BY
        a.adt_message_key,
        a.personid,
        a.admit_date
)

SELECT
    CASE
        WHEN nearest_claim_admit_difference = 0
            THEN 'Exact Date'

        WHEN nearest_claim_admit_difference = 1
            THEN '1 Day'

        WHEN nearest_claim_admit_difference BETWEEN 2 AND 3
            THEN '2–3 Days'

        WHEN nearest_claim_admit_difference BETWEEN 4 AND 7
            THEN '4–7 Days'

        WHEN nearest_claim_admit_difference BETWEEN 8 AND 14
            THEN '8–14 Days'

        ELSE 'Over 14 Days'
    END AS nearest_date_band,

    COUNT(*) AS authorization_messages

FROM authorization_nearest_claim

GROUP BY 1
ORDER BY
    MIN(nearest_claim_admit_difference);
```

If most authorization records are two or more days away, the problem is the common `fuzzy_days = 1` parameter.

---

# 4. Recommended source-specific tolerance

ADT/HIE messages and authorization records should not necessarily use the same date tolerance.

In the admission candidate query, replace:

```sql
WHERE ABS
(
    DATEDIFF
    (
        day,
        e.event_admit_date,
        a.admit_date
    )
) <= p.fuzzy_days
```

with:

```sql
WHERE ABS
(
    DATEDIFF
    (
        day,
        e.event_admit_date,
        a.admit_date
    )
) <=
CASE
    WHEN a.source_category = 'AUTHORIZATION'
        THEN 7

    ELSE p.fuzzy_days
END
```

I would start with seven days for QA, then choose the final tolerance based on the nearest-date distribution.

Also update `match_method`:

```sql
CASE
    WHEN e.event_admit_date = a.admit_date
        THEN 'Exact Admission Date'

    WHEN a.source_category = 'AUTHORIZATION'
        THEN 'Authorization Admission Date +/-7 Days'

    ELSE 'ADT Admission Date +/-'
         || CAST(p.fuzzy_days AS VARCHAR(10))
         || ' Day'
END AS match_method
```

---

# 5. Check whether patient class removes authorization

Run:

```sql
SELECT
    COALESCE(patient_class, 'NULL') AS authorization_patient_class,
    COUNT(*) AS authorization_rows

FROM adt_events_candidate

WHERE source_category = 'AUTHORIZATION'
  AND admission_candidate_flag = 1
  AND admit_date IS NOT NULL

GROUP BY 1
ORDER BY 2 DESC;
```

If authorization rows have unexpected non-null classes, the patient-class rule may be excluding them.

Since authorization is not a real-time ADT feed, you may decide not to enforce patient class for authorization:

```sql
AND
(
       a.source_category = 'AUTHORIZATION'

    OR a.patient_class = e.care_type

    OR a.patient_class IS NULL

    OR
    (
        e.care_type = 'OBS'
        AND a.patient_class IN ('ED', 'IP')
    )
)
```

This keeps care-setting rules for ADT feeds while allowing authorization matching to rely primarily on:

* Person
* Date proximity
* Facility when available
* Source-specific ranking

---

# Recommended order

1. Run the detailed authorization waterfall.
2. Run the nearest-date distribution.
3. Check authorization patient-class values.
4. Correct the candidate join or tolerance.
5. Confirm authorization rows now appear in `tmp_admission_match_candidates`.
6. Only then apply sequential ranking.

The syntax-correct assignment query will work, but it cannot recover authorization records until the candidate-generation issue is resolved.

