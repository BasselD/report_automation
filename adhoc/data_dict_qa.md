



## Recommended design

Do **not** replace the current patient-class-aware match. Add a second, parallel result that answers:

> Is there qualifying ADT activity for the same person and event date, regardless of patient class or claims bed type?

This gives leadership two defensible views:

| View | Meaning |
|---|---|
| **Confirmed match** | Date matched and the approved patient-class rules were satisfied |
| **Date-only evidence** | Date matched regardless of patient class or bed type |

The original exact-date followed by controlled ±1-day matching remains the foundation. fileciteturn0file0

This separation matters because a date-only match may represent:

- A valid status conversion, such as ED → OBS → IP
- A claims versus ADT classification difference
- Two related events occurring on the same date
- A false association between separate same-day events

Therefore, call it **date-only evidence**, not automatically a confirmed event match.

---

# Important clarification about bed type

Your current matching logic uses:

```sql
e.care_type
```

against:

```sql
a.patient_class
```

It does **not** use inpatient `bedtype` or `care_subtype` to accept or reject matches.

So leadership’s “regardless of bed type” question requires no change unless you later added a condition such as:

```sql
e.care_subtype = a.service_type
```

The actual restriction being tested is the relationship between:

```text
Claims care_type: IP / ED / OBS
ADT patient_class: IP / ED / OBS
```

---

# Implementation structure

Add the following two temporary tables:

```text
tmp_admission_date_only_evidence
tmp_discharge_date_only_evidence
```

Place them after the existing selected admission and discharge match sections.

The revised flow becomes:

```text
Class-aware admission candidates
→ selected confirmed admission match

Date-only admission candidates
→ best date-only admission evidence

Class-aware discharge candidates
→ selected confirmed discharge match

Date-only discharge candidates
→ best date-only discharge evidence

Join both views into the final reconciliation table
```

---

# Change 1: keep the current patient-class-aware logic

Keep your current controlled filter in the class-aware candidate tables:

```sql
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
```

That remains the primary, confirmed matching logic.

Also use the corrected status field:

```sql
CASE
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

---

# Change 2: add date-only admission evidence

## Where to add it

Place this immediately after:

```sql
CREATE TEMP TABLE tmp_admission_matches AS ...
```

## SQL

```sql
/*=============================================================================
  DATE-ONLY ADMISSION EVIDENCE

  Purpose:
  Identify whether any qualifying admission notification exists for the same
  person and admission date, without requiring patient-class agreement.

  This is supporting evidence, not the primary confirmed match.
=============================================================================*/

DROP TABLE IF EXISTS tmp_admission_date_only_evidence;

CREATE TEMP TABLE tmp_admission_date_only_evidence AS

WITH date_candidates AS
(
    SELECT
        e.claim_event_id,
        e.person_id,
        e.care_type,
        e.event_admit_date,
        e.servicefacility AS claim_servicefacility,

        a.adt_message_key,
        a.adt_message_id,
        a.native_adt_event_id,

        a.source_category,
        a.sending_source,
        a.patient_class AS adt_patient_class,
        a.event_location AS adt_event_location,

        a.message_type,
        a.admit_date AS adt_admit_date,

        a.message_timestamp,
        a.insert_timestamp,

        DATEDIFF
        (
            day,
            e.event_admit_date,
            a.admit_date
        ) AS admission_date_difference,

        ABS
        (
            DATEDIFF
            (
                day,
                e.event_admit_date,
                a.admit_date
            )
        ) AS absolute_admission_date_difference,

        CASE
            WHEN a.patient_class = e.care_type
                THEN 'Exact Patient Class'

            WHEN a.patient_class IS NULL
                THEN 'Missing Patient Class'

            WHEN e.care_type = 'OBS'
             AND a.patient_class IN ('ED', 'IP')
                THEN 'OBS Cross-Class'

            ELSE 'Other Cross-Class'
        END AS patient_class_relationship,

        CASE
            WHEN NULLIF(TRIM(e.servicefacility), '') IS NOT NULL
             AND
             (
                 UPPER(TRIM(e.servicefacility)) =
                 UPPER(TRIM(a.event_location))

                 OR

                 UPPER(TRIM(e.servicefacility)) =
                 UPPER(TRIM(a.sending_source))
             )
                THEN 1
            ELSE 0
        END AS facility_match_flag,

        DATEDIFF
        (
            day,
            e.event_admit_date,
            CAST(a.insert_timestamp AS DATE)
        ) AS notification_lag_days,

        DATEDIFF
        (
            minute,
            a.message_timestamp,
            a.insert_timestamp
        ) AS pipeline_lag_minutes,

        COUNT(*) OVER
        (
            PARTITION BY e.claim_event_id
        ) AS date_only_admission_candidate_count,

        ROW_NUMBER() OVER
        (
            PARTITION BY e.claim_event_id

            ORDER BY
                ABS
                (
                    DATEDIFF
                    (
                        day,
                        e.event_admit_date,
                        a.admit_date
                    )
                ),

                /* Facility agreement breaks date ties. */
                CASE
                    WHEN NULLIF(TRIM(e.servicefacility), '') IS NOT NULL
                     AND
                     (
                         UPPER(TRIM(e.servicefacility)) =
                         UPPER(TRIM(a.event_location))

                         OR

                         UPPER(TRIM(e.servicefacility)) =
                         UPPER(TRIM(a.sending_source))
                     )
                        THEN 0
                    ELSE 1
                END,

                /* Prefer primary admission/registration messages. */
                CASE
                    WHEN a.message_type = 'A01' THEN 0
                    WHEN a.message_type = 'A04' THEN 1
                    WHEN a.message_type = 'A06' THEN 2
                    WHEN a.message_type = 'A05' THEN 3
                    WHEN a.source_category = 'AUTHORIZATION' THEN 4
                    ELSE 5
                END,

                a.insert_timestamp ASC NULLS LAST,
                a.message_timestamp ASC NULLS LAST,
                a.adt_message_key
        ) AS date_only_rank

    FROM tmp_events_2025 e

    INNER JOIN adt_events_candidate a
        ON e.person_id = a.personid
       AND a.admission_candidate_flag = 1
       AND a.admit_date IS NOT NULL

       /* Intentionally no patient-class condition here. */

    CROSS JOIN tmp_params p

    WHERE ABS
    (
        DATEDIFF
        (
            day,
            e.event_admit_date,
            a.admit_date
        )
    ) <= p.fuzzy_days
)

SELECT
    claim_event_id,

    1 AS date_only_admission_evidence_flag,

    date_only_admission_candidate_count,

    adt_message_key,
    adt_message_id,
    native_adt_event_id,

    source_category
        AS date_only_admission_source_category,

    sending_source
        AS date_only_admission_sending_source,

    adt_patient_class
        AS date_only_admission_patient_class,

    adt_event_location
        AS date_only_admission_event_location,

    message_type
        AS date_only_admission_message_type,

    adt_admit_date
        AS date_only_admission_adt_date,

    message_timestamp
        AS date_only_admission_message_timestamp,

    insert_timestamp
        AS date_only_admission_insert_timestamp,

    admission_date_difference
        AS date_only_admission_date_difference,

    patient_class_relationship
        AS date_only_admission_class_relationship,

    facility_match_flag
        AS date_only_admission_facility_match_flag,

    notification_lag_days
        AS date_only_admission_lag_days,

    pipeline_lag_minutes
        AS date_only_admission_pipeline_lag_minutes,

    CASE
        WHEN admission_date_difference = 0
            THEN 'Exact Admission Date'
        ELSE 'Fuzzy Admission Date'
    END AS date_only_admission_match_method

FROM date_candidates

WHERE date_only_rank = 1;
```

---

# Change 3: add date-only discharge evidence

## Where to add it

Place this immediately after:

```sql
CREATE TEMP TABLE tmp_discharge_matches AS ...
```

## SQL

```sql
/*=============================================================================
  DATE-ONLY DISCHARGE EVIDENCE

  Purpose:
  Identify any qualifying discharge notification for the same person and
  discharge date without requiring patient-class agreement.
=============================================================================*/

DROP TABLE IF EXISTS tmp_discharge_date_only_evidence;

CREATE TEMP TABLE tmp_discharge_date_only_evidence AS

WITH date_candidates AS
(
    SELECT
        e.claim_event_id,
        e.person_id,
        e.care_type,
        e.event_discharge_date,
        e.servicefacility AS claim_servicefacility,

        a.adt_message_key,
        a.adt_message_id,
        a.native_adt_event_id,

        a.source_category,
        a.sending_source,
        a.patient_class AS adt_patient_class,
        a.event_location AS adt_event_location,

        a.message_type,
        a.discharge_match_date AS adt_discharge_date,

        a.discharge_match_date_inferred_flag,

        a.message_timestamp,
        a.insert_timestamp,

        DATEDIFF
        (
            day,
            e.event_discharge_date,
            a.discharge_match_date
        ) AS discharge_date_difference,

        ABS
        (
            DATEDIFF
            (
                day,
                e.event_discharge_date,
                a.discharge_match_date
            )
        ) AS absolute_discharge_date_difference,

        CASE
            WHEN a.patient_class = e.care_type
                THEN 'Exact Patient Class'

            WHEN a.patient_class IS NULL
                THEN 'Missing Patient Class'

            WHEN e.care_type = 'OBS'
             AND a.patient_class IN ('ED', 'IP')
                THEN 'OBS Cross-Class'

            ELSE 'Other Cross-Class'
        END AS patient_class_relationship,

        CASE
            WHEN NULLIF(TRIM(e.servicefacility), '') IS NOT NULL
             AND
             (
                 UPPER(TRIM(e.servicefacility)) =
                 UPPER(TRIM(a.event_location))

                 OR

                 UPPER(TRIM(e.servicefacility)) =
                 UPPER(TRIM(a.sending_source))
             )
                THEN 1
            ELSE 0
        END AS facility_match_flag,

        DATEDIFF
        (
            day,
            e.event_discharge_date,
            CAST(a.insert_timestamp AS DATE)
        ) AS notification_lag_days,

        DATEDIFF
        (
            minute,
            a.message_timestamp,
            a.insert_timestamp
        ) AS pipeline_lag_minutes,

        COUNT(*) OVER
        (
            PARTITION BY e.claim_event_id
        ) AS date_only_discharge_candidate_count,

        ROW_NUMBER() OVER
        (
            PARTITION BY e.claim_event_id

            ORDER BY
                ABS
                (
                    DATEDIFF
                    (
                        day,
                        e.event_discharge_date,
                        a.discharge_match_date
                    )
                ),

                CASE
                    WHEN NULLIF(TRIM(e.servicefacility), '') IS NOT NULL
                     AND
                     (
                         UPPER(TRIM(e.servicefacility)) =
                         UPPER(TRIM(a.event_location))

                         OR

                         UPPER(TRIM(e.servicefacility)) =
                         UPPER(TRIM(a.sending_source))
                     )
                        THEN 0
                    ELSE 1
                END,

                /* Prefer true A03 discharge messages. */
                CASE
                    WHEN a.message_type = 'A03' THEN 0
                    WHEN a.message_type = 'A08' THEN 1
                    WHEN a.source_category = 'AUTHORIZATION' THEN 2
                    ELSE 3
                END,

                a.insert_timestamp ASC NULLS LAST,
                a.message_timestamp ASC NULLS LAST,
                a.adt_message_key
        ) AS date_only_rank

    FROM tmp_events_2025 e

    INNER JOIN adt_events_candidate a
        ON e.person_id = a.personid
       AND a.discharge_candidate_flag = 1
       AND a.discharge_match_date IS NOT NULL

       /* Intentionally no patient-class condition here. */

    CROSS JOIN tmp_params p

    WHERE ABS
    (
        DATEDIFF
        (
            day,
            e.event_discharge_date,
            a.discharge_match_date
        )
    ) <= p.fuzzy_days
)

SELECT
    claim_event_id,

    1 AS date_only_discharge_evidence_flag,

    date_only_discharge_candidate_count,

    adt_message_key,
    adt_message_id,
    native_adt_event_id,

    source_category
        AS date_only_discharge_source_category,

    sending_source
        AS date_only_discharge_sending_source,

    adt_patient_class
        AS date_only_discharge_patient_class,

    adt_event_location
        AS date_only_discharge_event_location,

    message_type
        AS date_only_discharge_message_type,

    adt_discharge_date
        AS date_only_discharge_adt_date,

    discharge_match_date_inferred_flag
        AS date_only_discharge_date_inferred_flag,

    message_timestamp
        AS date_only_discharge_message_timestamp,

    insert_timestamp
        AS date_only_discharge_insert_timestamp,

    discharge_date_difference
        AS date_only_discharge_date_difference,

    patient_class_relationship
        AS date_only_discharge_class_relationship,

    facility_match_flag
        AS date_only_discharge_facility_match_flag,

    notification_lag_days
        AS date_only_discharge_lag_days,

    pipeline_lag_minutes
        AS date_only_discharge_pipeline_lag_minutes,

    CASE
        WHEN discharge_date_difference = 0
            THEN 'Exact Discharge Date'
        ELSE 'Fuzzy Discharge Date'
    END AS date_only_discharge_match_method

FROM date_candidates

WHERE date_only_rank = 1;
```

---

# Change 4: join the evidence into the final reconciliation table

## Where to change it

In the final creation of:

```sql
analytics.notification_reconciliation_2025
```

add two joins:

```sql
LEFT JOIN tmp_admission_date_only_evidence dao
    ON e.claim_event_id = dao.claim_event_id

LEFT JOIN tmp_discharge_date_only_evidence ddo
    ON e.claim_event_id = ddo.claim_event_id
```

Add these fields to the final `SELECT`.

## Admission date-only fields

```sql
COALESCE(
    dao.date_only_admission_evidence_flag,
    0
) AS date_only_admission_evidence_flag,

dao.date_only_admission_candidate_count,

dao.date_only_admission_source_category,
dao.date_only_admission_sending_source,
dao.date_only_admission_patient_class,
dao.date_only_admission_event_location,
dao.date_only_admission_message_type,
dao.date_only_admission_adt_date,
dao.date_only_admission_message_timestamp,
dao.date_only_admission_insert_timestamp,
dao.date_only_admission_date_difference,
dao.date_only_admission_class_relationship,
dao.date_only_admission_facility_match_flag,
dao.date_only_admission_lag_days,
dao.date_only_admission_pipeline_lag_minutes,
dao.date_only_admission_match_method,
```

## Discharge date-only fields

```sql
COALESCE(
    ddo.date_only_discharge_evidence_flag,
    0
) AS date_only_discharge_evidence_flag,

ddo.date_only_discharge_candidate_count,

ddo.date_only_discharge_source_category,
ddo.date_only_discharge_sending_source,
ddo.date_only_discharge_patient_class,
ddo.date_only_discharge_event_location,
ddo.date_only_discharge_message_type,
ddo.date_only_discharge_adt_date,
ddo.date_only_discharge_date_inferred_flag,
ddo.date_only_discharge_message_timestamp,
ddo.date_only_discharge_insert_timestamp,
ddo.date_only_discharge_date_difference,
ddo.date_only_discharge_class_relationship,
ddo.date_only_discharge_facility_match_flag,
ddo.date_only_discharge_lag_days,
ddo.date_only_discharge_pipeline_lag_minutes,
ddo.date_only_discharge_match_method,
```

---

# Change 5: add a date-only completeness status

Add this to the final table:

```sql
CASE
    WHEN COALESCE(
             dao.date_only_admission_evidence_flag,
             0
         ) = 1

     AND COALESCE(
             ddo.date_only_discharge_evidence_flag,
             0
         ) = 1
        THEN 'Admission and Discharge Date Evidence'

    WHEN COALESCE(
             dao.date_only_admission_evidence_flag,
             0
         ) = 1
        THEN 'Admission Date Evidence Only'

    WHEN COALESCE(
             ddo.date_only_discharge_evidence_flag,
             0
         ) = 1
        THEN 'Discharge Date Evidence Only'

    ELSE 'No Matched Date Evidence'
END AS date_only_completeness_status
```

---

# Change 6: explain the difference between confirmed and date-only coverage

Add a classification-gap field:

```sql
CASE
    WHEN COALESCE(a.admission_notification_flag, 0) = 1
     AND COALESCE(a.discharge_notification_flag, 0) = 1
        THEN 'Confirmed Admission and Discharge'

    WHEN COALESCE(a.admission_notification_flag, 0) = 0
     AND COALESCE(dao.date_only_admission_evidence_flag, 0) = 1
     AND COALESCE(a.discharge_notification_flag, 0) = 0
     AND COALESCE(ddo.date_only_discharge_evidence_flag, 0) = 1
        THEN 'Both Found Only When Patient Class Ignored'

    WHEN COALESCE(a.admission_notification_flag, 0) = 0
     AND COALESCE(dao.date_only_admission_evidence_flag, 0) = 1
        THEN 'Admission Found Only When Patient Class Ignored'

    WHEN COALESCE(a.discharge_notification_flag, 0) = 0
     AND COALESCE(ddo.date_only_discharge_evidence_flag, 0) = 1
        THEN 'Discharge Found Only When Patient Class Ignored'

    WHEN COALESCE(dao.date_only_admission_evidence_flag, 0) = 0
     AND COALESCE(ddo.date_only_discharge_evidence_flag, 0) = 0
        THEN 'No Matched Notification After Date-Only Search'

    ELSE 'Partially Confirmed'
END AS classification_gap_status
```

This field directly answers leadership’s question.

---

# Change 7: update “No Matched Notification”

Your current field is based on the approved patient-class-aware method:

```sql
no_notification_flag
```

Rename or reinterpret it as:

```sql
no_confirmed_match_flag
```

Add a second flag:

```sql
CASE
    WHEN COALESCE(
             dao.date_only_admission_evidence_flag,
             0
         ) = 0

     AND COALESCE(
             ddo.date_only_discharge_evidence_flag,
             0
         ) = 0
        THEN 1
    ELSE 0
END AS no_date_only_evidence_flag
```

Then you can report:

| Metric | Meaning |
|---|---|
| `no_confirmed_match_flag` | No match met the approved patient-class rules |
| `no_date_only_evidence_flag` | No qualifying admission or discharge evidence even when class was ignored |

The second is much stronger evidence of a true notification gap.

---

# Change 8: update the coverage gap aggregate table

In:

```sql
analytics.notification_coverage_gaps_2025
```

add:

```sql
SUM(date_only_admission_evidence_flag)
    AS events_with_date_only_admission_evidence,

ROUND
(
    100.0
    * SUM(date_only_admission_evidence_flag)
    / NULLIF(COUNT(*), 0),
    2
) AS date_only_admission_coverage_pct,

SUM(date_only_discharge_evidence_flag)
    AS events_with_date_only_discharge_evidence,

ROUND
(
    100.0
    * SUM(date_only_discharge_evidence_flag)
    / NULLIF(COUNT(*), 0),
    2
) AS date_only_discharge_coverage_pct,

SUM
(
    CASE
        WHEN date_only_admission_evidence_flag = 1
         AND date_only_discharge_evidence_flag = 1
            THEN 1
        ELSE 0
    END
) AS events_with_complete_date_only_evidence,

SUM(no_date_only_evidence_flag)
    AS events_with_no_date_only_evidence,

SUM
(
    CASE
        WHEN classification_gap_status LIKE
             '%Patient Class Ignored%'
            THEN 1
        ELSE 0
    END
) AS events_recovered_when_class_ignored
```

---

# Change 9: leadership summary query

```sql
SELECT
    care_type,

    COUNT(*) AS total_events,

    SUM(admission_notification_flag)
        AS confirmed_admission_matches,

    SUM(date_only_admission_evidence_flag)
        AS admission_date_evidence,

    SUM(discharge_notification_flag)
        AS confirmed_discharge_matches,

    SUM(date_only_discharge_evidence_flag)
        AS discharge_date_evidence,

    SUM(complete_admission_discharge_flag)
        AS confirmed_complete_matches,

    SUM
    (
        CASE
            WHEN date_only_admission_evidence_flag = 1
             AND date_only_discharge_evidence_flag = 1
                THEN 1
            ELSE 0
        END
    ) AS complete_date_evidence,

    SUM(no_confirmed_match_flag)
        AS no_confirmed_match,

    SUM(no_date_only_evidence_flag)
        AS no_date_only_evidence,

    SUM
    (
        CASE
            WHEN classification_gap_status LIKE
                 '%Patient Class Ignored%'
                THEN 1
            ELSE 0
        END
    ) AS recovered_when_class_ignored

FROM analytics.notification_reconciliation_2025

GROUP BY care_type

ORDER BY care_type;
```

---

# Change 10: add a confidence label

Date-only evidence is more ambiguous when:

- Multiple claims events occur for the same person on the same date
- Several ADT messages qualify
- Facility does not agree
- Patient class conflicts

Add:

```sql
CASE
    WHEN date_only_admission_candidate_count = 1
     AND date_only_admission_facility_match_flag = 1
        THEN 'High'

    WHEN date_only_admission_candidate_count <= 2
        THEN 'Medium'

    ELSE 'Low'
END AS date_only_admission_confidence
```

And similarly:

```sql
CASE
    WHEN date_only_discharge_candidate_count = 1
     AND date_only_discharge_facility_match_flag = 1
        THEN 'High'

    WHEN date_only_discharge_candidate_count <= 2
        THEN 'Medium'

    ELSE 'Low'
END AS date_only_discharge_confidence
```

This is especially important for same-day ED, OBS, and IP events.

---

# How `selected_match_flag` should work after this change

Keep:

```sql
selected_match_flag = 1
```

for the **approved class-aware selected match**.

Do not use it for the date-only layer.

Use separate fields:

```text
selected_match_flag                 Approved confirmed match
date_only_admission_evidence_flag   Admission evidence regardless of class
date_only_discharge_evidence_flag   Discharge evidence regardless of class
```

That prevents the looser date-only method from being mistaken for the primary validated result.

---

## Recommended leadership language

> Confirmed coverage requires the approved relationship between claims care setting and ADT patient class. A secondary date-only analysis identifies additional notifications occurring for the same member and event date regardless of patient class or bed type. The difference between the two represents potential status conversions, classification inconsistencies, or ambiguous same-day encounters, rather than automatically confirmed matches.
>
# DQA
## One correction before finalizing the QA

Because you now allow OBS claims to match ADT patient classes `ED` and `IP`, update `patient_class_match_status`. In the original script, every non-exact match is labeled as `Missing Patient Class Fallback`, even when the ADT class is actually ED or IP.

Use this in both admission and discharge candidate sections:

```sql
CASE
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

Also update the ranking penalty in both sections:

```sql
CASE
    WHEN a.patient_class = e.care_type THEN 0
    WHEN a.patient_class IS NULL THEN 1
    WHEN e.care_type = 'OBS'
     AND a.patient_class IN ('ED', 'IP') THEN 2
    ELSE 3
END AS patient_class_penalty
```

Use the same `CASE` in both `ROW_NUMBER()` orderings. This ensures exact OBS matches are preferred, followed by missing class, and then ED/IP fallback.

---

# QA query: validate “No Matched Notification”

This query examines events classified as having neither an admission nor discharge match and determines whether raw ADT evidence exists nearby.

It categorizes each unmatched event as:

1. No ADT activity within seven days
2. An approved candidate exists but lost during ranking/message assignment
3. An ADT candidate was blocked by patient-class rules
4. A candidate exists outside the ±1-day production tolerance
5. ADT activity exists, but no qualifying admission/discharge message exists

```sql
/*==============================================================================
  QA: VALIDATE EVENTS LABELED "NO MATCHED NOTIFICATION"

  Production tolerance: +/- 1 day
  QA sensitivity window: +/- 7 days
==============================================================================*/

DROP TABLE IF EXISTS tmp_no_matched_notification_qa;

CREATE TEMP TABLE tmp_no_matched_notification_qa AS

WITH qa_params AS
(
    SELECT
        1::INTEGER AS production_tolerance_days,
        7::INTEGER AS qa_tolerance_days
),

no_match_events AS
(
    SELECT
        claim_event_id,
        native_event_id,
        claims_source,

        person_id,
        memberno,

        care_type,
        event_admit_date,
        event_discharge_date,

        servicefacility,
        ipa_name,
        reporting_group

    FROM analytics.notification_reconciliation_2025

    WHERE admission_notification_flag = 0
      AND discharge_notification_flag = 0
),

adt_base AS
(
    SELECT
        adt.person_id,

        COALESCE
        (
            NULLIF(
                TRIM(
                    CAST(adt.adt_message_id AS VARCHAR(200))
                ),
                ''
            ),

            'SYNTHETIC|'
            || COALESCE(CAST(adt.person_id AS VARCHAR(100)), 'NULL')
            || '|'
            || COALESCE(CAST(adt.event_id AS VARCHAR(100)), 'NULL')
            || '|'
            || COALESCE(UPPER(TRIM(adt.message_type)), 'NULL')
            || '|'
            || COALESCE(CAST(adt.message_timestamp AS VARCHAR(100)), 'NULL')
        ) AS adt_message_key,

        adt.adt_message_id,
        adt.event_id AS native_adt_event_id,

        CAST(adt.admit_date AS DATE) AS adt_admit_date,
        CAST(adt.discharge_date AS DATE) AS adt_discharge_date,

        CASE
            WHEN adt.discharge_date IS NOT NULL
                THEN CAST(adt.discharge_date AS DATE)

            WHEN UPPER(TRIM(adt.message_type)) = 'A03'
             AND adt.message_timestamp IS NOT NULL
                THEN CAST(adt.message_timestamp AS DATE)

            ELSE NULL
        END AS discharge_match_date,

        CAST(adt.message_timestamp AS DATE) AS message_date,
        adt.message_timestamp,
        adt.insert_timestamp,

        UPPER(TRIM(adt.message_type)) AS message_type,

        CASE
            WHEN UPPER(TRIM(adt.patient_class))
                 IN ('IP', 'ED', 'OBS')
                THEN UPPER(TRIM(adt.patient_class))

            ELSE NULL
        END AS adt_patient_class,

        adt.sending_source,
        adt.event_location,
        adt.service_type,

        CASE
            WHEN UPPER(TRIM(adt.message_type))
                 IN ('A01', 'A04', 'A05', 'A06')
                THEN 1

            WHEN UPPER(TRIM(COALESCE(adt.sending_source, '')))
                 LIKE '%MEDHOK%'
             AND adt.admit_date IS NOT NULL
                THEN 1

            ELSE 0
        END AS admission_candidate_flag,

        CASE
            WHEN UPPER(TRIM(adt.message_type)) = 'A03'
                THEN 1

            WHEN UPPER(TRIM(adt.message_type)) = 'A08'
             AND adt.discharge_date IS NOT NULL
                THEN 1

            WHEN UPPER(TRIM(COALESCE(adt.sending_source, '')))
                 LIKE '%MEDHOK%'
             AND adt.discharge_date IS NOT NULL
                THEN 1

            ELSE 0
        END AS discharge_candidate_flag

    FROM qdwwh.dbo_adt adt

    WHERE EXISTS
    (
        SELECT 1
        FROM no_match_events n
        WHERE n.person_id = adt.person_id
    )
),

nearby_adt AS
(
    SELECT
        n.*,

        a.adt_message_key,
        a.adt_message_id,
        a.native_adt_event_id,

        a.adt_admit_date,
        a.adt_discharge_date,
        a.discharge_match_date,
        a.message_date,

        a.message_timestamp,
        a.insert_timestamp,

        a.message_type,
        a.adt_patient_class,

        a.sending_source,
        a.event_location,
        a.service_type,

        a.admission_candidate_flag,
        a.discharge_candidate_flag,

        ABS(
            DATEDIFF(
                day,
                n.event_admit_date,
                a.adt_admit_date
            )
        ) AS admission_date_difference,

        ABS(
            DATEDIFF(
                day,
                n.event_discharge_date,
                a.discharge_match_date
            )
        ) AS discharge_date_difference,

        CASE
            WHEN a.adt_patient_class = n.care_type
                THEN 1

            WHEN a.adt_patient_class IS NULL
                THEN 1

            WHEN n.care_type = 'OBS'
             AND a.adt_patient_class IN ('ED', 'IP')
                THEN 1

            ELSE 0
        END AS approved_patient_class_flag

    FROM no_match_events n

    LEFT JOIN adt_base a
        ON n.person_id = a.person_id

       AND
       (
              ABS(
                  DATEDIFF(
                      day,
                      n.event_admit_date,
                      a.adt_admit_date
                  )
              ) <= 7

           OR ABS(
                  DATEDIFF(
                      day,
                      n.event_discharge_date,
                      a.discharge_match_date
                  )
              ) <= 7

           OR ABS(
                  DATEDIFF(
                      day,
                      n.event_admit_date,
                      a.message_date
                  )
              ) <= 7

           OR ABS(
                  DATEDIFF(
                      day,
                      n.event_discharge_date,
                      a.message_date
                  )
              ) <= 7
       )
),

event_qa AS
(
    SELECT
        claim_event_id,
        native_event_id,
        claims_source,

        person_id,
        memberno,

        care_type,
        event_admit_date,
        event_discharge_date,

        servicefacility,
        ipa_name,
        reporting_group,

        COUNT(adt_message_key)
            AS nearby_adt_rows_within_7_days,

        MAX
        (
            CASE
                WHEN admission_candidate_flag = 1
                 AND admission_date_difference <= 1
                    THEN 1
                ELSE 0
            END
        ) AS raw_admission_candidate_within_1_day,

        MAX
        (
            CASE
                WHEN admission_candidate_flag = 1
                 AND admission_date_difference <= 1
                 AND approved_patient_class_flag = 1
                    THEN 1
                ELSE 0
            END
        ) AS approved_admission_candidate_within_1_day,

        MAX
        (
            CASE
                WHEN discharge_candidate_flag = 1
                 AND discharge_date_difference <= 1
                    THEN 1
                ELSE 0
            END
        ) AS raw_discharge_candidate_within_1_day,

        MAX
        (
            CASE
                WHEN discharge_candidate_flag = 1
                 AND discharge_date_difference <= 1
                 AND approved_patient_class_flag = 1
                    THEN 1
                ELSE 0
            END
        ) AS approved_discharge_candidate_within_1_day,

        MAX
        (
            CASE
                WHEN admission_candidate_flag = 1
                 AND admission_date_difference <= 7
                 AND approved_patient_class_flag = 1
                    THEN 1

                WHEN discharge_candidate_flag = 1
                 AND discharge_date_difference <= 7
                 AND approved_patient_class_flag = 1
                    THEN 1

                ELSE 0
            END
        ) AS approved_candidate_within_7_days,

        MAX
        (
            CASE
                WHEN admission_candidate_flag = 1
                 AND admission_date_difference <= 1
                 AND approved_patient_class_flag = 0
                    THEN 1

                WHEN discharge_candidate_flag = 1
                 AND discharge_date_difference <= 1
                 AND approved_patient_class_flag = 0
                    THEN 1

                ELSE 0
            END
        ) AS candidate_blocked_by_patient_class_flag

    FROM nearby_adt

    GROUP BY
        claim_event_id,
        native_event_id,
        claims_source,

        person_id,
        memberno,

        care_type,
        event_admit_date,
        event_discharge_date,

        servicefacility,
        ipa_name,
        reporting_group
)

SELECT
    *,

    CASE
        WHEN nearby_adt_rows_within_7_days = 0
            THEN '1 - No ADT activity within +/-7 days'

        WHEN approved_admission_candidate_within_1_day = 1
          OR approved_discharge_candidate_within_1_day = 1
            THEN '2 - Approved candidate exists; inspect ranking or message reuse'

        WHEN candidate_blocked_by_patient_class_flag = 1
            THEN '3 - Candidate blocked by patient-class rule'

        WHEN approved_candidate_within_7_days = 1
            THEN '4 - Candidate outside production +/-1-day tolerance'

        ELSE '5 - Nearby ADT exists, but no qualifying admission/discharge message'
    END AS no_match_qa_reason

FROM event_qa;
```

## Summarize the QA results

```sql
SELECT
    care_type,
    no_match_qa_reason,

    COUNT(*) AS event_count,

    ROUND
    (
        100.0
        * COUNT(*)
        / SUM(COUNT(*)) OVER (PARTITION BY care_type),
        2
    ) AS percent_of_no_match_events

FROM tmp_no_matched_notification_qa

GROUP BY
    care_type,
    no_match_qa_reason

ORDER BY
    care_type,
    no_match_qa_reason;
```

## Review events that should potentially have matched

```sql
SELECT *
FROM tmp_no_matched_notification_qa

WHERE no_match_qa_reason IN
(
    '2 - Approved candidate exists; inspect ranking or message reuse',
    '3 - Candidate blocked by patient-class rule',
    '4 - Candidate outside production +/-1-day tolerance'
)

ORDER BY
    care_type,
    reporting_group,
    ipa_name,
    event_admit_date;
```

### How to interpret the QA

| QA reason                         | Interpretation                                                           |
| --------------------------------- | ------------------------------------------------------------------------ |
| No ADT activity within ±7 days    | Strongest evidence of a true coverage gap                                |
| Approved candidate exists         | Potential ranking conflict or same ADT message assigned to another claim |
| Blocked by patient class          | Matching-policy exclusion rather than definite absence                   |
| Outside ±1-day tolerance          | Potential date discrepancy or delayed data                               |
| Nearby ADT but no qualifying role | ADT exists, but not as an eligible admission/discharge message           |

---

# Purpose of `selected_match_flag`

`selected_match_flag` indicates that the row survived both ranking rules:

```sql
claim_source_rank = 1
AND message_event_rank = 1
```

Conceptually:

* `claim_source_rank = 1`: best message for that claim, role, and source/feed
* `message_event_rank = 1`: that ADT message was assigned to its best claims event
* `selected_match_flag = 1`: final accepted match

## In the current table, it is redundant

`analytics.notification_match_detail_2025` contains only selected rows. Therefore:

```sql
selected_match_flag = 1
```

for every row.

You have two options:

### Keep it

Keep it as lineage metadata showing that the detail table contains accepted matches only.

### Remove it

Remove it because it does not distinguish rows within the current table.

The flag becomes useful only if you persist all candidates in a separate audit table:

```text
analytics.notification_match_candidate_2025
```

In that table:

* `1` = selected match
* `0` = candidate considered but rejected

**Recommendation:** retain it for now, but document it as an audit field. It does not need to be used as a report filter because all rows already equal 1.

---

# Description of the three analytics tables

## 1. `analytics.notification_match_detail_2025`

**Purpose:** Stores the selected ADT, authorization, or EMR matches supporting each claims event.

**Grain:**

> One row per claim event, notification target, and source/feed.

A claims event may have several rows:

```text
Claim event + admission + source A
Claim event + admission + source B
Claim event + discharge + source A
```

**Primary uses:**

* Source-level QA
* Patient-class QA
* Facility comparison
* Message-level timing
* Reviewing exact and fallback matches
* Determining which source notified first
* Investigating individual events

---

## 2. `analytics.notification_reconciliation_2025`

**Purpose:** Provides the primary event-level analytical and reporting dataset.

**Grain:**

> One row per eligible IP, ED, or OBS claims event.

**Primary uses:**

* Coverage reporting
* Executive summaries
* Dashboard development
* IPA and facility comparisons
* Admission versus discharge analysis
* Timeliness categories
* Identification of no-matched-notification events

This is the recommended primary dashboard table.

---

## 3. `analytics.notification_coverage_gaps_2025`

**Purpose:** Provides a pre-aggregated view of notification coverage and gaps.

**Grain:**

> One row per reporting group, IPA, claims facility, and care type.

**Primary uses:**

* Leadership summaries
* Facility and IPA performance comparisons
* Identification of ADT coverage deserts
* Same-day notification monitoring
* Source-category counts
* Negative-lag QA

Because this table is aggregated, it should not be used for member- or event-level investigation.

---

# SQL pipeline/table dictionary

| Table                              | Grain                                             | Purpose                                                                    |
| ---------------------------------- | ------------------------------------------------- | -------------------------------------------------------------------------- |
| `tmp_params`                       | One row                                           | Stores analysis dates and fuzzy-match tolerance.                           |
| `ip_raw`                           | One row per eligible inpatient event              | Filters inpatient events to CareAllies enrollment and 2025 discharges.     |
| `ed_obs_raw`                       | One row per eligible ED/OBS event                 | Filters outpatient events to ED/OBS and applicable enrollment.             |
| `tmp_events_2025`                  | One row per claims event                          | Unified claims denominator for IP, ED, and OBS.                            |
| `tmp_event_date_window`            | One row                                           | Derives ADT candidate extraction boundaries from claims dates.             |
| `adt_events_candidate`             | One row per deduplicated ADT message              | Classifies source, message role, patient class, and candidate eligibility. |
| `tmp_admission_match_candidates`   | One row per potential admission match             | Scores and ranks potential admission matches.                              |
| `tmp_admission_matches`            | One row per selected claim/source admission match | Retains accepted admission matches.                                        |
| `tmp_discharge_match_candidates`   | One row per potential discharge match             | Scores and ranks potential discharge matches.                              |
| `tmp_discharge_matches`            | One row per selected claim/source discharge match | Retains accepted discharge matches.                                        |
| `notification_match_detail_2025`   | Claim event × target × source/feed                | Persistent selected-match detail.                                          |
| `tmp_event_notification_counts`    | One row per claim event                           | Aggregates source and coverage flags.                                      |
| `tmp_event_*_source_list`          | One row per claim event                           | Produces source-category lists.                                            |
| `tmp_event_notification_agg`       | One row per claim event                           | Combines flags, counts, and source lists.                                  |
| `tmp_first_admission_notification` | One row per matched event                         | Selects earliest admission notification.                                   |
| `tmp_first_discharge_notification` | One row per matched event                         | Selects earliest discharge notification.                                   |
| `notification_reconciliation_2025` | One row per claim event                           | Persistent event-level reporting dataset.                                  |
| `notification_coverage_gaps_2025`  | Group × IPA × facility × care type                | Persistent aggregate coverage summary.                                     |

---

# Data dictionary: `analytics.notification_match_detail_2025`

Logical types are shown; physical types are inherited from the source fields.

## Claims event fields

| Column                  | Logical type | Definition                                                                     |
| ----------------------- | ------------ | ------------------------------------------------------------------------------ |
| `claim_event_id`        | VARCHAR      | Unique source-prefixed event key, such as `IP-123`, `ED-456`, or `OBS-789`.    |
| `native_event_id`       | ID           | Original event ID from the claims source table.                                |
| `claims_source`         | VARCHAR      | Originating claims table: inpatient or outpatient events.                      |
| `person_id`             | ID           | Internal person identifier used for claims-to-ADT matching.                    |
| `memberno`              | VARCHAR      | Cleaned member identifier used for traceability and QA.                        |
| `care_type`             | VARCHAR      | Claims care setting: `IP`, `ED`, or `OBS`.                                     |
| `care_subtype`          | VARCHAR      | Detailed claim classification, such as Acute, SNF, LTAC, BH Acute, ED, or OBS. |
| `event_admit_date`      | DATE         | Claims admission, ED start, or observation start date.                         |
| `event_discharge_date`  | DATE         | Claims discharge or event end date.                                            |
| `claim_servicefacility` | VARCHAR      | Service facility recorded on the claims event.                                 |
| `ipa_name`              | VARCHAR      | IPA attributed to the member at the event date.                                |
| `reporting_group`       | VARCHAR      | Higher-level CareAllies reporting group.                                       |

## ADT identification and source fields

| Column                | Logical type | Definition                                                                          |
| --------------------- | ------------ | ----------------------------------------------------------------------------------- |
| `adt_message_key`     | VARCHAR      | Unique ADT message identifier; synthetic key is created when message ID is missing. |
| `adt_message_id`      | ID           | Original ADT message ID.                                                            |
| `native_adt_event_id` | ID           | Original ADT event or encounter identifier.                                         |
| `source_category`     | VARCHAR      | Normalized category: HIE_ADT, AUTHORIZATION, EMR, OTHER, or UNCLASSIFIED.           |
| `sending_source`      | VARCHAR      | Raw hospital, HIE, vendor, or source name.                                          |
| `adt_patient_class`   | VARCHAR      | Cleaned ADT patient class: IP, ED, OBS, or null.                                    |
| `adt_event_location`  | VARCHAR      | Location or facility from the ADT message.                                          |
| `adt_city`            | VARCHAR      | City from the ADT record.                                                           |
| `adt_state`           | VARCHAR      | State from the ADT record.                                                          |
| `adt_zip`             | VARCHAR      | ZIP code from the ADT record.                                                       |

## Clinical/message fields

| Column                  | Logical type | Definition                                                                |
| ----------------------- | ------------ | ------------------------------------------------------------------------- |
| `message_type`          | VARCHAR      | ADT message type, such as A01, A03, A04, A06, or A08.                     |
| `notification_role`     | VARCHAR      | Message classification: admission, discharge, transfer, update, or other. |
| `admit_type`            | VARCHAR      | Admission type supplied by the ADT record.                                |
| `admit_source`          | VARCHAR      | Admission source supplied by the ADT record.                              |
| `discharge_disposition` | VARCHAR      | Discharge disposition from the selected discharge message.                |
| `discharge_location`    | VARCHAR      | Discharge location from the selected discharge message.                   |
| `service_type`          | VARCHAR      | Service or encounter type supplied by ADT.                                |

## Matching and timing fields

| Column                         | Logical type | Definition                                                                                  |
| ------------------------------ | ------------ | ------------------------------------------------------------------------------------------- |
| `adt_match_date`               | DATE         | ADT date matched to the claims admission or discharge date.                                 |
| `adt_match_date_inferred_flag` | INTEGER      | `1` when an A03 message date was used because ADT discharge date was missing.               |
| `message_timestamp`            | TIMESTAMP    | Timestamp generated or carried by the source message.                                       |
| `insert_timestamp`             | TIMESTAMP    | Timestamp when the record became available internally.                                      |
| `match_target`                 | VARCHAR      | Indicates whether the row supports `ADMISSION` or `DISCHARGE`.                              |
| `match_method`                 | VARCHAR      | Exact-date or fuzzy-date matching method.                                                   |
| `date_difference`              | INTEGER      | Signed calendar-day difference between claims and ADT match dates.                          |
| `patient_class_match_status`   | VARCHAR      | Exact class, missing-class fallback, or OBS cross-class fallback.                           |
| `facility_match_flag`          | INTEGER      | `1` when claims facility matches ADT location or sending source using current string logic. |
| `notification_lag_days`        | INTEGER      | Insert date minus claims event target date, in calendar days.                               |
| `pipeline_lag_minutes`         | INTEGER      | Insert timestamp minus message timestamp, in minutes.                                       |
| `source_feed_key`              | VARCHAR      | Normalized feed key based on sending source, with source category as fallback.              |
| `selected_match_flag`          | INTEGER      | `1` when the candidate survived claim/source and message/event ranking.                     |

---

# Data dictionary: `analytics.notification_reconciliation_2025`

## Event and enrollment fields

| Column                         | Definition                                                             |
| ------------------------------ | ---------------------------------------------------------------------- |
| `claim_event_id`               | Unique source-prefixed claims event key.                               |
| `native_event_id`              | Original claims event ID.                                              |
| `claims_source`                | Claims source table.                                                   |
| `person_id`                    | Internal person identifier.                                            |
| `memberno`                     | Cleaned member number.                                                 |
| `care_type`                    | IP, ED, or OBS.                                                        |
| `care_subtype`                 | Detailed care classification.                                          |
| `rptgrouper`                   | Original claims reporting grouper.                                     |
| `event_admit_date`             | Claims event start/admission date.                                     |
| `event_discharge_date`         | Claims event discharge/end date.                                       |
| `length_of_stay_days`          | Calendar days between event start and discharge.                       |
| `paiddate`                     | Claims paid date.                                                      |
| `discharge_date_inferred_flag` | `1` when outpatient event date substitutes for missing discharge date. |
| `servicefacility`              | Claims service facility.                                               |
| `providernpi`                  | Claims provider NPI.                                                   |
| `providerspecialty`            | Claims provider specialty.                                             |
| `ipa_name`                     | IPA at the event date.                                                 |
| `reporting_group`              | CareAllies reporting group.                                            |
| `enrollment_start_date`        | Beginning of the enrollment record supporting eligibility.             |
| `enrollment_end_date`          | End of the enrollment record supporting eligibility.                   |

## Coverage flags

| Column                              | Definition                                                      |
| ----------------------------------- | --------------------------------------------------------------- |
| `admission_notification_flag`       | At least one selected admission match exists.                   |
| `discharge_notification_flag`       | At least one selected discharge match exists.                   |
| `complete_admission_discharge_flag` | Both admission and discharge matches exist.                     |
| `admission_hie_flag`                | Admission notification received from HIE_ADT.                   |
| `admission_authorization_flag`      | Admission notification received through authorization.          |
| `admission_emr_flag`                | Admission notification received from an EMR source.             |
| `admission_other_flag`              | Admission notification received from a source mapped to OTHER.  |
| `admission_unclassified_flag`       | Admission source could not be classified.                       |
| `discharge_hie_flag`                | Discharge notification received from HIE_ADT.                   |
| `discharge_authorization_flag`      | Discharge notification received through authorization.          |
| `discharge_emr_flag`                | Discharge notification received from an EMR source.             |
| `discharge_other_flag`              | Discharge notification received from a source mapped to OTHER.  |
| `discharge_unclassified_flag`       | Discharge source could not be classified.                       |
| `hie_adt_flag`                      | At least one admission or discharge HIE match exists.           |
| `authorization_flag`                | At least one admission or discharge authorization match exists. |
| `emr_flag`                          | At least one admission or discharge EMR match exists.           |
| `other_flag`                        | At least one admission or discharge OTHER-source match exists.  |
| `unclassified_flag`                 | At least one unclassified-source match exists.                  |

## Source counts and categories

| Column                             | Definition                                                                                                                              |
| ---------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `admission_source_count`           | Distinct admission feeds matched to the event.                                                                                          |
| `discharge_source_count`           | Distinct discharge feeds matched to the event.                                                                                          |
| `overall_source_count`             | Distinct feeds matched across admission and discharge.                                                                                  |
| `admission_sources_notified`       | Comma-separated admission source categories.                                                                                            |
| `discharge_sources_notified`       | Comma-separated discharge source categories.                                                                                            |
| `sources_notified`                 | Comma-separated source categories across both targets.                                                                                  |
| `admission_coverage_category`      | Business category describing admission-source coverage.                                                                                 |
| `discharge_coverage_category`      | Business category describing discharge-source coverage.                                                                                 |
| `notification_completeness_status` | Admission and Discharge Received, Admission Only, Discharge Only, or No Matched Notification.                                           |
| `no_notification_flag`             | Physical field name in the current script; `1` means no selected admission or discharge match. Business label: No Matched Notification. |
| `multi_source_flag`                | `1` when two or more distinct feeds matched the event.                                                                                  |

## Earliest admission fields

| Column                                 | Definition                                                               |
| -------------------------------------- | ------------------------------------------------------------------------ |
| `first_admission_source_category`      | Source category of the earliest selected admission notification.         |
| `first_admission_sending_source`       | Raw source name of the earliest admission notification.                  |
| `first_admission_event_location`       | ADT event location from the earliest admission notification.             |
| `first_admission_message_type`         | Message type of the earliest admission notification.                     |
| `first_admission_message_timestamp`    | Source message timestamp for the earliest admission notification.        |
| `first_admission_insert_timestamp`     | Internal availability timestamp for the earliest admission notification. |
| `admission_notification_lag_days`      | Insert date minus claims admission date.                                 |
| `admission_pipeline_lag_minutes`       | Insert timestamp minus message timestamp.                                |
| `admission_match_method`               | Exact or fuzzy admission matching method.                                |
| `admission_date_difference`            | ADT admission date minus claims admission date.                          |
| `admission_patient_class_match_status` | Patient-class relationship for the earliest admission notification.      |
| `admission_facility_match_flag`        | Facility agreement indicator for the earliest admission notification.    |

## Earliest discharge fields

| Column                                 | Definition                                                                    |
| -------------------------------------- | ----------------------------------------------------------------------------- |
| `first_discharge_source_category`      | Source category of the earliest selected discharge notification.              |
| `first_discharge_sending_source`       | Raw source name of the earliest discharge notification.                       |
| `first_discharge_event_location`       | ADT event location from the earliest discharge notification.                  |
| `first_discharge_message_type`         | Message type of the earliest discharge notification.                          |
| `first_discharge_message_timestamp`    | Source message timestamp for the earliest discharge notification.             |
| `first_discharge_insert_timestamp`     | Internal availability timestamp for the earliest discharge notification.      |
| `discharge_notification_lag_days`      | Insert date minus claims discharge date.                                      |
| `discharge_pipeline_lag_minutes`       | Insert timestamp minus message timestamp.                                     |
| `discharge_match_method`               | Exact or fuzzy discharge matching method.                                     |
| `discharge_date_difference`            | ADT discharge date minus claims discharge date.                               |
| `discharge_patient_class_match_status` | Patient-class relationship for the earliest discharge notification.           |
| `discharge_facility_match_flag`        | Facility agreement indicator for the earliest discharge notification.         |
| `adt_discharge_date_inferred_flag`     | `1` when A03 message date was used instead of a populated ADT discharge date. |

## Timing categories

| Column                          | Definition                                                                           |
| ------------------------------- | ------------------------------------------------------------------------------------ |
| `admission_timeliness_category` | Same Day, 1 Day, 2 Days, 3–7 Days, Over 7 Days, Timing Unavailable, or Negative Lag. |
| `discharge_timeliness_category` | Same timing categories applied to discharge notification lag.                        |

---

# Data dictionary: `analytics.notification_coverage_gaps_2025`

| Column                                | Definition                                                                                                                  |
| ------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `reporting_group`                     | CareAllies reporting group.                                                                                                 |
| `ipa_name`                            | IPA name.                                                                                                                   |
| `servicefacility`                     | Claims service facility.                                                                                                    |
| `care_type`                           | IP, ED, or OBS.                                                                                                             |
| `total_events`                        | Number of eligible claims events in the group.                                                                              |
| `events_with_admission_notification`  | Events with at least one selected admission match.                                                                          |
| `admission_notification_coverage_pct` | Percentage of events with admission coverage.                                                                               |
| `events_with_discharge_notification`  | Events with at least one selected discharge match.                                                                          |
| `discharge_notification_coverage_pct` | Percentage of events with discharge coverage.                                                                               |
| `events_with_complete_notifications`  | Events with both admission and discharge matches.                                                                           |
| `complete_notification_coverage_pct`  | Percentage with complete coverage.                                                                                          |
| `events_with_no_notification`         | Physical field name in the current script; count of events with no selected match. Business label: No Matched Notification. |
| `percent_no_notification`             | Percentage of events with no selected match.                                                                                |
| `hie_adt_events`                      | Events with at least one HIE_ADT match.                                                                                     |
| `authorization_events`                | Events with at least one authorization match.                                                                               |
| `emr_events`                          | Events with at least one EMR match.                                                                                         |
| `other_source_events`                 | Events with at least one OTHER-source match.                                                                                |
| `unclassified_source_events`          | Events with at least one unclassified-source match.                                                                         |
| `multi_source_events`                 | Events matched to at least two distinct feeds.                                                                              |
| `same_day_admission_notifications`    | Events whose earliest admission notification was inserted on the claims admission date.                                     |
| `same_day_discharge_notifications`    | Events whose earliest discharge notification was inserted on the claims discharge date.                                     |
| `negative_admission_lag_events`       | Events whose admission notification appears before the claims admission date.                                               |
| `negative_discharge_lag_events`       | Events whose discharge notification appears before the claims discharge date.                                               |

For schema consistency, the physical columns can eventually be renamed to:

```sql
no_matched_notification_flag
events_with_no_matched_notification
percent_no_matched_notification
```

The business logic does not change; the new names simply make the limitation explicit.
