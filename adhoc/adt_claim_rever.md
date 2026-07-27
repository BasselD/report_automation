Yes. It is a useful **secondary validation**, but it answers a different question:

* **Claims left join notifications:** Of known claims events, how many notifications did we receive?
* **ADT left join claims:** Of notifications received, how many can eventually be corroborated by a claim?

Do not replace the current claims-led analysis. Present the two together as **coverage** versus **notification yield**.

## Important design decision

Use one row per:

```text
ADT message × match target
```

For example, the same MedHOK record may support both an admission and discharge date. Those should be evaluated separately.

Also separate:

* Clinical ADT/HIE/EMR/other feeds
* MedHOK authorization

Combining them would make the clinical-feed result look better than it really is.

---

# Quick ADT-left-join-claims analysis

This assumes the following tables still exist in the same Redshift session:

```text
adt_events_candidate
tmp_events_2025
enr_2025
```

It also uses the persistent selected-match table:

```text
analytics.notification_match_detail_2025
```

## 1. Create one row per notification target

```sql
/*==============================================================================
  ADT-FIRST RECONCILIATION

  Grain:
      One row per ADT message × notification target

  Purpose:
      Determine whether each received admission/discharge notification
      can be linked to an eligible claims event.
==============================================================================*/

DROP TABLE IF EXISTS tmp_adt_notification_spine;

CREATE TEMP TABLE tmp_adt_notification_spine AS

/* Admission notification instances */
SELECT
    a.adt_message_key
        || '|ADMISSION' AS notification_instance_id,

    a.adt_message_key,
    a.adt_message_id,
    a.native_adt_event_id,

    a.personid,
    a.source_category,
    a.sending_source,
    a.patient_class AS adt_patient_class,

    a.event_location,
    a.message_type,
    a.notification_role,

    'ADMISSION' AS match_target,
    a.admit_date AS notification_event_date,

    a.message_timestamp,
    a.insert_timestamp,

    a.admit_date,
    a.discharge_match_date

FROM adt_events_candidate a

WHERE a.admission_candidate_flag = 1
  AND a.admit_date >= DATE '2025-01-01'
  AND a.admit_date <  DATE '2026-01-01'


UNION ALL


/* Discharge notification instances */
SELECT
    a.adt_message_key
        || '|DISCHARGE' AS notification_instance_id,

    a.adt_message_key,
    a.adt_message_id,
    a.native_adt_event_id,

    a.personid,
    a.source_category,
    a.sending_source,
    a.patient_class AS adt_patient_class,

    a.event_location,
    a.message_type,
    a.notification_role,

    'DISCHARGE' AS match_target,
    a.discharge_match_date AS notification_event_date,

    a.message_timestamp,
    a.insert_timestamp,

    a.admit_date,
    a.discharge_match_date

FROM adt_events_candidate a

WHERE a.discharge_candidate_flag = 1
  AND a.discharge_match_date >= DATE '2025-01-01'
  AND a.discharge_match_date <  DATE '2026-01-01';
```

---

## 2. Restrict the ADT denominator to enrolled CareAllies members

Without this step, unmatched ADT counts may include people outside the CareAllies population.

```sql
DROP TABLE IF EXISTS tmp_eligible_adt_notification_spine;

CREATE TEMP TABLE tmp_eligible_adt_notification_spine AS

SELECT
    notification_instance_id,
    adt_message_key,
    adt_message_id,
    native_adt_event_id,

    personid,
    memberno,

    source_category,
    sending_source,
    adt_patient_class,

    event_location,
    message_type,
    notification_role,
    match_target,
    notification_event_date,

    message_timestamp,
    insert_timestamp,

    admit_date,
    discharge_match_date,

    ipa_name,
    reporting_group

FROM
(
    SELECT
        a.*,

        REPLACE(e.memberno, '*', '') AS memberno,
        e.ipa_name,
        e.reporting_group,

        ROW_NUMBER() OVER
        (
            PARTITION BY a.notification_instance_id

            ORDER BY
                CAST(e.startdate AS DATE) DESC,
                CAST(e.enddate AS DATE) DESC
        ) AS enrollment_rn

    FROM tmp_adt_notification_spine a

    INNER JOIN enr_2025 e
        ON a.personid = e.personid

       AND a.notification_event_date
           BETWEEN CAST(e.startdate AS DATE)
               AND CAST(e.enddate AS DATE)

    /*
      Add these filters here if enr_2025 is not already restricted:

      WHERE e.carealliesmanagedflag = 'Y'
        AND e.planpayer = 'Cigna MA'
    */
) eligible

WHERE enrollment_rn = 1;
```

---

## 3. Retrieve the claims event selected by the production pipeline

```sql
DROP TABLE IF EXISTS tmp_selected_claim_by_notification;

CREATE TEMP TABLE tmp_selected_claim_by_notification AS

SELECT
    adt_message_key,
    match_target,

    claim_event_id,
    native_event_id,
    claims_source,

    care_type,
    care_subtype,

    event_admit_date,
    event_discharge_date,

    claim_servicefacility,

    match_method,
    date_difference,

    patient_class_match_status,
    facility_match_flag,

    authorization_match_status,
    authorization_candidate_event_count,
    authorization_match_score

FROM
(
    SELECT
        d.*,

        ROW_NUMBER() OVER
        (
            PARTITION BY
                d.adt_message_key,
                d.match_target

            ORDER BY
                d.selected_match_flag DESC,
                ABS(d.date_difference),
                d.insert_timestamp ASC NULLS LAST,
                d.claim_event_id
        ) AS selected_rn

    FROM analytics.notification_match_detail_2025 d

    WHERE d.selected_match_flag = 1
) selected

WHERE selected_rn = 1;
```

---

## 4. Identify nearby claims that were not selected

This helps distinguish a true claims absence from a match that failed production criteria.

```sql
DROP TABLE IF EXISTS tmp_adt_claim_proximity;

CREATE TEMP TABLE tmp_adt_claim_proximity AS

SELECT
    a.notification_instance_id,

    MAX
    (
        CASE
            WHEN e.claim_event_id IS NOT NULL THEN 1
            ELSE 0
        END
    ) AS member_has_any_eligible_claim_flag,

    MAX
    (
        CASE
            WHEN e.claim_event_id IS NOT NULL

             AND ABS
                 (
                     DATEDIFF
                     (
                         day,

                         CASE
                             WHEN a.match_target = 'ADMISSION'
                                 THEN e.event_admit_date
                             ELSE e.event_discharge_date
                         END,

                         a.notification_event_date
                     )
                 ) <= 1

                THEN 1
            ELSE 0
        END
    ) AS nearby_claim_any_class_flag,

    MAX
    (
        CASE
            WHEN e.claim_event_id IS NOT NULL

             AND ABS
                 (
                     DATEDIFF
                     (
                         day,

                         CASE
                             WHEN a.match_target = 'ADMISSION'
                                 THEN e.event_admit_date
                             ELSE e.event_discharge_date
                         END,

                         a.notification_event_date
                     )
                 ) <= 1

             AND
             (
                    a.source_category = 'AUTHORIZATION'

                 OR a.adt_patient_class = e.care_type

                 OR a.adt_patient_class IS NULL

                 OR
                 (
                     e.care_type = 'OBS'
                     AND a.adt_patient_class IN ('ED', 'IP')
                 )
             )

                THEN 1
            ELSE 0
        END
    ) AS nearby_approved_claim_flag

FROM tmp_eligible_adt_notification_spine a

LEFT JOIN tmp_events_2025 e
    ON a.personid = e.person_id

GROUP BY
    a.notification_instance_id;
```

---

## 5. Create the ADT-left-join result

```sql
DROP TABLE IF EXISTS analytics.adt_to_claims_high_level_2025;

CREATE TABLE analytics.adt_to_claims_high_level_2025 AS

SELECT
    a.notification_instance_id,

    a.adt_message_key,
    a.adt_message_id,
    a.native_adt_event_id,

    a.personid,
    a.memberno,

    a.match_target,
    a.notification_event_date,

    a.source_category,
    a.sending_source,
    a.adt_patient_class,

    a.event_location,
    a.message_type,

    a.message_timestamp,
    a.insert_timestamp,

    a.ipa_name,
    a.reporting_group,

    m.claim_event_id,
    m.native_event_id AS claim_native_event_id,
    m.claims_source,

    m.care_type AS matched_claim_care_type,
    m.care_subtype AS matched_claim_care_subtype,

    m.event_admit_date AS claim_admit_date,
    m.event_discharge_date AS claim_discharge_date,

    m.claim_servicefacility,

    m.match_method,
    m.date_difference,

    m.patient_class_match_status,
    m.facility_match_flag,

    m.authorization_match_status,
    m.authorization_candidate_event_count,
    m.authorization_match_score,

    CASE
        WHEN m.claim_event_id IS NOT NULL THEN 1
        ELSE 0
    END AS claim_match_flag,

    p.member_has_any_eligible_claim_flag,
    p.nearby_claim_any_class_flag,
    p.nearby_approved_claim_flag,

    CASE
        WHEN m.claim_event_id IS NOT NULL
            THEN 'Matched to Claims'

        WHEN p.nearby_approved_claim_flag = 1
            THEN 'Nearby Approved Claim Not Selected'

        WHEN p.nearby_claim_any_class_flag = 1
            THEN 'Nearby Claim Failed Classification Rule'

        WHEN p.member_has_any_eligible_claim_flag = 1
            THEN 'Member Has Claims but No Nearby Event Date'

        ELSE 'No Eligible Claims Event'
    END AS adt_to_claim_status,

    CASE
        WHEN a.source_category = 'AUTHORIZATION'
            THEN 'MedHOK Authorization'

        ELSE 'Clinical ADT / Other Feed'
    END AS source_group

FROM tmp_eligible_adt_notification_spine a

LEFT JOIN tmp_selected_claim_by_notification m
    ON a.adt_message_key = m.adt_message_key
   AND a.match_target = m.match_target

LEFT JOIN tmp_adt_claim_proximity p
    ON a.notification_instance_id =
       p.notification_instance_id;
```

---

# High-level QA summaries

## 1. Overall clinical ADT versus MedHOK match rates

```sql
SELECT
    source_group,
    match_target,

    COUNT(*) AS notification_instances,

    SUM(claim_match_flag)
        AS notifications_matched_to_claims,

    COUNT(*) - SUM(claim_match_flag)
        AS notifications_without_claim_match,

    ROUND
    (
        100.0 * SUM(claim_match_flag)
        / NULLIF(COUNT(*), 0),
        2
    ) AS notification_to_claim_match_pct

FROM analytics.adt_to_claims_high_level_2025

GROUP BY
    source_group,
    match_target

ORDER BY
    source_group,
    match_target;
```

This is the main leadership-level flipped result.

---

## 2. Match rate by source category

```sql
SELECT
    source_category,
    match_target,

    COUNT(*) AS notification_instances,
    SUM(claim_match_flag) AS matched_instances,

    ROUND
    (
        100.0 * SUM(claim_match_flag)
        / NULLIF(COUNT(*), 0),
        2
    ) AS match_pct

FROM analytics.adt_to_claims_high_level_2025

GROUP BY
    source_category,
    match_target

ORDER BY
    source_category,
    match_target;
```

---

## 3. Why notifications did not match

```sql
SELECT
    source_group,
    match_target,
    adt_to_claim_status,

    COUNT(*) AS notification_instances,

    ROUND
    (
        100.0 * COUNT(*)
        / SUM(COUNT(*)) OVER
          (
              PARTITION BY
                  source_group,
                  match_target
          ),
        2
    ) AS percent_of_source_target

FROM analytics.adt_to_claims_high_level_2025

GROUP BY
    source_group,
    match_target,
    adt_to_claim_status

ORDER BY
    source_group,
    match_target,
    notification_instances DESC;
```

---

## 4. Highest-volume sending sources

```sql
SELECT
    sending_source,
    source_category,
    match_target,

    COUNT(*) AS notification_instances,
    SUM(claim_match_flag) AS matched_instances,

    ROUND
    (
        100.0 * SUM(claim_match_flag)
        / NULLIF(COUNT(*), 0),
        2
    ) AS match_pct

FROM analytics.adt_to_claims_high_level_2025

GROUP BY
    sending_source,
    source_category,
    match_target

HAVING COUNT(*) >= 100

ORDER BY
    notification_instances DESC;
```

---

## 5. Message-level match rate

Because one message may carry both admission and discharge information, this provides a second denominator:

```sql
SELECT
    source_group,

    COUNT(DISTINCT adt_message_key)
        AS distinct_messages,

    COUNT
    (
        DISTINCT CASE
            WHEN claim_match_flag = 1
                THEN adt_message_key
        END
    ) AS messages_with_any_claim_match,

    ROUND
    (
        100.0
        * COUNT
          (
              DISTINCT CASE
                  WHEN claim_match_flag = 1
                      THEN adt_message_key
              END
          )
        / NULLIF
          (
              COUNT(DISTINCT adt_message_key),
              0
          ),
        2
    ) AS message_match_pct

FROM analytics.adt_to_claims_high_level_2025

GROUP BY source_group

ORDER BY source_group;
```

# How to interpret the flipped comparison

| Result                                          | Meaning                                                                         |
| ----------------------------------------------- | ------------------------------------------------------------------------------- |
| High claims coverage + high ADT-to-claims match | Strong completeness and strong corroboration                                    |
| Low claims coverage + high ADT-to-claims match  | Notifications are accurate but incomplete                                       |
| High claims coverage + low ADT-to-claims match  | Many messages may be redundant, poorly classified, or outside the claims cohort |
| Low claims coverage + low ADT-to-claims match   | Both feed completeness and match quality require investigation                  |

## Caveats to state

An unmatched notification does not automatically mean bad ADT data. It may represent:

* Claims runout or delayed adjudication
* A member event outside the selected ED/IP/OBS claim definitions
* A non-covered or denied event
* An out-of-network encounter
* A date or patient-class discrepancy
* A notification outside the 2025 discharge cohort
* Multiple notifications for one clinical episode

The best leadership presentation is two side-by-side measures:

```text
Claims → Notification coverage
ADT → Claims corroboration rate
```

That gives leadership both sides of the reliability question without mixing the denominators.
