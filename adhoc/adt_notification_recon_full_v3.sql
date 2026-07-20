/*=============================================================================
  CAREALLIES 2025 ADT NOTIFICATION RECONCILIATION

  Purpose
  -------
  Reconcile CareAllies IP, ED, and OBS claim events to ADT/authorization/EMR
  notifications and measure admission and discharge coverage separately.

  Primary grains
  --------------
  1. analytics.notification_match_detail_2025
       One row per claim event + notification role + source/feed match.

  2. analytics.notification_reconciliation_2025
       One row per IP, ED, or OBS claim event.

  Important assumptions
  ---------------------
  - enr_2025 contains the CareAllies enrollment population and includes:
      personid, memberno, startdate, enddate, ipa_name, reporting_group
  - Enrollment eligibility is evaluated on the event discharge/end date.
  - qdwwh.dbo_adt.patient_class is cleaned to IP, ED, or OBS when populated.
  - Claims contain dates, not timestamps. Therefore business notification lag
    is calculated in calendar days. Pipeline lag is calculated in minutes from
    ADT message_timestamp to insert_timestamp.
=============================================================================*/

/*=============================================================================
  0. PARAMETERS
=============================================================================*/
DROP TABLE IF EXISTS tmp_params;
CREATE TEMP TABLE tmp_params AS
SELECT
    DATE '2025-01-01' AS start_date,
    DATE '2026-01-01' AS end_date,
    1::INTEGER        AS fuzzy_days;

/*=============================================================================
  1A. INPATIENT CLAIM EVENTS

  Population rule:
  - Discharged during 2025
  - CareAllies-enrolled on the discharge date
  - One row per inpatient event
=============================================================================*/
DROP TABLE IF EXISTS ip_raw;
CREATE TEMP TABLE ip_raw AS
WITH eligible_ip AS
(
    SELECT
        i.eventid,
        i.personid,
        REPLACE(e.memberno, '*', '') AS memberno,

        CAST(i.admitdate AS DATE)     AS admitdate,
        CAST(i.dischargedate AS DATE) AS dischargedate,
        CAST(i.paiddate AS DATE)      AS paiddate,

        i.rptgrouper,

        CASE
            WHEN i.rptgrouper = 'IP SNF'
                THEN 'SNF'
            WHEN i.rptgrouper = 'IP BEHAVIORAL HEALTH'
                THEN 'BH Acute'
            WHEN i.rptgrouper = 'IP LTC/REHAB'
                THEN 'LTAC'
            WHEN i.rptgrouper IN
                 (
                     'IP MEDICAL (ADULT)',
                     'IP SURGICAL (ADULT)',
                     'IP MATERNITY',
                     'IP OTHER'
                 )
                THEN 'Acute'
            ELSE 'Unknown'
        END AS bedtype,

        i.providernpi,
        i.providerspecialty,
        i.servicefacility,

        e.ipa_name,
        e.reporting_group,
        CAST(e.startdate AS DATE) AS enrollment_start_date,
        CAST(e.enddate AS DATE)   AS enrollment_end_date,

        ROW_NUMBER() OVER
        (
            PARTITION BY i.eventid
            ORDER BY
                CAST(e.startdate AS DATE) DESC,
                CAST(e.enddate AS DATE) DESC
        ) AS enrollment_rn

    FROM CIGNA_REPORTING_PRD62.dbo.inpatientevents i

    INNER JOIN enr_2025 e
        ON i.personid = e.personid
       AND CAST(i.dischargedate AS DATE)
           BETWEEN CAST(e.startdate AS DATE) AND CAST(e.enddate AS DATE)

    CROSS JOIN tmp_params p

    WHERE CAST(i.dischargedate AS DATE) >= p.start_date
      AND CAST(i.dischargedate AS DATE) <  p.end_date
)
SELECT
    eventid,
    personid,
    memberno,
    admitdate,
    dischargedate,
    paiddate,
    rptgrouper,
    bedtype,
    providernpi,
    providerspecialty,
    servicefacility,
    ipa_name,
    reporting_group,
    enrollment_start_date,
    enrollment_end_date
FROM eligible_ip
WHERE enrollment_rn = 1;

/*=============================================================================
  1B. ED AND OBSERVATION CLAIM EVENTS

  Population rule:
  - Ended/discharged during 2025
  - CareAllies-enrolled on the event end date
  - No direct join to inpatient events
  - If outpatient discharge date is missing, event date is used and flagged
=============================================================================*/
DROP TABLE IF EXISTS ed_obs_raw;
CREATE TEMP TABLE ed_obs_raw AS
WITH eligible_ed_obs AS
(
    SELECT
        o.eventid,
        o.personid,
        REPLACE(e.memberno, '*', '') AS memberno,

        CAST(o.eventdate AS DATE) AS admitdate,

        COALESCE
        (
            CAST(o.dischargedate AS DATE),
            CAST(o.eventdate AS DATE)
        ) AS dischargedate,

        CASE
            WHEN o.dischargedate IS NULL THEN 1
            ELSE 0
        END AS discharge_date_inferred_flag,

        CAST(o.paiddate AS DATE) AS paiddate,
        o.rptgrouper,

        CASE
            WHEN o.rptgrouper = 'ED VISITS' THEN 'ED'
            WHEN o.rptgrouper = 'OBSERVATION' THEN 'OBS'
        END AS care_setting,

        o.providernpi,
        o.providerspecialty,
        o.servicefacility,

        e.ipa_name,
        e.reporting_group,
        CAST(e.startdate AS DATE) AS enrollment_start_date,
        CAST(e.enddate AS DATE)   AS enrollment_end_date,

        ROW_NUMBER() OVER
        (
            PARTITION BY o.eventid
            ORDER BY
                CAST(e.startdate AS DATE) DESC,
                CAST(e.enddate AS DATE) DESC
        ) AS enrollment_rn

    FROM CIGNA_REPORTING_PRD62.dbo.outpatientevents o

    INNER JOIN enr_2025 e
        ON o.personid = e.personid
       AND COALESCE
           (
               CAST(o.dischargedate AS DATE),
               CAST(o.eventdate AS DATE)
           ) BETWEEN CAST(e.startdate AS DATE) AND CAST(e.enddate AS DATE)

    CROSS JOIN tmp_params p

    WHERE o.rptgrouper IN ('ED VISITS', 'OBSERVATION')
      AND COALESCE
          (
              CAST(o.dischargedate AS DATE),
              CAST(o.eventdate AS DATE)
          ) >= p.start_date
      AND COALESCE
          (
              CAST(o.dischargedate AS DATE),
              CAST(o.eventdate AS DATE)
          ) < p.end_date
)
SELECT
    eventid,
    personid,
    memberno,
    admitdate,
    dischargedate,
    discharge_date_inferred_flag,
    paiddate,
    rptgrouper,
    care_setting,
    providernpi,
    providerspecialty,
    servicefacility,
    ipa_name,
    reporting_group,
    enrollment_start_date,
    enrollment_end_date
FROM eligible_ed_obs
WHERE enrollment_rn = 1;

/*=============================================================================
  2. UNIFIED CLAIM EVENT SPINE

  Minimum change #1:
  claim_event_id is source-prefixed so IP, ED, and OBS native event IDs cannot
  collide.
=============================================================================*/
DROP TABLE IF EXISTS tmp_events_2025;
CREATE TEMP TABLE tmp_events_2025 AS

SELECT
    'IP-' || CAST(eventid AS VARCHAR(100)) AS claim_event_id,
    eventid AS native_event_id,
    'INPATIENTEVENTS' AS claims_source,

    personid AS person_id,
    memberno,

    'IP' AS care_type,
    bedtype AS care_subtype,
    rptgrouper,

    admitdate AS event_admit_date,
    dischargedate AS event_discharge_date,
    paiddate,

    0 AS discharge_date_inferred_flag,

    servicefacility,
    providernpi,
    providerspecialty,

    ipa_name,
    reporting_group,
    enrollment_start_date,
    enrollment_end_date

FROM ip_raw

UNION ALL

SELECT
    care_setting || '-' || CAST(eventid AS VARCHAR(100)) AS claim_event_id,
    eventid AS native_event_id,
    'OUTPATIENTEVENTS' AS claims_source,

    personid AS person_id,
    memberno,

    care_setting AS care_type,
    care_setting AS care_subtype,
    rptgrouper,

    admitdate AS event_admit_date,
    dischargedate AS event_discharge_date,
    paiddate,

    discharge_date_inferred_flag,

    servicefacility,
    providernpi,
    providerspecialty,

    ipa_name,
    reporting_group,
    enrollment_start_date,
    enrollment_end_date

FROM ed_obs_raw;

/*=============================================================================
  3. DATE WINDOW FOR THE ADT CANDIDATE PULL

  The claim population controls the analysis denominator. The ADT pull is a
  candidate universe around the actual claim-event date range.
=============================================================================*/
DROP TABLE IF EXISTS tmp_event_date_window;
CREATE TEMP TABLE tmp_event_date_window AS
SELECT
    DATEADD(day, -MAX(p.fuzzy_days), MIN(e.event_admit_date)) AS min_candidate_date,
    DATEADD(day,  MAX(p.fuzzy_days), MAX(e.event_discharge_date)) AS max_candidate_date
FROM tmp_events_2025 e
CROSS JOIN tmp_params p;

/*=============================================================================
  4. ADT MESSAGE-LEVEL CANDIDATE TABLE

  Minimum changes #2, #3, and #4:
  - Do not collapse messages before matching.
  - Retain admission and discharge message roles separately.
  - Retain message_timestamp, insert_timestamp, source, feed, patient class,
    event location, and other facility metadata.
=============================================================================*/
DROP TABLE IF EXISTS adt_events_candidate;
CREATE TEMP TABLE adt_events_candidate AS
WITH adt_base AS
(
    SELECT
        adt.person_id AS personid,

        CAST(adt.admit_date AS DATE)     AS admit_date,
        CAST(adt.discharge_date AS DATE) AS discharge_date,

        adt.diagnosis_date,
        adt.insert_timestamp,
        adt.message_timestamp,

        adt.adt_message_id,
        adt.event_id AS native_adt_event_id,

        UPPER(TRIM(adt.message_type)) AS message_type,
        adt.sending_source,

        CASE
            WHEN UPPER(TRIM(adt.patient_class)) IN ('IP', 'ED', 'OBS')
                THEN UPPER(TRIM(adt.patient_class))
            ELSE NULL
        END AS patient_class,

        adt.city,
        adt.state,
        adt.zip,

        adt.admit_type,
        adt.admit_source,
        adt.payer_name,
        adt.service_type,

        adt.discharge_disposition,
        adt.discharge_location,
        adt.event_location,

        CASE
            WHEN UPPER(TRIM(adt.message_type)) IN ('A01', 'A04', 'A05', 'A06')
                THEN 'ADMISSION'
            WHEN UPPER(TRIM(adt.message_type)) = 'A03'
                THEN 'DISCHARGE'
            WHEN UPPER(TRIM(adt.message_type)) = 'A02'
                THEN 'TRANSFER'
            WHEN UPPER(TRIM(adt.message_type)) = 'A08'
                THEN 'UPDATE'
            ELSE 'OTHER'
        END AS notification_role,

        CASE
            WHEN UPPER(TRIM(COALESCE(adt.sending_source, ''))) LIKE '%MEDHOK%'
                THEN 'AUTHORIZATION'
            WHEN UPPER(TRIM(COALESCE(adt.sending_source, ''))) LIKE '%EMR%'
              OR UPPER(TRIM(COALESCE(adt.sending_source, ''))) LIKE '%EPIC%'
              OR UPPER(TRIM(COALESCE(adt.sending_source, ''))) LIKE '%CERNER%'
                THEN 'EMR'
            WHEN UPPER(TRIM(COALESCE(adt.sending_source, ''))) LIKE '%HIE%'
                THEN 'HIE_ADT'
            WHEN adt.sending_source IS NOT NULL
                THEN 'OTHER'
            ELSE 'UNCLASSIFIED'
        END AS source_category,

        COALESCE
        (
            NULLIF(TRIM(CAST(adt.adt_message_id AS VARCHAR(200))), ''),
            'SYNTHETIC|'
            || COALESCE(CAST(adt.person_id AS VARCHAR(100)), 'NULL')
            || '|'
            || COALESCE(CAST(adt.event_id AS VARCHAR(100)), 'NULL')
            || '|'
            || COALESCE(UPPER(TRIM(adt.message_type)), 'NULL')
            || '|'
            || COALESCE(CAST(adt.message_timestamp AS VARCHAR(100)), 'NULL')
            || '|'
            || COALESCE(UPPER(TRIM(adt.sending_source)), 'NULL')
        ) AS adt_message_key

    FROM qdwwh.dbo_adt adt
    CROSS JOIN tmp_event_date_window w

    WHERE
    (
           CAST(adt.admit_date AS DATE)
               BETWEEN w.min_candidate_date AND w.max_candidate_date

        OR CAST(adt.discharge_date AS DATE)
               BETWEEN w.min_candidate_date AND w.max_candidate_date

        OR
        (
               adt.admit_date IS NULL
           AND adt.discharge_date IS NULL
           AND CAST(adt.message_timestamp AS DATE)
               BETWEEN w.min_candidate_date AND w.max_candidate_date
        )
    )
),
adt_deduplicated AS
(
    SELECT
        b.*,

        ROW_NUMBER() OVER
        (
            PARTITION BY b.adt_message_key
            ORDER BY
                b.insert_timestamp DESC NULLS LAST,
                b.message_timestamp DESC NULLS LAST,
                b.native_adt_event_id DESC NULLS LAST
        ) AS message_dedup_rn

    FROM adt_base b
)
SELECT
    adt_message_key,
    adt_message_id,
    native_adt_event_id,
    personid,

    admit_date,
    discharge_date,

    /* A03 without a populated discharge_date uses message date as a fallback. */
    CASE
        WHEN discharge_date IS NOT NULL
            THEN discharge_date
        WHEN message_type = 'A03' AND message_timestamp IS NOT NULL
            THEN CAST(message_timestamp AS DATE)
        ELSE NULL
    END AS discharge_match_date,

    CASE
        WHEN discharge_date IS NULL
         AND message_type = 'A03'
         AND message_timestamp IS NOT NULL
            THEN 1
        ELSE 0
    END AS discharge_match_date_inferred_flag,

    diagnosis_date,
    insert_timestamp,
    message_timestamp,

    message_type,
    notification_role,
    source_category,
    sending_source,
    patient_class,

    city,
    state,
    zip,

    admit_type,
    admit_source,
    payer_name,
    service_type,

    discharge_disposition,
    discharge_location,
    event_location,

    /*
       Admission candidates:
       - Standard admission/registration message types
       - Authorization rows containing an admission date
    */
    CASE
        WHEN notification_role = 'ADMISSION'
            THEN 1
        WHEN source_category = 'AUTHORIZATION' AND admit_date IS NOT NULL
            THEN 1
        ELSE 0
    END AS admission_candidate_flag,

    /*
       Discharge candidates:
       - A03 discharge
       - A08 carrying a discharge date
       - Authorization rows containing a discharge date
    */
    CASE
        WHEN notification_role = 'DISCHARGE'
            THEN 1
        WHEN notification_role = 'UPDATE' AND discharge_date IS NOT NULL
            THEN 1
        WHEN source_category = 'AUTHORIZATION' AND discharge_date IS NOT NULL
            THEN 1
        ELSE 0
    END AS discharge_candidate_flag

FROM adt_deduplicated
WHERE message_dedup_rn = 1;

/*=============================================================================
  5A. ADMISSION MATCH CANDIDATES

  Minimum change #5:
  Use cleaned patient_class to prevent ED, OBS, and IP events from competing
  for notifications belonging to another care setting.

  Matching hierarchy:
  1. Same person
  2. Same patient class, or missing ADT patient class as a fallback
  3. Admit date within configured tolerance
  4. Exact patient class preferred over missing class
  5. Exact date preferred over fuzzy date
  6. Facility agreement preferred
  7. Message-type priority
  8. Earliest internally available notification preferred
=============================================================================*/
DROP TABLE IF EXISTS tmp_admission_match_candidates;
CREATE TEMP TABLE tmp_admission_match_candidates AS
SELECT
    e.claim_event_id,
    e.native_event_id,
    e.claims_source,
    e.person_id,
    e.memberno,
    e.care_type,
    e.care_subtype,
    e.event_admit_date,
    e.event_discharge_date,
    e.servicefacility AS claim_servicefacility,
    e.ipa_name,
    e.reporting_group,

    a.adt_message_key,
    a.adt_message_id,
    a.native_adt_event_id,
    a.source_category,
    a.sending_source,
    a.patient_class AS adt_patient_class,
    a.event_location AS adt_event_location,
    a.city AS adt_city,
    a.state AS adt_state,
    a.zip AS adt_zip,
    a.message_type,
    a.notification_role,
    a.admit_type,
    a.admit_source,
    a.service_type,
    a.admit_date AS adt_match_date,
    a.message_timestamp,
    a.insert_timestamp,

    DATEDIFF(day, e.event_admit_date, a.admit_date) AS date_difference,

    CASE
        WHEN e.event_admit_date = a.admit_date
            THEN 'Exact Admission Date'
        ELSE 'Fuzzy Admission Date +/-'
             || CAST(p.fuzzy_days AS VARCHAR(10))
             || ' Day'
    END AS match_method,

    CASE
        WHEN a.patient_class = e.care_type THEN 0
        ELSE 1
    END AS patient_class_penalty,

    CASE
        WHEN a.patient_class = e.care_type
            THEN 'Exact Patient Class'
        ELSE 'Missing Patient Class Fallback'
    END AS patient_class_match_status,

    CASE
        WHEN NULLIF(TRIM(e.servicefacility), '') IS NOT NULL
         AND
         (
             UPPER(TRIM(e.servicefacility)) = UPPER(TRIM(a.event_location))
             OR UPPER(TRIM(e.servicefacility)) = UPPER(TRIM(a.sending_source))
         )
            THEN 1
        ELSE 0
    END AS facility_match_flag,

    CASE
        WHEN a.message_type = 'A01' THEN 0
        WHEN a.message_type = 'A04' THEN 1
        WHEN a.message_type = 'A06' THEN 2
        WHEN a.message_type = 'A05' THEN 3
        WHEN a.source_category = 'AUTHORIZATION' THEN 4
        ELSE 5
    END AS message_type_priority,

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

    COALESCE
    (
        NULLIF(UPPER(TRIM(a.sending_source)), ''),
        a.source_category
    ) AS source_feed_key,

    ROW_NUMBER() OVER
    (
        PARTITION BY
            e.claim_event_id,
            a.source_category,
            COALESCE
            (
                NULLIF(UPPER(TRIM(a.sending_source)), ''),
                a.source_category
            )

        ORDER BY
            CASE WHEN a.patient_class = e.care_type THEN 0 ELSE 1 END,
            ABS(DATEDIFF(day, e.event_admit_date, a.admit_date)),
            CASE
                WHEN NULLIF(TRIM(e.servicefacility), '') IS NOT NULL
                 AND
                 (
                     UPPER(TRIM(e.servicefacility)) = UPPER(TRIM(a.event_location))
                     OR UPPER(TRIM(e.servicefacility)) = UPPER(TRIM(a.sending_source))
                 )
                    THEN 0
                ELSE 1
            END,
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
    ) AS claim_source_rank,

    ROW_NUMBER() OVER
    (
        PARTITION BY a.adt_message_key

        ORDER BY
            CASE WHEN a.patient_class = e.care_type THEN 0 ELSE 1 END,
            ABS(DATEDIFF(day, e.event_admit_date, a.admit_date)),
            CASE
                WHEN NULLIF(TRIM(e.servicefacility), '') IS NOT NULL
                 AND
                 (
                     UPPER(TRIM(e.servicefacility)) = UPPER(TRIM(a.event_location))
                     OR UPPER(TRIM(e.servicefacility)) = UPPER(TRIM(a.sending_source))
                 )
                    THEN 0
                ELSE 1
            END,
            e.claim_event_id
    ) AS message_event_rank

FROM tmp_events_2025 e

INNER JOIN adt_events_candidate a
    ON e.person_id = a.personid
   AND a.admission_candidate_flag = 1
   AND a.admit_date IS NOT NULL

   /* Strict care-setting match when patient_class exists; null is fallback. */
   AND
   (
       a.patient_class = e.care_type
       OR a.patient_class IS NULL
   )

CROSS JOIN tmp_params p

WHERE ABS(DATEDIFF(day, e.event_admit_date, a.admit_date)) <= p.fuzzy_days;

/*=============================================================================
  5B. SELECTED ADMISSION MATCHES
=============================================================================*/
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

FROM tmp_admission_match_candidates
WHERE claim_source_rank = 1
  AND message_event_rank = 1;

/*=============================================================================
  6A. DISCHARGE MATCH CANDIDATES

  Discharge matching uses the claims discharge/end date and the ADT discharge
  date. A03 message date is used only when the A03 discharge date is missing,
  and that fallback is explicitly flagged.
=============================================================================*/
DROP TABLE IF EXISTS tmp_discharge_match_candidates;
CREATE TEMP TABLE tmp_discharge_match_candidates AS
SELECT
    e.claim_event_id,
    e.native_event_id,
    e.claims_source,
    e.person_id,
    e.memberno,
    e.care_type,
    e.care_subtype,
    e.event_admit_date,
    e.event_discharge_date,
    e.discharge_date_inferred_flag AS claim_discharge_date_inferred_flag,
    e.servicefacility AS claim_servicefacility,
    e.ipa_name,
    e.reporting_group,

    a.adt_message_key,
    a.adt_message_id,
    a.native_adt_event_id,
    a.source_category,
    a.sending_source,
    a.patient_class AS adt_patient_class,
    a.event_location AS adt_event_location,
    a.city AS adt_city,
    a.state AS adt_state,
    a.zip AS adt_zip,
    a.message_type,
    a.notification_role,
    a.discharge_disposition,
    a.discharge_location,
    a.service_type,
    a.discharge_match_date AS adt_match_date,
    a.discharge_match_date_inferred_flag AS adt_discharge_date_inferred_flag,
    a.message_timestamp,
    a.insert_timestamp,

    DATEDIFF
    (
        day,
        e.event_discharge_date,
        a.discharge_match_date
    ) AS date_difference,

    CASE
        WHEN e.event_discharge_date = a.discharge_match_date
            THEN 'Exact Discharge Date'
        ELSE 'Fuzzy Discharge Date +/-'
             || CAST(p.fuzzy_days AS VARCHAR(10))
             || ' Day'
    END AS match_method,

    CASE
        WHEN a.patient_class = e.care_type THEN 0
        ELSE 1
    END AS patient_class_penalty,

    CASE
        WHEN a.patient_class = e.care_type
            THEN 'Exact Patient Class'
        ELSE 'Missing Patient Class Fallback'
    END AS patient_class_match_status,

    CASE
        WHEN NULLIF(TRIM(e.servicefacility), '') IS NOT NULL
         AND
         (
             UPPER(TRIM(e.servicefacility)) = UPPER(TRIM(a.event_location))
             OR UPPER(TRIM(e.servicefacility)) = UPPER(TRIM(a.sending_source))
         )
            THEN 1
        ELSE 0
    END AS facility_match_flag,

    CASE
        WHEN a.message_type = 'A03' THEN 0
        WHEN a.message_type = 'A08' THEN 1
        WHEN a.source_category = 'AUTHORIZATION' THEN 2
        ELSE 3
    END AS message_type_priority,

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

    COALESCE
    (
        NULLIF(UPPER(TRIM(a.sending_source)), ''),
        a.source_category
    ) AS source_feed_key,

    ROW_NUMBER() OVER
    (
        PARTITION BY
            e.claim_event_id,
            a.source_category,
            COALESCE
            (
                NULLIF(UPPER(TRIM(a.sending_source)), ''),
                a.source_category
            )

        ORDER BY
            CASE WHEN a.patient_class = e.care_type THEN 0 ELSE 1 END,
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
                     UPPER(TRIM(e.servicefacility)) = UPPER(TRIM(a.event_location))
                     OR UPPER(TRIM(e.servicefacility)) = UPPER(TRIM(a.sending_source))
                 )
                    THEN 0
                ELSE 1
            END,
            CASE
                WHEN a.message_type = 'A03' THEN 0
                WHEN a.message_type = 'A08' THEN 1
                WHEN a.source_category = 'AUTHORIZATION' THEN 2
                ELSE 3
            END,
            a.insert_timestamp ASC NULLS LAST,
            a.message_timestamp ASC NULLS LAST,
            a.adt_message_key
    ) AS claim_source_rank,

    ROW_NUMBER() OVER
    (
        PARTITION BY a.adt_message_key

        ORDER BY
            CASE WHEN a.patient_class = e.care_type THEN 0 ELSE 1 END,
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
                     UPPER(TRIM(e.servicefacility)) = UPPER(TRIM(a.event_location))
                     OR UPPER(TRIM(e.servicefacility)) = UPPER(TRIM(a.sending_source))
                 )
                    THEN 0
                ELSE 1
            END,
            e.claim_event_id
    ) AS message_event_rank

FROM tmp_events_2025 e

INNER JOIN adt_events_candidate a
    ON e.person_id = a.personid
   AND a.discharge_candidate_flag = 1
   AND a.discharge_match_date IS NOT NULL

   /* Strict care-setting match when patient_class exists; null is fallback. */
   AND
   (
       a.patient_class = e.care_type
       OR a.patient_class IS NULL
   )

CROSS JOIN tmp_params p

WHERE ABS
      (
          DATEDIFF
          (
              day,
              e.event_discharge_date,
              a.discharge_match_date
          )
      ) <= p.fuzzy_days;

/*=============================================================================
  6B. SELECTED DISCHARGE MATCHES
=============================================================================*/
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

FROM tmp_discharge_match_candidates
WHERE claim_source_rank = 1
  AND message_event_rank = 1;

/*=============================================================================
  7. SOURCE-LEVEL NOTIFICATION MATCH DETAIL

  One row per claim event + notification role + source/feed.
=============================================================================*/
DROP TABLE IF EXISTS analytics.notification_match_detail_2025;
CREATE TABLE analytics.notification_match_detail_2025 AS

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
    NULL::VARCHAR(500) AS discharge_disposition,
    NULL::VARCHAR(500) AS discharge_location,
    service_type,

    adt_match_date,
    0 AS adt_match_date_inferred_flag,
    message_timestamp,
    insert_timestamp,

    match_target,
    match_method,
    date_difference,
    patient_class_match_status,
    facility_match_flag,
    notification_lag_days,
    pipeline_lag_minutes,
    source_feed_key,
    selected_match_flag

FROM tmp_admission_matches

UNION ALL

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

    NULL::VARCHAR(500) AS admit_type,
    NULL::VARCHAR(500) AS admit_source,
    discharge_disposition,
    discharge_location,
    service_type,

    adt_match_date,
    adt_discharge_date_inferred_flag AS adt_match_date_inferred_flag,
    message_timestamp,
    insert_timestamp,

    match_target,
    match_method,
    date_difference,
    patient_class_match_status,
    facility_match_flag,
    notification_lag_days,
    pipeline_lag_minutes,
    source_feed_key,
    selected_match_flag

FROM tmp_discharge_matches;

/*=============================================================================
  8A. EVENT-LEVEL SOURCE AND ROLE AGGREGATION

  Redshift note:
  Source lists are built in separate temp tables so LISTAGG does not conflict
  with the distinct count aggregates.
=============================================================================*/
DROP TABLE IF EXISTS tmp_event_notification_counts;
CREATE TEMP TABLE tmp_event_notification_counts AS
SELECT
    claim_event_id,

    MAX(CASE WHEN match_target = 'ADMISSION' THEN 1 ELSE 0 END)
        AS admission_notification_flag,

    MAX(CASE WHEN match_target = 'DISCHARGE' THEN 1 ELSE 0 END)
        AS discharge_notification_flag,

    MAX(CASE WHEN match_target = 'ADMISSION' AND source_category = 'HIE_ADT' THEN 1 ELSE 0 END)
        AS admission_hie_flag,
    MAX(CASE WHEN match_target = 'ADMISSION' AND source_category = 'AUTHORIZATION' THEN 1 ELSE 0 END)
        AS admission_authorization_flag,
    MAX(CASE WHEN match_target = 'ADMISSION' AND source_category = 'EMR' THEN 1 ELSE 0 END)
        AS admission_emr_flag,
    MAX(CASE WHEN match_target = 'ADMISSION' AND source_category = 'OTHER' THEN 1 ELSE 0 END)
        AS admission_other_flag,
    MAX(CASE WHEN match_target = 'ADMISSION' AND source_category = 'UNCLASSIFIED' THEN 1 ELSE 0 END)
        AS admission_unclassified_flag,

    MAX(CASE WHEN match_target = 'DISCHARGE' AND source_category = 'HIE_ADT' THEN 1 ELSE 0 END)
        AS discharge_hie_flag,
    MAX(CASE WHEN match_target = 'DISCHARGE' AND source_category = 'AUTHORIZATION' THEN 1 ELSE 0 END)
        AS discharge_authorization_flag,
    MAX(CASE WHEN match_target = 'DISCHARGE' AND source_category = 'EMR' THEN 1 ELSE 0 END)
        AS discharge_emr_flag,
    MAX(CASE WHEN match_target = 'DISCHARGE' AND source_category = 'OTHER' THEN 1 ELSE 0 END)
        AS discharge_other_flag,
    MAX(CASE WHEN match_target = 'DISCHARGE' AND source_category = 'UNCLASSIFIED' THEN 1 ELSE 0 END)
        AS discharge_unclassified_flag,

    MAX(CASE WHEN source_category = 'HIE_ADT' THEN 1 ELSE 0 END)
        AS hie_adt_flag,
    MAX(CASE WHEN source_category = 'AUTHORIZATION' THEN 1 ELSE 0 END)
        AS authorization_flag,
    MAX(CASE WHEN source_category = 'EMR' THEN 1 ELSE 0 END)
        AS emr_flag,
    MAX(CASE WHEN source_category = 'OTHER' THEN 1 ELSE 0 END)
        AS other_flag,
    MAX(CASE WHEN source_category = 'UNCLASSIFIED' THEN 1 ELSE 0 END)
        AS unclassified_flag,

    COUNT(DISTINCT CASE WHEN match_target = 'ADMISSION' THEN source_feed_key END)
        AS admission_source_count,

    COUNT(DISTINCT CASE WHEN match_target = 'DISCHARGE' THEN source_feed_key END)
        AS discharge_source_count,

    COUNT(DISTINCT source_feed_key)
        AS overall_source_count

FROM analytics.notification_match_detail_2025
GROUP BY claim_event_id;

DROP TABLE IF EXISTS tmp_event_admission_source_list;
CREATE TEMP TABLE tmp_event_admission_source_list AS
SELECT
    claim_event_id,
    LISTAGG(DISTINCT source_category, ', ')
        WITHIN GROUP (ORDER BY source_category) AS admission_sources_notified
FROM analytics.notification_match_detail_2025
WHERE match_target = 'ADMISSION'
GROUP BY claim_event_id;

DROP TABLE IF EXISTS tmp_event_discharge_source_list;
CREATE TEMP TABLE tmp_event_discharge_source_list AS
SELECT
    claim_event_id,
    LISTAGG(DISTINCT source_category, ', ')
        WITHIN GROUP (ORDER BY source_category) AS discharge_sources_notified
FROM analytics.notification_match_detail_2025
WHERE match_target = 'DISCHARGE'
GROUP BY claim_event_id;

DROP TABLE IF EXISTS tmp_event_all_source_list;
CREATE TEMP TABLE tmp_event_all_source_list AS
SELECT
    claim_event_id,
    LISTAGG(DISTINCT source_category, ', ')
        WITHIN GROUP (ORDER BY source_category) AS sources_notified
FROM analytics.notification_match_detail_2025
GROUP BY claim_event_id;

DROP TABLE IF EXISTS tmp_event_notification_agg;
CREATE TEMP TABLE tmp_event_notification_agg AS
SELECT
    c.*,
    adm.admission_sources_notified,
    dis.discharge_sources_notified,
    src.sources_notified
FROM tmp_event_notification_counts c
LEFT JOIN tmp_event_admission_source_list adm
    ON c.claim_event_id = adm.claim_event_id
LEFT JOIN tmp_event_discharge_source_list dis
    ON c.claim_event_id = dis.claim_event_id
LEFT JOIN tmp_event_all_source_list src
    ON c.claim_event_id = src.claim_event_id;

/*=============================================================================
  8B. EARLIEST ADMISSION NOTIFICATION PER CLAIM EVENT
=============================================================================*/
DROP TABLE IF EXISTS tmp_first_admission_notification;
CREATE TEMP TABLE tmp_first_admission_notification AS
SELECT
    claim_event_id,
    source_category AS first_admission_source_category,
    sending_source AS first_admission_sending_source,
    adt_event_location AS first_admission_event_location,
    message_type AS first_admission_message_type,
    message_timestamp AS first_admission_message_timestamp,
    insert_timestamp AS first_admission_insert_timestamp,
    notification_lag_days AS admission_notification_lag_days,
    pipeline_lag_minutes AS admission_pipeline_lag_minutes,
    match_method AS admission_match_method,
    date_difference AS admission_date_difference,
    patient_class_match_status AS admission_patient_class_match_status,
    facility_match_flag AS admission_facility_match_flag
FROM analytics.notification_match_detail_2025
WHERE match_target = 'ADMISSION'
QUALIFY ROW_NUMBER() OVER
(
    PARTITION BY claim_event_id
    ORDER BY
        insert_timestamp ASC NULLS LAST,
        message_timestamp ASC NULLS LAST,
        source_category,
        source_feed_key
) = 1;

/*=============================================================================
  8C. EARLIEST DISCHARGE NOTIFICATION PER CLAIM EVENT
=============================================================================*/
DROP TABLE IF EXISTS tmp_first_discharge_notification;
CREATE TEMP TABLE tmp_first_discharge_notification AS
SELECT
    claim_event_id,
    source_category AS first_discharge_source_category,
    sending_source AS first_discharge_sending_source,
    adt_event_location AS first_discharge_event_location,
    message_type AS first_discharge_message_type,
    message_timestamp AS first_discharge_message_timestamp,
    insert_timestamp AS first_discharge_insert_timestamp,
    notification_lag_days AS discharge_notification_lag_days,
    pipeline_lag_minutes AS discharge_pipeline_lag_minutes,
    match_method AS discharge_match_method,
    date_difference AS discharge_date_difference,
    patient_class_match_status AS discharge_patient_class_match_status,
    facility_match_flag AS discharge_facility_match_flag,
    adt_match_date_inferred_flag AS adt_discharge_date_inferred_flag
FROM analytics.notification_match_detail_2025
WHERE match_target = 'DISCHARGE'
QUALIFY ROW_NUMBER() OVER
(
    PARTITION BY claim_event_id
    ORDER BY
        insert_timestamp ASC NULLS LAST,
        message_timestamp ASC NULLS LAST,
        source_category,
        source_feed_key
) = 1;

/*=============================================================================
  9. FINAL EVENT-LEVEL RECONCILIATION TABLE
=============================================================================*/
DROP TABLE IF EXISTS analytics.notification_reconciliation_2025;
CREATE TABLE analytics.notification_reconciliation_2025 AS
SELECT
    e.claim_event_id,
    e.native_event_id,
    e.claims_source,

    e.person_id,
    e.memberno,

    e.care_type,
    e.care_subtype,
    e.rptgrouper,

    e.event_admit_date,
    e.event_discharge_date,
    DATEDIFF(day, e.event_admit_date, e.event_discharge_date) AS length_of_stay_days,
    e.paiddate,
    e.discharge_date_inferred_flag,

    e.servicefacility,
    e.providernpi,
    e.providerspecialty,

    e.ipa_name,
    e.reporting_group,
    e.enrollment_start_date,
    e.enrollment_end_date,

    COALESCE(a.admission_notification_flag, 0) AS admission_notification_flag,
    COALESCE(a.discharge_notification_flag, 0) AS discharge_notification_flag,

    CASE
        WHEN COALESCE(a.admission_notification_flag, 0) = 1
         AND COALESCE(a.discharge_notification_flag, 0) = 1
            THEN 1
        ELSE 0
    END AS complete_admission_discharge_flag,

    COALESCE(a.admission_hie_flag, 0) AS admission_hie_flag,
    COALESCE(a.admission_authorization_flag, 0) AS admission_authorization_flag,
    COALESCE(a.admission_emr_flag, 0) AS admission_emr_flag,
    COALESCE(a.admission_other_flag, 0) AS admission_other_flag,
    COALESCE(a.admission_unclassified_flag, 0) AS admission_unclassified_flag,

    COALESCE(a.discharge_hie_flag, 0) AS discharge_hie_flag,
    COALESCE(a.discharge_authorization_flag, 0) AS discharge_authorization_flag,
    COALESCE(a.discharge_emr_flag, 0) AS discharge_emr_flag,
    COALESCE(a.discharge_other_flag, 0) AS discharge_other_flag,
    COALESCE(a.discharge_unclassified_flag, 0) AS discharge_unclassified_flag,

    COALESCE(a.hie_adt_flag, 0) AS hie_adt_flag,
    COALESCE(a.authorization_flag, 0) AS authorization_flag,
    COALESCE(a.emr_flag, 0) AS emr_flag,
    COALESCE(a.other_flag, 0) AS other_flag,
    COALESCE(a.unclassified_flag, 0) AS unclassified_flag,

    COALESCE(a.admission_source_count, 0) AS admission_source_count,
    COALESCE(a.discharge_source_count, 0) AS discharge_source_count,
    COALESCE(a.overall_source_count, 0) AS overall_source_count,

    a.admission_sources_notified,
    a.discharge_sources_notified,
    a.sources_notified,

    CASE
        WHEN COALESCE(a.admission_notification_flag, 0) = 0
            THEN 'No Admission Notification'
        WHEN COALESCE(a.admission_hie_flag, 0) = 1
         AND COALESCE(a.admission_authorization_flag, 0) = 1
            THEN 'Authorization and HIE ADT'
        WHEN COALESCE(a.admission_hie_flag, 0) = 1
            THEN 'HIE ADT Only'
        WHEN COALESCE(a.admission_authorization_flag, 0) = 1
            THEN 'Authorization Only'
        WHEN COALESCE(a.admission_emr_flag, 0) = 1
            THEN 'EMR Only'
        WHEN COALESCE(a.admission_other_flag, 0) = 1
            THEN 'Other Source Only'
        ELSE 'Unclassified Admission Source'
    END AS admission_coverage_category,

    CASE
        WHEN COALESCE(a.discharge_notification_flag, 0) = 0
            THEN 'No Discharge Notification'
        WHEN COALESCE(a.discharge_hie_flag, 0) = 1
         AND COALESCE(a.discharge_authorization_flag, 0) = 1
            THEN 'Authorization and HIE ADT'
        WHEN COALESCE(a.discharge_hie_flag, 0) = 1
            THEN 'HIE ADT Only'
        WHEN COALESCE(a.discharge_authorization_flag, 0) = 1
            THEN 'Authorization Only'
        WHEN COALESCE(a.discharge_emr_flag, 0) = 1
            THEN 'EMR Only'
        WHEN COALESCE(a.discharge_other_flag, 0) = 1
            THEN 'Other Source Only'
        ELSE 'Unclassified Discharge Source'
    END AS discharge_coverage_category,

    CASE
        WHEN COALESCE(a.admission_notification_flag, 0) = 1
         AND COALESCE(a.discharge_notification_flag, 0) = 1
            THEN 'Admission and Discharge Received'
        WHEN COALESCE(a.admission_notification_flag, 0) = 1
         AND COALESCE(a.discharge_notification_flag, 0) = 0
            THEN 'Admission Only'
        WHEN COALESCE(a.admission_notification_flag, 0) = 0
         AND COALESCE(a.discharge_notification_flag, 0) = 1
            THEN 'Discharge Only'
        ELSE 'No Notification'
    END AS notification_completeness_status,

    CASE
        WHEN COALESCE(a.overall_source_count, 0) = 0 THEN 1
        ELSE 0
    END AS no_notification_flag,

    CASE
        WHEN COALESCE(a.overall_source_count, 0) >= 2 THEN 1
        ELSE 0
    END AS multi_source_flag,

    fa.first_admission_source_category,
    fa.first_admission_sending_source,
    fa.first_admission_event_location,
    fa.first_admission_message_type,
    fa.first_admission_message_timestamp,
    fa.first_admission_insert_timestamp,
    fa.admission_notification_lag_days,
    fa.admission_pipeline_lag_minutes,
    fa.admission_match_method,
    fa.admission_date_difference,
    fa.admission_patient_class_match_status,
    fa.admission_facility_match_flag,

    fd.first_discharge_source_category,
    fd.first_discharge_sending_source,
    fd.first_discharge_event_location,
    fd.first_discharge_message_type,
    fd.first_discharge_message_timestamp,
    fd.first_discharge_insert_timestamp,
    fd.discharge_notification_lag_days,
    fd.discharge_pipeline_lag_minutes,
    fd.discharge_match_method,
    fd.discharge_date_difference,
    fd.discharge_patient_class_match_status,
    fd.discharge_facility_match_flag,
    fd.adt_discharge_date_inferred_flag,

    CASE
        WHEN fa.admission_notification_lag_days IS NULL
            THEN 'Timing Unavailable'
        WHEN fa.admission_notification_lag_days < 0
            THEN 'Negative Lag - Investigate'
        WHEN fa.admission_notification_lag_days = 0
            THEN 'Same Day'
        WHEN fa.admission_notification_lag_days = 1
            THEN '1 Day'
        WHEN fa.admission_notification_lag_days = 2
            THEN '2 Days'
        WHEN fa.admission_notification_lag_days BETWEEN 3 AND 7
            THEN '3-7 Days'
        ELSE 'Over 7 Days'
    END AS admission_timeliness_category,

    CASE
        WHEN fd.discharge_notification_lag_days IS NULL
            THEN 'Timing Unavailable'
        WHEN fd.discharge_notification_lag_days < 0
            THEN 'Negative Lag - Investigate'
        WHEN fd.discharge_notification_lag_days = 0
            THEN 'Same Day'
        WHEN fd.discharge_notification_lag_days = 1
            THEN '1 Day'
        WHEN fd.discharge_notification_lag_days = 2
            THEN '2 Days'
        WHEN fd.discharge_notification_lag_days BETWEEN 3 AND 7
            THEN '3-7 Days'
        ELSE 'Over 7 Days'
    END AS discharge_timeliness_category

FROM tmp_events_2025 e

LEFT JOIN tmp_event_notification_agg a
    ON e.claim_event_id = a.claim_event_id

LEFT JOIN tmp_first_admission_notification fa
    ON e.claim_event_id = fa.claim_event_id

LEFT JOIN tmp_first_discharge_notification fd
    ON e.claim_event_id = fd.claim_event_id;

/*=============================================================================
  10. COVERAGE GAP ROLLUP
=============================================================================*/
DROP TABLE IF EXISTS analytics.notification_coverage_gaps_2025;
CREATE TABLE analytics.notification_coverage_gaps_2025 AS
SELECT
    reporting_group,
    ipa_name,
    servicefacility,
    care_type,

    COUNT(*) AS total_events,

    SUM(admission_notification_flag) AS events_with_admission_notification,
    ROUND
    (
        100.0 * SUM(admission_notification_flag) / NULLIF(COUNT(*), 0),
        2
    ) AS admission_notification_coverage_pct,

    SUM(discharge_notification_flag) AS events_with_discharge_notification,
    ROUND
    (
        100.0 * SUM(discharge_notification_flag) / NULLIF(COUNT(*), 0),
        2
    ) AS discharge_notification_coverage_pct,

    SUM(complete_admission_discharge_flag) AS events_with_complete_notifications,
    ROUND
    (
        100.0 * SUM(complete_admission_discharge_flag) / NULLIF(COUNT(*), 0),
        2
    ) AS complete_notification_coverage_pct,

    SUM(no_notification_flag) AS events_with_no_notification,
    ROUND
    (
        100.0 * SUM(no_notification_flag) / NULLIF(COUNT(*), 0),
        2
    ) AS percent_no_notification,

    SUM(hie_adt_flag) AS hie_adt_events,
    SUM(authorization_flag) AS authorization_events,
    SUM(emr_flag) AS emr_events,
    SUM(other_flag) AS other_source_events,
    SUM(unclassified_flag) AS unclassified_source_events,
    SUM(multi_source_flag) AS multi_source_events,

    SUM(CASE WHEN admission_timeliness_category = 'Same Day' THEN 1 ELSE 0 END)
        AS same_day_admission_notifications,

    SUM(CASE WHEN discharge_timeliness_category = 'Same Day' THEN 1 ELSE 0 END)
        AS same_day_discharge_notifications,

    SUM(CASE WHEN admission_timeliness_category = 'Negative Lag - Investigate' THEN 1 ELSE 0 END)
        AS negative_admission_lag_events,

    SUM(CASE WHEN discharge_timeliness_category = 'Negative Lag - Investigate' THEN 1 ELSE 0 END)
        AS negative_discharge_lag_events

FROM analytics.notification_reconciliation_2025
GROUP BY
    reporting_group,
    ipa_name,
    servicefacility,
    care_type;

/*=============================================================================
  11. OPTIONAL QA QUERIES
=============================================================================*/

/* QA 1: Confirm one row per claims event in the final table. */
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT claim_event_id) AS distinct_claim_events
FROM analytics.notification_reconciliation_2025;

/* QA 2: Look for native event ID collisions that the prefixed key prevents. */
SELECT
    native_event_id,
    COUNT(DISTINCT claims_source) AS claims_source_count,
    MIN(claims_source) AS first_claims_source,
    MAX(claims_source) AS second_claims_source
FROM analytics.notification_reconciliation_2025
GROUP BY native_event_id
HAVING COUNT(DISTINCT claims_source) > 1
ORDER BY claims_source_count DESC, native_event_id;

/* QA 3: Review patient-class fallback usage. */
SELECT
    match_target,
    care_type,
    patient_class_match_status,
    COUNT(*) AS matched_rows
FROM analytics.notification_match_detail_2025
GROUP BY
    match_target,
    care_type,
    patient_class_match_status
ORDER BY
    match_target,
    care_type,
    patient_class_match_status;

/* QA 4: Review source classifications and unmatched source names. */
SELECT
    source_category,
    sending_source,
    COUNT(*) AS matched_rows
FROM analytics.notification_match_detail_2025
GROUP BY
    source_category,
    sending_source
ORDER BY
    matched_rows DESC;

/* QA 5: Review coverage by care setting. */
SELECT
    care_type,
    COUNT(*) AS total_events,
    SUM(admission_notification_flag) AS admission_covered,
    SUM(discharge_notification_flag) AS discharge_covered,
    SUM(complete_admission_discharge_flag) AS complete_covered,
    SUM(no_notification_flag) AS no_notification
FROM analytics.notification_reconciliation_2025
GROUP BY care_type
ORDER BY care_type;
