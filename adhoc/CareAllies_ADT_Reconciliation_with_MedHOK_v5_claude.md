Below is a streamlined Redshift-compatible rewrite that keeps your v4 logic but applies the main improvements I suggested:

- uses `QUALIFY` instead of many nested rank-and-filter wrappers,
- keeps the separate clinical vs. MedHOK branches,
- preserves your OBS inferred cross-class fallback,
- preserves the MedHOK ambiguity controls,
- keeps admission and discharge as separate notification targets,
- reduces repeated wrapping CTE noise.

I have **not** tried to over-collapse the clinical admission/discharge and MedHOK admission/discharge branches into one giant meta-query, because that usually makes healthcare matching logic harder to audit. For this use case, the better balance is: **same logic, less scaffolding** rather than “fully abstracted SQL.”

## What I would change from v4

1. Replace nested `SELECT * FROM ( SELECT ..., ROW_NUMBER() ... ) ranked WHERE rn = 1` with `QUALIFY ROW_NUMBER() ... = 1`.
2. Add parentheses around mixed `AND`/`OR` predicates where precedence could be risky.
3. Add `DISTKEY/SORTKEY` back on the largest repeatedly-joined temp tables.
4. Keep the MedHOK branch separate, but simplify the ranking/selection steps.

## Important correctness fixes included

The biggest correctness risk in the v4 style you shared is boolean precedence in predicates like:

```sql
AND p.include_ed_authorization = 1
OR e.care_type <> 'ED'
```

That should be:

```sql
AND (p.include_ed_authorization = 1 OR e.care_type <> 'ED')
```

Similarly, the clinical patient_class matching branch should be fully parenthesized.

***

# Streamlined query

```sql
/*=============================================================================
CAREALLIES 2025 ADT NOTIFICATION RECONCILIATION
Streamlined Redshift version

Purpose
-------
Reconcile CareAllies IP, ED, and OBS claim events to ADT / authorization / EMR
notifications and measure admission and discharge coverage separately.

Design notes
------------
- Keeps clinical sources separate from MedHOK authorization.
- Keeps admission and discharge matching separate for auditability.
- Uses QUALIFY to reduce nested ranking layers.
- Preserves OBS inferred matching fallback.
- Preserves MedHOK ambiguity controls.
=============================================================================*/

/*=============================================================================
0. PARAMETERS
=============================================================================*/
DROP TABLE IF EXISTS tmp_params;
CREATE TEMP TABLE tmp_params AS
SELECT
    DATE '2025-01-01' AS start_date,
    DATE '2026-01-01' AS end_date,
    1::INTEGER AS adt_fuzzy_days,
    1::INTEGER AS medhok_fuzzy_days,
    1::INTEGER AS include_ed_authorization;

/*=============================================================================
1A. INPATIENT CLAIM EVENTS
=============================================================================*/
DROP TABLE IF EXISTS ip_raw;
CREATE TEMP TABLE ip_raw
DISTSTYLE KEY
DISTKEY (personid)
SORTKEY (personid, dischargedate) AS
WITH eligible_ip AS
(
    SELECT
        i.eventid,
        i.personid,
        REPLACE(e.memberno, '*', '') AS memberno,

        CAST(i.admitdate AS DATE) AS admitdate,
        CAST(i.dischargedate AS DATE) AS dischargedate,
        CAST(i.paiddate AS DATE) AS paiddate,

        i.rptgrouper,

        CASE
            WHEN i.rptgrouper = 'IP SNF' THEN 'SNF'
            WHEN i.rptgrouper = 'IP BEHAVIORAL HEALTH' THEN 'BH Acute'
            WHEN i.rptgrouper = 'IP LTC/REHAB' THEN 'LTAC'
            WHEN i.rptgrouper IN
            (
                'IP MEDICAL (ADULT)',
                'IP SURGICAL (ADULT)',
                'IP MATERNITY',
                'IP OTHER'
            ) THEN 'Acute'
            ELSE 'Unknown'
        END AS bedtype,

        i.providernpi,
        i.providerspecialty,
        i.servicefacility,

        e.ipa_name,
        e.reporting_group,
        CAST(e.startdate AS DATE) AS enrollment_start_date,
        CAST(e.enddate AS DATE) AS enrollment_end_date,

        ROW_NUMBER() OVER
        (
            PARTITION BY i.eventid
            ORDER BY CAST(e.startdate AS DATE) DESC,
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
1B. ED AND OBS CLAIM EVENTS
=============================================================================*/
DROP TABLE IF EXISTS ed_obs_raw;
CREATE TEMP TABLE ed_obs_raw
DISTSTYLE KEY
DISTKEY (personid)
SORTKEY (personid, dischargedate) AS
WITH eligible_ed_obs AS
(
    SELECT
        o.eventid,
        o.personid,
        REPLACE(e.memberno, '*', '') AS memberno,

        CAST(o.eventdate AS DATE) AS admitdate,
        COALESCE(CAST(o.dischargedate AS DATE), CAST(o.eventdate AS DATE)) AS dischargedate,

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
        CAST(e.enddate AS DATE) AS enrollment_end_date,

        ROW_NUMBER() OVER
        (
            PARTITION BY o.eventid
            ORDER BY CAST(e.startdate AS DATE) DESC,
                     CAST(e.enddate AS DATE) DESC
        ) AS enrollment_rn

    FROM CIGNA_REPORTING_PRD62.dbo.outpatientevents o
    INNER JOIN enr_2025 e
        ON o.personid = e.personid
       AND COALESCE(CAST(o.dischargedate AS DATE), CAST(o.eventdate AS DATE))
           BETWEEN CAST(e.startdate AS DATE) AND CAST(e.enddate AS DATE)
    CROSS JOIN tmp_params p
    WHERE o.rptgrouper IN ('ED VISITS', 'OBSERVATION')
      AND COALESCE(CAST(o.dischargedate AS DATE), CAST(o.eventdate AS DATE)) >= p.start_date
      AND COALESCE(CAST(o.dischargedate AS DATE), CAST(o.eventdate AS DATE)) <  p.end_date
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
=============================================================================*/
DROP TABLE IF EXISTS tmp_events_2025;
CREATE TEMP TABLE tmp_events_2025
DISTSTYLE KEY
DISTKEY (person_id)
SORTKEY (person_id, event_admit_date, event_discharge_date) AS
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
3. ADT CANDIDATE DATE WINDOW
=============================================================================*/
DROP TABLE IF EXISTS tmp_event_date_window;
CREATE TEMP TABLE tmp_event_date_window AS
SELECT
    DATEADD(
        day,
        -GREATEST(MAX(p.adt_fuzzy_days), MAX(p.medhok_fuzzy_days)),
        MIN(e.event_admit_date)
    ) AS min_candidate_date,
    DATEADD(
        day,
        GREATEST(MAX(p.adt_fuzzy_days), MAX(p.medhok_fuzzy_days)),
        MAX(e.event_discharge_date)
    ) AS max_candidate_date
FROM tmp_events_2025 e
CROSS JOIN tmp_params p;

/*=============================================================================
4. ADT MESSAGE-LEVEL CANDIDATE TABLE
=============================================================================*/
DROP TABLE IF EXISTS adt_events_candidate;
CREATE TEMP TABLE adt_events_candidate
DISTSTYLE KEY
DISTKEY (personid)
SORTKEY (personid, admit_date, discharge_match_date) AS
WITH adt_base AS
(
    SELECT
        adt.person_id AS personid,

        CAST(adt.admit_date AS DATE) AS admit_date,
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
            WHEN UPPER(TRIM(adt.message_type)) IN ('A01', 'A04', 'A05', 'A06') THEN 'ADMISSION'
            WHEN UPPER(TRIM(adt.message_type)) = 'A03' THEN 'DISCHARGE'
            WHEN UPPER(TRIM(adt.message_type)) = 'A02' THEN 'TRANSFER'
            WHEN UPPER(TRIM(adt.message_type)) = 'A08' THEN 'UPDATE'
            ELSE 'OTHER'
        END AS notification_role,

        CASE
            WHEN UPPER(TRIM(COALESCE(adt.sending_source, ''))) LIKE '%MEDHOK%' THEN 'AUTHORIZATION'
            WHEN UPPER(TRIM(COALESCE(adt.sending_source, ''))) LIKE '%EMR%'
              OR UPPER(TRIM(COALESCE(adt.sending_source, ''))) LIKE '%EPIC%'
              OR UPPER(TRIM(COALESCE(adt.sending_source, ''))) LIKE '%CERNER%' THEN 'EMR'
            WHEN UPPER(TRIM(COALESCE(adt.sending_source, ''))) LIKE '%HIE%' THEN 'HIE_ADT'
            WHEN adt.sending_source IS NOT NULL THEN 'OTHER'
            ELSE 'UNCLASSIFIED'
        END AS source_category,

        COALESCE(
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
          CAST(adt.admit_date AS DATE) BETWEEN w.min_candidate_date AND w.max_candidate_date
       OR CAST(adt.discharge_date AS DATE) BETWEEN w.min_candidate_date AND w.max_candidate_date
       OR (
              adt.admit_date IS NULL
          AND adt.discharge_date IS NULL
          AND CAST(adt.message_timestamp AS DATE) BETWEEN w.min_candidate_date AND w.max_candidate_date
       )
)
SELECT
    adt_message_key,
    adt_message_id,
    native_adt_event_id,
    personid,

    admit_date,
    discharge_date,

    CASE
        WHEN discharge_date IS NOT NULL THEN discharge_date
        WHEN message_type = 'A03' AND message_timestamp IS NOT NULL THEN CAST(message_timestamp AS DATE)
        ELSE NULL
    END AS discharge_match_date,

    CASE
        WHEN discharge_date IS NULL
         AND message_type = 'A03'
         AND message_timestamp IS NOT NULL THEN 1
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

    CASE
        WHEN notification_role = 'ADMISSION' THEN 1
        WHEN source_category = 'AUTHORIZATION' AND admit_date IS NOT NULL THEN 1
        ELSE 0
    END AS admission_candidate_flag,

    CASE
        WHEN notification_role = 'DISCHARGE' THEN 1
        WHEN notification_role = 'UPDATE' AND discharge_date IS NOT NULL THEN 1
        WHEN source_category = 'AUTHORIZATION' AND discharge_date IS NOT NULL THEN 1
        ELSE 0
    END AS discharge_candidate_flag
FROM adt_base
QUALIFY ROW_NUMBER() OVER
(
    PARTITION BY adt_message_key
    ORDER BY insert_timestamp DESC NULLS LAST,
             message_timestamp DESC NULLS LAST,
             native_adt_event_id DESC NULLS LAST
) = 1;

/*=============================================================================
5A. CLINICAL ADMISSION CANDIDATES
=============================================================================*/
DROP TABLE IF EXISTS tmp_adt_admission_match_candidates;
CREATE TEMP TABLE tmp_adt_admission_match_candidates
DISTSTYLE KEY
DISTKEY (person_id)
SORTKEY (person_id, claim_event_id, adt_message_key) AS
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
    'ADMISSION' AS notification_role,
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
        ELSE 'Fuzzy Admission Date +/-' || CAST(p.adt_fuzzy_days AS VARCHAR(10)) || ' Day'
    END AS match_method,

    CASE
        WHEN a.patient_class = e.care_type THEN 0
        WHEN a.patient_class IS NULL THEN 1
        WHEN e.care_type = 'OBS' AND a.patient_class IN ('ED', 'IP') THEN 2
        ELSE 3
    END AS patient_class_penalty,

    CASE
        WHEN a.patient_class = e.care_type THEN 'Exact Patient Class'
        WHEN a.patient_class IS NULL THEN 'Missing Patient Class Fallback'
        WHEN e.care_type = 'OBS' AND a.patient_class IN ('ED', 'IP') THEN 'OBS Cross-Class Fallback'
        ELSE 'Unsupported Cross-Class'
    END AS patient_class_match_status,

    CASE
        WHEN NULLIF(TRIM(e.servicefacility), '') IS NOT NULL
         AND (
                UPPER(TRIM(e.servicefacility)) = UPPER(TRIM(a.event_location))
             OR UPPER(TRIM(e.servicefacility)) = UPPER(TRIM(a.sending_source))
         ) THEN 1
        ELSE 0
    END AS facility_match_flag,

    CASE
        WHEN a.message_type = 'A01' THEN 0
        WHEN a.message_type = 'A04' THEN 1
        WHEN a.message_type = 'A06' THEN 2
        WHEN a.message_type = 'A05' THEN 3
        ELSE 4
    END AS message_type_priority,

    DATEDIFF(day, e.event_admit_date, CAST(a.insert_timestamp AS DATE)) AS notification_lag_days,
    DATEDIFF(minute, a.message_timestamp, a.insert_timestamp) AS pipeline_lag_minutes,

    COALESCE(NULLIF(UPPER(TRIM(a.sending_source)), ''), a.source_category) AS source_feed_key
FROM tmp_events_2025 e
INNER JOIN adt_events_candidate a
    ON e.person_id = a.personid
   AND a.source_category <> 'AUTHORIZATION'
   AND a.admission_candidate_flag = 1
   AND a.admit_date IS NOT NULL
   AND (
           a.patient_class = e.care_type
        OR a.patient_class IS NULL
        OR (e.care_type = 'OBS' AND a.patient_class IN ('ED', 'IP'))
   )
CROSS JOIN tmp_params p
WHERE ABS(DATEDIFF(day, e.event_admit_date, a.admit_date)) <= p.adt_fuzzy_days;

/*=============================================================================
5B. ASSIGN EACH CLINICAL ADMISSION MESSAGE TO ITS BEST CLAIM EVENT
=============================================================================*/
DROP TABLE IF EXISTS tmp_adt_admission_message_assignment;
CREATE TEMP TABLE tmp_adt_admission_message_assignment AS
SELECT *
FROM tmp_adt_admission_match_candidates c
QUALIFY ROW_NUMBER() OVER
(
    PARTITION BY c.adt_message_key
    ORDER BY c.patient_class_penalty,
             ABS(c.date_difference),
             c.facility_match_flag DESC,
             c.message_type_priority,
             c.insert_timestamp ASC NULLS LAST,
             c.message_timestamp ASC NULLS LAST,
             c.claim_event_id
) = 1;

/*=============================================================================
5C. SELECT BEST ASSIGNED CLINICAL ADMISSION MESSAGE PER CLAIM + FEED
=============================================================================*/
DROP TABLE IF EXISTS tmp_adt_admission_matches;
CREATE TEMP TABLE tmp_adt_admission_matches AS
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
    0::INTEGER AS adt_match_date_inferred_flag,
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

    NULL::VARCHAR(200) AS authorization_match_status,
    NULL::INTEGER AS authorization_candidate_event_count,
    NULL::INTEGER AS authorization_match_score,
    1::INTEGER AS selected_match_flag
FROM tmp_adt_admission_message_assignment a
QUALIFY ROW_NUMBER() OVER
(
    PARTITION BY a.claim_event_id, a.source_category, a.source_feed_key
    ORDER BY a.patient_class_penalty,
             ABS(a.date_difference),
             a.facility_match_flag DESC,
             a.message_type_priority,
             a.insert_timestamp ASC NULLS LAST,
             a.message_timestamp ASC NULLS LAST,
             a.adt_message_key
) = 1;

/*=============================================================================
6A. CLINICAL DISCHARGE CANDIDATES
=============================================================================*/
DROP TABLE IF EXISTS tmp_adt_discharge_match_candidates;
CREATE TEMP TABLE tmp_adt_discharge_match_candidates
DISTSTYLE KEY
DISTKEY (person_id)
SORTKEY (person_id, claim_event_id, adt_message_key) AS
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
    'DISCHARGE' AS notification_role,
    a.discharge_disposition,
    a.discharge_location,
    a.service_type,
    a.discharge_match_date AS adt_match_date,
    a.discharge_match_date_inferred_flag AS adt_match_date_inferred_flag,
    a.message_timestamp,
    a.insert_timestamp,

    DATEDIFF(day, e.event_discharge_date, a.discharge_match_date) AS date_difference,

    CASE
        WHEN e.event_discharge_date = a.discharge_match_date
            THEN 'Exact Discharge Date'
        ELSE 'Fuzzy Discharge Date +/-' || CAST(p.adt_fuzzy_days AS VARCHAR(10)) || ' Day'
    END AS match_method,

    CASE
        WHEN a.patient_class = e.care_type THEN 0
        WHEN a.patient_class IS NULL THEN 1
        WHEN e.care_type = 'OBS' AND a.patient_class IN ('ED', 'IP') THEN 2
        ELSE 3
    END AS patient_class_penalty,

    CASE
        WHEN a.patient_class = e.care_type THEN 'Exact Patient Class'
        WHEN a.patient_class IS NULL THEN 'Missing Patient Class Fallback'
        WHEN e.care_type = 'OBS' AND a.patient_class IN ('ED', 'IP') THEN 'OBS Cross-Class Fallback'
        ELSE 'Unsupported Cross-Class'
    END AS patient_class_match_status,

    CASE
        WHEN NULLIF(TRIM(e.servicefacility), '') IS NOT NULL
         AND (
                UPPER(TRIM(e.servicefacility)) = UPPER(TRIM(a.event_location))
             OR UPPER(TRIM(e.servicefacility)) = UPPER(TRIM(a.sending_source))
         ) THEN 1
        ELSE 0
    END AS facility_match_flag,

    CASE
        WHEN a.message_type = 'A03' THEN 0
        WHEN a.message_type = 'A08' THEN 1
        ELSE 2
    END AS message_type_priority,

    DATEDIFF(day, e.event_discharge_date, CAST(a.insert_timestamp AS DATE)) AS notification_lag_days,
    DATEDIFF(minute, a.message_timestamp, a.insert_timestamp) AS pipeline_lag_minutes,

    COALESCE(NULLIF(UPPER(TRIM(a.sending_source)), ''), a.source_category) AS source_feed_key
FROM tmp_events_2025 e
INNER JOIN adt_events_candidate a
    ON e.person_id = a.personid
   AND a.source_category <> 'AUTHORIZATION'
   AND a.discharge_candidate_flag = 1
   AND a.discharge_match_date IS NOT NULL
   AND (
           a.patient_class = e.care_type
        OR a.patient_class IS NULL
        OR (e.care_type = 'OBS' AND a.patient_class IN ('ED', 'IP'))
   )
CROSS JOIN tmp_params p
WHERE ABS(DATEDIFF(day, e.event_discharge_date, a.discharge_match_date)) <= p.adt_fuzzy_days;

/*=============================================================================
6B. ASSIGN EACH CLINICAL DISCHARGE MESSAGE TO ITS BEST CLAIM EVENT
=============================================================================*/
DROP TABLE IF EXISTS tmp_adt_discharge_message_assignment;
CREATE TEMP TABLE tmp_adt_discharge_message_assignment AS
SELECT *
FROM tmp_adt_discharge_match_candidates c
QUALIFY ROW_NUMBER() OVER
(
    PARTITION BY c.adt_message_key
    ORDER BY c.patient_class_penalty,
             ABS(c.date_difference),
             c.facility_match_flag DESC,
             c.message_type_priority,
             c.insert_timestamp ASC NULLS LAST,
             c.message_timestamp ASC NULLS LAST,
             c.claim_event_id
) = 1;

/*=============================================================================
6C. SELECT BEST ASSIGNED CLINICAL DISCHARGE MESSAGE PER CLAIM + FEED
=============================================================================*/
DROP TABLE IF EXISTS tmp_adt_discharge_matches;
CREATE TEMP TABLE tmp_adt_discharge_matches AS
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
    adt_match_date_inferred_flag,
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

    NULL::VARCHAR(200) AS authorization_match_status,
    NULL::INTEGER AS authorization_candidate_event_count,
    NULL::INTEGER AS authorization_match_score,
    1::INTEGER AS selected_match_flag
FROM tmp_adt_discharge_message_assignment a
QUALIFY ROW_NUMBER() OVER
(
    PARTITION BY a.claim_event_id, a.source_category, a.source_feed_key
    ORDER BY a.patient_class_penalty,
             ABS(a.date_difference),
             a.facility_match_flag DESC,
             a.message_type_priority,
             a.insert_timestamp ASC NULLS LAST,
             a.message_timestamp ASC NULLS LAST,
             a.adt_message_key
) = 1;

/*=============================================================================
7A. MEDHOK ADMISSION CANDIDATES
=============================================================================*/
DROP TABLE IF EXISTS tmp_medhok_admission_candidates;
CREATE TEMP TABLE tmp_medhok_admission_candidates
DISTSTYLE KEY
DISTKEY (person_id)
SORTKEY (person_id, claim_event_id, adt_message_key) AS
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
    a.sending_source,
    a.event_location AS adt_event_location,
    a.city AS adt_city,
    a.state AS adt_state,
    a.zip AS adt_zip,
    a.message_type,
    a.admit_type,
    a.admit_source,
    a.service_type,
    a.admit_date AS adt_match_date,
    a.discharge_date AS authorization_discharge_date,
    a.message_timestamp,
    a.insert_timestamp,

    DATEDIFF(day, e.event_admit_date, a.admit_date) AS date_difference,

    CASE
        WHEN NULLIF(TRIM(e.servicefacility), '') IS NOT NULL
         AND (
                UPPER(TRIM(e.servicefacility)) = UPPER(TRIM(a.event_location))
             OR UPPER(TRIM(e.servicefacility)) = UPPER(TRIM(a.sending_source))
         ) THEN 1
        ELSE 0
    END AS facility_match_flag,

    CASE
        WHEN e.event_admit_date = a.admit_date
         AND a.discharge_date IS NOT NULL
         AND e.event_discharge_date = a.discharge_date THEN 1
        ELSE 0
    END AS exact_admit_discharge_pair_flag,

    CASE
        WHEN e.event_admit_date = a.admit_date THEN 1
        ELSE 0
    END AS exact_target_date_flag,

    DATEDIFF(day, e.event_admit_date, CAST(a.insert_timestamp AS DATE)) AS notification_lag_days,
    DATEDIFF(minute, a.message_timestamp, a.insert_timestamp) AS pipeline_lag_minutes,

    COALESCE(NULLIF(UPPER(TRIM(a.sending_source)), ''), 'AUTHORIZATION') AS source_feed_key,

    CASE
        WHEN e.event_admit_date = a.admit_date
         AND a.discharge_date IS NOT NULL
         AND e.event_discharge_date = a.discharge_date THEN 1
        WHEN e.event_admit_date = a.admit_date
         AND (
                NULLIF(TRIM(e.servicefacility), '') IS NOT NULL
            AND (
                   UPPER(TRIM(e.servicefacility)) = UPPER(TRIM(a.event_location))
                OR UPPER(TRIM(e.servicefacility)) = UPPER(TRIM(a.sending_source))
            )
         ) THEN 2
        WHEN e.event_admit_date = a.admit_date THEN 3
        WHEN (
                NULLIF(TRIM(e.servicefacility), '') IS NOT NULL
            AND (
                   UPPER(TRIM(e.servicefacility)) = UPPER(TRIM(a.event_location))
                OR UPPER(TRIM(e.servicefacility)) = UPPER(TRIM(a.sending_source))
            )
             ) THEN 4
        ELSE 5
    END AS authorization_match_score
FROM tmp_events_2025 e
INNER JOIN adt_events_candidate a
    ON e.person_id = a.personid
   AND a.source_category = 'AUTHORIZATION'
   AND a.admission_candidate_flag = 1
   AND a.admit_date IS NOT NULL
CROSS JOIN tmp_params p
WHERE ABS(DATEDIFF(day, e.event_admit_date, a.admit_date)) <= p.medhok_fuzzy_days
  AND (p.include_ed_authorization = 1 OR e.care_type <> 'ED');

/* candidate counts */
DROP TABLE IF EXISTS tmp_medhok_admission_candidate_counts;
CREATE TEMP TABLE tmp_medhok_admission_candidate_counts AS
SELECT
    adt_message_key,
    COUNT(DISTINCT claim_event_id) AS candidate_event_count,
    COUNT(DISTINCT CASE WHEN exact_target_date_flag = 1 THEN claim_event_id END) AS exact_date_candidate_count,
    COUNT(DISTINCT CASE WHEN exact_admit_discharge_pair_flag = 1 THEN claim_event_id END) AS exact_pair_candidate_count,
    COUNT(DISTINCT CASE WHEN facility_match_flag = 1 THEN claim_event_id END) AS facility_supported_candidate_count
FROM tmp_medhok_admission_candidates
GROUP BY adt_message_key;

/* best claim assignment per authorization message */
DROP TABLE IF EXISTS tmp_medhok_admission_assignment;
CREATE TEMP TABLE tmp_medhok_admission_assignment AS
SELECT
    c.*,
    x.candidate_event_count,
    x.exact_date_candidate_count,
    x.exact_pair_candidate_count,
    x.facility_supported_candidate_count
FROM tmp_medhok_admission_candidates c
INNER JOIN tmp_medhok_admission_candidate_counts x
    ON c.adt_message_key = x.adt_message_key
QUALIFY ROW_NUMBER() OVER
(
    PARTITION BY c.adt_message_key
    ORDER BY c.authorization_match_score,
             ABS(c.date_difference),
             c.facility_match_flag DESC,
             c.insert_timestamp ASC NULLS LAST,
             c.message_timestamp ASC NULLS LAST,
             c.claim_event_id
) = 1;

/* final selected MedHOK admission matches */
DROP TABLE IF EXISTS tmp_medhok_admission_matches;
CREATE TEMP TABLE tmp_medhok_admission_matches AS
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
    'AUTHORIZATION' AS source_category,
    sending_source,
    NULL::VARCHAR(20) AS adt_patient_class,
    adt_event_location,
    adt_city,
    adt_state,
    adt_zip,
    message_type,
    'ADMISSION' AS notification_role,
    admit_type,
    admit_source,
    NULL::VARCHAR(500) AS discharge_disposition,
    NULL::VARCHAR(500) AS discharge_location,
    service_type,
    adt_match_date,
    0::INTEGER AS adt_match_date_inferred_flag,
    message_timestamp,
    insert_timestamp,

    'ADMISSION' AS match_target,
    CASE
        WHEN date_difference = 0 THEN 'Exact Authorization Admission Date'
        ELSE 'Authorization Admission Date +/-1 Day'
    END AS match_method,
    date_difference,
    'Authorization - Patient Class Unavailable' AS patient_class_match_status,
    facility_match_flag,
    notification_lag_days,
    pipeline_lag_minutes,
    source_feed_key,

    authorization_match_status,
    candidate_event_count AS authorization_candidate_event_count,
    authorization_match_score,
    1::INTEGER AS selected_match_flag
FROM
(
    SELECT
        a.*,
        CASE
            WHEN a.candidate_event_count = 1
                THEN 'Unique Authorization Member-Date Match'
            WHEN a.exact_admit_discharge_pair_flag = 1
             AND a.exact_pair_candidate_count = 1
                THEN 'Resolved by Exact Admit and Discharge Dates'
            WHEN a.facility_match_flag = 1
             AND a.facility_supported_candidate_count = 1
                THEN 'Resolved by Facility Match'
            WHEN a.exact_target_date_flag = 1
             AND a.exact_date_candidate_count = 1
                THEN 'Resolved by Unique Exact Admission Date'
            ELSE 'Ambiguous Authorization Match - Excluded'
        END AS authorization_match_status
    FROM tmp_medhok_admission_assignment a
) a
QUALIFY ROW_NUMBER() OVER
(
    PARTITION BY a.claim_event_id, a.source_feed_key
    ORDER BY a.authorization_match_score,
             ABS(a.date_difference),
             a.facility_match_flag DESC,
             a.insert_timestamp ASC NULLS LAST,
             a.message_timestamp ASC NULLS LAST,
             a.adt_message_key
) = 1
AND authorization_match_status <> 'Ambiguous Authorization Match - Excluded';

/*=============================================================================
7B. MEDHOK DISCHARGE CANDIDATES
=============================================================================*/
DROP TABLE IF EXISTS tmp_medhok_discharge_candidates;
CREATE TEMP TABLE tmp_medhok_discharge_candidates
DISTSTYLE KEY
DISTKEY (person_id)
SORTKEY (person_id, claim_event_id, adt_message_key) AS
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
    a.sending_source,
    a.event_location AS adt_event_location,
    a.city AS adt_city,
    a.state AS adt_state,
    a.zip AS adt_zip,
    a.message_type,
    a.discharge_disposition,
    a.discharge_location,
    a.service_type,
    a.discharge_match_date AS adt_match_date,
    a.discharge_match_date_inferred_flag AS adt_match_date_inferred_flag,
    a.admit_date AS authorization_admit_date,
    a.message_timestamp,
    a.insert_timestamp,

    DATEDIFF(day, e.event_discharge_date, a.discharge_match_date) AS date_difference,

    CASE
        WHEN NULLIF(TRIM(e.servicefacility), '') IS NOT NULL
         AND (
                UPPER(TRIM(e.servicefacility)) = UPPER(TRIM(a.event_location))
             OR UPPER(TRIM(e.servicefacility)) = UPPER(TRIM(a.sending_source))
         ) THEN 1
        ELSE 0
    END AS facility_match_flag,

    CASE
        WHEN a.admit_date IS NOT NULL
         AND e.event_admit_date = a.admit_date
         AND e.event_discharge_date = a.discharge_match_date THEN 1
        ELSE 0
    END AS exact_admit_discharge_pair_flag,

    CASE
        WHEN e.event_discharge_date = a.discharge_match_date THEN 1
        ELSE 0
    END AS exact_target_date_flag,

    DATEDIFF(day, e.event_discharge_date, CAST(a.insert_timestamp AS DATE)) AS notification_lag_days,
    DATEDIFF(minute, a.message_timestamp, a.insert_timestamp) AS pipeline_lag_minutes,

    COALESCE(NULLIF(UPPER(TRIM(a.sending_source)), ''), 'AUTHORIZATION') AS source_feed_key,

    CASE
        WHEN a.admit_date IS NOT NULL
         AND e.event_admit_date = a.admit_date
         AND e.event_discharge_date = a.discharge_match_date THEN 1
        WHEN e.event_discharge_date = a.discharge_match_date
         AND (
                NULLIF(TRIM(e.servicefacility), '') IS NOT NULL
            AND (
                   UPPER(TRIM(e.servicefacility)) = UPPER(TRIM(a.event_location))
                OR UPPER(TRIM(e.servicefacility)) = UPPER(TRIM(a.sending_source))
            )
         ) THEN 2
        WHEN e.event_discharge_date = a.discharge_match_date THEN 3
        WHEN (
                NULLIF(TRIM(e.servicefacility), '') IS NOT NULL
            AND (
                   UPPER(TRIM(e.servicefacility)) = UPPER(TRIM(a.event_location))
                OR UPPER(TRIM(e.servicefacility)) = UPPER(TRIM(a.sending_source))
            )
             ) THEN 4
        ELSE 5
    END AS authorization_match_score
FROM tmp_events_2025 e
INNER JOIN adt_events_candidate a
    ON e.person_id = a.personid
   AND a.source_category = 'AUTHORIZATION'
   AND a.discharge_candidate_flag = 1
   AND a.discharge_match_date IS NOT NULL
CROSS JOIN tmp_params p
WHERE ABS(DATEDIFF(day, e.event_discharge_date, a.discharge_match_date)) <= p.medhok_fuzzy_days
  AND (p.include_ed_authorization = 1 OR e.care_type <> 'ED');

/* candidate counts */
DROP TABLE IF EXISTS tmp_medhok_discharge_candidate_counts;
CREATE TEMP TABLE tmp_medhok_discharge_candidate_counts AS
SELECT
    adt_message_key,
    COUNT(DISTINCT claim_event_id) AS candidate_event_count,
    COUNT(DISTINCT CASE WHEN exact_target_date_flag = 1 THEN claim_event_id END) AS exact_date_candidate_count,
    COUNT(DISTINCT CASE WHEN exact_admit_discharge_pair_flag = 1 THEN claim_event_id END) AS exact_pair_candidate_count,
    COUNT(DISTINCT CASE WHEN facility_match_flag = 1 THEN claim_event_id END) AS facility_supported_candidate_count
FROM tmp_medhok_discharge_candidates
GROUP BY adt_message_key;

/* best claim assignment per authorization message */
DROP TABLE IF EXISTS tmp_medhok_discharge_assignment;
CREATE TEMP TABLE tmp_medhok_discharge_assignment AS
SELECT
    c.*,
    x.candidate_event_count,
    x.exact_date_candidate_count,
    x.exact_pair_candidate_count,
    x.facility_supported_candidate_count
FROM tmp_medhok_discharge_candidates c
INNER JOIN tmp_medhok_discharge_candidate_counts x
    ON c.adt_message_key = x.adt_message_key
QUALIFY ROW_NUMBER() OVER
(
    PARTITION BY c.adt_message_key
    ORDER BY c.authorization_match_score,
             ABS(c.date_difference),
             c.facility_match_flag DESC,
             c.insert_timestamp ASC NULLS LAST,
             c.message_timestamp ASC NULLS LAST,
             c.claim_event_id
) = 1;

/* final selected MedHOK discharge matches */
DROP TABLE IF EXISTS tmp_medhok_discharge_matches;
CREATE TEMP TABLE tmp_medhok_discharge_matches AS
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
    'AUTHORIZATION' AS source_category,
    sending_source,
    NULL::VARCHAR(20) AS adt_patient_class,
    adt_event_location,
    adt_city,
    adt_state,
    adt_zip,
    message_type,
    'DISCHARGE' AS notification_role,
    NULL::VARCHAR(500) AS admit_type,
    NULL::VARCHAR(500) AS admit_source,
    discharge_disposition,
    discharge_location,
    service_type,
    adt_match_date,
    adt_match_date_inferred_flag,
    message_timestamp,
    insert_timestamp,

    'DISCHARGE' AS match_target,
    CASE
        WHEN date_difference = 0 THEN 'Exact Authorization Discharge Date'
        ELSE 'Authorization Discharge Date +/-1 Day'
    END AS match_method,
    date_difference,
    'Authorization - Patient Class Unavailable' AS patient_class_match_status,
    facility_match_flag,
    notification_lag_days,
    pipeline_lag_minutes,
    source_feed_key,

    authorization_match_status,
    candidate_event_count AS authorization_candidate_event_count,
    authorization_match_score,
    1::INTEGER AS selected_match_flag
FROM
(
    SELECT
        a.*,
        CASE
            WHEN a.candidate_event_count = 1
                THEN 'Unique Authorization Member-Date Match'
            WHEN a.exact_admit_discharge_pair_flag = 1
             AND a.exact_pair_candidate_count = 1
                THEN 'Resolved by Exact Admit and Discharge Dates'
            WHEN a.facility_match_flag = 1
             AND a.facility_supported_candidate_count = 1
                THEN 'Resolved by Facility Match'
            WHEN a.exact_target_date_flag = 1
             AND a.exact_date_candidate_count = 1
                THEN 'Resolved by Unique Exact Discharge Date'
            ELSE 'Ambiguous Authorization Match - Excluded'
        END AS authorization_match_status
    FROM tmp_medhok_discharge_assignment a
) a
QUALIFY ROW_NUMBER() OVER
(
    PARTITION BY a.claim_event_id, a.source_feed_key
    ORDER BY a.authorization_match_score,
             ABS(a.date_difference),
             a.facility_match_flag DESC,
             a.insert_timestamp ASC NULLS LAST,
             a.message_timestamp ASC NULLS LAST,
             a.adt_message_key
) = 1
AND authorization_match_status <> 'Ambiguous Authorization Match - Excluded';

/*=============================================================================
8. COMBINE SELECTED MATCHES
=============================================================================*/
DROP TABLE IF EXISTS analytics.notification_match_detail_2025;
CREATE TABLE analytics.notification_match_detail_2025 AS
SELECT * FROM tmp_adt_admission_matches
UNION ALL
SELECT * FROM tmp_adt_discharge_matches
UNION ALL
SELECT * FROM tmp_medhok_admission_matches
UNION ALL
SELECT * FROM tmp_medhok_discharge_matches;

/*=============================================================================
8A. EVENT-LEVEL COUNTS
=============================================================================*/
DROP TABLE IF EXISTS tmp_event_notification_counts;
CREATE TEMP TABLE tmp_event_notification_counts AS
SELECT
    claim_event_id,

    MAX(CASE WHEN match_target = 'ADMISSION' THEN 1 ELSE 0 END) AS admission_notification_flag,
    MAX(CASE WHEN match_target = 'DISCHARGE' THEN 1 ELSE 0 END) AS discharge_notification_flag,

    MAX(CASE WHEN match_target = 'ADMISSION' AND source_category = 'HIE_ADT' THEN 1 ELSE 0 END) AS admission_hie_flag,
    MAX(CASE WHEN match_target = 'ADMISSION' AND source_category = 'AUTHORIZATION' THEN 1 ELSE 0 END) AS admission_authorization_flag,
    MAX(CASE WHEN match_target = 'ADMISSION' AND source_category = 'EMR' THEN 1 ELSE 0 END) AS admission_emr_flag,
    MAX(CASE WHEN match_target = 'ADMISSION' AND source_category = 'OTHER' THEN 1 ELSE 0 END) AS admission_other_flag,
    MAX(CASE WHEN match_target = 'ADMISSION' AND source_category = 'UNCLASSIFIED' THEN 1 ELSE 0 END) AS admission_unclassified_flag,

    MAX(CASE WHEN match_target = 'DISCHARGE' AND source_category = 'HIE_ADT' THEN 1 ELSE 0 END) AS discharge_hie_flag,
    MAX(CASE WHEN match_target = 'DISCHARGE' AND source_category = 'AUTHORIZATION' THEN 1 ELSE 0 END) AS discharge_authorization_flag,
    MAX(CASE WHEN match_target = 'DISCHARGE' AND source_category = 'EMR' THEN 1 ELSE 0 END) AS discharge_emr_flag,
    MAX(CASE WHEN match_target = 'DISCHARGE' AND source_category = 'OTHER' THEN 1 ELSE 0 END) AS discharge_other_flag,
    MAX(CASE WHEN match_target = 'DISCHARGE' AND source_category = 'UNCLASSIFIED' THEN 1 ELSE 0 END) AS discharge_unclassified_flag,

    MAX(CASE WHEN source_category = 'HIE_ADT' THEN 1 ELSE 0 END) AS hie_adt_flag,
    MAX(CASE WHEN source_category = 'AUTHORIZATION' THEN 1 ELSE 0 END) AS authorization_flag,
    MAX(CASE WHEN source_category = 'EMR' THEN 1 ELSE 0 END) AS emr_flag,
    MAX(CASE WHEN source_category = 'OTHER' THEN 1 ELSE 0 END) AS other_flag,
    MAX(CASE WHEN source_category = 'UNCLASSIFIED' THEN 1 ELSE 0 END) AS unclassified_flag,

    COUNT(DISTINCT CASE WHEN match_target = 'ADMISSION' THEN source_feed_key END) AS admission_source_count,
    COUNT(DISTINCT CASE WHEN match_target = 'DISCHARGE' THEN source_feed_key END) AS discharge_source_count,
    COUNT(DISTINCT source_feed_key) AS overall_source_count
FROM analytics.notification_match_detail_2025
GROUP BY claim_event_id;

/*=============================================================================
8B. SOURCE LISTS
=============================================================================*/
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

/*=============================================================================
9. FINAL EVENT-LEVEL RECONCILIATION
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
    e.paiddate,
    e.discharge_date_inferred_flag,
    e.servicefacility,
    e.providernpi,
    e.providerspecialty,
    e.ipa_name,
    e.reporting_group,
    e.enrollment_start_date,
    e.enrollment_end_date,

    COALESCE(c.admission_notification_flag, 0) AS admission_notification_flag,
    COALESCE(c.discharge_notification_flag, 0) AS discharge_notification_flag,

    COALESCE(c.admission_hie_flag, 0) AS admission_hie_flag,
    COALESCE(c.admission_authorization_flag, 0) AS admission_authorization_flag,
    COALESCE(c.admission_emr_flag, 0) AS admission_emr_flag,
    COALESCE(c.admission_other_flag, 0) AS admission_other_flag,
    COALESCE(c.admission_unclassified_flag, 0) AS admission_unclassified_flag,

    COALESCE(c.discharge_hie_flag, 0) AS discharge_hie_flag,
    COALESCE(c.discharge_authorization_flag, 0) AS discharge_authorization_flag,
    COALESCE(c.discharge_emr_flag, 0) AS discharge_emr_flag,
    COALESCE(c.discharge_other_flag, 0) AS discharge_other_flag,
    COALESCE(c.discharge_unclassified_flag, 0) AS discharge_unclassified_flag,

    COALESCE(c.hie_adt_flag, 0) AS hie_adt_flag,
    COALESCE(c.authorization_flag, 0) AS authorization_flag,
    COALESCE(c.emr_flag, 0) AS emr_flag,
    COALESCE(c.other_flag, 0) AS other_flag,
    COALESCE(c.unclassified_flag, 0) AS unclassified_flag,

    COALESCE(c.admission_source_count, 0) AS admission_source_count,
    COALESCE(c.discharge_source_count, 0) AS discharge_source_count,
    COALESCE(c.overall_source_count, 0) AS overall_source_count,

    a.admission_sources_notified,
    d.discharge_sources_notified,

    CASE WHEN COALESCE(c.admission_notification_flag, 0) = 0 THEN 1 ELSE 0 END AS missing_admission_notification_flag,
    CASE WHEN COALESCE(c.discharge_notification_flag, 0) = 0 THEN 1 ELSE 0 END AS missing_discharge_notification_flag,
    CASE WHEN COALESCE(c.overall_source_count, 0) >= 2 THEN 1 ELSE 0 END AS multi_source_flag
FROM tmp_events_2025 e
LEFT JOIN tmp_event_notification_counts c
    ON e.claim_event_id = c.claim_event_id
LEFT JOIN tmp_event_admission_source_list a
    ON e.claim_event_id = a.claim_event_id
LEFT JOIN tmp_event_discharge_source_list d
    ON e.claim_event_id = d.claim_event_id;
```

## What this improves

- The code is materially shorter and easier to scan because the repeated rank/filter wrappers are removed with `QUALIFY`. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/54907034/377c2b02-054b-4925-b4b3-5f393db3eaf6/CareAllies_ADT_Reconciliation_with_MedHOK_v4.sql?AWSAccessKeyId=ASIA2F3EMEYE7IFILXRD&Signature=mUCUSLM6oa45YyEfq84qIJ%2BMzZc%3D&x-amz-security-token=IQoJb3JpZ2luX2VjECsaCXVzLWVhc3QtMSJHMEUCICl%2BS1%2Fl%2Flpxex7vW6lQ16aIYfR%2BCveNac0tQLs9E7maAiEAl48xPy83jUWJtX%2BKPyLlXRpcaIpobm%2FT6xOPANzlUfIq%2FAQI9P%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARABGgw2OTk3NTMzMDk3MDUiDLxGjNyn7aqZAGuJxyrQBHDClI9BJ%2FOatjMxB3zJKMxS%2FmEj2xbY3ibXaawdiCN%2FcnwyIwdoy%2BqaOwjwcMTNr1MKYqP4%2BiqCUaBYWfdYStlC0VcUPxhz8Mh6rxcxbmbiDO5geOy97%2Bb0F46YSmzzV8TrP0aDG6cnj5sfakScRzgkeQHWakL%2BmFo5rN3%2Fsdu%2Bbh9dmZmGkgaTM7FDaoUdc6HG4%2F0OKkCXlpZT121sPy5wN9H64hR1SdbUygsJ6FrWjF3zY%2B1KJ6Oo9N9MiVWOPYE5%2FsPSUC3tfH9DOFlRxb53tn6qi399ww3LQlkYvGKKD6e3AlKUG9p4aPfjamHwZG%2BkT8y68vCYRl19h02J%2BQF5kbpMU77%2B43eLtETaqMuDm4RidcjtZRvqCEf9yliY5YaJRxURIFpTfMHyHGjUtng%2BrIsW8lra7ico0DSlHen4JOOxaxnMzsFLsz3iRJH%2BIMAW9NKdlV0PKZmeaO0uq9CPTGb3bkxXQXPL4f2EEHdAH6NMK8v9hRhL1bbW907ddeETgnhK3gzxVFnTaNxfrbImZsL5HgfY8AqJt4nDve5UxuSWljP8O17Jeg%2FNQzW0N%2FO9epSaP1sLogq7OVZCZRMYXuVb3mlqMI5Kk0VZs7wrDexyXIyb%2BoxS9qYCUc5jj2vZfJRTAy9apaR0C8wZq5hp0wZstTpXu%2FB6tIBVgxCR6b4ByBN9q4JslFeWcIF1szrazZtbhXvki4D9JCMBfxRYZpgjWsuo7bu5p29Sqhp4He75yXsLsDpU08DpObAVBexzw5qSTNVqgLBmPw606jAw%2BryJ0wY6mAEKPz3RXsPrB2BgcujS2BsK2wrPyLxfhLfscr9Vs9sBl7Vu33RS2w1Di1PNU651fky2VgSvAwzVhUvyJWaMtAu7K2nAs6TUbd7425fmbMKimqgpR0oTSoZq4gpvj7Wdr7EbGsGl5nKkcdT2p0tIf8rnD09GrIl6UGia%2BLFkSLmkInRlfFsaTpZ3RPNio%2F9%2BQXsW7Qp96dw03A%3D%3D&Expires=1784835149)
- Boolean precedence is made explicit in the two riskiest areas:
  - MedHOK ED inclusion logic,
  - clinical patient class fallback logic. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/54907034/377c2b02-054b-4925-b4b3-5f393db3eaf6/CareAllies_ADT_Reconciliation_with_MedHOK_v4.sql?AWSAccessKeyId=ASIA2F3EMEYE7IFILXRD&Signature=mUCUSLM6oa45YyEfq84qIJ%2BMzZc%3D&x-amz-security-token=IQoJb3JpZ2luX2VjECsaCXVzLWVhc3QtMSJHMEUCICl%2BS1%2Fl%2Flpxex7vW6lQ16aIYfR%2BCveNac0tQLs9E7maAiEAl48xPy83jUWJtX%2BKPyLlXRpcaIpobm%2FT6xOPANzlUfIq%2FAQI9P%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARABGgw2OTk3NTMzMDk3MDUiDLxGjNyn7aqZAGuJxyrQBHDClI9BJ%2FOatjMxB3zJKMxS%2FmEj2xbY3ibXaawdiCN%2FcnwyIwdoy%2BqaOwjwcMTNr1MKYqP4%2BiqCUaBYWfdYStlC0VcUPxhz8Mh6rxcxbmbiDO5geOy97%2Bb0F46YSmzzV8TrP0aDG6cnj5sfakScRzgkeQHWakL%2BmFo5rN3%2Fsdu%2Bbh9dmZmGkgaTM7FDaoUdc6HG4%2F0OKkCXlpZT121sPy5wN9H64hR1SdbUygsJ6FrWjF3zY%2B1KJ6Oo9N9MiVWOPYE5%2FsPSUC3tfH9DOFlRxb53tn6qi399ww3LQlkYvGKKD6e3AlKUG9p4aPfjamHwZG%2BkT8y68vCYRl19h02J%2BQF5kbpMU77%2B43eLtETaqMuDm4RidcjtZRvqCEf9yliY5YaJRxURIFpTfMHyHGjUtng%2BrIsW8lra7ico0DSlHen4JOOxaxnMzsFLsz3iRJH%2BIMAW9NKdlV0PKZmeaO0uq9CPTGb3bkxXQXPL4f2EEHdAH6NMK8v9hRhL1bbW907ddeETgnhK3gzxVFnTaNxfrbImZsL5HgfY8AqJt4nDve5UxuSWljP8O17Jeg%2FNQzW0N%2FO9epSaP1sLogq7OVZCZRMYXuVb3mlqMI5Kk0VZs7wrDexyXIyb%2BoxS9qYCUc5jj2vZfJRTAy9apaR0C8wZq5hp0wZstTpXu%2FB6tIBVgxCR6b4ByBN9q4JslFeWcIF1szrazZtbhXvki4D9JCMBfxRYZpgjWsuo7bu5p29Sqhp4He75yXsLsDpU08DpObAVBexzw5qSTNVqgLBmPw606jAw%2BryJ0wY6mAEKPz3RXsPrB2BgcujS2BsK2wrPyLxfhLfscr9Vs9sBl7Vu33RS2w1Di1PNU651fky2VgSvAwzVhUvyJWaMtAu7K2nAs6TUbd7425fmbMKimqgpR0oTSoZq4gpvj7Wdr7EbGsGl5nKkcdT2p0tIf8rnD09GrIl6UGia%2BLFkSLmkInRlfFsaTpZ3RPNio%2F9%2BQXsW7Qp96dw03A%3D%3D&Expires=1784835149)
- `DISTKEY`/`SORTKEY` were restored on the heavy temp tables to better fit Redshift’s join pattern on `person_id`. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/54907034/377c2b02-054b-4925-b4b3-5f393db3eaf6/CareAllies_ADT_Reconciliation_with_MedHOK_v4.sql?AWSAccessKeyId=ASIA2F3EMEYE7IFILXRD&Signature=mUCUSLM6oa45YyEfq84qIJ%2BMzZc%3D&x-amz-security-token=IQoJb3JpZ2luX2VjECsaCXVzLWVhc3QtMSJHMEUCICl%2BS1%2Fl%2Flpxex7vW6lQ16aIYfR%2BCveNac0tQLs9E7maAiEAl48xPy83jUWJtX%2BKPyLlXRpcaIpobm%2FT6xOPANzlUfIq%2FAQI9P%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARABGgw2OTk3NTMzMDk3MDUiDLxGjNyn7aqZAGuJxyrQBHDClI9BJ%2FOatjMxB3zJKMxS%2FmEj2xbY3ibXaawdiCN%2FcnwyIwdoy%2BqaOwjwcMTNr1MKYqP4%2BiqCUaBYWfdYStlC0VcUPxhz8Mh6rxcxbmbiDO5geOy97%2Bb0F46YSmzzV8TrP0aDG6cnj5sfakScRzgkeQHWakL%2BmFo5rN3%2Fsdu%2Bbh9dmZmGkgaTM7FDaoUdc6HG4%2F0OKkCXlpZT121sPy5wN9H64hR1SdbUygsJ6FrWjF3zY%2B1KJ6Oo9N9MiVWOPYE5%2FsPSUC3tfH9DOFlRxb53tn6qi399ww3LQlkYvGKKD6e3AlKUG9p4aPfjamHwZG%2BkT8y68vCYRl19h02J%2BQF5kbpMU77%2B43eLtETaqMuDm4RidcjtZRvqCEf9yliY5YaJRxURIFpTfMHyHGjUtng%2BrIsW8lra7ico0DSlHen4JOOxaxnMzsFLsz3iRJH%2BIMAW9NKdlV0PKZmeaO0uq9CPTGb3bkxXQXPL4f2EEHdAH6NMK8v9hRhL1bbW907ddeETgnhK3gzxVFnTaNxfrbImZsL5HgfY8AqJt4nDve5UxuSWljP8O17Jeg%2FNQzW0N%2FO9epSaP1sLogq7OVZCZRMYXuVb3mlqMI5Kk0VZs7wrDexyXIyb%2BoxS9qYCUc5jj2vZfJRTAy9apaR0C8wZq5hp0wZstTpXu%2FB6tIBVgxCR6b4ByBN9q4JslFeWcIF1szrazZtbhXvki4D9JCMBfxRYZpgjWsuo7bu5p29Sqhp4He75yXsLsDpU08DpObAVBexzw5qSTNVqgLBmPw606jAw%2BryJ0wY6mAEKPz3RXsPrB2BgcujS2BsK2wrPyLxfhLfscr9Vs9sBl7Vu33RS2w1Di1PNU651fky2VgSvAwzVhUvyJWaMtAu7K2nAs6TUbd7425fmbMKimqgpR0oTSoZq4gpvj7Wdr7EbGsGl5nKkcdT2p0tIf8rnD09GrIl6UGia%2BLFkSLmkInRlfFsaTpZ3RPNio%2F9%2BQXsW7Qp96dw03A%3D%3D&Expires=1784835149)

## What I intentionally did not change

- I kept clinical and MedHOK branches separate. That is the right call for auditability because MedHOK lacks usable `patient_class`, and its ambiguity resolution is genuinely different. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/54907034/377c2b02-054b-4925-b4b3-5f393db3eaf6/CareAllies_ADT_Reconciliation_with_MedHOK_v4.sql?AWSAccessKeyId=ASIA2F3EMEYE7IFILXRD&Signature=mUCUSLM6oa45YyEfq84qIJ%2BMzZc%3D&x-amz-security-token=IQoJb3JpZ2luX2VjECsaCXVzLWVhc3QtMSJHMEUCICl%2BS1%2Fl%2Flpxex7vW6lQ16aIYfR%2BCveNac0tQLs9E7maAiEAl48xPy83jUWJtX%2BKPyLlXRpcaIpobm%2FT6xOPANzlUfIq%2FAQI9P%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARABGgw2OTk3NTMzMDk3MDUiDLxGjNyn7aqZAGuJxyrQBHDClI9BJ%2FOatjMxB3zJKMxS%2FmEj2xbY3ibXaawdiCN%2FcnwyIwdoy%2BqaOwjwcMTNr1MKYqP4%2BiqCUaBYWfdYStlC0VcUPxhz8Mh6rxcxbmbiDO5geOy97%2Bb0F46YSmzzV8TrP0aDG6cnj5sfakScRzgkeQHWakL%2BmFo5rN3%2Fsdu%2Bbh9dmZmGkgaTM7FDaoUdc6HG4%2F0OKkCXlpZT121sPy5wN9H64hR1SdbUygsJ6FrWjF3zY%2B1KJ6Oo9N9MiVWOPYE5%2FsPSUC3tfH9DOFlRxb53tn6qi399ww3LQlkYvGKKD6e3AlKUG9p4aPfjamHwZG%2BkT8y68vCYRl19h02J%2BQF5kbpMU77%2B43eLtETaqMuDm4RidcjtZRvqCEf9yliY5YaJRxURIFpTfMHyHGjUtng%2BrIsW8lra7ico0DSlHen4JOOxaxnMzsFLsz3iRJH%2BIMAW9NKdlV0PKZmeaO0uq9CPTGb3bkxXQXPL4f2EEHdAH6NMK8v9hRhL1bbW907ddeETgnhK3gzxVFnTaNxfrbImZsL5HgfY8AqJt4nDve5UxuSWljP8O17Jeg%2FNQzW0N%2FO9epSaP1sLogq7OVZCZRMYXuVb3mlqMI5Kk0VZs7wrDexyXIyb%2BoxS9qYCUc5jj2vZfJRTAy9apaR0C8wZq5hp0wZstTpXu%2FB6tIBVgxCR6b4ByBN9q4JslFeWcIF1szrazZtbhXvki4D9JCMBfxRYZpgjWsuo7bu5p29Sqhp4He75yXsLsDpU08DpObAVBexzw5qSTNVqgLBmPw606jAw%2BryJ0wY6mAEKPz3RXsPrB2BgcujS2BsK2wrPyLxfhLfscr9Vs9sBl7Vu33RS2w1Di1PNU651fky2VgSvAwzVhUvyJWaMtAu7K2nAs6TUbd7425fmbMKimqgpR0oTSoZq4gpvj7Wdr7EbGsGl5nKkcdT2p0tIf8rnD09GrIl6UGia%2BLFkSLmkInRlfFsaTpZ3RPNio%2F9%2BQXsW7Qp96dw03A%3D%3D&Expires=1784835149)
- I kept admission and discharge as separate match targets. That supports your “coverage separately” requirement and keeps lag logic interpretable. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/54907034/377c2b02-054b-4925-b4b3-5f393db3eaf6/CareAllies_ADT_Reconciliation_with_MedHOK_v4.sql?AWSAccessKeyId=ASIA2F3EMEYE7IFILXRD&Signature=mUCUSLM6oa45YyEfq84qIJ%2BMzZc%3D&x-amz-security-token=IQoJb3JpZ2luX2VjECsaCXVzLWVhc3QtMSJHMEUCICl%2BS1%2Fl%2Flpxex7vW6lQ16aIYfR%2BCveNac0tQLs9E7maAiEAl48xPy83jUWJtX%2BKPyLlXRpcaIpobm%2FT6xOPANzlUfIq%2FAQI9P%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARABGgw2OTk3NTMzMDk3MDUiDLxGjNyn7aqZAGuJxyrQBHDClI9BJ%2FOatjMxB3zJKMxS%2FmEj2xbY3ibXaawdiCN%2FcnwyIwdoy%2BqaOwjwcMTNr1MKYqP4%2BiqCUaBYWfdYStlC0VcUPxhz8Mh6rxcxbmbiDO5geOy97%2Bb0F46YSmzzV8TrP0aDG6cnj5sfakScRzgkeQHWakL%2BmFo5rN3%2Fsdu%2Bbh9dmZmGkgaTM7FDaoUdc6HG4%2F0OKkCXlpZT121sPy5wN9H64hR1SdbUygsJ6FrWjF3zY%2B1KJ6Oo9N9MiVWOPYE5%2FsPSUC3tfH9DOFlRxb53tn6qi399ww3LQlkYvGKKD6e3AlKUG9p4aPfjamHwZG%2BkT8y68vCYRl19h02J%2BQF5kbpMU77%2B43eLtETaqMuDm4RidcjtZRvqCEf9yliY5YaJRxURIFpTfMHyHGjUtng%2BrIsW8lra7ico0DSlHen4JOOxaxnMzsFLsz3iRJH%2BIMAW9NKdlV0PKZmeaO0uq9CPTGb3bkxXQXPL4f2EEHdAH6NMK8v9hRhL1bbW907ddeETgnhK3gzxVFnTaNxfrbImZsL5HgfY8AqJt4nDve5UxuSWljP8O17Jeg%2FNQzW0N%2FO9epSaP1sLogq7OVZCZRMYXuVb3mlqMI5Kk0VZs7wrDexyXIyb%2BoxS9qYCUc5jj2vZfJRTAy9apaR0C8wZq5hp0wZstTpXu%2FB6tIBVgxCR6b4ByBN9q4JslFeWcIF1szrazZtbhXvki4D9JCMBfxRYZpgjWsuo7bu5p29Sqhp4He75yXsLsDpU08DpObAVBexzw5qSTNVqgLBmPw606jAw%2BryJ0wY6mAEKPz3RXsPrB2BgcujS2BsK2wrPyLxfhLfscr9Vs9sBl7Vu33RS2w1Di1PNU651fky2VgSvAwzVhUvyJWaMtAu7K2nAs6TUbd7425fmbMKimqgpR0oTSoZq4gpvj7Wdr7EbGsGl5nKkcdT2p0tIf8rnD09GrIl6UGia%2BLFkSLmkInRlfFsaTpZ3RPNio%2F9%2BQXsW7Qp96dw03A%3D%3D&Expires=1784835149)
- I did not attempt a single monolithic generic matching macro in SQL. In Redshift SQL, that usually becomes harder to maintain than duplicated-but-clear branches.

## Two follow-up recommendations

1. Add your QA block from v4 at the bottom unchanged or with minor updates, because this refactor should be validated as behavior-preserving against the current outputs. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/54907034/377c2b02-054b-4925-b4b3-5f393db3eaf6/CareAllies_ADT_Reconciliation_with_MedHOK_v4.sql?AWSAccessKeyId=ASIA2F3EMEYE7IFILXRD&Signature=mUCUSLM6oa45YyEfq84qIJ%2BMzZc%3D&x-amz-security-token=IQoJb3JpZ2luX2VjECsaCXVzLWVhc3QtMSJHMEUCICl%2BS1%2Fl%2Flpxex7vW6lQ16aIYfR%2BCveNac0tQLs9E7maAiEAl48xPy83jUWJtX%2BKPyLlXRpcaIpobm%2FT6xOPANzlUfIq%2FAQI9P%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARABGgw2OTk3NTMzMDk3MDUiDLxGjNyn7aqZAGuJxyrQBHDClI9BJ%2FOatjMxB3zJKMxS%2FmEj2xbY3ibXaawdiCN%2FcnwyIwdoy%2BqaOwjwcMTNr1MKYqP4%2BiqCUaBYWfdYStlC0VcUPxhz8Mh6rxcxbmbiDO5geOy97%2Bb0F46YSmzzV8TrP0aDG6cnj5sfakScRzgkeQHWakL%2BmFo5rN3%2Fsdu%2Bbh9dmZmGkgaTM7FDaoUdc6HG4%2F0OKkCXlpZT121sPy5wN9H64hR1SdbUygsJ6FrWjF3zY%2B1KJ6Oo9N9MiVWOPYE5%2FsPSUC3tfH9DOFlRxb53tn6qi399ww3LQlkYvGKKD6e3AlKUG9p4aPfjamHwZG%2BkT8y68vCYRl19h02J%2BQF5kbpMU77%2B43eLtETaqMuDm4RidcjtZRvqCEf9yliY5YaJRxURIFpTfMHyHGjUtng%2BrIsW8lra7ico0DSlHen4JOOxaxnMzsFLsz3iRJH%2BIMAW9NKdlV0PKZmeaO0uq9CPTGb3bkxXQXPL4f2EEHdAH6NMK8v9hRhL1bbW907ddeETgnhK3gzxVFnTaNxfrbImZsL5HgfY8AqJt4nDve5UxuSWljP8O17Jeg%2FNQzW0N%2FO9epSaP1sLogq7OVZCZRMYXuVb3mlqMI5Kk0VZs7wrDexyXIyb%2BoxS9qYCUc5jj2vZfJRTAy9apaR0C8wZq5hp0wZstTpXu%2FB6tIBVgxCR6b4ByBN9q4JslFeWcIF1szrazZtbhXvki4D9JCMBfxRYZpgjWsuo7bu5p29Sqhp4He75yXsLsDpU08DpObAVBexzw5qSTNVqgLBmPw606jAw%2BryJ0wY6mAEKPz3RXsPrB2BgcujS2BsK2wrPyLxfhLfscr9Vs9sBl7Vu33RS2w1Di1PNU651fky2VgSvAwzVhUvyJWaMtAu7K2nAs6TUbd7425fmbMKimqgpR0oTSoZq4gpvj7Wdr7EbGsGl5nKkcdT2p0tIf8rnD09GrIl6UGia%2BLFkSLmkInRlfFsaTpZ3RPNio%2F9%2BQXsW7Qp96dw03A%3D%3D&Expires=1784835149)
2. Before promoting this, compare counts between v4 and this version for:
   - total rows in `analytics.notification_match_detail_2025`
   - distinct `claim_event_id` with admission matches
   - distinct `claim_event_id` with discharge matches
   - distinct matched MedHOK claim events
   - final missing admission/discharge notification counts [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/54907034/377c2b02-054b-4925-b4b3-5f393db3eaf6/CareAllies_ADT_Reconciliation_with_MedHOK_v4.sql?AWSAccessKeyId=ASIA2F3EMEYE7IFILXRD&Signature=mUCUSLM6oa45YyEfq84qIJ%2BMzZc%3D&x-amz-security-token=IQoJb3JpZ2luX2VjECsaCXVzLWVhc3QtMSJHMEUCICl%2BS1%2Fl%2Flpxex7vW6lQ16aIYfR%2BCveNac0tQLs9E7maAiEAl48xPy83jUWJtX%2BKPyLlXRpcaIpobm%2FT6xOPANzlUfIq%2FAQI9P%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARABGgw2OTk3NTMzMDk3MDUiDLxGjNyn7aqZAGuJxyrQBHDClI9BJ%2FOatjMxB3zJKMxS%2FmEj2xbY3ibXaawdiCN%2FcnwyIwdoy%2BqaOwjwcMTNr1MKYqP4%2BiqCUaBYWfdYStlC0VcUPxhz8Mh6rxcxbmbiDO5geOy97%2Bb0F46YSmzzV8TrP0aDG6cnj5sfakScRzgkeQHWakL%2BmFo5rN3%2Fsdu%2Bbh9dmZmGkgaTM7FDaoUdc6HG4%2F0OKkCXlpZT121sPy5wN9H64hR1SdbUygsJ6FrWjF3zY%2B1KJ6Oo9N9MiVWOPYE5%2FsPSUC3tfH9DOFlRxb53tn6qi399ww3LQlkYvGKKD6e3AlKUG9p4aPfjamHwZG%2BkT8y68vCYRl19h02J%2BQF5kbpMU77%2B43eLtETaqMuDm4RidcjtZRvqCEf9yliY5YaJRxURIFpTfMHyHGjUtng%2BrIsW8lra7ico0DSlHen4JOOxaxnMzsFLsz3iRJH%2BIMAW9NKdlV0PKZmeaO0uq9CPTGb3bkxXQXPL4f2EEHdAH6NMK8v9hRhL1bbW907ddeETgnhK3gzxVFnTaNxfrbImZsL5HgfY8AqJt4nDve5UxuSWljP8O17Jeg%2FNQzW0N%2FO9epSaP1sLogq7OVZCZRMYXuVb3mlqMI5Kk0VZs7wrDexyXIyb%2BoxS9qYCUc5jj2vZfJRTAy9apaR0C8wZq5hp0wZstTpXu%2FB6tIBVgxCR6b4ByBN9q4JslFeWcIF1szrazZtbhXvki4D9JCMBfxRYZpgjWsuo7bu5p29Sqhp4He75yXsLsDpU08DpObAVBexzw5qSTNVqgLBmPw606jAw%2BryJ0wY6mAEKPz3RXsPrB2BgcujS2BsK2wrPyLxfhLfscr9Vs9sBl7Vu33RS2w1Di1PNU651fky2VgSvAwzVhUvyJWaMtAu7K2nAs6TUbd7425fmbMKimqgpR0oTSoZq4gpvj7Wdr7EbGsGl5nKkcdT2p0tIf8rnD09GrIl6UGia%2BLFkSLmkInRlfFsaTpZ3RPNio%2F9%2BQXsW7Qp96dw03A%3D%3D&Expires=1784835149)

If you want, I can next give you a **companion QA SQL pack** that compares this streamlined version to your current v4 output side by side.
