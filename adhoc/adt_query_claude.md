/*==============================================================================
  0. PARAMETERS
==============================================================================*/
DROP TABLE IF EXISTS tmp_params;
CREATE TEMP TABLE tmp_params AS
SELECT
    DATE '2025-01-01' AS start_date,
    DATE '2026-01-01' AS end_date,
    1::INTEGER       AS fuzzy_days;

/*==============================================================================
  1. ADT CANDIDATE PULL — full universe, no enrollment join
     Wide net: catches long stays admitted in 2024, discharged in 2025.
     Fallback clause only applies to records missing both admit/discharge dates.
==============================================================================*/
DROP TABLE IF EXISTS adt_events_candidate;
CREATE TEMP TABLE adt_events_candidate AS

SELECT
    adt.person_id AS personid,

    CAST(adt.admit_date AS DATE) AS admit_date,
    CAST(adt.discharge_date AS DATE) AS discharge_date,

    adt.diagnosis_date,
    adt.insert_timestamp,
    adt.message_timestamp,

    adt.adt_message_id,
    adt.sending_source,
    adt.event_id,
    UPPER(TRIM(adt.message_type)) AS message_type,

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
    END AS source_category

FROM qdwwh.dbo_adt adt

WHERE
(
       CAST(adt.admit_date AS DATE) >= DATE '2024-01-01'
       AND CAST(adt.admit_date AS DATE) <  DATE '2026-01-01'

    OR CAST(adt.discharge_date AS DATE)
           BETWEEN DATE '2025-01-01' AND DATE '2025-12-31'

    OR (
           adt.admit_date IS NULL
       AND adt.discharge_date IS NULL
       AND CAST(adt.message_timestamp AS DATE)
               BETWEEN DATE '2025-01-01' AND DATE '2025-12-31'
       )
);

/*==============================================================================
  2. COLLAPSE ADT TO ONE ROW PER PERSON + ADMIT DATE + SOURCE CATEGORY
==============================================================================*/
DROP TABLE IF EXISTS tmp_adt_events;
CREATE TEMP TABLE tmp_adt_events AS

SELECT
    ROW_NUMBER() OVER (ORDER BY personid, admit_date, source_category) ::BIGINT AS adt_event_id,
    personid,
    admit_date AS adt_admit_date,
    source_category,

    MAX(CASE WHEN message_type IN ('A01','A04','A05','A06') THEN admit_date END) AS admission_msg_date,
    MAX(CASE WHEN message_type = 'A03' THEN discharge_date END)                  AS a03_discharge_date,
    MAX(discharge_date)                                                          AS any_discharge_date,

    LISTAGG(DISTINCT sending_source, ', ') AS sending_sources,
    LISTAGG(DISTINCT message_type, ',')    AS message_types_seen

FROM adt_events_candidate
WHERE source_category IN ('AUTHORIZATION', 'HIE_ADT', 'EMR', 'OTHER')
  AND admit_date IS NOT NULL
GROUP BY personid, admit_date, source_category;

/*==============================================================================
  3. UNIFY IP + ED + OBS EVENTS (person_id-based, from your ip_raw/ed_obs_raw)
==============================================================================*/
DROP TABLE IF EXISTS tmp_events_2025;
CREATE TEMP TABLE tmp_events_2025 AS

SELECT
    eventid AS event_id, personid AS person_id, 'IP' AS care_type, bedtype AS care_subtype,
    admitdate AS event_admit_date, dischargedate AS event_discharge_date,
    0 AS discharge_date_inferred_flag, servicefacility, providernpi, providerspecialty,
    ipa_name, reporting_group
FROM ip_raw

UNION ALL

SELECT
    eventid AS event_id, personid AS person_id, care_setting AS care_type, care_setting AS care_subtype,
    admitdate AS event_admit_date, dischargedate AS event_discharge_date,
    discharge_date_inferred_flag, servicefacility, providernpi, providerspecialty,
    ipa_name, reporting_group
FROM ed_obs_raw;

/*==============================================================================
  4. EXACT + FUZZY MATCH — person_id based
==============================================================================*/
DROP TABLE IF EXISTS tmp_source_match_map;
CREATE TEMP TABLE tmp_source_match_map AS

WITH exact_match AS
(
    SELECT e.event_id, a.adt_event_id, a.source_category,
           'Exact Admit Date' AS match_method, 0 AS admit_date_difference
    FROM tmp_events_2025 e
    INNER JOIN tmp_adt_events a
        ON e.person_id = a.personid
       AND e.event_admit_date = a.adt_admit_date
),
fuzzy_candidates AS
(
    SELECT
        e.event_id, a.adt_event_id, a.source_category,
        DATEDIFF(day, e.event_admit_date, a.adt_admit_date) AS admit_date_difference,
        'Fuzzy Admit Date +/-1 Day' AS match_method,
        ROW_NUMBER() OVER (
            PARTITION BY e.event_id, a.source_category
            ORDER BY ABS(DATEDIFF(day, e.event_admit_date, a.adt_admit_date))
        ) AS event_rn,
        ROW_NUMBER() OVER (
            PARTITION BY a.adt_event_id
            ORDER BY ABS(DATEDIFF(day, e.event_admit_date, a.adt_admit_date))
        ) AS adt_rn
    FROM tmp_events_2025 e
    INNER JOIN tmp_adt_events a ON e.person_id = a.personid
    LEFT JOIN exact_match ex ON e.event_id = ex.event_id AND a.source_category = ex.source_category
    CROSS JOIN tmp_params p
    WHERE ex.event_id IS NULL
      AND ABS(DATEDIFF(day, e.event_admit_date, a.adt_admit_date)) <= p.fuzzy_days
),
fuzzy_match AS
(
    SELECT event_id, adt_event_id, source_category, match_method, admit_date_difference
    FROM fuzzy_candidates
    WHERE event_rn = 1 AND adt_rn = 1
)
SELECT event_id, adt_event_id, source_category, match_method, admit_date_difference FROM exact_match
UNION ALL
SELECT event_id, adt_event_id, source_category, match_method, admit_date_difference FROM fuzzy_match;

/*==============================================================================
  5. PIVOT TO PER-EVENT SOURCE FLAGS
==============================================================================*/
DROP TABLE IF EXISTS tmp_event_notification_flags;
CREATE TEMP TABLE tmp_event_notification_flags AS
SELECT
    event_id,
    MAX(CASE WHEN source_category = 'HIE_ADT' THEN 1 ELSE 0 END) AS hie_adt_flag,
    MAX(CASE WHEN source_category = 'AUTHORIZATION' THEN 1 ELSE 0 END) AS authorization_flag,
    MAX(CASE WHEN source_category = 'EMR' THEN 1 ELSE 0 END) AS emr_flag,
    MAX(CASE WHEN source_category = 'OTHER' THEN 1 ELSE 0 END) AS other_flag,
    COUNT(DISTINCT source_category) AS distinct_source_count,
    LISTAGG(DISTINCT source_category, ', ') AS sources_notified
FROM tmp_source_match_map
GROUP BY event_id;

/*==============================================================================
  6. FINAL EVENT-LEVEL RECONCILIATION TABLE
==============================================================================*/
DROP TABLE IF EXISTS analytics.notification_reconciliation_2025;
CREATE TABLE analytics.notification_reconciliation_2025 AS
SELECT
    e.event_id, e.person_id, e.care_type, e.care_subtype,
    e.event_admit_date, e.event_discharge_date, e.discharge_date_inferred_flag,
    e.servicefacility, e.providernpi, e.providerspecialty, e.ipa_name, e.reporting_group,

    COALESCE(f.hie_adt_flag, 0) AS hie_adt_flag,
    COALESCE(f.authorization_flag, 0) AS authorization_flag,
    COALESCE(f.emr_flag, 0) AS emr_flag,
    COALESCE(f.other_flag, 0) AS other_flag,
    COALESCE(f.distinct_source_count, 0) AS distinct_source_count,
    f.sources_notified,

    CASE WHEN COALESCE(f.distinct_source_count, 0) = 0 THEN 1 ELSE 0 END AS no_notification_flag,
    CASE WHEN COALESCE(f.distinct_source_count, 0) >= 2 THEN 1 ELSE 0 END AS multi_source_flag,

    CASE
        WHEN COALESCE(f.distinct_source_count, 0) = 0 THEN 'No Notification'
        WHEN f.distinct_source_count = 1 THEN f.sources_notified
        ELSE 'Multiple Sources: ' || f.sources_notified
    END AS notification_status
FROM tmp_events_2025 e
LEFT JOIN tmp_event_notification_flags f ON e.event_id = f.event_id;

/*==============================================================================
  7. COVERAGE GAP ROLLUP
==============================================================================*/
DROP TABLE IF EXISTS analytics.notification_coverage_gaps_2025;
CREATE TABLE analytics.notification_coverage_gaps_2025 AS
SELECT
    reporting_group, ipa_name, servicefacility, care_type,
    COUNT(*) AS total_events,
    SUM(no_notification_flag) AS events_with_no_notification,
    ROUND(100.0 * SUM(no_notification_flag) / NULLIF(COUNT(*), 0), 2) AS percent_no_notification,
    SUM(hie_adt_flag) AS hie_adt_events,
    SUM(authorization_flag) AS authorization_events,
    SUM(emr_flag) AS emr_events,
    SUM(other_flag) AS other_source_events,
    SUM(multi_source_flag) AS multi_source_events
FROM analytics.notification_reconciliation_2025
GROUP BY reporting_group, ipa_name, servicefacility, care_type;
