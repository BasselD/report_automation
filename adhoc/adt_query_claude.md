Here's the full pipeline rewritten with `person_id` as the join key throughout, `source_category` classification folded into the candidate table, and the date-window fixes from before applied. [hl7-definition.caristix](https://hl7-definition.caristix.com/v2/HL7v2.3/TriggerEvents)

```sql
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
```

## What changed from your candidate query

- Widened the admit_date lower bound from `2024-12-01` to `2024-01-01` so long LTAC/SNF stays admitted well before December 2024 aren't dropped.
- Restricted the `message_timestamp` clause to only fire when both `admit_date` and `discharge_date` are null, so it acts purely as a fallback rather than a third independent inclusion path.
- Added `source_category` classification directly into the candidate pull (Step 1), based on `sending_source` patterns per HL7 ADT conventions. [hl7-definition.caristix](https://hl7-definition.caristix.com/v2/HL7v2.3/TriggerEvents)

# DQA
Good instinct — validating output is just as important as building the pipeline correctly. Here's a structured QA plan tailored specifically to this reconciliation process, organized by what could silently go wrong at each stage. [designgurus](https://www.designgurus.io/answers/detail/quality-assurance-techniques-to-validate-coding-solutions-quickly)

## Row count integrity checks

The single most important check is confirming that joins didn't inflate your event population.

```sql
SELECT COUNT(*) AS ip_raw_count FROM ip_raw;
SELECT COUNT(*) AS ed_obs_raw_count FROM ed_obs_raw;
SELECT COUNT(*) AS tmp_events_2025_count FROM tmp_events_2025;
SELECT COUNT(DISTINCT event_id) AS distinct_events FROM tmp_events_2025;

SELECT COUNT(*) AS final_row_count FROM analytics.notification_reconciliation_2025;
SELECT COUNT(DISTINCT event_id) AS distinct_final_events FROM analytics.notification_reconciliation_2025;
```

If `final_row_count` doesn't equal `distinct_final_events`, the `LEFT JOIN` to `tmp_event_notification_flags` fanned out — meaning a single event matched multiple notification rows and duplicated instead of aggregating. This is the exact kind of silent inflation that breaks percentages downstream. [dfe-analytical-services.github](https://dfe-analytical-services.github.io/how-to-qa/coding.html)

## Match method distribution sanity check

```sql
SELECT source_category, match_method, COUNT(*) 
FROM tmp_source_match_map
GROUP BY source_category, match_method
ORDER BY source_category, match_method;
```

You want to see exact matches vastly outnumbering fuzzy matches. If fuzzy matches dominate any one source, that suggests a systematic date offset (e.g., timezone shift, or admit_date vs. discharge_date confusion) rather than genuine day-to-day noise. [dfe-analytical-services.github](https://dfe-analytical-services.github.io/how-to-qa/coding.html)

## Coverage category cross-check

Cross-tab `care_type` against `notification_status` to see if gaps cluster in a way that makes clinical sense.

```sql
SELECT care_type, notification_status, COUNT(*) 
FROM analytics.notification_reconciliation_2025
GROUP BY care_type, notification_status
ORDER BY care_type, notification_status;
```

If ED visits show near-zero authorization matches, that's expected (ED usually isn't authorized), but if IP stays show near-zero authorization matches too, that's a red flag worth investigating, since IP typically requires prior auth. [getempowerhealth](https://getempowerhealth.com/for-providers/utilization-management/prior-authorization-list/)

## Spot-check a known sample

Pick 10–15 events with known outcomes — ones you can manually trace in source systems — and confirm the pipeline classifies them correctly. This is the single highest-value QA step because automated checks can't catch logic errors that "look" correct in aggregate. [reddit](https://www.reddit.com/r/ExperiencedDevs/comments/1rzq738/what_tools_and_techniques_are_you_using_to_verify/)

```sql
SELECT * FROM analytics.notification_reconciliation_2025
WHERE person_id IN (12345, 67890, ...)
ORDER BY person_id, event_admit_date;
```

Manually verify: does the matched ADT record's admit date actually align with the claim? Is the source classification correct given the actual `sending_source` value?

## Source classification validation

Confirm your `LIKE` patterns in `source_category` aren't missing or misclassifying values.

```sql
SELECT sending_source, source_category, COUNT(*) 
FROM adt_events_candidate
GROUP BY sending_source, source_category
ORDER BY source_category, COUNT(*) DESC;
```

Look specifically for high-volume `sending_source` values landing in `OTHER` or `UNCLASSIFIED` — that means your patterns missed a major feed, and your EMR/HIE counts are understated. [hl7-definition.caristix](https://hl7-definition.caristix.com/v2/HL7v2.3/TriggerEvents)

## Duplicate and fan-out check on the match map

```sql
SELECT event_id, source_category, COUNT(*) 
FROM tmp_source_match_map
GROUP BY event_id, source_category
HAVING COUNT(*) > 1;
```

This should return zero rows. If it doesn't, one event matched two ADT records from the same source, meaning your fuzzy match `ROW_NUMBER` tie-breaking logic let a duplicate through. [designgurus](https://www.designgurus.io/answers/detail/quality-assurance-techniques-to-validate-coding-solutions-quickly)

## Discharge date null-rate check

Since ED/OBS discharge dates are sometimes inferred, and IP discharge is what anchors eligibility, confirm nulls aren't hiding a bigger problem.

```sql
SELECT care_type, 
       SUM(discharge_date_inferred_flag) AS inferred_count,
       COUNT(*) AS total_count,
       ROUND(100.0 * SUM(discharge_date_inferred_flag) / COUNT(*), 2) AS pct_inferred
FROM analytics.notification_reconciliation_2025
GROUP BY care_type;
```

## Reasonableness check against known benchmarks

Compare your `percent_no_notification` by care_type against what you'd expect operationally. If ED shows dramatically higher no-notification rates than IP, that's plausible (ED visits are shorter and often skip formal ADT), but if IP shows a high no-notification rate, that's the number leadership will scrutinize hardest, so trace a sample of those specific "No Notification" IP events back to source before presenting. [dss.mo](https://dss.mo.gov/mhd/hie-onboarding/files/RecommendedADTSpecifications.pdf)

## Trend check across time

```sql
SELECT DATE_TRUNC('month', event_admit_date) AS admit_month,
       care_type,
       COUNT(*) AS total_events,
       SUM(no_notification_flag) AS no_notification,
       ROUND(100.0 * SUM(no_notification_flag) / COUNT(*), 2) AS pct_no_notification
FROM analytics.notification_reconciliation_2025
GROUP BY DATE_TRUNC('month', event_admit_date), care_type
ORDER BY admit_month, care_type;
```

A sudden spike or drop in a specific month often points to a feed outage, a facility onboarding/offboarding, or a code change — this is one of the fastest ways to catch a data quality issue rather than a true trend. [dfe-analytical-services.github](https://dfe-analytical-services.github.io/how-to-qa/coding.html)

## Recommended QA sequence

| Step | What it catches |
|---|---|
| Row count integrity | Join fan-out / duplication |
| Match method distribution | Systematic date offset issues |
| Coverage cross-tab | Illogical clinical patterns |
| Manual spot-check | Logic errors invisible in aggregate |
| Source classification audit | Misclassified/missed feeds |
| Duplicate match check | Fuzzy-match tie-break failures |
| Discharge null-rate | Inferred-date reliability |
| Monthly trend | Feed outages, onboarding gaps |

Run these roughly in that order — the row-count and duplicate checks first, since if those fail, everything downstream is unreliable regardless of how reasonable the final numbers look. [designgurus](https://www.designgurus.io/answers/detail/quality-assurance-techniques-to-validate-coding-solutions-quickly)
- Switched every join and grouping key from `member_id`/`memberno` to `person_id`, matching `ip_raw`/`ed_obs_raw`.

One thing to sanity-check on your end: verify the actual distinct values in `sending_source` for MedHOK, EMR, and HIE feeds, since the `LIKE` patterns here are best guesses — if your feed uses different labels (e.g., a specific EHR vendor code instead of "EMR"), the `source_category` buckets will misclassify silently rather than error out.
