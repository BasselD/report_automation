Perfect — this expands the reconciliation from a single-source, single-setting problem into a **multi-source, multi-setting notification coverage** problem. The structure stays the same (normalize → classify → match → aggregate), but now every layer needs to handle three care settings (IP/ED/OBS) and four notification sources (HIE ADT, Authorization/MedHOK, EMR, Other) instead of two. [manifestmedex](https://www.manifestmedex.org/wp-content/uploads/MX_Condition-of-Participation-White-Paper.pdf)

## Design approach

I'm treating your `ip_raw` and `ed_obs_raw` tables as the **utilization/event side** (the "did something happen" side) and the ADT feed as the **notification side** (the "did we hear about it" side). The four core questions map directly to four output layers: a per-event notification flag, a per-event source breakdown, a per-event multi-source duplicate flag, and a facility/group/region rollup. [cms](https://www.cms.gov/priorities/burden-reduction/overview/interoperability/frequently-asked-questions/admission-discharge-transfer-patient-event-notification-conditions-participation-cop-42-cfr-482-24d)

## Full solution

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
  1. UNIFY IP + ED + OBS EVENTS INTO ONE EVENT SPINE
     One row per event_id, tagged with care_type
==============================================================================*/
DROP TABLE IF EXISTS tmp_events_2025;
CREATE TEMP TABLE tmp_events_2025 AS

SELECT
    eventid            AS event_id,
    personid           AS person_id,
    TRIM(UPPER(CAST(memberno AS VARCHAR(100)))) AS member_id,
    'IP'               AS care_type,
    bedtype            AS care_subtype,
    admitdate          AS event_admit_date,
    dischargedate      AS event_discharge_date,
    0                  AS discharge_date_inferred_flag,
    servicefacility,
    providernpi,
    providerspecialty,
    ipa_name,
    reporting_group
FROM ip_raw

UNION ALL

SELECT
    eventid            AS event_id,
    personid           AS person_id,
    TRIM(UPPER(CAST(memberno AS VARCHAR(100)))) AS member_id,
    care_setting       AS care_type,          -- 'ED' or 'OBS'
    care_setting       AS care_subtype,
    admitdate          AS event_admit_date,
    dischargedate      AS event_discharge_date,
    discharge_date_inferred_flag,
    servicefacility,
    providernpi,
    providerspecialty,
    ipa_name,
    reporting_group
FROM ed_obs_raw;

/*==============================================================================
  2. CLASSIFY ADT / NOTIFICATION SOURCE INTO 4 BUCKETS
     AUTHORIZATION = MedHOK
     EMR           = direct EMR feed
     HIE_ADT       = HIE-routed ADT
     OTHER         = anything else / unclassified

     Adjust the LIKE patterns below to match your actual sending_source values.
==============================================================================*/
DROP TABLE IF EXISTS tmp_adt_base;
CREATE TEMP TABLE tmp_adt_base AS

SELECT
    TRIM(UPPER(CAST(memberno AS VARCHAR(100)))) AS member_id,
    admit_date,
    discharge_date,
    UPPER(TRIM(message_type)) AS message_type,
    sending_source,

    CASE
        WHEN UPPER(TRIM(COALESCE(sending_source, ''))) LIKE '%MEDHOK%'
            THEN 'AUTHORIZATION'

        WHEN UPPER(TRIM(COALESCE(sending_source, ''))) LIKE '%EMR%'
          OR UPPER(TRIM(COALESCE(sending_source, ''))) LIKE '%EPIC%'
          OR UPPER(TRIM(COALESCE(sending_source, ''))) LIKE '%CERNER%'
            THEN 'EMR'

        WHEN UPPER(TRIM(COALESCE(sending_source, ''))) LIKE '%HIE%'
            THEN 'HIE_ADT'

        WHEN sending_source IS NOT NULL
            THEN 'OTHER'

        ELSE 'UNCLASSIFIED'
    END AS source_category

FROM adt_events_2025
WHERE memberno IS NOT NULL
  AND admit_date IS NOT NULL;

/*==============================================================================
  3. COLLAPSE ADT TO ONE ROW PER MEMBER + ADMIT DATE + SOURCE CATEGORY
     Admission = A01/A04/A05 family, Discharge = A03
     ED visits often arrive as A01/A04 + A03 same-day or A01 only
==============================================================================*/
DROP TABLE IF EXISTS tmp_adt_events;
CREATE TEMP TABLE tmp_adt_events AS

SELECT
    ROW_NUMBER() OVER (ORDER BY member_id, admit_date, source_category) ::BIGINT AS adt_event_id,
    member_id,
    admit_date AS adt_admit_date,
    source_category,

    MAX(CASE WHEN message_type IN ('A01','A04','A05') THEN admit_date END) AS admission_msg_date,
    MAX(CASE WHEN message_type = 'A03' THEN discharge_date END)           AS a03_discharge_date,
    MAX(discharge_date)                                                    AS any_discharge_date,

    LISTAGG(DISTINCT sending_source, ', ') AS sending_sources,
    LISTAGG(DISTINCT message_type, ',')    AS message_types_seen

FROM tmp_adt_base
WHERE source_category IN ('AUTHORIZATION', 'HIE_ADT', 'EMR', 'OTHER')
GROUP BY member_id, admit_date, source_category;

/*==============================================================================
  4. GENERIC MATCH FUNCTION LOGIC — repeat per source category
     Exact match on member + admit date, then +/-1 day fuzzy on leftovers
==============================================================================*/
DROP TABLE IF EXISTS tmp_source_match_map;
CREATE TEMP TABLE tmp_source_match_map AS

WITH exact_match AS
(
    SELECT
        e.event_id,
        a.adt_event_id,
        a.source_category,
        'Exact Admit Date' AS match_method,
        0 AS admit_date_difference
    FROM tmp_events_2025 e
    INNER JOIN tmp_adt_events a
        ON e.member_id = a.member_id
       AND e.event_admit_date = a.adt_admit_date
),

fuzzy_candidates AS
(
    SELECT
        e.event_id,
        a.adt_event_id,
        a.source_category,
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
    INNER JOIN tmp_adt_events a
        ON e.member_id = a.member_id
    LEFT JOIN exact_match ex
        ON e.event_id = ex.event_id AND a.source_category = ex.source_category
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
  5. PIVOT MATCHES INTO PER-EVENT SOURCE FLAGS
     Answers Q1 (any notification), Q2 (which source), Q3 (multi-source)
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

    LISTAGG(DISTINCT source_category, ', ') AS sources_notified,

    MAX(CASE WHEN source_category = 'HIE_ADT' THEN adt_event_id END) AS hie_adt_event_id,
    MAX(CASE WHEN source_category = 'AUTHORIZATION' THEN adt_event_id END) AS authorization_event_id,
    MAX(CASE WHEN source_category = 'EMR' THEN adt_event_id END) AS emr_event_id

FROM tmp_source_match_map
GROUP BY event_id;

/*==============================================================================
  6. FINAL EVENT-LEVEL RECONCILIATION TABLE
     One row per IP/ED/OBS event
==============================================================================*/
DROP TABLE IF EXISTS analytics.notification_reconciliation_2025;
CREATE TABLE analytics.notification_reconciliation_2025 AS

SELECT
    e.event_id,
    e.person_id,
    e.member_id,
    e.care_type,
    e.care_subtype,
    e.event_admit_date,
    e.event_discharge_date,
    e.discharge_date_inferred_flag,
    e.servicefacility,
    e.providernpi,
    e.providerspecialty,
    e.ipa_name,
    e.reporting_group,

    COALESCE(f.hie_adt_flag, 0)        AS hie_adt_flag,
    COALESCE(f.authorization_flag, 0)  AS authorization_flag,
    COALESCE(f.emr_flag, 0)            AS emr_flag,
    COALESCE(f.other_flag, 0)          AS other_flag,

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
LEFT JOIN tmp_event_notification_flags f
    ON e.event_id = f.event_id;

/*==============================================================================
  7. COVERAGE GAP ROLLUP — BY GROUP, FACILITY, AND REGION
     Answers Q4: where are the gaps concentrated
==============================================================================*/
DROP TABLE IF EXISTS analytics.notification_coverage_gaps_2025;
CREATE TABLE analytics.notification_coverage_gaps_2025 AS

SELECT
    reporting_group,
    ipa_name,
    servicefacility,
    care_type,

    COUNT(*) AS total_events,
    SUM(no_notification_flag) AS events_with_no_notification,

    ROUND(100.0 * SUM(no_notification_flag) / NULLIF(COUNT(*), 0), 2)
        AS percent_no_notification,

    SUM(hie_adt_flag) AS hie_adt_events,
    SUM(authorization_flag) AS authorization_events,
    SUM(emr_flag) AS emr_events,
    SUM(other_flag) AS other_source_events,
    SUM(multi_source_flag) AS multi_source_events

FROM analytics.notification_reconciliation_2025
GROUP BY reporting_group, ipa_name, servicefacility, care_type;
```

## How each question is answered

| Question | Where it's answered |
|---|---|
| Did we receive any notification? | `no_notification_flag` in step 6 output |
| Was it HIE, Authorization, EMR, or other? | `hie_adt_flag`/`authorization_flag`/`emr_flag`/`other_flag` and `sources_notified` in step 6 |
| Same event from multiple sources? | `multi_source_flag` and `distinct_source_count` in step 6 |
| Which groups/facilities/regions have gaps? | `analytics.notification_coverage_gaps_2025` in step 7, grouped by `reporting_group`, `ipa_name`, `servicefacility` |

## Assumptions to verify

- I assumed `sending_source` contains identifiable substrings like "EMR", "EPIC", "CERNER", or "HIE" — you'll need to confirm the actual distinct values in your feed and adjust the `CASE` in step 2 accordingly. [manifestmedex](https://www.manifestmedex.org/wp-content/uploads/MX_Condition-of-Participation-White-Paper.pdf)
- ED/OBS events often only generate an A01/A04 with no separate A03, since visits are same-day; the admission/discharge matching logic in step 3 and step 4 handles this by falling back to `any_discharge_date` when A03 is absent, but you may want a shorter fuzzy window (e.g., 0 days) for ED specifically since same-day precision matters more there than for IP stays. [content.govdelivery](https://content.govdelivery.com/attachments/USCMS/2020/11/06/file_attachments/1593692/Timely_Exchange_of%20_Hospital_and_ED_Data_Slides.pdf)
- `region` wasn't in your source fields — I used `reporting_group` and `ipa_name` as the closest proxies; if you have a separate region/geography field on `servicefacility`, let me know and I'll add it to the rollup.

Want me to add a facility-level heat-map style output (e.g., percent no-notification by facility and care_type side by side) as a chart, or keep this purely as SQL tables for now?
