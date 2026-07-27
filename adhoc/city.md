Add the ADT geography at the selected-match level, then carry it into the event-level reconciliation table through the earliest admission and discharge records.

Do not create only one generic adt_city and adt_state initially. Admission and discharge notifications may come from different sources or locations. Keep:

* first_admission_adt_city
* first_admission_adt_state
* first_discharge_adt_city
* first_discharge_adt_state

You can then derive one overall city/state based on the earliest notification.

1. Confirm the fields exist in the detail table

SELECT
    adt_city,
    adt_state,
    COUNT(*) AS matched_rows
FROM analytics.notification_match_detail_2025
GROUP BY
    adt_city,
    adt_state
ORDER BY
    matched_rows DESC;

Your detail table should already contain:

adt_city,
adt_state

from adt_events_candidate.

⸻

2. Add city and state to the earliest admission notification table

Find the section that creates:

tmp_first_admission_notification

Add adt_city and adt_state to the selected columns.

DROP TABLE IF EXISTS tmp_first_admission_notification;
CREATE TEMP TABLE tmp_first_admission_notification AS
SELECT
    claim_event_id,
    source_category
        AS first_admission_source_category,
    sending_source
        AS first_admission_sending_source,
    adt_event_location
        AS first_admission_event_location,
    adt_city
        AS first_admission_adt_city,
    adt_state
        AS first_admission_adt_state,
    message_type
        AS first_admission_message_type,
    message_timestamp
        AS first_admission_message_timestamp,
    insert_timestamp
        AS first_admission_insert_timestamp,
    notification_lag_days
        AS admission_notification_lag_days,
    pipeline_lag_minutes
        AS admission_pipeline_lag_minutes,
    match_method
        AS admission_match_method,
    date_difference
        AS admission_date_difference,
    patient_class_match_status
        AS admission_patient_class_match_status,
    facility_match_flag
        AS admission_facility_match_flag,
    authorization_match_status
        AS first_admission_authorization_match_status,
    authorization_candidate_event_count
        AS first_admission_authorization_candidate_count,
    authorization_match_score
        AS first_admission_authorization_match_score
FROM
(
    SELECT
        d.*,
        ROW_NUMBER() OVER
        (
            PARTITION BY claim_event_id
            ORDER BY
                insert_timestamp ASC NULLS LAST,
                message_timestamp ASC NULLS LAST,
                adt_message_key
        ) AS notification_rn
    FROM analytics.notification_match_detail_2025 d
    WHERE match_target = 'ADMISSION'
) ranked
WHERE notification_rn = 1;

⸻

3. Add city and state to the earliest discharge notification table

DROP TABLE IF EXISTS tmp_first_discharge_notification;
CREATE TEMP TABLE tmp_first_discharge_notification AS
SELECT
    claim_event_id,
    source_category
        AS first_discharge_source_category,
    sending_source
        AS first_discharge_sending_source,
    adt_event_location
        AS first_discharge_event_location,
    adt_city
        AS first_discharge_adt_city,
    adt_state
        AS first_discharge_adt_state,
    message_type
        AS first_discharge_message_type,
    message_timestamp
        AS first_discharge_message_timestamp,
    insert_timestamp
        AS first_discharge_insert_timestamp,
    notification_lag_days
        AS discharge_notification_lag_days,
    pipeline_lag_minutes
        AS discharge_pipeline_lag_minutes,
    match_method
        AS discharge_match_method,
    date_difference
        AS discharge_date_difference,
    patient_class_match_status
        AS discharge_patient_class_match_status,
    facility_match_flag
        AS discharge_facility_match_flag,
    adt_match_date_inferred_flag
        AS adt_discharge_date_inferred_flag,
    authorization_match_status
        AS first_discharge_authorization_match_status,
    authorization_candidate_event_count
        AS first_discharge_authorization_candidate_count,
    authorization_match_score
        AS first_discharge_authorization_match_score
FROM
(
    SELECT
        d.*,
        ROW_NUMBER() OVER
        (
            PARTITION BY claim_event_id
            ORDER BY
                insert_timestamp ASC NULLS LAST,
                message_timestamp ASC NULLS LAST,
                adt_message_key
        ) AS notification_rn
    FROM analytics.notification_match_detail_2025 d
    WHERE match_target = 'DISCHARGE'
) ranked
WHERE notification_rn = 1;

⸻

4. Add the fields to notification_reconciliation_2025

In the final reconciliation-table creation, add the four new fields:

adm.first_admission_adt_city,
adm.first_admission_adt_state,
dch.first_discharge_adt_city,
dch.first_discharge_adt_state,

Example placement:

CREATE TABLE analytics.notification_reconciliation_2025 AS
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
    e.servicefacility,
    e.ipa_name,
    e.reporting_group,
    /* Coverage fields */
    n.admission_notification_flag,
    n.discharge_notification_flag,
    n.complete_admission_discharge_flag,
    n.no_matched_notification_flag,
    /* Earliest admission notification */
    adm.first_admission_source_category,
    adm.first_admission_sending_source,
    adm.first_admission_event_location,
    adm.first_admission_adt_city,
    adm.first_admission_adt_state,
    adm.first_admission_message_type,
    adm.first_admission_insert_timestamp,
    adm.admission_notification_lag_days,
    /* Earliest discharge notification */
    dch.first_discharge_source_category,
    dch.first_discharge_sending_source,
    dch.first_discharge_event_location,
    dch.first_discharge_adt_city,
    dch.first_discharge_adt_state,
    dch.first_discharge_message_type,
    dch.first_discharge_insert_timestamp,
    dch.discharge_notification_lag_days
FROM tmp_events_2025 e
LEFT JOIN tmp_event_notification_agg n
    ON e.claim_event_id = n.claim_event_id
LEFT JOIN tmp_first_admission_notification adm
    ON e.claim_event_id = adm.claim_event_id
LEFT JOIN tmp_first_discharge_notification dch
    ON e.claim_event_id = dch.claim_event_id;

Keep the rest of your existing reconciliation columns in the actual query.

⸻

5. Add an overall first-notification city and state

Leadership may prefer one geography field per event. Derive it from whichever notification became available first.

CASE
    WHEN adm.first_admission_insert_timestamp IS NULL
        THEN dch.first_discharge_adt_city
    WHEN dch.first_discharge_insert_timestamp IS NULL
        THEN adm.first_admission_adt_city
    WHEN adm.first_admission_insert_timestamp
         <= dch.first_discharge_insert_timestamp
        THEN adm.first_admission_adt_city
    ELSE dch.first_discharge_adt_city
END AS first_notification_adt_city,
CASE
    WHEN adm.first_admission_insert_timestamp IS NULL
        THEN dch.first_discharge_adt_state
    WHEN dch.first_discharge_insert_timestamp IS NULL
        THEN adm.first_admission_adt_state
    WHEN adm.first_admission_insert_timestamp
         <= dch.first_discharge_insert_timestamp
        THEN adm.first_admission_adt_state
    ELSE dch.first_discharge_adt_state
END AS first_notification_adt_state,

I would also derive the source associated with that geography:

CASE
    WHEN adm.first_admission_insert_timestamp IS NULL
        THEN dch.first_discharge_sending_source
    WHEN dch.first_discharge_insert_timestamp IS NULL
        THEN adm.first_admission_sending_source
    WHEN adm.first_admission_insert_timestamp
         <= dch.first_discharge_insert_timestamp
        THEN adm.first_admission_sending_source
    ELSE dch.first_discharge_sending_source
END AS first_notification_sending_source,

⸻

6. Important MedHOK consideration

MedHOK may have blank city and state values. Therefore, the overall first notification could be MedHOK and still have missing geography.

A better leadership geography may be the first non-authorization notification geography.

Create this separately:

DROP TABLE IF EXISTS tmp_first_non_authorization_geography;
CREATE TEMP TABLE tmp_first_non_authorization_geography AS
SELECT
    claim_event_id,
    sending_source
        AS first_non_auth_sending_source,
    adt_event_location
        AS first_non_auth_event_location,
    adt_city
        AS first_non_auth_adt_city,
    adt_state
        AS first_non_auth_adt_state
FROM
(
    SELECT
        d.*,
        ROW_NUMBER() OVER
        (
            PARTITION BY claim_event_id
            ORDER BY
                insert_timestamp ASC NULLS LAST,
                message_timestamp ASC NULLS LAST,
                adt_message_key
        ) AS geography_rn
    FROM analytics.notification_match_detail_2025 d
    WHERE source_category <> 'AUTHORIZATION'
      AND
      (
          adt_city IS NOT NULL
          OR adt_state IS NOT NULL
      )
) ranked
WHERE geography_rn = 1;

Then join it into the reconciliation table:

LEFT JOIN tmp_first_non_authorization_geography geo
    ON e.claim_event_id = geo.claim_event_id

And select:

geo.first_non_auth_sending_source,
geo.first_non_auth_event_location,
geo.first_non_auth_adt_city,
geo.first_non_auth_adt_state,

Recommended fields for leadership

Use these in the reduced extract:

servicefacility                     -- Claims facility
first_non_auth_sending_source       -- ADT/HIE source
first_non_auth_event_location       -- ADT location
first_non_auth_adt_city
first_non_auth_adt_state

This is preferable to geography coming from MedHOK because it reflects the clinical notification source.

Recommended pivot

Rows:
    first_non_auth_adt_state
    first_non_auth_adt_city
Columns:
    care_type
Values:
    Count of claim_event_id
    Average of complete_admission_discharge_flag
    Average of no_matched_notification_flag
Filters:
    reporting_group
    ipa_name
    first_non_auth_sending_source

Label the fields clearly as ADT source geography, not member residence or claims-facility geography.