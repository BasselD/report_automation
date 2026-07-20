Your current query is understandable, but for this reconciliation it should be changed in a more fundamental way than simply replacing `admit_date` with `discharge_date`.

## The main issue

Your **claims table should define the CareAllies-eligible denominator**.

The ADT table should represent the available notification universe. If you also independently filter ADT through enrollment, you risk excluding legitimate notifications that should be matched to an eligible claim.

For example:

* Member admitted December 28, 2024
* Member discharged January 3, 2025
* Member is eligible on January 3
* A01 admission message occurred in December 2024
* A03 discharge message occurred in January 2025

Your current filter:

```sql
WHERE adt.admit_date >= DATE '2025-01-01'
  AND adt.admit_date <  DATE '2026-01-01'
```

would exclude the December 2024 admission notification, even though the hospitalization belongs in your 2025 discharge cohort.

Changing the filter entirely to `discharge_date` would create the opposite problem because admission messages often have no discharge date yet.

# Recommended approach

## 1. Let claims control eligibility

Use the CareAllies enrollment filter in:

* `ip_raw`
* `ed_obs_raw`

Those tables define which events belong in the analysis.

Then match all relevant ADT notifications to those eligible events.

## 2. Do not require enrollment in the ADT extraction

You generally do not need this join in the base ADT table:

```sql
INNER JOIN sandbox.CA_Core_Enrollment_Detail enr
    ON adt.person_id = enr.personid
```

The IPA and reporting group should come from the enrollment row associated with the **claims event**, not independently from the ADT message.

Otherwise, the same hospitalization could receive different attribution depending on whether you use:

* admission month
* discharge month
* message month
* enrollment snapshot month

That creates attribution inconsistencies.

# Recommended ADT base query

```sql
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
        WHEN UPPER(TRIM(adt.message_type)) IN ('A01', 'A04', 'A06')
            THEN 'ADMISSION'

        WHEN UPPER(TRIM(adt.message_type)) = 'A03'
            THEN 'DISCHARGE'

        WHEN UPPER(TRIM(adt.message_type)) = 'A02'
            THEN 'TRANSFER'

        WHEN UPPER(TRIM(adt.message_type)) = 'A08'
            THEN 'UPDATE'

        ELSE 'OTHER'
    END AS notification_role

FROM qdwwh.dbo_adt adt

WHERE
(
       CAST(adt.admit_date AS DATE)
           BETWEEN DATE '2024-12-01' AND DATE '2025-12-31'

    OR CAST(adt.discharge_date AS DATE)
           BETWEEN DATE '2025-01-01' AND DATE '2025-12-31'

    OR CAST(adt.message_timestamp AS DATE)
           BETWEEN DATE '2024-12-01' AND DATE '2025-12-31'
);
```

The December 2024 buffer allows you to capture admission messages associated with early-2025 discharges. You could widen the buffer if very long inpatient stays are possible.

# Better candidate-window logic

A stronger version is to derive the minimum and maximum dates directly from your claims spine:

```sql
DROP TABLE IF EXISTS adt_events_candidate;

CREATE TEMP TABLE adt_events_candidate AS

WITH claim_window AS
(
    SELECT
        DATEADD(day, -7, MIN(claim_admit_date)) AS min_candidate_date,
        DATEADD(day,  7, MAX(claim_discharge_date)) AS max_candidate_date

    FROM tmp_claims_2025
)

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
        WHEN UPPER(TRIM(adt.message_type)) IN ('A01', 'A04', 'A06')
            THEN 'ADMISSION'

        WHEN UPPER(TRIM(adt.message_type)) = 'A03'
            THEN 'DISCHARGE'

        WHEN UPPER(TRIM(adt.message_type)) = 'A02'
            THEN 'TRANSFER'

        WHEN UPPER(TRIM(adt.message_type)) = 'A08'
            THEN 'UPDATE'

        ELSE 'OTHER'
    END AS notification_role

FROM qdwwh.dbo_adt adt

CROSS JOIN claim_window w

WHERE
(
       CAST(adt.admit_date AS DATE)
           BETWEEN w.min_candidate_date AND w.max_candidate_date

    OR CAST(adt.discharge_date AS DATE)
           BETWEEN w.min_candidate_date AND w.max_candidate_date

    OR CAST(adt.message_timestamp AS DATE)
           BETWEEN w.min_candidate_date AND w.max_candidate_date
);
```

This keeps the ADT extraction aligned with the actual claims population rather than using an arbitrary calendar-year restriction.

# How to retrieve `memberno`

Your current query gets `memberno` from enrollment:

```sql
REPLACE(enr.memberno, '*', '')
```

If ADT only has `person_id`, you have two reasonable choices.

## Preferred

Match ADT to claims using `personid`, since both sources contain it.

```sql
ON claims.personid = adt.personid
```

This avoids enrollment-snapshot ambiguity.

## Alternative

Create a separate person-to-member crosswalk:

```sql
DROP TABLE IF EXISTS person_member_xwalk;

CREATE TEMP TABLE person_member_xwalk AS

SELECT
    personid,
    REPLACE(memberno, '*', '') AS memberno

FROM
(
    SELECT
        personid,
        memberno,

        ROW_NUMBER() OVER
        (
            PARTITION BY personid
            ORDER BY enddate DESC, startdate DESC
        ) AS rn

    FROM sandbox.CA_Core_Enrollment_Detail

    WHERE carealliesmanagedflag = 'Y'
      AND planpayer = 'Cigna MA'
) x

WHERE rn = 1;
```

Then left join it to ADT for descriptive purposes. Do not use that join to determine whether the ADT record is retained.

# If you must retain enrollment filtering in ADT

Then use a role-specific enrollment date rather than always `admit_date`.

```sql
CASE
    WHEN UPPER(TRIM(adt.message_type)) = 'A03'
        THEN CAST(adt.discharge_date AS DATE)

    WHEN UPPER(TRIM(adt.message_type)) IN ('A01', 'A04', 'A06')
        THEN CAST(adt.admit_date AS DATE)

    ELSE COALESCE(
        CAST(adt.discharge_date AS DATE),
        CAST(adt.admit_date AS DATE),
        CAST(adt.message_timestamp AS DATE)
    )
END AS enrollment_reference_date
```

Then:

```sql
AND enrollment_reference_date
    BETWEEN enr.startdate AND enr.enddate
```

However, this is still less clean than letting the eligible claims population control enrollment.

## Final recommendation

Do **not** simply change `admit_date` to `discharge_date`.

Use this design:

```text
Claims event table
    → filters CareAllies eligibility on the claims event discharge date
    → carries member, IPA, and reporting group attribution

ADT candidate table
    → keeps all relevant admission and discharge messages
    → does not independently determine CareAllies eligibility

Matching layer
    → admission messages matched to claims admission date
    → discharge messages matched to claims discharge date
```

That avoids losing 2024 admission messages for 2025 discharges and prevents conflicting enrollment attribution between claims and ADT.
