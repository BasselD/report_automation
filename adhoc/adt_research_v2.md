Below is a simplified **Redshift pipeline** based on:

```text
Claims admission
→ match directly to an ADT event anchor
→ select exact or nearest ±1-day match
→ join back to raw ADT messages
→ summarize A01/A02/A03/A08
→ assign authorization/HIE category
```

It avoids prematurely collapsing all ADT messages into one record. It also uses only claims admission and discharge dates, consistent with your claims data. 

Replace:

```sql
analytics.inpatient_claims
clinical.adt_events
```

with your actual table names.

---

# Simplified Redshift SQL

```sql
/*==============================================================================
  SIMPLIFIED 2025 CLAIMS VS ADT QA PIPELINE

  Claims columns:
      MemberID
      Admit
      Discharge

  ADT columns:
      memberno
      admit_date
      discharge_date
      message_timestamp
      insert_timestamp
      message_type
      sending_source
==============================================================================*/


/*==============================================================================
  0. PARAMETERS
==============================================================================*/

DROP TABLE IF EXISTS tmp_params;

CREATE TEMP TABLE tmp_params AS
SELECT
    DATE '2025-01-01' AS start_date,
    DATE '2026-01-01' AS end_date,
    1::INTEGER AS fuzzy_days;


/*==============================================================================
  1. PREPARE 2025 CLAIMS

  Claims are the reference population.
  One row represents one expected inpatient event.
==============================================================================*/

DROP TABLE IF EXISTS tmp_claims_2025;

CREATE TEMP TABLE tmp_claims_2025
DISTSTYLE KEY
DISTKEY (member_id)
SORTKEY (member_id, claim_admit_date)
AS

WITH normalized AS
(
    SELECT
        TRIM(
            UPPER(
                CAST(MemberID AS VARCHAR(100))
            )
        ) AS member_id,

        TRY_CAST(Admit AS DATE)
            AS claim_admit_date,

        TRY_CAST(Discharge AS DATE)
            AS claim_discharge_date

    FROM analytics.inpatient_claims
),

deduplicated AS
(
    SELECT
        member_id,
        claim_admit_date,
        claim_discharge_date,

        ROW_NUMBER() OVER
        (
            PARTITION BY
                member_id,
                claim_admit_date

            ORDER BY
                claim_discharge_date DESC NULLS LAST
        ) AS duplicate_rn

    FROM normalized
    CROSS JOIN tmp_params p

    WHERE claim_admit_date >= p.start_date
      AND claim_admit_date <  p.end_date
      AND member_id IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER
    (
        ORDER BY
            member_id,
            claim_admit_date,
            claim_discharge_date
    )::BIGINT AS claim_event_id,

    member_id,
    claim_admit_date,
    claim_discharge_date

FROM deduplicated

WHERE duplicate_rn = 1;


ANALYZE tmp_claims_2025;


/*==============================================================================
  2. FILTER AND STANDARDIZE RAW ADT

  Keep a small date buffer for fuzzy matching.

  MEDHOK = Authorization
  Hospital names = HIE ADT
==============================================================================*/

DROP TABLE IF EXISTS tmp_adt_base;

CREATE TEMP TABLE tmp_adt_base
DISTSTYLE KEY
DISTKEY (member_id)
SORTKEY (member_id, adt_admit_date)
AS

SELECT
    TRIM(
        UPPER(
            CAST(memberno AS VARCHAR(100))
        )
    ) AS member_id,

    TRY_CAST(admit_date AS DATE)
        AS adt_admit_date,

    TRY_CAST(discharge_date AS TIMESTAMP)
        AS adt_discharge_timestamp,

    TRY_CAST(discharge_date AS DATE)
        AS adt_discharge_date,

    TRY_CAST(message_timestamp AS TIMESTAMP)
        AS message_timestamp,

    TRY_CAST(insert_timestamp AS TIMESTAMP)
        AS insert_timestamp,

    UPPER(
        TRIM(
            COALESCE(
                CAST(message_type AS VARCHAR(20)),
                'UNKNOWN'
            )
        )
    ) AS message_type,

    UPPER(
        TRIM(
            CAST(sending_source AS VARCHAR(250))
        )
    ) AS sending_source,

    CASE
        WHEN UPPER(
            TRIM(
                COALESCE(
                    CAST(sending_source AS VARCHAR(250)),
                    ''
                )
            )
        ) LIKE '%MEDHOK%'
            THEN 'AUTHORIZATION'

        WHEN sending_source IS NOT NULL
            THEN 'HIE_ADT'

        ELSE 'UNCLASSIFIED'
    END AS source_category,

    COALESCE
    (
        TRY_CAST(message_timestamp AS TIMESTAMP),
        TRY_CAST(insert_timestamp AS TIMESTAMP),
        TRY_CAST(admit_date AS TIMESTAMP)
    ) AS event_timestamp

FROM clinical.adt_events
CROSS JOIN tmp_params p

WHERE TRY_CAST(admit_date AS DATE) >=
      DATEADD(day, 0 - p.fuzzy_days, p.start_date)

  AND TRY_CAST(admit_date AS DATE) <
      DATEADD(day, p.fuzzy_days, p.end_date)

  AND memberno IS NOT NULL
  AND admit_date IS NOT NULL;


ANALYZE tmp_adt_base;


/*==============================================================================
  3. CREATE LIGHTWEIGHT ADT EVENT ANCHORS

  This does not summarize all message details.

  It only identifies distinct potential hospitalization/source events used
  for matching. After matching, we join back to the raw messages.
==============================================================================*/

DROP TABLE IF EXISTS tmp_adt_event_anchors;

CREATE TEMP TABLE tmp_adt_event_anchors
DISTSTYLE KEY
DISTKEY (member_id)
SORTKEY (member_id, adt_admit_date)
AS

SELECT
    ROW_NUMBER() OVER
    (
        ORDER BY
            member_id,
            adt_admit_date,
            source_category,
            sending_source
    )::BIGINT AS adt_event_id,

    member_id,
    adt_admit_date,
    source_category,
    sending_source,

    MIN(event_timestamp)
        AS first_event_timestamp,

    MIN(insert_timestamp)
        AS first_insert_timestamp

FROM tmp_adt_base

WHERE source_category IN
(
    'AUTHORIZATION',
    'HIE_ADT'
)

GROUP BY
    member_id,
    adt_admit_date,
    source_category,
    sending_source;


ANALYZE tmp_adt_event_anchors;


/*==============================================================================
  4. MATCH CLAIMS TO HIE ADT

  Candidate priority:
      1. Exact admission date
      2. Admission date within ±1 day
      3. Earliest available event
==============================================================================*/

DROP TABLE IF EXISTS tmp_hie_candidates;

CREATE TEMP TABLE tmp_hie_candidates AS

SELECT
    c.claim_event_id,
    h.adt_event_id,

    h.adt_admit_date,
    h.sending_source,

    DATEDIFF
    (
        day,
        c.claim_admit_date,
        h.adt_admit_date
    ) AS admit_date_difference,

    ABS(
        DATEDIFF
        (
            day,
            c.claim_admit_date,
            h.adt_admit_date
        )
    ) AS absolute_admit_difference,

    CASE
        WHEN c.claim_admit_date =
             h.adt_admit_date
            THEN 'Exact Admit Date'

        ELSE 'Fuzzy Admit Date ±1 Day'
    END AS match_method,

    ROW_NUMBER() OVER
    (
        PARTITION BY c.claim_event_id

        ORDER BY
            ABS(
                DATEDIFF
                (
                    day,
                    c.claim_admit_date,
                    h.adt_admit_date
                )
            ),

            h.first_event_timestamp,
            h.first_insert_timestamp,
            h.adt_event_id
    ) AS claim_candidate_rn

FROM tmp_claims_2025 c

INNER JOIN tmp_adt_event_anchors h
    ON c.member_id = h.member_id
   AND h.source_category = 'HIE_ADT'

CROSS JOIN tmp_params p

WHERE ABS(
        DATEDIFF
        (
            day,
            c.claim_admit_date,
            h.adt_admit_date
        )
      ) <= p.fuzzy_days;


/* First choose the best HIE event for each claim. */
DROP TABLE IF EXISTS tmp_hie_best_by_claim;

CREATE TEMP TABLE tmp_hie_best_by_claim AS

SELECT
    claim_event_id,
    adt_event_id,
    adt_admit_date,
    sending_source,
    admit_date_difference,
    absolute_admit_difference,
    match_method

FROM tmp_hie_candidates

WHERE claim_candidate_rn = 1;


/*
  Prevent the same HIE event from matching multiple claims.
  If that happens, keep the closest claim.
*/
DROP TABLE IF EXISTS tmp_hie_match;

CREATE TEMP TABLE tmp_hie_match AS

SELECT
    claim_event_id,
    adt_event_id,
    adt_admit_date,
    sending_source,
    admit_date_difference,
    match_method

FROM tmp_hie_best_by_claim

QUALIFY
    ROW_NUMBER() OVER
    (
        PARTITION BY adt_event_id

        ORDER BY
            absolute_admit_difference,
            claim_event_id
    ) = 1;


/*==============================================================================
  5. MATCH CLAIMS TO AUTHORIZATION EVENTS

  Authorization matching is independent of HIE matching.
==============================================================================*/

DROP TABLE IF EXISTS tmp_authorization_candidates;

CREATE TEMP TABLE tmp_authorization_candidates AS

SELECT
    c.claim_event_id,
    a.adt_event_id AS authorization_event_id,

    a.adt_admit_date AS authorization_admit_date,
    a.sending_source AS authorization_source,

    DATEDIFF
    (
        day,
        c.claim_admit_date,
        a.adt_admit_date
    ) AS authorization_date_difference,

    ABS(
        DATEDIFF
        (
            day,
            c.claim_admit_date,
            a.adt_admit_date
        )
    ) AS absolute_authorization_difference,

    CASE
        WHEN c.claim_admit_date =
             a.adt_admit_date
            THEN 'Exact Authorization Date'

        ELSE 'Fuzzy Authorization Date ±1 Day'
    END AS authorization_match_method,

    ROW_NUMBER() OVER
    (
        PARTITION BY c.claim_event_id

        ORDER BY
            ABS(
                DATEDIFF
                (
                    day,
                    c.claim_admit_date,
                    a.adt_admit_date
                )
            ),

            a.first_event_timestamp,
            a.first_insert_timestamp,
            a.adt_event_id
    ) AS authorization_candidate_rn

FROM tmp_claims_2025 c

INNER JOIN tmp_adt_event_anchors a
    ON c.member_id = a.member_id
   AND a.source_category = 'AUTHORIZATION'

CROSS JOIN tmp_params p

WHERE ABS(
        DATEDIFF
        (
            day,
            c.claim_admit_date,
            a.adt_admit_date
        )
      ) <= p.fuzzy_days;


DROP TABLE IF EXISTS tmp_authorization_match;

CREATE TEMP TABLE tmp_authorization_match AS

SELECT
    claim_event_id,
    authorization_event_id,
    authorization_admit_date,
    authorization_source,
    authorization_date_difference,
    authorization_match_method

FROM tmp_authorization_candidates

WHERE authorization_candidate_rn = 1;


/*==============================================================================
  6. JOIN MATCHED HIE EVENTS BACK TO RAW ADT MESSAGES

  We summarize only the ADT messages belonging to the selected match.
==============================================================================*/

DROP TABLE IF EXISTS tmp_matched_hie_messages;

CREATE TEMP TABLE tmp_matched_hie_messages
DISTSTYLE KEY
DISTKEY (member_id)
SORTKEY (claim_event_id, event_timestamp)
AS

SELECT
    m.claim_event_id,
    m.adt_event_id,
    m.match_method,
    m.admit_date_difference,

    b.member_id,
    b.adt_admit_date,
    b.adt_discharge_timestamp,
    b.adt_discharge_date,

    b.message_type,
    b.sending_source,
    b.message_timestamp,
    b.insert_timestamp,
    b.event_timestamp

FROM tmp_hie_match m

INNER JOIN tmp_adt_event_anchors a
    ON m.adt_event_id = a.adt_event_id

INNER JOIN tmp_adt_base b
    ON  a.member_id = b.member_id
    AND a.adt_admit_date = b.adt_admit_date
    AND a.source_category = b.source_category
    AND COALESCE(a.sending_source, '') =
        COALESCE(b.sending_source, '')

WHERE b.source_category = 'HIE_ADT';


ANALYZE tmp_matched_hie_messages;


/*==============================================================================
  7. IDENTIFY THE FIRST A03 FOR EACH MATCHED CLAIM
==============================================================================*/

DROP TABLE IF EXISTS tmp_first_a03;

CREATE TEMP TABLE tmp_first_a03 AS

SELECT
    claim_event_id,

    adt_discharge_timestamp
        AS first_a03_discharge_timestamp,

    adt_discharge_date
        AS first_a03_discharge_date,

    message_timestamp
        AS first_a03_message_timestamp,

    insert_timestamp
        AS first_a03_insert_timestamp

FROM tmp_matched_hie_messages

WHERE message_type = 'A03'

QUALIFY
    ROW_NUMBER() OVER
    (
        PARTITION BY claim_event_id

        ORDER BY
            event_timestamp,
            insert_timestamp
    ) = 1;


/*==============================================================================
  8. FIND THE LATEST AVAILABLE ADT DISCHARGE VALUE

  This helps identify cases where an A08 carries the discharge date even when
  an A03 is missing.
==============================================================================*/

DROP TABLE IF EXISTS tmp_latest_discharge;

CREATE TEMP TABLE tmp_latest_discharge AS

SELECT
    claim_event_id,

    adt_discharge_timestamp
        AS latest_adt_discharge_timestamp,

    adt_discharge_date
        AS latest_adt_discharge_date,

    message_type
        AS latest_discharge_message_type

FROM tmp_matched_hie_messages

WHERE adt_discharge_date IS NOT NULL

QUALIFY
    ROW_NUMBER() OVER
    (
        PARTITION BY claim_event_id

        ORDER BY
            event_timestamp DESC,
            insert_timestamp DESC
    ) = 1;


/*==============================================================================
  9. MESSAGE COUNTS BY MATCHED CLAIM
==============================================================================*/

DROP TABLE IF EXISTS tmp_hie_message_summary;

CREATE TEMP TABLE tmp_hie_message_summary AS

SELECT
    claim_event_id,

    COUNT(*) AS total_hie_message_count,

    SUM(
        CASE WHEN message_type = 'A01' THEN 1 ELSE 0 END
    ) AS a01_message_count,

    SUM(
        CASE WHEN message_type = 'A02' THEN 1 ELSE 0 END
    ) AS a02_transfer_count,

    SUM(
        CASE WHEN message_type = 'A03' THEN 1 ELSE 0 END
    ) AS a03_message_count,

    SUM(
        CASE WHEN message_type = 'A08' THEN 1 ELSE 0 END
    ) AS a08_message_count,

    MIN(event_timestamp)
        AS first_hie_event_timestamp,

    MAX(event_timestamp)
        AS last_hie_event_timestamp

FROM tmp_matched_hie_messages

GROUP BY claim_event_id;


/*==============================================================================
  10. BUILD PATIENT EVENT SEQUENCE

  Only one LISTAGG appears here, avoiding the Redshift ordering error.
==============================================================================*/

DROP TABLE IF EXISTS tmp_hie_event_sequence;

CREATE TEMP TABLE tmp_hie_event_sequence AS

WITH sequence_rows AS
(
    SELECT
        claim_event_id,
        event_timestamp,
        insert_timestamp,
        message_type,

        CASE
            WHEN message_type IN ('A01', 'A04', 'A06')
                THEN 'Admission'

            WHEN message_type = 'A02'
                THEN 'Transfer'

            WHEN message_type = 'A03'
                THEN 'Discharge'

            WHEN message_type = 'A08'
                THEN 'Update'

            ELSE 'Other'
        END AS event_name

    FROM tmp_matched_hie_messages
),

deduplicated_sequence AS
(
    SELECT
        claim_event_id,
        event_timestamp,
        insert_timestamp,
        message_type,
        event_name

    FROM sequence_rows

    QUALIFY
        ROW_NUMBER() OVER
        (
            PARTITION BY
                claim_event_id,
                message_type,
                event_timestamp

            ORDER BY insert_timestamp
        ) = 1
)

SELECT
    claim_event_id,

    LISTAGG(
        event_name,
        ' > '
    )
    WITHIN GROUP
    (
        ORDER BY
            event_timestamp,
            insert_timestamp,
            message_type
    ) AS hie_event_sequence

FROM deduplicated_sequence

GROUP BY claim_event_id;


/*==============================================================================
  11. BUILD AUTHORIZATION SEQUENCE LABEL
==============================================================================*/

DROP TABLE IF EXISTS tmp_claim_sequence;

CREATE TEMP TABLE tmp_claim_sequence AS

SELECT
    c.claim_event_id,

    CASE
        WHEN a.authorization_event_id IS NOT NULL
         AND h.hie_event_sequence IS NOT NULL
            THEN 'Authorization > ' ||
                 h.hie_event_sequence

        WHEN a.authorization_event_id IS NOT NULL
         AND h.hie_event_sequence IS NULL
            THEN 'Authorization'

        WHEN a.authorization_event_id IS NULL
         AND h.hie_event_sequence IS NOT NULL
            THEN h.hie_event_sequence

        ELSE NULL
    END AS patient_event_sequence

FROM tmp_claims_2025 c

LEFT JOIN tmp_authorization_match a
    ON c.claim_event_id = a.claim_event_id

LEFT JOIN tmp_hie_event_sequence h
    ON c.claim_event_id = h.claim_event_id;


/*==============================================================================
  12. FINAL CLAIMS-CENTERED QA TABLE
==============================================================================*/

DROP TABLE IF EXISTS tmp_adt_discharge_qa_2025;

CREATE TEMP TABLE tmp_adt_discharge_qa_2025
DISTSTYLE KEY
DISTKEY (member_id)
SORTKEY (claim_admit_date, member_id)
AS

WITH base AS
(
    SELECT
        c.claim_event_id,
        c.member_id,
        c.claim_admit_date,
        c.claim_discharge_date,

        h.adt_event_id,
        h.adt_admit_date,
        h.sending_source AS hie_sending_source,
        h.match_method,
        h.admit_date_difference,

        a.authorization_event_id,
        a.authorization_admit_date,
        a.authorization_source,
        a.authorization_match_method,
        a.authorization_date_difference,

        s.total_hie_message_count,
        s.a01_message_count,
        s.a02_transfer_count,
        s.a03_message_count,
        s.a08_message_count,
        s.first_hie_event_timestamp,
        s.last_hie_event_timestamp,

        a03.first_a03_discharge_timestamp,
        a03.first_a03_discharge_date,
        a03.first_a03_message_timestamp,
        a03.first_a03_insert_timestamp,

        ld.latest_adt_discharge_timestamp,
        ld.latest_adt_discharge_date,
        ld.latest_discharge_message_type,

        COALESCE
        (
            a03.first_a03_discharge_date,
            ld.latest_adt_discharge_date
        ) AS final_adt_discharge_date,

        seq.patient_event_sequence,

        DATEDIFF
        (
            day,
            c.claim_discharge_date,
            COALESCE
            (
                a03.first_a03_discharge_date,
                ld.latest_adt_discharge_date
            )
        ) AS discharge_date_difference,

        DATEDIFF
        (
            second,
            a03.first_a03_discharge_timestamp,
            a03.first_a03_message_timestamp
        ) / 3600.0 AS source_lag_hours,

        DATEDIFF
        (
            second,
            a03.first_a03_message_timestamp,
            a03.first_a03_insert_timestamp
        ) / 60.0 AS pipeline_lag_minutes,

        DATEDIFF
        (
            second,
            a03.first_a03_discharge_timestamp,
            a03.first_a03_insert_timestamp
        ) / 3600.0 AS end_to_end_lag_hours,

        DATEDIFF
        (
            day,
            a03.first_a03_discharge_date,
            CAST(
                a03.first_a03_insert_timestamp
                AS DATE
            )
        ) AS end_to_end_lag_days

    FROM tmp_claims_2025 c

    LEFT JOIN tmp_hie_match h
        ON c.claim_event_id = h.claim_event_id

    LEFT JOIN tmp_authorization_match a
        ON c.claim_event_id = a.claim_event_id

    LEFT JOIN tmp_hie_message_summary s
        ON c.claim_event_id = s.claim_event_id

    LEFT JOIN tmp_first_a03 a03
        ON c.claim_event_id = a03.claim_event_id

    LEFT JOIN tmp_latest_discharge ld
        ON c.claim_event_id = ld.claim_event_id

    LEFT JOIN tmp_claim_sequence seq
        ON c.claim_event_id = seq.claim_event_id
)

SELECT
    base.*,

    CASE
        WHEN adt_event_id IS NOT NULL THEN 1
        ELSE 0
    END AS hie_adt_matched_flag,

    CASE
        WHEN authorization_event_id IS NOT NULL THEN 1
        ELSE 0
    END AS authorization_present_flag,

    CASE
        WHEN COALESCE(a03_message_count, 0) > 0 THEN 1
        ELSE 0
    END AS a03_present_flag,

    CASE
        WHEN authorization_event_id IS NOT NULL
         AND adt_event_id IS NOT NULL
            THEN 'Authorization and HIE ADT'

        WHEN authorization_event_id IS NOT NULL
         AND adt_event_id IS NULL
            THEN 'Authorization Only'

        WHEN authorization_event_id IS NULL
         AND adt_event_id IS NOT NULL
            THEN 'HIE ADT Only'

        ELSE 'No Authorization or HIE ADT'
    END AS coverage_category,

    CASE
        WHEN claim_discharge_date IS NULL
            THEN 'Claims Discharge Missing'

        WHEN adt_event_id IS NULL
            THEN 'Missing HIE ADT'

        WHEN COALESCE(a03_message_count, 0) = 0
         AND final_adt_discharge_date IS NULL
            THEN 'HIE Matched, Discharge Missing'

        WHEN COALESCE(a03_message_count, 0) = 0
         AND final_adt_discharge_date IS NOT NULL
            THEN 'Discharge Found, No A03'

        WHEN COALESCE(a03_message_count, 0) > 0
         AND first_a03_discharge_date IS NULL
            THEN 'A03 Present, Discharge Date Missing'

        WHEN discharge_date_difference = 0
            THEN 'Exact Discharge Match'

        WHEN ABS(discharge_date_difference) <= 1
            THEN 'Discharge Within ±1 Day'

        ELSE 'Discharge Mismatch >1 Day'
    END AS discharge_quality_status,

    CASE
        WHEN end_to_end_lag_hours IS NULL
            THEN 'Lag Unavailable'

        WHEN end_to_end_lag_hours < 0
            THEN 'Negative Lag - Investigate'

        WHEN end_to_end_lag_hours <= 4
            THEN '0-4 Hours'

        WHEN end_to_end_lag_hours <= 24
            THEN '4-24 Hours'

        WHEN end_to_end_lag_hours <= 48
            THEN '24-48 Hours'

        ELSE 'Over 48 Hours'
    END AS lag_category

FROM base;


/*==============================================================================
  13. CATEGORY SUMMARY
==============================================================================*/

DROP TABLE IF EXISTS tmp_coverage_summary_2025;

CREATE TEMP TABLE tmp_coverage_summary_2025 AS

SELECT
    coverage_category,

    COUNT(DISTINCT claim_event_id)
        AS claim_event_count,

    ROUND
    (
        100.0
        * COUNT(DISTINCT claim_event_id)
        / NULLIF(
            SUM(
                COUNT(DISTINCT claim_event_id)
            ) OVER (),
            0
        ),
        2
    ) AS percent_of_claim_events

FROM tmp_adt_discharge_qa_2025

GROUP BY coverage_category;


/*==============================================================================
  14. OVERALL SUMMARY
==============================================================================*/

DROP TABLE IF EXISTS tmp_overall_summary_2025;

CREATE TEMP TABLE tmp_overall_summary_2025 AS

SELECT
    COUNT(DISTINCT claim_event_id)
        AS total_claim_events,

    SUM(
        CASE
            WHEN hie_adt_matched_flag = 1
                THEN 1
            ELSE 0
        END
    ) AS claims_with_hie_adt,

    SUM(
        CASE
            WHEN hie_adt_matched_flag = 0
                THEN 1
            ELSE 0
        END
    ) AS claims_missing_hie_adt,

    ROUND
    (
        100.0
        * SUM(
            CASE
                WHEN hie_adt_matched_flag = 0
                    THEN 1
                ELSE 0
            END
        )
        / NULLIF(COUNT(DISTINCT claim_event_id), 0),
        2
    ) AS percent_missing_hie_adt,

    SUM(
        CASE
            WHEN match_method = 'Exact Admit Date'
                THEN 1
            ELSE 0
        END
    ) AS exact_hie_matches,

    SUM(
        CASE
            WHEN match_method LIKE 'Fuzzy%'
                THEN 1
            ELSE 0
        END
    ) AS fuzzy_hie_matches,

    SUM(
        CASE
            WHEN a03_present_flag = 1
                THEN 1
            ELSE 0
        END
    ) AS claims_with_a03,

    SUM(
        CASE
            WHEN discharge_quality_status =
                 'Exact Discharge Match'
                THEN 1
            ELSE 0
        END
    ) AS exact_discharge_matches,

    SUM(
        CASE
            WHEN discharge_quality_status =
                 'Discharge Within ±1 Day'
                THEN 1
            ELSE 0
        END
    ) AS discharge_matches_within_one_day

FROM tmp_adt_discharge_qa_2025;


/*==============================================================================
  15. REVIEW RESULTS
==============================================================================*/

SELECT *
FROM tmp_coverage_summary_2025
ORDER BY claim_event_count DESC;


SELECT *
FROM tmp_overall_summary_2025;


SELECT *
FROM tmp_adt_discharge_qa_2025
ORDER BY
    claim_admit_date,
    member_id;
```

## Why this version should improve the match rate

The previous approach summarized the ADT dataset before matching. If that summarization grouped events incorrectly, valid hospitalization records could disappear.

This version:

1. Creates only a lightweight ADT anchor using member, admit date, source category, and source.
2. Matches each claims admission to the closest anchor.
3. Joins the selected anchor back to the raw ADT messages.
4. Calculates A03 counts, A08 counts, transfers, discharge dates, sequence, and lag after matching.

## First QA checks to run

```sql
SELECT COUNT(*)
FROM tmp_claims_2025;
```

```sql
SELECT
    source_category,
    COUNT(*) AS event_count,
    COUNT(DISTINCT member_id) AS member_count
FROM tmp_adt_event_anchors
GROUP BY source_category;
```

```sql
SELECT
    coverage_category,
    COUNT(*) AS claim_count
FROM tmp_adt_discharge_qa_2025
GROUP BY coverage_category
ORDER BY claim_count DESC;
```

```sql
SELECT
    match_method,
    COUNT(*) AS match_count
FROM tmp_adt_discharge_qa_2025
GROUP BY match_method;
```

The four coverage categories should sum exactly to the total number of claims events:

```text
Authorization and HIE ADT
+ Authorization Only
+ HIE ADT Only
+ No Authorization or HIE ADT
= Total 2025 claims events
```
