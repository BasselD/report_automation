This SQL follows the same **exact-match → fuzzy-match → unmatched claims** reconciliation structure from the Python process, but performs the filtering and aggregation inside Redshift before exporting the final result. 

## Before running

Replace these table names:

```sql
analytics.inpatient_claims
clinical.adt_events
```

Expected columns:

```text
Claims:
MemberID, Admit, Discharge

ADT:
memberno, admit_date, discharge_date,
message_timestamp, insert_timestamp,
event_registry_timestamp,
message_type, sending_source
```

If `event_registry_timestamp` is unavailable, remove it from the `COALESCE` expressions.

---

# Redshift SQL: 2025 ADT discharge QA

```sql
/*==============================================================================
  ADT DISCHARGE QUALITY AND TIMELINESS QA
  Amazon Redshift

  Main outputs:
    tmp_adt_discharge_qa_2025
    tmp_adt_summary_2025
    tmp_adt_monthly_summary_2025
    tmp_unmatched_hie_adt_2025
    tmp_claim_event_sequence_2025
==============================================================================*/


/*==============================================================================
  0. PARAMETERS
==============================================================================*/

DROP TABLE IF EXISTS tmp_adt_params;

CREATE TEMP TABLE tmp_adt_params AS
SELECT
    DATE '2025-01-01' AS start_date,
    DATE '2026-01-01' AS end_date,
    1::INTEGER       AS fuzzy_days;


/*==============================================================================
  1. PREPARE AND DEDUPLICATE 2025 CLAIMS

  One row per member and claims admission date.

  IMPORTANT:
  If you have a unique claims hospitalization ID, use it instead of deduplicating
  only by member and admit date.
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
                CAST(c.MemberID AS VARCHAR(100))
            )
        ) AS member_id,

        TRY_CAST(c.Admit AS TIMESTAMP) AS claim_admit_ts,

        TRY_CAST(c.Discharge AS TIMESTAMP) AS claim_discharge_ts

    FROM analytics.inpatient_claims c
    CROSS JOIN tmp_adt_params p

    WHERE TRY_CAST(c.Admit AS DATE) >= p.start_date
      AND TRY_CAST(c.Admit AS DATE) <  p.end_date
),

deduplicated AS
(
    SELECT
        member_id,
        claim_admit_ts,
        claim_discharge_ts,

        CAST(claim_admit_ts AS DATE) AS claim_admit_date,

        CAST(claim_discharge_ts AS DATE) AS claim_discharge_date,

        ROW_NUMBER() OVER
        (
            PARTITION BY
                member_id,
                CAST(claim_admit_ts AS DATE)

            ORDER BY
                claim_discharge_ts DESC NULLS LAST
        ) AS duplicate_rn

    FROM normalized

    WHERE member_id IS NOT NULL
      AND claim_admit_ts IS NOT NULL
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
    claim_admit_ts,
    claim_discharge_ts,
    claim_admit_date,
    claim_discharge_date

FROM deduplicated

WHERE duplicate_rn = 1;


ANALYZE tmp_claims_2025;


/*==============================================================================
  2. FILTER AND STANDARDIZE ADT DATA

  ADT is limited to the 2025 claims period plus the fuzzy-match buffer.

  Source categories:
    MEDHOK       = Authorization
    Other source = HIE ADT
    Null source  = Unclassified
==============================================================================*/

DROP TABLE IF EXISTS tmp_adt_base;

CREATE TEMP TABLE tmp_adt_base
DISTSTYLE KEY
DISTKEY (member_id)
SORTKEY (member_id, adt_admit_date, event_sequence_ts)
AS

SELECT
    TRIM(
        UPPER(
            CAST(a.memberno AS VARCHAR(100))
        )
    ) AS member_id,

    TRY_CAST(a.admit_date AS TIMESTAMP) AS adt_admit_ts,

    CAST(
        TRY_CAST(a.admit_date AS TIMESTAMP)
        AS DATE
    ) AS adt_admit_date,

    TRY_CAST(a.discharge_date AS TIMESTAMP) AS adt_discharge_ts,

    TRY_CAST(a.message_timestamp AS TIMESTAMP)
        AS message_timestamp,

    TRY_CAST(a.insert_timestamp AS TIMESTAMP)
        AS insert_timestamp,

    TRY_CAST(a.event_registry_timestamp AS TIMESTAMP)
        AS event_registry_timestamp,

    UPPER(
        TRIM(
            COALESCE(
                CAST(a.message_type AS VARCHAR(20)),
                'UNKNOWN'
            )
        )
    ) AS message_type,

    TRIM(
        CAST(a.sending_source AS VARCHAR(250))
    ) AS sending_source,

    CASE
        WHEN UPPER(
            TRIM(
                COALESCE(
                    CAST(a.sending_source AS VARCHAR(250)),
                    ''
                )
            )
        ) LIKE '%MEDHOK%'
            THEN 'AUTHORIZATION'

        WHEN a.sending_source IS NOT NULL
            THEN 'HIE_ADT'

        ELSE 'UNCLASSIFIED'
    END AS source_category,

    COALESCE
    (
        TRY_CAST(a.message_timestamp AS TIMESTAMP),
        TRY_CAST(a.insert_timestamp AS TIMESTAMP),
        TRY_CAST(a.event_registry_timestamp AS TIMESTAMP),
        TRY_CAST(a.admit_date AS TIMESTAMP)
    ) AS event_sequence_ts

FROM clinical.adt_events a
CROSS JOIN tmp_adt_params p

WHERE TRY_CAST(a.admit_date AS DATE) >=
      DATEADD(
          day,
          0 - p.fuzzy_days,
          p.start_date
      )

  AND TRY_CAST(a.admit_date AS DATE) <
      DATEADD(
          day,
          p.fuzzy_days,
          p.end_date
      )

  AND a.memberno IS NOT NULL
  AND a.admit_date IS NOT NULL;


ANALYZE tmp_adt_base;


/*==============================================================================
  3. RANK ADT MESSAGES

  This replaces the old logic that kept only the earliest hospitalization row.

  The important partitions now include:
    member
    admission date
    source
    message type

  This prevents the A01 from eliminating the later A02, A03 and A08 messages.
==============================================================================*/

DROP TABLE IF EXISTS tmp_adt_ranked;

CREATE TEMP TABLE tmp_adt_ranked
DISTSTYLE KEY
DISTKEY (member_id)
SORTKEY (member_id, adt_admit_date, event_sequence_ts)
AS

SELECT
    b.*,

    COUNT(*) OVER
    (
        PARTITION BY
            member_id,
            adt_admit_date,
            source_category,
            sending_source,
            message_type
    ) AS message_type_count,

    ROW_NUMBER() OVER
    (
        PARTITION BY
            member_id,
            adt_admit_date,
            source_category,
            sending_source

        ORDER BY
            event_sequence_ts,
            insert_timestamp
    ) AS first_source_event_rn,

    ROW_NUMBER() OVER
    (
        PARTITION BY
            member_id,
            adt_admit_date,
            source_category,
            sending_source,
            message_type

        ORDER BY
            event_sequence_ts,
            insert_timestamp
    ) AS first_message_type_rn,

    ROW_NUMBER() OVER
    (
        PARTITION BY
            member_id,
            adt_admit_date,
            source_category,
            sending_source,
            message_type

        ORDER BY
            event_sequence_ts DESC,
            insert_timestamp DESC
    ) AS last_message_type_rn,

    ROW_NUMBER() OVER
    (
        PARTITION BY
            member_id,
            adt_admit_date,
            source_category,
            sending_source,
            message_type,
            event_sequence_ts

        ORDER BY
            insert_timestamp,
            event_registry_timestamp
    ) AS exact_duplicate_rn

FROM tmp_adt_base b;


/*==============================================================================
  4. REDUCE MESSAGE VOLUME WITHOUT DESTROYING THE EVENT SEQUENCE

  Retained:
    Authorization: first MedHOK event
    Admission:     first A01/A04/A06
    Transfer:      all unique A02 messages
    Discharge:     first and last A03
    Update:        first and last A08

  This is the recommended replacement for "earliest event overall."
==============================================================================*/

DROP TABLE IF EXISTS tmp_adt_reduced;

CREATE TEMP TABLE tmp_adt_reduced
DISTSTYLE KEY
DISTKEY (member_id)
SORTKEY (member_id, adt_admit_date, event_sequence_ts)
AS

SELECT
    r.*,

    CASE
        WHEN source_category = 'AUTHORIZATION'
            THEN 'Authorization'

        WHEN message_type IN ('A01', 'A04', 'A06')
            THEN 'Admission'

        WHEN message_type = 'A02'
            THEN 'Transfer'

        WHEN message_type = 'A03'
            THEN 'Discharge'

        WHEN message_type = 'A08'
            THEN 'Update'

        ELSE 'Other'
    END AS event_name,

    CASE
        WHEN source_category = 'AUTHORIZATION' THEN 1
        WHEN message_type IN ('A01', 'A04', 'A06') THEN 2
        WHEN message_type = 'A02' THEN 3
        WHEN message_type = 'A08' THEN 4
        WHEN message_type = 'A03' THEN 5
        ELSE 6
    END AS event_priority

FROM tmp_adt_ranked r

WHERE exact_duplicate_rn = 1

  AND
  (
        (
            source_category = 'AUTHORIZATION'
            AND first_source_event_rn = 1
        )

        OR

        (
            source_category = 'HIE_ADT'

            AND
            (
                (
                    message_type IN ('A01', 'A04', 'A06')
                    AND first_message_type_rn = 1
                )

                OR message_type = 'A02'

                OR
                (
                    message_type IN ('A03', 'A08')
                    AND
                    (
                        first_message_type_rn = 1
                        OR last_message_type_rn = 1
                    )
                )

                OR
                (
                    message_type NOT IN
                    (
                        'A01',
                        'A02',
                        'A03',
                        'A04',
                        'A06',
                        'A08'
                    )
                    AND first_message_type_rn = 1
                )
            )
        )
  );


ANALYZE tmp_adt_reduced;


/*==============================================================================
  5. MESSAGE COUNTS FROM THE FILTERED ADT DATA

  Counts come from tmp_adt_base, not the reduced table. Therefore the full
  number of A03, A08 and transfer messages is retained.
==============================================================================*/

DROP TABLE IF EXISTS tmp_adt_message_counts;

CREATE TEMP TABLE tmp_adt_message_counts
DISTSTYLE KEY
DISTKEY (member_id)
SORTKEY (member_id, adt_admit_date)
AS

SELECT
    member_id,
    adt_admit_date,
    source_category,

    COUNT(*) AS total_message_count,

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
    ) AS a08_message_count

FROM tmp_adt_base

WHERE source_category IN
(
    'AUTHORIZATION',
    'HIE_ADT'
)

GROUP BY
    member_id,
    adt_admit_date,
    source_category;


/*==============================================================================
  6. CONSOLIDATE TRUE HIE ADT MESSAGES INTO ONE HOSPITALIZATION EVENT
==============================================================================*/

DROP TABLE IF EXISTS tmp_hie_message_ranked;

CREATE TEMP TABLE tmp_hie_message_ranked
DISTSTYLE KEY
DISTKEY (member_id)
SORTKEY (member_id, adt_admit_date, event_sequence_ts)
AS

SELECT
    r.*,

    ROW_NUMBER() OVER
    (
        PARTITION BY
            member_id,
            adt_admit_date

        ORDER BY
            event_sequence_ts,
            insert_timestamp
    ) AS first_hie_event_rn,

    ROW_NUMBER() OVER
    (
        PARTITION BY
            member_id,
            adt_admit_date

        ORDER BY
            CASE
                WHEN message_type IN ('A01', 'A04', 'A06')
                    THEN 0
                ELSE 1
            END,

            event_sequence_ts,
            insert_timestamp
    ) AS first_admission_message_rn,

    ROW_NUMBER() OVER
    (
        PARTITION BY
            member_id,
            adt_admit_date

        ORDER BY
            CASE
                WHEN message_type = 'A03'
                    THEN 0
                ELSE 1
            END,

            event_sequence_ts,
            insert_timestamp
    ) AS first_a03_message_rn,

    ROW_NUMBER() OVER
    (
        PARTITION BY
            member_id,
            adt_admit_date

        ORDER BY
            CASE
                WHEN adt_discharge_ts IS NOT NULL
                    THEN 0
                ELSE 1
            END,

            event_sequence_ts DESC,
            insert_timestamp DESC
    ) AS latest_discharge_value_rn

FROM tmp_adt_reduced r

WHERE source_category = 'HIE_ADT';


DROP TABLE IF EXISTS tmp_hie_events;

CREATE TEMP TABLE tmp_hie_events
DISTSTYLE KEY
DISTKEY (member_id)
SORTKEY (member_id, adt_admit_date)
AS

WITH aggregated AS
(
    SELECT
        h.member_id,
        h.adt_admit_date,

        MIN(h.adt_admit_ts) AS adt_admit_ts,

        MAX(
            CASE
                WHEN first_hie_event_rn = 1
                    THEN sending_source
            END
        ) AS first_hie_source,

        LISTAGG(
            DISTINCT sending_source,
            ', '
        )
        WITHIN GROUP
        (
            ORDER BY sending_source
        ) AS hie_sending_sources,

        LISTAGG(
            DISTINCT message_type,
            ','
        )
        WITHIN GROUP
        (
            ORDER BY message_type
        ) AS message_types_seen,

        MAX(
            CASE
                WHEN first_admission_message_rn = 1
                 AND message_type IN ('A01', 'A04', 'A06')
                    THEN message_type
            END
        ) AS first_admission_message_type,

        MAX(
            CASE
                WHEN first_admission_message_rn = 1
                 AND message_type IN ('A01', 'A04', 'A06')
                    THEN message_timestamp
            END
        ) AS first_admission_message_timestamp,

        MAX(
            CASE
                WHEN first_admission_message_rn = 1
                 AND message_type IN ('A01', 'A04', 'A06')
                    THEN insert_timestamp
            END
        ) AS first_admission_insert_timestamp,

        MAX(
            CASE
                WHEN first_a03_message_rn = 1
                 AND message_type = 'A03'
                    THEN adt_discharge_ts
            END
        ) AS first_a03_discharge_ts,

        MAX(
            CASE
                WHEN first_a03_message_rn = 1
                 AND message_type = 'A03'
                    THEN message_timestamp
            END
        ) AS first_a03_message_timestamp,

        MAX(
            CASE
                WHEN first_a03_message_rn = 1
                 AND message_type = 'A03'
                    THEN insert_timestamp
            END
        ) AS first_a03_insert_timestamp,

        MAX(
            CASE
                WHEN latest_discharge_value_rn = 1
                    THEN adt_discharge_ts
            END
        ) AS latest_discharge_ts_any,

        MAX(
            CASE
                WHEN latest_discharge_value_rn = 1
                    THEN message_type
            END
        ) AS latest_discharge_message_type,

        MAX(c.total_message_count) AS total_hie_message_count,
        MAX(c.a01_message_count)   AS a01_message_count,
        MAX(c.a02_transfer_count)  AS a02_transfer_count,
        MAX(c.a03_message_count)   AS a03_message_count,
        MAX(c.a08_message_count)   AS a08_message_count

    FROM tmp_hie_message_ranked h

    LEFT JOIN tmp_adt_message_counts c
        ON  h.member_id = c.member_id
        AND h.adt_admit_date = c.adt_admit_date
        AND c.source_category = 'HIE_ADT'

    GROUP BY
        h.member_id,
        h.adt_admit_date
)

SELECT
    ROW_NUMBER() OVER
    (
        ORDER BY
            member_id,
            adt_admit_date
    )::BIGINT AS adt_event_id,

    aggregated.*

FROM aggregated;


ANALYZE tmp_hie_events;


/*==============================================================================
  7. CONSOLIDATE AUTHORIZATION EVENTS

  Authorization events are retained separately and do not count as HIE ADT.
==============================================================================*/

DROP TABLE IF EXISTS tmp_authorization_events;

CREATE TEMP TABLE tmp_authorization_events
DISTSTYLE KEY
DISTKEY (member_id)
SORTKEY (member_id, authorization_admit_date)
AS

WITH ranked AS
(
    SELECT
        member_id,

        adt_admit_date AS authorization_admit_date,

        sending_source AS authorization_source,

        event_sequence_ts AS authorization_event_timestamp,

        insert_timestamp AS authorization_insert_timestamp,

        ROW_NUMBER() OVER
        (
            PARTITION BY
                member_id,
                adt_admit_date

            ORDER BY
                event_sequence_ts,
                insert_timestamp
        ) AS authorization_rn

    FROM tmp_adt_reduced

    WHERE source_category = 'AUTHORIZATION'
)

SELECT
    ROW_NUMBER() OVER
    (
        ORDER BY
            member_id,
            authorization_admit_date
    )::BIGINT AS authorization_event_id,

    member_id,
    authorization_admit_date,
    authorization_source,
    authorization_event_timestamp,
    authorization_insert_timestamp

FROM ranked

WHERE authorization_rn = 1;


/*==============================================================================
  8. EXACT CLAIMS-TO-HIE ADT MATCH
==============================================================================*/

DROP TABLE IF EXISTS tmp_exact_hie_match;

CREATE TEMP TABLE tmp_exact_hie_match AS

SELECT
    c.claim_event_id,
    h.adt_event_id,

    'Exact Admit Date'::VARCHAR(50) AS match_method,

    0::INTEGER AS admit_date_difference

FROM tmp_claims_2025 c

INNER JOIN tmp_hie_events h
    ON  c.member_id = h.member_id
    AND c.claim_admit_date = h.adt_admit_date;


/*==============================================================================
  9. FUZZY CLAIMS-TO-HIE ADT MATCH

  Only unmatched claims and unmatched ADT events are considered.

  The nearest admission date is selected first.
  Claims discharge difference is used as a tie-breaker.
==============================================================================*/

DROP TABLE IF EXISTS tmp_fuzzy_hie_candidates;

CREATE TEMP TABLE tmp_fuzzy_hie_candidates AS

SELECT
    c.claim_event_id,
    h.adt_event_id,

    DATEDIFF(
        day,
        c.claim_admit_date,
        h.adt_admit_date
    ) AS admit_date_difference,

    ABS(
        DATEDIFF(
            day,
            c.claim_admit_date,
            h.adt_admit_date
        )
    ) AS absolute_admit_difference,

    COALESCE
    (
        ABS(
            DATEDIFF
            (
                day,
                c.claim_discharge_date,
                CAST(
                    COALESCE
                    (
                        h.first_a03_discharge_ts,
                        h.latest_discharge_ts_any
                    )
                    AS DATE
                )
            )
        ),
        9999
    ) AS absolute_discharge_difference,

    ROW_NUMBER() OVER
    (
        PARTITION BY
            c.claim_event_id

        ORDER BY
            ABS(
                DATEDIFF(
                    day,
                    c.claim_admit_date,
                    h.adt_admit_date
                )
            ),

            COALESCE
            (
                ABS(
                    DATEDIFF
                    (
                        day,
                        c.claim_discharge_date,
                        CAST(
                            COALESCE
                            (
                                h.first_a03_discharge_ts,
                                h.latest_discharge_ts_any
                            )
                            AS DATE
                        )
                    )
                ),
                9999
            ),

            h.first_a03_insert_timestamp,
            h.adt_event_id
    ) AS claim_candidate_rn

FROM tmp_claims_2025 c

INNER JOIN tmp_hie_events h
    ON c.member_id = h.member_id

LEFT JOIN tmp_exact_hie_match exact_claim
    ON c.claim_event_id = exact_claim.claim_event_id

LEFT JOIN tmp_exact_hie_match exact_adt
    ON h.adt_event_id = exact_adt.adt_event_id

CROSS JOIN tmp_adt_params p

WHERE exact_claim.claim_event_id IS NULL
  AND exact_adt.adt_event_id IS NULL

  AND ABS(
        DATEDIFF(
            day,
            c.claim_admit_date,
            h.adt_admit_date
        )
      ) <= p.fuzzy_days;


DROP TABLE IF EXISTS tmp_fuzzy_hie_match;

CREATE TEMP TABLE tmp_fuzzy_hie_match AS

SELECT
    claim_event_id,
    adt_event_id,

    'Fuzzy Admit Date ±1 Day'::VARCHAR(50)
        AS match_method,

    admit_date_difference

FROM tmp_fuzzy_hie_candidates

WHERE claim_candidate_rn = 1

QUALIFY
    ROW_NUMBER() OVER
    (
        PARTITION BY adt_event_id

        ORDER BY
            absolute_admit_difference,
            absolute_discharge_difference,
            claim_event_id
    ) = 1;


/*==============================================================================
  10. COMBINE EXACT AND FUZZY HIE MATCHES
==============================================================================*/

DROP TABLE IF EXISTS tmp_hie_match_map;

CREATE TEMP TABLE tmp_hie_match_map AS

SELECT
    claim_event_id,
    adt_event_id,
    match_method,
    admit_date_difference

FROM tmp_exact_hie_match

UNION ALL

SELECT
    claim_event_id,
    adt_event_id,
    match_method,
    admit_date_difference

FROM tmp_fuzzy_hie_match;


/*==============================================================================
  11. MATCH AUTHORIZATION EVENTS TO CLAIMS

  Authorization matching is independent of HIE matching.
==============================================================================*/

DROP TABLE IF EXISTS tmp_authorization_match;

CREATE TEMP TABLE tmp_authorization_match AS

SELECT
    c.claim_event_id,
    a.authorization_event_id,

    DATEDIFF(
        day,
        c.claim_admit_date,
        a.authorization_admit_date
    ) AS authorization_date_difference,

    CASE
        WHEN c.claim_admit_date =
             a.authorization_admit_date
            THEN 'Exact Authorization Date'

        ELSE 'Fuzzy Authorization Date ±1 Day'
    END AS authorization_match_method

FROM tmp_claims_2025 c

INNER JOIN tmp_authorization_events a
    ON c.member_id = a.member_id

CROSS JOIN tmp_adt_params p

WHERE ABS(
        DATEDIFF(
            day,
            c.claim_admit_date,
            a.authorization_admit_date
        )
      ) <= p.fuzzy_days

QUALIFY
    ROW_NUMBER() OVER
    (
        PARTITION BY c.claim_event_id

        ORDER BY
            ABS(
                DATEDIFF(
                    day,
                    c.claim_admit_date,
                    a.authorization_admit_date
                )
            ),

            a.authorization_event_timestamp,
            a.authorization_event_id
    ) = 1;


/*==============================================================================
  12. BUILD CLAIM-CENTERED PATIENT EVENT SEQUENCES

  Example:
    Authorization > Admission > Transfer > Update > Discharge
==============================================================================*/

DROP TABLE IF EXISTS tmp_claim_event_sequence_rows;

CREATE TEMP TABLE tmp_claim_event_sequence_rows AS

SELECT
    m.claim_event_id,
    r.event_sequence_ts,
    r.event_priority,
    r.event_name,
    r.message_type,
    r.sending_source

FROM tmp_hie_match_map m

INNER JOIN tmp_hie_events h
    ON m.adt_event_id = h.adt_event_id

INNER JOIN tmp_adt_reduced r
    ON  h.member_id = r.member_id
    AND h.adt_admit_date = r.adt_admit_date
    AND r.source_category = 'HIE_ADT'

UNION ALL

SELECT
    am.claim_event_id,
    r.event_sequence_ts,
    r.event_priority,
    r.event_name,
    r.message_type,
    r.sending_source

FROM tmp_authorization_match am

INNER JOIN tmp_authorization_events a
    ON am.authorization_event_id =
       a.authorization_event_id

INNER JOIN tmp_adt_reduced r
    ON  a.member_id = r.member_id
    AND a.authorization_admit_date =
        r.adt_admit_date
    AND r.source_category = 'AUTHORIZATION';


DROP TABLE IF EXISTS tmp_claim_event_sequence_2025;

CREATE TEMP TABLE tmp_claim_event_sequence_2025 AS

SELECT
    claim_event_id,

    LISTAGG(
        event_name,
        ' > '
    )
    WITHIN GROUP
    (
        ORDER BY
            event_sequence_ts,
            event_priority,
            sending_source,
            message_type
    ) AS patient_event_sequence,

    LISTAGG(
        COALESCE(message_type, 'AUTH'),
        ' > '
    )
    WITHIN GROUP
    (
        ORDER BY
            event_sequence_ts,
            event_priority,
            sending_source,
            message_type
    ) AS message_type_sequence,

    LISTAGG(
        COALESCE(sending_source, 'UNKNOWN'),
        ' > '
    )
    WITHIN GROUP
    (
        ORDER BY
            event_sequence_ts,
            event_priority,
            sending_source,
            message_type
    ) AS sending_source_sequence

FROM tmp_claim_event_sequence_rows

GROUP BY claim_event_id;


/*==============================================================================
  13. FINAL CLAIMS-CENTERED ADT DISCHARGE QA TABLE
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

        c.claim_admit_ts,
        c.claim_discharge_ts,
        c.claim_admit_date,
        c.claim_discharge_date,

        m.match_method,
        m.admit_date_difference,

        h.adt_event_id,
        h.adt_admit_ts,
        h.adt_admit_date,

        h.first_hie_source,
        h.hie_sending_sources,
        h.message_types_seen,

        h.first_admission_message_type,
        h.first_admission_message_timestamp,
        h.first_admission_insert_timestamp,

        h.first_a03_discharge_ts,
        h.first_a03_message_timestamp,
        h.first_a03_insert_timestamp,

        h.latest_discharge_ts_any,
        h.latest_discharge_message_type,

        COALESCE
        (
            h.first_a03_discharge_ts,
            h.latest_discharge_ts_any
        ) AS adt_discharge_ts,

        CAST(
            COALESCE
            (
                h.first_a03_discharge_ts,
                h.latest_discharge_ts_any
            )
            AS DATE
        ) AS adt_discharge_date,

        h.total_hie_message_count,
        h.a01_message_count,
        h.a02_transfer_count,
        h.a03_message_count,
        h.a08_message_count,

        a.authorization_event_id,
        a.authorization_admit_date,
        a.authorization_source,
        a.authorization_event_timestamp,
        a.authorization_insert_timestamp,

        am.authorization_match_method,
        am.authorization_date_difference,

        s.patient_event_sequence,
        s.message_type_sequence,
        s.sending_source_sequence,

        DATEDIFF
        (
            day,
            c.claim_discharge_date,
            CAST
            (
                COALESCE
                (
                    h.first_a03_discharge_ts,
                    h.latest_discharge_ts_any
                )
                AS DATE
            )
        ) AS discharge_date_difference,

        DATEDIFF
        (
            second,
            h.first_a03_discharge_ts,
            h.first_a03_message_timestamp
        ) / 3600.0 AS a03_source_lag_hours,

        DATEDIFF
        (
            second,
            h.first_a03_message_timestamp,
            h.first_a03_insert_timestamp
        ) / 60.0 AS a03_pipeline_lag_minutes,

        DATEDIFF
        (
            second,
            h.first_a03_discharge_ts,
            h.first_a03_insert_timestamp
        ) / 3600.0 AS discharge_end_to_end_lag_hours,

        DATEDIFF
        (
            day,
            CAST(h.first_a03_discharge_ts AS DATE),
            CAST(h.first_a03_insert_timestamp AS DATE)
        ) AS discharge_calendar_lag_days

    FROM tmp_claims_2025 c

    LEFT JOIN tmp_hie_match_map m
        ON c.claim_event_id = m.claim_event_id

    LEFT JOIN tmp_hie_events h
        ON m.adt_event_id = h.adt_event_id

    LEFT JOIN tmp_authorization_match am
        ON c.claim_event_id = am.claim_event_id

    LEFT JOIN tmp_authorization_events a
        ON am.authorization_event_id =
           a.authorization_event_id

    LEFT JOIN tmp_claim_event_sequence_2025 s
        ON c.claim_event_id = s.claim_event_id
)

SELECT
    base.*,

    CASE
        WHEN adt_event_id IS NOT NULL THEN 1
        ELSE 0
    END AS hie_adt_matched_flag,

    CASE
        WHEN adt_event_id IS NULL THEN 1
        ELSE 0
    END AS missing_hie_adt_flag,

    CASE
        WHEN authorization_event_id IS NOT NULL THEN 1
        ELSE 0
    END AS authorization_present_flag,

    CASE
        WHEN COALESCE(a03_message_count, 0) > 0 THEN 1
        ELSE 0
    END AS a03_present_flag,

    CASE
        WHEN adt_event_id IS NOT NULL
         AND
         (
             COALESCE(a03_message_count, 0) = 0
             OR first_a03_discharge_ts IS NULL
         )
            THEN 1
        ELSE 0
    END AS missing_a03_discharge_flag,

    CASE
        WHEN adt_discharge_ts IS NULL THEN 1
        ELSE 0
    END AS missing_any_adt_discharge_flag,

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
        WHEN first_a03_discharge_ts IS NOT NULL
            THEN 'A03'

        WHEN latest_discharge_ts_any IS NOT NULL
            THEN 'Non-A03 ADT Message'

        ELSE 'Missing'
    END AS discharge_source,

    CASE
        WHEN claim_discharge_date IS NULL
            THEN 'Claims Discharge Date Missing'

        WHEN adt_event_id IS NULL
            THEN 'Missing HIE ADT Encounter'

        WHEN COALESCE(a03_message_count, 0) = 0
         AND adt_discharge_ts IS NULL
            THEN 'HIE Matched, ADT Discharge Missing'

        WHEN COALESCE(a03_message_count, 0) = 0
         AND adt_discharge_ts IS NOT NULL
            THEN 'Discharge Found, No A03'

        WHEN COALESCE(a03_message_count, 0) > 0
         AND first_a03_discharge_ts IS NULL
            THEN 'A03 Present, Discharge Date Missing'

        WHEN discharge_date_difference = 0
            THEN 'Matched Discharge Date'

        WHEN ABS(discharge_date_difference) <= 1
            THEN 'Discharge Within ±1 Day'

        ELSE 'Discharge Mismatch >1 Day'
    END AS discharge_quality_status,

    CASE
        WHEN discharge_end_to_end_lag_hours IS NULL
            THEN 'Lag Unavailable'

        WHEN discharge_end_to_end_lag_hours < 0
            THEN 'Negative Lag - Investigate'

        WHEN discharge_end_to_end_lag_hours <= 4
            THEN '0-4 Hours'

        WHEN discharge_end_to_end_lag_hours <= 24
            THEN '4-24 Hours'

        WHEN discharge_end_to_end_lag_hours <= 48
            THEN '24-48 Hours'

        ELSE 'Over 48 Hours'
    END AS discharge_lag_status

FROM base;


/*==============================================================================
  14. 2025 SUMMARY

  The primary missing ADT percentage is:

    Claims admissions without a hospital-sourced HIE ADT
    -----------------------------------------------------
                 All 2025 claims admissions
==============================================================================*/

DROP TABLE IF EXISTS tmp_adt_summary_2025;

CREATE TEMP TABLE tmp_adt_summary_2025 AS

SELECT
    COUNT(*) AS total_2025_claims_admissions,

    SUM(hie_adt_matched_flag)
        AS hie_adt_matched_admissions,

    SUM(missing_hie_adt_flag)
        AS admissions_missing_hie_adt,

    ROUND
    (
        100.0
        * SUM(missing_hie_adt_flag)
        / NULLIF(COUNT(*), 0),
        2
    ) AS percent_admissions_missing_hie_adt,

    SUM(authorization_present_flag)
        AS admissions_with_authorization,

    SUM(
        CASE
            WHEN coverage_category = 'Authorization Only'
                THEN 1
            ELSE 0
        END
    ) AS authorization_only_admissions,

    ROUND
    (
        100.0
        * SUM(
            CASE
                WHEN coverage_category = 'Authorization Only'
                    THEN 1
                ELSE 0
            END
        )
        / NULLIF(COUNT(*), 0),
        2
    ) AS percent_authorization_only,

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

    SUM(a03_present_flag)
        AS matched_admissions_with_a03,

    SUM(missing_a03_discharge_flag)
        AS matched_admissions_missing_valid_a03,

    SUM(
        CASE
            WHEN discharge_quality_status =
                 'Matched Discharge Date'
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
    ) AS discharge_matches_within_one_day,

    SUM(
        CASE
            WHEN discharge_quality_status =
                 'Discharge Mismatch >1 Day'
                THEN 1
            ELSE 0
        END
    ) AS discharge_mismatches_over_one_day,

    (
        SELECT
            PERCENTILE_CONT(0.50)
            WITHIN GROUP
            (
                ORDER BY discharge_end_to_end_lag_hours
            )

        FROM tmp_adt_discharge_qa_2025

        WHERE discharge_end_to_end_lag_hours >= 0
    ) AS median_discharge_lag_hours,

    (
        SELECT
            PERCENTILE_CONT(0.90)
            WITHIN GROUP
            (
                ORDER BY discharge_end_to_end_lag_hours
            )

        FROM tmp_adt_discharge_qa_2025

        WHERE discharge_end_to_end_lag_hours >= 0
    ) AS p90_discharge_lag_hours,

    ROUND
    (
        100.0
        * SUM(
            CASE
                WHEN discharge_end_to_end_lag_hours
                     BETWEEN 0 AND 24
                    THEN 1
                ELSE 0
            END
        )
        / NULLIF(
            SUM(
                CASE
                    WHEN discharge_end_to_end_lag_hours >= 0
                        THEN 1
                    ELSE 0
                END
            ),
            0
        ),
        2
    ) AS percent_valid_a03_lags_within_24_hours

FROM tmp_adt_discharge_qa_2025;


/*==============================================================================
  15. MONTHLY QA SUMMARY
==============================================================================*/

DROP TABLE IF EXISTS tmp_adt_monthly_summary_2025;

CREATE TEMP TABLE tmp_adt_monthly_summary_2025 AS

SELECT
    CAST(
        DATE_TRUNC(
            'month',
            claim_admit_date
        )
        AS DATE
    ) AS admission_month,

    COUNT(*) AS claims_admissions,

    SUM(hie_adt_matched_flag)
        AS hie_adt_matched,

    SUM(missing_hie_adt_flag)
        AS missing_hie_adt,

    ROUND
    (
        100.0
        * SUM(missing_hie_adt_flag)
        / NULLIF(COUNT(*), 0),
        2
    ) AS percent_missing_hie_adt,

    SUM(authorization_present_flag)
        AS authorization_present,

    SUM(a03_present_flag)
        AS a03_present,

    SUM(missing_a03_discharge_flag)
        AS missing_valid_a03,

    AVG(
        CASE
            WHEN discharge_end_to_end_lag_hours >= 0
                THEN discharge_end_to_end_lag_hours
        END
    ) AS average_discharge_lag_hours

FROM tmp_adt_discharge_qa_2025

GROUP BY
    CAST(
        DATE_TRUNC(
            'month',
            claim_admit_date
        )
        AS DATE
    );


/*==============================================================================
  16. HIE ADT EVENTS THAT DID NOT MATCH CLAIMS
==============================================================================*/

DROP TABLE IF EXISTS tmp_unmatched_hie_adt_2025;

CREATE TEMP TABLE tmp_unmatched_hie_adt_2025 AS

SELECT
    h.*

FROM tmp_hie_events h

LEFT JOIN tmp_hie_match_map m
    ON h.adt_event_id = m.adt_event_id

WHERE m.adt_event_id IS NULL;


/*==============================================================================
  17. REVIEW RESULTS
==============================================================================*/

SELECT *
FROM tmp_adt_summary_2025;


SELECT *
FROM tmp_adt_monthly_summary_2025
ORDER BY admission_month;


SELECT *
FROM tmp_adt_discharge_qa_2025
ORDER BY
    claim_admit_date,
    member_id;
```

# Persist the final tables

Temporary tables disappear when the Redshift session closes. To retain the results:

```sql
DROP TABLE IF EXISTS analytics.adt_discharge_qa_2025;

CREATE TABLE analytics.adt_discharge_qa_2025
DISTSTYLE KEY
DISTKEY (member_id)
SORTKEY (claim_admit_date, member_id)
AS

SELECT *
FROM tmp_adt_discharge_qa_2025;
```

```sql
DROP TABLE IF EXISTS analytics.adt_discharge_summary_2025;

CREATE TABLE analytics.adt_discharge_summary_2025 AS

SELECT *
FROM tmp_adt_summary_2025;
```

```sql
DROP TABLE IF EXISTS analytics.adt_event_sequence_2025;

CREATE TABLE analytics.adt_event_sequence_2025 AS

SELECT *
FROM tmp_claim_event_sequence_2025;
```

# Export for the spreadsheet

The simplest option is to export `analytics.adt_discharge_qa_2025` through your SQL client. You can also unload it to S3:

```sql
UNLOAD
(
    '
    SELECT *
    FROM analytics.adt_discharge_qa_2025
    ORDER BY claim_admit_date, member_id
    '
)
TO 's3://YOUR-BUCKET/adt-qa/adt_discharge_qa_2025_'
IAM_ROLE 'arn:aws:iam::ACCOUNT-ID:role/YOUR-REDSHIFT-ROLE'
FORMAT AS CSV
HEADER
ALLOWOVERWRITE
PARALLEL OFF;
```

## Key adjustment to the existing window function

Do not use:

```sql
ROW_NUMBER() OVER
(
    PARTITION BY member_id, admit_date
    ORDER BY event_registry_timestamp
) = 1
```

That keeps only the earliest hospitalization message and can remove A02, A03 and A08.

The SQL above instead keeps:

* First authorization event
* First admission message
* All unique transfers
* First and last A03
* First and last A08

This preserves discharge timeliness and patient event sequencing while still reducing the ADT volume substantially.

```sql
DROP TABLE IF EXISTS tmp_hie_events;

CREATE TEMP TABLE tmp_hie_events
DISTSTYLE KEY
DISTKEY (member_id)
SORTKEY (member_id, adt_admit_date)
AS

WITH event_metrics AS
(
    SELECT
        h.member_id,
        h.adt_admit_date,

        MIN(h.adt_admit_ts) AS adt_admit_ts,

        MAX(
            CASE
                WHEN h.first_hie_event_rn = 1
                    THEN h.sending_source
            END
        ) AS first_hie_source,

        MAX(
            CASE
                WHEN h.first_admission_message_rn = 1
                 AND h.message_type IN ('A01', 'A04', 'A06')
                    THEN h.message_type
            END
        ) AS first_admission_message_type,

        MAX(
            CASE
                WHEN h.first_admission_message_rn = 1
                 AND h.message_type IN ('A01', 'A04', 'A06')
                    THEN h.message_timestamp
            END
        ) AS first_admission_message_timestamp,

        MAX(
            CASE
                WHEN h.first_admission_message_rn = 1
                 AND h.message_type IN ('A01', 'A04', 'A06')
                    THEN h.insert_timestamp
            END
        ) AS first_admission_insert_timestamp,

        MAX(
            CASE
                WHEN h.first_a03_message_rn = 1
                 AND h.message_type = 'A03'
                    THEN h.adt_discharge_ts
            END
        ) AS first_a03_discharge_ts,

        MAX(
            CASE
                WHEN h.first_a03_message_rn = 1
                 AND h.message_type = 'A03'
                    THEN h.message_timestamp
            END
        ) AS first_a03_message_timestamp,

        MAX(
            CASE
                WHEN h.first_a03_message_rn = 1
                 AND h.message_type = 'A03'
                    THEN h.insert_timestamp
            END
        ) AS first_a03_insert_timestamp,

        MAX(
            CASE
                WHEN h.latest_discharge_value_rn = 1
                    THEN h.adt_discharge_ts
            END
        ) AS latest_discharge_ts_any,

        MAX(
            CASE
                WHEN h.latest_discharge_value_rn = 1
                    THEN h.message_type
            END
        ) AS latest_discharge_message_type

    FROM tmp_hie_message_ranked h

    GROUP BY
        h.member_id,
        h.adt_admit_date
),


/* Distinct sending sources */
source_values AS
(
    SELECT DISTINCT
        member_id,
        adt_admit_date,
        sending_source

    FROM tmp_hie_message_ranked

    WHERE sending_source IS NOT NULL
),

source_list AS
(
    SELECT
        member_id,
        adt_admit_date,

        LISTAGG(
            sending_source,
            ', '
        )
        WITHIN GROUP
        (
            ORDER BY sending_source
        ) AS hie_sending_sources

    FROM source_values

    GROUP BY
        member_id,
        adt_admit_date
),


/* Distinct message types */
message_type_values AS
(
    SELECT DISTINCT
        member_id,
        adt_admit_date,
        message_type

    FROM tmp_hie_message_ranked

    WHERE message_type IS NOT NULL
),

message_type_list AS
(
    SELECT
        member_id,
        adt_admit_date,

        LISTAGG(
            message_type,
            ','
        )
        WITHIN GROUP
        (
            ORDER BY message_type
        ) AS message_types_seen

    FROM message_type_values

    GROUP BY
        member_id,
        adt_admit_date
),


/* Message counts */
message_counts AS
(
    SELECT
        member_id,
        adt_admit_date,

        MAX(total_message_count)
            AS total_hie_message_count,

        MAX(a01_message_count)
            AS a01_message_count,

        MAX(a02_transfer_count)
            AS a02_transfer_count,

        MAX(a03_message_count)
            AS a03_message_count,

        MAX(a08_message_count)
            AS a08_message_count

    FROM tmp_adt_message_counts

    WHERE source_category = 'HIE_ADT'

    GROUP BY
        member_id,
        adt_admit_date
),


combined AS
(
    SELECT
        e.member_id,
        e.adt_admit_date,
        e.adt_admit_ts,

        e.first_hie_source,

        s.hie_sending_sources,
        mt.message_types_seen,

        e.first_admission_message_type,
        e.first_admission_message_timestamp,
        e.first_admission_insert_timestamp,

        e.first_a03_discharge_ts,
        e.first_a03_message_timestamp,
        e.first_a03_insert_timestamp,

        e.latest_discharge_ts_any,
        e.latest_discharge_message_type,

        COALESCE(
            c.total_hie_message_count,
            0
        ) AS total_hie_message_count,

        COALESCE(
            c.a01_message_count,
            0
        ) AS a01_message_count,

        COALESCE(
            c.a02_transfer_count,
            0
        ) AS a02_transfer_count,

        COALESCE(
            c.a03_message_count,
            0
        ) AS a03_message_count,

        COALESCE(
            c.a08_message_count,
            0
        ) AS a08_message_count

    FROM event_metrics e

    LEFT JOIN source_list s
        ON  e.member_id = s.member_id
        AND e.adt_admit_date = s.adt_admit_date

    LEFT JOIN message_type_list mt
        ON  e.member_id = mt.member_id
        AND e.adt_admit_date = mt.adt_admit_date

    LEFT JOIN message_counts c
        ON  e.member_id = c.member_id
        AND e.adt_admit_date = c.adt_admit_date
)

SELECT
    ROW_NUMBER() OVER
    (
        ORDER BY
            member_id,
            adt_admit_date
    )::BIGINT AS adt_event_id,

    combined.*

FROM combined;
```
