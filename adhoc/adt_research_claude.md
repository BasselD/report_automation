Good catches on both — dates-only vs timestamps changes the fuzzy-match math, and using `message_type` explicitly is much more accurate than inferring admission/discharge from a generic "ADT event". Let me flag one real risk in your eligibility query first, then give you the corrected core reconciliation query. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/54907034/a42fb239-6824-4a64-980e-162dc1b3667c/paste.txt?AWSAccessKeyId=ASIA2F3EMEYEUN34RBXN&Signature=xEI1YfFJ6JQn8UC2eLwVCwIBqVA%3D&x-amz-security-token=IQoJb3JpZ2luX2VjEH0aCXVzLWVhc3QtMSJGMEQCIAhbhou2gZMf0Ue%2FxyiZD%2FnEHcj2rPd58k4nk2DLK5iBAiAp8ByPlSpfLBB3AQdbYzBOG%2B8UPqHziApazOs2xJCxaSrzBAhGEAEaDDY5OTc1MzMwOTcwNSIMMMrdzdxDEHMNP0RDKtAEvVBYlj%2FJFDUHUIWjkvIaXggIgPhWEHknu6RbJVLaxU3VeKg%2Bv8skpne2epkYz%2B3zZqDIy2oXoPp6yGR3EnS7D12VdWOJJcxIhx4XMcbvMPgkBm%2FtwLMsqtthufENQ1CnnkeOmtNnxEkkCLVqlbdnuFTMJc9ST6tSPob2SlT7ZvRvuECC%2Fe0DjG3FzATVRyX%2FWQSgjXCpiGIcPRVNv3nhYogeuiSsClcLnIRxCniKvLyjVPI8dKEEQhyKDNRqBvq09vPeZn6v4o7vZUoLUVus1zJ0XKsLmgYhrrHTn7Q2%2BEFFAHtXqmnOzMHuBrV3CyahNScCSjwzCVZSbp6USER0LkufqBpk%2F%2Ff9PEn8lXQKUJkAMMNilVFLXZYfrs%2FdkAKTt5IaJM3%2FYU%2BC6ACQVSJ8KXKHb5vbXE1BBPFcElb%2FWrSAqzjJXWpT%2Bw%2FN32Rif2VIdJsK2nO4NYVrC6c3bNaFBYFuGqasBQvg%2BJVrklb%2BZpsulmeT5UkEzzMSnOHCnRlOlWBv%2FdJkjFzyX4hD5FLxihDzMOig3bDATeyUSJjfBasJoV%2BOF1FFgb8ctca%2B%2BtvAf85%2FwRSNKMVDv8s5dXJwCOZbQTWKaCFO%2FMqzObQFD5hDTC4F%2FhPGMR4k1n3hacCsE3dkiACjfjZQyGGT5p2a2SXj7sG6I92mvX2p%2BjtZt%2F1EtvimAd2dtsMnrpp8cPsqRq0i%2B0R35o%2BfFtM9JrnNii%2BbHenMI9OwF4UFTFefvEi6DCqEXH0le145NbNzjtQ7sKSN3u3cDDly2a7MNf9J7TDQqOPSBjqZAcZm6XtIaayHbqI9Lii4aRvM5xOPNsE5eG64kZ9GUfQ31xTH5yhVXgr0kAVWF6uX5apxaCyQjUtWo1J4PD8LaRWPgEWCrgyu0JgE94Nsex%2Bq2%2FOnyLRk4I4HQzExA07zsvE3Jz2iRqJ44evGOC9eEX82V72kIdH5c%2FYqpN4QehdYeIrRKc2uCtnk3hblr%2FRfNDs6bwKxlt%2BVtg%3D%3D&Expires=1784209955)

## Issue in your eligibility query

```sql
SELECT DISTINCT personid, memberno
FROM sandbox.CA_Core_Enrollment_Detail
WHERE carealliesmanagedflag = 'Y'
  AND startdate <= '2025-12-31'
  AND enddate >= '2025-01-01'
```

This is a classic join fan-out risk. Since the source table is a **monthly snapshot**, a single `personid` can have multiple distinct `memberno` values across 2025 (e.g., a plan/ID change mid-year). `DISTINCT personid, memberno` doesn't collapse to one row per person — it collapses to one row per person-memberno combination. When you then join `adt.person_id = enr.personid`, any person with 2+ historical member numbers will get their ADT rows duplicated once per memberno match. [datarekha](https://datarekha.com/interview/sql/join-fan-out-duplicates/)

**Fix — pick one memberno per person deterministically:**

```sql
WITH Target_2025_Members AS (
    SELECT personid, memberno
    FROM (
        SELECT
            personid,
            memberno,
            ROW_NUMBER() OVER (
                PARTITION BY personid
                ORDER BY enddate DESC, startdate DESC
            ) AS rn
        FROM sandbox.CA_Core_Enrollment_Detail
        WHERE carealliesmanagedflag = 'Y'
          AND startdate <= '2025-12-31'
          AND enddate >= '2025-01-01'
    ) ranked
    WHERE rn = 1
)
```

This keeps the most recent memberno per person and guarantees a 1:1 roster before you join to ADT, eliminating the fan-out risk entirely. Everything else in your query — the overlap logic (`startdate <= '2025-12-31' AND enddate >= '2025-01-01'`) and the inner join structure — is correct for capturing anyone who touched 2025. [tryexponent](https://www.tryexponent.com/courses/sql-interviews/sql-joins-and-duplicate-control)

## Updated core reconciliation query

This version treats `admit_date`/`discharge_date` as plain dates (no timestamp casting), uses `message_type` explicitly to separate A01 (admission) from A03 (discharge), keeps `bed_type`, and sources ADT from your corrected `adt_events_2025` roster-filtered table.

```sql
DROP TABLE IF EXISTS tmp_adt_params;
CREATE TEMP TABLE tmp_adt_params AS
SELECT 1::INTEGER AS fuzzy_days;

/*==============================================================================
  1. CLAIMS (already deduped) — Admit/Discharge are dates, plus BedType
==============================================================================*/
DROP TABLE IF EXISTS tmp_claims_2025;
CREATE TEMP TABLE tmp_claims_2025 AS
SELECT
    ROW_NUMBER() OVER (ORDER BY member_id, claim_admit_date) ::BIGINT AS claim_event_id,
    member_id,
    claim_admit_date,
    claim_discharge_date,
    bed_type
FROM (
    SELECT
        TRIM(UPPER(CAST(c.MemberID AS VARCHAR(100)))) AS member_id,
        TRY_CAST(c.Admit AS DATE)     AS claim_admit_date,
        TRY_CAST(c.Discharge AS DATE) AS claim_discharge_date,
        TRIM(CAST(c.BedType AS VARCHAR(100))) AS bed_type
    FROM analytics.inpatient_claims c
    WHERE TRY_CAST(c.Admit AS DATE) >= '2025-01-01'
      AND TRY_CAST(c.Admit AS DATE) <  '2026-01-01'
) normalized
WHERE member_id IS NOT NULL
  AND claim_admit_date IS NOT NULL;

/*==============================================================================
  2. CLASSIFY ADT — source (MedHOK vs HIE) AND message_type (A01 vs A03)
     Source: adt_events_2025 (already roster-filtered to 2025 CA members)
==============================================================================*/
DROP TABLE IF EXISTS tmp_adt_base;
CREATE TEMP TABLE tmp_adt_base AS
SELECT
    TRIM(UPPER(CAST(memberno AS VARCHAR(100)))) AS member_id,
    admit_date,
    discharge_date,
    UPPER(TRIM(message_type)) AS message_type,
    CASE
        WHEN UPPER(TRIM(COALESCE(sending_source, ''))) LIKE '%MEDHOK%'
            THEN 'AUTHORIZATION'
        WHEN sending_source IS NOT NULL
            THEN 'HIE_ADT'
        ELSE 'UNCLASSIFIED'
    END AS source_category
FROM adt_events_2025
WHERE memberno IS NOT NULL
  AND admit_date IS NOT NULL;

/*==============================================================================
  3. ONE ROW PER MEMBER + ADMIT DATE + SOURCE, using message_type to pick
     the admission date (A01) and discharge date (A03) explicitly
==============================================================================*/
DROP TABLE IF EXISTS tmp_adt_events;
CREATE TEMP TABLE tmp_adt_events AS
SELECT
    ROW_NUMBER() OVER (ORDER BY member_id, admit_date, source_category) ::BIGINT AS adt_event_id,
    member_id,
    admit_date AS adt_admit_date,
    source_category,
    MAX(CASE WHEN message_type = 'A01' THEN admit_date END) AS a01_admit_date,
    MAX(CASE WHEN message_type = 'A03' THEN discharge_date END) AS a03_discharge_date,
    MAX(discharge_date) AS any_discharge_date,
    LISTAGG(DISTINCT message_type, ',') AS message_types_seen
FROM tmp_adt_base
WHERE source_category IN ('AUTHORIZATION', 'HIE_ADT')
GROUP BY member_id, admit_date, source_category;

DROP TABLE IF EXISTS tmp_hie_events;
CREATE TEMP TABLE tmp_hie_events AS
SELECT * FROM tmp_adt_events WHERE source_category = 'HIE_ADT';

DROP TABLE IF EXISTS tmp_authorization_events;
CREATE TEMP TABLE tmp_authorization_events AS
SELECT * FROM tmp_adt_events WHERE source_category = 'AUTHORIZATION';

/*==============================================================================
  4. EXACT + FUZZY MATCH — HIE
==============================================================================*/
DROP TABLE IF EXISTS tmp_exact_hie_match;
CREATE TEMP TABLE tmp_exact_hie_match AS
SELECT c.claim_event_id, h.adt_event_id, 'Exact Admit Date' AS match_method, 0 AS admit_date_difference
FROM tmp_claims_2025 c
INNER JOIN tmp_hie_events h
    ON c.member_id = h.member_id AND c.claim_admit_date = h.adt_admit_date;

DROP TABLE IF EXISTS tmp_fuzzy_hie_match;
CREATE TEMP TABLE tmp_fuzzy_hie_match AS
SELECT claim_event_id, adt_event_id, match_method, admit_date_difference
FROM (
    SELECT
        c.claim_event_id,
        h.adt_event_id,
        DATEDIFF(day, c.claim_admit_date, h.adt_admit_date) AS admit_date_difference,
        'Fuzzy Admit Date +/-1 Day' AS match_method,
        ROW_NUMBER() OVER (PARTITION BY c.claim_event_id
            ORDER BY ABS(DATEDIFF(day, c.claim_admit_date, h.adt_admit_date))) AS claim_rn,
        ROW_NUMBER() OVER (PARTITION BY h.adt_event_id
            ORDER BY ABS(DATEDIFF(day, c.claim_admit_date, h.adt_admit_date))) AS adt_rn
    FROM tmp_claims_2025 c
    INNER JOIN tmp_hie_events h ON c.member_id = h.member_id
    LEFT JOIN tmp_exact_hie_match ec ON c.claim_event_id = ec.claim_event_id
    LEFT JOIN tmp_exact_hie_match ea ON h.adt_event_id = ea.adt_event_id
    CROSS JOIN tmp_adt_params p
    WHERE ec.claim_event_id IS NULL
      AND ea.adt_event_id IS NULL
      AND ABS(DATEDIFF(day, c.claim_admit_date, h.adt_admit_date)) <= p.fuzzy_days
) x
WHERE claim_rn = 1 AND adt_rn = 1;

DROP TABLE IF EXISTS tmp_hie_match_map;
CREATE TEMP TABLE tmp_hie_match_map AS
SELECT * FROM tmp_exact_hie_match
UNION ALL
SELECT * FROM tmp_fuzzy_hie_match;

/*==============================================================================
  5. EXACT + FUZZY MATCH — AUTHORIZATION (MedHOK)
==============================================================================*/
DROP TABLE IF EXISTS tmp_authorization_match;
CREATE TEMP TABLE tmp_authorization_match AS
SELECT claim_event_id, adt_event_id, match_method
FROM (
    SELECT
        c.claim_event_id,
        a.adt_event_id,
        CASE WHEN c.claim_admit_date = a.adt_admit_date
             THEN 'Exact Authorization Date'
             ELSE 'Fuzzy Authorization Date +/-1 Day' END AS match_method,
        ROW_NUMBER() OVER (PARTITION BY c.claim_event_id
            ORDER BY ABS(DATEDIFF(day, c.claim_admit_date, a.adt_admit_date))) AS claim_rn
    FROM tmp_claims_2025 c
    INNER JOIN tmp_authorization_events a ON c.member_id = a.member_id
    CROSS JOIN tmp_adt_params p
    WHERE ABS(DATEDIFF(day, c.claim_admit_date, a.adt_admit_date)) <= p.fuzzy_days
) x
WHERE claim_rn = 1;

/*==============================================================================
  6. FINAL RECONCILED OUTPUT — includes A01/A03 dates, BedType, discharge check
==============================================================================*/
DROP TABLE IF EXISTS analytics.adt_claims_reconciliation_2025;
CREATE TABLE analytics.adt_claims_reconciliation_2025 AS
SELECT
    c.claim_event_id,
    c.member_id,
    c.claim_admit_date,
    c.claim_discharge_date,
    c.bed_type,

    h.adt_admit_date        AS hie_adt_admit_date,
    h.a01_admit_date         AS hie_a01_admit_date,
    h.a03_discharge_date     AS hie_a03_discharge_date,
    h.message_types_seen     AS hie_message_types_seen,
    hm.match_method          AS hie_match_method,

    a.adt_admit_date          AS authorization_admit_date,
    am.match_method           AS authorization_match_method,

    CASE WHEN h.adt_event_id IS NOT NULL AND a.adt_event_id IS NOT NULL THEN 'Authorization and HIE ADT'
         WHEN a.adt_event_id IS NOT NULL AND h.adt_event_id IS NULL THEN 'Authorization Only'
         WHEN h.adt_event_id IS NOT NULL AND a.adt_event_id IS NULL THEN 'HIE ADT Only'
         ELSE 'No Authorization or HIE ADT'
    END AS coverage_category,

    CASE WHEN h.a03_discharge_date = c.claim_discharge_date THEN 'Matched Discharge Date'
         WHEN h.a03_discharge_date IS NOT NULL THEN 'Discharge Mismatch'
         ELSE 'No A03 Discharge Found'
    END AS discharge_quality_status

FROM tmp_claims_2025 c
LEFT JOIN tmp_hie_match_map hm ON c.claim_event_id = hm.claim_event_id
LEFT JOIN tmp_hie_events h ON hm.adt_event_id = h.adt_event_id
LEFT JOIN tmp_authorization_match am ON c.claim_event_id = am.claim_event_id
LEFT JOIN tmp_authorization_events a ON am.adt_event_id = a.adt_event_id;
```

## Key changes from the prior version

- All timestamp casts (`TRY_CAST(... AS TIMESTAMP)`) replaced with `DATE` casts throughout, since Admit/Discharge on claims and admit_date/discharge_date on ADT are dates only.
- `message_type` now explicitly drives `a01_admit_date` and `a03_discharge_date` in step 3, so admission and discharge are pulled from the correct HL7 message type rather than just "earliest/latest timestamp on the record."
- `bed_type` carried through untouched from claims.
- Source table swapped to `adt_events_2025`, which already applies your 2025 roster filter — just make sure to apply the `ROW_NUMBER` fix above first so that filter doesn't introduce duplicate ADT rows per person.

Do you want me to also add a quick validation query that checks `COUNT(*)` vs `COUNT(DISTINCT personid)` on your corrected `Target_2025_Members` CTE, so you can confirm the fan-out fix worked before running the full reconciliation?
