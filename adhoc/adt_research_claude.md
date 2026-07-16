Here's the simplified core reconciliation query, stripped down to just the essential matching logic, with claims deduplication removed (since your claims are already deduped) and `BedType` carried through. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/54907034/a42fb239-6824-4a64-980e-162dc1b3667c/paste.txt?AWSAccessKeyId=ASIA2F3EMEYE7GCRHNCQ&Signature=8neJEXhzWI8li70Q2gS%2B%2B8%2BSXn0%3D&x-amz-security-token=IQoJb3JpZ2luX2VjEH0aCXVzLWVhc3QtMSJGMEQCIE7ocZbiyCRRXztQsJ175PUjfsLy54%2FXpbmpkjktKFkSAiB1bG5xDYBtdOhiaD6qyA17xUjLjbXB%2FMJhc9rhNsF8QyrzBAhGEAEaDDY5OTc1MzMwOTcwNSIM4XPwDJC0nkk7CFXPKtAExR%2Fe8r7ifDMc7jX0fvY48HdNS%2BitQxMlJyko2Z2fSSd7U4Ko6TtkEsg8ZaoS%2BpNSegUc1%2BoscVdl8SNTzRpmj%2Fxbb2UH0pXxnB4EYPfYFKFTvsz0rlGC%2F4GvgFM4EFP0dPztGGste17Kyj5ESXzA4xgiXNwkZQG4G8baNtY%2FXuDGe%2FaqeIqaOV3DbNkwbj3KxasCvdLjQ8I%2BUzjNX0%2FlA2zuh6FJY%2FZHVQ2qJZJwAzDC%2FbqMJxtEVwxhVZT%2F7rifjydK7tE2lNuVdUWgFQ5rDWBTFaP%2BXyaHI1LQNiQDnn0niJ3KskPeOgc6OMAUZSAEvkkJbMUDeHNRcDbYdu6ibhwf4MJ%2FTizxK7GV0Zs07PMT0iOxqljpnUGg0TckzN%2FuByKRy%2Fr2WOpWhMCbAyZuFSVY8vzndq7YHf%2BYFpj5XoXDIKWLOBDRrn875wVBlDB3g8vHEyo7jsH%2BOO2jw3tfQnIZ9%2FEOhO2Eicgm7oRV32eanpFuyTcGl%2BCPGxBdJsnD7n6yZUFEqTXg20AyiPBH4c70cQFZ8HRD85CDh4635wx6fse5bi13uYxVclpqPVHEWot46S0OtLXcPDgYzA%2B%2BfeeLx3dJAMSIsFqH6Y0WPECjpl1RROvNJV5HKpExLpkGK4GuWs2dRynGPYDxcOTP2RDczayjbzmCgR04xS3UekroWpnXFZR968kncHZ54OJpxYurSrkx8bG9DMLxAx4Y9qFe4AtQzZ7zLBR0NO337q%2Fc8RNnG29WYaRfUFkT%2F2y97zCEwvurmiOK54PorCbjcTCIn%2BPSBjqZAaZnbd2xNviyG4x%2FfErHUi5OmE9iNj1ltfb0T70ChYr4A2hmKboE%2FzCpf6AJ9%2B99tIjsVtFMQbjW5MSA696DRh%2BR29Xq%2FYI2K0H26mL4S8H5F3YoXukGCbUDu2w%2FV0COuEzr7lPmCVUXWE6t2xecUZ0QP8DRGgkwPaKlEZ%2Bb%2BOjL%2BWD492r4kTa1Zitf8V9O1AC33GMFPVIDGg%3D%3D&Expires=1784208731)

```sql
/*==============================================================================
  SIMPLIFIED ADT-TO-CLAIMS RECONCILIATION
  MedHOK (Authorization) + HIE ADT vs Inpatient Claims

  Assumes: claims are already deduplicated (one row per member + admit date)
  Expected claims columns: MemberID, Admit, Discharge, BedType
==============================================================================*/

DROP TABLE IF EXISTS tmp_adt_params;
CREATE TEMP TABLE tmp_adt_params AS
SELECT
    DATE '2025-01-01' AS start_date,
    DATE '2026-01-01' AS end_date,
    1::INTEGER       AS fuzzy_days;

/*==============================================================================
  1. NORMALIZE CLAIMS (no dedup needed — already deduped upstream)
==============================================================================*/
DROP TABLE IF EXISTS tmp_claims_2025;
CREATE TEMP TABLE tmp_claims_2025 AS

SELECT
    ROW_NUMBER() OVER (ORDER BY member_id, claim_admit_date) ::BIGINT AS claim_event_id,
    member_id,
    claim_admit_ts,
    claim_discharge_ts,
    CAST(claim_admit_ts AS DATE) AS claim_admit_date,
    CAST(claim_discharge_ts AS DATE) AS claim_discharge_date,
    bed_type
FROM
(
    SELECT
        TRIM(UPPER(CAST(c.MemberID AS VARCHAR(100)))) AS member_id,
        TRY_CAST(c.Admit AS TIMESTAMP)     AS claim_admit_ts,
        TRY_CAST(c.Discharge AS TIMESTAMP) AS claim_discharge_ts,
        TRIM(CAST(c.BedType AS VARCHAR(100))) AS bed_type
    FROM analytics.inpatient_claims c
    CROSS JOIN tmp_adt_params p
    WHERE TRY_CAST(c.Admit AS DATE) >= p.start_date
      AND TRY_CAST(c.Admit AS DATE) <  p.end_date
) normalized
WHERE member_id IS NOT NULL
  AND claim_admit_ts IS NOT NULL;

/*==============================================================================
  2. NORMALIZE + CLASSIFY ADT (MedHOK = Authorization, else = HIE_ADT)
==============================================================================*/
DROP TABLE IF EXISTS tmp_adt_base;
CREATE TEMP TABLE tmp_adt_base AS

SELECT
    TRIM(UPPER(CAST(a.memberno AS VARCHAR(100)))) AS member_id,
    TRY_CAST(a.admit_date AS TIMESTAMP) AS adt_admit_ts,
    CAST(TRY_CAST(a.admit_date AS TIMESTAMP) AS DATE) AS adt_admit_date,
    TRY_CAST(a.discharge_date AS TIMESTAMP) AS adt_discharge_ts,
    CASE
        WHEN UPPER(TRIM(COALESCE(CAST(a.sending_source AS VARCHAR(250)), ''))) LIKE '%MEDHOK%'
            THEN 'AUTHORIZATION'
        WHEN a.sending_source IS NOT NULL
            THEN 'HIE_ADT'
        ELSE 'UNCLASSIFIED'
    END AS source_category
FROM clinical.adt_events a
CROSS JOIN tmp_adt_params p
WHERE TRY_CAST(a.admit_date AS DATE) >= DATEADD(day, 0 - p.fuzzy_days, p.start_date)
  AND TRY_CAST(a.admit_date AS DATE) <  DATEADD(day, p.fuzzy_days, p.end_date)
  AND a.memberno IS NOT NULL
  AND a.admit_date IS NOT NULL;

/*==============================================================================
  3. COLLAPSE ADT TO ONE ROW PER MEMBER + ADMIT DATE + SOURCE CATEGORY
==============================================================================*/
DROP TABLE IF EXISTS tmp_adt_events;
CREATE TEMP TABLE tmp_adt_events AS

SELECT
    ROW_NUMBER() OVER (ORDER BY member_id, adt_admit_date, source_category) ::BIGINT AS adt_event_id,
    member_id,
    adt_admit_date,
    source_category,
    MIN(adt_admit_ts) AS adt_admit_ts,
    MAX(adt_discharge_ts) AS adt_discharge_ts
FROM tmp_adt_base
WHERE source_category IN ('AUTHORIZATION', 'HIE_ADT')
GROUP BY member_id, adt_admit_date, source_category;

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
FROM
(
    SELECT
        c.claim_event_id,
        h.adt_event_id,
        DATEDIFF(day, c.claim_admit_date, h.adt_admit_date) AS admit_date_difference,
        'Fuzzy Admit Date +/-1 Day' AS match_method,
        ROW_NUMBER() OVER (
            PARTITION BY c.claim_event_id
            ORDER BY ABS(DATEDIFF(day, c.claim_admit_date, h.adt_admit_date))
        ) AS claim_rn,
        ROW_NUMBER() OVER (
            PARTITION BY h.adt_event_id
            ORDER BY ABS(DATEDIFF(day, c.claim_admit_date, h.adt_admit_date))
        ) AS adt_rn
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
SELECT claim_event_id, adt_event_id, match_method, admit_date_difference
FROM
(
    SELECT
        c.claim_event_id,
        a.adt_event_id,
        DATEDIFF(day, c.claim_admit_date, a.adt_admit_date) AS admit_date_difference,
        CASE WHEN c.claim_admit_date = a.adt_admit_date
             THEN 'Exact Authorization Date'
             ELSE 'Fuzzy Authorization Date +/-1 Day' END AS match_method,
        ROW_NUMBER() OVER (
            PARTITION BY c.claim_event_id
            ORDER BY ABS(DATEDIFF(day, c.claim_admit_date, a.adt_admit_date))
        ) AS claim_rn
    FROM tmp_claims_2025 c
    INNER JOIN tmp_authorization_events a ON c.member_id = a.member_id
    CROSS JOIN tmp_adt_params p
    WHERE ABS(DATEDIFF(day, c.claim_admit_date, a.adt_admit_date)) <= p.fuzzy_days
) x
WHERE claim_rn = 1;

/*==============================================================================
  6. FINAL RECONCILED OUTPUT (with BedType)
==============================================================================*/
DROP TABLE IF EXISTS analytics.adt_claims_reconciliation_2025;
CREATE TABLE analytics.adt_claims_reconciliation_2025 AS

SELECT
    c.claim_event_id,
    c.member_id,
    c.claim_admit_date,
    c.claim_discharge_date,
    c.bed_type,

    hm.match_method       AS hie_match_method,
    hm.admit_date_difference AS hie_admit_date_difference,
    h.adt_admit_date       AS hie_adt_admit_date,
    h.adt_discharge_ts     AS hie_adt_discharge_ts,

    am.match_method        AS authorization_match_method,
    a.adt_admit_date        AS authorization_admit_date,

    CASE WHEN h.adt_event_id IS NOT NULL AND a.adt_event_id IS NOT NULL THEN 'Authorization and HIE ADT'
         WHEN a.adt_event_id IS NOT NULL AND h.adt_event_id IS NULL THEN 'Authorization Only'
         WHEN h.adt_event_id IS NOT NULL AND a.adt_event_id IS NULL THEN 'HIE ADT Only'
         ELSE 'No Authorization or HIE ADT'
    END AS coverage_category

FROM tmp_claims_2025 c
LEFT JOIN tmp_hie_match_map hm ON c.claim_event_id = hm.claim_event_id
LEFT JOIN tmp_hie_events h ON hm.adt_event_id = h.adt_event_id
LEFT JOIN tmp_authorization_match am ON c.claim_event_id = am.claim_event_id
LEFT JOIN tmp_authorization_events a ON am.adt_event_id = a.adt_event_id;
```

## What changed vs. the original

- Removed claims deduplication logic entirely since your claims are already deduped. [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/54907034/a42fb239-6824-4a64-980e-162dc1b3667c/paste.txt?AWSAccessKeyId=ASIA2F3EMEYE7GCRHNCQ&Signature=8neJEXhzWI8li70Q2gS%2B%2B8%2BSXn0%3D&x-amz-security-token=IQoJb3JpZ2luX2VjEH0aCXVzLWVhc3QtMSJGMEQCIE7ocZbiyCRRXztQsJ175PUjfsLy54%2FXpbmpkjktKFkSAiB1bG5xDYBtdOhiaD6qyA17xUjLjbXB%2FMJhc9rhNsF8QyrzBAhGEAEaDDY5OTc1MzMwOTcwNSIM4XPwDJC0nkk7CFXPKtAExR%2Fe8r7ifDMc7jX0fvY48HdNS%2BitQxMlJyko2Z2fSSd7U4Ko6TtkEsg8ZaoS%2BpNSegUc1%2BoscVdl8SNTzRpmj%2Fxbb2UH0pXxnB4EYPfYFKFTvsz0rlGC%2F4GvgFM4EFP0dPztGGste17Kyj5ESXzA4xgiXNwkZQG4G8baNtY%2FXuDGe%2FaqeIqaOV3DbNkwbj3KxasCvdLjQ8I%2BUzjNX0%2FlA2zuh6FJY%2FZHVQ2qJZJwAzDC%2FbqMJxtEVwxhVZT%2F7rifjydK7tE2lNuVdUWgFQ5rDWBTFaP%2BXyaHI1LQNiQDnn0niJ3KskPeOgc6OMAUZSAEvkkJbMUDeHNRcDbYdu6ibhwf4MJ%2FTizxK7GV0Zs07PMT0iOxqljpnUGg0TckzN%2FuByKRy%2Fr2WOpWhMCbAyZuFSVY8vzndq7YHf%2BYFpj5XoXDIKWLOBDRrn875wVBlDB3g8vHEyo7jsH%2BOO2jw3tfQnIZ9%2FEOhO2Eicgm7oRV32eanpFuyTcGl%2BCPGxBdJsnD7n6yZUFEqTXg20AyiPBH4c70cQFZ8HRD85CDh4635wx6fse5bi13uYxVclpqPVHEWot46S0OtLXcPDgYzA%2B%2BfeeLx3dJAMSIsFqH6Y0WPECjpl1RROvNJV5HKpExLpkGK4GuWs2dRynGPYDxcOTP2RDczayjbzmCgR04xS3UekroWpnXFZR968kncHZ54OJpxYurSrkx8bG9DMLxAx4Y9qFe4AtQzZ7zLBR0NO337q%2Fc8RNnG29WYaRfUFkT%2F2y97zCEwvurmiOK54PorCbjcTCIn%2BPSBjqZAaZnbd2xNviyG4x%2FfErHUi5OmE9iNj1ltfb0T70ChYr4A2hmKboE%2FzCpf6AJ9%2B99tIjsVtFMQbjW5MSA696DRh%2BR29Xq%2FYI2K0H26mL4S8H5F3YoXukGCbUDu2w%2FV0COuEzr7lPmCVUXWE6t2xecUZ0QP8DRGgkwPaKlEZ%2Bb%2BOjL%2BWD492r4kTa1Zitf8V9O1AC33GMFPVIDGg%3D%3D&Expires=1784208731)
- Added `bed_type` straight through from the claims table into the final output.
- Collapsed the message-level ranking (steps 3–7 in the original) into a single `GROUP BY member_id, adt_admit_date, source_category` — you lose the granular A01/A02/A03/A08 sequencing, but gain a much shorter, easier-to-audit query.
- Dropped discharge-lag QA, monthly summaries, and patient event sequencing — those can be layered on separately if you need them later.

The output table gives you `coverage_category` per claim, plus `bed_type`, so you can slice missing-ADT rates by bed type (e.g., is HIE ADT less likely to hit for certain bed types) in one pass.

If you want the discharge-date match quality (matched/±1 day/mismatch) added back in as a lightweight column rather than a whole separate QA layer, let me know and I can bolt that onto this same query.
