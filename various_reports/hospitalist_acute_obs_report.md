Below is a cleaner **Teradata volatile-table version** of your query. I split it into logical stages so it is easier to debug, optimize, and extend.

I also removed the **readmission fields and join**, since you said the readmission logic does not work.

---

# Recommended Breakdown

## Why split it this way?

Your current query has several competing jobs:

1. Date parameter setup
2. VOP group reference mapping
3. Hospitalist provider attribution
4. Active member flag
5. Observation claim matching
6. Main inpatient/observation detail pull
7. Facility and VOP enrichment
8. Final deduplication

That is a lot for one CTE chain. Teradata will sometimes handle CTEs well, but once logic gets this layered, volatile tables give you better control, easier testing, and often better performance.

---

# 1. Dates Volatile Table

```sql
CREATE VOLATILE TABLE vt_dates AS
(
    SELECT
        TRUNC(ADD_MONTHS(CURRENT_DATE, -12), 'Y') AS StartDate,
        CURRENT_DATE AS EndDate
) WITH DATA
ON COMMIT PRESERVE ROWS;
```

Optional but useful:

```sql
COLLECT STATISTICS COLUMN (StartDate) ON vt_dates;
COLLECT STATISTICS COLUMN (EndDate) ON vt_dates;
```

---

# 2. VOP Group Mapping

Your `UNION ALL SELECT ... FROM (SELECT 1 AS DUMMY)` pattern works, but it is noisy. In Teradata, this is cleaner:

```sql
CREATE VOLATILE TABLE vt_vop_groups
(
    TIN VARCHAR(20),
    GroupName VARCHAR(100)
)
ON COMMIT PRESERVE ROWS;

INSERT INTO vt_vop_groups VALUES ('301102684', 'BENCHMARK IP');
INSERT INTO vt_vop_groups VALUES ('742810500', 'IM');
INSERT INTO vt_vop_groups VALUES ('814005993', 'WHG');
INSERT INTO vt_vop_groups VALUES ('273479673', 'FEMI');
INSERT INTO vt_vop_groups VALUES ('271724682', 'Catalyst');
INSERT INTO vt_vop_groups VALUES ('800725733', 'HNI');

COLLECT STATISTICS COLUMN (TIN) ON vt_vop_groups;
```

This is easier to maintain than `UNION ALL`. Future you will not hate present you. A rare analytics win.

---

# 3. Hospitalist Attribution Table

This is one of the most important pieces. Create it once and collect stats.

```sql
CREATE VOLATILE TABLE vt_hospitalist AS
(
    SELECT DISTINCT
        CLM.MemberID,
        CLM.Level1Bucket,
        CLM.Level2Bucket,
        CLM.Level3Bucket,
        CLM.DOSBegin,
        CLM.ProviderID,
        CLM.ProviderName,
        CLM.ProviderNPI,
        CLM.ProviderTIN,
        CLM.ProviderSpecialtyCode,
        CLM.ProviderSpecialtyDesc,
        CLM.ProviderTaxonomyCode,
        CLM.ProviderTaxonomyCodeDesc,

        CASE
            WHEN CLM.Level3Bucket = 'Hospitalist' THEN 1
            WHEN CLM.Level3Bucket IN ('Critical Care (Intensivists)', 'Intensivist') THEN 2
            WHEN CLM.Level3Bucket IN 
                ('Internal Medicine', 'Family Medicine', 'General Practice', 'Geriatric Medicine') THEN 3
            WHEN CLM.Level3Bucket IN 
                ('Nurse Practitioner', 'Physician Assistant') THEN 4
            WHEN CLM.Level3Bucket IN 
                ('Nephrology', 'Cardiology', 'Pulmonary Disease', 'Infectious Disease') THEN 5
            ELSE 6
        END AS SpecialtyPriority

    FROM OSS_PROVISIONING_V.sdogBSAClaim CLM
    CROSS JOIN vt_dates D

    WHERE CLM.ProductType = 'Medicare'
      AND CLM.SourceDataKey IN ('50', '210')
      AND CLM.DOSBegin >= D.StartDate

      /* Intentionally keeping iteration/final-paid filters removed 
         to capture more possible hospitalists */
) WITH DATA
ON COMMIT PRESERVE ROWS;
```

Recommended stats:

```sql
COLLECT STATISTICS COLUMN (MemberID) ON vt_hospitalist;
COLLECT STATISTICS COLUMN (MemberID, DOSBegin) ON vt_hospitalist;
COLLECT STATISTICS COLUMN (ProviderNPI) ON vt_hospitalist;
COLLECT STATISTICS COLUMN (ProviderTIN) ON vt_hospitalist;
```

---

# 4. Active Member Flag

```sql
CREATE VOLATILE TABLE vt_active AS
(
    SELECT DISTINCT
        MemberID
    FROM BISDM_CA_VIEW_PRD.MSO_Current_Enrollment_Detail_CA
) WITH DATA
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (MemberID) ON vt_active;
```

---

# 5. Observation Claims

I would split observation into two separate tables:

## 5A. Observation by Authorization Key

```sql
CREATE VOLATILE TABLE vt_observ_key AS
(
    SELECT DISTINCT
        AuthorizationID AS AuthznKey,
        MemberID,
        'Observation' AS Source_Flag
    FROM OSS_PROVISIONING_V.sdogBSAClaim
    CROSS JOIN vt_dates D
    WHERE DOSBegin >= D.StartDate
      AND RevenueCode IN ('0760', '0762', '0769')
      AND LatestFinalizedClaimIndicator = 'Y'
) WITH DATA
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (AuthznKey) ON vt_observ_key;
COLLECT STATISTICS COLUMN (MemberID) ON vt_observ_key;
```

## 5B. Observation by Member + Date

```sql
CREATE VOLATILE TABLE vt_observ_date AS
(
    SELECT DISTINCT
        MemberID,
        DOSBegin AS Obs_DOS,
        'Observation' AS Source_Flag
    FROM OSS_PROVISIONING_V.sdogBSAClaim
    CROSS JOIN vt_dates D
    WHERE DOSBegin >= D.StartDate
      AND RevenueCode IN ('0760', '0762', '0769')
      AND LatestFinalizedClaimIndicator = 'Y'
) WITH DATA
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (MemberID, Obs_DOS) ON vt_observ_date;
```

This avoids one big observation CTE being joined two different ways.

---

# 6. Optional Facility Mapping Table

If `Facility_Mapping_TX` is small, you can join directly. If it is large or dirty, materialize a cleaned distinct version.

```sql
CREATE VOLATILE TABLE vt_facility_map AS
(
    SELECT DISTINCT
        OriginalFacilityName,
        CleanFacilityName,
        GeneralFacilityName
    FROM BISDM_CA_BASE_PRD.Facility_Mapping_TX
) WITH DATA
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (OriginalFacilityName) ON vt_facility_map;
```

---

# 7. Optional Catalyst/Federal ID Table

Your current query does this inline:

```sql
LEFT JOIN (
    SELECT F.*, 'CATALYST MEDICAL GROUP' AS GroupName
    FROM BISDM_CA_BASE_PRD.AllProviders_FederalIDs F
    WHERE EntityName LIKE '%Catalyst%'
) FEDID
```

Better as a volatile table:

```sql
CREATE VOLATILE TABLE vt_catalyst_fedid AS
(
    SELECT DISTINCT
        ProviderNPI,
        FederalTaxID,
        'CATALYST MEDICAL GROUP' AS GroupName
    FROM BISDM_CA_BASE_PRD.AllProviders_FederalIDs
    WHERE EntityName LIKE '%Catalyst%'
) WITH DATA
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (ProviderNPI) ON vt_catalyst_fedid;
```

---

# 8. Optional VOP Hospitalist Group Table

```sql
CREATE VOLATILE TABLE vt_vop_hospitalist_group AS
(
    SELECT DISTINCT
        VendorTIN,
        "Group",
        Hospital,
        "VendorNPI"
    FROM BISDM_CA_BASE_PRD.VOP_Hospitalist_Group
) WITH DATA
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (VendorTIN) ON vt_vop_hospitalist_group;
```

---

# 9. Final Base Table Without Readmission Logic

This is your main output logic, minus the readmission fields.

```sql
CREATE VOLATILE TABLE vt_final_base AS
(
    SELECT
        IP.BedType,
        IP.OperationalMarket,
        IP.OperationalSubMarket,
        IP.ReportingPod,
        IP.ManagingEntity,
        IP.ManagingProviderName,
        IP.PodCd,
        IP.PodName,
        IP.PCPName,
        IP.PCPNPI,
        IP.AuthznKey,
        IP.MemberID,
        YEAR(IP.Admit) AS AdmitYear,
        IP.Admit,
        IP.Discharge,
        IP.Discharge - IP.Admit AS LengthOfStay,

        CASE
            WHEN OB_Key.AuthznKey IS NOT NULL THEN 'Key Match'
            WHEN OB_Date.MemberID IS NOT NULL THEN 'Fuzzy Date Match'
            ELSE 'No Obs Found'
        END AS Obs_Match_Method,

        COALESCE(OB_Key.Source_Flag, OB_Date.Source_Flag) AS Obs_Claim_Indicator,
        COALESCE(OB_Key.AuthznKey, CAST(OB_Date.Obs_DOS AS VARCHAR(30))) AS Obs_Date,

        CASE 
            WHEN AC.MemberID IS NOT NULL THEN 1 
            ELSE 0 
        END AS Active,

        IP.AdmittingProviderName AS AdmittingFacilityName,
        IP.AdmittingProviderNPI AS AdmittingFacilityNPI,
        FA.CleanFacilityName AS AdmittingFacilityNameClean,
        FA.GeneralFacilityName AS AdmittingFacilityNameGroup,

        IP.AdmissionReason,
        IP.Expired,
        IP.MemberDOB,
        IP.HCODE,
        IP.PlanType,
        IP.AdmitDischargeError,
        IP.PrimaryDiagnosisCode,
        IP.PrimaryDiagnosis,
        IP.LACE,
        IP.AdmissionType,
        IP.AdmitFrom,
        IP.DischargeStatusInd,
        IP.DischargeStatusCode,
        IP.DischargeStatusDescription,

        CASE
            WHEN IP.DischargeStatusCode IN ('03', '61', '62', '63') THEN 'SNF/Rehab'
            WHEN IP.DischargeStatusCode IN ('06', '50', '51') THEN 'Home Health'
            WHEN IP.DischargeStatusCode = '01' THEN 'Home'
            ELSE 'Other'
        END AS Discharge_Disposition_Group,

        HP.Level1Bucket AS AttendingProviderLevel1Bucket,
        HP.Level2Bucket AS AttendingProviderLevel2Bucket,
        HP.Level3Bucket AS AttendingProviderLevel3Bucket,
        HP.DOSBegin AS AttendingProviderDOSBegin,
        HP.ProviderName AS AttendingProviderName,
        HP.ProviderNPI AS AttendingProviderNPI,
        HP.ProviderTIN AS AttendingProviderTIN,
        HP.ProviderSpecialtyCode AS AttendingProviderSpecialtyCode,
        HP.ProviderSpecialtyDesc AS AttendingProviderSpecialtyDesc,

        CASE 
            WHEN V.TIN IS NOT NULL THEN 1
            WHEN VOP."VendorNPI" IS NOT NULL THEN 1
            WHEN FEDID.ProviderNPI IS NOT NULL THEN 1
            ELSE 0
        END AS AttendingProvider_VOP,

        VOP.Hospital AS AttendingProviderHospital_VOP,

        UPPER(
            COALESCE(
                V.GroupName,
                VOP."Group",
                FEDID.GroupName
            )
        ) AS AttendingProviderHospitalGroupName_VOP,

        VOP."VendorNPI" AS VOPAttendingProviderHospitalGroupNPI_VOP,

        HP.SpecialtyPriority

    FROM BISDM_CA_BASE_PRD.MSO_Core_Utilization_Detail IP
    CROSS JOIN vt_dates D

    LEFT JOIN vt_observ_key OB_Key
        ON OREPLACE(IP.AuthznKey, '5|', '') = OB_Key.AuthznKey
       AND IP.MemberID = OB_Key.MemberID

    LEFT JOIN vt_observ_date OB_Date
        ON IP.MemberID = OB_Date.MemberID
       AND OB_Date.Obs_DOS BETWEEN (IP.Admit - 1) AND (IP.Admit + 1)
       AND OB_Key.AuthznKey IS NULL

    LEFT JOIN vt_hospitalist HP
        ON IP.MemberID = HP.MemberID
       AND HP.DOSBegin BETWEEN IP.Admit AND IP.Discharge

    LEFT JOIN vt_active AC
        ON IP.MemberID = AC.MemberID

    LEFT JOIN vt_vop_hospitalist_group VOP
        ON HP.ProviderNPI = VOP."VendorNPI"

    LEFT JOIN vt_vop_groups V
        ON HP.ProviderTIN = V.TIN

    LEFT JOIN vt_catalyst_fedid FEDID
        ON HP.ProviderNPI = FEDID.ProviderNPI

    LEFT JOIN vt_facility_map FA
        ON FA.OriginalFacilityName = IP.AdmittingProviderName

    WHERE IP.Admit BETWEEN D.StartDate AND D.EndDate
      AND IP.BedType IN ('Acute', 'OBS')
      AND IP.DataSource = 'Authorizations'
      AND IP.CareAlliesManagedFlag = 1
      AND IP.OperationalMarket = 'TX'
      AND IP.ManagingEntity = 'VALLEY ORGANIZED PHYSICIANS LLC (VOP)'

) WITH DATA
ON COMMIT PRESERVE ROWS;
```

Recommended stats:

```sql
COLLECT STATISTICS COLUMN (AuthznKey) ON vt_final_base;
COLLECT STATISTICS COLUMN (MemberID) ON vt_final_base;
COLLECT STATISTICS COLUMN (MemberID, Admit) ON vt_final_base;
COLLECT STATISTICS COLUMN (AttendingProviderNPI) ON vt_final_base;
COLLECT STATISTICS COLUMN (SpecialtyPriority) ON vt_final_base;
```

---

# 10. Final Deduplicated Output

Instead of using `QUALIFY` inside the monster query, do it at the final stage:

```sql
CREATE VOLATILE TABLE vt_final AS
(
    SELECT *
    FROM vt_final_base
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY AuthznKey
        ORDER BY SpecialtyPriority ASC, AttendingProviderDOSBegin ASC
    ) = 1
) WITH DATA
ON COMMIT PRESERVE ROWS;
```

Then query:

```sql
SELECT *
FROM vt_final;
```

---

# Efficiency Improvements I Recommend

## 1. Avoid function calls in join conditions when possible

This line is expensive:

```sql
OREPLACE(IP.AuthznKey, '5|', '') = OB_Key.AuthznKey
```

Better: create a normalized authorization key in the base table.

For example, in `vt_final_base`, before joining, use a staged IP table:

```sql
CREATE VOLATILE TABLE vt_ip_base AS
(
    SELECT
        IP.*,
        OREPLACE(IP.AuthznKey, '5|', '') AS AuthznKey_Clean
    FROM BISDM_CA_BASE_PRD.MSO_Core_Utilization_Detail IP
    CROSS JOIN vt_dates D
    WHERE IP.Admit BETWEEN D.StartDate AND D.EndDate
      AND IP.BedType IN ('Acute', 'OBS')
      AND IP.DataSource = 'Authorizations'
      AND IP.CareAlliesManagedFlag = 1
      AND IP.OperationalMarket = 'TX'
      AND IP.ManagingEntity = 'VALLEY ORGANIZED PHYSICIANS LLC (VOP)'
) WITH DATA
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (AuthznKey_Clean) ON vt_ip_base;
COLLECT STATISTICS COLUMN (MemberID, Admit) ON vt_ip_base;
```

Then use:

```sql
ON IP.AuthznKey_Clean = OB_Key.AuthznKey
```

That is cleaner and usually faster.

---

## 2. Create `vt_ip_base` first

I would strongly recommend this instead of pulling directly from `MSO_Core_Utilization_Detail` in the final join.

Why?

Because this table is likely large. You want to filter it early:

```sql
CREATE VOLATILE TABLE vt_ip_base AS
(
    SELECT
        IP.*,
        OREPLACE(IP.AuthznKey, '5|', '') AS AuthznKey_Clean
    FROM BISDM_CA_BASE_PRD.MSO_Core_Utilization_Detail IP
    CROSS JOIN vt_dates D
    WHERE IP.Admit BETWEEN D.StartDate AND D.EndDate
      AND IP.BedType IN ('Acute', 'OBS')
      AND IP.DataSource = 'Authorizations'
      AND IP.CareAlliesManagedFlag = 1
      AND IP.OperationalMarket = 'TX'
      AND IP.ManagingEntity = 'VALLEY ORGANIZED PHYSICIANS LLC (VOP)'
) WITH DATA
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (AuthznKey) ON vt_ip_base;
COLLECT STATISTICS COLUMN (AuthznKey_Clean) ON vt_ip_base;
COLLECT STATISTICS COLUMN (MemberID) ON vt_ip_base;
COLLECT STATISTICS COLUMN (MemberID, Admit) ON vt_ip_base;
```

Then in the final table, replace:

```sql
FROM BISDM_CA_BASE_PRD.MSO_Core_Utilization_Detail IP
CROSS JOIN vt_dates D
```

with:

```sql
FROM vt_ip_base IP
```

And remove the duplicated `WHERE` filters from the final query.

---

## 3. Consider splitting hospitalist matching into its own ranked table

Right now, this join can create a lot of duplication:

```sql
LEFT JOIN vt_hospitalist HP
    ON IP.MemberID = HP.MemberID
   AND HP.DOSBegin BETWEEN IP.Admit AND IP.Discharge
```

A member may have multiple professional claims during a stay. You then dedupe later. That works, but it may blow up row counts before deduplication.

Better approach:

```sql
CREATE VOLATILE TABLE vt_stay_hospitalist_ranked AS
(
    SELECT
        IP.AuthznKey,
        IP.MemberID,
        HP.Level1Bucket,
        HP.Level2Bucket,
        HP.Level3Bucket,
        HP.DOSBegin,
        HP.ProviderName,
        HP.ProviderNPI,
        HP.ProviderTIN,
        HP.ProviderSpecialtyCode,
        HP.ProviderSpecialtyDesc,
        HP.SpecialtyPriority,

        ROW_NUMBER() OVER (
            PARTITION BY IP.AuthznKey
            ORDER BY HP.SpecialtyPriority ASC, HP.DOSBegin ASC
        ) AS rn

    FROM vt_ip_base IP
    LEFT JOIN vt_hospitalist HP
        ON IP.MemberID = HP.MemberID
       AND HP.DOSBegin BETWEEN IP.Admit AND IP.Discharge
) WITH DATA
ON COMMIT PRESERVE ROWS;
```

Then keep only the top hospitalist:

```sql
CREATE VOLATILE TABLE vt_stay_hospitalist AS
(
    SELECT *
    FROM vt_stay_hospitalist_ranked
    WHERE rn = 1
) WITH DATA
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (AuthznKey) ON vt_stay_hospitalist;
COLLECT STATISTICS COLUMN (ProviderNPI) ON vt_stay_hospitalist;
COLLECT STATISTICS COLUMN (ProviderTIN) ON vt_stay_hospitalist;
```

Then your final query joins one hospitalist row per stay, not many.

This is probably the biggest performance win.

---

# Cleaner Final Architecture

I would structure the script like this:

```sql
-- 1. Date params
vt_dates

-- 2. Main filtered authorization/IP base
vt_ip_base

-- 3. Active members
vt_active

-- 4. Observation match tables
vt_observ_key
vt_observ_date

-- 5. Hospitalist professional claims
vt_hospitalist

-- 6. Ranked hospitalist attribution per stay
vt_stay_hospitalist_ranked
vt_stay_hospitalist

-- 7. Reference mappings
vt_vop_groups
vt_vop_hospitalist_group
vt_catalyst_fedid
vt_facility_map

-- 8. Final output
vt_final
```

---

# Main Logic Changes I Would Make

## Remove readmission logic

Delete this CTE/table:

```sql
READMITS AS (...)
```

Delete this join:

```sql
LEFT JOIN READMITS RM
    ON IP.MemberID = RM.MemberID
   AND IP.Admit BETWEEN RM.ReportBeginDate AND RM.ReportEndDate
```

Delete these output fields:

```sql
RM.Auth_Readmits AS Readmit,
RM.Auth_ReadmitsDenominator AS ReadmitDenominator
```

---

## Add `vt_ip_base`

This makes your script much cleaner and faster.

```sql
CREATE VOLATILE TABLE vt_ip_base AS
(
    SELECT
        IP.*,
        OREPLACE(IP.AuthznKey, '5|', '') AS AuthznKey_Clean
    FROM BISDM_CA_BASE_PRD.MSO_Core_Utilization_Detail IP
    CROSS JOIN vt_dates D
    WHERE IP.Admit BETWEEN D.StartDate AND D.EndDate
      AND IP.BedType IN ('Acute', 'OBS')
      AND IP.DataSource = 'Authorizations'
      AND IP.CareAlliesManagedFlag = 1
      AND IP.OperationalMarket = 'TX'
      AND IP.ManagingEntity = 'VALLEY ORGANIZED PHYSICIANS LLC (VOP)'
) WITH DATA
ON COMMIT PRESERVE ROWS;
```

Then everything downstream is working on a much smaller inpatient/observation population.

---

# Bottom Line

Your query is a good candidate for volatile tables because the logic is now more like a mini-pipeline than a single query.

The best improvements are:

1. **Create `vt_ip_base` first** to reduce the main population early.
2. **Materialize hospitalist attribution separately** and rank it before the final output.
3. **Remove readmission fields and joins completely.**
4. **Collect stats on join keys**, especially `MemberID`, `AuthznKey`, `ProviderNPI`, `ProviderTIN`, and date fields.
5. **Avoid functions directly in joins** by creating `AuthznKey_Clean` once.
6. **Keep observation key match and fuzzy date match separate** for easier QA.

The main thing I would not do is keep expanding the CTE monster. That thing is becoming a SQL lasagna, and not the good kind.


---
Yes. **Add a volume-based reliability indicator**, but I would not call it only a “95th percentile indicator” unless your goal is specifically to flag unusually high-volume hospitalists.

What you really need is a **minimum-volume / statistical reliability flag** so low-volume providers do not distort averages.

---

## Recommended approach

Use **provider stay volume** as the denominator and create tiers like this:

| Provider Volume Tier |                       Logic | Interpretation                                     |
| -------------------- | --------------------------: | -------------------------------------------------- |
| **Low Volume**       |              Below 25 stays | Do not over-interpret averages                     |
| **Moderate Volume**  |              25 to 49 stays | Directional only                                   |
| **Reliable Volume**  |                   50+ stays | More stable comparison                             |
| **Top Volume**       | At or above 95th percentile | High-volume provider, useful for operational focus |

The 95th percentile is useful, but it answers a different question:

> “Who are the highest-volume hospitalists?”

It does **not** directly solve:

> “Which provider averages are unreliable because volume is too low?”

For that, you need a **low-volume flag**.

---

# Best indicator set to add

I would add these fields:

```sql
AttendingProviderStayCount
AttendingProviderVolumePercentile
AttendingProviderVolumeTier
LowVolumeProviderFlag
HighVolumeProviderFlag
```

Example interpretation:

```text
Provider A: 4 stays, 80% observation rate
```

That may look extreme, but it is based on tiny volume. Tiny denominators are how dashboards commit small crimes.

---

# Teradata SQL Pattern

Assuming your final deduped table is `vt_final`, you can create a provider volume table:

```sql
CREATE VOLATILE TABLE vt_provider_volume AS
(
    SELECT
        AttendingProviderNPI,
        AttendingProviderName,
        COUNT(DISTINCT AuthznKey) AS AttendingProviderStayCount
    FROM vt_final
    WHERE AttendingProviderNPI IS NOT NULL
    GROUP BY
        AttendingProviderNPI,
        AttendingProviderName
) WITH DATA
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (AttendingProviderNPI) ON vt_provider_volume;
COLLECT STATISTICS COLUMN (AttendingProviderStayCount) ON vt_provider_volume;
```

---

## Add percentile ranking

You can use `PERCENT_RANK()` or `CUME_DIST()`.

For your use case, I prefer **CUME_DIST** because it gives a clean “at or above 95th percentile” interpretation.

```sql
CREATE VOLATILE TABLE vt_provider_volume_ranked AS
(
    SELECT
        AttendingProviderNPI,
        AttendingProviderName,
        AttendingProviderStayCount,

        CUME_DIST() OVER (
            ORDER BY AttendingProviderStayCount
        ) AS AttendingProviderVolumePercentile,

        CASE
            WHEN AttendingProviderStayCount < 25 THEN 'Low Volume'
            WHEN AttendingProviderStayCount BETWEEN 25 AND 49 THEN 'Moderate Volume'
            WHEN AttendingProviderStayCount >= 50 THEN 'Reliable Volume'
            ELSE 'Unknown'
        END AS AttendingProviderVolumeTier,

        CASE
            WHEN AttendingProviderStayCount < 25 THEN 1
            ELSE 0
        END AS LowVolumeProviderFlag,

        CASE
            WHEN CUME_DIST() OVER (
                ORDER BY AttendingProviderStayCount
            ) >= 0.95 THEN 1
            ELSE 0
        END AS HighVolumeProviderFlag

    FROM vt_provider_volume
) WITH DATA
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (AttendingProviderNPI) ON vt_provider_volume_ranked;
```

---

# Join it back to final output

```sql
CREATE VOLATILE TABLE vt_final_with_volume AS
(
    SELECT
        F.*,
        PV.AttendingProviderStayCount,
        PV.AttendingProviderVolumePercentile,
        PV.AttendingProviderVolumeTier,
        PV.LowVolumeProviderFlag,
        PV.HighVolumeProviderFlag
    FROM vt_final F
    LEFT JOIN vt_provider_volume_ranked PV
        ON F.AttendingProviderNPI = PV.AttendingProviderNPI
) WITH DATA
ON COMMIT PRESERVE ROWS;
```

---

# Better option for reporting

For averages like observation rate, readmission rate, length of stay, or SNF discharge rate, I would show:

## Provider-level display logic

```text
If provider volume < 25:
    show rate, but gray it out / mark as low confidence

If provider volume >= 25:
    include in comparison averages

If provider volume >= 50:
    include in ranked provider comparisons
```

So instead of hiding low-volume providers, label them:

```text
Low Volume: interpret with caution
```

That protects the analysis without throwing away potentially useful signal.

---

# Optional: Create adjusted comparison fields

For example:

```sql
CASE 
    WHEN AttendingProviderStayCount >= 25 THEN LengthOfStay
    ELSE NULL
END AS LengthOfStay_ReliableOnly
```

Or for observation:

```sql
CASE 
    WHEN AttendingProviderStayCount >= 25 THEN Obs_Claim_Indicator
    ELSE NULL
END AS Obs_Claim_Indicator_ReliableOnly
```

This lets your dashboard calculate “reliable provider average” separately from “all provider average.”

---

# My recommendation

Add both:

1. **Low-volume reliability flag**
2. **95th percentile high-volume flag**

Use them differently:

| Flag                       | Purpose                                             |
| -------------------------- | --------------------------------------------------- |
| **LowVolumeProviderFlag**  | Prevent misleading averages                         |
| **HighVolumeProviderFlag** | Identify providers with the most operational impact |
| **VolumePercentile**       | Support filtering, ranking, and context             |
| **VolumeTier**             | Make the report easier to interpret                 |

The strongest reporting message would be:

> Provider averages are volume-adjusted for interpretability. Low-volume providers are retained for visibility but excluded from ranked comparisons when stay count is below the reliability threshold.

That gives you analytical credibility and avoids the “one patient made someone look terrible” problem.

---
Yes. At this point, I would **not force readmission into the encounter-level table as if it belongs to each stay**.

You are now building an analytics backbone for:

* Tableau
* Excel pivots
* Python charts
* hospital-level aggregation
* attending provider aggregation
* hospitalist group aggregation
* month-over-month reporting

That means you need to separate the data into **fact tables at the correct grain**. Otherwise, your averages and denominators will start lying with a straight face.

---

# Recommended Structure

## Use 2 core fact tables

| Table                      | Grain                                                                                    | Purpose                                                                              |
| -------------------------- | ---------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `fact_ip_stay`             | **1 row per authorization / inpatient stay**                                             | LOS, bed type, facility, attending provider, observation flag, discharge disposition |
| `fact_readmission_monthly` | **1 row per month + member / provider / hospital grouping**, depending on available keys | Readmission numerator and denominator                                                |

Do **not** jam monthly aggregate readmission numerator/denominator into every stay row unless you are extremely careful. That can duplicate the numerator/denominator and inflate results when users pivot by provider, hospital, or group.

---

# Why the current readmission join is risky

Your current logic is roughly:

```sql
IP.MemberID = RM.MemberID
AND IP.Admit BETWEEN RM.ReportBeginDate AND RM.ReportEndDate
```

That means if a member had multiple IP stays in the same month, the same monthly readmission numerator and denominator could attach to multiple encounter rows.

Example:

| Member | Month | Readmit Numerator | Readmit Denominator | IP Stays |
| ------ | ----: | ----------------: | ------------------: | -------: |
| A      | March |                 1 |                   1 |        3 |

If you join that monthly readmission row to all 3 stays, your Tableau pivot may show:

```text
Readmits = 3
Denominator = 3
```

But the real monthly readmission value was:

```text
Readmits = 1
Denominator = 1
```

That is the danger.

---

# Best Practice Design

## 1. Build your encounter-level table as the main backbone

Call it something like:

```sql
fact_ip_stay
```

Grain:

```text
One row per AuthznKey
```

This table should include fields like:

```text
AuthznKey
MemberID
Admit
Discharge
AdmitMonth
DischargeMonth
BedType
LengthOfStay
Obs_Claim_Indicator
Obs_Match_Method
AdmittingFacilityName
AdmittingFacilityNameClean
AdmittingFacilityNameGroup
AttendingProviderNPI
AttendingProviderName
AttendingProviderTIN
AttendingProviderSpecialty
AttendingProviderHospitalGroupName_VOP
AttendingProvider_VOP
ProviderVolumeTier
LowVolumeProviderFlag
HighVolumeProviderFlag
Discharge_Disposition_Group
Active
```

This becomes the main table for:

* admission counts
* observation rate
* average LOS
* discharge disposition
* provider attribution
* facility grouping
* volume tiering

---

## 2. Create a separate readmission monthly fact table

Call it:

```sql
fact_readmission_monthly
```

The correct grain depends on what exists in the readmission source table.

From what you described, it sounds like the readmission table gives:

```text
MemberID
ReportBeginDate
ReportEndDate
Auth_Readmits
Auth_ReadmitsDenominator
```

So the safe grain is probably:

```text
One row per MemberID + ReportBeginDate + ReportEndDate
```

Example:

```sql
CREATE VOLATILE TABLE vt_readmission_monthly AS
(
    SELECT
        MemberID,
        ReportBeginDate,
        ReportEndDate,
        CAST(ReportBeginDate AS DATE) AS ReportMonth,

        SUM(Auth_Readmits) AS ReadmitNumerator,
        SUM(Auth_ReadmitsDenominator) AS ReadmitDenominator

    FROM BISDM_CA_BASE_PRD.MSO_Core_Utilization_Summary
    CROSS JOIN vt_dates D

    WHERE ReportBeginDate >= D.StartDate
      AND OperationalMarket = 'TX'
      AND CareAlliesManagedFlag = 1
      AND Auth_Readmits IS NOT NULL

    GROUP BY
        MemberID,
        ReportBeginDate,
        ReportEndDate,
        CAST(ReportBeginDate AS DATE)
) WITH DATA
ON COMMIT PRESERVE ROWS;
```

---

# The key question

To report readmissions by **hospital**, **attending provider**, and **group**, you need an attribution bridge.

Because your readmission table is monthly and member-level, it does not naturally know which hospitalist, hospital, or hospital group caused the readmission denominator.

So you need to decide:

> Which stay should receive the readmission attribution?

Usually, the denominator belongs to the **index admission**, not every admission in the month.

---

# Best Approach: Create a Readmission Attribution Bridge

Create a third table:

```sql
bridge_readmission_attribution
```

Grain:

```text
One row per MemberID + ReportMonth + attributed AuthznKey
```

Purpose:

Take the monthly readmission metric and attach it to the most reasonable encounter from your IP stay table.

---

# Attribution Logic Options

## Option A. Attribute to the first eligible discharge in the month

This works if the readmission denominator represents index admissions within that reporting month.

```text
Readmission month = month of index discharge
```

Best for:

* hospital quality metrics
* discharge-to-readmit tracking
* facility/provider accountability

Recommended.

---

## Option B. Attribute to the admission in the same month

This is simpler, but less clinically precise.

```text
Readmission month = month of admission
```

Useful if your summary table was created from authorization admits.

---

## Option C. Keep readmission separate and only aggregate by month/member

This is safest mathematically, but does not answer provider/hospital performance unless the readmission summary already contains those dimensions.

---

# My Recommendation

Use this structure:

```text
fact_ip_stay
        |
        |  AuthznKey
        |
bridge_readmission_attribution
        |
        |  MemberID + ReportMonth
        |
fact_readmission_monthly
```

That gives you clean encounter analytics **and** clean readmission aggregation.

---

Yes, but **do not mix observation into the official readmission numerator unless the business definition says to**.

Best structure:

| Metric                                 |       Denominator |                                   Numerator | Use                             |
| -------------------------------------- | ----------------: | ------------------------------------------: | ------------------------------- |
| **Acute 30-Day Readmission**           | Acute index stays |  Next qualifying acute admit within 30 days | Main readmission metric         |
| **Observation Return Within 30 Days**  | Acute index stays |               Next OBS event within 30 days | Utilization / leakage signal    |
| **Acute or OBS Return Within 30 Days** | Acute index stays |      Next acute or OBS event within 30 days | Broader avoidable-return metric |
| **OBS-to-Acute Escalation**            |   OBS index stays | Acute admit within 0-3 days or same episode | Conversion / escalation metric  |

## My recommendation

Keep these as **separate flags** on the event-level table:

```text
ReadmitDenominator
AcuteReadmitNumerator
ObsReturnNumerator
AcuteOrObsReturnNumerator
ObsToAcuteEscalationFlag
```

Then in Tableau / Excel / Python, you can calculate:

```text
Acute Readmission Rate =
SUM(AcuteReadmitNumerator) / SUM(ReadmitDenominator)

OBS Return Rate =
SUM(ObsReturnNumerator) / SUM(ReadmitDenominator)

Acute or OBS Return Rate =
SUM(AcuteOrObsReturnNumerator) / SUM(ReadmitDenominator)
```

This gives you flexibility without contaminating the main readmission metric. Otherwise, someone will ask why your readmission rate does not match the enterprise/CMS-style number, and then the dashboard starts sweating.

---

# Suggested logic

## 1. Acute index stay denominator

Use only **acute authorization events** as the readmission denominator.

```sql
CASE 
    WHEN BedType = 'Acute' THEN 1 
    ELSE 0 
END AS ReadmitDenominator
```

## 2. Acute readmission numerator

Next qualifying **acute** admit after discharge.

```sql
CASE
    WHEN BedType = 'Acute'
     AND NextAcuteAdmit IS NOT NULL
     AND NextAcuteAdmit - Discharge > 2
     AND NextAcuteAdmit - Discharge <= 30
    THEN 1
    ELSE 0
END AS AcuteReadmitNumerator
```

## 3. Observation return numerator

Next **OBS** event after discharge.

```sql
CASE
    WHEN BedType = 'Acute'
     AND NextObsAdmit IS NOT NULL
     AND NextObsAdmit - Discharge > 2
     AND NextObsAdmit - Discharge <= 30
    THEN 1
    ELSE 0
END AS ObsReturnNumerator
```

## 4. Acute or OBS return numerator

Use the earliest next acute or observation event.

```sql
CASE
    WHEN BedType = 'Acute'
     AND NextAcuteOrObsAdmit IS NOT NULL
     AND NextAcuteOrObsAdmit - Discharge > 2
     AND NextAcuteOrObsAdmit - Discharge <= 30
    THEN 1
    ELSE 0
END AS AcuteOrObsReturnNumerator
```

## 5. OBS-to-Acute escalation

This is separate. It answers whether an observation event turned into or was followed by an acute admission.

```sql
CASE
    WHEN BedType = 'OBS'
     AND NextAcuteAdmit IS NOT NULL
     AND NextAcuteAdmit - Admit BETWEEN 0 AND 3
    THEN 1
    ELSE 0
END AS ObsToAcuteEscalationFlag
```

---

# Final design recommendation

Add observation logic, but label it clearly:

```text
AcuteReadmitNumerator
ObsReturnNumerator
AcuteOrObsReturnNumerator
ObsToAcuteEscalationFlag
```

Do **not** rename the combined acute/OBS metric as “readmission” unless leadership agrees. I would call it:

```text
30-Day Acute or OBS Return Rate
```

That is cleaner, defensible, and much easier to explain.

