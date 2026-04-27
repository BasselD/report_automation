/*
================================================================================
  PURPOSE
  -------
  Build an event-level IP/OBS authorization backbone with attending provider,
  facility, hospitalist group, observation-claim indicator, provider-volume flags,
  and authorization-based 30-day readmission numerator/denominator.

  Intended consumers:
    - Tableau
    - Excel pivots
    - Python charts / notebooks

  Core design:
    - Final event table grain = 1 row per authorization stay / AuthznKey.
    - Readmission numerator and denominator live on the INDEX stay row.
    - Aggregation rule = SUM(ReadmitNumerator) / SUM(ReadmitDenominator).

  Important:
    - This does NOT use ReportBeginDate / ReportEndDate.
    - Readmission period is driven by the next acute authorization admission after
      the index discharge.
    - Default readmission window = 30 days.
    - Default transfer/same-episode buffer = 2 days. A next admit within 0-2 days
      is flagged as TransferBufferFlag and does not count as readmission numerator.

  Review before production:
    - Confirm whether 2-day buffer should be excluded from denominator or only
      excluded from numerator. This script keeps it in the denominator because the
      denominator represents eligible index acute stays.
    - Confirm whether readmission month should use AdmitMonth or DischargeMonth
      in downstream reporting. This script exposes both and uses DischargeMonth
      as ReadmissionIndexMonth in the monthly mart.
================================================================================
*/

/*===============================================================================
  00. PARAMETERS
===============================================================================*/

CREATE MULTISET VOLATILE TABLE vt_params AS
(
    SELECT
          TRUNC(ADD_MONTHS(CURRENT_DATE, -12), 'Y') AS StartDate
        , CURRENT_DATE AS EndDate
        , 30 AS ReadmitWindowDays
        , 2  AS TransferBufferDays
) WITH DATA
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS COLUMN (StartDate) ON vt_params;
COLLECT STATISTICS COLUMN (EndDate)   ON vt_params;


/*===============================================================================
  01. FILTERED IP / OBS AUTHORIZATION EVENT BASE
      Grain: one row per authorization stay candidate.
===============================================================================*/

CREATE MULTISET VOLATILE TABLE vt_ip_base AS
(
    SELECT DISTINCT
          IP.BedType
        , IP.DataSource
        , IP.OperationalMarket
        , IP.OperationalSubMarket
        , IP.ReportingPod
        , IP.ManagingEntity
        , IP.ManagingProviderName
        , IP.PodCd
        , IP.PodName
        , IP.PCPName
        , IP.PCPNPI
        , IP.AuthznKey
        , OREPLACE(IP.AuthznKey, '5|', '') AS AuthznKey_Clean
        , IP.MemberID
        , YEAR(IP.Admit) AS AdmitYear
        , IP.Admit
        , IP.Discharge
        , IP.Admit - EXTRACT(DAY FROM IP.Admit) + 1 AS AdmitMonth
        , CASE
              WHEN IP.Discharge IS NOT NULL
              THEN IP.Discharge - EXTRACT(DAY FROM IP.Discharge) + 1
              ELSE NULL
          END AS DischargeMonth
        , IP.Discharge - IP.Admit AS LengthOfStay
        , IP.AdmittingProviderName AS AdmittingFacilityName
        , IP.AdmittingProviderNPI  AS AdmittingFacilityNPI
        , IP.AdmissionReason
        , IP.Expired
        , IP.MemberDOB
        , IP.HCODE
        , IP.PlanType
        , IP.AdmitDischargeError
        , IP.PrimaryDiagnosisCode
        , IP.PrimaryDiagnosis
        , IP.LACE
        , IP.AdmissionType
        , IP.AdmitFrom
        , IP.DischargeStatusInd
        , IP.DischargeStatusCode
        , IP.DischargeStatusDescription
        , CASE
              WHEN IP.DischargeStatusCode IN ('03', '61', '62', '63') THEN 'SNF/Rehab'
              WHEN IP.DischargeStatusCode IN ('06', '50', '51')       THEN 'Home Health'
              WHEN IP.DischargeStatusCode = '01'                      THEN 'Home'
              ELSE 'Other'
          END AS Discharge_Disposition_Group
    FROM BISDM_CA_BASE_PRD.MSO_Core_Utilization_Detail AS IP
    CROSS JOIN vt_params AS P
    WHERE IP.Admit BETWEEN P.StartDate AND P.EndDate
      AND IP.BedType IN ('Acute', 'OBS')
      AND IP.DataSource = 'Authorizations'
      AND IP.CareAlliesManagedFlag = 1
      AND IP.OperationalMarket = 'TX'
      AND IP.ManagingEntity = 'VALLEY ORGANIZED PHYSICIANS LLC (VOP)'
) WITH DATA
PRIMARY INDEX (AuthznKey)
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS COLUMN (AuthznKey)       ON vt_ip_base;
COLLECT STATISTICS COLUMN (AuthznKey_Clean) ON vt_ip_base;
COLLECT STATISTICS COLUMN (MemberID)        ON vt_ip_base;
COLLECT STATISTICS COLUMN (MemberID, Admit) ON vt_ip_base;
COLLECT STATISTICS COLUMN (AdmitMonth)      ON vt_ip_base;
COLLECT STATISTICS COLUMN (DischargeMonth)  ON vt_ip_base;


/*===============================================================================
  02. ACTIVE MEMBER FLAG
===============================================================================*/

CREATE MULTISET VOLATILE TABLE vt_active AS
(
    SELECT DISTINCT
        MemberID
    FROM BISDM_CA_VIEW_PRD.MSO_Current_Enrollment_Detail_CA
) WITH DATA
PRIMARY INDEX (MemberID)
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS COLUMN (MemberID) ON vt_active;


/*===============================================================================
  03. OBSERVATION CLAIM MATCH TABLES
      Separate key and fuzzy date matches to avoid one large mixed-condition join.
===============================================================================*/

CREATE MULTISET VOLATILE TABLE vt_observ_key AS
(
    SELECT DISTINCT
          CLM.AuthorizationID AS AuthznKey
        , CLM.MemberID
        , 'Observation' AS Source_Flag
    FROM OSS_PROVISIONING_V.sdogBSAClaim AS CLM
    CROSS JOIN vt_params AS P
    INNER JOIN (SELECT DISTINCT MemberID FROM vt_ip_base) AS M
        ON M.MemberID = CLM.MemberID
    WHERE CLM.DOSBegin >= P.StartDate
      AND CLM.RevenueCode IN ('0760', '0762', '0769')
      AND CLM.LatestFinalizedClaimIndicator = 'Y'
) WITH DATA
PRIMARY INDEX (AuthznKey)
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS COLUMN (AuthznKey) ON vt_observ_key;
COLLECT STATISTICS COLUMN (MemberID)  ON vt_observ_key;

CREATE MULTISET VOLATILE TABLE vt_observ_date AS
(
    SELECT DISTINCT
          CLM.MemberID
        , CLM.DOSBegin AS Obs_DOS
        , 'Observation' AS Source_Flag
    FROM OSS_PROVISIONING_V.sdogBSAClaim AS CLM
    CROSS JOIN vt_params AS P
    INNER JOIN (SELECT DISTINCT MemberID FROM vt_ip_base) AS M
        ON M.MemberID = CLM.MemberID
    WHERE CLM.DOSBegin >= P.StartDate
      AND CLM.RevenueCode IN ('0760', '0762', '0769')
      AND CLM.LatestFinalizedClaimIndicator = 'Y'
) WITH DATA
PRIMARY INDEX (MemberID, Obs_DOS)
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS COLUMN (MemberID, Obs_DOS) ON vt_observ_date;


/*===============================================================================
  04. ATTENDING / HOSPITALIST PROFESSIONAL CLAIM CANDIDATES
===============================================================================*/

CREATE MULTISET VOLATILE TABLE vt_hospitalist_claims AS
(
    SELECT DISTINCT
          CLM.MemberID
        , CLM.Level1Bucket
        , CLM.Level2Bucket
        , CLM.Level3Bucket
        , CLM.DOSBegin
        , CLM.ProviderID
        , CLM.ProviderName
        , CLM.ProviderNPI
        , CLM.ProviderTIN
        , CLM.ProviderSpecialtyCode
        , CLM.ProviderSpecialtyDesc
        , CLM.ProviderTaxonomyCode
        , CLM.ProviderTaxonomyCodeDesc
        , CASE
              WHEN CLM.Level3Bucket = 'Hospitalist' THEN 1
              WHEN CLM.Level3Bucket IN ('Critical Care (Intensivists)', 'Intensivist') THEN 2
              WHEN CLM.Level3Bucket IN ('Internal Medicine', 'Family Medicine', 'General Practice', 'Geriatric Medicine') THEN 3
              WHEN CLM.Level3Bucket IN ('Nurse Practitioner', 'Physician Assistant') THEN 4
              WHEN CLM.Level3Bucket IN ('Nephrology', 'Cardiology', 'Pulmonary Disease', 'Infectious Disease') THEN 5
              ELSE 6
          END AS SpecialtyPriority
    FROM OSS_PROVISIONING_V.sdogBSAClaim AS CLM
    CROSS JOIN vt_params AS P
    INNER JOIN (SELECT DISTINCT MemberID FROM vt_ip_base) AS M
        ON M.MemberID = CLM.MemberID
    WHERE CLM.ProductType = 'Medicare'
      AND CLM.SourceDataKey IN ('50', '210')
      AND CLM.DOSBegin >= P.StartDate
) WITH DATA
PRIMARY INDEX (MemberID, DOSBegin)
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS COLUMN (MemberID)        ON vt_hospitalist_claims;
COLLECT STATISTICS COLUMN (MemberID, DOSBegin) ON vt_hospitalist_claims;
COLLECT STATISTICS COLUMN (ProviderNPI)     ON vt_hospitalist_claims;
COLLECT STATISTICS COLUMN (ProviderTIN)     ON vt_hospitalist_claims;


/*===============================================================================
  05. BEST ATTENDING PROVIDER PER AUTHORIZATION STAY
      Grain: one row per AuthznKey.
===============================================================================*/

CREATE MULTISET VOLATILE TABLE vt_attending_ranked AS
(
    SELECT
          IP.AuthznKey
        , HP.Level1Bucket
        , HP.Level2Bucket
        , HP.Level3Bucket
        , HP.DOSBegin
        , HP.ProviderID
        , HP.ProviderName
        , HP.ProviderNPI
        , HP.ProviderTIN
        , HP.ProviderSpecialtyCode
        , HP.ProviderSpecialtyDesc
        , HP.ProviderTaxonomyCode
        , HP.ProviderTaxonomyCodeDesc
        , HP.SpecialtyPriority
        , ROW_NUMBER() OVER
          (
              PARTITION BY IP.AuthznKey
              ORDER BY
                  CASE WHEN HP.ProviderNPI IS NULL THEN 9 ELSE HP.SpecialtyPriority END ASC,
                  HP.DOSBegin ASC,
                  HP.ProviderNPI ASC
          ) AS AttendingRank
    FROM vt_ip_base AS IP
    LEFT JOIN vt_hospitalist_claims AS HP
        ON IP.MemberID = HP.MemberID
       AND HP.DOSBegin BETWEEN IP.Admit AND COALESCE(IP.Discharge, IP.Admit)
) WITH DATA
PRIMARY INDEX (AuthznKey)
ON COMMIT PRESERVE ROWS
;

CREATE MULTISET VOLATILE TABLE vt_attending_best AS
(
    SELECT *
    FROM vt_attending_ranked
    WHERE AttendingRank = 1
) WITH DATA
PRIMARY INDEX (AuthznKey)
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS COLUMN (AuthznKey)   ON vt_attending_best;
COLLECT STATISTICS COLUMN (ProviderNPI) ON vt_attending_best;
COLLECT STATISTICS COLUMN (ProviderTIN) ON vt_attending_best;


/*===============================================================================
  06. REFERENCE TABLES FOR FACILITY AND ATTENDING GROUP ENRICHMENT
===============================================================================*/

CREATE MULTISET VOLATILE TABLE vt_facility_map AS
(
    SELECT DISTINCT
          OriginalFacilityName
        , CleanFacilityName
        , GeneralFacilityName
    FROM BISDM_CA_BASE_PRD.Facility_Mapping_TX
) WITH DATA
PRIMARY INDEX (OriginalFacilityName)
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS COLUMN (OriginalFacilityName) ON vt_facility_map;

CREATE MULTISET VOLATILE TABLE vt_vop_hospitalist_group AS
(
    SELECT DISTINCT
          VendorTIN
        , "Group"
        , Hospital
        , "VendorNPI"
    FROM BISDM_CA_BASE_PRD.VOP_Hospitalist_Group
) WITH DATA
PRIMARY INDEX ("VendorNPI")
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS COLUMN ("VendorNPI") ON vt_vop_hospitalist_group;
COLLECT STATISTICS COLUMN (VendorTIN)   ON vt_vop_hospitalist_group;

CREATE MULTISET VOLATILE TABLE vt_catalyst_fedid AS
(
    SELECT DISTINCT
          ProviderNPI
        , FederalTaxID
        , 'CATALYST MEDICAL GROUP' AS GroupName
    FROM BISDM_CA_BASE_PRD.AllProviders_FederalIDs
    WHERE EntityName LIKE '%Catalyst%'
) WITH DATA
PRIMARY INDEX (ProviderNPI)
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS COLUMN (ProviderNPI) ON vt_catalyst_fedid;

CREATE MULTISET VOLATILE TABLE vt_attending_group_map
(
      MatchType  VARCHAR(20)
    , MatchValue VARCHAR(50)
    , GroupName  VARCHAR(100)
)
PRIMARY INDEX (MatchType, MatchValue)
ON COMMIT PRESERVE ROWS
;

/* TIN-based mappings. Update these as Sarah / source-of-truth alignment changes. */
INSERT INTO vt_attending_group_map VALUES ('TIN', '301102684', 'BENCHMARK / BEYOND');
INSERT INTO vt_attending_group_map VALUES ('TIN', '742810500', 'IM');
INSERT INTO vt_attending_group_map VALUES ('TIN', '814005993', 'WHG');
INSERT INTO vt_attending_group_map VALUES ('TIN', '273479673', 'FEMI');
INSERT INTO vt_attending_group_map VALUES ('TIN', '271724682', 'CATALYST');
INSERT INTO vt_attending_group_map VALUES ('TIN', '800725733', 'HNI');

/* Optional NPI overrides, if Beyond / Benchmark needs NPI-specific attribution. */
-- INSERT INTO vt_attending_group_map VALUES ('NPI', '1234567890', 'BENCHMARK / BEYOND');

COLLECT STATISTICS COLUMN (MatchType, MatchValue) ON vt_attending_group_map;


/*===============================================================================
  07. READMISSION INDEX EVENT LOGIC FROM AUTHORIZATION EVENTS
      Grain: one row per eligible acute authorization index stay.
===============================================================================*/

CREATE MULTISET VOLATILE TABLE vt_readmit_index_candidates AS
(
    SELECT
          IP.AuthznKey
        , IP.AuthznKey_Clean
        , IP.MemberID
        , IP.Admit
        , IP.Discharge
        , IP.AdmitMonth
        , IP.DischargeMonth
        , IP.BedType
        , IP.PlanType
        , IP.HCODE
        , IP.OperationalMarket
        , IP.OperationalSubMarket
        , IP.ManagingEntity
        , IP.ReportingPod
        , IP.PodCd
        , IP.PodName
        , IP.PCPName
        , IP.PCPNPI
        , IP.AdmittingFacilityName
        , IP.AdmittingFacilityNPI
        , ROW_NUMBER() OVER
          (
              PARTITION BY IP.AuthznKey
              ORDER BY IP.Admit ASC, IP.Discharge ASC, IP.AuthznKey ASC
          ) AS StayRank
    FROM vt_ip_base AS IP
    WHERE IP.BedType = 'Acute'
      AND COALESCE(IP.PlanType, '') <> 'PPO'
      AND LEFT(IP.HCODE, 1) = 'H'
      AND IP.Discharge IS NOT NULL
) WITH DATA
PRIMARY INDEX (MemberID, Admit)
ON COMMIT PRESERVE ROWS
;

CREATE MULTISET VOLATILE TABLE vt_readmit_index_events AS
(
    SELECT
          AuthznKey
        , AuthznKey_Clean
        , MemberID
        , Admit
        , Discharge
        , AdmitMonth
        , DischargeMonth
        , BedType
        , PlanType
        , HCODE
        , OperationalMarket
        , OperationalSubMarket
        , ManagingEntity
        , ReportingPod
        , PodCd
        , PodName
        , PCPName
        , PCPNPI
        , AdmittingFacilityName
        , AdmittingFacilityNPI
    FROM vt_readmit_index_candidates
    WHERE StayRank = 1
) WITH DATA
PRIMARY INDEX (MemberID, Admit)
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS COLUMN (AuthznKey)       ON vt_readmit_index_events;
COLLECT STATISTICS COLUMN (MemberID, Admit) ON vt_readmit_index_events;
COLLECT STATISTICS COLUMN (DischargeMonth)  ON vt_readmit_index_events;

CREATE MULTISET VOLATILE TABLE vt_readmit_sequenced AS
(
    SELECT
          IDX.*
        , LEAD(IDX.AuthznKey) OVER
          (
              PARTITION BY IDX.MemberID
              ORDER BY IDX.Admit ASC, IDX.Discharge ASC, IDX.AuthznKey ASC
          ) AS NextAuthznKey
        , LEAD(IDX.Admit) OVER
          (
              PARTITION BY IDX.MemberID
              ORDER BY IDX.Admit ASC, IDX.Discharge ASC, IDX.AuthznKey ASC
          ) AS NextAdmit
        , LEAD(IDX.Discharge) OVER
          (
              PARTITION BY IDX.MemberID
              ORDER BY IDX.Admit ASC, IDX.Discharge ASC, IDX.AuthznKey ASC
          ) AS NextDischarge
    FROM vt_readmit_index_events AS IDX
) WITH DATA
PRIMARY INDEX (AuthznKey)
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS COLUMN (AuthznKey)       ON vt_readmit_sequenced;
COLLECT STATISTICS COLUMN (MemberID, Admit) ON vt_readmit_sequenced;

CREATE MULTISET VOLATILE TABLE vt_readmission_flags AS
(
    SELECT
          R.AuthznKey
        , R.AuthznKey_Clean
        , R.MemberID
        , R.Admit
        , R.Discharge
        , R.AdmitMonth
        , R.DischargeMonth
        , R.DischargeMonth AS ReadmissionIndexMonth
        , R.NextAuthznKey
        , R.NextAdmit
        , R.NextDischarge
        , CASE
              WHEN R.NextAdmit IS NOT NULL
              THEN R.NextAdmit - R.Discharge
              ELSE NULL
          END AS DaysToNextAcuteAdmit
        , 1 AS ReadmitDenominator
        , CASE
              WHEN R.NextAdmit IS NOT NULL
               AND R.NextAdmit - R.Discharge > P.TransferBufferDays
               AND R.NextAdmit - R.Discharge <= P.ReadmitWindowDays
              THEN 1
              ELSE 0
          END AS ReadmitNumerator
        , CASE
              WHEN R.NextAdmit IS NOT NULL
               AND R.NextAdmit - R.Discharge BETWEEN 0 AND P.TransferBufferDays
              THEN 1
              ELSE 0
          END AS TransferBufferFlag
        , CASE
              WHEN R.NextAdmit IS NOT NULL
               AND R.NextAdmit - R.Discharge < 0
              THEN 1
              ELSE 0
          END AS OverlapOrDateIssueFlag
        , CASE
              WHEN R.NextAdmit IS NULL THEN 'No Subsequent Acute Admit'
              WHEN R.NextAdmit - R.Discharge < 0 THEN 'Overlap / Date Issue'
              WHEN R.NextAdmit - R.Discharge BETWEEN 0 AND P.TransferBufferDays THEN '0-2 Day Buffer / Possible Transfer'
              WHEN R.NextAdmit - R.Discharge <= P.ReadmitWindowDays THEN 'Readmission Numerator'
              ELSE 'Outside Readmission Window'
          END AS ReadmissionClassification
    FROM vt_readmit_sequenced AS R
    CROSS JOIN vt_params AS P
) WITH DATA
PRIMARY INDEX (AuthznKey)
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS COLUMN (AuthznKey)       ON vt_readmission_flags;
COLLECT STATISTICS COLUMN (MemberID)        ON vt_readmission_flags;
COLLECT STATISTICS COLUMN (DischargeMonth)  ON vt_readmission_flags;
COLLECT STATISTICS COLUMN (ReadmissionIndexMonth) ON vt_readmission_flags;


/*===============================================================================
  08. EVENT-LEVEL BACKBONE BEFORE PROVIDER VOLUME FLAGS
      Grain: one row per AuthznKey.
===============================================================================*/

CREATE MULTISET VOLATILE TABLE vt_event_backbone_pre_volume AS
(
    SELECT
          IP.BedType
        , IP.DataSource
        , IP.OperationalMarket
        , IP.OperationalSubMarket
        , IP.ReportingPod
        , IP.ManagingEntity
        , IP.ManagingProviderName
        , IP.PodCd
        , IP.PodName
        , IP.PCPName
        , IP.PCPNPI
        , IP.AuthznKey
        , IP.AuthznKey_Clean
        , IP.MemberID
        , IP.AdmitYear
        , IP.Admit
        , IP.Discharge
        , IP.AdmitMonth
        , IP.DischargeMonth
        , IP.LengthOfStay

        , CASE
              WHEN OB_Key.AuthznKey IS NOT NULL THEN 'Key Match'
              WHEN OB_Date.MemberID IS NOT NULL THEN 'Fuzzy Date Match'
              ELSE 'No Obs Found'
          END AS Obs_Match_Method
        , COALESCE(OB_Key.Source_Flag, OB_Date.Source_Flag) AS Obs_Claim_Indicator
        , COALESCE(OB_Key.AuthznKey, CAST(OB_Date.Obs_DOS AS VARCHAR(30))) AS Obs_Date

        , CASE WHEN AC.MemberID IS NOT NULL THEN 1 ELSE 0 END AS Active

        , IP.AdmittingFacilityName
        , IP.AdmittingFacilityNPI
        , FA.CleanFacilityName AS AdmittingFacilityNameClean
        , FA.GeneralFacilityName AS AdmittingFacilityNameGroup

        , IP.AdmissionReason
        , IP.Expired
        , IP.MemberDOB
        , IP.HCODE
        , IP.PlanType
        , IP.AdmitDischargeError
        , IP.PrimaryDiagnosisCode
        , IP.PrimaryDiagnosis
        , IP.LACE
        , IP.AdmissionType
        , IP.AdmitFrom
        , IP.DischargeStatusInd
        , IP.DischargeStatusCode
        , IP.DischargeStatusDescription
        , IP.Discharge_Disposition_Group

        , HP.Level1Bucket AS AttendingProviderLevel1Bucket
        , HP.Level2Bucket AS AttendingProviderLevel2Bucket
        , HP.Level3Bucket AS AttendingProviderLevel3Bucket
        , HP.DOSBegin AS AttendingProviderDOSBegin
        , HP.ProviderName AS AttendingProviderName
        , HP.ProviderNPI AS AttendingProviderNPI
        , HP.ProviderTIN AS AttendingProviderTIN
        , HP.ProviderSpecialtyCode AS AttendingProviderSpecialtyCode
        , HP.ProviderSpecialtyDesc AS AttendingProviderSpecialtyDesc
        , HP.ProviderTaxonomyCode AS AttendingProviderTaxonomyCode
        , HP.ProviderTaxonomyCodeDesc AS AttendingProviderTaxonomyCodeDesc
        , HP.SpecialtyPriority AS AttendingProviderSpecialtyPriority

        , CASE
              WHEN GM_NPI.MatchValue IS NOT NULL THEN 1
              WHEN GM_TIN.MatchValue IS NOT NULL THEN 1
              WHEN VOP."VendorNPI" IS NOT NULL THEN 1
              WHEN FEDID.ProviderNPI IS NOT NULL THEN 1
              ELSE 0
          END AS AttendingProvider_VOP

        , VOP.Hospital AS AttendingProviderHospital_VOP

        , CASE
              WHEN UPPER(COALESCE(GM_NPI.GroupName, GM_TIN.GroupName, VOP."Group", FEDID.GroupName))
                   IN ('BEYOND', 'BEYOND PHYSICIANS', 'BENCHMARK', 'BENCHMARK IP', 'BENCHMARK / BEYOND')
              THEN 'BENCHMARK / BEYOND'
              ELSE UPPER(COALESCE(GM_NPI.GroupName, GM_TIN.GroupName, VOP."Group", FEDID.GroupName, 'UNMAPPED'))
          END AS AttendingProviderHospitalGroupName_VOP

        , VOP."VendorNPI" AS VOPAttendingProviderHospitalGroupNPI_VOP

        , COALESCE(RF.ReadmitDenominator, 0) AS ReadmitDenominator
        , COALESCE(RF.ReadmitNumerator, 0) AS ReadmitNumerator
        , RF.ReadmissionIndexMonth
        , RF.NextAuthznKey
        , RF.NextAdmit
        , RF.NextDischarge
        , RF.DaysToNextAcuteAdmit
        , COALESCE(RF.TransferBufferFlag, 0) AS TransferBufferFlag
        , COALESCE(RF.OverlapOrDateIssueFlag, 0) AS OverlapOrDateIssueFlag
        , RF.ReadmissionClassification

    FROM vt_ip_base AS IP

    LEFT JOIN vt_observ_key AS OB_Key
        ON IP.AuthznKey_Clean = OB_Key.AuthznKey
       AND IP.MemberID = OB_Key.MemberID

    LEFT JOIN vt_observ_date AS OB_Date
        ON IP.MemberID = OB_Date.MemberID
       AND OB_Date.Obs_DOS BETWEEN (IP.Admit - 1) AND (IP.Admit + 1)
       AND OB_Key.AuthznKey IS NULL

    LEFT JOIN vt_active AS AC
        ON IP.MemberID = AC.MemberID

    LEFT JOIN vt_attending_best AS HP
        ON IP.AuthznKey = HP.AuthznKey

    LEFT JOIN vt_vop_hospitalist_group AS VOP
        ON HP.ProviderNPI = VOP."VendorNPI"

    LEFT JOIN vt_attending_group_map AS GM_NPI
        ON GM_NPI.MatchType = 'NPI'
       AND HP.ProviderNPI = GM_NPI.MatchValue

    LEFT JOIN vt_attending_group_map AS GM_TIN
        ON GM_TIN.MatchType = 'TIN'
       AND HP.ProviderTIN = GM_TIN.MatchValue

    LEFT JOIN vt_catalyst_fedid AS FEDID
        ON HP.ProviderNPI = FEDID.ProviderNPI

    LEFT JOIN vt_facility_map AS FA
        ON FA.OriginalFacilityName = IP.AdmittingFacilityName

    LEFT JOIN vt_readmission_flags AS RF
        ON IP.AuthznKey = RF.AuthznKey

    QUALIFY ROW_NUMBER() OVER
    (
        PARTITION BY IP.AuthznKey
        ORDER BY
            CASE WHEN HP.ProviderNPI IS NULL THEN 9 ELSE HP.SpecialtyPriority END ASC,
            HP.DOSBegin ASC,
            IP.AuthznKey ASC
    ) = 1
) WITH DATA
PRIMARY INDEX (AuthznKey)
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS COLUMN (AuthznKey) ON vt_event_backbone_pre_volume;
COLLECT STATISTICS COLUMN (MemberID) ON vt_event_backbone_pre_volume;
COLLECT STATISTICS COLUMN (AdmitMonth) ON vt_event_backbone_pre_volume;
COLLECT STATISTICS COLUMN (DischargeMonth) ON vt_event_backbone_pre_volume;
COLLECT STATISTICS COLUMN (ReadmissionIndexMonth) ON vt_event_backbone_pre_volume;
COLLECT STATISTICS COLUMN (AttendingProviderNPI) ON vt_event_backbone_pre_volume;
COLLECT STATISTICS COLUMN (AdmittingFacilityNameGroup) ON vt_event_backbone_pre_volume;
COLLECT STATISTICS COLUMN (AttendingProviderHospitalGroupName_VOP) ON vt_event_backbone_pre_volume;


/*===============================================================================
  09. PROVIDER VOLUME FLAGS
===============================================================================*/

CREATE MULTISET VOLATILE TABLE vt_provider_volume AS
(
    SELECT
          AttendingProviderNPI
        , MAX(AttendingProviderName) AS AttendingProviderName
        , COUNT(DISTINCT AuthznKey) AS AttendingProviderStayCount
    FROM vt_event_backbone_pre_volume
    WHERE AttendingProviderNPI IS NOT NULL
    GROUP BY 1
) WITH DATA
PRIMARY INDEX (AttendingProviderNPI)
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS COLUMN (AttendingProviderNPI) ON vt_provider_volume;
COLLECT STATISTICS COLUMN (AttendingProviderStayCount) ON vt_provider_volume;

CREATE MULTISET VOLATILE TABLE vt_provider_volume_ranked AS
(
    SELECT
          PV.AttendingProviderNPI
        , PV.AttendingProviderName
        , PV.AttendingProviderStayCount
        , CUME_DIST() OVER (ORDER BY PV.AttendingProviderStayCount) AS AttendingProviderVolumePercentile
        , CASE
              WHEN PV.AttendingProviderStayCount < 25 THEN 'Low Volume'
              WHEN PV.AttendingProviderStayCount BETWEEN 25 AND 49 THEN 'Moderate Volume'
              WHEN PV.AttendingProviderStayCount >= 50 THEN 'Reliable Volume'
              ELSE 'Unknown'
          END AS AttendingProviderVolumeTier
        , CASE WHEN PV.AttendingProviderStayCount < 25 THEN 1 ELSE 0 END AS LowVolumeProviderFlag
        , CASE WHEN CUME_DIST() OVER (ORDER BY PV.AttendingProviderStayCount) >= 0.95 THEN 1 ELSE 0 END AS HighVolumeProviderFlag
    FROM vt_provider_volume AS PV
) WITH DATA
PRIMARY INDEX (AttendingProviderNPI)
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS COLUMN (AttendingProviderNPI) ON vt_provider_volume_ranked;


/*===============================================================================
  10. FINAL EVENT BACKBONE
      Grain: one row per AuthznKey.
===============================================================================*/

CREATE MULTISET VOLATILE TABLE vt_event_backbone AS
(
    SELECT
          F.*
        , PV.AttendingProviderStayCount
        , PV.AttendingProviderVolumePercentile
        , PV.AttendingProviderVolumeTier
        , COALESCE(PV.LowVolumeProviderFlag, 0) AS LowVolumeProviderFlag
        , COALESCE(PV.HighVolumeProviderFlag, 0) AS HighVolumeProviderFlag
    FROM vt_event_backbone_pre_volume AS F
    LEFT JOIN vt_provider_volume_ranked AS PV
        ON F.AttendingProviderNPI = PV.AttendingProviderNPI
) WITH DATA
PRIMARY INDEX (AuthznKey)
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS COLUMN (AuthznKey) ON vt_event_backbone;
COLLECT STATISTICS COLUMN (MemberID) ON vt_event_backbone;
COLLECT STATISTICS COLUMN (AdmitMonth) ON vt_event_backbone;
COLLECT STATISTICS COLUMN (DischargeMonth) ON vt_event_backbone;
COLLECT STATISTICS COLUMN (ReadmissionIndexMonth) ON vt_event_backbone;
COLLECT STATISTICS COLUMN (AttendingProviderNPI) ON vt_event_backbone;
COLLECT STATISTICS COLUMN (AdmittingFacilityNameGroup) ON vt_event_backbone;
COLLECT STATISTICS COLUMN (AttendingProviderHospitalGroupName_VOP) ON vt_event_backbone;


/*===============================================================================
  11. MONTHLY MART FOR TABLEAU / EXCEL / PYTHON
      Grain: month + facility group + attending provider + attending group.

      Readmission rate rule:
          SUM(ReadmitNumerator) / SUM(ReadmitDenominator)
===============================================================================*/

CREATE MULTISET VOLATILE TABLE vt_mart_provider_facility_monthly AS
(
    SELECT
          COALESCE(ReadmissionIndexMonth, DischargeMonth, AdmitMonth) AS MetricMonth
        , OperationalMarket
        , OperationalSubMarket
        , ManagingEntity
        , ReportingPod
        , PodCd
        , PodName
        , PCPName
        , PCPNPI
        , AdmittingFacilityNameGroup
        , AttendingProviderNPI
        , AttendingProviderName
        , AttendingProviderTIN
        , AttendingProviderHospitalGroupName_VOP
        , AttendingProviderVolumeTier
        , LowVolumeProviderFlag
        , HighVolumeProviderFlag

        , COUNT(DISTINCT AuthznKey) AS StayCount
        , COUNT(DISTINCT CASE WHEN BedType = 'Acute' THEN AuthznKey END) AS AcuteStayCount
        , COUNT(DISTINCT CASE WHEN BedType = 'OBS' THEN AuthznKey END) AS OBSStayCount
        , SUM(CASE WHEN Obs_Claim_Indicator = 'Observation' THEN 1 ELSE 0 END) AS ObsClaimIndicatorCount
        , AVG(LengthOfStay) AS AvgLengthOfStay
        , SUM(CASE WHEN Discharge_Disposition_Group = 'SNF/Rehab' THEN 1 ELSE 0 END) AS SNFRehabDischargeCount
        , SUM(CASE WHEN Discharge_Disposition_Group = 'Home Health' THEN 1 ELSE 0 END) AS HomeHealthDischargeCount
        , SUM(CASE WHEN Discharge_Disposition_Group = 'Home' THEN 1 ELSE 0 END) AS HomeDischargeCount
        , SUM(ReadmitNumerator) AS ReadmitNumerator
        , SUM(ReadmitDenominator) AS ReadmitDenominator
        , CASE
              WHEN SUM(ReadmitDenominator) > 0
              THEN CAST(SUM(ReadmitNumerator) AS DECIMAL(18,4)) / NULLIFZERO(SUM(ReadmitDenominator))
              ELSE NULL
          END AS ReadmissionRate
    FROM vt_event_backbone
    GROUP BY
          COALESCE(ReadmissionIndexMonth, DischargeMonth, AdmitMonth)
        , OperationalMarket
        , OperationalSubMarket
        , ManagingEntity
        , ReportingPod
        , PodCd
        , PodName
        , PCPName
        , PCPNPI
        , AdmittingFacilityNameGroup
        , AttendingProviderNPI
        , AttendingProviderName
        , AttendingProviderTIN
        , AttendingProviderHospitalGroupName_VOP
        , AttendingProviderVolumeTier
        , LowVolumeProviderFlag
        , HighVolumeProviderFlag
) WITH DATA
PRIMARY INDEX (MetricMonth, AttendingProviderNPI)
ON COMMIT PRESERVE ROWS
;

COLLECT STATISTICS COLUMN (MetricMonth) ON vt_mart_provider_facility_monthly;
COLLECT STATISTICS COLUMN (AttendingProviderNPI) ON vt_mart_provider_facility_monthly;
COLLECT STATISTICS COLUMN (AdmittingFacilityNameGroup) ON vt_mart_provider_facility_monthly;
COLLECT STATISTICS COLUMN (AttendingProviderHospitalGroupName_VOP) ON vt_mart_provider_facility_monthly;


/*===============================================================================
  12. OPTIONAL PERSISTENT TABLE CREATION
      Uncomment after QA. Avoid DROP in production unless controlled by deployment.
===============================================================================*/

/*
CREATE TABLE BISDM_CA_BASE_PRD.MSO_IP_Event_Readmission_Backbone AS
(
    SELECT *
    FROM vt_event_backbone
) WITH DATA
PRIMARY INDEX (AuthznKey)
;

CREATE TABLE BISDM_CA_BASE_PRD.MSO_IP_Provider_Facility_Monthly_Mart AS
(
    SELECT *
    FROM vt_mart_provider_facility_monthly
) WITH DATA
PRIMARY INDEX (MetricMonth, AttendingProviderNPI)
;
*/


/*===============================================================================
  13. QA CHECKS
===============================================================================*/

/* 13A. Confirm final event backbone is one row per AuthznKey. */
SELECT
      COUNT(*) AS RowCount
    , COUNT(DISTINCT AuthznKey) AS DistinctAuthznKeyCount
FROM vt_event_backbone
;

/* 13B. Check duplicate AuthznKeys, should return zero rows. */
SELECT
      AuthznKey
    , COUNT(*) AS RowCount
FROM vt_event_backbone
GROUP BY 1
HAVING COUNT(*) > 1
;

/* 13C. Overall readmission rate. */
SELECT
      SUM(ReadmitNumerator) AS ReadmitNumerator
    , SUM(ReadmitDenominator) AS ReadmitDenominator
    , CASE
          WHEN SUM(ReadmitDenominator) > 0
          THEN CAST(SUM(ReadmitNumerator) AS DECIMAL(18,4)) / NULLIFZERO(SUM(ReadmitDenominator))
          ELSE NULL
      END AS ReadmissionRate
FROM vt_event_backbone
;

/* 13D. Readmission classification counts. */
SELECT
      ReadmissionClassification
    , COUNT(*) AS StayCount
    , SUM(ReadmitNumerator) AS ReadmitNumerator
    , SUM(ReadmitDenominator) AS ReadmitDenominator
FROM vt_event_backbone
WHERE BedType = 'Acute'
GROUP BY 1
ORDER BY 1
;

/* 13E. Provider/facility monthly mart preview. */
SELECT *
FROM vt_mart_provider_facility_monthly
ORDER BY MetricMonth, AdmittingFacilityNameGroup, AttendingProviderHospitalGroupName_VOP, AttendingProviderName
;
