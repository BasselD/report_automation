

CREATE MULTISET VOLATILE TABLE A AS (
    SELECT DISTINCT
          e.OperationalMarket
        , e.OperationalSubMarket
        , e.ManagingEntity
        , e.ReportingPod
        , e.PodName
        , e.PodCode
        , e.HCODE
        , e.engagementCode
        , e.PCPName
        , e.PCPID
        , e.PCPNPI
        , e.MemberID
        , e.PlanType AS PlanDescription
        , e.PBP
        , e.MemberName AS MemberFullName
        , e.MemberDOB
        , ee.DeathDate AS MemberExpiredDate
        , CASE WHEN e.Deceased = 1 THEN 'Yes' ELSE 'No' END AS MemberExpired
        , e.ContinuousEffectiveDate
        , e.ReportBeginDate
        , e.ReportEndDate
        , CASE WHEN YEAR(e.ContinuousEffectiveDate) = YEAR(e.ReportBeginDate) THEN 1 ELSE 0 END AS NewMemberYTD
        , CASE WHEN YEAR(e.ContinuousEffectiveDate) <> YEAR(e.ReportBeginDate) THEN 1 ELSE 0 END AS ExistingMemberYTD
        , e.CareAlliesManagedFlag
    FROM BISDM_CA_BASE_PRD.MSO_Core_Enrollment_Detail AS e
    LEFT JOIN (
        SELECT DISTINCT
              ee.memberid
            , ee.DeathDate
        FROM OSS_PROVISIONING_V.sdoGBSAEnrollment AS ee
        /* TODO: screenshot cuts off this predicate. Confirm death-date filter below. */
        -- WHERE CAST(ee.DeathDate AS DATE) IS NOT NULL
    ) AS ee
        ON ee.memberid = e.MemberID
    WHERE 1 = 1
      AND e.ReportBeginDate BETWEEN CAST((EXTRACT(YEAR FROM DATE) - 2) || '/01/01' AS DATE FORMAT 'YYYY-MM-DD') AND DATE
      AND e.ProductType = 'Medicare'
) WITH DATA
PRIMARY INDEX (MemberID, PCPID, ReportBeginDate, ReportEndDate)
ON COMMIT PRESERVE ROWS
;

UPDATE A
FROM (
    SELECT DISTINCT
          d.MemberID
        , d.Admit - EXTRACT(DAY FROM d.Admit) + 1 AS Admit
        , d.PCPID AS UtilPCP
        , a.PCPID AS ShipPCP
    FROM BISDM_CA_BASE_PRD.MSO_Core_Utilization_Detail AS d
    LEFT JOIN (
        SELECT DISTINCT
              a.PCPID
            , a.PCPNPI
            , a.MemberID
            , a.ReportBeginDate
            , a.ReportEndDate
        FROM A AS a
    ) AS x
        ON x.MemberID = d.MemberID
       AND d.Admit BETWEEN x.ReportBeginDate AND x.ReportEndDate
    WHERE 1 = 1
      AND d.PCPID <> 'U'
      AND a.PCPID = 'U'   /* TODO: screenshot appears to reference a.PCPID, but alias a is not visible in the join. Confirm alias. */
) AS x
SET PCPID = x.UtilPCP
WHERE 1 = 1
  AND A.MemberID = x.MemberID
  AND A.ReportBeginDate = x.Admit
;

CREATE MULTISET VOLATILE TABLE B AS (
    SELECT
          d.MemberID
        , d.Admit - EXTRACT(DAY FROM d.Admit) + 1 AS Admit
        , d.BedType
        , COUNT(DISTINCT CASE WHEN d.DataSource = 'Claims' THEN d.ClaimID END) AS ClaimCnt
        , COUNT(DISTINCT CASE WHEN d.DataSource = 'Authorizations' THEN d.AuthznKey END) AS AuthCnt
    FROM BISDM_CA_BASE_PRD.MSO_Core_Utilization_Detail AS d
    WHERE 1 = 1
    GROUP BY 1, 2, 3
) WITH DATA
PRIMARY INDEX (MemberID, Admit, BedType)
ON COMMIT PRESERVE ROWS
;

CREATE MULTISET VOLATILE TABLE readmits AS (
    SELECT
          x.ReportDate
        , x.OperationalMarket
        , x.OperationalSubMarket
        , x.ReportingPod
        , x.PodName
        , x.ManagingEntity
        , x.PodCd AS PodCode
        , x.PCPName
        , x.PCPID
        , x.PCPNPI
        , x.MemberID
        , SUM(x.ReadmitFlg) AS Readmits
        , SUM(x.ReadmitDenominator) AS ReadmitDenominator
    FROM (
        SELECT
              CAST(CAST(YEAR(d.Admit) AS VARCHAR(4)) || '-' || RIGHT('0' || CAST(MONTH(d.Admit) AS VARCHAR(2)), 2) || '-15' AS DATE) AS ReportDate
            , d.PCPName
            , d.PCPID
            , d.PCPNPI
            , d.OperationalMarket
            , d.OperationalSubMarket
            , d.ManagingEntity
            , d.PodName
            , d.ReportingPod
            , d.PodCd
            , d.MemberID
            , d.MemberName
            , d.Admit
            , d.Discharge
            -- , LEAD(d.Admit) OVER (PARTITION BY d.MemberID ORDER BY d.Admit) AS NextAdmit
            -- , CASE WHEN NextAdmit - d.Discharge <= 2 THEN NULL ELSE NextAdmit - d.Discharge END AS ReadmitDays
            -- , CASE WHEN ReadmitDays <= 30 THEN 1 ELSE 0 END AS ReadmitFlg
            , d.Readmit AS ReadmitFlg
            , d.ReadmitDenominator
        FROM BISDM_CA_BASE_PRD.MSO_Core_Utilization_Detail AS d
        WHERE 1 = 1
          AND YEAR(d.Admit) BETWEEN YEAR(CURRENT_DATE) - 2 AND YEAR(CURRENT_DATE)
          AND d.PlanType <> 'PPO'
          AND LEFT(d.HCODE, 1) = 'H'
          AND d.BedType = 'Acute'
          AND d.DataSource = 'Authorizations'
          -- AND d.ReportingPod = 'RPO MERIT'
          -- AND d.MemberID = '35434576'
          -- ORDER BY d.MemberID, d.Admit, d.Discharge
    ) AS x
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
) WITH DATA
PRIMARY INDEX (PCPID, PodCode, ReportDate)
ON COMMIT PRESERVE ROWS
;

-- DELETE BISDM_CA_BASE_PRD.MSO_Core_Utilization_Summary;
DROP TABLE BISDM_CA_BASE_PRD.MSO_Core_Utilization_Summary;

-- INSERT INTO BISDM_CA_BASE_PRD.MSO_Core_Utilization_Summary
CREATE TABLE BISDM_CA_BASE_PRD.MSO_Core_Utilization_Summary AS (
    SELECT
          a.*
        , det.ClaimCnt  AS Acute_Claims
        , det.AuthCnt   AS Acute_Auths
        , det2.ClaimCnt AS BH_Acute_Claims
        , det2.AuthCnt  AS BH_Acute_Auths
        , det3.ClaimCnt AS BH_OBS_Claims
        , det3.AuthCnt  AS BH_OBS_Auths
        , det4.ClaimCnt AS BH_REHAB_Claims
        , det4.AuthCnt  AS BH_REHAB_Auths
        , det5.ClaimCnt AS ER_Claims
        , det5.AuthCnt  AS ER_Auths
        , det6.ClaimCnt AS ER_IP_Claims
        , det6.AuthCnt  AS ER_IP_Auths
        , det7.ClaimCnt AS ER_IP_BH_Claims
        , det7.AuthCnt  AS ER_IP_BH_Auths
        , det8.ClaimCnt AS ER_IP_REHAB_Claims
        , det8.AuthCnt  AS ER_IP_REHAB_Auths
        , det9.ClaimCnt AS ER_IP_SNF_Claims
        , det9.AuthCnt  AS ER_IP_SNF_Auths
        , det10.ClaimCnt AS ER_OBS_Claims
        , det10.AuthCnt  AS ER_OBS_Auths
        , det11.ClaimCnt AS LTAC_Claims
        , det11.AuthCnt  AS LTAC_Auths
        , det12.ClaimCnt AS OBS_Claims
        , det12.AuthCnt  AS OBS_Auths
        , det13.ClaimCnt AS REHAB_Claims
        , det13.AuthCnt  AS REHAB_Auths
        , det14.ClaimCnt AS SNF_Claims
        , det14.AuthCnt  AS SNF_Auths
        , cms.Eligible AS CMS_Eligible
        , cms.AllCause AS CMS_AllCause
        , cms.AllCauseNonElective AS CMS_AllCauseNonElective
        , cms.RelatedCause AS CMS_RelatedCause
        , cms.RelatedCauseNonElective AS CMS_RelatedCauseNonElective
        , cms.NonRelatedCause AS CMS_NonRelatedCause
        , cms.NonRelatedNonElective AS CMS_NonRelatedNonElective
        , hed.HEDISReadmits AS HEDIS_Readmits
        , r.Readmits AS Auth_Readmits
        , r.ReadmitDenominator AS Auth_ReadmitsDenominator
        , CAST(CURRENT_DATE AS TIMESTAMP(0)) + ((CURRENT_TIME - TIME '00:00:00') HOUR TO SECOND(0)) AS LastUpdate
        , USER AS LastUpdateBy
    FROM A AS a
    LEFT JOIN B AS det
        ON a.MemberID = det.MemberID
       AND det.Admit BETWEEN a.ReportBeginDate AND a.ReportEndDate
       AND det.BedType = 'Acute'
    LEFT JOIN B AS det2
        ON a.MemberID = det2.MemberID
       AND det2.Admit BETWEEN a.ReportBeginDate AND a.ReportEndDate
       AND det2.BedType = 'BH Acute'
    LEFT JOIN B AS det3
        ON a.MemberID = det3.MemberID
       AND det3.Admit BETWEEN a.ReportBeginDate AND a.ReportEndDate
       AND det3.BedType = 'BH OBS'
    LEFT JOIN B AS det4
        ON a.MemberID = det4.MemberID
       AND det4.Admit BETWEEN a.ReportBeginDate AND a.ReportEndDate
       AND det4.BedType = 'BH REHAB'
    LEFT JOIN B AS det5
        ON a.MemberID = det5.MemberID
       AND det5.Admit BETWEEN a.ReportBeginDate AND a.ReportEndDate
       AND det5.BedType = 'ER'
    LEFT JOIN B AS det6
        ON a.MemberID = det6.MemberID
       AND det6.Admit BETWEEN a.ReportBeginDate AND a.ReportEndDate
       AND det6.BedType = 'ER-IP Acute'
    LEFT JOIN B AS det7
        ON a.MemberID = det7.MemberID
       AND det7.Admit BETWEEN a.ReportBeginDate AND a.ReportEndDate
       AND det7.BedType = 'ER-IP BH Acute'
    LEFT JOIN B AS det8
        ON a.MemberID = det8.MemberID
       AND det8.Admit BETWEEN a.ReportBeginDate AND a.ReportEndDate
       AND det8.BedType = 'ER-IP Rehab'
    LEFT JOIN B AS det9
        ON a.MemberID = det9.MemberID
       AND det9.Admit BETWEEN a.ReportBeginDate AND a.ReportEndDate
       AND det9.BedType = 'ER-IP SNF'
    LEFT JOIN B AS det10
        ON a.MemberID = det10.MemberID
       AND det10.Admit BETWEEN a.ReportBeginDate AND a.ReportEndDate
       AND det10.BedType = 'ER-OBS'
    LEFT JOIN B AS det11
        ON a.MemberID = det11.MemberID
       AND det11.Admit BETWEEN a.ReportBeginDate AND a.ReportEndDate
       AND det11.BedType = 'LTAC'
    LEFT JOIN B AS det12
        ON a.MemberID = det12.MemberID
       AND det12.Admit BETWEEN a.ReportBeginDate AND a.ReportEndDate
       AND det12.BedType = 'OBS'
    LEFT JOIN B AS det13
        ON a.MemberID = det13.MemberID
       AND det13.Admit BETWEEN a.ReportBeginDate AND a.ReportEndDate
       AND det13.BedType = 'Rehab'
    LEFT JOIN B AS det14
        ON a.MemberID = det14.MemberID
       AND det14.Admit BETWEEN a.ReportBeginDate AND a.ReportEndDate
       AND det14.BedType = 'SNF'
    LEFT JOIN (
        SELECT
              cms.MemberID
            , cms.AdmissionDate - EXTRACT(DAY FROM cms.AdmissionDate) + 1 AS AdmissionDate
            , SUM(cms.Readmits_Eligible) AS Eligible
            , SUM(cms.AllCause_Readmit) AS AllCause
            , SUM(cms.AllCause_NonElective_readmit) AS AllCauseNonElective
            , SUM(cms.RelatedCause_readmit) AS RelatedCause
            , SUM(cms.RelatedCause_NonElective_readmit) AS RelatedCauseNonElective
            , SUM(cms.NonRelatedCause_readmit) AS NonRelatedCause
            , SUM(cms.NonRelatedCause_NonElective_readmit) AS NonRelatedNonElective
        FROM BISDM_CA_BASE_PRD.MSO_Core_CMS_Readmits AS cms
        GROUP BY 1, 2
    ) AS cms
        ON a.MemberID = cms.MemberID
       AND cms.AdmissionDate BETWEEN a.ReportBeginDate AND a.ReportEndDate
    LEFT JOIN (
        SELECT
              hed.MemberID
            , hed.Admit - EXTRACT(DAY FROM hed.Admit) + 1 AS Admit
            , SUM(hed.ReadmitIndex) AS HEDISReadmits
        FROM BISDM_CA_BASE_PRD.MSO_Core_HEDIS_Readmits AS hed
        WHERE 1 = 1
          AND hed.ReadmitIndex = 1
          AND hed.Age65Discharge = 1
          AND hed.ContEnrol1 = 1
        GROUP BY 1, 2
    ) AS hed
        ON a.MemberID = hed.MemberID
       AND hed.Admit BETWEEN a.ReportBeginDate AND a.ReportEndDate
    LEFT JOIN (
        SELECT
              r.ReportDate
            , r.MemberID
            , SUM(r.Readmits) AS Readmits
            , SUM(r.ReadmitDenominator) AS ReadmitDenominator
        FROM readmits AS r
        GROUP BY 1, 2
    ) AS r
        ON r.MemberID = a.MemberID
       AND r.ReportDate BETWEEN a.ReportBeginDate AND a.ReportEndDate
) WITH DATA;
