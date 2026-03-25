WITH base AS
(
    SELECT
          Market
        , SubMarket
        , ManagingEntity
        , ReportingPod
        , PCPName
        , PCPNPI
        , MemberName
        , MemberID
        , MemberDOB
        , HealthPlanName
        , AdherenceMeasureTag
        , LastFillMedicationName
        , LastDaysSupply
        , LastPharmacyName
        , LastPharmacyPhone
        , TotalUniqueFillCount
        , CurrentPDC
        , DaysLatetoRefill
        , RefillGapScore
        , WeightedAvgHistPDC
        , PDCTrajectoryScore
        , HighSpendNonSubsidyFlag
        , HighReversalsFlag
        , DementiaDiagnosisFlag
        , AdherenceBarriersCount
        , DaysUntilUnrecoverable
        , AdherenceBarrierScore
        , NonAdherenceRiskTier
        , MedicationRunoutDate
        , MedicationRunoutDays
        , Prior2YearPDC
        , Prior1YearPDC
        , NonAdherenceRiskModelFlag
        , NonAdherenceRiskModelReason
        , RecommendedActionBullet1
        , RecommendedActionBullet2
        , RecommendedActionTag1
        , RecommendedActionTag2
        , RecommendedActionTag3
        , RecommendedActionTags
        , HighPerformerPodExclusion
        , MultiFillsUnrecoverableExclusion
        , GracePeriodExclusion
        , OneFill2WeeksPriorExclusion
        , DoNotIncludeMemberExclusion
        , DoNotIncludeOpportunityExclusion
        , MedicationAdherenceInbox
        , MedicationAdherenceFax
        , HighLevelMATExclusion
        , ReportAnchorDate
    FROM your_database.your_table
    WHERE 1 = 1
      -- Optional filters. Uncomment if these should be excluded from Excel output.
      -- AND COALESCE(DoNotIncludeMemberExclusion, 0) = 0
      -- AND COALESCE(DoNotIncludeOpportunityExclusion, 0) = 0
      -- AND COALESCE(HighLevelMATExclusion, 0) = 0
),

fmt AS
(
    SELECT
          b.*

        /* PDC values normalized to percentages */
        , CAST(
            CASE
                WHEN b.CurrentPDC IS NULL THEN NULL
                WHEN b.CurrentPDC <= 1 THEN b.CurrentPDC * 100
                ELSE b.CurrentPDC
            END
          AS DECIMAL(5,0)) AS CurrentPDC_Pct

        , CAST(
            CASE
                WHEN b.Prior1YearPDC IS NULL THEN NULL
                WHEN b.Prior1YearPDC <= 1 THEN b.Prior1YearPDC * 100
                ELSE b.Prior1YearPDC
            END
          AS DECIMAL(5,0)) AS Prior1YearPDC_Pct

        , CAST(
            CASE
                WHEN b.Prior2YearPDC IS NULL THEN NULL
                WHEN b.Prior2YearPDC <= 1 THEN b.Prior2YearPDC * 100
                ELSE b.Prior2YearPDC
            END
          AS DECIMAL(5,0)) AS Prior2YearPDC_Pct

        /* MM/DD DOB */
        , CASE
            WHEN b.MemberDOB IS NOT NULL THEN
                LPAD(TRIM(CAST(EXTRACT(MONTH FROM b.MemberDOB) AS VARCHAR(2))), 2, '0')
                || '/'
                || LPAD(TRIM(CAST(EXTRACT(DAY FROM b.MemberDOB) AS VARCHAR(2))), 2, '0')
            ELSE NULL
          END AS MemberDOB_MMDD

        /* MM/DD due date */
        , CASE
            WHEN b.MedicationRunoutDate IS NOT NULL THEN
                LPAD(TRIM(CAST(EXTRACT(MONTH FROM b.MedicationRunoutDate) AS VARCHAR(2))), 2, '0')
                || '/'
                || LPAD(TRIM(CAST(EXTRACT(DAY FROM b.MedicationRunoutDate) AS VARCHAR(2))), 2, '0')
            ELSE NULL
          END AS DueDate_MMDD

        /* report year labels */
        , CAST(EXTRACT(YEAR FROM b.ReportAnchorDate) - 2 AS VARCHAR(4)) AS HistYear_1
        , CAST(EXTRACT(YEAR FROM b.ReportAnchorDate) - 1 AS VARCHAR(4)) AS HistYear_2
        , CAST(EXTRACT(YEAR FROM b.ReportAnchorDate)     AS VARCHAR(4)) AS HistYear_3

        /* fallback tag string if RecommendedActionTags is blank */
        , CASE
            WHEN NULLIF(TRIM(b.RecommendedActionTags), '') IS NOT NULL THEN TRIM(b.RecommendedActionTags)
            ELSE
                COALESCE(TRIM(b.RecommendedActionTag1), '')
                ||
                CASE
                    WHEN NULLIF(TRIM(b.RecommendedActionTag2), '') IS NOT NULL
                    THEN
                        CASE
                            WHEN NULLIF(TRIM(b.RecommendedActionTag1), '') IS NOT NULL THEN ' | '
                            ELSE ''
                        END
                        || TRIM(b.RecommendedActionTag2)
                    ELSE ''
                END
                ||
                CASE
                    WHEN NULLIF(TRIM(b.RecommendedActionTag3), '') IS NOT NULL
                    THEN
                        CASE
                            WHEN NULLIF(TRIM(b.RecommendedActionTag1), '') IS NOT NULL
                              OR NULLIF(TRIM(b.RecommendedActionTag2), '') IS NOT NULL
                            THEN ' | '
                            ELSE ''
                        END
                        || TRIM(b.RecommendedActionTag3)
                    ELSE ''
                END
          END AS ActionTags_Display

    FROM base b
)

SELECT
      Market
    , SubMarket
    , ManagingEntity
    , ReportingPod
    , PCPName
    , PCPNPI
    , MemberID
    , MemberName
    , NonAdherenceRiskTier AS Tier

    /* Excel table column 1. Patient */
    , TRIM(COALESCE(MemberName, ''))
      || CHR(10) ||
      '(' || TRIM(COALESCE(CAST(MemberID AS VARCHAR(50)), '')) || ')'
      || CHR(10) ||
      'DOB: ' || COALESCE(MemberDOB_MMDD, '')
      || CHR(10) ||
      'Plan: ' || COALESCE(TRIM(HealthPlanName), '')
      AS Patient_Display

    /* Excel table column 2. Measure / Medication */
    , COALESCE(TRIM(AdherenceMeasureTag), '')
      || CHR(10) ||
      COALESCE(TRIM(LastFillMedicationName), '')
      || CHR(10) ||
      COALESCE(TRIM(LastPharmacyName), '')
      AS Measure_Medication_Display

    /* Excel table column 3. Status */
    , 'PDC: ' || COALESCE(TRIM(CAST(CurrentPDC_Pct AS VARCHAR(5))), '0') || '%'
      || CHR(10) ||
      'Fills: ' || COALESCE(TRIM(CAST(TotalUniqueFillCount AS VARCHAR(10))), '0')
      || CHR(10) ||
      'Late: ' || COALESCE(TRIM(CAST(DaysLatetoRefill AS VARCHAR(10))), '0')
      || ' | Due: ' || COALESCE(DueDate_MMDD, '')
      || CHR(10) ||
      'Recoverable in ' || COALESCE(TRIM(CAST(DaysUntilUnrecoverable AS VARCHAR(10))), '0') || 'd'
      AS Status_Display

    /* Excel table column 4. Recommended Action */
    , COALESCE('- ' || TRIM(RecommendedActionBullet1), '')
      || CHR(10) ||
      COALESCE('- ' || TRIM(RecommendedActionBullet2), '')
      || CHR(10) ||
      COALESCE(ActionTags_Display, '')
      AS Recommended_Action_Display

    /* Excel table column 5. Adherence History. 3 lines only, year + percent */
    , HistYear_1 || ': ' || COALESCE(TRIM(CAST(Prior2YearPDC_Pct AS VARCHAR(5))), '0') || '%'
      || CHR(10) ||
      HistYear_2 || ': ' || COALESCE(TRIM(CAST(Prior1YearPDC_Pct AS VARCHAR(5))), '0') || '%'
      || CHR(10) ||
      HistYear_3 || ': ' || COALESCE(TRIM(CAST(CurrentPDC_Pct AS VARCHAR(5))), '0') || '%'
      AS Adherence_History_Display

    /* Keep useful raw fields too */
    , CurrentPDC
    , Prior1YearPDC
    , Prior2YearPDC
    , TotalUniqueFillCount
    , DaysLatetoRefill
    , DaysUntilUnrecoverable
    , MedicationRunoutDate
    , RecommendedActionBullet1
    , RecommendedActionBullet2
    , RecommendedActionTags
    , ReportAnchorDate

FROM fmt

ORDER BY
      Market
    , SubMarket
    , ManagingEntity
    , ReportingPod
    , PCPName
    , PCPNPI
    , CASE UPPER(NonAdherenceRiskTier)
          WHEN 'HIGH' THEN 1
          WHEN 'MED'  THEN 2
          WHEN 'LOW'  THEN 3
          ELSE 9
      END
    , CurrentPDC_Pct ASC
    , MemberName;
