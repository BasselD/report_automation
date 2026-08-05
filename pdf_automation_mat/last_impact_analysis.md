Yes. **The calculations above do account for both:**

1. **`LastFillAdjusted`**
2. **Extra medication on hand after `PDCCalculationDate` / report date**

But the key is **where** they are accounted for.

---

## Where `LastFillAdjusted` Is Used

This line accounts for overlap-adjusted prescription timing:

```sql
MedicationRunoutDate =
CAST(PDC.LastFillAdjusted AS DATE)
    + CAST(RNKR.LastDS AS INT)
    - 1
```

Meaning:

> Start from the adjusted fill date after overlap logic, then add the day supply to determine when the member actually runs out.

Example:

```text
LastFillDateAdjusted = 05/26/2026
LastDaysSupply = 30

MedicationRunoutDate = 06/24/2026
```

So yes, **overlap is accounted for as long as `PDC.LastFillAdjusted` is truly adjusted for overlap.**

---

## Where Extra Medication on Hand Is Accounted For

This line calculates how many future covered days the member already has after the report date:

```sql
MedicationRunoutDays =
GREATEST(MedicationRunoutDate - PDCCalculationDate, 0)
```

Example:

```text
PDCCalculationDate = 06/01/2026
MedicationRunoutDate = 06/24/2026

MedicationRunoutDays = 23
```

That means the member has **23 future covered days already on hand**.

Then we calculate:

```sql
CoveredDaysWithRunout =
CurrentNumerator + MedicationRunoutDays
```

So yes, this explicitly accounts for medication supply beyond the report date.

---

## Where It Affects Last Impact Date

The most important part is this:

```sql
CoverageGapStartDate =
GREATEST(
    PDCCalculationDate,
    MedicationRunoutDate + 1
)
```

This prevents the countdown to unrecoverable from starting too early.

### If the patient is already overdue

```text
MedicationRunoutDate < PDCCalculationDate
CoverageGapStartDate = PDCCalculationDate
```

The countdown starts now.

### If the patient still has medication on hand

```text
MedicationRunoutDate > PDCCalculationDate
CoverageGapStartDate = MedicationRunoutDate + 1
```

The countdown starts **after the medication runs out**.

That is the right behavior.

---

## Final Last Impact Date Logic

```sql
LastImpactDate =
CASE
    WHEN RecoverableFlag = 0 THEN NULL
    ELSE CoverageGapStartDate + RemainingMissedDayBudget
END
```

So yes, the Last Impact Date is pushed later when the patient still has medication on hand.

---

## Important Clarification

The `LastImpactDate` means:

> The date the member must resume continuous medication coverage to avoid becoming unrecoverable.

It does **not** mean the last uncovered day.

Example:

```text
CoverageGapStartDate = 06/25
RemainingMissedDayBudget = 10

LastImpactDate = 07/05
```

Meaning the member can miss 10 additional days, then must be back on coverage by **07/05**.

---

## Bottom Line

Yes, the formulas account for medication on hand **if**:

```sql
PDC.LastFillAdjusted
```

is truly the **overlap-adjusted fill start date**.

Then:

```sql
MedicationRunoutDate = LastFillDateAdjusted + LastDaysSupply - 1
```

and:

```sql
CoverageGapStartDate = GREATEST(PDCCalculationDate, MedicationRunoutDate + 1)
```

are the two key lines that make the logic work.
