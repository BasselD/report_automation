# Streamlit interaction flow for the Cost–Volume Outlier Scatter

## 0. App entry and data readiness

* **On load**

  * Show data freshness (min/max `DOSbegin`, row count, distinct members, distinct claims).
  * Validate required fields exist.
  * Precompute lightweight reference tables (optional, cached):

    * Procedure code list + descriptions
    * Specialty list
    * Market/pod/PCP hierarchy values
    * NetworkStatus list
    * PlaceOfService list

---

## 1. Left sidebar. Filters and controls

### A) Time window

* **Date range picker** on `DOSbegin` (default last 12 months or last full quarter).

### B) Hierarchical filters (top-down)

Use cascading filters so choices constrain downstream lists:

1. `OperationalMarket`
2. `OperationalSubMarket`
3. `ManagingEntity`
4. `ReportingPod` / `PodName`
5. `PCPName`
6. Optional: `ProviderName` or `ProviderNPI`

### C) Clinical / claim filters

* `Specialty` (multi-select)
* `ProcedureCode` (searchable select)

  * show `ProcedureDesc`
  * optional toggle: “Top procedures only” (based on spend/volume in current filter context)
* `NetworkStatus` (All / In-Network / Out-of-Network)
* `PlaceOfService` (multi-select)

### D) Scatter configuration

* **Granularity selector (Group By)**

  * Market, SubMarket, ManagingEntity, Pod, PCP, Rendering Provider, Referring Provider, ProcedureCode
* **Metric mode**

  * Mode 1: “Raw” (Avg Paid, Claim Count, Total Paid)
  * Mode 2: “Benchmark” (Cost Index vs selected benchmark)
* **Axis and encoding**

  * X-axis dropdown (default: `AvgCost` or `CostIndex`)
  * Y-axis dropdown (default: `ClaimCount` or `VolumeIndex`)
  * Bubble size dropdown (default: `TotalPaid`)
  * Color dropdown (default: `NetworkStatus` or `Specialty`)
* **Outlier detection**

  * Method: Z-score / Percentile / Isolation Forest (optional)
  * Threshold slider (e.g., |Z| ≥ 2.0 or percentile ≥ 95th)
  * Toggle: “Show only outliers”

### E) Minimum volume guardrails

* Slider: `Min ClaimCount` (default 20) to reduce noise.
* Toggle: “Winsorize AvgCost” (optional, if you want stability).

---

## 2. Top of main page. KPI strip (context)

Display KPIs for the **current filter state**:

* Total Paid
* Total Allowed
* Claims
* Unique Members
* Avg Paid per Claim
* % Out-of-Network Paid
* Optional: Top procedure by spend

Include a small text line:

* “Grouping by: PCP. Procedure filter: CPT 93000. Date: 2024-01-01 to 2024-12-31.”

---

## 3. Main panel. Scatter plot section

### A) Scatter plot (primary visual)

* Interactive scatter:

  * Hover shows:

    * group label (e.g., PCPName)
    * ClaimCount
    * TotalPaid
    * AvgCost
    * CostIndex (if benchmark mode)
    * Top 3 procedures (optional, if grouping above procedure level)
    * OON spend and %
  * Lasso/box select to pick multiple points.
  * Click a point to “pin” it for drilldown.

### B) Reference lines / quadrants (recommended)

* Add benchmark lines:

  * Vertical line at CostIndex = 1.0 (or AvgCost overall median)
  * Horizontal line at median volume (or VolumeIndex = 1.0)
* Label quadrants:

  * “High Cost. High Volume (priority)”
  * “High Cost. Low Volume”
  * “Low Cost. High Volume (benchmark)”
  * “Low Cost. Low Volume”

### C) Legend / outlier badge

* Badge: “Outliers flagged: 14 (Z ≥ 2.0)”

---

## 4. Below scatter. Opportunity table (auto-generated call to action)

A ranked table that updates with filters and grouping.

**Columns**

* Group (PCP/Provider/etc.)
* ClaimCount
* AvgCost
* CostIndex
* TotalPaid
* Estimated Savings Opportunity
* OON % (optional)
* POS mix (optional)

**Estimated Savings Opportunity (simple and defensible)**

* If benchmark mode:

  * `Opportunity = max(0, AvgCost - BenchmarkAvgCost) * ClaimCount`
* BenchmarkAvgCost can be:

  * Market-level for same ProcedureCode + Specialty
  * Or Pod-level, depending on selection

**User actions per row**

* “Drill into details”
* “Download filtered claims”

---

## 5. Drilldown drawer (right panel or expandable section)

When user clicks a point (or a row):

### A) Detail summary

* Selected entity metadata (PCP, NPI, Pod, Market, Specialty)
* Top procedures by spend within selection
* Network split (IN vs OON)
* POS distribution

### B) Mini charts (fast, high value)

* Bar: Top 10 ProcedureCodes by spend (within entity)
* Bar: POS distribution for selected CPT(s)
* Trend: Monthly spend (optional)

### C) Detailed claims table (preview)

Show first N rows with:

* MemberID, memberFirst, memberLast (if allowed), gender
* ClaimID, DOSbegin
* PrimaryDx / Desc
* ProcedureCode / Desc
* TotalPaid, TotalAllowed
* ProviderName, ProviderNPI
* ReferringProviderName
* NetworkStatus, PlaceOfServiceDesc

---

## 6. Download center (two-tier export)

Two download buttons, always visible when results exist:

1. **Download Summary**

   * Exports the opportunity table (grouped metrics + outlier flags).
   * File name includes date range + group-by.

2. **Download Detailed Claims**

   * Exports the filtered claim-level table using the same filters plus:

     * Selected points (if any selection)
     * Or “all claims in current view”
   * Offer formats:

     * CSV (default)
     * Parquet (optional for power users)

Add a confirmation note:

* “Export includes 128,412 rows. Consider filtering by procedure or increasing min volume.”

---

## 7. Power-user features (optional, but worth it)

* **Bookmark / Share settings**

  * Save current filter state to a JSON and reload later.
* **Compare mode**

  * Side-by-side scatter: two markets or two time ranges.
* **Explain outlier**

  * Auto-generated text: “High cost driven by POS=Hospital Outpatient and OON share 22%.”

---

## 8. Guardrails and performance

* Cache query results by:

  * date range
  * market filters
  * procedure selection
  * group-by
* Limit scatter points with:

  * Top N by TotalPaid (default 500) + toggle “Show all” if needed
* Enforce `Min ClaimCount` default to reduce false outliers.

---

## Minimal user flow (what the RME does in 20 seconds)

1. Choose Market + time period
2. Pick Specialty and optionally a CPT
3. Set Group By = PCP
4. Click “Show outliers only”
5. Click top outlier bubble
6. Download detailed claims for outreach / review

---

