
# VOP Inpatient Utilization & Hospitalist Performance Dashboard

Plotly Dash application built for the event-level authorization backbone.

## What is included

- Executive KPI summary
- Monthly Acute, OBS, and LTAC utilization
- Correct additive readmission reporting
- Facility and hospitalist-group routing
- Harlingen facility spotlight
- Observation-rate heatmap with low-volume masking
- Provider drilldown with selected-period reliability
- 95th-percentile provider-volume indicator
- Consult specialty and consult-rate analysis
- LTAC utilization
- Data-quality and methodology guardrails
- Filtered aggregated CSV export

## Expected dataset grain

One row per authorization event, keyed by `AuthznKey`.

The dashboard is designed around the output of:

`readmission_event_backbone_teradata_v2_ltac_consult.sql`

## Run locally

```bash
cd vop_inpatient_dash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

Open:

`http://127.0.0.1:8050`

## Use the real dataset

Export the final Teradata event table as CSV, Parquet, or Feather, then set:

```bash
export DASH_DATA_PATH="/secure/path/MSO_IP_Event_Readmission_Backbone.parquet"
python app.py
```

The application uses the included synthetic sample only when `DASH_DATA_PATH` is not set.

Parquet is strongly preferred for larger extracts.

## Important metric definitions

- **Observation rate:** `OBS / (OBS + Acute)`. LTAC is excluded.
- **Readmission rate:** `SUM(ReadmitNumerator) / SUM(ReadmitDenominator)`.
- **Consult rate:** stays with a first qualifying consult divided by total stays.
- **Share of identified business:** group volume divided by mapped group volume. `UNMAPPED` is excluded.
- **Provider reliability:** recalculated after dashboard filters.
  - Low: 1–9 stays
  - Moderate: 10–24 stays
  - Reliable: 25+ stays

The thresholds can be changed through environment variables:

```bash
export LOW_VOLUME_MAX=9
export MODERATE_VOLUME_MAX=24
export HEATMAP_MIN_VOLUME=10
```

## Production notes

Do not place database credentials in `app.py`.

For production deployment:

- Put the app behind enterprise SSO.
- Apply user or provider-level authorization.
- Use TLS.
- Keep member identifiers out of browser downloads.
- Prefer an aggregated or de-identified extract for external users.
- Use server-side caching or database-side aggregation for large datasets.

## Main files

- `app.py`: application and callbacks
- `assets/style.css`: dashboard styling
- `data/sample_event_backbone.csv`: synthetic preview data
- `requirements.txt`: dependencies
