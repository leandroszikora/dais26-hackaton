# DAIS '26 Hackathon — Medical Desert Planner (Track 2)

A Databricks App that turns 10,000 messy Indian healthcare-facility records into decisions a
non-technical planner can trust.

> **Question (Track 2):** *Where are the highest-risk gaps in care, and how confident are we
> that those gaps are real?*

The app distinguishes **real care gaps** (many facilities, little trustworthy evidence of a
capability) from **data-poor regions** (too few records to judge), citing the underlying
facility text and communicating uncertainty honestly.

## Repository layout

```
.
├── medical-desert-planner/         # the Databricks App (Flask)
│   ├── app_simple.py               #   ← live app (see app.yaml)
│   ├── app.py / app_v2.py          #   earlier iterations
│   └── app.yaml, requirements.txt
├── notebooks/
│   └── vf_medallion_etl.sql        # data layer: Bronze→Silver→Gold pipeline (SQL notebook)
├── Medical Desert Planner Data Pipeline.ipynb   # earlier PySpark profiling/pipeline
├── docs/
│   ├── medallion_overview.md       # architecture explainer (start here)
│   ├── gold_schema_reference.md    # column-level data dictionary for vf_gold.*
│   ├── app_migration_guide.md      # how the app reads the gold schema
│   └── scenario_design.md          # coordinator wizard → scenario / what-if spec
├── data/
│   └── india_census_2011.csv       # population enrichment (Census 2011, 640 districts)
└── tools/
    ├── etl.py                      # generator: emits the notebook + runs it on a warehouse
    └── sql.py                      # SQL-over-warehouse helper
```

## Data layer — medallion architecture

**Bronze** (raw snapshots + census) → **Silver** (NULL cleanup, pincode/state normalization,
3-tier district resolution, geo crosswalk) → **Gold** (curated facility + aggregated
district/state tables). Everything lives in the writable `workspace` catalog; the source is
read-only Delta Sharing. The pipeline (`notebooks/vf_medallion_etl.sql`) is idempotent
(`CREATE OR REPLACE`) and **precomputes the trust scoring the app used to do per-request over
10k rows**, so a request becomes a single filtered `SELECT`.

### Gold schema (what the app queries)

| table | grain | purpose |
|---|---|---|
| `vf_gold.facility_capability` | facility × capability | trust signal + **evidence citation** |
| `vf_gold.district_capability` | district × capability | **headline** — coverage vs NFHS demand, `gap_risk`, `scarcity_pct`, confidence, priority |
| `vf_gold.state_capability` | state × capability | state rollup |
| `vf_gold.facility_directory` | facility | profile + completeness rating |
| `vf_gold.field_coverage` | — | true field coverage for uncertainty messaging |

**Gap-risk model (confidence-honest):**
```
gap_risk = unmet_need_score × confidence
  unmet_need_score = NFHS demand × (1 − trust-weighted coverage)
```
so a thin-evidence `DATA-POOR` district can't masquerade as a confirmed gap. `scarcity_pct`
(per-capita facility density from Census population) is an orthogonal second axis — a true
desert scores high on both.

## Running it

**Data pipeline**
1. Import `notebooks/vf_medallion_etl.sql` into Databricks.
2. Create a volume and upload the census file:
   `/Volumes/workspace/vf_bronze/files/india_census_2011.csv` (from `data/`).
3. Attach a SQL warehouse and **Run all** (safe to re-run — idempotent).
   `tools/etl.py` can regenerate the notebook (`python3 etl.py emit <path>`) or run the whole
   pipeline against a warehouse (`python3 etl.py run`).

**App** — deploy `medical-desert-planner/` as a Databricks App (entrypoint per `app.yaml`).
See `docs/app_migration_guide.md` to point it at the Gold schema.

## Sources
- Virtue Foundation facility dataset (`databricks_virtue_foundation_dataset_dais_2026`)
- NFHS-5 district health indicators (demand signal)
- India Post pincode directory (district resolution)
- Census 2011 district population (scarcity enrichment)
