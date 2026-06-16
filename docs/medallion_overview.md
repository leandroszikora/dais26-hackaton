# Medical Desert Planner — Medallion Architecture Overview

How the data layer is built, with focus on the **Gold** schema. Track 2 (Medical Desert
Planner) for the Virtue Foundation dataset.

## Bronze → Silver → Gold

All layers live in the writable **`workspace`** catalog (the source catalog is read-only
Delta Sharing). One idempotent, re-runnable notebook (`vf_medallion_etl`) rebuilds the whole
pipeline top-to-bottom on the SQL warehouse.

| layer | schema | role |
|---|---|---|
| **Bronze** | `vf_bronze` | Raw snapshots of the 3 source tables (`facilities`, `india_post_pincode_directory`, `nfhs_5_district_health_indicators`) + the uploaded **Census 2011** population CSV. No logic — just `SELECT *`. |
| **Silver** | `vf_silver` | The cleaning every downstream table depends on (see below). |
| **Gold** | `vf_gold` | Curated, app-ready tables that encode the trust model + medical-desert signal. |

### Silver — the cleaning that makes Gold possible
- Treat the strings `'null'`, `'[]'`, `''` as **real NULLs** (raw fields look ~99% populated
  but aren't — this single rule reproduces the challenge's stated coverage).
- Extract 6-digit pincodes from messy `address_zipOrPostcode`.
- **Normalize state names** across all sources (`Maharastra`→`maharashtra`, `NCT of Delhi`→
  `delhi`, `&`→`and`, trim).
- **Resolve district via 3 tiers** — PIN → city+state → post-office name → **98.9% coverage**.
- Build the geo-crosswalk tables (`geo_pincode`, `geo_office`, `geo_city_district`,
  `geo_census`, normalized `nfhs`).

---

## The Gold schema (the focus)

**Core idea:** the app previously scored ~10,000 rows in Python on *every request*. Gold
**precomputes all of it**, replicating the app's exact scoring logic, so a request becomes a
single filtered `SELECT`.

| table | grain | what it precomputes |
|---|---|---|
| **`facility_capability`** | facility × capability (80,704) | trust signal (`STRONG`/`PARTIAL`/`WEAK`/`NO CLAIM`), sub-scores, and **`evidence_snippet`** — the citation |
| **`district_capability`** | district × capability (4,504) | **Track 2 headline** — coverage vs NFHS demand, `gap_risk`, `scarcity_pct`, confidence, priority |
| **`state_capability`** | state × capability | same signals, rolled up |
| **`facility_directory`** | facility | profile + `completeness_rating` (0–5) |
| **`field_coverage`** | — | true field coverage, for honest uncertainty messaging |

### Why Gold is the centerpiece

**1. It encodes the trust model, not just data.**
`facility_capability` runs the specialty + keyword scoring → a trust level **with an evidence
citation**. `district_capability` then aggregates that into the medical-desert signal, and is
where the **gap-risk model** lives:

```
gap_risk = unmet_need_score × confidence
  unmet_need_score (severity) = need (NFHS demand) × (1 − coverage)
  confidence = sample size + evidence quality + district-vs-state NFHS match
```

So thin evidence (a 1-facility `DATA-POOR` district) can no longer masquerade as a confirmed
gap — confidence discounts it. `unmet_need_score` is kept separately so the UI can still say
"this *could* be a gap, but we're not sure."

**2. It fuses three sources into one decision surface.**
Facility evidence (**supply**) × NFHS indicators (**demand**) × Census population
(**scarcity**), joined through the Silver geo-crosswalk. The planner sees coverage, demand,
and per-capita density (`scarcity_pct`, ~uncorrelated with coverage) side by side — a true
desert scores high on both axes.

---

## Lineage at a glance
```
SOURCE (Delta Sharing, read-only)        ENRICHMENT
  facilities                              Census 2011 CSV
  india_post_pincode_directory                 │
  nfhs_5_district_health_indicators            │
        │                                       │
        ▼                                       ▼
  vf_bronze.*  (raw snapshots) ───────────► vf_bronze.census_2011_district
        │
        ▼
  vf_silver.*  (NULL cleanup, pincode/state normalize, 3-tier district, geo crosswalk)
        │
        ▼
  vf_gold.*    facility_capability ─► district_capability / state_capability
               facility_directory      field_coverage
        │
        ▼
  app_simple.py  (single filtered SELECT per request)
```

**Pipeline:** `vf_medallion_etl` (notebook) · **Schema reference:** `vf_gold_schema_README`
· **App migration:** `vf_app_migration_guide`.
