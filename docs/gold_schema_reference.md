# Medical Desert Planner — Gold Schema (Track 2)

Medallion ETL output for the Virtue Foundation dataset. All logic mirrors
`app_simple.py` so the app can read precomputed tables instead of scoring 10k
rows per request.

- **Sources:** `databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset` (read-only) + **Census 2011 district population** (uploaded CSV → `/Volumes/workspace/vf_bronze/files/india_census_2011.csv`, 640 districts, ~1.21B).
- **Pipeline notebook:** `/Workspace/Users/leandroszikora@hotmail.com/vf_medallion_etl` (idempotent, re-runnable on a SQL warehouse). Re-upload the census CSV to the volume before a clean re-run.
- **Layers:** `workspace.vf_bronze` (raw snapshot + census) → `workspace.vf_silver` (cleaned/normalized) → `workspace.vf_gold` (curated + aggregated)

## Key cleaning rules applied
- Evidence fields (`capability`, `procedure`, `equipment`, `specialties`) are JSON-array strings; literal `'null'`, `'[]'`, `''` are treated as **missing** (NULL).
- 6-digit pincode extracted from `address_zipOrPostcode` via regex.
- **District resolved 3 tiers** (PIN → city+state → post-office name) → 98.9% coverage.
- State names normalized across all sources (`Maharastra`→`maharashtra`, `NCT of Delhi`→`delhi`, `&`→`and`, trim, etc.).
- NFHS district join match rate: **80.6%** of resolved districts; Census population match: **75.3%** (misses are mostly post-2011 districts and the 2014 Telangana/AP split — a documented limitation).

---

## GOLD tables (what the app should query)

### `vf_gold.district_capability` — Track 2 headline
One row per **district × capability** (8 capabilities). Trust-weighted supply vs NFHS demand.
| column | meaning |
|---|---|
| `district`, `state_norm`, `capability` | grain |
| `facility_count` | facilities in district |
| `strong_count` / `partial_count` / `weak_count` / `no_claim_count` | trust-level breakdown |
| `coverage_pct` | trust-weighted coverage = (strong + 0.5·partial + 0.25·weak)/n ·100 |
| `nfhs_matched` | whether district matched NFHS (else demand imputed from state avg) |
| `nfhs_value`, `nfhs_field` | raw NFHS indicator + its label |
| `need_district`, `need_state` | demand from district NFHS / state-avg NFHS |
| `need_score` | demand 0–100 used in risk = `coalesce(need_district, need_state)` |
| `need_imputed` | true when district NFHS missing and state-avg used (no fabricated value) |
| `priority` | `DATA-POOR` / `CRITICAL GAP` / `HIGH PRIORITY` / `MODERATE` / `ADEQUATE` |
| `confidence_pct` | 0–100 (sample size + evidence quality + district vs state NFHS) |
| `margin_of_error_pct` | ±, from sample size |
| **`unmet_need_score`** | severity = (1−coverage)·need ·100 — confidence-independent |
| **`gap_risk`** | **headline** = unmet_need · confidence ·100 — confidence-discounted so thin evidence can't masquerade as a real gap |
| `risk_score_legacy` | old v1 formula (flat need=50 fallback), kept for comparison only |
| `population` / `population_matched` | Census 2011 district population (75% match) |
| `facilities_per_100k` | facility_count / population ·100k |
| **`scarcity_pct`** | 0–100 (100 = scarcest per-capita within capability); orthogonal to coverage — a *second axis* for desert detection |

**Two-axis model (by design):** `gap_risk` measures evidence-of-capability vs health demand, discounted by confidence; `scarcity_pct` measures per-capita facility *density*. They are near-uncorrelated (|corr|<0.15) — plot both. A true medical desert scores high on both.

**The core distinction:** `DATA-POOR` = too few records to judge (not a confirmed gap); `CRITICAL GAP` = many facilities, almost no capability evidence (high-confidence real gap). `gap_risk` operationalizes this: a 1-facility district gets low `gap_risk` even at high `unmet_need_score`, because confidence is low.

### `vf_gold.facility_capability` — evidence & citations
One row per **facility × capability** (80,704 rows). Powers drill-down.
Key cols: `trust_level` (STRONG/PARTIAL/WEAK/NO CLAIM), `spec_score`, `text_score`, `quality_score`, `total_score`, `n_field_hits`, `evidence_fields`, **`evidence_snippet`** (±55-char window of source text — the citation), `has_evidence`, plus geo cols.

### `vf_gold.state_capability`
State-level rollup with state-average NFHS demand. Same `gap_risk` / `unmet_need_score` / `confidence_pct` decomposition.

### `vf_gold.facility_directory`
Per-facility profile + `completeness_rating` (0–5) + resolved `district`/`district_source`.

### `vf_gold.field_coverage`
Dataset field coverage % (true coverage) — for honest uncertainty messaging in the UI.

---

## SILVER (intermediate, reusable)
`facilities` (cleaned + resolved district + `combined_text`), `facilities_base`, `geo_pincode`, `geo_office`, `geo_city_district`, `nfhs` (normalized + 7 demand indicators).

## Example queries
```sql
-- Ranked real gaps for a capability in a state (confidence-discounted)
SELECT district, facility_count, coverage_pct, need_score, unmet_need_score,
       gap_risk, scarcity_pct, facilities_per_100k, priority, confidence_pct
FROM workspace.vf_gold.district_capability
WHERE capability = 'Maternity' AND state_norm = 'bihar'
ORDER BY gap_risk DESC;

-- Drill into the facilities behind an aggregate, with citations
SELECT facility_name, trust_level, evidence_fields, evidence_snippet
FROM workspace.vf_gold.facility_capability
WHERE capability = 'Maternity' AND district = 'NALANDA' AND has_evidence
ORDER BY total_score DESC;
```
