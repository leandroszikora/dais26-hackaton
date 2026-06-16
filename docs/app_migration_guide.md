# Migration Guide: point `app_simple.py` at the new Gold schema

**Audience:** the coding agent maintaining `app_simple.py` (Medical Desert Planner, Track 2).
**Goal:** replace the app's per-request runtime computation (capability scoring + geo
resolution + NFHS aggregation over ~10k rows) with reads from the precomputed
`workspace.vf_gold.*` tables. **The gold tables were built to mirror `app_simple.py`'s
exact logic**, so outputs match — this is a performance + maintainability swap, not a
behavior change.

## Ground rules
- **Do NOT change the scenario-persistence path.** Saving/loading planner scenarios,
  notes, overrides, and shortlists stays exactly as-is (its own table, e.g.
  `workspace.medical_desert.user_scenarios` or `main.default.*`). Gold tables are
  **read-only inputs**; never write to `vf_gold`.
- Keep the existing `_query()` helper and warehouse wiring — only the SQL/table names change.
- Capability values are identical to the app's: `ICU, Maternity, Emergency, Dialysis,
  Oncology, Cardiology, Pediatrics, Surgery`. No remapping needed.

## Why migrate
The app currently runs `load_data()` + `score_facility()` for every facility×capability
and aggregates in Python on each request. All of that is now precomputed and stored.
A request becomes a single filtered `SELECT`.

---

## Gold tables (read-only)

Catalog/schema: **`workspace.vf_gold`**. Same SQL warehouse the app already uses.

### 1. `district_capability` — headline ranked table (district level)
Grain: one row per **(district, state_norm, capability)**.

| column | type | use |
|---|---|---|
| `district`, `state_norm`, `capability` | string | grain / filters |
| `facility_count` | long | n facilities |
| `strong_count`, `partial_count`, `weak_count`, `no_claim_count` | long | trust breakdown |
| `coverage_pct` | dec | trust-weighted coverage (strong+0.5·partial+0.25·weak)/n·100 |
| `has_evidence` | bool | any strong/partial/weak |
| `nfhs_matched` | bool | district matched NFHS (else demand imputed from state avg) |
| `nfhs_value`, `nfhs_field` | dbl/str | raw NFHS indicator + its label |
| `need_district`, `need_state`, `need_score` | dbl | demand 0–100; `need_score`=coalesce(district, state) |
| `need_imputed` | bool | true → demand came from state avg (show as lower confidence) |
| `priority` | string | `DATA-POOR` / `CRITICAL GAP` / `HIGH PRIORITY` / `MODERATE` / `ADEQUATE` |
| `confidence_pct` | dec | 0–100 |
| `margin_of_error_pct` | dec | ± |
| **`unmet_need_score`** | dbl | severity = (1−coverage)·need·100 (**== the app's old `risk`**) |
| **`gap_risk`** | dbl | **recommended headline** = unmet_need·confidence·100 (confidence-discounted) |
| `risk_score_legacy` | dbl | old v1 (flat need=50 fallback); ignore unless comparing |
| `population`, `population_matched` | long/bool | Census 2011 (75% match) |
| `facilities_per_100k` | dbl | density |
| **`scarcity_pct`** | dec | 0–100, 100=scarcest per-capita; **second axis**, ~uncorrelated with coverage |

### 2. `state_capability` — same as above at state level
Grain: **(state_norm, capability)**. Same columns minus the district/population/scarcity
set: `facility_count, strong/partial/weak/no_claim_count, coverage_pct, has_evidence,
nfhs_value, need_score, nfhs_field, priority, confidence_pct, unmet_need_score, gap_risk,
risk_score_legacy`.

### 3. `facility_capability` — drill-down + citations
Grain: one row per **(facility, capability)** (80,704 rows). Use to expand a region into
its facilities and show evidence.

`facility_id, facility_name, facility_type, city, state_raw, state_norm, district,
district_source, district_state_norm, pincode, latitude, longitude, coords_valid,
capability, spec_score, n_field_hits, text_score, quality_score, total_score,
trust_level (STRONG/PARTIAL/WEAK/NO CLAIM), has_evidence,`
**`evidence_fields`** (comma list of fields that matched: capability/procedure/equipment/description),
**`evidence_snippet`** (±55-char source-text window — the **citation** to display).

### 4. `facility_directory` — per-facility profile
`facility_id, facility_name, facility_type, operator_type, city, state_raw, state_norm,
district, district_source, pincode, latitude, longitude, coords_valid, number_doctors,
capacity, year_established, officialPhone, officialWebsite,` **`completeness_rating`** (0–5,
== app's `fac_rating`).

### 5. `field_coverage` — honesty/uncertainty banner
`field, coverage_pct`. Use for "X% of facilities have this field" messaging.

---

## Function-by-function replacement map

| Remove / stop computing in app | Replace with |
|---|---|
| `load_data()` (loads 10k facilities + builds pin/office/city/NFHS indexes) | nothing — gold is precomputed; delete the heavy load + caches or keep only for scenario features |
| `_district_of()`, `get_nfhs()` (3-tier geo + NFHS lookup) | already applied: use `district`, `state_norm`, `nfhs_*` columns |
| `score_facility(row, cap)` → level/score/evidence | `SELECT trust_level, spec_score, text_score, quality_score, total_score, evidence_fields, evidence_snippet FROM facility_capability WHERE ...` |
| `coverage_pct()`, `demand_score()`, `priority_label()`, `confidence_pct()`, `margin_of_error()` per region | columns `coverage_pct, need_score, priority, confidence_pct, margin_of_error_pct, unmet_need_score, gap_risk` in `district_capability`/`state_capability` |
| `load_fac_dir()` + `fac_rating()` | `facility_directory` (incl. `completeness_rating`) |
| `region_key()` for state/district | filter/group by `state_norm` / `district` |

### Term mapping (important)
- App's old **`risk`** (from `priority_label`) == gold **`unmet_need_score`**. If you want
  byte-for-byte parity, use `unmet_need_score`. **Recommended:** switch the headline to
  **`gap_risk`** (same severity, discounted by confidence so a 1-facility "DATA-POOR"
  district no longer scores like a confirmed gap).
- App's `level` (STRONG/PARTIAL/WEAK/NO CLAIM) == gold `trust_level` (identical strings).
- App's `priority` labels == gold `priority` (identical strings).
- New signal with no app equivalent: **`scarcity_pct`** / `facilities_per_100k` — render as
  a *second axis* next to gap_risk (a true desert is high on both).

---

## Ready-to-use queries (parametrize `:cap`, `:state`, `:district`)

**Ranked districts for a capability in a state** (headline view):
```sql
SELECT district, facility_count, coverage_pct, need_score, need_imputed,
       unmet_need_score, gap_risk, scarcity_pct, facilities_per_100k,
       confidence_pct, margin_of_error_pct, priority,
       strong_count, partial_count, weak_count, no_claim_count
FROM workspace.vf_gold.district_capability
WHERE capability = :cap AND state_norm = :state
ORDER BY gap_risk DESC;
```

**Ranked states for a capability** (national view): same from `state_capability`,
`WHERE capability = :cap ORDER BY gap_risk DESC`.

**Drill into the facilities behind a district aggregate, with citations:**
```sql
SELECT facility_name, facility_type, trust_level, total_score,
       evidence_fields, evidence_snippet, latitude, longitude
FROM workspace.vf_gold.facility_capability
WHERE capability = :cap AND district = :district
ORDER BY (trust_level='STRONG') DESC, total_score DESC;
```

**City or PIN level (NOT precomputed — aggregate `facility_capability` on the fly).**
The per-facility trust is already computed, so this is a light GROUP BY. This snippet
reproduces `coverage_pct`, `priority`, and `confidence_pct` exactly — change the grain
column (`city` or `pincode`):
```sql
WITH agg AS (
  SELECT city AS region, state_norm,
         COUNT(*) n,
         SUM(CASE WHEN trust_level='STRONG'  THEN 1 ELSE 0 END) strong,
         SUM(CASE WHEN trust_level='PARTIAL' THEN 1 ELSE 0 END) partial,
         SUM(CASE WHEN trust_level='WEAK'    THEN 1 ELSE 0 END) weak
  FROM workspace.vf_gold.facility_capability
  WHERE capability = :cap AND state_norm = :state
  GROUP BY city, state_norm
)
SELECT region, n,
  round((strong + partial*0.5 + weak*0.25)/n*100,1) AS coverage_pct,
  CASE
    WHEN n<3 AND (strong+partial+weak)=0 THEN 'DATA-POOR'
    WHEN round((strong+partial*0.5+weak*0.25)/n*100,1)<20 AND n>=3 THEN 'CRITICAL GAP'
    WHEN round((strong+partial*0.5+weak*0.25)/n*100,1)<40 THEN 'HIGH PRIORITY'
    WHEN round((strong+partial*0.5+weak*0.25)/n*100,1)<65 THEN 'MODERATE'
    ELSE 'ADEQUATE' END AS priority,
  least(round(least(n/20.0,1.0)*40 + (strong+partial*0.5)/n*40 + 8),100) AS confidence_pct
FROM agg ORDER BY coverage_pct ASC;
```
> Note: city/PIN have no NFHS demand, so `gap_risk`/`need_score` aren't available at those
> grains; use `coverage_pct` + `priority` there, and reserve gap_risk for district/state.

**Facility directory / profile cards:**
```sql
SELECT * FROM workspace.vf_gold.facility_directory
WHERE state_norm = :state ORDER BY completeness_rating DESC;
```

**Field-coverage banner:**
```sql
SELECT field, coverage_pct FROM workspace.vf_gold.field_coverage ORDER BY coverage_pct DESC;
```

---

## UI / honesty notes (Track 2 requirements still satisfied)
- **Cite evidence:** show `evidence_snippet` (+ `evidence_fields`) on every facility row.
- **Communicate uncertainty:** show `confidence_pct` and `margin_of_error_pct`; treat
  `priority='DATA-POOR'` distinctly (not a confirmed gap). When `need_imputed=true`, label
  demand as "state estimate". `gap_risk` already down-weights low-confidence rows.
- **Two-axis desert view:** plot `gap_risk` (evidence vs demand) against `scarcity_pct`
  (per-capita density). High-high = strongest desert signal.

## Caveats
- `district_capability` only covers rows where `district` resolved (98.9% of facilities).
- `population`/`scarcity_pct` are NULL for ~25% of districts (post-2011 districts &
  the 2014 Telangana/AP split). Guard for NULL; fall back to `gap_risk` alone.
- `state_norm`/`district` are normalized lowercase; display-case as needed (`initcap`).

## Migration checklist
1. Add table constants: `DC = "workspace.vf_gold.district_capability"`, `SC`, `FC =
   "...facility_capability"`, `FD = "...facility_directory"`, `FCOV = "...field_coverage"`.
2. Replace the region-aggregation route to `SELECT ... FROM DC/SC` (queries above).
3. Replace facility drill-down to `SELECT ... FROM FC`.
4. Replace `load_fac_dir()` usage with `FD`.
5. Delete `score_facility`, `_district_of`, `get_nfhs`, `coverage_pct`, `priority_label`,
   `confidence_pct`, `margin_of_error`, and the heavy `load_data()` index-building (keep
   only what scenario persistence needs).
6. Swap the headline metric from old `risk` → `gap_risk` (or `unmet_need_score` for exact
   parity); add `scarcity_pct` as a secondary column.
7. Add city/PIN aggregation snippet for those geo levels.
8. Leave scenario save/load untouched. Smoke-test each geo level + a drill-down.
