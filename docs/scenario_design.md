# Design Spec: Coordinator Wizard → Scenarios → Confidence/Gap-Risk Recompute

**Status:** design for review. **No tables created yet.**
**Track 2 tie-in:** satisfies "persist user actions" + "communicate uncertainty";
lets a coordinator fill missing data to raise *confidence*, and (Phase 2) simulate
deploying a mobile unit.

## 0. Decisions locked in
- **Verification model = "bump `quality_score`"** (no new trust tier). See §4 for the
  consequence: coordinator input raises **confidence** and ranking, **not** `coverage_pct`.
- Build is phased: **Phase 1** = fill/verify → confidence; **Phase 2** = hypothetical
  facilities → what-if simulation.

## 1. Ownership split
- **Wizard (app, colleague):** collects input, `INSERT`s into `vf_user.*`, reads back
  `vf_gold.scenario_*` filtered by `scenario_id`.
- **Data layer (this spec):** `vf_user.*` persistence + a **parameterized recompute view**
  that overlays coordinator input on base evidence. No per-scenario materialized tables —
  one view, `WHERE scenario_id = :id`. A reserved `'baseline'` scenario reproduces today's
  numbers.

## 2. Persistence schema — `workspace.vf_user`
> Aligns with the existing (empty) `workspace.medical_desert.user_scenarios`; extends it
> rather than replacing. Final location TBD with colleague.

### `vf_user.scenarios`
| col | type | notes |
|---|---|---|
| `scenario_id` | string | uuid; `'baseline'` reserved |
| `name` | string | coordinator-given |
| `owner` | string | email/user |
| `context_capability` | string | the capability under study (nullable) |
| `context_state`, `context_district` | string | geographic focus |
| `status` | string | `draft` / `active` / `archived` |
| `parent_scenario_id` | string | for branching ("what-if on top of a saved scenario") |
| `created_at`, `updated_at` | timestamp | |

### `vf_user.facility_field_overrides` — Phase 1 "fill missing info"
Facility-level structured fills (not capability-specific).
| col | type | notes |
|---|---|---|
| `scenario_id`, `facility_id` | string | FK |
| `field` | string | one of `number_doctors`,`capacity`,`year_established`,`official_phone`,`official_website` |
| `new_value` | string | cast on read |
| `note` | string | justification / citation |
| `coordinator` | string | who |
| `created_at` | timestamp | |

### `vf_user.capability_reviews` — Phase 1 "verify / refute"
Per facility × capability human verdict.
| col | type | notes |
|---|---|---|
| `scenario_id`, `facility_id`, `capability` | string | grain |
| `verdict` | string | `CONFIRMED` / `REFUTED` / `UNVERIFIED` |
| `note` | string | **citation** (what the coordinator saw/heard) |
| `coordinator` | string | who |
| `created_at` | timestamp | |

### `vf_user.scenario_facilities` — Phase 2 "hypothetical / mobile unit"
| col | type | notes |
|---|---|---|
| `scenario_id` | string | FK |
| `name` | string | e.g. "Mobile ICU unit A" |
| `facility_kind` | string | `mobile_unit` / `planned_fixed` |
| `district`, `state_norm` | string | placement |
| `latitude`, `longitude` | double | optional |
| `capabilities` | array<string> | declared (from the 8) |
| `capacity`, `number_doctors` | int | declared |
| `note`, `coordinator`, `created_at` | | |

## 3. Wizard field contract (what each step collects)
1. **Pick a facility** (from `facility_directory`, pre-filtered to the gap district).
2. **Fill structured gaps** — show fields where `facility_directory` is null
   (`number_doctors`, `capacity`, `year_established`, phone, website) → `facility_field_overrides`.
3. **Review capabilities** — for each relevant capability show `trust_level` +
   `evidence_snippet`; coordinator picks `CONFIRMED` / `REFUTED` / `UNVERIFIED` + a note →
   `capability_reviews`.
4. **(Phase 2) Add a planned/mobile unit** — declare district, capabilities, capacity →
   `scenario_facilities`.
5. **Save scenario** (name) → `scenarios`.

## 4. Recompute logic (the engine)

### 4a. Per-facility, per-scenario — `vf_gold.scenario_facility_capability` (view)
For a given `scenario_id`, overlay inputs on `silver.facilities`:
- **Filled fields:** `eff_number_doctors = coalesce(override.number_doctors, base.number_doctors)`,
  same for capacity/year/phone/website.
- **`quality_score`** recomputed with effective fields **plus** a verification bonus:
  - `+0.5` if `eff_number_doctors > 0`, `+0.25` if `eff_capacity > 0` (existing rules, now
    fed by overrides)
  - **`+1.0` verification bonus** when `capability_reviews.verdict = 'CONFIRMED'`
  - `reviewed = (verdict IN ('CONFIRMED','REFUTED'))` flag
- **`trust_level`:** unchanged formula (spec_score + keyword hits) **except**
  `verdict='REFUTED'` ⇒ force `'NO CLAIM'` and `has_evidence=false`.
- **`total_score`** rises with quality → improves ranking.

> **⚠ Consequence of the "quality_score only" choice (by design):**
> `coverage_pct` is computed from `trust_level` weights, and `trust_level` is **not**
> affected by `quality_score`. So **CONFIRMED verification raises confidence and ranking
> but does NOT increase `coverage_pct`** (a confirmed-but-text-thin facility stays
> PARTIAL/WEAK). REFUTED *does* lower coverage (it drops to NO CLAIM). If the team later
> wants confirmation to also lift coverage, switch to the **VERIFIED-tier** model — left as
> a one-line future toggle (`CONFIRMED ⇒ trust_level='VERIFIED'`, weight 1.0 in coverage).

### 4b. Per-district, per-scenario — `vf_gold.scenario_district_capability` (view)
Aggregate 4a (base ⊕ overrides ⊕ Phase-2 hypotheticals) by `(scenario_id, district, capability)`.
- `coverage_pct`, `priority`, `unmet_need_score`, `gap_risk`: **same formulas** as
  `vf_gold.district_capability`.
- **`confidence_pct` extended** to make review visible (re-weighted to sum 100):
  ```
  confidence_pct =
      min(n/20, 1) * 30                              -- sample size
    + (strong + 0.5*partial)/n * 30                  -- evidence quality
    + (nfhs_matched ? 15 : 6)                         -- geo resolution
    + avg(quality_score)/2.25 * 10                    -- structured completeness (rises w/ fills)
    + reviewed_fraction * 15                          -- NEW: % of facilities coordinator-reviewed
  ```
  → Filling fields and reviewing facilities both push confidence up; because
  `gap_risk = unmet_need × confidence`, a coordinator who **confirms a district genuinely
  lacks capability** turns a low-confidence guess into a **confident gap** (gap_risk rises);
  one who fills/verifies supply raises confidence in an adequate verdict.

### 4c. Baseline
`scenario_id = 'baseline'` (or no override rows) ⇒ review_fraction=0, no fills ⇒ identical
to current `vf_gold.district_capability` (after the §4b confidence re-weighting; we keep the
old weights for baseline OR re-baseline the gold table — decision below).

## 5. Phase 2 — what-if simulation (mobile unit)
- Coordinator adds a `scenario_facilities` row (mobile ICU, district X, capacity 8).
- Engine scores it (declared capabilities ⇒ strong text/spec equivalents; `reviewed=true`)
  and folds it into 4a/4b for that scenario.
- App shows **before/after**: baseline vs scenario `gap_risk`, `coverage_pct`, `priority`,
  `confidence_pct`, `facilities_per_100k`.
  Example framing: *"Mobile ICU in Sitapur → CRITICAL GAP (49) → MODERATE (18);
  confidence 22 → 55; density 0.02 → 0.04 per 100k."*
- **Honesty caveat:** this is **counterfactual what-if simulation**, not temporal
  *forecasting* — we have no time series. Label it "scenario simulation" in the UI. True
  forecasting would need longitudinal data we don't have.

## 6. Open decisions before building
1. **Schema home:** new `workspace.vf_user` vs extend `workspace.medical_desert.user_scenarios`.
2. **Confidence re-weighting:** apply §4b weights to the *baseline* gold table too (consistent
   but changes current numbers), or keep baseline as-is and use new weights only in scenarios.
3. **Verification strength:** keep "quality only" (confidence moves, coverage doesn't) or adopt
   the VERIFIED-tier toggle so confirmation also moves coverage.
4. **Mobile-unit scoring:** treat declared capabilities as STRONG-equivalent, or as a separate
   "planned" trust level so simulated supply is visually distinct from real evidence.

## 7. Build order when approved
Phase 1: create `vf_user.*` → `scenario_facility_capability` view → `scenario_district_capability`
view → demo an override moving confidence/gap_risk. Phase 2: add `scenario_facilities` to the
two views → before/after query. App wizard writes/reads per §3.
