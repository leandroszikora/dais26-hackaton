# Databricks notebook source

# MAGIC %md
# MAGIC # Medical Desert Planner — KPI Logic Notebook
# MAGIC **DAIS 2026 Hackathon Track 2**
# MAGIC
# MAGIC Reproduces every scoring formula from the Medical Desert Planner app.
# MAGIC Each section maps to a KPI visible in the app's results panel.
# MAGIC
# MAGIC **Dataset:** `databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset`
# MAGIC
# MAGIC **How to use:**
# MAGIC 1. Run **Cell 2** to register the capability widget
# MAGIC 2. Pick a capability from the dropdown (top of notebook)
# MAGIC 3. Run **Cell 3** (config) then run remaining cells in order
# MAGIC
# MAGIC ---

# COMMAND ----------

# Register capability selector widget
dbutils.widgets.dropdown(
    "capability", "Dialysis",
    ["ICU", "Maternity", "Emergency", "Dialysis", "Oncology", "Cardiology", "Pediatrics", "Surgery"],
    "Capability"
)

# COMMAND ----------

# ── Table constants ───────────────────────────────────────────────────────────
FAC_TABLE  = "databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.facilities"
NFHS_TABLE = "databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.nfhs_5_district_health_indicators"
PIN_TABLE  = "databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.india_post_pincode_directory"

# ── Per-capability configuration ──────────────────────────────────────────────
# keywords   : text patterns searched across capability/procedure/equipment/description fields
# specialties: structured taxonomy IDs matched against the specialties JSON field
# demand_col : NFHS-5 column that signals unmet need for this capability
# invert     : True = low indicator value means high need (e.g. few institutional births)
CAPABILITY_CONFIG = {
    "ICU": {
        "keywords":   ["icu", "intensive care", "critical care", "ventilator", "icu bed"],
        "specialties": ["criticalCareMedicine", "pulmonologyCriticalCare", "anesthesiaCriticalCare"],
        "demand_col":  "hh_member_covered_health_insurance_pct",
        "invert":      True,
        "demand_label": "Health insurance %",
    },
    "Maternity": {
        "keywords":   ["maternity", "obstetric", "gynecol", "delivery", "labour", "labor",
                       "prenatal", "antenatal", "postnatal", "birth"],
        "specialties": ["gynecologyAndObstetrics", "maternalAndFetalMedicine",
                        "reproductiveEndocrinologyAndInfertility", "neonatologyPerinatalMedicine"],
        "demand_col":  "institutional_birth_5y_pct",
        "invert":      True,
        "demand_label": "Institutional births %",
    },
    "Emergency": {
        "keywords":   ["emergency", "trauma", "ambulance", "casualty", "accident"],
        "specialties": ["emergencyMedicine", "pediatricEmergencyMedicine", "traumaSurgery"],
        "demand_col":  "hh_member_covered_health_insurance_pct",
        "invert":      True,
        "demand_label": "Health insurance %",
    },
    "Dialysis": {
        "keywords":   ["dialysis", "hemodialysis", "renal", "nephrology"],
        "specialties": ["nephrology", "pediatricNephrology"],
        "demand_col":  "all_w15_49_who_are_anaemic_pct",
        "invert":      False,
        "demand_label": "Women anaemia %",
    },
    "Oncology": {
        "keywords":   ["oncology", "cancer", "chemotherapy", "radiation therapy", "radiotherapy", "tumor"],
        "specialties": ["oncology", "radiationOncology", "surgicalOncology",
                        "hematologyOncology", "pediatricHematologyOncology", "gynecologicOncology"],
        "demand_col":  "hh_member_covered_health_insurance_pct",
        "invert":      True,
        "demand_label": "Health insurance %",
    },
    "Cardiology": {
        "keywords":   ["cardiology", "cardiac", "coronary", "angioplasty", "echocardiography"],
        "specialties": ["cardiology", "interventionalCardiology", "pediatricCardiology"],
        "demand_col":  "w15_plus_with_high_bp_sys_gte_140_mmhg_and_or_dia_gte_90_mm_pct",
        "invert":      False,
        "demand_label": "High BP prevalence %",
    },
    "Pediatrics": {
        "keywords":   ["pediatric", "paediatric", "nicu", "neonatal", "children", "child health"],
        "specialties": ["pediatrics", "neonatologyPerinatalMedicine", "pediatricSurgery",
                        "pediatricOrthopedicSurgery", "pediatricEmergencyMedicine",
                        "pediatricNephrology", "pediatricHematologyOncology", "pediatricCardiology"],
        "demand_col":  "prev_diarrhoea_2wk_child_u5_pct",
        "invert":      False,
        "demand_label": "Child diarrhoea %",
    },
    "Surgery": {
        "keywords":   ["surgery", "surgical", "operation theater", "laparoscopic", "general surgery"],
        "specialties": ["generalSurgery", "traumaSurgery", "neurosurgery", "orthopedicSurgery",
                        "plasticSurgery", "urology", "spineNeurosurgery", "jointReconstructionSurgery"],
        "demand_col":  "births_delivered_by_csection_5y_pct",
        "invert":      False,
        "demand_label": "C-section births %",
    },
}

cap        = dbutils.widgets.get("capability")
cfg        = CAPABILITY_CONFIG[cap]
kw_pattern = "|".join(cfg["keywords"])
spec_list  = cfg["specialties"]
demand_col = cfg["demand_col"]
invert     = cfg["invert"]

# Build dynamic CASE expressions for specialty matching
spec_cases = "\n           + ".join(
    f"CASE WHEN specialties_raw LIKE '%\"{s}\"%' THEN 1 ELSE 0 END"
    for s in spec_list
)

# Gap risk demand expression: invert=True means low indicator = high need
demand_expr = (
    f"(100 - COALESCE({demand_col}, 50))"
    if invert else
    f"COALESCE({demand_col}, 50)"
)

print(f"Capability    : {cap}")
print(f"Keywords      : {kw_pattern}")
print(f"Specialties   : {spec_list}")
print(f"Demand column : {demand_col}  (invert={invert}  →  {cfg['demand_label']})")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 1 — Per-Facility Evidence Scoring
# MAGIC
# MAGIC Each facility receives a **0–12 point evidence score** built from three components:
# MAGIC
# MAGIC | Component | Max | Logic |
# MAGIC |-----------|-----|-------|
# MAGIC | Specialty taxonomy match | 6 pts | `min(matching_specialty_ids, 3) × 2` |
# MAGIC | Text keyword evidence    | 4 pts | `min(fields_with_hits × 1.5, 4)` |
# MAGIC | Data quality signals     | 2 pts | staff, doctors, beds, social media, multi-source |
# MAGIC
# MAGIC **Evidence level** is then assigned:
# MAGIC - **STRONG** — spec ≥ 4 pts OR (spec ≥ 2 AND text fields ≥ 2)
# MAGIC - **PARTIAL** — spec ≥ 2 OR text fields ≥ 2 OR (spec > 0 AND text > 0)
# MAGIC - **WEAK** — any specialty hit OR any text field hit
# MAGIC - **NO CLAIM** — no evidence at all

# COMMAND ----------

facility_score_sql = f"""
CREATE OR REPLACE TEMPORARY VIEW facility_scores AS
WITH base AS (
  SELECT
    name,
    address_city,
    address_stateOrRegion                                             AS state,
    CAST(address_zipOrPostcode AS STRING)                            AS pin,
    COALESCE(CAST(specialties  AS STRING), '')                       AS specialties_raw,
    LOWER(COALESCE(CAST(capability AS STRING), ''))                  AS cap_text,
    LOWER(COALESCE(CAST(procedure  AS STRING), ''))                  AS proc_text,
    LOWER(COALESCE(CAST(equipment  AS STRING), ''))                  AS equip_text,
    LOWER(COALESCE(description, ''))                                  AS desc_text,
    LOWER(COALESCE(CAST(affiliated_staff_presence AS STRING), ''))   AS has_staff,
    LOWER(COALESCE(CAST(custom_logo_presence      AS STRING), ''))   AS has_logo,
    TRY_CAST(distinct_social_media_presence_count AS INT)            AS social_ct,
    TRY_CAST(numberDoctors AS INT)                                   AS doctors,
    TRY_CAST(capacity      AS INT)                                   AS beds,
    COALESCE(CAST(source_types AS STRING), '')                       AS src_types
  FROM {FAC_TABLE}
),

scored AS (
  SELECT *,

    -- ── Component 1: Specialty taxonomy match (0–6 pts) ──────────────────
    -- Each matched specialty ID = 2 pts, max 3 matches counted
    LEAST(
      {spec_cases},
      3
    ) * 2.0 AS spec_score,

    -- Number of distinct specialty IDs matched (used in evidence level logic)
    LEAST(
      {spec_cases},
      3
    ) AS spec_hits,

    -- ── Component 2: Text keyword evidence (0–4 pts) ─────────────────────
    -- 1.5 pts per distinct field containing any keyword, capped at 4
    LEAST(
      1.5 * (
        CASE WHEN cap_text   RLIKE '{kw_pattern}' THEN 1 ELSE 0 END +
        CASE WHEN proc_text  RLIKE '{kw_pattern}' THEN 1 ELSE 0 END +
        CASE WHEN equip_text RLIKE '{kw_pattern}' THEN 1 ELSE 0 END +
        CASE WHEN desc_text  RLIKE '{kw_pattern}' THEN 1 ELSE 0 END
      ),
      4.0
    ) AS text_score,

    -- Count of distinct text fields with keyword hits (used in evidence level logic)
    (CASE WHEN cap_text   RLIKE '{kw_pattern}' THEN 1 ELSE 0 END +
     CASE WHEN proc_text  RLIKE '{kw_pattern}' THEN 1 ELSE 0 END +
     CASE WHEN equip_text RLIKE '{kw_pattern}' THEN 1 ELSE 0 END +
     CASE WHEN desc_text  RLIKE '{kw_pattern}' THEN 1 ELSE 0 END
    ) AS text_field_hits,

    -- ── Component 3: Data quality signals (0–2 pts) ───────────────────────
    LEAST(
      CASE WHEN has_staff IN ('true','1') THEN 0.50 ELSE 0 END +
      CASE WHEN has_logo  IN ('true','1') THEN 0.25 ELSE 0 END +
      CASE WHEN social_ct >= 2 THEN 0.50 WHEN social_ct = 1 THEN 0.20 ELSE 0 END +
      CASE WHEN src_types LIKE '%,%' THEN 0.25 ELSE 0 END +
      CASE WHEN doctors > 0 THEN 0.50 ELSE 0 END +
      CASE WHEN beds    > 0 THEN 0.25 ELSE 0 END,
      2.0
    ) AS quality_score

  FROM base
)

SELECT *,
  ROUND(spec_score + text_score + quality_score, 2) AS total_score,

  -- ── Evidence level ────────────────────────────────────────────────────────
  CASE
    WHEN spec_hits >= 2 AND text_field_hits >= 2 THEN 'STRONG'
    WHEN spec_hits >= 2                          THEN 'STRONG'
    WHEN text_field_hits >= 2                    THEN 'PARTIAL'
    WHEN spec_hits >= 1 AND text_field_hits >= 1 THEN 'PARTIAL'
    WHEN spec_hits >= 1 OR  text_field_hits >= 1 THEN 'WEAK'
    ELSE 'NO CLAIM'
  END AS evidence_level

FROM scored
"""

spark.sql(facility_score_sql)
print("✓ facility_scores view created")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview: top 20 facilities by evidence score
# MAGIC SELECT
# MAGIC   name, address_city, state, pin,
# MAGIC   spec_score, text_score, quality_score, total_score,
# MAGIC   evidence_level, text_field_hits, spec_hits
# MAGIC FROM facility_scores
# MAGIC ORDER BY total_score DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 2 — Region-Level KPIs
# MAGIC
# MAGIC Aggregates facility scores to district level and computes:
# MAGIC
# MAGIC | KPI | Formula |
# MAGIC |-----|---------|
# MAGIC | **Coverage %** | `(strong + partial×0.5 + weak×0.25) / n × 100` |
# MAGIC | **Gap Risk** | `(1 - Coverage/100) × (Demand/100) × 100` |
# MAGIC | **Confidence %** | `min(sample_pts + evidence_pts + geo_pts, 100)` — see formula detail below |
# MAGIC | **Margin of Error** | `max(5, round(50 / √n))` |
# MAGIC | **Data Quality** | Verified (conf ≥ 70 AND n ≥ 10) / Moderate (conf ≥ 40) / Sparse |
# MAGIC | **Priority Tier** | CRITICAL GAP / HIGH PRIORITY / MODERATE / DATA-POOR / ADEQUATE |
# MAGIC
# MAGIC **Confidence detail:**
# MAGIC - Sample size component: `min(n/20, 1) × 40` → 0–40 pts
# MAGIC - Evidence quality component: `(strong + partial×0.5) / n × 40` → 0–40 pts
# MAGIC - Geographic resolution: `20` pts if district NFHS match, `8` pts if state-level fallback

# COMMAND ----------

region_kpi_sql = f"""
CREATE OR REPLACE TEMPORARY VIEW region_kpis AS
WITH pin_lookup AS (
  SELECT CAST(pincode AS STRING) AS pin, district, statename
  FROM {PIN_TABLE}
  GROUP BY pincode, district, statename
),

nfhs AS (
  SELECT
    LOWER(district_name)                                                      AS district,
    LOWER(state_ut)                                                           AS nfhs_state,
    {demand_col}                                                              AS demand_raw,
    -- Health Burden indicators
    COALESCE(all_w15_49_who_are_anaemic_pct, 0)                              AS hb_anaemia,
    (COALESCE(w15_plus_with_high_bp_sys_gte_140_mmhg_and_or_dia_gte_90_mm_pct,0) +
     COALESCE(m15_plus_with_high_bp_sys_gte_140_mmhg_and_or_dia_gte_90_mm_pct,0)) / 2.0 AS hb_bp,
    COALESCE(w15_plus_with_high_or_very_high_gt_140_mg_dl_blood_sugar_or_pct, 0) AS hb_sugar,
    COALESCE(women_age_15_49_years_who_are_overweight_obese_bmi_gte_25_0_pct, 0) AS hb_obesity,
    COALESCE(prev_diarrhoea_2wk_child_u5_pct, 0)                            AS hb_diarrhoea,
    COALESCE(children_prev_symptoms_of_acute_respiratory_infection_ari_2_pct,0) AS hb_ari,
    -- Social Vulnerability indicators (protective — we invert these)
    COALESCE(women_age_15_49_who_are_literate_pct,       50)                AS sv_literacy,
    COALESCE(hh_use_improved_sanitation_pct,             50)                AS sv_sanitation,
    COALESCE(hh_member_covered_health_insurance_pct,     50)                AS sv_insurance,
    COALESCE(hh_improved_water_pct,                      50)                AS sv_water,
    COALESCE(hh_electricity_pct,                         50)                AS sv_electricity,
    COALESCE(institutional_birth_5y_pct,                 50)                AS sv_inst_birth
  FROM {NFHS_TABLE}
),

joined AS (
  SELECT
    f.*,
    p.district,
    n.demand_raw,
    n.hb_anaemia, n.hb_bp, n.hb_sugar, n.hb_obesity, n.hb_diarrhoea, n.hb_ari,
    n.sv_literacy, n.sv_sanitation, n.sv_insurance, n.sv_water, n.sv_electricity, n.sv_inst_birth,
    CASE WHEN n.district IS NOT NULL THEN 1 ELSE 0 END AS has_district_nfhs
  FROM facility_scores f
  LEFT JOIN pin_lookup p ON f.pin = p.pin
  LEFT JOIN nfhs n       ON LOWER(p.district) = n.district
                        AND LOWER(p.statename) = n.nfhs_state
),

agg AS (
  SELECT
    COALESCE(district, CONCAT('Unknown (', state, ')')) AS region,
    state,
    COUNT(*)                                                        AS n_facilities,
    SUM(CASE WHEN evidence_level = 'STRONG'   THEN 1 ELSE 0 END)   AS n_strong,
    SUM(CASE WHEN evidence_level = 'PARTIAL'  THEN 1 ELSE 0 END)   AS n_partial,
    SUM(CASE WHEN evidence_level = 'WEAK'     THEN 1 ELSE 0 END)   AS n_weak,
    SUM(CASE WHEN evidence_level = 'NO CLAIM' THEN 1 ELSE 0 END)   AS n_none,
    MAX(has_district_nfhs)                                          AS has_district_nfhs,
    AVG(demand_raw)    AS demand_avg,
    AVG(hb_anaemia)    AS hb_anaemia,   AVG(hb_bp)          AS hb_bp,
    AVG(hb_sugar)      AS hb_sugar,     AVG(hb_obesity)     AS hb_obesity,
    AVG(hb_diarrhoea)  AS hb_diarrhoea, AVG(hb_ari)         AS hb_ari,
    AVG(sv_literacy)   AS sv_literacy,  AVG(sv_sanitation)  AS sv_sanitation,
    AVG(sv_insurance)  AS sv_insurance, AVG(sv_water)       AS sv_water,
    AVG(sv_electricity) AS sv_electricity, AVG(sv_inst_birth) AS sv_inst_birth
  FROM joined
  GROUP BY region, state
),

kpis AS (
  SELECT *,

    -- ── Coverage % ────────────────────────────────────────────────────────
    ROUND((n_strong + n_partial * 0.5 + n_weak * 0.25) / n_facilities * 100, 1)
      AS coverage_pct,

    -- ── Demand % (direction depends on capability) ────────────────────────
    ROUND({demand_expr.replace("demand_col", "demand_avg").replace(demand_col, "demand_avg")}, 1)
      AS demand_pct,

    -- ── Confidence % ─────────────────────────────────────────────────────
    LEAST(CAST(
      LEAST(n_facilities / 20.0, 1.0) * 40.0 +
      CASE WHEN n_facilities > 0
           THEN (n_strong + n_partial * 0.5) / n_facilities * 40.0
           ELSE 0 END +
      CASE WHEN has_district_nfhs = 1 THEN 20 ELSE 8 END
    AS INT), 100) AS confidence_pct,

    -- ── Margin of Error ───────────────────────────────────────────────────
    GREATEST(5, CAST(ROUND(50.0 / SQRT(n_facilities), 0) AS INT)) AS margin_of_error_pct

  FROM agg
)

SELECT *,

  -- ── Gap Risk ─────────────────────────────────────────────────────────────
  ROUND((1 - coverage_pct / 100.0) * (demand_pct / 100.0) * 100, 1) AS gap_risk,

  -- ── Priority Tier ─────────────────────────────────────────────────────────
  CASE
    WHEN n_facilities < 3 AND (n_strong + n_partial + n_weak) = 0 THEN 'DATA-POOR'
    WHEN coverage_pct < 20 AND n_facilities >= 3                  THEN 'CRITICAL GAP'
    WHEN coverage_pct < 40                                        THEN 'HIGH PRIORITY'
    WHEN coverage_pct < 65                                        THEN 'MODERATE'
    ELSE 'ADEQUATE'
  END AS priority_tier,

  -- ── Data Quality Label ────────────────────────────────────────────────────
  CASE
    WHEN LEAST(CAST(
           LEAST(n_facilities / 20.0, 1.0) * 40.0 +
           CASE WHEN n_facilities > 0
                THEN (n_strong + n_partial * 0.5) / n_facilities * 40.0
                ELSE 0 END +
           CASE WHEN has_district_nfhs = 1 THEN 20 ELSE 8 END
         AS INT), 100) >= 70
      AND n_facilities >= 10 THEN 'Verified'
    WHEN LEAST(CAST(
           LEAST(n_facilities / 20.0, 1.0) * 40.0 +
           CASE WHEN n_facilities > 0
                THEN (n_strong + n_partial * 0.5) / n_facilities * 40.0
                ELSE 0 END +
           CASE WHEN has_district_nfhs = 1 THEN 20 ELSE 8 END
         AS INT), 100) >= 40 THEN 'Moderate'
    ELSE 'Sparse'
  END AS data_quality

FROM kpis
"""

spark.sql(region_kpi_sql)
print("✓ region_kpis view created")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Region KPI results sorted by priority
# MAGIC SELECT
# MAGIC   region, state, n_facilities,
# MAGIC   n_strong, n_partial, n_weak, n_none,
# MAGIC   coverage_pct,
# MAGIC   demand_pct,
# MAGIC   gap_risk,
# MAGIC   priority_tier,
# MAGIC   confidence_pct,
# MAGIC   margin_of_error_pct,
# MAGIC   data_quality
# MAGIC FROM region_kpis
# MAGIC ORDER BY
# MAGIC   CASE priority_tier
# MAGIC     WHEN 'CRITICAL GAP'  THEN 1
# MAGIC     WHEN 'HIGH PRIORITY' THEN 2
# MAGIC     WHEN 'MODERATE'      THEN 3
# MAGIC     WHEN 'DATA-POOR'     THEN 4
# MAGIC     ELSE 5
# MAGIC   END,
# MAGIC   gap_risk DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 3 — CGR Score (A / H / S / C Breakdown)
# MAGIC
# MAGIC **Composite Gap Risk = 0.40×A + 0.25×H + 0.25×S + 0.10×C**
# MAGIC
# MAGIC | Component | Weight | Source |
# MAGIC |-----------|--------|--------|
# MAGIC | **A** Geographic Access Deficit | 40% | `(1 - Coverage/100) × 95` |
# MAGIC | **H** Health Burden | 25% | Average of 6 NFHS-5 disease burden indicators |
# MAGIC | **S** Social Vulnerability | 25% | Inverse average of 6 NFHS-5 protective indicators |
# MAGIC | **C** Capability Density Gap | 10% | Step function on strong-evidence facility ratio |
# MAGIC
# MAGIC **CGR Tiers:** CRITICAL ≥ 57 · HIGH 47–57 · ELEVATED 35–47 · MODERATE 22–35 · LOW < 22

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW cgr_scores AS
# MAGIC SELECT
# MAGIC   region, state, n_facilities, coverage_pct,
# MAGIC   n_strong, n_partial, n_weak,
# MAGIC
# MAGIC   -- ── A: Geographic Access Deficit (0–95) ──────────────────────────────
# MAGIC   ROUND((1.0 - LEAST(coverage_pct, 100) / 100.0) * 95, 1)  AS cgr_A,
# MAGIC
# MAGIC   -- ── H: Health Burden — average of 6 NFHS-5 burden indicators (0–100) ─
# MAGIC   ROUND(LEAST(100, GREATEST(0,
# MAGIC     (hb_anaemia + hb_bp + hb_sugar + hb_obesity + hb_diarrhoea + hb_ari) / 6.0
# MAGIC   )), 1) AS cgr_H,
# MAGIC
# MAGIC   -- ── S: Social Vulnerability — inverse of 6 protective indicators (0–100)
# MAGIC   ROUND(LEAST(100, GREATEST(0,
# MAGIC     ((100 - sv_literacy)   +
# MAGIC      (100 - sv_sanitation) +
# MAGIC      (100 - sv_insurance)  +
# MAGIC      (100 - sv_water)      +
# MAGIC      (100 - sv_electricity)+
# MAGIC      (100 - sv_inst_birth)) / 6.0
# MAGIC   )), 1) AS cgr_S,
# MAGIC
# MAGIC   -- ── C: Capability Density Gap — step function on strong-evidence ratio ─
# MAGIC   -- Lower C = more strong evidence = lower gap risk from this component
# MAGIC   CASE
# MAGIC     WHEN n_facilities = 0                                       THEN 90.0
# MAGIC     WHEN CAST(n_strong AS DOUBLE) / n_facilities >= 0.4         THEN 10.0
# MAGIC     WHEN CAST(n_strong AS DOUBLE) / n_facilities >= 0.2         THEN 30.0
# MAGIC     WHEN n_strong > 0                                           THEN 50.0
# MAGIC     WHEN CAST(n_partial AS DOUBLE) / n_facilities >= 0.2        THEN 65.0
# MAGIC     ELSE 85.0
# MAGIC   END AS cgr_C
# MAGIC
# MAGIC FROM region_kpis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CGR composite score with tier
# MAGIC SELECT
# MAGIC   region, state, n_facilities,
# MAGIC   cgr_A, cgr_H, cgr_S, cgr_C,
# MAGIC   ROUND(0.40 * cgr_A + 0.25 * cgr_H + 0.25 * cgr_S + 0.10 * cgr_C, 1) AS cgr_score,
# MAGIC   CASE
# MAGIC     WHEN 0.40*cgr_A + 0.25*cgr_H + 0.25*cgr_S + 0.10*cgr_C >= 57 THEN 'CRITICAL'
# MAGIC     WHEN 0.40*cgr_A + 0.25*cgr_H + 0.25*cgr_S + 0.10*cgr_C >= 47 THEN 'HIGH'
# MAGIC     WHEN 0.40*cgr_A + 0.25*cgr_H + 0.25*cgr_S + 0.10*cgr_C >= 35 THEN 'ELEVATED'
# MAGIC     WHEN 0.40*cgr_A + 0.25*cgr_H + 0.25*cgr_S + 0.10*cgr_C >= 22 THEN 'MODERATE'
# MAGIC     ELSE 'LOW'
# MAGIC   END AS cgr_tier
# MAGIC FROM cgr_scores
# MAGIC ORDER BY cgr_score DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 4 — Cited Evidence
# MAGIC
# MAGIC For each region, surfaces the **top 2 facilities** (by evidence score)
# MAGIC and the first keyword-matched field with a quoted text excerpt.
# MAGIC
# MAGIC If no facility has any keyword match, the app displays:
# MAGIC > *"No facility text evidence found for this capability here — score and priority reflect
# MAGIC > that absence, not a confirmed gap."*

# COMMAND ----------

cited_evidence_sql = f"""
CREATE OR REPLACE TEMPORARY VIEW cited_evidence AS
WITH pin_lookup AS (
  SELECT CAST(pincode AS STRING) AS pin, district, statename
  FROM {PIN_TABLE}
  GROUP BY pincode, district, statename
),

with_district AS (
  SELECT
    f.name, f.address_city, f.state, f.pin,
    f.total_score, f.evidence_level,
    f.cap_text, f.proc_text, f.equip_text, f.desc_text,
    COALESCE(p.district, CONCAT('Unknown (', f.state, ')')) AS region
  FROM facility_scores f
  LEFT JOIN pin_lookup p ON f.pin = p.pin
  WHERE f.evidence_level != 'NO CLAIM'
),

ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY total_score DESC) AS rank_in_region
  FROM with_district
)

SELECT
  region,
  name                AS facility_name,
  address_city,
  state,
  total_score,
  evidence_level,
  -- Which field holds the first keyword match (mirrors app citation logic)
  CASE
    WHEN cap_text   RLIKE '{kw_pattern}' THEN 'capability'
    WHEN proc_text  RLIKE '{kw_pattern}' THEN 'procedure'
    WHEN equip_text RLIKE '{kw_pattern}' THEN 'equipment'
    WHEN desc_text  RLIKE '{kw_pattern}' THEN 'description'
  END AS cited_field,
  -- Excerpt from the matched field (first 120 chars as proxy for ±55 char window)
  CASE
    WHEN cap_text   RLIKE '{kw_pattern}' THEN SUBSTRING(cap_text,   1, 120)
    WHEN proc_text  RLIKE '{kw_pattern}' THEN SUBSTRING(proc_text,  1, 120)
    WHEN equip_text RLIKE '{kw_pattern}' THEN SUBSTRING(equip_text, 1, 120)
    WHEN desc_text  RLIKE '{kw_pattern}' THEN SUBSTRING(desc_text,  1, 120)
  END AS cited_excerpt
FROM ranked
WHERE rank_in_region <= 2
"""

spark.sql(cited_evidence_sql)
print("✓ cited_evidence view created")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT region, facility_name, total_score, evidence_level, cited_field, cited_excerpt
# MAGIC FROM cited_evidence
# MAGIC ORDER BY region, total_score DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 5 — Full Dashboard View
# MAGIC
# MAGIC Joins all KPIs into a single output matching what the app displays per region.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   k.region,
# MAGIC   k.state,
# MAGIC   k.n_facilities                                               AS facilities,
# MAGIC   k.n_strong, k.n_partial, k.n_weak, k.n_none,
# MAGIC   k.coverage_pct,
# MAGIC   k.demand_pct,
# MAGIC   k.gap_risk,
# MAGIC   k.priority_tier,
# MAGIC   k.confidence_pct,
# MAGIC   k.margin_of_error_pct,
# MAGIC   k.data_quality,
# MAGIC   ROUND(0.40 * c.cgr_A + 0.25 * c.cgr_H + 0.25 * c.cgr_S + 0.10 * c.cgr_C, 1) AS cgr_score,
# MAGIC   c.cgr_A  AS access_deficit,
# MAGIC   c.cgr_H  AS health_burden,
# MAGIC   c.cgr_S  AS social_vulnerability,
# MAGIC   c.cgr_C  AS capability_density_gap,
# MAGIC   CASE
# MAGIC     WHEN 0.40*c.cgr_A + 0.25*c.cgr_H + 0.25*c.cgr_S + 0.10*c.cgr_C >= 57 THEN 'CRITICAL'
# MAGIC     WHEN 0.40*c.cgr_A + 0.25*c.cgr_H + 0.25*c.cgr_S + 0.10*c.cgr_C >= 47 THEN 'HIGH'
# MAGIC     WHEN 0.40*c.cgr_A + 0.25*c.cgr_H + 0.25*c.cgr_S + 0.10*c.cgr_C >= 35 THEN 'ELEVATED'
# MAGIC     WHEN 0.40*c.cgr_A + 0.25*c.cgr_H + 0.25*c.cgr_S + 0.10*c.cgr_C >= 22 THEN 'MODERATE'
# MAGIC     ELSE 'LOW'
# MAGIC   END AS cgr_tier
# MAGIC FROM region_kpis k
# MAGIC LEFT JOIN cgr_scores c USING (region)
# MAGIC ORDER BY
# MAGIC   CASE k.priority_tier
# MAGIC     WHEN 'CRITICAL GAP'  THEN 1
# MAGIC     WHEN 'HIGH PRIORITY' THEN 2
# MAGIC     WHEN 'MODERATE'      THEN 3
# MAGIC     WHEN 'DATA-POOR'     THEN 4
# MAGIC     ELSE 5
# MAGIC   END,
# MAGIC   k.gap_risk DESC
