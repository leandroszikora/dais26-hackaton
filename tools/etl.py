#!/usr/bin/env python3
"""Virtue Foundation medallion ETL builder (Track 2: Medical Desert Planner).
Generates bronze->silver->gold SQL, executes it on the SQL warehouse, and
emits a re-runnable .sql notebook. Logic mirrors app_simple.py exactly."""
import sys
sys.path.insert(0, "/tmp")
from sql import run

GOLD = "workspace.vf_gold"
SILVER = "workspace.vf_silver"
BRONZE = "workspace.vf_bronze"

# ---- taxonomy copied verbatim from app_simple.py -----------------------------
SPECIALTY_CAPS = {
    "ICU":        ["criticalCareMedicine","pulmonologyCriticalCare","anesthesiaCriticalCare"],
    "Maternity":  ["gynecologyAndObstetrics","maternalAndFetalMedicine",
                   "reproductiveEndocrinologyAndInfertility","neonatologyPerinatalMedicine"],
    "Emergency":  ["emergencyMedicine","pediatricEmergencyMedicine","traumaSurgery"],
    "Dialysis":   ["nephrology","pediatricNephrology"],
    "Oncology":   ["oncology","radiationOncology","surgicalOncology","hematologyOncology",
                   "pediatricHematologyOncology","gynecologicOncology"],
    "Cardiology": ["cardiology","interventionalCardiology","pediatricCardiology"],
    "Pediatrics": ["pediatrics","neonatologyPerinatalMedicine","pediatricSurgery",
                   "pediatricOrthopedicSurgery","pediatricEmergencyMedicine",
                   "pediatricNephrology","pediatricHematologyOncology","pediatricCardiology"],
    "Surgery":    ["generalSurgery","traumaSurgery","neurosurgery","orthopedicSurgery",
                   "plasticSurgery","urology","spineNeurosurgery","jointReconstructionSurgery"],
}
KEYWORDS = {
    "ICU":        ["icu","intensive care","critical care","ventilator","icu bed"],
    "Maternity":  ["maternity","obstetric","gynecol","delivery","labour","labor",
                   "prenatal","antenatal","postnatal","birth"],
    "Emergency":  ["emergency","trauma","ambulance","casualty","accident"],
    "Dialysis":   ["dialysis","hemodialysis","renal","nephrology"],
    "Oncology":   ["oncology","cancer","chemotherapy","radiation therapy","radiotherapy","tumor"],
    "Cardiology": ["cardiology","cardiac","coronary","angioplasty","echocardiography"],
    "Pediatrics": ["pediatric","paediatric","nicu","neonatal","children","child health"],
    "Surgery":    ["surgery","surgical","operation theater","laparoscopic","general surgery"],
}
# (nfhs_column, label, invert)  invert=True -> low value = high need
DEMAND = {
    "Maternity":  ("institutional_birth_5y_pct", "Institutional births %", True),
    "Emergency":  ("hh_member_covered_health_insurance_pct", "Health insurance %", True),
    "ICU":        ("hh_member_covered_health_insurance_pct", "Health insurance %", True),
    "Dialysis":   ("all_w15_49_who_are_anaemic_pct", "Women anaemia %", False),
    "Oncology":   ("hh_member_covered_health_insurance_pct", "Health insurance %", True),
    "Cardiology": ("w15_plus_with_high_bp_sys_gte_140_mmhg_and_or_dia_gte_90_mm_pct", "High BP prevalence %", False),
    "Pediatrics": ("prev_diarrhoea_2wk_child_u5_pct", "Child diarrhoea %", False),
    "Surgery":    ("births_delivered_by_csection_5y_pct", "C-section births %", False),
}
CAPS = list(KEYWORDS.keys())

def rx(words):
    return "|".join(w.replace("'", "''") for w in words)

def norm_state(c):
    n = (f"regexp_replace(regexp_replace(lower(trim({c})), '\\\\s*&\\\\s*', ' and '), "
         f"'\\\\s+', ' ')")
    return (f"CASE {n} "
            "WHEN 'maharastra' THEN 'maharashtra' "
            "WHEN 'nct of delhi' THEN 'delhi' "
            "WHEN 'orissa' THEN 'odisha' "
            "WHEN 'pondicherry' THEN 'puducherry' "
            "WHEN 'uttaranchal' THEN 'uttarakhand' "
            f"ELSE {n} END")

STMTS = []
def add(name, sql):
    STMTS.append((name, sql.strip()))

# ============================ ENRICHMENT: Census 2011 ==========================
# Source CSV uploaded to volume /Volumes/workspace/vf_bronze/files/india_census_2011.csv
# (Census 2011 district population; 640 districts, ~1.21B total). Re-upload before
# re-running on a fresh workspace.
add("bronze.census_2011_district", f"""
CREATE OR REPLACE TABLE {BRONZE}.census_2011_district AS
SELECT `District code` AS district_code, `State name` AS state_name,
       `District name` AS district_name, Population AS population,
       Male, Female, Literate, Households, Urban_Households, Rural_Households
FROM read_files('/Volumes/workspace/vf_bronze/files/india_census_2011.csv',
  format => 'csv', header => true, inferSchema => true)
""")

# normalized for joining; district aliases map 2011 names -> modern district names
_cdist = "regexp_replace(regexp_replace(lower(trim(district_name)), '\\\\s*\\\\(.*\\\\)', ''), '[^a-z ]','')"
add("silver.geo_census", f"""
CREATE OR REPLACE TABLE {SILVER}.geo_census AS
SELECT district_code, district_name AS district_raw,
  CASE {_cdist}
    WHEN 'bangalore' THEN 'bengaluru urban'
    WHEN 'gurgaon'   THEN 'gurugram'
    WHEN 'allahabad' THEN 'prayagraj'
    ELSE {_cdist} END                AS district_norm,
  state_name AS state_raw,
  {norm_state('state_name')}         AS state_norm,
  population
FROM {BRONZE}.census_2011_district
""")

# ============================ SILVER ==========================================
add("silver.geo_pincode", f"""
CREATE OR REPLACE TABLE {SILVER}.geo_pincode AS
SELECT pincode,
       trim(district)               AS district,
       lower(trim(district))        AS district_norm,
       statename                    AS state_raw,
       {norm_state('statename')}    AS state_norm
FROM (
  SELECT pincode, district, statename,
         ROW_NUMBER() OVER (PARTITION BY pincode ORDER BY district) rn
  FROM {BRONZE}.pincode_directory
  WHERE pincode IS NOT NULL AND district IS NOT NULL
) WHERE rn = 1
""")

add("silver.geo_office", f"""
CREATE OR REPLACE TABLE {SILVER}.geo_office AS
SELECT office_norm, state_norm, district FROM (
  SELECT
    regexp_replace(lower(trim(officename)),
      '\\\\s+(b\\\\.?o\\\\.?|h\\\\.?o\\\\.?|s\\\\.?o\\\\.?|g\\\\.?p\\\\.?o\\\\.?|p\\\\.?o\\\\.?|b/o|h/o|s/o)\\\\s*$','')
      AS office_norm,
    {norm_state('statename')} AS state_norm,
    trim(district)            AS district,
    ROW_NUMBER() OVER (PARTITION BY
      regexp_replace(lower(trim(officename)),
        '\\\\s+(b\\\\.?o\\\\.?|h\\\\.?o\\\\.?|s\\\\.?o\\\\.?|g\\\\.?p\\\\.?o\\\\.?|p\\\\.?o\\\\.?|b/o|h/o|s/o)\\\\s*$',''),
      {norm_state('statename')} ORDER BY trim(district)) rn
  FROM {BRONZE}.pincode_directory
  WHERE officename IS NOT NULL AND officename <> '' AND district IS NOT NULL
) WHERE rn = 1
""")

add("silver.nfhs", f"""
CREATE OR REPLACE TABLE {SILVER}.nfhs AS
SELECT district_name,
       lower(trim(district_name))     AS district_norm,
       state_ut                       AS state_raw,
       {norm_state('state_ut')}       AS state_norm,
       institutional_birth_5y_pct,
       hh_member_covered_health_insurance_pct,
       all_w15_49_who_are_anaemic_pct,
       w15_plus_with_high_bp_sys_gte_140_mmhg_and_or_dia_gte_90_mm_pct,
       prev_diarrhoea_2wk_child_u5_pct,
       births_delivered_by_csection_5y_pct,
       births_attended_by_skilled_hp_5y_10_pct
FROM {BRONZE}.nfhs_district_health
""")

# facilities_base: clean fields + tier-1 (PIN) district resolution
add("silver.facilities_base", f"""
CREATE OR REPLACE TABLE {SILVER}.facilities_base AS
SELECT
  f.unique_id                                   AS facility_id,
  f.name                                        AS facility_name,
  f.facilityTypeId                              AS facility_type,
  f.operatorTypeId                              AS operator_type,
  trim(f.address_city)                          AS city,
  lower(trim(f.address_city))                   AS city_norm,
  f.address_stateOrRegion                       AS state_raw,
  {norm_state('f.address_stateOrRegion')}       AS state_norm,
  try_cast(regexp_extract(f.address_zipOrPostcode,'([0-9]{{6}})',1) AS BIGINT) AS pincode,
  f.latitude, f.longitude,
  (f.latitude  BETWEEN 6.0 AND 37.5 AND
   f.longitude BETWEEN 67.0 AND 97.5)           AS coords_valid,
  NULLIF(NULLIF(f.description,'null'),'')        AS description,
  CASE WHEN f.capability IN ('null','[]','') OR f.capability IS NULL THEN NULL ELSE f.capability END AS capability_raw,
  CASE WHEN f.procedure  IN ('null','[]','') OR f.procedure  IS NULL THEN NULL ELSE f.procedure  END AS procedure_raw,
  CASE WHEN f.equipment  IN ('null','[]','') OR f.equipment  IS NULL THEN NULL ELSE f.equipment  END AS equipment_raw,
  CASE WHEN f.specialties IN ('null','[]','') OR f.specialties IS NULL THEN NULL ELSE f.specialties END AS specialties_raw,
  try_cast(f.numberDoctors AS INT)              AS number_doctors,
  try_cast(f.capacity AS INT)                   AS capacity,
  CASE WHEN f.yearEstablished IN ('null','') OR f.yearEstablished IS NULL THEN NULL ELSE f.yearEstablished END AS year_established,
  f.officialPhone, f.officialWebsite,
  lower(coalesce(f.affiliated_staff_presence,'')) IN ('true','1') AS has_staff,
  lower(coalesce(f.custom_logo_presence,''))      IN ('true','1') AS has_logo,
  try_cast(f.distinct_social_media_presence_count AS INT) AS social_count,
  try_cast(f.engagement_metrics_n_followers AS INT)       AS followers,
  try_cast(f.number_of_facts_about_the_organization AS INT) AS num_facts,
  coalesce(size(try_cast(from_json(f.source_types,'array<string>') AS array<string>)),0) AS n_source_types,
  p.district     AS district_pin,
  p.state_norm   AS district_state_pin
FROM {BRONZE}.facilities f
LEFT JOIN {SILVER}.geo_pincode p ON f.address_zipOrPostcode RLIKE '[0-9]{{6}}'
     AND try_cast(regexp_extract(f.address_zipOrPostcode,'([0-9]{{6}})',1) AS BIGINT) = p.pincode
""")

# tier-2 index: city+state -> district (from facilities that have a PIN match)
add("silver.geo_city_district", f"""
CREATE OR REPLACE TABLE {SILVER}.geo_city_district AS
SELECT city_norm, state_norm, district FROM (
  SELECT city_norm, state_norm, district_pin AS district,
         ROW_NUMBER() OVER (PARTITION BY city_norm, state_norm ORDER BY district_pin) rn
  FROM {SILVER}.facilities_base
  WHERE district_pin IS NOT NULL AND city_norm <> '' AND state_norm <> ''
) WHERE rn = 1
""")

# final silver.facilities with 3-tier district resolution + source tag
add("silver.facilities", f"""
CREATE OR REPLACE TABLE {SILVER}.facilities AS
SELECT b.*,
  coalesce(b.district_pin, cd.district, of.district)                  AS district,
  CASE WHEN b.district_pin IS NOT NULL THEN 'pin'
       WHEN cd.district    IS NOT NULL THEN 'city'
       WHEN of.district    IS NOT NULL THEN 'office'
       ELSE NULL END                                                  AS district_source,
  coalesce(b.district_state_pin, b.state_norm)                        AS district_state_norm,
  lower(concat_ws(' ', coalesce(b.capability_raw,''), coalesce(b.procedure_raw,''),
                       coalesce(b.equipment_raw,''), coalesce(b.description,''))) AS combined_text
FROM {SILVER}.facilities_base b
LEFT JOIN {SILVER}.geo_city_district cd ON b.city_norm=cd.city_norm AND b.state_norm=cd.state_norm
LEFT JOIN {SILVER}.geo_office of        ON b.city_norm=of.office_norm AND b.state_norm=of.state_norm
""")

# ============================ GOLD: facility_capability =======================
def cap_select(cap):
    toks = SPECIALTY_CAPS[cap]
    spec_terms = " + ".join(
        f"(CASE WHEN specialties_raw LIKE '%\"{t}\"%' OR specialties_raw LIKE '%''{t}''%' THEN 1 ELSE 0 END)"
        for t in toks)
    pat = rx(KEYWORDS[cap])
    capf = f"(CASE WHEN lower(coalesce(capability_raw,'')) RLIKE '{pat}' THEN 1 ELSE 0 END)"
    prof = f"(CASE WHEN lower(coalesce(procedure_raw,''))  RLIKE '{pat}' THEN 1 ELSE 0 END)"
    eqf  = f"(CASE WHEN lower(coalesce(equipment_raw,''))  RLIKE '{pat}' THEN 1 ELSE 0 END)"
    desf = f"(CASE WHEN lower(coalesce(description,''))     RLIKE '{pat}' THEN 1 ELSE 0 END)"
    return f"""
SELECT facility_id, facility_name, facility_type, city, state_raw, state_norm,
       district, district_source, district_state_norm, pincode, latitude, longitude, coords_valid,
       '{cap}' AS capability,
       least({spec_terms}, 3) * 2.0                              AS spec_score,
       ({capf}+{prof}+{eqf}+{desf})                              AS n_field_hits,
       least(({capf}+{prof}+{eqf}+{desf}) * 1.5, 4.0)            AS text_score,
       (CASE WHEN has_staff THEN 0.5 ELSE 0 END)
         + (CASE WHEN has_logo THEN 0.25 ELSE 0 END)
         + (CASE WHEN social_count>=2 THEN 0.5 WHEN social_count=1 THEN 0.2 ELSE 0 END)
         + (CASE WHEN n_source_types>=2 THEN 0.25 ELSE 0 END)
         + (CASE WHEN number_doctors>0 THEN 0.5 ELSE 0 END)
         + (CASE WHEN capacity>0 THEN 0.25 ELSE 0 END)          AS quality_score,
       regexp_extract(combined_text, '(.{{0,55}}({pat}).{{0,110}})', 1) AS evidence_snippet,
       concat_ws(',',
         CASE WHEN {capf}=1 THEN 'capability' END,
         CASE WHEN {prof}=1 THEN 'procedure'  END,
         CASE WHEN {eqf}=1  THEN 'equipment'  END,
         CASE WHEN {desf}=1 THEN 'description' END)             AS evidence_fields
FROM {SILVER}.facilities
"""

union = "\nUNION ALL\n".join(cap_select(c) for c in CAPS)
add("gold.facility_capability", f"""
CREATE OR REPLACE TABLE {GOLD}.facility_capability
COMMENT 'One row per facility x capability with trust signal + evidence citation (mirrors app_simple.py scoring)' AS
WITH scored AS (
{union}
)
SELECT *,
  round(spec_score + text_score + quality_score, 2) AS total_score,
  CASE
    WHEN spec_score>=4 OR (spec_score>=2 AND n_field_hits>=2) THEN 'STRONG'
    WHEN spec_score>=2 OR n_field_hits>=2 OR (spec_score>0 AND n_field_hits>=1) THEN 'PARTIAL'
    WHEN spec_score>0 OR n_field_hits>=1 THEN 'WEAK'
    ELSE 'NO CLAIM' END AS trust_level,
  (spec_score>0 OR n_field_hits>=1) AS has_evidence
FROM scored
""")

# ============================ GOLD aggregates =================================
# CASE expressions for selecting the per-capability NFHS demand column.
nfhs_case = "CASE capability\n" + "\n".join(
    f"    WHEN '{c}' THEN nf.{DEMAND[c][0]}" for c in CAPS) + "\n  END"
need_case = "CASE capability\n" + "\n".join(
    f"    WHEN '{c}' THEN {'100 - nf.'+DEMAND[c][0] if DEMAND[c][2] else 'nf.'+DEMAND[c][0]}"
    for c in CAPS) + "\n  END"
label_case = "CASE capability\n" + "\n".join(
    f"    WHEN '{c}' THEN '{DEMAND[c][1]}'" for c in CAPS) + "\n  END"
# state-average variants (column alias <col>_avg)
nfhs_case_s = "CASE capability\n" + "\n".join(
    f"    WHEN '{c}' THEN sa.{DEMAND[c][0]}_avg" for c in CAPS) + "\n  END"
need_case_s = "CASE capability\n" + "\n".join(
    f"    WHEN '{c}' THEN {'100 - sa.'+DEMAND[c][0]+'_avg' if DEMAND[c][2] else 'sa.'+DEMAND[c][0]+'_avg'}"
    for c in CAPS) + "\n  END"
label_case_s = "CASE capability\n" + "\n".join(
    f"    WHEN '{c}' THEN '{DEMAND[c][1]}'" for c in CAPS) + "\n  END"
nfhs_avgs = ",\n         ".join(
    f"AVG({col}) AS {col}_avg" for col in sorted({DEMAND[c][0] for c in CAPS}))

# Shared priority CASE (coverage-driven, unchanged semantics)
PRIORITY_CASE = """CASE
    WHEN facility_count<3 AND NOT has_evidence THEN 'DATA-POOR'
    WHEN coverage_pct<20 AND facility_count>=3 THEN 'CRITICAL GAP'
    WHEN coverage_pct<40 THEN 'HIGH PRIORITY'
    WHEN coverage_pct<65 THEN 'MODERATE'
    ELSE 'ADEQUATE' END"""

add("gold.district_capability", f"""
CREATE OR REPLACE TABLE {GOLD}.district_capability
COMMENT 'Per district x capability: trust-weighted coverage vs NFHS demand, decomposed gap_risk + confidence (Track 2 headline)' AS
WITH agg AS (
  SELECT district, district_state_norm AS state_norm, capability,
         COUNT(*) AS facility_count,
         SUM(CASE WHEN trust_level='STRONG'   THEN 1 ELSE 0 END) AS strong_count,
         SUM(CASE WHEN trust_level='PARTIAL'  THEN 1 ELSE 0 END) AS partial_count,
         SUM(CASE WHEN trust_level='WEAK'     THEN 1 ELSE 0 END) AS weak_count,
         SUM(CASE WHEN trust_level='NO CLAIM' THEN 1 ELSE 0 END) AS no_claim_count,
         round(AVG(total_score),2) AS avg_score
  FROM {GOLD}.facility_capability
  WHERE district IS NOT NULL
  GROUP BY district, district_state_norm, capability
),
state_avg AS (
  SELECT state_norm, {nfhs_avgs} FROM {SILVER}.nfhs GROUP BY state_norm
),
pop AS (
  SELECT district_norm, state_norm, SUM(population) AS population
  FROM {SILVER}.geo_census GROUP BY district_norm, state_norm
),
joined AS (
  SELECT a.*,
    round((a.strong_count + a.partial_count*0.5 + a.weak_count*0.25)/a.facility_count*100, 1) AS coverage_pct,
    (a.strong_count + a.partial_count + a.weak_count) > 0 AS has_evidence,
    nf.district_name IS NOT NULL AS nfhs_matched,
    ({nfhs_case}) AS nfhs_value,
    round(({need_case}),1) AS need_district,
    round(({need_case_s}),1) AS need_state,
    ({label_case}) AS nfhs_field,
    p.population AS population,
    p.population IS NOT NULL AS population_matched,
    CASE WHEN p.population>0 THEN round(a.facility_count/p.population*100000, 3) END AS facilities_per_100k
  FROM agg a
  LEFT JOIN {SILVER}.nfhs nf
    ON lower(a.district) = nf.district_norm AND a.state_norm = nf.state_norm
  LEFT JOIN state_avg sa ON a.state_norm = sa.state_norm
  LEFT JOIN pop p ON lower(a.district) = p.district_norm AND a.state_norm = p.state_norm
),
scored AS (
  SELECT *,
    coalesce(need_district, need_state) AS need_score,
    (need_district IS NULL AND need_state IS NOT NULL) AS need_imputed,
    {PRIORITY_CASE} AS priority,
    least(round(
        least(facility_count/20.0,1.0)*40
      + (CASE WHEN facility_count>0 THEN (strong_count + partial_count*0.5)/facility_count*40 ELSE 0 END)
      + (CASE WHEN nfhs_matched THEN 20 ELSE 8 END)), 100) AS confidence_pct,
    greatest(5, round(50/sqrt(facility_count))) AS margin_of_error_pct
  FROM joined
)
SELECT *,
  -- scarcity: relative per-capita facility density within capability (100 = scarcest), where population known
  CASE WHEN population_matched THEN
    round((1 - percent_rank() OVER (PARTITION BY capability ORDER BY facilities_per_100k)) * 100, 1)
  END AS scarcity_pct,
  -- severity: demand x supply-gap, independent of confidence (0-100)
  round((1 - coverage_pct/100) * (need_score/100) * 100, 1) AS unmet_need_score,
  -- headline: severity discounted by confidence so thin/imputed evidence cannot masquerade as a real gap (0-100)
  round((1 - coverage_pct/100) * (need_score/100) * (confidence_pct/100.0) * 100, 1) AS gap_risk,
  -- legacy v1 formula (flat need=50 fallback) kept for comparison
  round((1 - coverage_pct/100) * (coalesce(need_score,50)/100) * 100, 1) AS risk_score_legacy
FROM scored
""")

add("gold.state_capability", f"""
CREATE OR REPLACE TABLE {GOLD}.state_capability
COMMENT 'Per state x capability rollup with state-average NFHS demand' AS
WITH agg AS (
  SELECT state_norm, capability,
         COUNT(*) AS facility_count,
         SUM(CASE WHEN trust_level='STRONG'   THEN 1 ELSE 0 END) AS strong_count,
         SUM(CASE WHEN trust_level='PARTIAL'  THEN 1 ELSE 0 END) AS partial_count,
         SUM(CASE WHEN trust_level='WEAK'     THEN 1 ELSE 0 END) AS weak_count,
         SUM(CASE WHEN trust_level='NO CLAIM' THEN 1 ELSE 0 END) AS no_claim_count
  FROM {GOLD}.facility_capability
  GROUP BY state_norm, capability
),
sa AS (
  SELECT state_norm, {nfhs_avgs}
  FROM {SILVER}.nfhs GROUP BY state_norm
),
joined AS (
  SELECT a.*,
    round((a.strong_count + a.partial_count*0.5 + a.weak_count*0.25)/a.facility_count*100,1) AS coverage_pct,
    (a.strong_count + a.partial_count + a.weak_count) > 0 AS has_evidence,
    ({nfhs_case_s}) AS nfhs_value,
    round(({need_case_s}),1) AS need_score,
    ({label_case_s}) AS nfhs_field
  FROM agg a LEFT JOIN sa USING (state_norm)
),
scored AS (
  SELECT *,
    {PRIORITY_CASE} AS priority,
    least(round(least(facility_count/20.0,1.0)*60
      + (CASE WHEN facility_count>0 THEN (strong_count + partial_count*0.5)/facility_count*40 ELSE 0 END)),100) AS confidence_pct
  FROM joined
)
SELECT *,
  round((1 - coverage_pct/100) * (need_score/100) * 100,1) AS unmet_need_score,
  round((1 - coverage_pct/100) * (need_score/100) * (confidence_pct/100.0) * 100,1) AS gap_risk,
  round((1 - coverage_pct/100) * (coalesce(need_score,50)/100) * 100,1) AS risk_score_legacy
FROM scored
""")

# ============================ GOLD: facility_directory ========================
add("gold.facility_directory", f"""
CREATE OR REPLACE TABLE {GOLD}.facility_directory
COMMENT 'Per-facility profile + completeness rating (0-5) + resolved district' AS
SELECT facility_id, facility_name, facility_type, operator_type,
       city, state_raw, state_norm, district, district_source, pincode,
       latitude, longitude, coords_valid,
       number_doctors, capacity, year_established, officialPhone, officialWebsite,
       round(least((
         (CASE WHEN number_doctors>0 THEN 1.0 ELSE 0 END)
       + (CASE WHEN capacity>0 THEN 0.75 ELSE 0 END)
       + (CASE WHEN officialPhone IS NOT NULL AND officialPhone<>'' THEN 0.5 ELSE 0 END)
       + (CASE WHEN officialWebsite IS NOT NULL AND officialWebsite<>'' THEN 0.5 ELSE 0 END)
       + (CASE WHEN year_established IS NOT NULL THEN 0.25 ELSE 0 END)
       + (CASE WHEN has_staff THEN 0.75 ELSE 0 END)
       + (CASE WHEN has_logo THEN 0.25 ELSE 0 END)
       + (CASE WHEN social_count>=2 THEN 0.5 WHEN social_count=1 THEN 0.2 ELSE 0 END)
       + (CASE WHEN followers>1000 THEN 0.5 WHEN followers>100 THEN 0.25 ELSE 0 END)
       + (CASE WHEN num_facts>20 THEN 0.5 WHEN num_facts>5 THEN 0.25 ELSE 0 END)
       ), 5.0), 1) AS completeness_rating
FROM {SILVER}.facilities
""")

# ============================ GOLD: field_coverage (honesty) ==================
add("gold.field_coverage", f"""
CREATE OR REPLACE TABLE {GOLD}.field_coverage
COMMENT 'Dataset field coverage (true coverage excluding null/[] placeholders) for uncertainty messaging' AS
WITH c AS (
  SELECT
    round(100.0*count_if(description     IS NOT NULL)/count(*),1) AS description,
    round(100.0*count_if(capability_raw  IS NOT NULL)/count(*),1) AS capability,
    round(100.0*count_if(procedure_raw   IS NOT NULL)/count(*),1) AS procedure,
    round(100.0*count_if(equipment_raw   IS NOT NULL)/count(*),1) AS equipment,
    round(100.0*count_if(specialties_raw IS NOT NULL)/count(*),1) AS specialties,
    round(100.0*count_if(number_doctors  IS NOT NULL)/count(*),1) AS numberDoctors,
    round(100.0*count_if(capacity        IS NOT NULL)/count(*),1) AS capacity,
    round(100.0*count_if(year_established IS NOT NULL)/count(*),1) AS yearEstablished,
    round(100.0*count_if(pincode         IS NOT NULL)/count(*),1) AS pincode,
    round(100.0*count_if(district        IS NOT NULL)/count(*),1) AS district,
    round(100.0*count_if(coords_valid)/count(*),1)                AS coords_valid
  FROM {SILVER}.facilities
)
SELECT stack(11,
  'description', description, 'capability', capability, 'procedure', procedure,
  'equipment', equipment, 'specialties', specialties, 'numberDoctors', numberDoctors,
  'capacity', capacity, 'yearEstablished', yearEstablished, 'pincode', pincode,
  'district', district, 'coords_valid', coords_valid) AS (field, coverage_pct)
FROM c
""")

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "run"
    if mode == "emit":
        out = sys.argv[2] if len(sys.argv) > 2 else "/tmp/medallion_etl.sql"
        SRC = "databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset"
        # ordered (section, name, sql) — 3 source snapshots first, then STMTS
        items = [("bronze", nm,
                  f"CREATE OR REPLACE TABLE {BRONZE}.{nm} AS SELECT * FROM {SRC}.{src}")
                 for nm, src in [("facilities","facilities"),
                                 ("pincode_directory","india_post_pincode_directory"),
                                 ("nfhs_district_health","nfhs_5_district_health_indicators")]]
        items += [(name.split(".")[0], name, sql) for name, sql in STMTS]

        def md_cell(lines):  # markdown cell: every line gets the MAGIC prefix
            return "\n".join("-- MAGIC " + l for l in lines)

        cells = [md_cell([
            "%md",
            "# Virtue Foundation Medallion ETL — Track 2 (Medical Desert Planner)",
            "",
            "Bronze → Silver → Gold. Idempotent (`CREATE OR REPLACE`). Run top-to-bottom on a",
            "SQL warehouse. Scoring/trust logic mirrors `app_simple.py`.",
            "",
            "**Prereq:** upload `data/india_census_2011.csv` to",
            "`/Volumes/workspace/vf_bronze/files/` before the census cell (population enrichment).",
        ])]
        cells.append("-- Create target schemas\n"
                     "CREATE SCHEMA IF NOT EXISTS workspace.vf_bronze;\n"
                     "CREATE SCHEMA IF NOT EXISTS workspace.vf_silver;\n"
                     "CREATE SCHEMA IF NOT EXISTS workspace.vf_gold;")
        section_titles = {"bronze": "## 🥉 BRONZE — raw snapshots + enrichment source",
                          "silver": "## 🥈 SILVER — cleaned, normalized, geo-resolved",
                          "gold":   "## 🥇 GOLD — curated facility + aggregated district/state tables"}
        cur = None
        for section, name, sql in items:
            if section != cur:                       # section divider (its own md cell)
                cells.append(md_cell(["%md", section_titles[section]]))
                cur = section
            cells.append(f"-- {name}\n{sql.strip()};")  # pure SQL cell (executable)

        sep = "\n\n-- COMMAND ----------\n\n"
        text = "-- Databricks notebook source\n" + sep.join(cells) + "\n"
        open(out, "w").write(text)
        print("wrote", out)
    else:
        for name, sql in STMTS:
            cols, rows = run(sql)
            status = "OK" if cols is not None else "FAILED"
            print(f"[{status}] {name}")
            if cols is None:
                print(sql[:500]); sys.exit(1)
        print("ALL SILVER+GOLD STATEMENTS SUCCEEDED")
