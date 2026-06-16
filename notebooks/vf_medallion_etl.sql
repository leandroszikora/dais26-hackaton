-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Virtue Foundation Medallion ETL — Track 2 (Medical Desert Planner)
-- MAGIC 
-- MAGIC Bronze → Silver → Gold. Idempotent (`CREATE OR REPLACE`). Run top-to-bottom on a
-- MAGIC SQL warehouse. Scoring/trust logic mirrors `app_simple.py`.
-- MAGIC 
-- MAGIC **Prereq:** upload `data/india_census_2011.csv` to
-- MAGIC `/Volumes/workspace/vf_bronze/files/` before the census cell (population enrichment).

-- COMMAND ----------

-- Create target schemas
CREATE SCHEMA IF NOT EXISTS workspace.vf_bronze;
CREATE SCHEMA IF NOT EXISTS workspace.vf_silver;
CREATE SCHEMA IF NOT EXISTS workspace.vf_gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🥉 BRONZE — raw snapshots + enrichment source

-- COMMAND ----------

-- facilities
CREATE OR REPLACE TABLE workspace.vf_bronze.facilities AS SELECT * FROM databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.facilities;

-- COMMAND ----------

-- pincode_directory
CREATE OR REPLACE TABLE workspace.vf_bronze.pincode_directory AS SELECT * FROM databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.india_post_pincode_directory;

-- COMMAND ----------

-- nfhs_district_health
CREATE OR REPLACE TABLE workspace.vf_bronze.nfhs_district_health AS SELECT * FROM databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.nfhs_5_district_health_indicators;

-- COMMAND ----------

-- bronze.census_2011_district
CREATE OR REPLACE TABLE workspace.vf_bronze.census_2011_district AS
SELECT `District code` AS district_code, `State name` AS state_name,
       `District name` AS district_name, Population AS population,
       Male, Female, Literate, Households, Urban_Households, Rural_Households
FROM read_files('/Volumes/workspace/vf_bronze/files/india_census_2011.csv',
  format => 'csv', header => true, inferSchema => true);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🥈 SILVER — cleaned, normalized, geo-resolved

-- COMMAND ----------

-- silver.geo_census
CREATE OR REPLACE TABLE workspace.vf_silver.geo_census AS
SELECT district_code, district_name AS district_raw,
  CASE regexp_replace(regexp_replace(lower(trim(district_name)), '\\s*\\(.*\\)', ''), '[^a-z ]','')
    WHEN 'bangalore' THEN 'bengaluru urban'
    WHEN 'gurgaon'   THEN 'gurugram'
    WHEN 'allahabad' THEN 'prayagraj'
    ELSE regexp_replace(regexp_replace(lower(trim(district_name)), '\\s*\\(.*\\)', ''), '[^a-z ]','') END                AS district_norm,
  state_name AS state_raw,
  CASE regexp_replace(regexp_replace(lower(trim(state_name)), '\\s*&\\s*', ' and '), '\\s+', ' ') WHEN 'maharastra' THEN 'maharashtra' WHEN 'nct of delhi' THEN 'delhi' WHEN 'orissa' THEN 'odisha' WHEN 'pondicherry' THEN 'puducherry' WHEN 'uttaranchal' THEN 'uttarakhand' ELSE regexp_replace(regexp_replace(lower(trim(state_name)), '\\s*&\\s*', ' and '), '\\s+', ' ') END         AS state_norm,
  population
FROM workspace.vf_bronze.census_2011_district;

-- COMMAND ----------

-- silver.geo_pincode
CREATE OR REPLACE TABLE workspace.vf_silver.geo_pincode AS
SELECT pincode,
       trim(district)               AS district,
       lower(trim(district))        AS district_norm,
       statename                    AS state_raw,
       CASE regexp_replace(regexp_replace(lower(trim(statename)), '\\s*&\\s*', ' and '), '\\s+', ' ') WHEN 'maharastra' THEN 'maharashtra' WHEN 'nct of delhi' THEN 'delhi' WHEN 'orissa' THEN 'odisha' WHEN 'pondicherry' THEN 'puducherry' WHEN 'uttaranchal' THEN 'uttarakhand' ELSE regexp_replace(regexp_replace(lower(trim(statename)), '\\s*&\\s*', ' and '), '\\s+', ' ') END    AS state_norm
FROM (
  SELECT pincode, district, statename,
         ROW_NUMBER() OVER (PARTITION BY pincode ORDER BY district) rn
  FROM workspace.vf_bronze.pincode_directory
  WHERE pincode IS NOT NULL AND district IS NOT NULL
) WHERE rn = 1;

-- COMMAND ----------

-- silver.geo_office
CREATE OR REPLACE TABLE workspace.vf_silver.geo_office AS
SELECT office_norm, state_norm, district FROM (
  SELECT
    regexp_replace(lower(trim(officename)),
      '\\s+(b\\.?o\\.?|h\\.?o\\.?|s\\.?o\\.?|g\\.?p\\.?o\\.?|p\\.?o\\.?|b/o|h/o|s/o)\\s*$','')
      AS office_norm,
    CASE regexp_replace(regexp_replace(lower(trim(statename)), '\\s*&\\s*', ' and '), '\\s+', ' ') WHEN 'maharastra' THEN 'maharashtra' WHEN 'nct of delhi' THEN 'delhi' WHEN 'orissa' THEN 'odisha' WHEN 'pondicherry' THEN 'puducherry' WHEN 'uttaranchal' THEN 'uttarakhand' ELSE regexp_replace(regexp_replace(lower(trim(statename)), '\\s*&\\s*', ' and '), '\\s+', ' ') END AS state_norm,
    trim(district)            AS district,
    ROW_NUMBER() OVER (PARTITION BY
      regexp_replace(lower(trim(officename)),
        '\\s+(b\\.?o\\.?|h\\.?o\\.?|s\\.?o\\.?|g\\.?p\\.?o\\.?|p\\.?o\\.?|b/o|h/o|s/o)\\s*$',''),
      CASE regexp_replace(regexp_replace(lower(trim(statename)), '\\s*&\\s*', ' and '), '\\s+', ' ') WHEN 'maharastra' THEN 'maharashtra' WHEN 'nct of delhi' THEN 'delhi' WHEN 'orissa' THEN 'odisha' WHEN 'pondicherry' THEN 'puducherry' WHEN 'uttaranchal' THEN 'uttarakhand' ELSE regexp_replace(regexp_replace(lower(trim(statename)), '\\s*&\\s*', ' and '), '\\s+', ' ') END ORDER BY trim(district)) rn
  FROM workspace.vf_bronze.pincode_directory
  WHERE officename IS NOT NULL AND officename <> '' AND district IS NOT NULL
) WHERE rn = 1;

-- COMMAND ----------

-- silver.nfhs
CREATE OR REPLACE TABLE workspace.vf_silver.nfhs AS
SELECT district_name,
       lower(trim(district_name))     AS district_norm,
       state_ut                       AS state_raw,
       CASE regexp_replace(regexp_replace(lower(trim(state_ut)), '\\s*&\\s*', ' and '), '\\s+', ' ') WHEN 'maharastra' THEN 'maharashtra' WHEN 'nct of delhi' THEN 'delhi' WHEN 'orissa' THEN 'odisha' WHEN 'pondicherry' THEN 'puducherry' WHEN 'uttaranchal' THEN 'uttarakhand' ELSE regexp_replace(regexp_replace(lower(trim(state_ut)), '\\s*&\\s*', ' and '), '\\s+', ' ') END       AS state_norm,
       institutional_birth_5y_pct,
       hh_member_covered_health_insurance_pct,
       all_w15_49_who_are_anaemic_pct,
       w15_plus_with_high_bp_sys_gte_140_mmhg_and_or_dia_gte_90_mm_pct,
       prev_diarrhoea_2wk_child_u5_pct,
       births_delivered_by_csection_5y_pct,
       births_attended_by_skilled_hp_5y_10_pct
FROM workspace.vf_bronze.nfhs_district_health;

-- COMMAND ----------

-- silver.facilities_base
CREATE OR REPLACE TABLE workspace.vf_silver.facilities_base AS
SELECT
  f.unique_id                                   AS facility_id,
  f.name                                        AS facility_name,
  f.facilityTypeId                              AS facility_type,
  f.operatorTypeId                              AS operator_type,
  trim(f.address_city)                          AS city,
  lower(trim(f.address_city))                   AS city_norm,
  f.address_stateOrRegion                       AS state_raw,
  CASE regexp_replace(regexp_replace(lower(trim(f.address_stateOrRegion)), '\\s*&\\s*', ' and '), '\\s+', ' ') WHEN 'maharastra' THEN 'maharashtra' WHEN 'nct of delhi' THEN 'delhi' WHEN 'orissa' THEN 'odisha' WHEN 'pondicherry' THEN 'puducherry' WHEN 'uttaranchal' THEN 'uttarakhand' ELSE regexp_replace(regexp_replace(lower(trim(f.address_stateOrRegion)), '\\s*&\\s*', ' and '), '\\s+', ' ') END       AS state_norm,
  try_cast(regexp_extract(f.address_zipOrPostcode,'([0-9]{6})',1) AS BIGINT) AS pincode,
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
FROM workspace.vf_bronze.facilities f
LEFT JOIN workspace.vf_silver.geo_pincode p ON f.address_zipOrPostcode RLIKE '[0-9]{6}'
     AND try_cast(regexp_extract(f.address_zipOrPostcode,'([0-9]{6})',1) AS BIGINT) = p.pincode;

-- COMMAND ----------

-- silver.geo_city_district
CREATE OR REPLACE TABLE workspace.vf_silver.geo_city_district AS
SELECT city_norm, state_norm, district FROM (
  SELECT city_norm, state_norm, district_pin AS district,
         ROW_NUMBER() OVER (PARTITION BY city_norm, state_norm ORDER BY district_pin) rn
  FROM workspace.vf_silver.facilities_base
  WHERE district_pin IS NOT NULL AND city_norm <> '' AND state_norm <> ''
) WHERE rn = 1;

-- COMMAND ----------

-- silver.facilities
CREATE OR REPLACE TABLE workspace.vf_silver.facilities AS
SELECT b.*,
  coalesce(b.district_pin, cd.district, of.district)                  AS district,
  CASE WHEN b.district_pin IS NOT NULL THEN 'pin'
       WHEN cd.district    IS NOT NULL THEN 'city'
       WHEN of.district    IS NOT NULL THEN 'office'
       ELSE NULL END                                                  AS district_source,
  coalesce(b.district_state_pin, b.state_norm)                        AS district_state_norm,
  lower(concat_ws(' ', coalesce(b.capability_raw,''), coalesce(b.procedure_raw,''),
                       coalesce(b.equipment_raw,''), coalesce(b.description,''))) AS combined_text
FROM workspace.vf_silver.facilities_base b
LEFT JOIN workspace.vf_silver.geo_city_district cd ON b.city_norm=cd.city_norm AND b.state_norm=cd.state_norm
LEFT JOIN workspace.vf_silver.geo_office of        ON b.city_norm=of.office_norm AND b.state_norm=of.state_norm;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🥇 GOLD — curated facility + aggregated district/state tables

-- COMMAND ----------

-- gold.facility_capability
CREATE OR REPLACE TABLE workspace.vf_gold.facility_capability
COMMENT 'One row per facility x capability with trust signal + evidence citation (mirrors app_simple.py scoring)' AS
WITH scored AS (

SELECT facility_id, facility_name, facility_type, city, state_raw, state_norm,
       district, district_source, district_state_norm, pincode, latitude, longitude, coords_valid,
       'ICU' AS capability,
       least((CASE WHEN specialties_raw LIKE '%"criticalCareMedicine"%' OR specialties_raw LIKE '%''criticalCareMedicine''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"pulmonologyCriticalCare"%' OR specialties_raw LIKE '%''pulmonologyCriticalCare''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"anesthesiaCriticalCare"%' OR specialties_raw LIKE '%''anesthesiaCriticalCare''%' THEN 1 ELSE 0 END), 3) * 2.0                              AS spec_score,
       ((CASE WHEN lower(coalesce(capability_raw,'')) RLIKE 'icu|intensive care|critical care|ventilator|icu bed' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(procedure_raw,''))  RLIKE 'icu|intensive care|critical care|ventilator|icu bed' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(equipment_raw,''))  RLIKE 'icu|intensive care|critical care|ventilator|icu bed' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(description,''))     RLIKE 'icu|intensive care|critical care|ventilator|icu bed' THEN 1 ELSE 0 END))                              AS n_field_hits,
       least(((CASE WHEN lower(coalesce(capability_raw,'')) RLIKE 'icu|intensive care|critical care|ventilator|icu bed' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(procedure_raw,''))  RLIKE 'icu|intensive care|critical care|ventilator|icu bed' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(equipment_raw,''))  RLIKE 'icu|intensive care|critical care|ventilator|icu bed' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(description,''))     RLIKE 'icu|intensive care|critical care|ventilator|icu bed' THEN 1 ELSE 0 END)) * 1.5, 4.0)            AS text_score,
       (CASE WHEN has_staff THEN 0.5 ELSE 0 END)
         + (CASE WHEN has_logo THEN 0.25 ELSE 0 END)
         + (CASE WHEN social_count>=2 THEN 0.5 WHEN social_count=1 THEN 0.2 ELSE 0 END)
         + (CASE WHEN n_source_types>=2 THEN 0.25 ELSE 0 END)
         + (CASE WHEN number_doctors>0 THEN 0.5 ELSE 0 END)
         + (CASE WHEN capacity>0 THEN 0.25 ELSE 0 END)          AS quality_score,
       regexp_extract(combined_text, '(.{0,55}(icu|intensive care|critical care|ventilator|icu bed).{0,110})', 1) AS evidence_snippet,
       concat_ws(',',
         CASE WHEN (CASE WHEN lower(coalesce(capability_raw,'')) RLIKE 'icu|intensive care|critical care|ventilator|icu bed' THEN 1 ELSE 0 END)=1 THEN 'capability' END,
         CASE WHEN (CASE WHEN lower(coalesce(procedure_raw,''))  RLIKE 'icu|intensive care|critical care|ventilator|icu bed' THEN 1 ELSE 0 END)=1 THEN 'procedure'  END,
         CASE WHEN (CASE WHEN lower(coalesce(equipment_raw,''))  RLIKE 'icu|intensive care|critical care|ventilator|icu bed' THEN 1 ELSE 0 END)=1  THEN 'equipment'  END,
         CASE WHEN (CASE WHEN lower(coalesce(description,''))     RLIKE 'icu|intensive care|critical care|ventilator|icu bed' THEN 1 ELSE 0 END)=1 THEN 'description' END)             AS evidence_fields
FROM workspace.vf_silver.facilities

UNION ALL

SELECT facility_id, facility_name, facility_type, city, state_raw, state_norm,
       district, district_source, district_state_norm, pincode, latitude, longitude, coords_valid,
       'Maternity' AS capability,
       least((CASE WHEN specialties_raw LIKE '%"gynecologyAndObstetrics"%' OR specialties_raw LIKE '%''gynecologyAndObstetrics''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"maternalAndFetalMedicine"%' OR specialties_raw LIKE '%''maternalAndFetalMedicine''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"reproductiveEndocrinologyAndInfertility"%' OR specialties_raw LIKE '%''reproductiveEndocrinologyAndInfertility''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"neonatologyPerinatalMedicine"%' OR specialties_raw LIKE '%''neonatologyPerinatalMedicine''%' THEN 1 ELSE 0 END), 3) * 2.0                              AS spec_score,
       ((CASE WHEN lower(coalesce(capability_raw,'')) RLIKE 'maternity|obstetric|gynecol|delivery|labour|labor|prenatal|antenatal|postnatal|birth' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(procedure_raw,''))  RLIKE 'maternity|obstetric|gynecol|delivery|labour|labor|prenatal|antenatal|postnatal|birth' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(equipment_raw,''))  RLIKE 'maternity|obstetric|gynecol|delivery|labour|labor|prenatal|antenatal|postnatal|birth' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(description,''))     RLIKE 'maternity|obstetric|gynecol|delivery|labour|labor|prenatal|antenatal|postnatal|birth' THEN 1 ELSE 0 END))                              AS n_field_hits,
       least(((CASE WHEN lower(coalesce(capability_raw,'')) RLIKE 'maternity|obstetric|gynecol|delivery|labour|labor|prenatal|antenatal|postnatal|birth' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(procedure_raw,''))  RLIKE 'maternity|obstetric|gynecol|delivery|labour|labor|prenatal|antenatal|postnatal|birth' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(equipment_raw,''))  RLIKE 'maternity|obstetric|gynecol|delivery|labour|labor|prenatal|antenatal|postnatal|birth' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(description,''))     RLIKE 'maternity|obstetric|gynecol|delivery|labour|labor|prenatal|antenatal|postnatal|birth' THEN 1 ELSE 0 END)) * 1.5, 4.0)            AS text_score,
       (CASE WHEN has_staff THEN 0.5 ELSE 0 END)
         + (CASE WHEN has_logo THEN 0.25 ELSE 0 END)
         + (CASE WHEN social_count>=2 THEN 0.5 WHEN social_count=1 THEN 0.2 ELSE 0 END)
         + (CASE WHEN n_source_types>=2 THEN 0.25 ELSE 0 END)
         + (CASE WHEN number_doctors>0 THEN 0.5 ELSE 0 END)
         + (CASE WHEN capacity>0 THEN 0.25 ELSE 0 END)          AS quality_score,
       regexp_extract(combined_text, '(.{0,55}(maternity|obstetric|gynecol|delivery|labour|labor|prenatal|antenatal|postnatal|birth).{0,110})', 1) AS evidence_snippet,
       concat_ws(',',
         CASE WHEN (CASE WHEN lower(coalesce(capability_raw,'')) RLIKE 'maternity|obstetric|gynecol|delivery|labour|labor|prenatal|antenatal|postnatal|birth' THEN 1 ELSE 0 END)=1 THEN 'capability' END,
         CASE WHEN (CASE WHEN lower(coalesce(procedure_raw,''))  RLIKE 'maternity|obstetric|gynecol|delivery|labour|labor|prenatal|antenatal|postnatal|birth' THEN 1 ELSE 0 END)=1 THEN 'procedure'  END,
         CASE WHEN (CASE WHEN lower(coalesce(equipment_raw,''))  RLIKE 'maternity|obstetric|gynecol|delivery|labour|labor|prenatal|antenatal|postnatal|birth' THEN 1 ELSE 0 END)=1  THEN 'equipment'  END,
         CASE WHEN (CASE WHEN lower(coalesce(description,''))     RLIKE 'maternity|obstetric|gynecol|delivery|labour|labor|prenatal|antenatal|postnatal|birth' THEN 1 ELSE 0 END)=1 THEN 'description' END)             AS evidence_fields
FROM workspace.vf_silver.facilities

UNION ALL

SELECT facility_id, facility_name, facility_type, city, state_raw, state_norm,
       district, district_source, district_state_norm, pincode, latitude, longitude, coords_valid,
       'Emergency' AS capability,
       least((CASE WHEN specialties_raw LIKE '%"emergencyMedicine"%' OR specialties_raw LIKE '%''emergencyMedicine''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"pediatricEmergencyMedicine"%' OR specialties_raw LIKE '%''pediatricEmergencyMedicine''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"traumaSurgery"%' OR specialties_raw LIKE '%''traumaSurgery''%' THEN 1 ELSE 0 END), 3) * 2.0                              AS spec_score,
       ((CASE WHEN lower(coalesce(capability_raw,'')) RLIKE 'emergency|trauma|ambulance|casualty|accident' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(procedure_raw,''))  RLIKE 'emergency|trauma|ambulance|casualty|accident' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(equipment_raw,''))  RLIKE 'emergency|trauma|ambulance|casualty|accident' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(description,''))     RLIKE 'emergency|trauma|ambulance|casualty|accident' THEN 1 ELSE 0 END))                              AS n_field_hits,
       least(((CASE WHEN lower(coalesce(capability_raw,'')) RLIKE 'emergency|trauma|ambulance|casualty|accident' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(procedure_raw,''))  RLIKE 'emergency|trauma|ambulance|casualty|accident' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(equipment_raw,''))  RLIKE 'emergency|trauma|ambulance|casualty|accident' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(description,''))     RLIKE 'emergency|trauma|ambulance|casualty|accident' THEN 1 ELSE 0 END)) * 1.5, 4.0)            AS text_score,
       (CASE WHEN has_staff THEN 0.5 ELSE 0 END)
         + (CASE WHEN has_logo THEN 0.25 ELSE 0 END)
         + (CASE WHEN social_count>=2 THEN 0.5 WHEN social_count=1 THEN 0.2 ELSE 0 END)
         + (CASE WHEN n_source_types>=2 THEN 0.25 ELSE 0 END)
         + (CASE WHEN number_doctors>0 THEN 0.5 ELSE 0 END)
         + (CASE WHEN capacity>0 THEN 0.25 ELSE 0 END)          AS quality_score,
       regexp_extract(combined_text, '(.{0,55}(emergency|trauma|ambulance|casualty|accident).{0,110})', 1) AS evidence_snippet,
       concat_ws(',',
         CASE WHEN (CASE WHEN lower(coalesce(capability_raw,'')) RLIKE 'emergency|trauma|ambulance|casualty|accident' THEN 1 ELSE 0 END)=1 THEN 'capability' END,
         CASE WHEN (CASE WHEN lower(coalesce(procedure_raw,''))  RLIKE 'emergency|trauma|ambulance|casualty|accident' THEN 1 ELSE 0 END)=1 THEN 'procedure'  END,
         CASE WHEN (CASE WHEN lower(coalesce(equipment_raw,''))  RLIKE 'emergency|trauma|ambulance|casualty|accident' THEN 1 ELSE 0 END)=1  THEN 'equipment'  END,
         CASE WHEN (CASE WHEN lower(coalesce(description,''))     RLIKE 'emergency|trauma|ambulance|casualty|accident' THEN 1 ELSE 0 END)=1 THEN 'description' END)             AS evidence_fields
FROM workspace.vf_silver.facilities

UNION ALL

SELECT facility_id, facility_name, facility_type, city, state_raw, state_norm,
       district, district_source, district_state_norm, pincode, latitude, longitude, coords_valid,
       'Dialysis' AS capability,
       least((CASE WHEN specialties_raw LIKE '%"nephrology"%' OR specialties_raw LIKE '%''nephrology''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"pediatricNephrology"%' OR specialties_raw LIKE '%''pediatricNephrology''%' THEN 1 ELSE 0 END), 3) * 2.0                              AS spec_score,
       ((CASE WHEN lower(coalesce(capability_raw,'')) RLIKE 'dialysis|hemodialysis|renal|nephrology' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(procedure_raw,''))  RLIKE 'dialysis|hemodialysis|renal|nephrology' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(equipment_raw,''))  RLIKE 'dialysis|hemodialysis|renal|nephrology' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(description,''))     RLIKE 'dialysis|hemodialysis|renal|nephrology' THEN 1 ELSE 0 END))                              AS n_field_hits,
       least(((CASE WHEN lower(coalesce(capability_raw,'')) RLIKE 'dialysis|hemodialysis|renal|nephrology' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(procedure_raw,''))  RLIKE 'dialysis|hemodialysis|renal|nephrology' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(equipment_raw,''))  RLIKE 'dialysis|hemodialysis|renal|nephrology' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(description,''))     RLIKE 'dialysis|hemodialysis|renal|nephrology' THEN 1 ELSE 0 END)) * 1.5, 4.0)            AS text_score,
       (CASE WHEN has_staff THEN 0.5 ELSE 0 END)
         + (CASE WHEN has_logo THEN 0.25 ELSE 0 END)
         + (CASE WHEN social_count>=2 THEN 0.5 WHEN social_count=1 THEN 0.2 ELSE 0 END)
         + (CASE WHEN n_source_types>=2 THEN 0.25 ELSE 0 END)
         + (CASE WHEN number_doctors>0 THEN 0.5 ELSE 0 END)
         + (CASE WHEN capacity>0 THEN 0.25 ELSE 0 END)          AS quality_score,
       regexp_extract(combined_text, '(.{0,55}(dialysis|hemodialysis|renal|nephrology).{0,110})', 1) AS evidence_snippet,
       concat_ws(',',
         CASE WHEN (CASE WHEN lower(coalesce(capability_raw,'')) RLIKE 'dialysis|hemodialysis|renal|nephrology' THEN 1 ELSE 0 END)=1 THEN 'capability' END,
         CASE WHEN (CASE WHEN lower(coalesce(procedure_raw,''))  RLIKE 'dialysis|hemodialysis|renal|nephrology' THEN 1 ELSE 0 END)=1 THEN 'procedure'  END,
         CASE WHEN (CASE WHEN lower(coalesce(equipment_raw,''))  RLIKE 'dialysis|hemodialysis|renal|nephrology' THEN 1 ELSE 0 END)=1  THEN 'equipment'  END,
         CASE WHEN (CASE WHEN lower(coalesce(description,''))     RLIKE 'dialysis|hemodialysis|renal|nephrology' THEN 1 ELSE 0 END)=1 THEN 'description' END)             AS evidence_fields
FROM workspace.vf_silver.facilities

UNION ALL

SELECT facility_id, facility_name, facility_type, city, state_raw, state_norm,
       district, district_source, district_state_norm, pincode, latitude, longitude, coords_valid,
       'Oncology' AS capability,
       least((CASE WHEN specialties_raw LIKE '%"oncology"%' OR specialties_raw LIKE '%''oncology''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"radiationOncology"%' OR specialties_raw LIKE '%''radiationOncology''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"surgicalOncology"%' OR specialties_raw LIKE '%''surgicalOncology''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"hematologyOncology"%' OR specialties_raw LIKE '%''hematologyOncology''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"pediatricHematologyOncology"%' OR specialties_raw LIKE '%''pediatricHematologyOncology''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"gynecologicOncology"%' OR specialties_raw LIKE '%''gynecologicOncology''%' THEN 1 ELSE 0 END), 3) * 2.0                              AS spec_score,
       ((CASE WHEN lower(coalesce(capability_raw,'')) RLIKE 'oncology|cancer|chemotherapy|radiation therapy|radiotherapy|tumor' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(procedure_raw,''))  RLIKE 'oncology|cancer|chemotherapy|radiation therapy|radiotherapy|tumor' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(equipment_raw,''))  RLIKE 'oncology|cancer|chemotherapy|radiation therapy|radiotherapy|tumor' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(description,''))     RLIKE 'oncology|cancer|chemotherapy|radiation therapy|radiotherapy|tumor' THEN 1 ELSE 0 END))                              AS n_field_hits,
       least(((CASE WHEN lower(coalesce(capability_raw,'')) RLIKE 'oncology|cancer|chemotherapy|radiation therapy|radiotherapy|tumor' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(procedure_raw,''))  RLIKE 'oncology|cancer|chemotherapy|radiation therapy|radiotherapy|tumor' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(equipment_raw,''))  RLIKE 'oncology|cancer|chemotherapy|radiation therapy|radiotherapy|tumor' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(description,''))     RLIKE 'oncology|cancer|chemotherapy|radiation therapy|radiotherapy|tumor' THEN 1 ELSE 0 END)) * 1.5, 4.0)            AS text_score,
       (CASE WHEN has_staff THEN 0.5 ELSE 0 END)
         + (CASE WHEN has_logo THEN 0.25 ELSE 0 END)
         + (CASE WHEN social_count>=2 THEN 0.5 WHEN social_count=1 THEN 0.2 ELSE 0 END)
         + (CASE WHEN n_source_types>=2 THEN 0.25 ELSE 0 END)
         + (CASE WHEN number_doctors>0 THEN 0.5 ELSE 0 END)
         + (CASE WHEN capacity>0 THEN 0.25 ELSE 0 END)          AS quality_score,
       regexp_extract(combined_text, '(.{0,55}(oncology|cancer|chemotherapy|radiation therapy|radiotherapy|tumor).{0,110})', 1) AS evidence_snippet,
       concat_ws(',',
         CASE WHEN (CASE WHEN lower(coalesce(capability_raw,'')) RLIKE 'oncology|cancer|chemotherapy|radiation therapy|radiotherapy|tumor' THEN 1 ELSE 0 END)=1 THEN 'capability' END,
         CASE WHEN (CASE WHEN lower(coalesce(procedure_raw,''))  RLIKE 'oncology|cancer|chemotherapy|radiation therapy|radiotherapy|tumor' THEN 1 ELSE 0 END)=1 THEN 'procedure'  END,
         CASE WHEN (CASE WHEN lower(coalesce(equipment_raw,''))  RLIKE 'oncology|cancer|chemotherapy|radiation therapy|radiotherapy|tumor' THEN 1 ELSE 0 END)=1  THEN 'equipment'  END,
         CASE WHEN (CASE WHEN lower(coalesce(description,''))     RLIKE 'oncology|cancer|chemotherapy|radiation therapy|radiotherapy|tumor' THEN 1 ELSE 0 END)=1 THEN 'description' END)             AS evidence_fields
FROM workspace.vf_silver.facilities

UNION ALL

SELECT facility_id, facility_name, facility_type, city, state_raw, state_norm,
       district, district_source, district_state_norm, pincode, latitude, longitude, coords_valid,
       'Cardiology' AS capability,
       least((CASE WHEN specialties_raw LIKE '%"cardiology"%' OR specialties_raw LIKE '%''cardiology''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"interventionalCardiology"%' OR specialties_raw LIKE '%''interventionalCardiology''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"pediatricCardiology"%' OR specialties_raw LIKE '%''pediatricCardiology''%' THEN 1 ELSE 0 END), 3) * 2.0                              AS spec_score,
       ((CASE WHEN lower(coalesce(capability_raw,'')) RLIKE 'cardiology|cardiac|coronary|angioplasty|echocardiography' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(procedure_raw,''))  RLIKE 'cardiology|cardiac|coronary|angioplasty|echocardiography' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(equipment_raw,''))  RLIKE 'cardiology|cardiac|coronary|angioplasty|echocardiography' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(description,''))     RLIKE 'cardiology|cardiac|coronary|angioplasty|echocardiography' THEN 1 ELSE 0 END))                              AS n_field_hits,
       least(((CASE WHEN lower(coalesce(capability_raw,'')) RLIKE 'cardiology|cardiac|coronary|angioplasty|echocardiography' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(procedure_raw,''))  RLIKE 'cardiology|cardiac|coronary|angioplasty|echocardiography' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(equipment_raw,''))  RLIKE 'cardiology|cardiac|coronary|angioplasty|echocardiography' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(description,''))     RLIKE 'cardiology|cardiac|coronary|angioplasty|echocardiography' THEN 1 ELSE 0 END)) * 1.5, 4.0)            AS text_score,
       (CASE WHEN has_staff THEN 0.5 ELSE 0 END)
         + (CASE WHEN has_logo THEN 0.25 ELSE 0 END)
         + (CASE WHEN social_count>=2 THEN 0.5 WHEN social_count=1 THEN 0.2 ELSE 0 END)
         + (CASE WHEN n_source_types>=2 THEN 0.25 ELSE 0 END)
         + (CASE WHEN number_doctors>0 THEN 0.5 ELSE 0 END)
         + (CASE WHEN capacity>0 THEN 0.25 ELSE 0 END)          AS quality_score,
       regexp_extract(combined_text, '(.{0,55}(cardiology|cardiac|coronary|angioplasty|echocardiography).{0,110})', 1) AS evidence_snippet,
       concat_ws(',',
         CASE WHEN (CASE WHEN lower(coalesce(capability_raw,'')) RLIKE 'cardiology|cardiac|coronary|angioplasty|echocardiography' THEN 1 ELSE 0 END)=1 THEN 'capability' END,
         CASE WHEN (CASE WHEN lower(coalesce(procedure_raw,''))  RLIKE 'cardiology|cardiac|coronary|angioplasty|echocardiography' THEN 1 ELSE 0 END)=1 THEN 'procedure'  END,
         CASE WHEN (CASE WHEN lower(coalesce(equipment_raw,''))  RLIKE 'cardiology|cardiac|coronary|angioplasty|echocardiography' THEN 1 ELSE 0 END)=1  THEN 'equipment'  END,
         CASE WHEN (CASE WHEN lower(coalesce(description,''))     RLIKE 'cardiology|cardiac|coronary|angioplasty|echocardiography' THEN 1 ELSE 0 END)=1 THEN 'description' END)             AS evidence_fields
FROM workspace.vf_silver.facilities

UNION ALL

SELECT facility_id, facility_name, facility_type, city, state_raw, state_norm,
       district, district_source, district_state_norm, pincode, latitude, longitude, coords_valid,
       'Pediatrics' AS capability,
       least((CASE WHEN specialties_raw LIKE '%"pediatrics"%' OR specialties_raw LIKE '%''pediatrics''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"neonatologyPerinatalMedicine"%' OR specialties_raw LIKE '%''neonatologyPerinatalMedicine''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"pediatricSurgery"%' OR specialties_raw LIKE '%''pediatricSurgery''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"pediatricOrthopedicSurgery"%' OR specialties_raw LIKE '%''pediatricOrthopedicSurgery''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"pediatricEmergencyMedicine"%' OR specialties_raw LIKE '%''pediatricEmergencyMedicine''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"pediatricNephrology"%' OR specialties_raw LIKE '%''pediatricNephrology''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"pediatricHematologyOncology"%' OR specialties_raw LIKE '%''pediatricHematologyOncology''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"pediatricCardiology"%' OR specialties_raw LIKE '%''pediatricCardiology''%' THEN 1 ELSE 0 END), 3) * 2.0                              AS spec_score,
       ((CASE WHEN lower(coalesce(capability_raw,'')) RLIKE 'pediatric|paediatric|nicu|neonatal|children|child health' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(procedure_raw,''))  RLIKE 'pediatric|paediatric|nicu|neonatal|children|child health' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(equipment_raw,''))  RLIKE 'pediatric|paediatric|nicu|neonatal|children|child health' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(description,''))     RLIKE 'pediatric|paediatric|nicu|neonatal|children|child health' THEN 1 ELSE 0 END))                              AS n_field_hits,
       least(((CASE WHEN lower(coalesce(capability_raw,'')) RLIKE 'pediatric|paediatric|nicu|neonatal|children|child health' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(procedure_raw,''))  RLIKE 'pediatric|paediatric|nicu|neonatal|children|child health' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(equipment_raw,''))  RLIKE 'pediatric|paediatric|nicu|neonatal|children|child health' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(description,''))     RLIKE 'pediatric|paediatric|nicu|neonatal|children|child health' THEN 1 ELSE 0 END)) * 1.5, 4.0)            AS text_score,
       (CASE WHEN has_staff THEN 0.5 ELSE 0 END)
         + (CASE WHEN has_logo THEN 0.25 ELSE 0 END)
         + (CASE WHEN social_count>=2 THEN 0.5 WHEN social_count=1 THEN 0.2 ELSE 0 END)
         + (CASE WHEN n_source_types>=2 THEN 0.25 ELSE 0 END)
         + (CASE WHEN number_doctors>0 THEN 0.5 ELSE 0 END)
         + (CASE WHEN capacity>0 THEN 0.25 ELSE 0 END)          AS quality_score,
       regexp_extract(combined_text, '(.{0,55}(pediatric|paediatric|nicu|neonatal|children|child health).{0,110})', 1) AS evidence_snippet,
       concat_ws(',',
         CASE WHEN (CASE WHEN lower(coalesce(capability_raw,'')) RLIKE 'pediatric|paediatric|nicu|neonatal|children|child health' THEN 1 ELSE 0 END)=1 THEN 'capability' END,
         CASE WHEN (CASE WHEN lower(coalesce(procedure_raw,''))  RLIKE 'pediatric|paediatric|nicu|neonatal|children|child health' THEN 1 ELSE 0 END)=1 THEN 'procedure'  END,
         CASE WHEN (CASE WHEN lower(coalesce(equipment_raw,''))  RLIKE 'pediatric|paediatric|nicu|neonatal|children|child health' THEN 1 ELSE 0 END)=1  THEN 'equipment'  END,
         CASE WHEN (CASE WHEN lower(coalesce(description,''))     RLIKE 'pediatric|paediatric|nicu|neonatal|children|child health' THEN 1 ELSE 0 END)=1 THEN 'description' END)             AS evidence_fields
FROM workspace.vf_silver.facilities

UNION ALL

SELECT facility_id, facility_name, facility_type, city, state_raw, state_norm,
       district, district_source, district_state_norm, pincode, latitude, longitude, coords_valid,
       'Surgery' AS capability,
       least((CASE WHEN specialties_raw LIKE '%"generalSurgery"%' OR specialties_raw LIKE '%''generalSurgery''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"traumaSurgery"%' OR specialties_raw LIKE '%''traumaSurgery''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"neurosurgery"%' OR specialties_raw LIKE '%''neurosurgery''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"orthopedicSurgery"%' OR specialties_raw LIKE '%''orthopedicSurgery''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"plasticSurgery"%' OR specialties_raw LIKE '%''plasticSurgery''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"urology"%' OR specialties_raw LIKE '%''urology''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"spineNeurosurgery"%' OR specialties_raw LIKE '%''spineNeurosurgery''%' THEN 1 ELSE 0 END) + (CASE WHEN specialties_raw LIKE '%"jointReconstructionSurgery"%' OR specialties_raw LIKE '%''jointReconstructionSurgery''%' THEN 1 ELSE 0 END), 3) * 2.0                              AS spec_score,
       ((CASE WHEN lower(coalesce(capability_raw,'')) RLIKE 'surgery|surgical|operation theater|laparoscopic|general surgery' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(procedure_raw,''))  RLIKE 'surgery|surgical|operation theater|laparoscopic|general surgery' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(equipment_raw,''))  RLIKE 'surgery|surgical|operation theater|laparoscopic|general surgery' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(description,''))     RLIKE 'surgery|surgical|operation theater|laparoscopic|general surgery' THEN 1 ELSE 0 END))                              AS n_field_hits,
       least(((CASE WHEN lower(coalesce(capability_raw,'')) RLIKE 'surgery|surgical|operation theater|laparoscopic|general surgery' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(procedure_raw,''))  RLIKE 'surgery|surgical|operation theater|laparoscopic|general surgery' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(equipment_raw,''))  RLIKE 'surgery|surgical|operation theater|laparoscopic|general surgery' THEN 1 ELSE 0 END)+(CASE WHEN lower(coalesce(description,''))     RLIKE 'surgery|surgical|operation theater|laparoscopic|general surgery' THEN 1 ELSE 0 END)) * 1.5, 4.0)            AS text_score,
       (CASE WHEN has_staff THEN 0.5 ELSE 0 END)
         + (CASE WHEN has_logo THEN 0.25 ELSE 0 END)
         + (CASE WHEN social_count>=2 THEN 0.5 WHEN social_count=1 THEN 0.2 ELSE 0 END)
         + (CASE WHEN n_source_types>=2 THEN 0.25 ELSE 0 END)
         + (CASE WHEN number_doctors>0 THEN 0.5 ELSE 0 END)
         + (CASE WHEN capacity>0 THEN 0.25 ELSE 0 END)          AS quality_score,
       regexp_extract(combined_text, '(.{0,55}(surgery|surgical|operation theater|laparoscopic|general surgery).{0,110})', 1) AS evidence_snippet,
       concat_ws(',',
         CASE WHEN (CASE WHEN lower(coalesce(capability_raw,'')) RLIKE 'surgery|surgical|operation theater|laparoscopic|general surgery' THEN 1 ELSE 0 END)=1 THEN 'capability' END,
         CASE WHEN (CASE WHEN lower(coalesce(procedure_raw,''))  RLIKE 'surgery|surgical|operation theater|laparoscopic|general surgery' THEN 1 ELSE 0 END)=1 THEN 'procedure'  END,
         CASE WHEN (CASE WHEN lower(coalesce(equipment_raw,''))  RLIKE 'surgery|surgical|operation theater|laparoscopic|general surgery' THEN 1 ELSE 0 END)=1  THEN 'equipment'  END,
         CASE WHEN (CASE WHEN lower(coalesce(description,''))     RLIKE 'surgery|surgical|operation theater|laparoscopic|general surgery' THEN 1 ELSE 0 END)=1 THEN 'description' END)             AS evidence_fields
FROM workspace.vf_silver.facilities

)
SELECT *,
  round(spec_score + text_score + quality_score, 2) AS total_score,
  CASE
    WHEN spec_score>=4 OR (spec_score>=2 AND n_field_hits>=2) THEN 'STRONG'
    WHEN spec_score>=2 OR n_field_hits>=2 OR (spec_score>0 AND n_field_hits>=1) THEN 'PARTIAL'
    WHEN spec_score>0 OR n_field_hits>=1 THEN 'WEAK'
    ELSE 'NO CLAIM' END AS trust_level,
  (spec_score>0 OR n_field_hits>=1) AS has_evidence
FROM scored;

-- COMMAND ----------

-- gold.district_capability
CREATE OR REPLACE TABLE workspace.vf_gold.district_capability
COMMENT 'Per district x capability: trust-weighted coverage vs NFHS demand, decomposed gap_risk + confidence (Track 2 headline)' AS
WITH agg AS (
  SELECT district, district_state_norm AS state_norm, capability,
         COUNT(*) AS facility_count,
         SUM(CASE WHEN trust_level='STRONG'   THEN 1 ELSE 0 END) AS strong_count,
         SUM(CASE WHEN trust_level='PARTIAL'  THEN 1 ELSE 0 END) AS partial_count,
         SUM(CASE WHEN trust_level='WEAK'     THEN 1 ELSE 0 END) AS weak_count,
         SUM(CASE WHEN trust_level='NO CLAIM' THEN 1 ELSE 0 END) AS no_claim_count,
         round(AVG(total_score),2) AS avg_score
  FROM workspace.vf_gold.facility_capability
  WHERE district IS NOT NULL
  GROUP BY district, district_state_norm, capability
),
state_avg AS (
  SELECT state_norm, AVG(all_w15_49_who_are_anaemic_pct) AS all_w15_49_who_are_anaemic_pct_avg,
         AVG(births_delivered_by_csection_5y_pct) AS births_delivered_by_csection_5y_pct_avg,
         AVG(hh_member_covered_health_insurance_pct) AS hh_member_covered_health_insurance_pct_avg,
         AVG(institutional_birth_5y_pct) AS institutional_birth_5y_pct_avg,
         AVG(prev_diarrhoea_2wk_child_u5_pct) AS prev_diarrhoea_2wk_child_u5_pct_avg,
         AVG(w15_plus_with_high_bp_sys_gte_140_mmhg_and_or_dia_gte_90_mm_pct) AS w15_plus_with_high_bp_sys_gte_140_mmhg_and_or_dia_gte_90_mm_pct_avg FROM workspace.vf_silver.nfhs GROUP BY state_norm
),
pop AS (
  SELECT district_norm, state_norm, SUM(population) AS population
  FROM workspace.vf_silver.geo_census GROUP BY district_norm, state_norm
),
joined AS (
  SELECT a.*,
    round((a.strong_count + a.partial_count*0.5 + a.weak_count*0.25)/a.facility_count*100, 1) AS coverage_pct,
    (a.strong_count + a.partial_count + a.weak_count) > 0 AS has_evidence,
    nf.district_name IS NOT NULL AS nfhs_matched,
    (CASE capability
    WHEN 'ICU' THEN nf.hh_member_covered_health_insurance_pct
    WHEN 'Maternity' THEN nf.institutional_birth_5y_pct
    WHEN 'Emergency' THEN nf.hh_member_covered_health_insurance_pct
    WHEN 'Dialysis' THEN nf.all_w15_49_who_are_anaemic_pct
    WHEN 'Oncology' THEN nf.hh_member_covered_health_insurance_pct
    WHEN 'Cardiology' THEN nf.w15_plus_with_high_bp_sys_gte_140_mmhg_and_or_dia_gte_90_mm_pct
    WHEN 'Pediatrics' THEN nf.prev_diarrhoea_2wk_child_u5_pct
    WHEN 'Surgery' THEN nf.births_delivered_by_csection_5y_pct
  END) AS nfhs_value,
    round((CASE capability
    WHEN 'ICU' THEN 100 - nf.hh_member_covered_health_insurance_pct
    WHEN 'Maternity' THEN 100 - nf.institutional_birth_5y_pct
    WHEN 'Emergency' THEN 100 - nf.hh_member_covered_health_insurance_pct
    WHEN 'Dialysis' THEN nf.all_w15_49_who_are_anaemic_pct
    WHEN 'Oncology' THEN 100 - nf.hh_member_covered_health_insurance_pct
    WHEN 'Cardiology' THEN nf.w15_plus_with_high_bp_sys_gte_140_mmhg_and_or_dia_gte_90_mm_pct
    WHEN 'Pediatrics' THEN nf.prev_diarrhoea_2wk_child_u5_pct
    WHEN 'Surgery' THEN nf.births_delivered_by_csection_5y_pct
  END),1) AS need_district,
    round((CASE capability
    WHEN 'ICU' THEN 100 - sa.hh_member_covered_health_insurance_pct_avg
    WHEN 'Maternity' THEN 100 - sa.institutional_birth_5y_pct_avg
    WHEN 'Emergency' THEN 100 - sa.hh_member_covered_health_insurance_pct_avg
    WHEN 'Dialysis' THEN sa.all_w15_49_who_are_anaemic_pct_avg
    WHEN 'Oncology' THEN 100 - sa.hh_member_covered_health_insurance_pct_avg
    WHEN 'Cardiology' THEN sa.w15_plus_with_high_bp_sys_gte_140_mmhg_and_or_dia_gte_90_mm_pct_avg
    WHEN 'Pediatrics' THEN sa.prev_diarrhoea_2wk_child_u5_pct_avg
    WHEN 'Surgery' THEN sa.births_delivered_by_csection_5y_pct_avg
  END),1) AS need_state,
    (CASE capability
    WHEN 'ICU' THEN 'Health insurance %'
    WHEN 'Maternity' THEN 'Institutional births %'
    WHEN 'Emergency' THEN 'Health insurance %'
    WHEN 'Dialysis' THEN 'Women anaemia %'
    WHEN 'Oncology' THEN 'Health insurance %'
    WHEN 'Cardiology' THEN 'High BP prevalence %'
    WHEN 'Pediatrics' THEN 'Child diarrhoea %'
    WHEN 'Surgery' THEN 'C-section births %'
  END) AS nfhs_field,
    p.population AS population,
    p.population IS NOT NULL AS population_matched,
    CASE WHEN p.population>0 THEN round(a.facility_count/p.population*100000, 3) END AS facilities_per_100k
  FROM agg a
  LEFT JOIN workspace.vf_silver.nfhs nf
    ON lower(a.district) = nf.district_norm AND a.state_norm = nf.state_norm
  LEFT JOIN state_avg sa ON a.state_norm = sa.state_norm
  LEFT JOIN pop p ON lower(a.district) = p.district_norm AND a.state_norm = p.state_norm
),
scored AS (
  SELECT *,
    coalesce(need_district, need_state) AS need_score,
    (need_district IS NULL AND need_state IS NOT NULL) AS need_imputed,
    CASE
    WHEN facility_count<3 AND NOT has_evidence THEN 'DATA-POOR'
    WHEN coverage_pct<20 AND facility_count>=3 THEN 'CRITICAL GAP'
    WHEN coverage_pct<40 THEN 'HIGH PRIORITY'
    WHEN coverage_pct<65 THEN 'MODERATE'
    ELSE 'ADEQUATE' END AS priority,
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
FROM scored;

-- COMMAND ----------

-- gold.state_capability
CREATE OR REPLACE TABLE workspace.vf_gold.state_capability
COMMENT 'Per state x capability rollup with state-average NFHS demand' AS
WITH agg AS (
  SELECT state_norm, capability,
         COUNT(*) AS facility_count,
         SUM(CASE WHEN trust_level='STRONG'   THEN 1 ELSE 0 END) AS strong_count,
         SUM(CASE WHEN trust_level='PARTIAL'  THEN 1 ELSE 0 END) AS partial_count,
         SUM(CASE WHEN trust_level='WEAK'     THEN 1 ELSE 0 END) AS weak_count,
         SUM(CASE WHEN trust_level='NO CLAIM' THEN 1 ELSE 0 END) AS no_claim_count
  FROM workspace.vf_gold.facility_capability
  GROUP BY state_norm, capability
),
sa AS (
  SELECT state_norm, AVG(all_w15_49_who_are_anaemic_pct) AS all_w15_49_who_are_anaemic_pct_avg,
         AVG(births_delivered_by_csection_5y_pct) AS births_delivered_by_csection_5y_pct_avg,
         AVG(hh_member_covered_health_insurance_pct) AS hh_member_covered_health_insurance_pct_avg,
         AVG(institutional_birth_5y_pct) AS institutional_birth_5y_pct_avg,
         AVG(prev_diarrhoea_2wk_child_u5_pct) AS prev_diarrhoea_2wk_child_u5_pct_avg,
         AVG(w15_plus_with_high_bp_sys_gte_140_mmhg_and_or_dia_gte_90_mm_pct) AS w15_plus_with_high_bp_sys_gte_140_mmhg_and_or_dia_gte_90_mm_pct_avg
  FROM workspace.vf_silver.nfhs GROUP BY state_norm
),
joined AS (
  SELECT a.*,
    round((a.strong_count + a.partial_count*0.5 + a.weak_count*0.25)/a.facility_count*100,1) AS coverage_pct,
    (a.strong_count + a.partial_count + a.weak_count) > 0 AS has_evidence,
    (CASE capability
    WHEN 'ICU' THEN sa.hh_member_covered_health_insurance_pct_avg
    WHEN 'Maternity' THEN sa.institutional_birth_5y_pct_avg
    WHEN 'Emergency' THEN sa.hh_member_covered_health_insurance_pct_avg
    WHEN 'Dialysis' THEN sa.all_w15_49_who_are_anaemic_pct_avg
    WHEN 'Oncology' THEN sa.hh_member_covered_health_insurance_pct_avg
    WHEN 'Cardiology' THEN sa.w15_plus_with_high_bp_sys_gte_140_mmhg_and_or_dia_gte_90_mm_pct_avg
    WHEN 'Pediatrics' THEN sa.prev_diarrhoea_2wk_child_u5_pct_avg
    WHEN 'Surgery' THEN sa.births_delivered_by_csection_5y_pct_avg
  END) AS nfhs_value,
    round((CASE capability
    WHEN 'ICU' THEN 100 - sa.hh_member_covered_health_insurance_pct_avg
    WHEN 'Maternity' THEN 100 - sa.institutional_birth_5y_pct_avg
    WHEN 'Emergency' THEN 100 - sa.hh_member_covered_health_insurance_pct_avg
    WHEN 'Dialysis' THEN sa.all_w15_49_who_are_anaemic_pct_avg
    WHEN 'Oncology' THEN 100 - sa.hh_member_covered_health_insurance_pct_avg
    WHEN 'Cardiology' THEN sa.w15_plus_with_high_bp_sys_gte_140_mmhg_and_or_dia_gte_90_mm_pct_avg
    WHEN 'Pediatrics' THEN sa.prev_diarrhoea_2wk_child_u5_pct_avg
    WHEN 'Surgery' THEN sa.births_delivered_by_csection_5y_pct_avg
  END),1) AS need_score,
    (CASE capability
    WHEN 'ICU' THEN 'Health insurance %'
    WHEN 'Maternity' THEN 'Institutional births %'
    WHEN 'Emergency' THEN 'Health insurance %'
    WHEN 'Dialysis' THEN 'Women anaemia %'
    WHEN 'Oncology' THEN 'Health insurance %'
    WHEN 'Cardiology' THEN 'High BP prevalence %'
    WHEN 'Pediatrics' THEN 'Child diarrhoea %'
    WHEN 'Surgery' THEN 'C-section births %'
  END) AS nfhs_field
  FROM agg a LEFT JOIN sa USING (state_norm)
),
scored AS (
  SELECT *,
    CASE
    WHEN facility_count<3 AND NOT has_evidence THEN 'DATA-POOR'
    WHEN coverage_pct<20 AND facility_count>=3 THEN 'CRITICAL GAP'
    WHEN coverage_pct<40 THEN 'HIGH PRIORITY'
    WHEN coverage_pct<65 THEN 'MODERATE'
    ELSE 'ADEQUATE' END AS priority,
    least(round(least(facility_count/20.0,1.0)*60
      + (CASE WHEN facility_count>0 THEN (strong_count + partial_count*0.5)/facility_count*40 ELSE 0 END)),100) AS confidence_pct
  FROM joined
)
SELECT *,
  round((1 - coverage_pct/100) * (need_score/100) * 100,1) AS unmet_need_score,
  round((1 - coverage_pct/100) * (need_score/100) * (confidence_pct/100.0) * 100,1) AS gap_risk,
  round((1 - coverage_pct/100) * (coalesce(need_score,50)/100) * 100,1) AS risk_score_legacy
FROM scored;

-- COMMAND ----------

-- gold.facility_directory
CREATE OR REPLACE TABLE workspace.vf_gold.facility_directory
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
FROM workspace.vf_silver.facilities;

-- COMMAND ----------

-- gold.field_coverage
CREATE OR REPLACE TABLE workspace.vf_gold.field_coverage
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
  FROM workspace.vf_silver.facilities
)
SELECT stack(11,
  'description', description, 'capability', capability, 'procedure', procedure,
  'equipment', equipment, 'specialties', specialties, 'numberDoctors', numberDoctors,
  'capacity', capacity, 'yearEstablished', yearEstablished, 'pincode', pincode,
  'district', district, 'coords_valid', coords_valid) AS (field, coverage_pct)
FROM c;
