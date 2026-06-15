-- Setup SQL for Medical Desert Planner
-- Run this in a Databricks SQL notebook before deploying the app

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS workspace.medical_desert
COMMENT 'Medical Desert Planner output tables';

USE workspace.medical_desert;

-- 1. Facility Trust Scores Table
-- Pre-computed trust scores for all capabilities
CREATE TABLE IF NOT EXISTS facility_trust_scores (
  facility_id STRING COMMENT 'Unique facility identifier',
  facility_name STRING COMMENT 'Facility name',
  facility_type STRING COMMENT 'Hospital, clinic, etc.',
  
  -- Geography
  state STRING COMMENT 'State/Region',
  district STRING COMMENT 'District',
  city STRING COMMENT 'City',
  pincode INT COMMENT 'Postal code',
  latitude DOUBLE COMMENT 'Latitude coordinate',
  longitude DOUBLE COMMENT 'Longitude coordinate',
  
  -- Capability trust scores (one column per capability)
  maternity_trust_score INT COMMENT 'Maternity care trust score (0-3)',
  maternity_evidence STRING COMMENT 'Evidence citations for maternity',
  
  emergency_trust_score INT COMMENT 'Emergency care trust score (0-3)',
  emergency_evidence STRING COMMENT 'Evidence citations for emergency',
  
  dialysis_trust_score INT COMMENT 'Dialysis trust score (0-3)',
  dialysis_evidence STRING COMMENT 'Evidence citations for dialysis',
  
  blood_bank_trust_score INT COMMENT 'Blood bank trust score (0-3)',
  blood_bank_evidence STRING COMMENT 'Evidence citations for blood bank',
  
  nicu_trust_score INT COMMENT 'NICU trust score (0-3)',
  nicu_evidence STRING COMMENT 'Evidence citations for NICU',
  
  surgery_trust_score INT COMMENT 'Surgery trust score (0-3)',
  surgery_evidence STRING COMMENT 'Evidence citations for surgery',
  
  -- Metadata
  last_updated TIMESTAMP COMMENT 'Last score update timestamp'
) USING DELTA
COMMENT 'Pre-computed trust scores for all facility capabilities'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- Add indexes for common queries
-- CREATE INDEX IF NOT EXISTS idx_state ON facility_trust_scores(state);
-- CREATE INDEX IF NOT EXISTS idx_district ON facility_trust_scores(district);
-- CREATE INDEX IF NOT EXISTS idx_pincode ON facility_trust_scores(pincode);


-- 2. Gap Validation Results Table
-- Aggregated gap analysis with NFHS validation
CREATE TABLE IF NOT EXISTS gap_validation (
  geography_id STRING COMMENT 'Unique geographic identifier',
  geography_level STRING COMMENT 'state, district, or pincode',
  
  -- Geography details
  state STRING COMMENT 'State name',
  district STRING COMMENT 'District name (if district level)',
  pincode INT COMMENT 'Pincode (if pincode level)',
  
  -- Capability
  capability STRING COMMENT 'Healthcare capability analyzed',
  
  -- Facility metrics
  facility_count INT COMMENT 'Number of facilities in region',
  avg_trust_score DOUBLE COMMENT 'Average trust score',
  trust_percentage DOUBLE COMMENT 'Trust percentage (0-100)',
  strong_evidence_count INT COMMENT 'Facilities with strong evidence',
  partial_evidence_count INT COMMENT 'Facilities with partial evidence',
  no_evidence_count INT COMMENT 'Facilities with no evidence',
  
  -- NFHS validation
  nfhs_outcome_metric DOUBLE COMMENT 'NFHS health outcome percentage',
  nfhs_field STRING COMMENT 'NFHS field used for validation',
  
  -- Gap classification
  gap_type STRING COMMENT 'HIGH_CONFIDENCE_GAP, DATA_QUALITY_ISSUE, WELL_SERVED, QUALITY_CONCERN',
  confidence_score DOUBLE COMMENT 'Confidence in gap identification (0-100)',
  confidence_level STRING COMMENT 'HIGH, MEDIUM, or LOW',
  
  -- Confidence components (for explainability)
  data_completeness_score DOUBLE COMMENT 'Data completeness component',
  signal_alignment_score DOUBLE COMMENT 'Signal alignment component',
  sample_size_score DOUBLE COMMENT 'Sample size component',
  field_coverage_score DOUBLE COMMENT 'Field coverage component',
  
  -- Metadata
  analysis_timestamp TIMESTAMP COMMENT 'When analysis was performed',
  last_updated TIMESTAMP COMMENT 'Last update timestamp'
) USING DELTA
COMMENT 'Gap validation results with confidence scoring'
PARTITIONED BY (capability)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- Add indexes
-- CREATE INDEX IF NOT EXISTS idx_gap_state ON gap_validation(state);
-- CREATE INDEX IF NOT EXISTS idx_gap_type ON gap_validation(gap_type);
-- CREATE INDEX IF NOT EXISTS idx_confidence ON gap_validation(confidence_score);


-- 3. User Scenarios Table
-- Saved planning scenarios with version history
CREATE TABLE IF NOT EXISTS user_scenarios (
  scenario_id STRING COMMENT 'Unique scenario UUID',
  scenario_name STRING COMMENT 'User-provided scenario name',
  
  -- User info
  user_id STRING COMMENT 'User who created the scenario',
  user_email STRING COMMENT 'User email (optional)',
  
  -- Scenario parameters
  capability STRING COMMENT 'Healthcare capability analyzed',
  geography STRING COMMENT 'Geographic scope (JSON string)',
  geography_level STRING COMMENT 'state, district, or pincode',
  
  -- Selected facilities
  selected_facilities ARRAY<STRING> COMMENT 'List of facility IDs in scenario',
  facility_count INT COMMENT 'Number of facilities selected',
  
  -- Gap summary
  gap_type STRING COMMENT 'Primary gap type identified',
  confidence_score DOUBLE COMMENT 'Overall confidence score',
  
  -- User inputs
  user_notes STRING COMMENT 'Planning notes and decisions',
  user_corrections MAP<STRING, STRING> COMMENT 'User overrides and corrections',
  
  -- Versioning
  version INT COMMENT 'Scenario version number',
  parent_scenario_id STRING COMMENT 'Parent scenario UUID (if branched)',
  
  -- Metadata
  created_at TIMESTAMP COMMENT 'Scenario creation timestamp',
  last_modified TIMESTAMP COMMENT 'Last modification timestamp',
  is_active BOOLEAN COMMENT 'Whether scenario is active or archived'
) USING DELTA
COMMENT 'Saved planning scenarios with version history'
PARTITIONED BY (user_id)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- Add indexes
-- CREATE INDEX IF NOT EXISTS idx_scenario_user ON user_scenarios(user_id);
-- CREATE INDEX IF NOT EXISTS idx_scenario_created ON user_scenarios(created_at);
-- CREATE INDEX IF NOT EXISTS idx_scenario_active ON user_scenarios(is_active);


-- 4. Create views for common queries

-- View: High Confidence Gaps Summary
CREATE OR REPLACE VIEW high_confidence_gaps AS
SELECT
  capability,
  geography_level,
  state,
  district,
  facility_count,
  trust_percentage,
  nfhs_outcome_metric,
  confidence_score,
  confidence_level,
  analysis_timestamp
FROM gap_validation
WHERE gap_type = 'HIGH_CONFIDENCE_GAP'
  AND confidence_level IN ('HIGH', 'MEDIUM')
ORDER BY confidence_score DESC, facility_count ASC;


-- View: Data Quality Issues (need field verification)
CREATE OR REPLACE VIEW data_quality_issues AS
SELECT
  capability,
  geography_level,
  state,
  district,
  facility_count,
  trust_percentage,
  nfhs_outcome_metric,
  confidence_score,
  analysis_timestamp
FROM gap_validation
WHERE gap_type = 'DATA_QUALITY_ISSUE'
ORDER BY state, district;


-- View: Recent Scenarios by User
CREATE OR REPLACE VIEW recent_scenarios AS
SELECT
  scenario_id,
  scenario_name,
  user_id,
  capability,
  geography,
  facility_count,
  confidence_score,
  user_notes,
  created_at,
  version
FROM user_scenarios
WHERE is_active = true
ORDER BY created_at DESC
LIMIT 50;


-- View: Capability Coverage Summary
CREATE OR REPLACE VIEW capability_coverage_summary AS
SELECT
  capability,
  COUNT(DISTINCT geography_id) as regions_analyzed,
  SUM(facility_count) as total_facilities,
  ROUND(AVG(trust_percentage), 1) as avg_trust_pct,
  ROUND(AVG(confidence_score), 1) as avg_confidence,
  SUM(CASE WHEN gap_type = 'HIGH_CONFIDENCE_GAP' THEN 1 ELSE 0 END) as high_confidence_gaps,
  SUM(CASE WHEN gap_type = 'DATA_QUALITY_ISSUE' THEN 1 ELSE 0 END) as data_quality_issues,
  MAX(analysis_timestamp) as last_analysis
FROM gap_validation
GROUP BY capability
ORDER BY capability;


-- Grant permissions (adjust as needed)
-- GRANT SELECT, INSERT, UPDATE ON main.medical_desert.facility_trust_scores TO `users`;
-- GRANT SELECT, INSERT, UPDATE ON main.medical_desert.gap_validation TO `users`;
-- GRANT SELECT, INSERT, UPDATE ON main.medical_desert.user_scenarios TO `users`;

-- -- Grant view access
-- GRANT SELECT ON main.medical_desert.high_confidence_gaps TO `users`;
-- GRANT SELECT ON main.medical_desert.data_quality_issues TO `users`;
-- GRANT SELECT ON main.medical_desert.recent_scenarios TO `users`;
-- GRANT SELECT ON main.medical_desert.capability_coverage_summary TO `users`;


-- Verify setup
SELECT 
  'Setup complete!' as status,
  COUNT(*) as tables_created
FROM information_schema.tables
WHERE table_schema = 'medical_desert'
  AND table_name IN ('facility_trust_scores', 'gap_validation', 'user_scenarios');
