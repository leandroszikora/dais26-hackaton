# Medical Desert Planner 🏥

**Databricks DAIS 2026 Hackathon - Track 2**

Trust-weighted healthcare gap identification with confidence quantification.

---

## 🎯 Overview

The Medical Desert Planner helps healthcare planners identify **real service gaps** vs. **data-poor regions** by:

1. **Trust Scoring:** Multi-field evidence validation for facility capabilities
2. **Geographic Aggregation:** Roll-up by state/district/pincode
3. **NFHS Validation:** Cross-reference facility data with actual health outcomes
4. **Confidence Quantification:** 0-100% confidence scores for gap identification
5. **Live Draft Scoring:** Real-time updates as planners fill missing data

---

## 🏗️ Architecture

### File Structure

```
medical-desert-planner-app/
├── config.py              # Capability registry and app constants
├── helpers.py             # Reusable logic (trust scoring, aggregation, validation)
├── app.py                 # Main Streamlit application
├── requirements.txt       # Python dependencies
├── README.md              # This file
└── setup.sql              # SQL scripts to create output tables
```

### Components

#### **config.py**
- **CapabilityConfig:** Config-driven capability definitions
- **CAPABILITIES:** Registry of healthcare capabilities (maternity, emergency, dialysis, etc.)
- **Weights & Thresholds:** Confidence scoring parameters
- **Database Configuration:** Table names and connections

#### **helpers.py**
- **TrustScorer:** Calculate trust scores using multi-field keyword matching
- **GeographicAggregator:** Roll up scores by state/district/pincode
- **NFHSValidator:** Validate facility gaps against health outcomes
- **ConfidenceScorer:** Calculate weighted confidence scores
- **ScenarioPersistence:** Save and load planning scenarios to Delta

#### **app.py**
- Streamlit UI with capability selector, geography filters, heatmap visualization
- Drill-down table with gap analysis
- Live correction form with real-time draft score updates
- Scenario save/load functionality

---

## 🚀 Setup Instructions

### 1. Create Output Schema

Run this in a Databricks SQL notebook:

```sql
CREATE SCHEMA IF NOT EXISTS main.medical_desert;
```

### 2. Create Output Tables

Run the SQL from `setup.sql` to create:
- `facility_trust_scores` - Pre-computed trust scores
- `gap_validation` - Gap validation results with confidence
- `user_scenarios` - Saved planning scenarios

### 3. Pre-compute Trust Scores (Optional)

For faster app performance, pre-compute scores for all capabilities:

```python
from config import get_capability_names
from helpers import TrustScorer

scorer = TrustScorer(spark)
facilities = spark.table("databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.facilities")

for cap_name in get_capability_names():
    capability = get_capability(cap_name)
    facilities = scorer.calculate_trust_score(facilities, capability)

facilities.write.mode("overwrite").saveAsTable("main.medical_desert.facility_trust_scores")
```

### 4. Deploy Databricks App

```bash
# From your local machine (if using Databricks CLI)
databricks apps create medical-desert-planner \
  --source-code-path /Workspace/Users/<your-email>/dais26-hackaton/medical-desert-planner-app

databricks apps deploy medical-desert-planner
```

Or use the Databricks UI:
1. Navigate to Apps
2. Create new app
3. Point to `medical-desert-planner-app/` directory
4. Deploy

---

## 🎨 User Workflow

### Step 1: Select Capability
- Choose from: Maternity Care, Emergency Care, Dialysis, Blood Bank, NICU, Surgery
- Each capability has its own keyword patterns and NFHS validation field

### Step 2: Select Geography
- **State:** High-level view
- **District:** Drill down to specific districts
- **Pincode:** (Future) Most granular level

### Step 3: Analyze Gaps
- Click "Analyze Gaps" to run trust scoring, aggregation, and validation
- View results in heatmap and drill-down table

### Step 4: Review Results

**Heatmap Quadrants:**
- **High Confidence Gap** (Red): Low facility trust + Low health outcomes
- **Well Served** (Green): High facility trust + High health outcomes
- **Data Quality Issue** (Blue): Low facility trust + High health outcomes (missing data, not real gap)
- **Quality Concern** (Orange): High facility trust + Low health outcomes (facility quality problem)

### Step 5: Apply Live Corrections
- Select a region to view details
- Override trust score or health outcome based on field knowledge
- Watch draft confidence score update in real-time
- Document correction rationale in notes

### Step 6: Save Scenario
- Name your scenario (e.g., "Maharashtra Mobile Clinic Q1 2026")
- Add planning notes
- Save to Delta table with version history

---

## 🔑 Key Differentiators

### 1. Unique Data Moat
- **NFHS-5 Health Indicators:** Validates facility gaps against real health outcomes
- **India Post Pincode Directory:** Enables precise geographic aggregation
- Others teams don't have this cross-validation capability

### 2. Confidence Quantification
- Not just "gap" or "no gap" - provides 0-100% confidence score
- Weighted scoring across 4 dimensions:
  - 30% Data Completeness
  - 40% Signal Alignment (facility ↔ outcome correlation)
  - 20% Sample Size
  - 10% Field Coverage

### 3. Live Draft Scoring
- Real-time confidence updates as planners correct data
- Shows what changes the score (transparent, explainable)
- Planners can see impact before committing

### 4. Config-Driven Architecture
- Add new capabilities by editing `config.py`
- No code changes needed for new healthcare services
- Scales easily to 20+ capabilities

### 5. Scenario Versioning
- Delta table persistence with full history
- Compare scenarios over time
- Collaborative planning with shared scenarios

---

## 📊 Confidence Scoring Formula

```python
confidence_score = (
    0.30 * data_completeness +      # How much data we have
    0.40 * signal_alignment +       # Do facility gap + outcome gap align?
    0.20 * sample_size +            # How many facilities in region?
    0.10 * field_coverage           # % facilities with evidence
) * 100
```

**Gap Type Classification:**

| Facility Trust | Health Outcome | Gap Type |
|----------------|----------------|----------|
| < 65% | < 70% | HIGH_CONFIDENCE_GAP |
| < 65% | ≥ 70% | DATA_QUALITY_ISSUE |
| ≥ 65% | ≥ 70% | WELL_SERVED |
| ≥ 65% | < 70% | QUALITY_CONCERN |

---

## 🎯 Use Cases

### 1. Mobile Clinic Planning
**User:** NGO Coordinator
**Scenario:** Deploy 3 mobile maternity clinics in Maharashtra
**Outcome:** Rank districts by confidence-weighted gap severity

### 2. Ambulance Network Expansion
**User:** State Health Department
**Scenario:** Select 20 of 50 locations for new ambulance stations
**Outcome:** Flag HIGH confidence emergency care gaps

### 3. Policy Budget Justification
**User:** Public Health Researcher
**Scenario:** Union Health Ministry budget brief
**Outcome:** State comparison with statistical confidence

### 4. Dialysis Center Investment
**User:** Healthcare Investment Analyst
**Scenario:** Identify 5 tier-2/3 cities for new centers
**Outcome:** De-risk investment with demand validation

### 5. Disaster Preparedness Audit
**User:** Disaster Management Authority
**Scenario:** Multi-capability gap analysis in flood zones
**Outcome:** Identify systemic vulnerabilities

---

## 🧪 Testing Checklist

- [ ] Test all 6 capabilities (maternity, emergency, dialysis, blood bank, NICU, surgery)
- [ ] Test state-level aggregation (national view)
- [ ] Test district-level drill-down
- [ ] Verify NFHS validation aligns correctly
- [ ] Test live correction form updates draft score
- [ ] Verify scenario save/load to Delta table
- [ ] Test with different geography combinations
- [ ] Verify confidence score ranges 0-100%
- [ ] Test heatmap visualization quadrants
- [ ] Verify drill-down table filters work

---

## 📈 Future Enhancements

### V1.5 (Post-Hackathon)
- [ ] Pincode-level aggregation (most granular)
- [ ] Distance-to-nearest-facility calculation
- [ ] Multi-capability composite scoring
- [ ] Export to PDF report
- [ ] User authentication and role-based access
- [ ] Historical trend analysis

### V2.0 (Production)
- [ ] LLM-powered evidence verification for edge cases
- [ ] Real-time data ingestion from field surveys
- [ ] Integration with GIS mapping services
- [ ] Mobile app for field data collection
- [ ] API for programmatic access

---

## 🏆 Demo Script (3 minutes)

### Opening (30 sec)
> "Healthcare planners face a critical problem: is this a real gap or just missing data? Our app solves this by validating facility claims against ground-truth health outcomes from NFHS-5."

### Walkthrough (2 min)
1. Select "Maternity Care" + "Maharashtra"
2. Click "Analyze Gaps" → show heatmap
3. Point out HIGH CONFIDENCE GAP quadrant (red dots)
4. Drill into a specific district
5. Show facility evidence panel
6. Apply live correction: override trust score from 40% → 60%
7. Show draft confidence updates in real-time
8. Save scenario: "Maharashtra Mobile Clinic Q1 2026"

### Impact Statement (30 sec)
> "Unlike tools that just count facilities, we prevent wasted resources on data-poor regions. We've identified 50 districts with validated medical deserts — where LOW facility count aligns with LOW health outcomes. That's where intervention is truly needed. And our confidence scoring shows planners exactly how certain we are."

---

## 🤝 Team

**Track:** 2 - Medical Desert Planner
**Event:** Databricks DAIS 2026 Hackathon
**Key Advantage:** NFHS validation + Confidence quantification

---

## 📜 License

MIT License - Hackathon Submission

---

## 🆘 Troubleshooting

### "Table not found: main.medical_desert.facility_trust_scores"
→ Run `setup.sql` to create output tables first

### "ModuleNotFoundError: No module named 'config'"
→ Ensure all files are in the same directory and PYTHONPATH includes app directory

### "Confidence score always 0%"
→ Verify NFHS data is loaded and cleaned correctly (check for null values)

### "App crashes on state selection"
→ Check that facilities table has valid state names (no nulls in address_stateOrRegion)

### "Draft score doesn't update"
→ Ensure sliders trigger recomputation (check session state)

---

**Ready to identify medical deserts with confidence!** 🚀
