"""Medical Desert Planner - DAIS 2026 Hackathon

Helps non-technical healthcare planners identify critical gaps in care across India.
Distinguishes real healthcare gaps from data-poor regions using trust-weighted evidence.
"""

import streamlit as st
import pandas as pd
import re
import json
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

# ============================================================================
# CONFIGURATION
# ============================================================================

DATASET_CATALOG = "databricks_virtue_foundation_dataset_dais_2026"
DATASET_SCHEMA = "virtue_foundation_dataset"
FACILITIES_TABLE = f"{DATASET_CATALOG}.{DATASET_SCHEMA}.facilities"
HEALTH_INDICATORS_TABLE = f"{DATASET_CATALOG}.{DATASET_SCHEMA}.nfhs_5_district_health_indicators"
PINCODE_TABLE = f"{DATASET_CATALOG}.{DATASET_SCHEMA}.india_post_pincode_directory"

# Unity Catalog table for scenario persistence
SCENARIO_TABLE = "main.default.medical_desert_planner_scenarios"

# Capability keywords for trust scoring
CAPABILITY_KEYWORDS = {
    "ICU": ["icu", "intensive care", "critical care", "icu bed"],
    "Maternity": ["maternity", "obstetric", "gynecology", "delivery", "labor", "prenatal", "antenatal"],
    "Emergency": ["emergency", "trauma", "ambulance", "24/7", "casualty", "er"],
    "Dialysis": ["dialysis", "hemodialysis", "kidney", "renal"],
    "Oncology": ["oncology", "cancer", "chemotherapy", "radiation therapy", "tumor"],
    "Cardiology": ["cardiology", "cardiac", "heart", "coronary", "angioplasty"],
    "Pediatrics": ["pediatric", "children", "nicu", "neonatal"],
    "Surgery": ["surgery", "surgical", "operating room", "operation theater", "ot"]
}

# ============================================================================
# DATABRICKS CONNECTION
# ============================================================================

@st.cache_resource
def get_databricks_connection():
    """Initialize Databricks SQL connection using serverless warehouse."""
    try:
        w = WorkspaceClient()
        warehouses = list(w.warehouses.list())
        
        if not warehouses:
            st.error("No SQL warehouses available. Please create one first.")
            return None
            
        warehouse_id = warehouses[0].id
        config = Config()
        
        connection = sql.connect(
            server_hostname=config.host.replace("https://", ""),
            http_path=f"/sql/1.0/warehouses/{warehouse_id}",
            credentials_provider=lambda: config.authenticate
        )
        return connection
    except Exception as e:
        st.error(f"Failed to connect to Databricks: {str(e)}")
        return None

# ============================================================================
# TRUST SCORING ENGINE
# ============================================================================

def extract_capability_evidence(row: pd.Series, capability: str) -> Dict[str, any]:
    """Extract evidence of a capability from facility record.
    
    Returns dict with:
        - score: STRONG (2), PARTIAL (1), WEAK (0.5), NO_CLAIM (0)
        - evidence: list of (field, excerpt) tuples
        - confidence: description of evidence strength
    """
    keywords = [kw.lower() for kw in CAPABILITY_KEYWORDS.get(capability, [])]
    evidence = []
    field_matches = {}
    
    # Check each text field for capability mentions
    text_fields = [
        ("description", row.get("description", "")),
        ("capability", row.get("capability", "")),
        ("procedure", row.get("procedure", "")),
        ("equipment", row.get("equipment", "")),
        ("specialties", row.get("specialties", ""))
    ]
    
    for field_name, field_value in text_fields:
        if pd.isna(field_value) or not field_value:
            continue
            
        field_value = str(field_value).lower()
        
        for keyword in keywords:
            if keyword in field_value:
                # Extract context around the keyword (50 chars before/after)
                matches = list(re.finditer(re.escape(keyword), field_value))
                for match in matches:
                    start = max(0, match.start() - 50)
                    end = min(len(field_value), match.end() + 50)
                    excerpt = field_value[start:end].strip()
                    evidence.append((field_name, f"...{excerpt}..."))
                    field_matches[field_name] = field_matches.get(field_name, 0) + 1
    
    # Calculate trust score based on evidence breadth and depth
    unique_fields = len(field_matches)
    total_mentions = sum(field_matches.values())
    
    if unique_fields >= 3 and total_mentions >= 4:
        score = 2.0  # STRONG
        confidence = "STRONG - Multiple corroborating fields"
    elif unique_fields >= 2 or total_mentions >= 3:
        score = 1.0  # PARTIAL
        confidence = "PARTIAL - Limited evidence"
    elif unique_fields >= 1:
        score = 0.5  # WEAK
        confidence = "WEAK - Single mention, may be vague"
    else:
        score = 0.0  # NO_CLAIM
        confidence = "NO CLAIM - No evidence found"
    
    # Boost score for credibility signals
    credibility_boost = 0.0
    if row.get("recency_of_page_update") and float(row.get("recency_of_page_update", 999)) < 365:
        credibility_boost += 0.1
    if row.get("distinct_social_media_presence_count") and int(row.get("distinct_social_media_presence_count", 0)) > 0:
        credibility_boost += 0.1
    if row.get("officialWebsite") and str(row.get("officialWebsite", "")) != "":
        credibility_boost += 0.1
    
    final_score = min(2.0, score + credibility_boost)
    
    return {
        "score": final_score,
        "evidence": evidence[:5],  # Top 5 pieces of evidence
        "confidence": confidence,
        "field_matches": field_matches,
        "credibility_boost": credibility_boost
    }

def calculate_facility_trust_score(facilities_df: pd.DataFrame, capability: str) -> pd.DataFrame:
    """Calculate trust scores for all facilities for a given capability."""
    
    trust_data = []
    for idx, row in facilities_df.iterrows():
        result = extract_capability_evidence(row, capability)
        trust_data.append({
            "facility_id": row.get("id"),
            "facility_name": row.get("name"),
            "city": row.get("address_city"),
            "state": row.get("address_stateOrRegion"),
            "trust_score": result["score"],
            "confidence": result["confidence"],
            "evidence_count": len(result["evidence"]),
            "evidence": result["evidence"],
            "field_matches": result["field_matches"],
            "credibility_boost": result["credibility_boost"]
        })
    
    return pd.DataFrame(trust_data)

# ============================================================================
# GEOGRAPHIC AGGREGATION
# ============================================================================

def aggregate_by_geography(trust_df: pd.DataFrame, health_df: pd.DataFrame, 
                          geo_level: str) -> pd.DataFrame:
    """Aggregate trust scores by geographic level and combine with health indicators.
    
    geo_level: 'state', 'city', 'district'
    """
    
    if geo_level == "state":
        group_col = "state"
    elif geo_level == "city":
        group_col = ["state", "city"]
    elif geo_level == "district":
        # Need to map cities to districts (simplified: use state-level)
        group_col = "state"
    else:
        group_col = "state"
    
    # Calculate geographic aggregates
    geo_agg = trust_df.groupby(group_col).agg({
        "trust_score": ["mean", "sum", "count"],
        "facility_id": "count"
    }).reset_index()
    
    geo_agg.columns = [
        group_col if isinstance(group_col, str) else "_".join(col) if col[0] == col[1] else "_".join(col)
        for col in geo_agg.columns
    ]
    
    # Rename for clarity
    rename_map = {}
    for col in geo_agg.columns:
        if "trust_score_mean" in col:
            rename_map[col] = "avg_trust_score"
        elif "trust_score_sum" in col:
            rename_map[col] = "total_trust_score"
        elif "trust_score_count" in col:
            rename_map[col] = "facilities_with_capability"
        elif "facility_id_count" in col:
            rename_map[col] = "total_facilities"
    
    geo_agg.rename(columns=rename_map, inplace=True)
    
    # Calculate coverage percentage
    if "facilities_with_capability" in geo_agg.columns and "total_facilities" in geo_agg.columns:
        geo_agg["coverage_pct"] = (
            geo_agg["facilities_with_capability"] / geo_agg["total_facilities"] * 100
        ).round(1)
    
    # Calculate trust-weighted coverage
    if "total_trust_score" in geo_agg.columns and "facilities_with_capability" in geo_agg.columns:
        geo_agg["trust_weighted_coverage"] = (
            geo_agg["total_trust_score"] / (geo_agg["facilities_with_capability"] * 2.0) * 100
        ).round(1)
    
    # Merge with health indicators
    if not health_df.empty and geo_level == "state":
        # Prepare health data
        health_agg = health_df.groupby("state_ut").agg({
            "institutional_birth_5y_pct": "mean",
            "women_age_15_49_who_are_literate_pct": "mean"
        }).reset_index()
        
        health_agg.columns = ["state", "avg_institutional_birth_pct", "avg_female_literacy_pct"]
        
        # Merge
        geo_agg = geo_agg.merge(
            health_agg,
            left_on="state" if isinstance(group_col, str) else "state_state",
            right_on="state",
            how="left"
        )
    
    return geo_agg

# ============================================================================
# GAP ANALYSIS
# ============================================================================

def classify_priority(row: pd.Series) -> str:
    """Classify region priority based on health outcomes and facility coverage."""
    
    trust_coverage = row.get("trust_weighted_coverage", 0)
    health_score = row.get("avg_institutional_birth_pct", 100)
    facility_count = row.get("total_facilities", 999)
    
    # CRITICAL GAP: Poor health + Low facility coverage
    if health_score < 70 and trust_coverage < 30 and facility_count < 150:
        return "🔴 CRITICAL GAP"
    
    # HIGH PRIORITY: Moderate health issues + Low coverage
    elif health_score < 80 and trust_coverage < 50 and facility_count < 300:
        return "🟠 HIGH PRIORITY"
    
    # DATA-POOR: Low facility count but no health data
    elif facility_count < 50 and pd.isna(health_score):
        return "⚪ DATA-POOR"
    
    # MODERATE: Some gaps but not critical
    elif trust_coverage < 60 or health_score < 85:
        return "🟡 MODERATE PRIORITY"
    
    # ADEQUATE: Good coverage and health outcomes
    else:
        return "🟢 ADEQUATE COVERAGE"

def add_priority_classification(geo_agg_df: pd.DataFrame) -> pd.DataFrame:
    """Add priority classification to geographic aggregates."""
    geo_agg_df["priority_level"] = geo_agg_df.apply(classify_priority, axis=1)
    return geo_agg_df

# ============================================================================
# SCENARIO PERSISTENCE
# ============================================================================

def save_scenario(conn, scenario_name: str, capability: str, geography: str, 
                 notes: str, priority_regions: List[str]):
    """Save planning scenario to Unity Catalog."""
    
    try:
        # Create table if doesn't exist
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {SCENARIO_TABLE} (
            scenario_id STRING,
            scenario_name STRING,
            capability STRING,
            geography_level STRING,
            notes STRING,
            priority_regions STRING,
            created_at TIMESTAMP,
            created_by STRING
        ) USING DELTA
        """
        
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        
        # Insert scenario
        scenario_id = f"scenario_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        insert_sql = f"""
        INSERT INTO {SCENARIO_TABLE} VALUES (
            '{scenario_id}',
            '{scenario_name}',
            '{capability}',
            '{geography}',
            '{notes}',
            '{json.dumps(priority_regions)}',
            current_timestamp(),
            current_user()
        )
        """
        
        cursor.execute(insert_sql)
        cursor.close()
        
        return True, scenario_id
    except Exception as e:
        return False, str(e)

def load_scenarios(conn) -> pd.DataFrame:
    """Load all saved scenarios from Unity Catalog."""
    
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {SCENARIO_TABLE} ORDER BY created_at DESC")
        
        rows = cursor.fetchall()
        if rows:
            columns = [desc[0] for desc in cursor.description]
            return pd.DataFrame(rows, columns=columns)
        else:
            return pd.DataFrame()
    except Exception as e:
        # Table might not exist yet
        return pd.DataFrame()

# ============================================================================
# DATA LOADING
# ============================================================================

@st.cache_data(ttl=3600)
def load_facilities_data(_conn) -> pd.DataFrame:
    """Load facilities data from Unity Catalog."""
    query = f"""
    SELECT 
        id,
        name,
        facilityTypeId,
        address_city,
        address_stateOrRegion,
        description,
        capability,
        procedure,
        equipment,
        specialties,
        capacity,
        numberDoctors,
        coordinates,
        latitude,
        longitude,
        officialWebsite,
        phone_numbers,
        recency_of_page_update,
        distinct_social_media_presence_count,
        affiliated_staff_presence,
        custom_logo_presence
    FROM {FACILITIES_TABLE}
    WHERE address_stateOrRegion IS NOT NULL
    """
    
    cursor = _conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    cursor.close()
    
    return pd.DataFrame(rows, columns=columns)

@st.cache_data(ttl=3600)
def load_health_indicators(_conn) -> pd.DataFrame:
    """Load health indicators from Unity Catalog."""
    query = f"""
    SELECT 
        state_ut,
        district_name,
        institutional_birth_5y_pct,
        women_age_15_49_who_are_literate_pct,
        households_using_clean_fuel_for_cooking_pct
    FROM {HEALTH_INDICATORS_TABLE}
    """
    
    cursor = _conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    cursor.close()
    
    return pd.DataFrame(rows, columns=columns)

# ============================================================================
# STREAMLIT UI
# ============================================================================

def main():
    st.set_page_config(
        page_title="Medical Desert Planner",
        page_icon="🏥",
        layout="wide"
    )
    
    st.title("🏥 Medical Desert Planner")
    st.markdown("""
    **DAIS 2026 Hackathon - Track 2**
    
    Identify critical gaps in healthcare coverage across India by combining:
    - Trust-weighted facility capability analysis
    - National health outcome indicators (NFHS-5)
    - Geographic coverage mapping
    """)
    
    # Initialize connection
    conn = get_databricks_connection()
    if not conn:
        st.stop()
    
    # Sidebar: Configuration
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        capability = st.selectbox(
            "Healthcare Capability",
            options=list(CAPABILITY_KEYWORDS.keys()),
            help="Select the healthcare capability to analyze"
        )
        
        geo_level = st.selectbox(
            "Geographic Level",
            options=["state", "city", "district"],
            help="Aggregation level for analysis"
        )
        
        min_facilities = st.slider(
            "Minimum Facilities for Valid Region",
            min_value=1,
            max_value=50,
            value=5,
            help="Regions with fewer facilities will be marked DATA-POOR"
        )
        
        st.divider()
        
        if st.button("🔄 Run Analysis", type="primary", use_container_width=True):
            st.session_state["run_analysis"] = True
    
    # Main content area
    if "run_analysis" not in st.session_state:
        st.info("👈 Configure your analysis parameters in the sidebar and click 'Run Analysis' to begin.")
        st.stop()
    
    # Load data
    with st.spinner("Loading facility data..."):
        facilities_df = load_facilities_data(conn)
        health_df = load_health_indicators(conn)
    
    st.success(f"✅ Loaded {len(facilities_df):,} facilities and {len(health_df):,} health indicators")
    
    # Calculate trust scores
    with st.spinner(f"Analyzing {capability} capability across facilities..."):
        trust_df = calculate_facility_trust_score(facilities_df, capability)
    
    # Aggregate by geography
    with st.spinner("Aggregating by geography and health indicators..."):
        geo_agg_df = aggregate_by_geography(trust_df, health_df, geo_level)
        geo_agg_df = add_priority_classification(geo_agg_df)
    
    # Display results in tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Regional Overview",
        "🔍 Facility Details",
        "📝 Save Scenario",
        "💾 Saved Scenarios"
    ])
    
    # TAB 1: Regional Overview
    with tab1:
        st.header("Regional Coverage Analysis")
        
        # Filter and sort
        display_cols = ["state", "total_facilities", "facilities_with_capability", 
                       "trust_weighted_coverage", "avg_institutional_birth_pct", 
                       "priority_level"]
        
        # Keep only columns that exist
        display_cols = [col for col in display_cols if col in geo_agg_df.columns]
        
        display_df = geo_agg_df[display_cols].sort_values(
            "priority_level", 
            ascending=True
        )
        
        # Priority filter
        priority_filter = st.multiselect(
            "Filter by Priority Level",
            options=display_df["priority_level"].unique(),
            default=display_df["priority_level"].unique()
        )
        
        filtered_df = display_df[display_df["priority_level"].isin(priority_filter)]
        
        st.dataframe(
            filtered_df,
            use_container_width=True,
            hide_index=True
        )
        
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        
        critical = len(filtered_df[filtered_df["priority_level"].str.contains("CRITICAL")])
        high = len(filtered_df[filtered_df["priority_level"].str.contains("HIGH")])
        data_poor = len(filtered_df[filtered_df["priority_level"].str.contains("DATA-POOR")])
        adequate = len(filtered_df[filtered_df["priority_level"].str.contains("ADEQUATE")])
        
        col1.metric("🔴 Critical Gaps", critical)
        col2.metric("🟠 High Priority", high)
        col3.metric("⚪ Data-Poor", data_poor)
        col4.metric("🟢 Adequate", adequate)
    
    # TAB 2: Facility Details
    with tab2:
        st.header("Facility-Level Evidence")
        
        # Region selector
        selected_state = st.selectbox(
            "Select State/Region",
            options=sorted(trust_df["state"].dropna().unique())
        )
        
        state_facilities = trust_df[trust_df["state"] == selected_state]
        
        st.subheader(f"Facilities in {selected_state}")
        st.caption(f"Showing {len(state_facilities)} facilities with evidence for {capability}")
        
        # Display facilities with evidence
        for idx, row in state_facilities.iterrows():
            with st.expander(
                f"{row['facility_name']} - {row['confidence']} (Score: {row['trust_score']:.1f}/2.0)"
            ):
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    st.markdown(f"**Location:** {row['city']}, {row['state']}")
                    st.markdown(f"**Trust Score:** {row['trust_score']:.2f} / 2.0")
                    st.markdown(f"**Confidence:** {row['confidence']}")
                    st.markdown(f"**Evidence Count:** {row['evidence_count']} pieces")
                    
                    if row["evidence"]:
                        st.markdown("**Evidence Excerpts:**")
                        for field, excerpt in row["evidence"]:
                            st.text(f"[{field}] {excerpt}")
                
                with col2:
                    st.markdown("**Field Matches:**")
                    if row["field_matches"]:
                        for field, count in row["field_matches"].items():
                            st.metric(field, count)
                    
                    if row["credibility_boost"] > 0:
                        st.success(f"Credibility Boost: +{row['credibility_boost']:.1f}")
    
    # TAB 3: Save Scenario
    with tab3:
        st.header("Save Planning Scenario")
        
        scenario_name = st.text_input("Scenario Name", placeholder="Q4 2026 Maternity Expansion")
        notes = st.text_area(
            "Notes",
            placeholder="Add planning notes, decisions, or follow-up actions...",
            height=150
        )
        
        # Show priority regions
        st.subheader("Priority Regions in This Scenario")
        priority_regions = geo_agg_df[
            geo_agg_df["priority_level"].str.contains("CRITICAL|HIGH")
        ]["state"].tolist()
        
        st.write(priority_regions)
        
        if st.button("💾 Save Scenario", type="primary"):
            if not scenario_name:
                st.error("Please provide a scenario name")
            else:
                success, result = save_scenario(
                    conn,
                    scenario_name,
                    capability,
                    geo_level,
                    notes,
                    priority_regions
                )
                
                if success:
                    st.success(f"✅ Scenario saved with ID: {result}")
                else:
                    st.error(f"❌ Failed to save: {result}")
    
    # TAB 4: Saved Scenarios
    with tab4:
        st.header("Saved Planning Scenarios")
        
        scenarios_df = load_scenarios(conn)
        
        if scenarios_df.empty:
            st.info("No saved scenarios yet. Create one in the 'Save Scenario' tab.")
        else:
            for idx, row in scenarios_df.iterrows():
                with st.expander(
                    f"{row['scenario_name']} - {row['capability']} ({row['created_at']})"
                ):
                    st.markdown(f"**Geography Level:** {row['geography_level']}")
                    st.markdown(f"**Created By:** {row['created_by']}")
                    st.markdown(f"**Notes:** {row['notes']}")
                    st.markdown(f"**Priority Regions:** {row['priority_regions']}")

if __name__ == "__main__":
    main()