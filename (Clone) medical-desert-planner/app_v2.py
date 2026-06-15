"""Medical Desert Planner - DAIS 2026 Hackathon - Simplified Version

Helps non-technical healthcare planners identify critical gaps in care across India.
"""

import streamlit as st
import pandas as pd
import re
from typing import Dict

st.set_page_config(page_title="Medical Desert Planner", page_icon="🏥", layout="wide")

# Try to import Spark
try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    HAS_SPARK = True
except Exception as e:
    st.error(f"Spark not available: {e}")
    HAS_SPARK = False

# Configuration
FACILITIES_TABLE = "databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.facilities"
HEALTH_TABLE = "databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.nfhs_5_district_health_indicators"

CAPABILITY_KEYWORDS = {
    "ICU": ["icu", "intensive care", "critical care"],
    "Maternity": ["maternity", "obstetric", "gynecology", "delivery"],
    "Emergency": ["emergency", "trauma", "ambulance", "24/7"],
    "Dialysis": ["dialysis", "hemodialysis", "kidney", "renal"],
    "Oncology": ["oncology", "cancer", "chemotherapy"],
    "Cardiology": ["cardiology", "cardiac", "heart"],
    "Pediatrics": ["pediatric", "children", "nicu"],
    "Surgery": ["surgery", "surgical", "operating room"]
}

@st.cache_data
def load_data():
    """Load facility and health data."""
    if not HAS_SPARK:
        return pd.DataFrame(), pd.DataFrame()
    
    try:
        # Load facilities
        facilities_df = spark.table(FACILITIES_TABLE).limit(1000).toPandas()
        
        # Load health indicators  
        health_df = spark.table(HEALTH_TABLE).toPandas()
        
        return facilities_df, health_df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame(), pd.DataFrame()

def score_facility(row, capability):
    """Score a facility for a capability."""
    keywords = CAPABILITY_KEYWORDS.get(capability, [])
    score = 0
    evidence = []
    
    for field in ['description', 'capability', 'procedure', 'equipment']:
        text = str(row.get(field, '')).lower()
        for kw in keywords:
            if kw in text:
                score += 0.5
                idx = text.find(kw)
                excerpt = text[max(0,idx-30):min(len(text),idx+50)]
                evidence.append(f"{field}: ...{excerpt}...")
                break
    
    return min(2.0, score), evidence[:3]

def main():
    st.title("🏥 Medical Desert Planner")
    st.markdown("**DAIS 2026 Hackathon - Track 2**")
    st.markdown("Identify critical gaps in healthcare coverage across India")
    
    if not HAS_SPARK:
        st.warning("Spark session not available. App requires Spark to access data.")
        return
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Configuration")
        capability = st.selectbox("Healthcare Capability", list(CAPABILITY_KEYWORDS.keys()))
        run_btn = st.button("🔄 Run Analysis", type="primary")
    
    if not run_btn:
        st.info("👈 Select a capability and click 'Run Analysis' to begin.")
        return
    
    # Load data
    with st.spinner("Loading data..."):
        facilities_df, health_df = load_data()
    
    if facilities_df.empty:
        st.error("Failed to load facility data")
        return
    
    st.success(f"✅ Loaded {len(facilities_df):,} facilities")
    
    # Score facilities
    with st.spinner(f"Analyzing {capability} capability..."):
        results = []
        for idx, row in facilities_df.iterrows():
            score, evidence = score_facility(row, capability)
            if score > 0:
                results.append({
                    'name': row.get('name', 'Unknown'),
                    'city': row.get('address_city', 'Unknown'),
                    'state': row.get('address_stateOrRegion', 'Unknown'),
                    'score': score,
                    'evidence': evidence
                })
        
        results_df = pd.DataFrame(results)
    
    # Display
    tab1, tab2 = st.tabs(["📊 Regional Overview", "🔍 Facility Details"])
    
    with tab1:
        st.header("Regional Coverage")
        if not results_df.empty:
            agg = results_df.groupby('state').agg({
                'score': ['mean', 'sum', 'count']
            }).reset_index()
            agg.columns = ['State', 'Avg Score', 'Total Score', 'Facility Count']
            agg['Coverage (%)'] = (agg['Avg Score'] / 2.0 * 100).round(1)
            agg = agg.sort_values('Coverage (%)')
            
            st.dataframe(agg, use_container_width=True, hide_index=True)
            
            col1, col2, col3 = st.columns(3)
            col1.metric("States Analyzed", len(agg))
            col2.metric("Facilities with Capability", len(results_df))
            col3.metric("Avg Coverage", f"{agg['Coverage (%)'].mean():.1f}%")
        else:
            st.warning("No facilities found with this capability")
    
    with tab2:
        st.header("Facility Evidence")
        if not results_df.empty:
            for idx, row in results_df.head(20).iterrows():
                with st.expander(f"{row['name']} - Score: {row['score']:.1f}/2.0"):
                    st.markdown(f"**Location:** {row['city']}, {row['state']}")
                    st.markdown(f"**Trust Score:** {row['score']:.1f} / 2.0")
                    if row['evidence']:
                        st.markdown("**Evidence:**")
                        for ev in row['evidence']:
                            st.text(ev)
        else:
            st.info("No facility details available")

if __name__ == "__main__":
    main()