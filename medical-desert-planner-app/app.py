"""Medical Desert Planner - Streamlit App.

Main application for trust-weighted healthcare gap identification.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, List, Optional
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from config import (
    APP_CONFIG, CAPABILITIES, GEOGRAPHY_LEVELS, TABLES,
    get_capability, get_capability_display_names
)
from helpers import (
    TrustScorer, GeographicAggregator, NFHSValidator, 
    ConfidenceScorer, ScenarioPersistence
)


# Page configuration
st.set_page_config(
    page_title=APP_CONFIG["title"],
    page_icon=APP_CONFIG["page_icon"],
    layout=APP_CONFIG["layout"],
    initial_sidebar_state=APP_CONFIG["initial_sidebar_state"]
)


# Initialize Spark session
@st.cache_resource
def get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


spark = get_spark()


# Initialize helper classes
@st.cache_resource
def get_helpers():
    return {
        "trust_scorer": TrustScorer(spark),
        "geo_aggregator": GeographicAggregator(spark),
        "nfhs_validator": NFHSValidator(spark),
        "confidence_scorer": ConfidenceScorer(),
        "scenario_persistence": ScenarioPersistence(spark)
    }


helpers = get_helpers()


# Load and cache base data
@st.cache_data
def load_facilities():
    """Load facilities table."""
    df = spark.table(TABLES["facilities"])
    return df.toPandas()


@st.cache_data
def load_pincode_directory():
    """Load pincode directory."""
    df = spark.table(TABLES["pincode"])
    return df.toPandas()


# Session state initialization
if "selected_capability" not in st.session_state:
    st.session_state.selected_capability = None
if "selected_state" not in st.session_state:
    st.session_state.selected_state = None
if "selected_district" not in st.session_state:
    st.session_state.selected_district = None
if "gap_data" not in st.session_state:
    st.session_state.gap_data = None
if "user_edits" not in st.session_state:
    st.session_state.user_edits = {}


# Header
st.title(f"{APP_CONFIG['page_icon']} {APP_CONFIG['title']}")
st.markdown(f"*{APP_CONFIG['subtitle']}*")
st.divider()


# Sidebar - Capability and Geography Selection
with st.sidebar:
    st.header("🎯 Planner Inputs")
    
    # Capability selector
    capability_options = get_capability_display_names()
    selected_capability_display = st.selectbox(
        "🏥 Healthcare Capability",
        capability_options,
        help="Select the healthcare service to analyze"
    )
    
    # Map display name back to capability name
    capability_name = None
    for name, cap in CAPABILITIES.items():
        if cap.display_name == selected_capability_display:
            capability_name = name
            break
    
    st.session_state.selected_capability = capability_name
    
    st.divider()
    
    # Geography selector
    st.subheader("🗺️ Geographic Scope")
    
    # Load unique states
    facilities_df = load_facilities()
    states = sorted(facilities_df["address_stateOrRegion"].dropna().unique())
    
    selected_state = st.selectbox(
        "State",
        ["(All States)"] + states,
        help="Select a state to analyze"
    )
    
    if selected_state != "(All States)":
        st.session_state.selected_state = selected_state
        
        # Filter districts for selected state
        state_facilities = facilities_df[
            facilities_df["address_stateOrRegion"] == selected_state
        ]
        districts = sorted(state_facilities["pin_district"].dropna().unique())
        
        selected_district = st.selectbox(
            "District (Optional)",
            ["(All Districts)"] + districts,
            help="Drill down to a specific district"
        )
        
        if selected_district != "(All Districts)":
            st.session_state.selected_district = selected_district
        else:
            st.session_state.selected_district = None
    else:
        st.session_state.selected_state = None
        st.session_state.selected_district = None
    
    st.divider()
    
    # Analyze button
    if st.button("🔍 Analyze Gaps", type="primary", use_container_width=True):
        with st.spinner("Analyzing medical deserts..."):
            # Load facilities
            facilities_spark = spark.table(TABLES["facilities"])
            
            # Calculate trust scores
            capability_config = get_capability(capability_name)
            facilities_scored = helpers["trust_scorer"].calculate_trust_score(
                facilities_spark, capability_config
            )
            
            # Join with pincode directory
            pincode_df = spark.table(TABLES["pincode"])
            facilities_geo = facilities_scored.join(
                pincode_df,
                facilities_scored["address_zipOrPostcode"] == pincode_df["pincode"],
                "left"
            ).select(
                facilities_scored["*"],
                pincode_df["statename"].alias("pin_state"),
                pincode_df["districtname"].alias("pin_district")
            )
            
            # Determine geography level
            if st.session_state.selected_district:
                geography_level = "district"
                facilities_filtered = facilities_geo.filter(
                    (col("address_stateOrRegion") == st.session_state.selected_state) &
                    (col("pin_district") == st.session_state.selected_district)
                )
            elif st.session_state.selected_state:
                geography_level = "state"
                facilities_filtered = facilities_geo.filter(
                    col("address_stateOrRegion") == st.session_state.selected_state
                )
            else:
                geography_level = "state"
                facilities_filtered = facilities_geo
            
            # Aggregate
            facility_agg = helpers["geo_aggregator"].aggregate_by_geography(
                facilities_filtered, capability_name, geography_level
            )
            
            # Validate against NFHS
            gap_validated = helpers["nfhs_validator"].validate_gaps(
                facility_agg, capability_config, geography_level
            )
            
            # Calculate confidence
            gap_with_confidence = helpers["confidence_scorer"].calculate_confidence(
                gap_validated
            )
            
            # Store in session state
            st.session_state.gap_data = gap_with_confidence.toPandas()
            st.rerun()


# Main content area
if st.session_state.gap_data is not None:
    gap_df = st.session_state.gap_data
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Regions",
            len(gap_df),
            help="Number of geographic regions analyzed"
        )
    
    with col2:
        high_confidence_gaps = len(gap_df[gap_df["gap_type"] == "HIGH_CONFIDENCE_GAP"])
        st.metric(
            "🔴 High Confidence Gaps",
            high_confidence_gaps,
            help="Regions with both low facility trust AND poor health outcomes"
        )
    
    with col3:
        avg_confidence = gap_df["confidence_score"].mean()
        st.metric(
            "Avg Confidence Score",
            f"{avg_confidence:.1f}%",
            help="Average confidence in gap identification"
        )
    
    with col4:
        data_quality_issues = len(gap_df[gap_df["gap_type"] == "DATA_QUALITY_ISSUE"])
        st.metric(
            "⚠️ Data Quality Issues",
            data_quality_issues,
            help="Regions with low facility data but good health outcomes"
        )
    
    st.divider()
    
    # Heatmap visualization
    st.subheader("🗺️ Coverage Heatmap")
    
    # Create choropleth-style visualization
    fig = px.scatter(
        gap_df,
        x="trust_percentage",
        y="avg_outcome",
        size="facility_count",
        color="confidence_score",
        hover_data=["address_stateOrRegion", "gap_type", "confidence_level"],
        color_continuous_scale="RdYlGn",
        labels={
            "trust_percentage": "Facility Trust (%)",
            "avg_outcome": "Health Outcome (%)",
            "confidence_score": "Confidence"
        },
        title=f"{st.session_state.selected_capability.title()} Care Coverage"
    )
    
    # Add quadrant lines
    fig.add_hline(y=70, line_dash="dash", line_color="gray", opacity=0.5)
    fig.add_vline(x=65, line_dash="dash", line_color="gray", opacity=0.5)
    
    # Add quadrant annotations
    fig.add_annotation(
        x=30, y=85,
        text="HIGH CONFIDENCE GAP",
        showarrow=False,
        font=dict(color="red", size=10)
    )
    fig.add_annotation(
        x=85, y=85,
        text="WELL SERVED",
        showarrow=False,
        font=dict(color="green", size=10)
    )
    fig.add_annotation(
        x=30, y=55,
        text="QUALITY CONCERN",
        showarrow=False,
        font=dict(color="orange", size=10)
    )
    fig.add_annotation(
        x=85, y=55,
        text="DATA QUALITY ISSUE",
        showarrow=False,
        font=dict(color="blue", size=10)
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # Drill-down table
    st.subheader("📊 Gap Analysis Drill-Down")
    
    # Filter options
    col1, col2 = st.columns(2)
    with col1:
        gap_type_filter = st.multiselect(
            "Filter by Gap Type",
            gap_df["gap_type"].unique(),
            default=gap_df["gap_type"].unique()
        )
    with col2:
        confidence_filter = st.multiselect(
            "Filter by Confidence Level",
            gap_df["confidence_level"].unique(),
            default=gap_df["confidence_level"].unique()
        )
    
    # Apply filters
    filtered_df = gap_df[
        (gap_df["gap_type"].isin(gap_type_filter)) &
        (gap_df["confidence_level"].isin(confidence_filter))
    ]
    
    # Sort by confidence score
    filtered_df = filtered_df.sort_values("confidence_score", ascending=False)
    
    # Display table
    st.dataframe(
        filtered_df[[
            "address_stateOrRegion",
            "pin_district",
            "facility_count",
            "trust_percentage",
            "avg_outcome",
            "gap_type",
            "confidence_score",
            "confidence_level"
        ]].rename(columns={
            "address_stateOrRegion": "State",
            "pin_district": "District",
            "facility_count": "Facilities",
            "trust_percentage": "Trust %",
            "avg_outcome": "Outcome %",
            "gap_type": "Gap Type",
            "confidence_score": "Confidence",
            "confidence_level": "Level"
        }),
        use_container_width=True,
        height=400
    )
    
    st.divider()
    
    # Facility Evidence Panel
    st.subheader("📋 Facility Evidence & Live Corrections")
    
    with st.expander("ℹ️ How This Works"):
        st.markdown("""
        **Draft Confidence Score:** The score you see updates in real-time as you:
        - Fill in missing facility data
        - Override trust scores based on field knowledge
        - Add scenario assumptions
        
        The draft score shows you what would happen if these corrections were applied.
        Save the scenario to persist your analysis.
        """)
    
    # Select a region to view details
    selected_region_idx = st.selectbox(
        "Select Region for Details",
        range(len(filtered_df)),
        format_func=lambda x: f"{filtered_df.iloc[x]['address_stateOrRegion']} - {filtered_df.iloc[x]['pin_district'] if 'pin_district' in filtered_df.columns else 'State Level'}"
    )
    
    if selected_region_idx is not None:
        selected_region = filtered_df.iloc[selected_region_idx]
        
        st.markdown(f"### Region: {selected_region['address_stateOrRegion']}")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Current Confidence", f"{selected_region['confidence_score']:.1f}%")
        with col2:
            st.metric("Facility Count", int(selected_region['facility_count']))
        with col3:
            st.metric("Gap Type", selected_region['gap_type'])
        
        # Live correction form
        st.markdown("#### ✏️ Apply Corrections")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Override trust score
            new_trust = st.slider(
                "Override Trust Score",
                0.0, 100.0, 
                float(selected_region['trust_percentage']),
                help="Adjust based on field knowledge or additional evidence"
            )
        
        with col2:
            # Override outcome
            new_outcome = st.slider(
                "Override Health Outcome",
                0.0, 100.0,
                float(selected_region['avg_outcome']) if pd.notna(selected_region['avg_outcome']) else 50.0,
                help="Adjust based on recent surveys or local data"
            )
        
        # Additional notes
        correction_notes = st.text_area(
            "Correction Notes",
            placeholder="Explain the rationale for these corrections...",
            help="Document your reasoning for future reference"
        )
        
        # Calculate draft score with corrections
        if new_trust != selected_region['trust_percentage'] or \
           new_outcome != selected_region['avg_outcome']:
            
            st.markdown("#### 📊 Draft Score with Corrections")
            
            # Recalculate gap type
            if new_trust < 65 and new_outcome < 70:
                draft_gap_type = "HIGH_CONFIDENCE_GAP"
            elif new_trust < 65 and new_outcome >= 70:
                draft_gap_type = "DATA_QUALITY_ISSUE"
            elif new_trust >= 65 and new_outcome >= 70:
                draft_gap_type = "WELL_SERVED"
            else:
                draft_gap_type = "QUALITY_CONCERN"
            
            # Recalculate confidence (simplified)
            signal_alignment = 1.0 if draft_gap_type in ["HIGH_CONFIDENCE_GAP", "WELL_SERVED"] else 0.3
            draft_confidence = (
                0.30 * (selected_region['facility_count'] / 50.0) +
                0.40 * signal_alignment +
                0.20 * (selected_region['facility_count'] / 100.0) +
                0.10 * (new_trust / 100.0)
            ) * 100
            
            draft_confidence = min(100, max(0, draft_confidence))
            
            col1, col2 = st.columns(2)
            with col1:
                delta = draft_confidence - selected_region['confidence_score']
                st.metric(
                    "Draft Confidence",
                    f"{draft_confidence:.1f}%",
                    delta=f"{delta:+.1f}%",
                    delta_color="normal"
                )
            with col2:
                st.metric("Draft Gap Type", draft_gap_type)
            
            if correction_notes:
                st.info(f"📝 Notes: {correction_notes}")
    
    st.divider()
    
    # Save scenario
    st.subheader("💾 Save Planning Scenario")
    
    col1, col2 = st.columns(2)
    
    with col1:
        scenario_name = st.text_input(
            "Scenario Name",
            value=f"{st.session_state.selected_capability.title()} - {st.session_state.selected_state or 'National'} - {datetime.now().strftime('%Y-%m-%d')}"
        )
    
    with col2:
        user_notes = st.text_area(
            "Planning Notes",
            placeholder="Document your planning decisions and priorities..."
        )
    
    if st.button("💾 Save Scenario", type="primary"):
        try:
            # Convert back to Spark DataFrame for saving
            gap_spark_df = spark.createDataFrame(gap_df)
            
            scenario_id = helpers["scenario_persistence"].save_scenario(
                scenario_name=scenario_name,
                user_id="current_user",  # Replace with actual user ID
                capability=st.session_state.selected_capability,
                geography={
                    "state": st.session_state.selected_state,
                    "district": st.session_state.selected_district
                },
                gap_data=gap_spark_df,
                user_notes=user_notes
            )
            
            st.success(f"✅ Scenario saved! ID: {scenario_id}")
        except Exception as e:
            st.error(f"❌ Error saving scenario: {str(e)}")

else:
    # Welcome screen
    st.info("👋 Welcome! Select a capability and geography from the sidebar, then click 'Analyze Gaps' to begin.")
    
    # Show capability descriptions
    st.subheader("🎯 Available Capabilities")
    
    for cap_name, cap_config in CAPABILITIES.items():
        with st.expander(f"{cap_config.display_name}"):
            st.markdown(cap_config.description)
            st.markdown(f"**Validated against:** {cap_config.nfhs_outcome_field}")


# Footer
st.divider()
st.caption("🏆 Databricks DAIS 2026 Hackathon - Track 2: Medical Desert Planner")
st.caption("🔑 Key Differentiator: Confidence quantification via NFHS outcome validation")
