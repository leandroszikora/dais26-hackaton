"""Configuration module for Medical Desert Planner App.

Defines capability registry, scoring weights, and app constants.
"""

from typing import Dict, List, Any
from dataclasses import dataclass


@dataclass
class CapabilityConfig:
    """Configuration for a healthcare capability."""
    name: str
    display_name: str
    keywords: Dict[str, List[str]]
    nfhs_outcome_field: str  # Field in NFHS data to validate against
    description: str


# Capability Registry - fully configurable
CAPABILITIES: Dict[str, CapabilityConfig] = {
    "maternity": CapabilityConfig(
        name="maternity",
        display_name="Maternity Care",
        keywords={
            "primary": ["maternity", "gynecology", "obstetrics", "delivery", 
                       "antenatal", "postnatal", "prenatal", "labor", "childbirth"],
            "procedures": ["cesarean", "c-section", "episiotomy", "normal delivery"],
            "equipment": ["delivery table", "incubator", "fetal monitor", "ultrasound"],
            "specialties": ["obgyn", "gynecologist", "obstetrician"]
        },
        nfhs_outcome_field="institutional_birth_5y_pct",
        description="Maternity and childbirth services including prenatal care, delivery, and postnatal support"
    ),
    
    "emergency": CapabilityConfig(
        name="emergency",
        display_name="Emergency Care",
        keywords={
            "primary": ["emergency", "trauma", "icu", "intensive care", "critical care", 
                       "casualty", "urgent", "ambulance"],
            "procedures": ["resuscitation", "intubation", "defibrillation", "trauma surgery"],
            "equipment": ["ventilator", "defibrillator", "oxygen", "ecg", "crash cart"],
            "specialties": ["emergency medicine", "critical care", "trauma"]
        },
        nfhs_outcome_field="anc_4_visits_pct",  # Proxy: good healthcare access
        description="Emergency and critical care services for life-threatening conditions"
    ),
    
    "dialysis": CapabilityConfig(
        name="dialysis",
        display_name="Dialysis",
        keywords={
            "primary": ["dialysis", "kidney", "renal", "nephrology", "hemodialysis"],
            "procedures": ["dialysis treatment", "kidney treatment", "hemodialysis", "peritoneal dialysis"],
            "equipment": ["dialysis machine", "dialyzer", "hemodialysis unit"],
            "specialties": ["nephrologist", "nephrology"]
        },
        nfhs_outcome_field="clean_fuel_pct",  # Proxy: economic development
        description="Kidney dialysis and renal care services"
    ),
    
    "blood_bank": CapabilityConfig(
        name="blood_bank",
        display_name="Blood Bank",
        keywords={
            "primary": ["blood bank", "blood storage", "blood donation", "blood transfusion"],
            "procedures": ["blood collection", "blood screening", "transfusion"],
            "equipment": ["blood refrigerator", "blood bag", "cross-match"],
            "specialties": ["transfusion medicine", "hematology"]
        },
        nfhs_outcome_field="institutional_birth_5y_pct",  # Blood banks critical for deliveries
        description="Blood collection, storage, and transfusion services"
    ),
    
    "nicu": CapabilityConfig(
        name="nicu",
        display_name="NICU",
        keywords={
            "primary": ["nicu", "neonatal", "newborn", "pediatric intensive care"],
            "procedures": ["neonatal care", "premature care", "newborn resuscitation"],
            "equipment": ["incubator", "infant ventilator", "phototherapy", "radiant warmer"],
            "specialties": ["neonatologist", "neonatology", "pediatric intensive care"]
        },
        nfhs_outcome_field="institutional_birth_5y_pct",
        description="Neonatal intensive care for premature and critically ill newborns"
    ),
    
    "surgery": CapabilityConfig(
        name="surgery",
        display_name="General Surgery",
        keywords={
            "primary": ["surgery", "operation", "surgical", "operating room", "ot"],
            "procedures": ["appendectomy", "hernia", "laparoscopy", "surgical procedure"],
            "equipment": ["operation table", "anesthesia machine", "surgical instruments", "ot lights"],
            "specialties": ["surgeon", "general surgery", "surgical"]
        },
        nfhs_outcome_field="csection_pct",  # Proxy: surgical capacity
        description="General surgical services and operating room facilities"
    )
}


# Confidence Scoring Weights
CONFIDENCE_WEIGHTS = {
    "data_completeness": 0.30,    # 30% - How much data we have
    "signal_alignment": 0.40,     # 40% - Do facility gap + outcome gap align?
    "sample_size": 0.20,          # 20% - How many facilities/districts in region?
    "field_coverage": 0.10        # 10% - % of facilities with evidence
}


# Trust Score Thresholds
TRUST_THRESHOLDS = {
    "strong": 3,    # Mentioned in 3+ fields
    "partial": 2,   # Mentioned in 1-2 fields
    "weak": 1,      # Single vague mention
    "none": 0       # No mention
}


# Geographic Levels
GEOGRAPHY_LEVELS = ["state", "district", "pincode"]


# Gap Classification Thresholds
GAP_THRESHOLDS = {
    "facility_trust_low": 65,        # Trust % below this = low facility trust
    "outcome_low": 70,               # NFHS outcome below this = poor outcome
    "high_confidence_threshold": 80  # Combined confidence above this = high confidence
}


# Database Configuration
CATALOG = "databricks_virtue_foundation_dataset_dais_2026"
SCHEMA = "virtue_foundation_dataset"
OUTPUT_CATALOG = "main"  # For saving scenarios
OUTPUT_SCHEMA = "medical_desert"

# Table names
TABLES = {
    "facilities": f"{CATALOG}.{SCHEMA}.facilities",
    "nfhs": f"{CATALOG}.{SCHEMA}.nfhs_5_district_health_indicators",
    "pincode": f"{CATALOG}.{SCHEMA}.india_post_pincode_directory",
    
    # Output tables
    "facility_trust_scores": f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.facility_trust_scores",
    "gap_validation": f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.gap_validation",
    "user_scenarios": f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.user_scenarios"
}


# App UI Configuration
APP_CONFIG = {
    "title": "Medical Desert Planner",
    "subtitle": "Trust-weighted healthcare gap identification",
    "page_icon": "🏥",
    "layout": "wide",
    "initial_sidebar_state": "expanded"
}


def get_capability(capability_name: str) -> CapabilityConfig:
    """Get capability configuration by name."""
    if capability_name not in CAPABILITIES:
        raise ValueError(f"Unknown capability: {capability_name}. Available: {list(CAPABILITIES.keys())}")
    return CAPABILITIES[capability_name]


def get_capability_names() -> List[str]:
    """Get list of all capability names."""
    return list(CAPABILITIES.keys())


def get_capability_display_names() -> List[str]:
    """Get list of all capability display names."""
    return [cap.display_name for cap in CAPABILITIES.values()]
