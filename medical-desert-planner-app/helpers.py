"""Helper functions for Medical Desert Planner App.

Extracted from notebook logic for reusability.
"""

import re
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lower, concat_ws, when, lit, count, avg, sum as spark_sum,
    round as spark_round, trim, upper, regexp_replace, expr, coalesce
)
from config import (
    CapabilityConfig, CONFIDENCE_WEIGHTS, TRUST_THRESHOLDS, 
    GAP_THRESHOLDS, TABLES, get_capability
)


class TrustScorer:
    """Calculate trust scores for facility capabilities."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def calculate_trust_score(self, df: DataFrame, capability: CapabilityConfig) -> DataFrame:
        """
        Calculate trust score for a capability based on multi-field keyword matching.
        
        Args:
            df: Facilities DataFrame
            capability: Capability configuration with keywords
            
        Returns:
            DataFrame with {capability}_trust_score column added
        """
        score_col = f"{capability.name}_trust_score"
        
        # Combine all text fields into searchable text
        df = df.withColumn(
            "_search_text",
            lower(concat_ws(
                " ",
                coalesce(col("description"), lit("")),
                coalesce(col("capability"), lit("")),
                coalesce(col("procedure"), lit("")),
                coalesce(col("equipment"), lit("")),
                coalesce(col("specialties"), lit(""))
            ))
        )
        
        # Build keyword patterns
        all_keywords = []
        for keyword_list in capability.keywords.values():
            all_keywords.extend(keyword_list)
        
        pattern = "|".join([re.escape(kw.lower()) for kw in all_keywords])
        
        # Count field mentions
        field_mentions = (
            when(lower(col("description")).rlike(pattern), 1).otherwise(0) +
            when(lower(col("capability")).rlike(pattern), 1).otherwise(0) +
            when(lower(col("procedure")).rlike(pattern), 1).otherwise(0) +
            when(lower(col("equipment")).rlike(pattern), 1).otherwise(0) +
            when(lower(col("specialties")).rlike(pattern), 1).otherwise(0)
        )
        
        # Assign trust score based on field count
        df = df.withColumn(
            score_col,
            when(field_mentions >= 3, TRUST_THRESHOLDS["strong"])
            .when(field_mentions >= 1, TRUST_THRESHOLDS["partial"])
            .otherwise(TRUST_THRESHOLDS["none"])
        )
        
        # Extract evidence citations
        df = df.withColumn(
            f"{capability.name}_evidence",
            concat_ws(
                " | ",
                when(lower(col("description")).rlike(pattern), col("description")).otherwise(lit("")),
                when(lower(col("capability")).rlike(pattern), col("capability")).otherwise(lit("")),
                when(lower(col("procedure")).rlike(pattern), col("procedure")).otherwise(lit("")),
                when(lower(col("equipment")).rlike(pattern), col("equipment")).otherwise(lit("")),
                when(lower(col("specialties")).rlike(pattern), col("specialties")).otherwise(lit(""))
            )
        )
        
        return df.drop("_search_text")
    
    def score_all_capabilities(self, df: DataFrame, capabilities: List[str]) -> DataFrame:
        """Score multiple capabilities for a facilities DataFrame."""
        for cap_name in capabilities:
            capability = get_capability(cap_name)
            df = self.calculate_trust_score(df, capability)
        return df


class GeographicAggregator:
    """Aggregate facility trust scores by geography."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def aggregate_by_geography(
        self, 
        df: DataFrame, 
        capability_name: str,
        geography_level: str
    ) -> DataFrame:
        """
        Aggregate trust scores by geographic level.
        
        Args:
            df: Facilities DataFrame with trust scores
            capability_name: Name of capability to aggregate
            geography_level: 'state', 'district', or 'pincode'
            
        Returns:
            Aggregated DataFrame with trust metrics
        """
        score_col = f"{capability_name}_trust_score"
        
        # Map geography level to column names
        geo_cols = {
            "state": ["address_stateOrRegion"],
            "district": ["address_stateOrRegion", "pin_district"],
            "pincode": ["address_stateOrRegion", "pin_district", "address_zipOrPostcode"]
        }
        
        group_cols = geo_cols.get(geography_level, geo_cols["state"])
        
        # Aggregate
        agg_df = df.filter(col(score_col).isNotNull()).groupBy(*group_cols).agg(
            count("*").alias("facility_count"),
            avg(score_col).alias("avg_trust_score"),
            spark_round(avg(score_col) * 100 / TRUST_THRESHOLDS["strong"], 1).alias("trust_percentage"),
            spark_sum(when(col(score_col) == TRUST_THRESHOLDS["strong"], 1).otherwise(0)).alias("strong_evidence_count"),
            spark_sum(when(col(score_col) == TRUST_THRESHOLDS["partial"], 1).otherwise(0)).alias("partial_evidence_count"),
            spark_sum(when(col(score_col) == TRUST_THRESHOLDS["none"], 1).otherwise(0)).alias("no_evidence_count")
        )
        
        # Add capability name
        agg_df = agg_df.withColumn("capability", lit(capability_name))
        agg_df = agg_df.withColumn("geography_level", lit(geography_level))
        
        return agg_df


class NFHSValidator:
    """Validate facility gaps against NFHS health outcomes."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._nfhs_clean = None
    
    def load_nfhs_clean(self) -> DataFrame:
        """Load and clean NFHS data."""
        if self._nfhs_clean is not None:
            return self._nfhs_clean
        
        nfhs = self.spark.table(TABLES["nfhs"])
        
        # Clean NFHS data - remove parentheses and asterisks, cast to double
        nfhs_clean = nfhs.select(
            trim(upper(col("state_ut"))).alias("state"),
            trim(col("district")).alias("district"),
            
            # Clean numeric fields
            expr("TRY_CAST(REGEXP_REPLACE(institutional_birth_5y, '[()]|\\*', '') AS DOUBLE)").alias("institutional_birth_5y_pct"),
            expr("TRY_CAST(REGEXP_REPLACE(female_literacy_rate_15_49_years, '[()]|\\*', '') AS DOUBLE)").alias("female_literacy_pct"),
            expr("TRY_CAST(REGEXP_REPLACE(households_using_clean_fuel_for_cooking, '[()]|\\*', '') AS DOUBLE)").alias("clean_fuel_pct"),
            expr("TRY_CAST(REGEXP_REPLACE(mothers_who_had_at_least_4_anc_visits, '[()]|\\*', '') AS DOUBLE)").alias("anc_4_visits_pct"),
            expr("TRY_CAST(REGEXP_REPLACE(deliveries_by_caesarean_section, '[()]|\\*', '') AS DOUBLE)").alias("csection_pct")
        )
        
        self._nfhs_clean = nfhs_clean
        return nfhs_clean
    
    def validate_gaps(
        self, 
        facility_agg: DataFrame, 
        capability: CapabilityConfig,
        geography_level: str = "state"
    ) -> DataFrame:
        """
        Cross-validate facility gaps with NFHS outcomes.
        
        Args:
            facility_agg: Aggregated facility trust scores
            capability: Capability configuration
            geography_level: Geographic level of aggregation
            
        Returns:
            DataFrame with gap validation and confidence scores
        """
        nfhs_clean = self.load_nfhs_clean()
        
        # Aggregate NFHS by geography
        if geography_level == "state":
            nfhs_agg = nfhs_clean.groupBy("state").agg(
                spark_round(avg(capability.nfhs_outcome_field), 1).alias("avg_outcome")
            )
            join_col = "state"
            facility_join_col = "address_stateOrRegion"
        else:
            # For district level
            nfhs_agg = nfhs_clean.groupBy("state", "district").agg(
                spark_round(avg(capability.nfhs_outcome_field), 1).alias("avg_outcome")
            )
            join_col = ["state", "district"]
            facility_join_col = ["address_stateOrRegion", "pin_district"]
        
        # Join facility and NFHS data
        if isinstance(join_col, list):
            joined = facility_agg.join(
                nfhs_agg,
                (facility_agg[facility_join_col[0]] == nfhs_agg[join_col[0]]) &
                (facility_agg[facility_join_col[1]] == nfhs_agg[join_col[1]]),
                "left"
            )
        else:
            joined = facility_agg.join(
                nfhs_agg,
                facility_agg[facility_join_col] == nfhs_agg[join_col],
                "left"
            )
        
        # Classify gap type
        joined = joined.withColumn(
            "gap_type",
            when(
                (col("trust_percentage") < GAP_THRESHOLDS["facility_trust_low"]) &
                (col("avg_outcome") < GAP_THRESHOLDS["outcome_low"]),
                lit("HIGH_CONFIDENCE_GAP")
            ).when(
                (col("trust_percentage") < GAP_THRESHOLDS["facility_trust_low"]) &
                (col("avg_outcome") >= GAP_THRESHOLDS["outcome_low"]),
                lit("DATA_QUALITY_ISSUE")
            ).when(
                (col("trust_percentage") >= GAP_THRESHOLDS["facility_trust_low"]) &
                (col("avg_outcome") >= GAP_THRESHOLDS["outcome_low"]),
                lit("WELL_SERVED")
            ).otherwise(lit("QUALITY_CONCERN"))
        )
        
        return joined


class ConfidenceScorer:
    """Calculate confidence scores for gap identification."""
    
    @staticmethod
    def calculate_confidence(
        gap_df: DataFrame,
        facility_count_col: str = "facility_count",
        trust_pct_col: str = "trust_percentage",
        outcome_col: str = "avg_outcome"
    ) -> DataFrame:
        """
        Calculate confidence score based on multiple factors.
        
        Confidence = 
            30% data completeness (field coverage) +
            40% signal alignment (trust vs outcome correlation) +
            20% sample size (facility count) +
            10% field coverage (evidence quality)
        """
        # Data completeness: normalized facility count
        gap_df = gap_df.withColumn(
            "_data_completeness",
            when(col(facility_count_col) >= 50, lit(1.0))
            .when(col(facility_count_col) >= 20, lit(0.8))
            .when(col(facility_count_col) >= 10, lit(0.6))
            .when(col(facility_count_col) >= 5, lit(0.4))
            .otherwise(lit(0.2))
        )
        
        # Signal alignment: do trust and outcome align?
        gap_df = gap_df.withColumn(
            "_signal_alignment",
            when(col("gap_type") == "HIGH_CONFIDENCE_GAP", lit(1.0))
            .when(col("gap_type") == "WELL_SERVED", lit(1.0))
            .when(col("gap_type") == "DATA_QUALITY_ISSUE", lit(0.3))
            .otherwise(lit(0.5))
        )
        
        # Sample size: normalized facility count
        gap_df = gap_df.withColumn(
            "_sample_size",
            when(col(facility_count_col) >= 100, lit(1.0))
            .when(col(facility_count_col) >= 50, lit(0.9))
            .when(col(facility_count_col) >= 20, lit(0.7))
            .when(col(facility_count_col) >= 10, lit(0.5))
            .otherwise(lit(0.3))
        )
        
        # Field coverage: trust percentage as proxy
        gap_df = gap_df.withColumn(
            "_field_coverage",
            col(trust_pct_col) / 100.0
        )
        
        # Calculate weighted confidence
        gap_df = gap_df.withColumn(
            "confidence_score",
            spark_round(
                (
                    col("_data_completeness") * CONFIDENCE_WEIGHTS["data_completeness"] +
                    col("_signal_alignment") * CONFIDENCE_WEIGHTS["signal_alignment"] +
                    col("_sample_size") * CONFIDENCE_WEIGHTS["sample_size"] +
                    col("_field_coverage") * CONFIDENCE_WEIGHTS["field_coverage"]
                ) * 100,
                1
            )
        )
        
        # Add confidence level
        gap_df = gap_df.withColumn(
            "confidence_level",
            when(col("confidence_score") >= 80, lit("HIGH"))
            .when(col("confidence_score") >= 60, lit("MEDIUM"))
            .otherwise(lit("LOW"))
        )
        
        # Drop intermediate columns
        return gap_df.drop(
            "_data_completeness", "_signal_alignment", 
            "_sample_size", "_field_coverage"
        )


class ScenarioPersistence:
    """Save and load planning scenarios to Delta tables."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def save_scenario(
        self,
        scenario_name: str,
        user_id: str,
        capability: str,
        geography: Dict[str, str],
        gap_data: DataFrame,
        user_notes: str = ""
    ) -> str:
        """
        Save a planning scenario with version history.
        
        Returns:
            scenario_id (UUID)
        """
        import uuid
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType
        
        scenario_id = str(uuid.uuid4())
        timestamp = datetime.now()
        
        # Convert gap_data to JSON for storage
        selected_facilities = gap_data.select("facility_id").rdd.flatMap(lambda x: x).collect()
        
        # Create scenario record
        scenario_data = [(
            scenario_id,
            scenario_name,
            user_id,
            capability,
            str(geography),
            selected_facilities,
            user_notes,
            timestamp
        )]
        
        schema = StructType([
            StructField("scenario_id", StringType(), False),
            StructField("scenario_name", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("capability", StringType(), False),
            StructField("geography", StringType(), False),
            StructField("selected_facilities", ArrayType(StringType()), True),
            StructField("user_notes", StringType(), True),
            StructField("created_at", TimestampType(), False)
        ])
        
        scenario_df = self.spark.createDataFrame(scenario_data, schema)
        
        # Append to Delta table
        scenario_df.write.mode("append").saveAsTable(TABLES["user_scenarios"])
        
        return scenario_id
    
    def load_scenarios(self, user_id: Optional[str] = None) -> DataFrame:
        """Load saved scenarios, optionally filtered by user."""
        scenarios = self.spark.table(TABLES["user_scenarios"])
        
        if user_id:
            scenarios = scenarios.filter(col("user_id") == user_id)
        
        return scenarios.orderBy(col("created_at").desc())
