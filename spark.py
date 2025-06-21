# Data Lakehouse Implementation with PySpark
# This implementation follows the medallion architecture pattern

import os
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataLakehouse:
    """
    Main class for managing the data lakehouse architecture
    Implements medallion architecture: Bronze -> Silver -> Gold
    """
    
    def __init__(self, spark_config: Dict[str, str] = None):
        """Initialize the data lakehouse with Spark configuration"""
        self.spark = self._create_spark_session(spark_config)
        self.bronze_path = "s3a://datalake/bronze"  # Raw data layer
        self.silver_path = "s3a://datalake/silver"  # Cleaned data layer
        self.gold_path = "s3a://datalake/gold"      # Business-ready layer
        self.checkpoint_path = "s3a://datalake/checkpoints"
        
    def _create_spark_session(self, config: Dict[str, str] = None) -> SparkSession:
        """Create and configure Spark session"""
        builder = SparkSession.builder \
            .appName("DataLakehouse") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        
        # Add additional configurations if provided
        if config:
            for key, value in config.items():
                builder = builder.config(key, value)
        
        return builder.getOrCreate()
    
    def close(self):
        """Close Spark session"""
        self.spark.stop()

class BronzeLayer:
    """
    Bronze layer - Raw data ingestion and storage
    Data is stored as-is with minimal transformation
    """
    
    def __init__(self, lakehouse: DataLakehouse):
        self.lakehouse = lakehouse
        self.spark = lakehouse.spark
        
    def ingest_batch_data(self, source_path: str, table_name: str, 
                        file_format: str = "parquet", 
                        partition_cols: List[str] = None) -> None:
        """
        Ingest batch data into bronze layer
        
        Args:
            source_path: Path to source data
            table_name: Name of the table in bronze layer
            file_format: Format of source files (parquet, json, csv, etc.)
            partition_cols: Columns to partition by
        """
        try:
            # Read source data
            if file_format.lower() == "csv":
                df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)
            elif file_format.lower() == "json":
                df = self.spark.read.json(source_path)
            elif file_format.lower() == "parquet":
                df = self.spark.read.parquet(source_path)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
            
            # Add metadata columns
            df = df.withColumn("_ingestion_timestamp", current_timestamp()) \
                    .withColumn("_source_file", input_file_name()) \
                    .withColumn("_processing_date", current_date())
            
            # Write to bronze layer
            bronze_path = f"{self.lakehouse.bronze_path}/{table_name}"
            
            writer = df.write.mode("append").format("delta")
            
            if partition_cols:
                writer = writer.partitionBy(partition_cols)
            
            writer.save(bronze_path)
            
            logger.info(f"Successfully ingested {df.count()} records to bronze layer table: {table_name}")
            
        except Exception as e:
            logger.error(f"Error ingesting data to bronze layer: {str(e)}")
            raise
    
    def ingest_streaming_data(self, source_stream, table_name: str, 
                            checkpoint_location: str = None) -> None:
        """
        Ingest streaming data into bronze layer
        
        Args:
            source_stream: Streaming DataFrame
            table_name: Name of the table in bronze layer
            checkpoint_location: Checkpoint location for streaming
        """
        try:
            # Add metadata columns
            df = source_stream.withColumn("_ingestion_timestamp", current_timestamp()) \
                             .withColumn("_processing_date", current_date())
            
            bronze_path = f"{self.lakehouse.bronze_path}/{table_name}"
            checkpoint_loc = checkpoint_location or f"{self.lakehouse.checkpoint_path}/bronze_{table_name}"
            
            query = df.writeStream \
                     .format("delta") \
                     .outputMode("append") \
                     .option("checkpointLocation", checkpoint_loc) \
                     .start(bronze_path)
            
            logger.info(f"Started streaming ingestion to bronze layer table: {table_name}")
            return query
            
        except Exception as e:
            logger.error(f"Error starting streaming ingestion: {str(e)}")
            raise

class SilverLayer:
    """
    Silver layer - Cleaned and validated data
    Apply data quality rules, deduplication, and basic transformations
    """
    
    def __init__(self, lakehouse: DataLakehouse):
        self.lakehouse = lakehouse
        self.spark = lakehouse.spark
    
    def process_to_silver(self, bronze_table: str, silver_table: str, 
                         transformation_rules: Dict[str, Any] = None,
                         data_quality_rules: Dict[str, Any] = None) -> None:
        """
        Process bronze data to silver layer with cleaning and validation
        
        Args:
            bronze_table: Name of bronze table
            silver_table: Name of silver table
            transformation_rules: Dictionary of transformation rules
            data_quality_rules: Dictionary of data quality rules
        """
        try:
            # Read from bronze layer
            bronze_path = f"{self.lakehouse.bronze_path}/{bronze_table}"
            df = self.spark.read.format("delta").load(bronze_path)
            
            # Apply transformations
            if transformation_rules:
                df = self._apply_transformations(df, transformation_rules)
            
            # Apply data quality rules
            if data_quality_rules:
                df = self._apply_data_quality_rules(df, data_quality_rules)
            
            # Remove duplicates
            df = self._deduplicate_data(df)
            
            # Add silver layer metadata
            df = df.withColumn("_silver_processed_timestamp", current_timestamp()) \
                   .withColumn("_data_quality_score", lit(1.0))  # Placeholder for DQ score
            
            # Write to silver layer
            silver_path = f"{self.lakehouse.silver_path}/{silver_table}"
            
            df.write.mode("overwrite").format("delta").save(silver_path)
            
            logger.info(f"Successfully processed {df.count()} records to silver layer table: {silver_table}")
            
        except Exception as e:
            logger.error(f"Error processing to silver layer: {str(e)}")
            raise
    
    def _apply_transformations(self, df: DataFrame, rules: Dict[str, Any]) -> DataFrame:
        """Apply transformation rules to DataFrame"""
        
        # Example transformations
        if "column_rename" in rules:
            for old_name, new_name in rules["column_rename"].items():
                df = df.withColumnRenamed(old_name, new_name)
        
        if "column_casting" in rules:
            for col_name, data_type in rules["column_casting"].items():
                df = df.withColumn(col_name, col(col_name).cast(data_type))
        
        if "derived_columns" in rules:
            for col_name, expression in rules["derived_columns"].items():
                df = df.withColumn(col_name, expr(expression))
        
        return df
    
    def _apply_data_quality_rules(self, df: DataFrame, rules: Dict[str, Any]) -> DataFrame:
        """Apply data quality rules and filtering"""
        
        # Remove null values in specified columns
        if "not_null_columns" in rules:
            for col_name in rules["not_null_columns"]:
                df = df.filter(col(col_name).isNotNull())
        
        # Apply value range filters
        if "value_ranges" in rules:
            for col_name, range_dict in rules["value_ranges"].items():
                if "min" in range_dict:
                    df = df.filter(col(col_name) >= range_dict["min"])
                if "max" in range_dict:
                    df = df.filter(col(col_name) <= range_dict["max"])
        
        # Apply regex patterns
        if "regex_patterns" in rules:
            for col_name, pattern in rules["regex_patterns"].items():
                df = df.filter(col(col_name).rlike(pattern))
        
        return df
    
    def _deduplicate_data(self, df: DataFrame, key_columns: List[str] = None) -> DataFrame:
        """Remove duplicate records"""
        if key_columns:
            # Deduplicate based on specific columns
            window = Window.partitionBy(key_columns).orderBy(col("_ingestion_timestamp").desc())
            df = df.withColumn("_row_number", row_number().over(window)) \
                   .filter(col("_row_number") == 1) \
                   .drop("_row_number")
        else:
            # Remove exact duplicates
            df = df.dropDuplicates()
        
        return df

class GoldLayer:
    """
    Gold layer - Business-ready aggregated data
    Create dimensional models, aggregations, and business metrics
    """
    
    def __init__(self, lakehouse: DataLakehouse):
        self.lakehouse = lakehouse
        self.spark = lakehouse.spark
    
    def create_dimension_table(self, silver_table: str, dimension_name: str,
                             dimension_columns: List[str], 
                             scd_type: int = 1) -> None:
        """
        Create dimension table from silver layer data
        
        Args:
            silver_table: Source silver table
            dimension_name: Name of dimension table
            dimension_columns: Columns to include in dimension
            scd_type: Slowly Changing Dimension type (1 or 2)
        """
        try:
            silver_path = f"{self.lakehouse.silver_path}/{silver_table}"
            df = self.spark.read.format("delta").load(silver_path)
            
            # Select dimension columns
            dim_df = df.select(dimension_columns).distinct()
            
            # Add surrogate key
            dim_df = dim_df.withColumn("dim_key", monotonically_increasing_id())
            
            if scd_type == 2:
                # Add SCD Type 2 columns
                dim_df = dim_df.withColumn("effective_date", current_date()) \
                              .withColumn("end_date", lit(None).cast(DateType())) \
                              .withColumn("is_current", lit(True))
            
            # Add dimension metadata
            dim_df = dim_df.withColumn("_created_timestamp", current_timestamp()) \
                          .withColumn("_updated_timestamp", current_timestamp())
            
            # Write to gold layer
            gold_path = f"{self.lakehouse.gold_path}/dimensions/{dimension_name}"
            dim_df.write.mode("overwrite").format("delta").save(gold_path)
            
            logger.info(f"Created dimension table: {dimension_name} with {dim_df.count()} records")
            
        except Exception as e:
            logger.error(f"Error creating dimension table: {str(e)}")
            raise
    
    def create_fact_table(self, silver_table: str, fact_name: str,
                         fact_columns: List[str], 
                         dimension_mappings: Dict[str, str] = None) -> None:
        """
        Create fact table from silver layer data
        
        Args:
            silver_table: Source silver table
            fact_name: Name of fact table
            fact_columns: Columns to include in fact table
            dimension_mappings: Mapping of dimension keys
        """
        try:
            silver_path = f"{self.lakehouse.silver_path}/{silver_table}"
            df = self.spark.read.format("delta").load(silver_path)
            
            # Select fact columns
            fact_df = df.select(fact_columns)
            
            # Join with dimensions to get surrogate keys
            if dimension_mappings:
                for dim_name, join_columns in dimension_mappings.items():
                    dim_path = f"{self.lakehouse.gold_path}/dimensions/{dim_name}"
                    dim_df = self.spark.read.format("delta").load(dim_path)
                    
                    # Join with dimension table
                    fact_df = fact_df.join(dim_df.select("dim_key", *join_columns), 
                                          on=join_columns, how="left") \
                                    .withColumnRenamed("dim_key", f"{dim_name}_key")
            
            # Add fact metadata
            fact_df = fact_df.withColumn("_created_timestamp", current_timestamp())
            
            # Write to gold layer
            gold_path = f"{self.lakehouse.gold_path}/facts/{fact_name}"
            fact_df.write.mode("overwrite").format("delta").save(gold_path)
            
            logger.info(f"Created fact table: {fact_name} with {fact_df.count()} records")
            
        except Exception as e:
            logger.error(f"Error creating fact table: {str(e)}")
            raise
    
    def create_aggregated_table(self, source_table: str, agg_name: str,
                               group_by_columns: List[str],
                               aggregations: Dict[str, str]) -> None:
        """
        Create aggregated table for business metrics
        
        Args:
            source_table: Source table (can be from any layer)
            agg_name: Name of aggregated table
            group_by_columns: Columns to group by
            aggregations: Dictionary of column -> aggregation function
        """
        try:
            # Determine source path
            if source_table.startswith("gold."):
                source_path = f"{self.lakehouse.gold_path}/{source_table.replace('gold.', '')}"
            elif source_table.startswith("silver."):
                source_path = f"{self.lakehouse.silver_path}/{source_table.replace('silver.', '')}"
            else:
                source_path = f"{self.lakehouse.bronze_path}/{source_table}"
            
            df = self.spark.read.format("delta").load(source_path)
            
            # Perform aggregations
            agg_expressions = []
            for col_name, agg_func in aggregations.items():
                if agg_func.upper() == "SUM":
                    agg_expressions.append(sum(col_name).alias(f"{col_name}_sum"))
                elif agg_func.upper() == "COUNT":
                    agg_expressions.append(count(col_name).alias(f"{col_name}_count"))
                elif agg_func.upper() == "AVG":
                    agg_expressions.append(avg(col_name).alias(f"{col_name}_avg"))
                elif agg_func.upper() == "MAX":
                    agg_expressions.append(max(col_name).alias(f"{col_name}_max"))
                elif agg_func.upper() == "MIN":
                    agg_expressions.append(min(col_name).alias(f"{col_name}_min"))
            
            agg_df = df.groupBy(group_by_columns).agg(*agg_expressions)
            
            # Add aggregation metadata
            agg_df = agg_df.withColumn("_aggregation_timestamp", current_timestamp())
            
            # Write to gold layer
            gold_path = f"{self.lakehouse.gold_path}/aggregations/{agg_name}"
            agg_df.write.mode("overwrite").format("delta").save(gold_path)
            
            logger.info(f"Created aggregated table: {agg_name} with {agg_df.count()} records")
            
        except Exception as e:
            logger.error(f"Error creating aggregated table: {str(e)}")
            raise

class DataQualityManager:
    """
    Data quality monitoring and validation
    """
    
    def __init__(self, lakehouse: DataLakehouse):
        self.lakehouse = lakehouse
        self.spark = lakehouse.spark
    
    def run_data_quality_checks(self, table_path: str, 
                               quality_rules: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run comprehensive data quality checks
        
        Args:
            table_path: Path to table to check
            quality_rules: Dictionary of quality rules to apply
            
        Returns:
            Dictionary containing quality check results
        """
        df = self.spark.read.format("delta").load(table_path)
        total_records = df.count()
        
        results = {
            "table_path": table_path,
            "total_records": total_records,
            "timestamp": datetime.now().isoformat(),
            "checks": {}
        }
        
        # Null checks
        if "null_checks" in quality_rules:
            for column in quality_rules["null_checks"]:
                null_count = df.filter(col(column).isNull()).count()
                null_percentage = (null_count / total_records) * 100 if total_records > 0 else 0
                results["checks"][f"{column}_null_check"] = {
                    "null_count": null_count,
                    "null_percentage": null_percentage
                }
        
        # Uniqueness checks
        if "uniqueness_checks" in quality_rules:
            for column in quality_rules["uniqueness_checks"]:
                unique_count = df.select(column).distinct().count()
                uniqueness_percentage = (unique_count / total_records) * 100 if total_records > 0 else 0
                results["checks"][f"{column}_uniqueness_check"] = {
                    "unique_count": unique_count,
                    "uniqueness_percentage": uniqueness_percentage
                }
        
        # Range checks
        if "range_checks" in quality_rules:
            for column, range_dict in quality_rules["range_checks"].items():
                if "min" in range_dict:
                    below_min = df.filter(col(column) < range_dict["min"]).count()
                    results["checks"][f"{column}_below_min"] = below_min
                if "max" in range_dict:
                    above_max = df.filter(col(column) > range_dict["max"]).count()
                    results["checks"][f"{column}_above_max"] = above_max
        
        return results

# Example usage and pipeline orchestration
class DataPipeline:
    """
    Main pipeline orchestrator
    """
    
    def __init__(self, config_path: str = None):
        self.lakehouse = DataLakehouse()
        self.bronze_layer = BronzeLayer(self.lakehouse)
        self.silver_layer = SilverLayer(self.lakehouse)
        self.gold_layer = GoldLayer(self.lakehouse)
        self.dq_manager = DataQualityManager(self.lakehouse)
        
        # Load configuration
        self.config = self._load_config(config_path) if config_path else {}
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load pipeline configuration from JSON file"""
        with open(config_path, 'r') as f:
            return json.load(f)
    
    def run_full_pipeline(self, source_data_path: str, table_name: str) -> None:
        """
        Run complete pipeline from bronze to gold
        """
        try:
            logger.info(f"Starting full pipeline for table: {table_name}")
            
            # Step 1: Ingest to Bronze
            self.bronze_layer.ingest_batch_data(
                source_path=source_data_path,
                table_name=table_name,
                file_format="parquet"
            )
            
            # Step 2: Process to Silver
            transformation_rules = {
                "column_casting": {
                    "amount": "decimal(10,2)",
                    "transaction_date": "date"
                },
                "derived_columns": {
                    "year": "year(transaction_date)",
                    "month": "month(transaction_date)"
                }
            }
            
            data_quality_rules = {
                "not_null_columns": ["customer_id", "transaction_date"],
                "value_ranges": {
                    "amount": {"min": 0, "max": 1000000}
                }
            }
            
            self.silver_layer.process_to_silver(
                bronze_table=table_name,
                silver_table=table_name,
                transformation_rules=transformation_rules,
                data_quality_rules=data_quality_rules
            )
            
            # Step 3: Create Gold layer artifacts
            # Create dimension table
            self.gold_layer.create_dimension_table(
                silver_table=table_name,
                dimension_name="customer_dim",
                dimension_columns=["customer_id", "customer_name", "customer_segment"]
            )
            
            # Create fact table
            self.gold_layer.create_fact_table(
                silver_table=table_name,
                fact_name="transaction_fact",
                fact_columns=["transaction_id", "customer_id", "amount", "transaction_date"],
                dimension_mappings={"customer_dim": ["customer_id"]}
            )
            
            # Create aggregations
            self.gold_layer.create_aggregated_table(
                source_table=f"silver.{table_name}",
                agg_name="monthly_customer_summary",
                group_by_columns=["customer_id", "year", "month"],
                aggregations={
                    "amount": "sum",
                    "transaction_id": "count"
                }
            )
            
            # Step 4: Run data quality checks
            silver_path = f"{self.lakehouse.silver_path}/{table_name}"
            quality_rules = {
                "null_checks": ["customer_id", "transaction_date"],
                "uniqueness_checks": ["transaction_id"],
                "range_checks": {
                    "amount": {"min": 0, "max": 1000000}
                }
            }
            
            dq_results = self.dq_manager.run_data_quality_checks(silver_path, quality_rules)
            logger.info(f"Data quality results: {dq_results}")
            
            logger.info(f"Pipeline completed successfully for table: {table_name}")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise
        finally:
            self.lakehouse.close()

# Example usage
if __name__ == "__main__":
    # Initialize pipeline
    pipeline = DataPipeline()
    
    # Run full pipeline
    pipeline.run_full_pipeline(
        source_data_path=r"C:\Users\ghoniem\Downloads\local (2).xlsx",
        table_name="customer_transactions"
    )
    
    # Example of streaming ingestion
    # streaming_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "transactions").load()
    # bronze_layer = BronzeLayer(lakehouse)
    # bronze_layer.ingest_streaming_data(streaming_df, "streaming_transactions")