from typing import Dict


config: Dict[str, str]= {
    "spark.sql.catalogImplementation": "hive",
    "spark.sql.warehouse.dir": "storage",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.localShuffleReader.enabled": "true",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
    "spark.sql.cbo.enabled": "true",
    "spark.sql.statistics.histogram.enabled": "true",
    "spark.sql.sources.bucketing.enabled": "true",
    "spark.sql.bucketing.coalesceBucketsInJoin.enabled": "true",
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.sql.optimizer.dynamicPartitionPruning.enabled": "true",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
}