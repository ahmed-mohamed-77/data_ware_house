# Data Lake House Architecture

## ⚙️ Spark Configuration for Data Lakehouse

### 📁 Catalog & Warehouse

```json
{
  "spark.sql.catalogImplementation": "hive",
  "spark.sql.warehouse.dir": "/lakehouse/warehouse"
}
```

| Key | Description |
|-----|-------------|
| `spark.sql.catalogImplementation` | Enables Hive support for schema metadata management. |
| `spark.sql.warehouse.dir` | Directory for storing managed tables in the lakehouse. |

---

### 🚀 Query Optimization (AQE & CBO)

```json
{
  "spark.sql.adaptive.enabled": "true",
  "spark.sql.adaptive.coalescePartitions.enabled": "true",
  "spark.sql.adaptive.localShuffleReader.enabled": "true",
  "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
  "spark.sql.cbo.enabled": "true",
  "spark.sql.statistics.histogram.enabled": "true"
}
```

| Key | Description |
|-----|-------------|
| `spark.sql.adaptive.enabled` | Turns on Adaptive Query Execution for dynamic plan optimization. |
| `spark.sql.adaptive.coalescePartitions.enabled` | Merges small shuffle partitions at runtime. |
| `spark.sql.adaptive.localShuffleReader.enabled` | Improves performance by reading shuffle data locally. |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | Handles skewed partitions larger than 256MB. |
| `spark.sql.cbo.enabled` | Enables cost-based optimization based on statistics. |
| `spark.sql.statistics.histogram.enabled` | Collects histograms for better cardinality estimates. |

---

### 🧱 Partitioning & Bucketing

```json
{
  "spark.sql.sources.bucketing.enabled": "true",
  "spark.sql.bucketing.coalesceBucketsInJoin.enabled": "true",
  "spark.sql.sources.partitionOverwriteMode": "dynamic",
  "spark.sql.optimizer.dynamicPartitionPruning.enabled": "true"
}
```

| Key | Description |
|-----|-------------|
| `spark.sql.sources.bucketing.enabled` | Enables bucketing to improve join performance. |
| `spark.sql.bucketing.coalesceBucketsInJoin.enabled` | Allows joining tables with different bucket counts. |
| `spark.sql.sources.partitionOverwriteMode` | Dynamically overwrites only affected partitions. |
| `spark.sql.optimizer.dynamicPartitionPruning.enabled` | Improves partition filtering during join execution. |

---

### 💾 Parquet Format Settings

**Handle This File ext if exist [OPTIONAL]**

```json
{
  "spark.sql.parquet.compression.codec": "snappy",
  "spark.sql.parquet.filterPushdown": "true",
  "spark.sql.parquet.mergeSchema": "false",
  "spark.sql.parquet.enableVectorizedReader": "true"
}
```

| Key | Description |
|-----|-------------|
| `spark.sql.parquet.compression.codec` | Uses Snappy compression for speed and efficiency. |
| `spark.sql.parquet.filterPushdown` | Applies filters early for performance gains. |
| `spark.sql.parquet.mergeSchema` | Prevents costly schema merging. |
| `spark.sql.parquet.enableVectorizedReader` | Improves performance via columnar reading. |

---

### 🔁 Shuffle & Execution Tuning

```json
{
  "spark.sql.shuffle.partitions": "200"
}
```

| Key | Description |
|-----|-------------|
| `spark.sql.shuffle.partitions` | Sets the number of partitions after a shuffle (tune based on workload and cluster size). |

---

### 🌊 Delta Lake Support (If Used)

```json
{
  "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
  "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
}
```

| Key | Description |
|-----|-------------|
| `spark.sql.extensions` | Registers Delta Lake SQL extension. |
| `spark.sql.catalog.spark_catalog` | Uses Delta-compatible catalog for table operations. |

---

### ✅ Full Example Configuration

```json
{
  "spark.sql.catalogImplementation": "hive",
  "spark.sql.warehouse.dir": "/lakehouse/warehouse",

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

  "spark.sql.parquet.compression.codec": "snappy",
  "spark.sql.parquet.filterPushdown": "true",
  "spark.sql.parquet.mergeSchema": "false",
  "spark.sql.parquet.enableVectorizedReader": "true",

  "spark.sql.shuffle.partitions": "200",

  "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
  "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
}
```

| Question | Answer |
|----------|--------|
| Is Delta similar to PySpark? | ❌ No, Delta is a storage layer, PySpark is a computation engine API.|
| Do I need Delta for a Data Lakehouse? |✅ Highly recommended — Delta makes your data lake reliable and structured like a warehouse.|
