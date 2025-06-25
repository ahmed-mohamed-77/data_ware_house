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

## Why the Data engineers so important in this process?

Data engineers are the architects behind those pipelines. they don't just build pipeline - they makes us reliable, efficient and scalable, beyond pipeline developments data engineers

- **Optimize data storage** to keep cost low and performance high
- **Ensure data quality and integrity** addressing duplicates inconsistent and missing value
- **Implement governance** for secure, compliant and manged data
- **- Adapt data Architectures** to meet the changing needs of the organization - IDK what's it so skip it

Ultimately, their role is to strategically mange the entire **data lifecycle** from collection to consumption

## 💥 **What happens if you ignore best practices?**

Many engineers focus on quick wins and ignore best practices—until they hit a wall:

❌ You spend more time fixing than building.

❌ Data breaks unexpectedly, leading to trust issues.

❌ Your team avoids your pipelines because they’re unreliable.

❌ Scaling is painful, requiring constant refactoring.

Senior engineers don’t wait for these problems to happen. They design ingestion pipelines that work now, and in the future.

---

## **What is data ingestion?**  

Data ingestion is the process of **extracting** data from a source, transporting it to a suitable environment, and preparing it for use. This often includes **normalizing**, **cleaning**, and **adding metadata**.  

---

### **“A wild dataset Magically appears!”**  

In many data science teams, data seems to appear out of nowhere — because an engineer loads it.  

- **Well-structured data** (with an explicit schema) can be used immediately.  
  - Examples: Parquet, Avro, or database tables where data types and structures are predefined.  
- **Unstructured or weakly typed data** (without a defined schema) often needs cleaning and formatting first.  
  - Examples: CSV, JSON, where fields might be inconsistent, nested or missing key details.

---

💡 **What is a schema?**  
A schema defines the expected format and structure of data, including field names, data types, and relationships.  

---

### **Be the Magician! 😎**  

Since you're here to learn data engineering, **you** will be the one making datasets magically appear!  

To build effective pipelines, you need to master:  

✅ **Extracting** data from various sources (APIs, databases, files).  
✅ **Normalization** data by transforming, cleaning, and defining schemas.  
✅ **Loading** data where it can be used (data warehouse, lake, or database).

---

## Extracting data

### Data Streaming vs. Batching

When extracting data, you need to decide how to process it:

- **Batching**: Processing data in chunks at scheduled intervals.
- **Streaming**: Processing data continuously as it arrives.

---

### How should the data be processed?

| Batching | Streaming |
|----------|-----------|
| Suitable for scheduled tasks and reduces system load. | Ideal for real-time data processing and immediate insights. |

Choosing the right approach depends on factors like **data volume, latency requirements, and system architecture**.

In this course, **we will focus primarily on batch processing**, as it is the most common approach in data engineering. While streaming data is important in real-time applications, most data pipelines, ETL processes, and business intelligence workflows rely on batch extraction and transformation. However, we will briefly cover streaming concepts to understand their role in modern data architectures.

#### **1. Batch processing**  

Batch processing is best when you can wait for data to accumulate before processing it in large chunks. It is **cost-efficient** and works well for non-time-sensitive workloads.

📌 **Common use cases**  

- Nightly database updates  
- Generating daily or weekly reports  
- Ingesting large files from an FTP server  

---

#### **2. Streaming data processing**  

Streaming is useful when you need to **process data in real-time** or **with minimal delay**. Instead of waiting for a batch, events are processed continuously.  

📌 **Common use cases**  

- Fraud detection (e.g., analyzing transactions in real-time)  
- IoT device monitoring (e.g., temperature sensors)  
- Event-driven applications (e.g., user activity tracking)  
- Log and telemetry data ingestion  

---

#### **3. When to use Batch vs. Streaming**  

| **Factor**        | **Batch processing**  | **Streaming processing** |
|------------------|------------------|-------------------|
| **Latency**      | High (minutes, hours) | Low (milliseconds, seconds) |
| **Data volume**  | Large batches | Continuous small events |
| **Use case**     | Reports, ETL, backups | Real-time analytics, event-driven apps |
| **Complexity**   | Easier to manage | Requires event-driven architecture |
| **Cost**         | Lower for periodic runs | Higher for always-on processing |

---

#### **4. Tools**  

Many tools support both **batch** and **streaming** data extraction. Some tools are optimized for one approach, while others provide flexibility for both.

---

**Message queues & Event streaming**  
These tools enable real-time data ingestion and processing but can also buffer data for mini-batch processing.  

- **Apache Kafka** – Distributed event streaming platform for real-time and batch workloads.  
- **RabbitMQ** – Message broker that supports real-time message passing.  
- **AWS Kinesis** – Cloud-native alternative to Kafka for real-time ingestion.  
- **Google Pub/Sub** – Managed messaging service for real-time and batch workloads.  

---

**ETL & ELT Pipelines**  
These tools handle extraction, transformation, and loading (ETL) for both batch and streaming pipelines.  

- **Apache Spark** – Supports batch processing and structured streaming.  
- **dbt (Data Build Tool)** – Focuses on batch transformations but can be used with streaming inputs.  
- **Flink** – Real-time stream processing but can also handle mini-batch workloads.  
- **NiFi** – A data flow tool for moving and transforming data in real time or batch.  
- **AWS Glue** – Serverless ETL service for batch workloads, with limited streaming support.  
- **Google Cloud Dataflow** – Managed ETL platform supporting both batch and streaming.  
- **dlt** – Automates API extraction, incremental ingestion, and schema evolution for both batch and streaming pipelines.  

---

### **APIs as a data source: Batch vs. Streaming approaches**  

APIs are a major source of data ingestion. Depending on how APIs provide data, they can be used in both **batch** and **streaming** workflows.  

#### **1. APIs for batch extraction**  

Some APIs return large datasets at once. This data is often fetched on a schedule or as part of an ETL process.  

**Common batch API examples:**  

- **CRM APIs (Salesforce, HubSpot)** – Export customer data daily.  
- **E-commerce APIs (Shopify, Amazon)** – Download product catalogs or sales reports periodically.  
- **Public APIs (Weather, Financial Data)** – Retrieve daily stock market updates.  

**How batch API extraction works:**  

1. Call an API at **scheduled intervals** (e.g., every hour or day).  
2. Retrieve all available data (e.g., last 24 hours of records).  
3. Store results in a database, data warehouse, or file storage.  

```python
import requests
import json

def fetch_batch_data():
    url = "https://api.example.com/daily_reports"
    response = requests.get(url)
    data = response.json()
    
    with open("daily_report.json", "w") as file:
        json.dump(data, file)

fetch_batch_data()
```

---

#### **2. APIs for streaming data extraction**  

Some APIs support **event-driven** data extraction, where updates are pushed in real-time. This method is used for systems that require immediate action on new data.  

**Common streaming API examples:**  

- **Webhooks (Stripe, GitHub, Slack)** – Real-time event notifications.  
- **Social Media APIs (Twitter Streaming, Reddit Firehose)** – Continuous data from user interactions.  
- **Financial Market APIs (Binance WebSocket, AlphaVantage Streaming)** – Live stock prices and cryptocurrency trades.  

**How streaming API extraction works:**  

1. API sends **real-time updates** as data changes.  
2. A webhook or WebSocket **listens for events**.  
3. Data is **processed immediately** instead of being stored in bulk.  

```python
import websocket

def on_message(ws, message):
    print("Received event:", message)

ws = websocket.WebSocketApp("wss://api.example.com/stream", on_message=on_message)
ws.run_forever()
```

As an engineer, you will need to build pipelines that “just work”.

So here’s what you need to consider on extraction, to prevent the pipelines from breaking, and to keep them running smoothly:  

1. **Hardware limits**: Be mindful of memory (RAM) and storage (disk space). Overloading these can crash your system.  
2. **Network reliability**: Networks can fail! Always account for retries to make your pipelines more robust.  
   - Tip: Use libraries like `dlt` that have built-in retry mechanisms.  
3. **API rate limits**: APIs often restrict the number of requests you can make in a given time.  
   - Tip: Check the API documentation to understand its limits (e.g., [Zendesk](https://developer.zendesk.com/api-reference/introduction/rate-limits/), [Shopify](https://shopify.dev/docs/api/usage/rate-limits)).  

There are even more challenges to consider when working with APIs — such as **pagination and authentication**. Let’s explore how to handle these effectively when working with **REST APIs**.

### **Working with REST APIs**

REST APIs (Representational State Transfer APIs) are one of the most common ways to extract data. They allow you to retrieve structured data using simple HTTP requests (**GET**, POST, PUT, DELETE). However, working with APIs comes with its own challenges.

There is no common way how to design an API, so each of them is unique in a way.

Let's try to request GitHub API and get events from the `DataTalksClub/data-engineering-zoomcamp` repository.

DataTalks.Club GitHub: https://github.com/DataTalksClub

#### **1. Rate limits**  

Many APIs **limit the number of requests** you can make within a certain time frame to prevent overloading their servers. If you exceed this limit, the API may **reject your requests** temporarily or even block you for a period.  

To avoid hitting these limits, we can:

- **Monitor API rate limits** – Some APIs provide headers that tell you how many requests you have left.
- **Pause requests when needed** – If we're close to the limit, we wait before making more requests.  
- **Implement automatic retries** – If a request fails due to rate limiting, we can wait and retry after some time.  

💡Some APIs provide a **retry-after** header, which tells you how long to wait before making another request. Always check the API documentation for best practices!
