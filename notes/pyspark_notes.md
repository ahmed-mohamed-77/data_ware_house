# Py spark Overview

**Can apply pandas function through pandas [API]**
**Map in Pandas and arrow through Iterator have to use for loop**

## User-defined scalar functions - Python

- UDF function must be defined in the py-spark before using it

### 🧠 Core Difference: Execution Model

| Feature               | Regular PySpark UDF                  | pandas UDF (vectorized)             |
|-----------------------|--------------------------------------|-------------------------------------|
| **Execution**         | Row-by-row (scalar)                  | Vectorized (batch with pandas)      |
| **Performance**       | 🐢 Slower (Python-JVM overhead)      | 🚀 Faster (Arrow-optimized)         |
| **Use pandas syntax** | ❌ No                                | ✅ Yes                               |
| **Arrow required**    | ❌ No                                | ✅ Yes (Apache Arrow)                |
| **Best for**          | Simple logic, low volume             | Batch transformations, heavy logic  |

## 🚀 The Amazing Data Pipeline

A modern data pipeline efficiently transports data from raw sources to refined insights, enabling real-time and batch-driven analytics, AI, and business intelligence.

---

## 🗂️ 1. Data Sources

These are the origins of data, which can be structured, semi-structured, or unstructured:

- **Kafka**: Message broker for real-time streaming.
- **API**: Pulls data from external services.
- **Databases**: Relational/NoSQL systems.
- **Files**: CSV, JSON, logs, etc.
- **Hadoop**: Distributed storage and processing system.

---

## 🔄 2. Ingestion Layer (Data Collector)

Collects raw data from sources in various modes:

- **Streaming**: Continuous real-time data flow.
- **CDC (Change Data Capture)**: Captures incremental changes in source databases.
- **Batch**: Periodic or scheduled bulk data loads.

➡️ All ingested data is stored in:

### 🧊 Raw Storage Zone

- Temporary staging area (e.g., S3, HDFS).
- Used for backup, traceability, and reprocessing.

---

## 🔧 3. ETL Transformer

Processes and transforms raw data:

- **Extract** from source systems.
- **Transform** for structure, quality, and meaning.
- **Load** into curated or conformed data stores.

---

## 🧪 4. Data Storage Zones

### Curated Zone: Data Lakes & Spark Platforms

- Stores semi-structured or unstructured data.
- Optimized for flexibility and scalability.
- 🔁 Supports:
  - 🧠 **Data Science**
  - 🤖 **AI/ML**

### Conformed Zone: Data Warehouses

- Stores cleaned, normalized, and structured data.
- Follows business schema and governance.
- 🔁 Supports:
  - 📊 **Business Intelligence**
  - 📈 **Analytics**

---

## 🎯 5. Data Consumers

| Consumer            | Purpose                                       |
|---------------------|-----------------------------------------------|
| 🔬 Data Science      | Exploratory analysis, statistical modeling    |
| 🤖 AI/ML             | Training, evaluation, deployment of ML models |
| 📊 Business Intelligence | Dashboards, metrics, and KPIs          |
| 📈 Analytics          | Insights, trends, and forecasting             |

---

## 📌 Summary Diagram (ASCII View)

```text
[Sources] 
   ↓
[Ingest (Streaming, CDC, Batch)] 
   ↓
[Raw Storage: Object Stores / Landing Zones] 
   ↓
[ETL Transformer]
   ↓
 ┌───────────────┐              ┌─────────────┐
 │ Curated Zone  │────────────► │ Data Science│
 │ (Data Lakes)  │────────────► │ AI/ML       │
 └───────────────┘              └─────────────┘
   ↓
 ┌────────────────┐             ┌──────────────┐
 │ Conformed Zone │───────────► │ BI Dashboards│
 │ (Data Warehouse)───────────► │ Analytics    │
 └────────────────┘             └──────────────┘
```

---
