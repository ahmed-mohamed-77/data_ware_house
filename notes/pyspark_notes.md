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
