# 🚕 NYC Taxi Data Engineering Pipeline on Azure
 
An end-to-end data engineering project that ingests NYC Taxi trip data, processes it through a **Medallion Architecture (Bronze → Silver → Gold)** using **Azure Data Factory**, **Azure Data Lake Storage Gen2** and **Azure Databricks**, and serves it to a **Power BI** dashboard.
 
![Azure](https://img.shields.io/badge/Azure-Data%20Factory-0078D4?logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Azure-Databricks-FF3621?logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta-Lake-00ADD8)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?logo=apachespark&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?logo=powerbi&logoColor=black)
 
---
 
## 📌 Table of Contents
- [Project Overview](#-project-overview)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Data Source](#-data-source)
- [Pipeline Walkthrough](#-pipeline-walkthrough)
- [Dashboard](#-dashboard)
- [Key Learnings](#-key-learnings)
- [Future Improvements](#-future-improvements)
- [Author](#-author)
---
 
## 🎯 Project Overview
 
NYC publishes taxi trip records as monthly Parquet files. Downloading and processing them by hand is slow and error-prone, so this project automates the whole flow:
 
1. **Ingest** all 12 monthly files automatically using a parameterized ADF pipeline.
2. **Store** the raw data in the Bronze layer of Azure Data Lake.
3. **Transform & clean** the data with PySpark in Azure Databricks.
4. **Save** the cleaned data as Delta tables in the Silver layer.
5. **Publish** business-ready Delta tables to the Gold layer and register them in a `gold` database in Databricks.
6. **Visualize** insights in Power BI.

**Problem solved:** instead of hard-coding 12 different URLs and copy activities, a single dynamic pipeline loops over months and builds each URL on the fly.

---
 
## 🏗 Architecture
 <p>
  <img src="https://res.cloudinary.com/dh5wlux0q/image/upload/v1792779269/pipeline_architecture_b1yauv.png" width="520" alt="ADF Sink"/>
 </p>
  
```
NYC TLC Website (Parquet, monthly)
        │
        ▼
Azure Data Factory  (ForEach + dynamic URL + Copy Activity)
        │
        ▼
Azure Data Lake Gen2 ── Bronze  (raw Parquet)
        │
        ▼
Azure Databricks  (PySpark: clean + transform)
        │
        ▼
Azure Data Lake Gen2 ── Silver  (Delta tables, 3 datasets)
        │
        ▼
Azure Databricks  (PySpark: read Silver, write Gold)
        │
        ▼
Azure Data Lake Gen2 ── Gold  (Delta tables, registered in `gold` database)
        │
        ▼
Power BI  (via Databricks connector)
```
 
---
 
## 🛠 Tech Stack
 
| Layer | Tool |
|---|---|
| Orchestration / Ingestion | Azure Data Factory (ADF) |
| Storage | Azure Data Lake Storage Gen2 |
| Processing | Azure Databricks, PySpark |
| Table Format | Delta Lake |
| Governance / Access | Databricks Credential + External Location |
| Visualization | Power BI |
| Source Format | Parquet |
 
---
 
## 📂 Data Source
 
- **Dataset:** NYC Taxi & Limousine Commission (TLC) Trip Record Data
- **Website:** [nyc.gov – TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- **Format:** Parquet, one file per month
---
 
## 🔄 Pipeline Walkthrough
 
### 1️⃣ Ingestion with Azure Data Factory (Bronze Layer)

## Pipeline Screenshots
<p align="center">
  <img src="https://res.cloudinary.com/dh5wlux0q/image/upload/v1792778934/Screenshot_2026-10-20_121724_e756rx.png" width="420" alt="ADF Pipeline" />
  <img src="https://res.cloudinary.com/dh5wlux0q/image/upload/v1792778934/Screenshot_2026-10-22_215930_bxbkkh.png" width="420" alt="ADF Source" />
  
</p>
<p>
  <img src="https://res.cloudinary.com/dh5wlux0q/image/upload/v1792778932/Screenshot_2026-10-20_121745_g5mwtd.png" width="420" alt="ADF Sink" />
  <img src="https://res.cloudinary.com/dh5wlux0q/image/upload/v1792778931/Screenshot_2026-10-23_034023_szw6uu.png" width="420" alt="ADF Output" />
</p>

 
- Used a **ForEach** activity with the `range()` function to generate month values **1 to 12**.
- Inside the loop, an **If-Else condition** handles the month format so the URL changes **dynamically** for each month (for example `01`, `02` … `12`).
- A **Copy activity** downloads each monthly Parquet file from the NYC website and stores it in the **Bronze** container of Azure Data Lake.
> ✅ One pipeline, 12 files, zero manual work.
 
### 2️⃣ Connecting Databricks to Data Lake
 
To securely connect Databricks to the Data Lake (no access keys or secrets in notebooks), I used an **Access Connector for Azure Databricks** with a managed identity:
 
1. **Created an Access Connector for Azure Databricks** in the Azure portal.
2. **Assigned the role** `Storage Blob Data Contributor` to the connector on the storage account, via **Access Control (IAM)**, so it can read from Bronze and write to Silver.
3. **Created a Storage Credential** in Databricks using the Access Connector's ID.
4. **Created an External Location** in Databricks that points to the Data Lake path, using the storage credential above.
> ✅ Result: Databricks reads and writes Data Lake files through a managed identity with least-privilege access, with no hard-coded keys.
 
### 3️⃣ Transformation with PySpark (Silver Layer)
 
- Read the Bronze Parquet files into a Spark DataFrame.
- Applied cleaning and transformation steps (handling nulls, fixing data types, filtering invalid records, adding derived columns).
- Saved the cleaned data as a **Parquet** in the Silver layer:
```python
(
    df.write
      .format("parquet")
      .mode("append")
      .option("path", "abfss://silver@nycstorage.dfs.core.windows.net/trips2023data")
      .saveAsTable("gold.trips2023data")
)
```

 
### 4️⃣ Gold Layer (Delta Tables + `gold` Database)
 
- Read all three datasets from the Silver layer into Spark DataFrames.
- Created a `gold` database (schema) in Azure Databricks.
- Wrote each dataset to the **Gold container** of Azure Data Lake in **Delta format** and registered it as a table in the `gold` database:
```python
(
    df_trip_zone.write
      .format("delta")
      .mode("append")
      .option("path", f"{gold_path}/trip_zone")
      .saveAsTable("gold.trip_zone")
)
```
 
> ℹ️ Because a `path` is provided, these are **external tables**: the data files live in the Gold container of the Data Lake, while the `gold` database only stores the table metadata pointing to that location. This means the data stays in the lake even if a table is dropped, and can be queried with SQL from Databricks.


### 5️⃣ Reporting with Power BI
 
- Connected Power BI to Azure Databricks using the **Partner Connect**.
- Loaded the Gold Delta tables and built an interactive dashboard.
---
 
## 📊 Dashboard
 
<p>
  <img src="https://res.cloudinary.com/dh5wlux0q/image/upload/v1792778930/Screenshot_2026-10-23_184149_ekhuhx.png" width="420" alt="ADF Sink" />
  <img src="https://res.cloudinary.com/dh5wlux0q/image/upload/v1792779715/Screenshot_2026-10-23_183752_g78dfi.png" width="420" alt="ADF Output" />
</p>
  
---
 
## 💡 Key Learnings
 
- Building **parameterized, dynamic pipelines** in ADF with ForEach and expressions.
- Implementing the **Medallion Architecture** (Bronze → Silver → Gold).
- Understanding **external vs managed tables** in Databricks (data in the lake vs. metadata in the metastore).
- Securely connecting Databricks and ADLS using **Credentials and External Locations**.
- Writing and managing **Delta tables** with PySpark.
- Connecting Power BI directly to Databricks for reporting.
---
 
## 🚀 Future Improvements
 
- Build **aggregated, business-ready** Gold tables (joins and KPIs) on top of the current Gold layer.
- Add **incremental loading** so only new months are processed.
- Replace `append` with **upserts using Delta Lake `MERGE`**, so new records are inserted and existing records are updated (no duplicates on re-runs).
- Schedule the pipeline with **ADF triggers**.
- Add **email alerts** on pipeline failure (Logic Apps).
- Add data quality checks before writing to Silver.
---
 
 
## 👤 Author
 
**Priyesh Kumar** </br>
BCA Student | Aspiring Data Engineer
 
- GitHub: [@Priyesh7998](https://github.com/priyesh7998)
- LinkedIn: [@Priyesh Kumar](https://linkedin/com/in/priyesh7998) </br>

⭐ If you found this project useful, consider giving it a star!
