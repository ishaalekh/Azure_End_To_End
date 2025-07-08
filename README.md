# End-to-End Azure Data Engineering Project

## Project Overview
This project showcases a complete data engineering pipeline built using Azure services with the Adventure Works dataset (Dataset - <a href = 'https://github.com/anshlambaoldgit/Adventure-Works-Data-Engineering-Project/tree/main/Data'>Link</a>). It covers everything from data ingestion to reporting and visualization, focusing on **scalability**, **security**, and **best practices** in modern data architecture.

By building this project, I explored how cloud-native tools can be orchestrated for real-world data engineering tasks, including advanced data transformations and analytics.


## 🔗 Project Workflow & Architecture

### 1️⃣ Data Ingestion (Bronze Layer)
- Used **Azure Data Factory (ADF)** to pull data via **HTTP linked service** from <a href = 'https://github.com/anshlambaoldgit/Adventure-Works-Data-Engineering-Project/tree/main/Data'>Link</a>.
- The pipeline was fully **parameterized** for flexibility and reusability.
- Raw data was stored in **Azure Data Lake Storage Gen2** under the *bronze* layer.

### 2️⃣ Data Transformation (Silver Layer)
- Performed authentication via **Azure Entra ID (App Registration)** to securely access storage from **Azure Databricks**.
- Processed data using **PySpark** for tasks such as:
  - Column splitting & merging.
  - Data type conversions.
  - Date/time transformations.
  - Regex-based cleaning for column standardization.
  - Derived/Calculated columns for advanced analysis.

Aggregations for sales trend analysis.
- Conducted initial analytics, including sales trends and customer aggregations.
- Saved clean, optimized data in **Parquet format** to the *silver* layer.

### 3️⃣ Data Serving & Modeling (Gold Layer)
- Set up **Azure Synapse Analytics (Serverless SQL Pools)** for querying.
- Assigned **Blob Contributor roles** to Synapse and my account via ADLS Gen2 Access Control.
- Created:
  - Master Key for secure credentials.
  - External Data Source & File Format.
  - Views for each Silver layer table.
  - External table for Sales data to enable direct querying.

### 4️⃣ Visualization & Insights
- Connected **Power BI** to Synapse SQL Endpoint for reporting.
- Built two key visualizations:
  - **Total Order Count** – provides insight into overall order volumes.
  - **Year-over-Year Sales Comparison** – helps spot sales trends and seasonal patterns.


## 🔥 Key Insights & Takeaways
- Total orders reached 56.05K across 2015–2017, showing strong business volume.
- Orders grew rapidly from 2015 to 2016, with nearly a 5x increase.
- Growth continued into 2017 but at a slower rate, indicating possible market saturation or stabilization.
- The upward trend reflects successful scaling; further analysis is needed to sustain growth momentum.


## ⚙️ Tools & Technologies Used
- **Azure Data Factory (ADF)** — Orchestration & Ingestion
- **Azure Data Lake Gen2 (ADLS)** — Storage Layers (Bronze/Silver)
- **Azure Databricks (PySpark)** — Data Cleansing & Transformation
- **Azure Synapse Analytics (Serverless SQL)** — Data Serving & Modeling
- **Power BI** — Dashboarding & Visualization
- **Azure Entra ID (App Registration)** — Secure OAuth Authentication

