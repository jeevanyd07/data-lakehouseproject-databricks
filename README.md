# Data Lakehouseproject Databricks


Welcome to the **Databricks Data Lakehouse Project** by **Jeevan Y D**.

This repository contains a complete, real-world **Data Lakehouse implementation** built on Databricks, including datasets, notebooks, SQL examples, and exercises. Everything here is designed to help you understand how modern data teams use Databricks in practice, from data ingestion and transformation to analytics-ready data products.


---

# 📊 Data Architecture – Medallion Architecture
### 🔹 Overview
his project follows the Medallion Architecture pattern using three layers: Bronze, Silver, and Gold. The goal is to progressively refine raw data into business-ready insights.

The data source for this architecture is ERP system files (XLSX format).

## 🔹 Architecture Layers
### 🥉 Bronze Layer (Raw Data Layer)
he Bronze layer is responsible for ingesting raw data from the ERP system without any transformations.

- Source: ERP (XLSX files)
- Data Type: Raw data (as-is)
- Object Type: Tables
- Load Type: Batch Processing
- Load Strategy: Full Load (Truncate & Insert)
- Transformations: None
- Data Model: None (Raw ingestion)
### 👉 Purpose:
- Store original data for traceability
- Act as a backup/reference layer

### 🥈 Silver Layer (Cleaned & Standardized Layer)
The Silver layer transforms raw data into clean, structured, and consistent datasets.

- Input: Bronze Layer tables
- Object Type: Tables
- Load Type: Batch Processing
- Load Strategy: Full Load (Truncate & Insert)
- Transformations Applied:
-                       Data Cleaning (handling nulls, duplicates)
-                       Data Normalization
-                       Data Standardization (formats, naming conventions)
-                       Derived Columns (calculated fields)
-                       Data Enrichment (adding business context)
- Data Model: None (As-Is structured tables)


---

## 🛠️ Technologies Used

- Databricks  
- Apache Spark  
- PySpark  
- Spark SQL  
- Delta Lake  
- Unity Catalog  


---


## 📁 Repository Structure

```text
📦 project-root
│
├── 📂 datasets
│   └── 📂 erp_files
│       ├── Dealer_Invoice_Report.xlsx
│       ├── Dealer_Performance_Report.xlsx
│       ├── Stock_Report.xlsx
│       └── Wholesale_Report.xlsx
│
├── 📂 docs
│   ├── data_architecture.md
│   └── data_catalogue.md
│
├── 📂 scripts
│   ├── 📂 bronze
│   │   ├── bronze_basic.py
│   │   └── bronze_advanced.py
│   │
│   ├── 📂 silver
│   │   └── silver.py
│   │
│   ├── 📂 gold
│   │   └── gold.sql
│   │
│   └── 📂 report
│       └── report.py
│
├── 📂 tests
│   ├── silver_test.sql
│   └── gold_test.sql
│
├── LICENSE
└── README.md
```

---


## ⚖️ License

This project is licensed under the [MIT License].(LICENSE). You are free to use, modify, and share this project with proper attribution.


---


## 👨‍💻 About Me

Hi there! I'm **Jeevan Y D** SQL Developer, Data Analyst & Data Engineer 
Linked IN - https://www.linkedin.com/in/-jeevanyd/
