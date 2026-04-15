# Data Lakehouseproject Databricks


Welcome to the **Databricks Data Lakehouse Project** by **Jeevan Y D**.

This repository contains a complete, real-world **Data Lakehouse implementation** built on Databricks, including datasets, notebooks, SQL examples, and exercises. Everything here is designed to help you understand how modern data teams use Databricks in practice, from data ingestion and transformation to analytics-ready data products.


---


## 🏗️ Architecture

This project follows the **Medallion Architecture**:

### 🥉 Bronze Layer
- Raw data ingestion  
- Schema inference and storage as Delta tables  

### 🥈 Silver Layer
- Data cleaning and standardization  
- Type casting and validation  

### 🥇 Gold Layer
- Dimensional Data Model (Business Transformation)
- Ready for BI and analysis  

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
