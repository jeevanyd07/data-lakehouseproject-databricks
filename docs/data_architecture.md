create a data_architecture 
This data Architecture for this project follows Medallion Architecture Bronze,Silver,and Gold Layers
Source is ERP : xlsx file
Bronze Layer : Row Date, Object Type - Table, Load - Bactch Processing Full Load(Truncate & insert), No Transformation, Model - None.
Silver Layer : Clean Standard Data, Object Type - Table, Load - Bactch Processing Full Load(Truncate & insert), Transformation - Data Cleaning, Data Normalization, Data Standardization, Derived Columns, Data Enrichnment , Data Model - None (As-Is)
Gold Layer : Buisness Ready Data, 

                ┌──────────────────────────┐
                │        SOURCE            │
                │        ERP System        │
                │        (XLSX Files)      │
                └────────────┬─────────────┘
                             │
                             ▼
                ┌──────────────────────────┐
                │      BRONZE LAYER        │
                │   (Raw Data Layer)       │
                │                          │
                │ • Object: Table          │
                │ • Load: Batch (Full)     │
                │   (Truncate & Insert)    │
                │ • Data: Raw Data         │
                │ • Transformation: None   │
                │ • Model: None (As-Is)    │
                └────────────┬─────────────┘
                             │
                             ▼
                ┌──────────────────────────┐
                │      SILVER LAYER        │
                │ (Clean & Standard Data)  │
                │                          │
                │ • Object: Table          │
                │ • Load: Batch (Full)     │
                │   (Truncate & Insert)    │
                │ • Transformations:       │
                │   - Data Cleaning        │
                │   - Normalization        │
                │   - Standardization      │
                │   - Derived Columns      │
                │   - Data Enrichment      │
                │ • Model: None (As-Is)    │
                └────────────┬─────────────┘
                             │
                             ▼
                ┌──────────────────────────┐
                │       GOLD LAYER         │
                │  (Business Ready Data)   │
                │                          │
                │ • Object: View           │
                │ • Transformations:       │
                │   - Data Integration     │
                │   - Aggregations         │
                │   - Business Logic       │
                │ • Data Model:            │
                │   - Star Schema          │
                │   - Fact Tables          │
                │   - Aggregated Tables    │
                └────────────┬─────────────┘
                             │
                             ▼
                ┌──────────────────────────┐
                │      REPORTING / BI      │
                │ (Dashboards & Reports)   │
                └──────────────────────────┘
