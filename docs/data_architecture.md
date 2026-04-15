## 📊 Data Architecture
```
    [ SOURCES ]                                      [ DATA WAREHOUSE ]                                     [ CONSUME ]



 ┌────────────┐                     ┌──────────────┐   ┌──────────────┐   ┌──────────────┐                ┌──────────────┐
 │   ERP      │                     │   BRONZE     │   │    SILVER    │   │     GOLD     │                │   Power BI   │
 │  (XLSX)    │                     │──────────────│   │──────────────│   │──────────────│                └──────────────┘
 └────────────┘                     │ • Raw Data   │   │ • Clean Data │   │ • Business   │                ┌──────────────┐
                                    │ • Table      │   │ • Table      │   │   Data       │                │ SQL Queries │
                     ─────────▶    │ • Batch Load │   │ • Batch Load │   │ • KPIs       │    ─────────▶  └──────────────┘
                                    │ • Full Load  │   │ • Full Load  │   │ • Metrics    │                ┌──────────────┐
                                    │ • No Transf. │   │ • Cleaning   │   │ • Aggregates │                │ Machine Learn│
                                    │ • Model: None│   │ • Normalize  │   │ • Fact/Dim   │                └──────────────┘
                                    │              │   │ • Standardize│   │ • Star Schema│
                                    │              │   │ • Derived Col│   │              │
                                    │              │   │ • Enrichment │   │              │
                                    │              │   │ • Model: None│   │              │
                                    └──────────────┘   └──────────────┘   └──────────────┘
```


<img width="848" height="501" alt="Draw Architecture" src="https://github.com/user-attachments/assets/3478f64d-5150-4059-bcc9-c1d81fd5d6c4" />
