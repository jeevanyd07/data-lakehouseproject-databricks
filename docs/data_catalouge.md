# 📊 GOLD LAYER – DATA CATALOG
## 🟡 Gold Layer Overview
The Gold Layer contains business-ready, aggregated tables used for reporting, dashboards, and decision-making.

These tables are built from the Silver Layer and optimized for:

### 📈 Reporting
### 📊 Dashboards (Power BI / Tableau)
### 📉 Business insights

### 📌 1. sale_stock_table
### 🔹 Description
This table combines sales and stock data to identify:

Sales performance
Stock availability
Required stock (reorder quantity)

### 🔹 Source Tables
silver.dealer_invoices
silver.stock_report

| Column Name         | Description                    |
| ------------------- | ------------------------------ |
| Company_Name        | Company name from stock data   |
| Location_Name       | Dealer location name           |
| Dealer_Location_Key | Unique key (Dealer + Location) |
| Dealer_State        | State of dealer                |
| Network_Type        | FICOCO / FIFO classification   |
| Item                | Item name                      |
| Description         | Item description               |
| Item_Type           | Spare / Oil / etc              |
| Item_Group          | Item grouping                  |
| Total_Sale_Qty      | Total quantity sold            |
| Avg_Sale_Per_Month  | Average monthly sales          |
| Item_Rate           | Average selling rate           |
| Balance_Qty         | Available stock                |
| Required_qty        | Quantity to reorder            |
| Total_Sale_Amt      | Total sales amount             |

