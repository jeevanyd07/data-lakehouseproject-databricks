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
1.silver.dealer_invoices
2.silver.stock_report

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

### 🔹 Business Use
1.Inventory planning
2.Stock replenishment
3.Fast/slow moving items

---

## 📌 2. customer_data_table
### 🔹 Description

This table provides customer-level insights such as:

Visit frequency
Last visit date
Customer activity status

### 🔹 Source Table
silver.dealer_workshop_performance

| Column Name            | Description            |
| ---------------------- | ---------------------- |
| Fiscal_Year            | Financial year         |
| Month                  | Month                  |
| Dealer_Location_Key    | Dealer + Location key  |
| Network_Type           | FICOCO / FIFO          |
| Network_Name           | Dealer network name    |
| Network_Location       | Location               |
| Dealer_State           | State                  |
| Service_Type           | Type of service        |
| Job_In_Date            | Vehicle check-in date  |
| Job_Out_Date           | Delivery date          |
| Customer_Name          | Customer name          |
| Phone_Number           | Customer phone number  |
| Job_Card_Status        | Status of job          |
| Job_Source             | Walk-in / etc          |
| Registration_Number    | Vehicle number         |
| ODO_Reading            | Vehicle reading        |
| Model_Category         | Category               |
| Brand_Name             | Brand                  |
| Technician_Name        | Technician             |
| Service_Advisor_Name   | Advisor                |
| Total_Job_Card_Revenue | Revenue generated      |
| Service_Days           | Days taken for service |
| Last_Visit_Date        | Most recent visit      |
| Visit_Count            | Total visits           |
| Customer_Status        | Active / Lost          |
| Load_Date              | Data load date         |


### 🔹 Business Use
Customer retention analysis
Repeat customer tracking
Service performance
