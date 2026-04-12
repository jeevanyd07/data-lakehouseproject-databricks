-- =========================================================
-- 🥇 Gold Layer – Fact & Dimension Tables (Databricks SQL)
-- =========================================================


-- =========================================================
-- 🔹 1. FACT TABLE: Sale vs Stock
-- =========================================================

CREATE OR REPLACE TABLE workspace.gold.sale_stock_table
USING DELTA
AS

WITH Sale_Data AS (
    SELECT
        TRIM(UPPER(Dealer_Location_Key)) AS Dealer_Location_Key,
        Dealer_State,
        Network_Type,
        Item,
        Description,
        Item_Type,
        Item_Group,

        SUM(Qty) AS Total_Sale_Qty,

        -- ✅ Safe calculations
        ROUND(TRY_DIVIDE(SUM(Total), SUM(Qty))) AS Item_Rate,
        ROUND(SUM(Qty) / 5) AS Avg_Sale_Per_Month,
        ROUND(SUM(Total)) AS Total_Sale_Amt

    FROM workspace.silver.dealer_invoices
    WHERE Item_Type != 'Labour'
      AND Date_Inv >= '2025-10-01'

    GROUP BY
        TRIM(UPPER(Dealer_Location_Key)),
        Dealer_State,
        Network_Type,
        Item,
        Description,
        Item_Type,
        Item_Group
),

Stock_Data AS (
    SELECT
        TRIM(UPPER(Dealer_Location_Key)) AS Dealer_Location_Key,
        Company_Name,
        Location_Name,
        Item_Modl,

        -- ✅ Null handling
        COALESCE(Balance_Qty, 0) AS Balance_Qty

    FROM workspace.silver.stock_report
)

SELECT
    st.Company_Name,
    st.Location_Name,
    sl.Dealer_Location_Key,
    sl.Dealer_State,
    sl.Network_Type,
    sl.Item,
    sl.Description,
    sl.Item_Type,
    sl.Item_Group,

    sl.Total_Sale_Qty,
    sl.Avg_Sale_Per_Month,
    sl.Item_Rate,

    -- ✅ Clean stock value
    CASE 
        WHEN st.Balance_Qty < 0 THEN 0
        ELSE COALESCE(st.Balance_Qty, 0)
    END AS Balance_Qty,

    -- ✅ Required Quantity Calculation
    CASE
        WHEN sl.Avg_Sale_Per_Month - COALESCE(st.Balance_Qty, 0) < 0 THEN 0
        ELSE sl.Avg_Sale_Per_Month - COALESCE(st.Balance_Qty, 0)
    END AS Required_Qty,

    sl.Total_Sale_Amt

FROM Sale_Data sl
LEFT JOIN Stock_Data st
    ON sl.Dealer_Location_Key = st.Dealer_Location_Key
    AND sl.Item = st.Item_Modl

ORDER BY sl.Avg_Sale_Per_Month;



-- =========================================================
-- 🔹 2. DIM TABLE: Customer Data
-- =========================================================

CREATE OR REPLACE TABLE workspace.gold.customer_data_table
USING DELTA
AS

WITH Base AS (
    SELECT
        Fiscal_Year,
        Month,
        Dealer_Location_Key,
        Network_Type,
        Network_Name,
        Network_Location,
        Dealer_State,
        Service_Type,
        Job_In_Date,
        Job_Out_Date,
        Customer_Name,
        Phone_Number,
        Job_Card_Status,
        Job_Source,
        Registration_Number,
        ODO_Reading,
        Model_Category,
        Brand_Name,
        Technician_Name,
        Service_Advisor_Name,
        Total_Job_Card_Revenue,

        -- ✅ Service duration
        DATEDIFF(Job_Out_Date, Job_In_Date) AS Service_Days

    FROM workspace.silver.dealer_workshop_performance
),

Customer_Aggregate AS (
    SELECT
        Dealer_Location_Key,
        Phone_Number,

        MAX(Job_In_Date) AS Last_Visit_Date,
        COUNT(*) AS Visit_Count

    FROM Base
    GROUP BY Dealer_Location_Key, Phone_Number
)

SELECT
    b.*,
    c.Last_Visit_Date,
    c.Visit_Count,

    -- ✅ Customer Segmentation
    CASE 
        WHEN DATEDIFF(CURRENT_DATE(), c.Last_Visit_Date) > 180 THEN 'Lost Customer'
        ELSE 'Active Customer'
    END AS Customer_Status,

    CURRENT_DATE() AS Load_Date

FROM Base b
LEFT JOIN Customer_Aggregate c
    ON b.Dealer_Location_Key = c.Dealer_Location_Key
    AND b.Phone_Number = c.Phone_Number;
