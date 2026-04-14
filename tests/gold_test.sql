-- ============================================
-- Sale vs Stock Table (Silver → Gold)
-- ============================================

-- 🔹 Step 1: Create Temp View for Testing
CREATE OR REPLACE TEMP VIEW sale_stock_table_test AS

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

    -- ✅ Safe Stock
    CASE 
      WHEN st.Balance_Qty < 0 THEN 0
      ELSE COALESCE(st.Balance_Qty, 0)
    END AS Balance_Qty,

    -- ✅ Required Quantity
    CASE
      WHEN sl.Avg_Sale_Per_Month - COALESCE(st.Balance_Qty, 0) < 0 THEN 0
      ELSE sl.Avg_Sale_Per_Month - COALESCE(st.Balance_Qty, 0)
    END AS Required_qty,

    sl.Total_Sale_Amt

FROM Sale_Data sl
LEFT JOIN Stock_Data st
  ON sl.Dealer_Location_Key = st.Dealer_Location_Key
  AND sl.Item = st.Item_Modl;


-- ============================================
-- 🔹 Step 2: Data Validation
-- ============================================

-- ✅ Sample Data
SELECT * FROM sale_stock_table_test LIMIT 10;

-- ✅ Row Count
SELECT COUNT(*) AS total_rows FROM sale_stock_table_test;

-- ✅ Null Checks
SELECT
    COUNT(*) AS total_rows,
    COUNT(Item) AS valid_item,
    COUNT(Dealer_Location_Key) AS valid_location,
    COUNT(Avg_Sale_Per_Month) AS valid_avg_sales
FROM sale_stock_table_test;


-- ✅ Negative / Data Issues Check
SELECT *
FROM sale_stock_table_test
WHERE Balance_Qty < 0 OR Required_qty < 0;

-- ✅ Data Quality Status
SELECT 
  CASE 
    WHEN COUNT(*) = 0 THEN 'FAIL'
    ELSE 'PASS'
  END AS data_status
FROM sale_stock_table_test;


-- ============================================
-- 🔹 Step 3: Load to Gold Table
-- ============================================

CREATE OR REPLACE TABLE workspace.gold.sale_stock_table
USING DELTA
AS
SELECT *
FROM sale_stock_table_test
ORDER BY Avg_Sale_Per_Month;

--===========================================================================================================

-- ============================================
-- Customer Data Table (Silver → Gold)
-- ============================================

-- 🔹 Step 1: Create Temp View for Testing
CREATE OR REPLACE TEMP VIEW customer_data_table_test AS

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

        -- ✅ Service Days
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

    -- ✅ Customer Status
    CASE 
        WHEN DATEDIFF(CURRENT_DATE(), c.Last_Visit_Date) > 180 THEN 'Lost Customer'
        ELSE 'Active Customer'
    END AS Customer_Status,

    CURRENT_DATE() AS Load_Date

FROM Base b
LEFT JOIN Customer_Aggregate c
ON b.Dealer_Location_Key = c.Dealer_Location_Key
AND b.Phone_Number = c.Phone_Number;


-- ============================================
-- 🔹 Step 2: Data Validation
-- ============================================

-- ✅ Sample Data
SELECT * FROM customer_data_table_test LIMIT 10;

-- ✅ Row Count
SELECT COUNT(*) AS total_rows FROM customer_data_table_test;

-- ✅ Null Checks
SELECT
    COUNT(*) AS total_rows,
    COUNT(Phone_Number) AS valid_phone,
    COUNT(Dealer_Location_Key) AS valid_location,
    COUNT(Customer_Status) AS valid_status
FROM customer_data_table_test;

-- ✅ Negative / Invalid Service Days
SELECT *
FROM customer_data_table_test
WHERE Service_Days < 0;

-- ✅ Customer Segmentation Check
SELECT Customer_Status, COUNT(*) 
FROM customer_data_table_test
GROUP BY Customer_Status;

-- ✅ Data Quality Status
SELECT 
  CASE 
    WHEN COUNT(*) = 0 THEN 'FAIL'
    ELSE 'PASS'
  END AS data_status
FROM customer_data_table_test;


-- ============================================
-- 🔹 Step 3: Load to Gold Table
-- ============================================

CREATE OR REPLACE TABLE workspace.gold.customer_data_table
USING DELTA
AS
SELECT *
FROM customer_data_table_test;
