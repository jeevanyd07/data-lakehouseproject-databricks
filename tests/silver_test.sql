-- ============================================
-- Dealer Invoices Transformation (Bronze → Silver)
-- ============================================

-- 🔹 Step 1: Create Temp View for Testing
CREATE OR REPLACE TEMP VIEW dealer_invoices_test AS
SELECT
    _c0 AS Dealer_Code,

    CONCAT(_c0, '_', _c20) AS Dealer_Location_Key,

    _c1 AS Dealer_State,

    CAST(
        COALESCE(
            try_to_date(_c2, 'M/d/yy'),
            try_to_date(_c2, 'd/M/yy'),
            try_to_date(_c2, 'dd-MM-yyyy'),
            try_to_date(_c2, 'dd/MM/yyyy')
        ) AS DATE
    ) AS Date_Inv,

    _c3 AS Bill_No,
    _c5 AS Party_Name,
    
    CASE 
      WHEN _c0 IN ('KA010001','TN310001','TS010011','TN310002','AP040001','AP020002') THEN 'FICOCO'
      ELSE 'FIFO'
    END AS Network_Type,
    
    _c6 AS Job_Number,

    CAST(
        COALESCE(
            try_to_date(_c7, 'M/d/yy'),
            try_to_date(_c7, 'd/M/yy'),
            try_to_date(_c7, 'dd-MM-yyyy'),
            try_to_date(_c7, 'dd/MM/yyyy')
        ) AS DATE
    ) AS Job_Date,
    
    _c10 AS Service_Type,
    _c11 AS Job_Source,
    
    CASE 
      WHEN _c12 IS NULL THEN 'NA'
      ELSE _c12
    END AS Insurance_Party,
    
    _c13 AS Model_Name,
    _c14 AS Brand_Classification,
    _c16 AS ODO_Reading,
    TRIM(_c17) AS Reg_No,
    _c19 AS Dealer_Location,
    _c20 AS Location_Code,
    _c21 AS Item,
    _c22 AS Description,
    _c23 AS Item_Type,
    _c24 AS Item_Group,

    CAST(CAST(_c26 AS FLOAT) AS INT) AS Qty,
    CAST(_c27 AS FLOAT) AS Rate,
    CAST(_c28 AS FLOAT) AS Txbl_Total,
    CAST(CAST(_c35 AS FLOAT) AS INT) AS Discount,
    CAST(CAST(_c36 AS FLOAT) AS INT) AS Total

FROM workspace.bronze.dealer_invoices
WHERE _c2 != 'Date_Inv' AND _c26 != 'Qty';


-- ============================================
-- 🔹 Step 2: Data Validation (Testing)
-- ============================================

-- ✅ Sample Data Check
SELECT * FROM dealer_invoices_test LIMIT 10;

-- ✅ Row Count Check
SELECT COUNT(*) AS total_rows FROM dealer_invoices_test;

-- ✅ Null Validation
SELECT
    COUNT(*) AS total_rows,
    COUNT(Date_Inv) AS valid_dates,
    COUNT(Job_Date) AS valid_job_dates,
    COUNT(Qty) AS valid_qty
FROM dealer_invoices_test;

-- ✅ Duplicate Check
SELECT Dealer_Code, Bill_No, COUNT(*) AS duplicate_count
FROM dealer_invoices_test
GROUP BY Dealer_Code, Bill_No
HAVING COUNT(*) > 1;

-- ✅ Data Quality Status
SELECT 
  CASE 
    WHEN COUNT(*) = 0 THEN 'FAIL'
    ELSE 'PASS'
  END AS data_status
FROM dealer_invoices_test;


-- ============================================
-- 🔹 Step 3: Load to Silver Table
-- ============================================

CREATE OR REPLACE TABLE silver.dealer_invoices
USING DELTA
AS
SELECT * FROM dealer_invoices_test;


-- ==============================================================================================================


-- ============================================
-- Dealer Workshop Performance (Bronze → Silver)
-- ============================================

-- 🔹 Step 1: Create Temp View for Testing
CREATE OR REPLACE TEMP VIEW dealer_workshop_performance_test AS
SELECT
    _c0 AS Fiscal_Year,
    _c1 AS Day,
    _c2 AS Month,
    _c3 AS Job_Card_Number,
    _c4 AS Invoice_Number,

    -- ✅ Concatenated Key
    CONCAT(_c8, '_', _c15) AS Dealer_Location_Key,

    -- ✅ Job_In_Date
    CAST(
        COALESCE(
            try_to_date(_c5, 'M/d/yy'),
            try_to_date(_c5, 'd/M/yy'),
            try_to_date(_c5, 'dd-MM-yyyy'),
            try_to_date(_c5, 'dd/MM/yyyy')
        ) AS DATE
    ) AS Job_In_Date,

    -- ✅ Job_Out_Date
    CAST(
        COALESCE(
            try_to_date(_c6, 'M/d/yy'),
            try_to_date(_c6, 'd/M/yy'),
            try_to_date(_c6, 'dd-MM-yyyy'),
            try_to_date(_c6, 'dd/MM/yyyy')
        ) AS DATE
    ) AS Job_Out_Date,

    CASE 
      WHEN _c8 IN ('KA010001','TN310001','TS010011','TN310002','AP040001','AP020002') THEN 'FICOCO'
      ELSE 'FIFO'
    END AS Network_Type,

    _c8 AS Network_Code,
    _c9 AS Network_Name,
    _c10 AS Dealer_State,
    _c12 AS Service_Type,
    _c13 AS ODO_Reading,
    _c14 AS Network_Location,
    _c15 AS Location_Code,
    _c16 AS Customer_Name,
    _c17 AS Phone_Number,
    _c18 AS Job_Card_Status,
    _c19 AS Job_Source,
    TRIM(_c22) AS Registration_Number,
    _c23 AS Model_Category,
    _c24 AS Brand_Name,
    _c25 AS Model_Name,
    _c26 AS Technician_Name,
    _c28 AS Service_Advisor_Name,

    -- ✅ Revenue Columns
    CAST(_c31 AS FLOAT) AS Labour_Revenue,
    CAST(_c32 AS FLOAT) AS Parts_Revenue,
    CAST(_c34 AS FLOAT) AS Oil_Revenue,
    CAST(_c36 AS FLOAT) AS Accessories_Revenue,

    -- ✅ Discount
    CAST(_c40 AS FLOAT) AS Discount_Amount,

    -- ✅ Total Revenue
    CAST(CAST(_c41 AS FLOAT) AS INT) AS Total_Job_Card_Revenue,

    CASE 
      WHEN _c42 IS NULL THEN 'NA'
      ELSE _c42
    END AS Insurance_Party

FROM workspace.bronze.dealer_workshop_performance
WHERE _c5 != 'Job_Card_Open_Date';


-- ============================================
-- 🔹 Step 2: Data Validation
-- ============================================

-- ✅ Sample Data
SELECT * FROM dealer_workshop_performance_test LIMIT 10;

-- ✅ Row Count
SELECT COUNT(*) AS total_rows FROM dealer_workshop_performance_test;

-- ✅ Null Checks
SELECT
    COUNT(*) AS total_rows,
    COUNT(Job_In_Date) AS valid_job_in_date,
    COUNT(Job_Out_Date) AS valid_job_out_date,
    COUNT(Total_Job_Card_Revenue) AS valid_revenue
FROM dealer_workshop_performance_test;

-- ✅ Data Quality Status
SELECT 
  CASE 
    WHEN COUNT(*) = 0 THEN 'FAIL'
    ELSE 'PASS'
  END AS data_status
FROM dealer_workshop_performance_test;


-- ============================================
-- 🔹 Step 3: Load to Silver Table
-- ============================================

CREATE OR REPLACE TABLE silver.dealer_workshop_performance
USING DELTA
AS
SELECT * FROM dealer_workshop_performance_test;

--==============================================================================================

-- ============================================
-- Stock Report Transformation (Bronze → Silver)
-- ============================================

-- 🔹 Step 1: Create Temp View for Testing
CREATE OR REPLACE TEMP VIEW stock_report_test AS
SELECT 
    _c0 AS Company_Name,
    _c1 AS Dealer_Code,

    -- ✅ Concatenated Key
    CONCAT(_c1, '_', _c3) AS Dealer_Location_Key,

    _c2 AS Location_Name,
    _c3 AS Loc_Code,
    _c4 AS Item_Name,
    _c5 AS Item_Modl,
    _c6 AS Item_Desc,
    _c7 AS HSNSAC_Code,
    _c8 AS Igrp_Name,

    -- ✅ Numeric conversions
    CAST(CAST(_c12 AS FLOAT) AS INT) AS OpenQty,
    CAST(_c13 AS FLOAT) AS Open_Rate,
    CAST(CAST(_c15 AS FLOAT) AS INT) AS Balance_Qty,
    CAST(_c17 AS FLOAT) AS CloseAmt,
    CAST(_c22 AS FLOAT) AS ITEM_RATE,

    _c24 AS State

FROM workspace.bronze.stock_report
WHERE _c0 != 'Company_Name';


-- ============================================
-- 🔹 Step 2: Data Validation
-- ============================================

-- ✅ Sample Data
SELECT * FROM stock_report_test LIMIT 10;

-- ✅ Row Count
SELECT COUNT(*) AS total_rows FROM stock_report_test;

-- ✅ Null Checks
SELECT
    COUNT(*) AS total_rows,
    COUNT(OpenQty) AS valid_open_qty,
    COUNT(Balance_Qty) AS valid_balance_qty,
    COUNT(ITEM_RATE) AS valid_item_rate
FROM stock_report_test;

-- ✅ Duplicate Check
SELECT Dealer_Code, Item_Name, COUNT(*) AS duplicate_count
FROM stock_report_test
GROUP BY Dealer_Code, Item_Name
HAVING COUNT(*) > 1;

-- ✅ Data Quality Status
SELECT 
  CASE 
    WHEN COUNT(*) = 0 THEN 'FAIL'
    ELSE 'PASS'
  END AS data_status
FROM stock_report_test;


-- ============================================
-- 🔹 Step 3: Load to Silver Table
-- ============================================

CREATE OR REPLACE TABLE silver.stock_report
USING DELTA
AS
SELECT * FROM stock_report_test;

--==================================================================================================

-- ============================================
-- Wholesale VOC Bangalore (Bronze → Silver)
-- ============================================

-- 🔹 Step 1: Create Temp View for Testing
CREATE OR REPLACE TEMP VIEW wholesale_voc_bangalore_test AS
SELECT 
    CONCAT(_c0, '_', _c4) AS Dealer_Location_Key,
    _c1 AS Dealer_Name, 
    _c3 AS Location_Name,

    -- ✅ Bill Date Conversion
    CAST(
        COALESCE(
            try_to_date(_c5, 'M/d/yy'),
            try_to_date(_c5, 'd/M/yy'),
            try_to_date(_c5, 'dd-MM-yyyy'),
            try_to_date(_c5, 'dd/MM/yyyy')
        ) AS DATE
    ) AS Bill_Date,

    _c6 AS Bill_No,
    _c8 AS Party_Name,
    _c9 AS Party_Code,
    _c10 AS Item,
    _c11 AS Item_Desc,
    _c12 AS Item_Type,

    -- ✅ Numeric Conversions
    CAST(CAST(_c13 AS FLOAT) AS INT) AS Qty,
    CAST(_c14 AS FLOAT) AS Rate,
    CAST(_c22 AS FLOAT) AS Discount,
    CAST(_c23 AS FLOAT) AS Total

FROM workspace.bronze.wholesale_voc_bangalore
WHERE _c1 != 'Dealer_Name';


-- ============================================
-- 🔹 Step 2: Data Validation
-- ============================================

-- ✅ Sample Data
SELECT * FROM wholesale_voc_bangalore_test LIMIT 10;

-- ✅ Row Count
SELECT COUNT(*) AS total_rows FROM wholesale_voc_bangalore_test;

-- ✅ Null Checks
SELECT
    COUNT(*) AS total_rows,
    COUNT(Bill_Date) AS valid_bill_date,
    COUNT(Qty) AS valid_qty,
    COUNT(Total) AS valid_total
FROM wholesale_voc_bangalore_test;

-- ✅ Duplicate Check
SELECT Bill_No, Item, COUNT(*) AS duplicate_count
FROM wholesale_voc_bangalore_test
GROUP BY Bill_No, Item
HAVING COUNT(*) > 1;

-- ✅ Data Quality Status
SELECT 
  CASE 
    WHEN COUNT(*) = 0 THEN 'FAIL'
    ELSE 'PASS'
  END AS data_status
FROM wholesale_voc_bangalore_test;


-- ============================================
-- 🔹 Step 3: Load to Silver Table
-- ============================================

CREATE OR REPLACE TABLE silver.wholesale_voc_bangalore
USING DELTA
AS
SELECT * FROM wholesale_voc_bangalore_test;
