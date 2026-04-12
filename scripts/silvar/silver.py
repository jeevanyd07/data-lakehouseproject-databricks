# ================================================================
# 📦 Silver Layer – Data Cleaning & Transformation (PySpark & SQL)
# ================================================================


# =========================================================
# 🔹 1. Dealer Invoices
# =========================================================

dealer_invoices_df = spark.sql("""
SELECT
    _c0 AS Dealer_Code,

    CONCAT(_c0, '_', _c20) AS Dealer_Location_Key,

    _c1 AS Dealer_State,

    -- ✅ Date_Inv
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

    -- ✅ Network Type
    CASE 
      WHEN _c0 IN ('KA010001','TN310001','TS010011','TN310002','AP040001','AP020002') THEN 'FICOCO'
      ELSE 'FIFO'
    END AS Network_Type,

    _c6 AS Job_Number,

    -- ✅ Job Date
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

    -- ✅ Insurance Party
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

    -- ✅ Numeric conversions
    CAST(CAST(_c26 AS FLOAT) AS INT) AS Qty,
    CAST(_c27 AS FLOAT) AS Rate,
    CAST(_c28 AS FLOAT) AS Txbl_Total,
    CAST(CAST(_c35 AS FLOAT) AS INT) AS Discount,
    CAST(CAST(_c36 AS FLOAT) AS INT) AS Total

FROM workspace.bronze.dealer_invoices
WHERE _c2 != 'Date_Inv'
""")

dealer_invoices_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.dealer_invoices")


# =========================================================
# 🔹 2. Dealer Workshop Performance
# =========================================================

dealer_workshop_df = spark.sql("""
SELECT
    _c0 AS Fiscal_Year,
    _c1 AS Day,
    _c2 AS Month,
    _c3 AS Job_Card_Number,
    _c4 AS Invoice_Number,

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

    CAST(_c31 AS FLOAT) AS Labour_Revenue,
    CAST(_c32 AS FLOAT) AS Parts_Revenue,
    CAST(_c34 AS FLOAT) AS Oil_Revenue,
    CAST(_c36 AS FLOAT) AS Accessories_Revenue,
    CAST(_c40 AS FLOAT) AS Discount_Amount,
    CAST(CAST(_c41 AS FLOAT) AS INT) AS Total_Job_Card_Revenue,

    CASE 
      WHEN _c42 IS NULL THEN 'NA'
      ELSE _c42
    END AS Insurance_Party

FROM workspace.bronze.dealer_workshop_performance
WHERE _c5 != 'Job_Card_Open_Date'
""")

dealer_workshop_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.dealer_workshop_performance")


# =========================================================
# 🔹 3. Stock Report
# =========================================================

stock_report_df = spark.sql("""
SELECT 
    _c0 AS Company_Name,
    _c1 AS Dealer_Code,

    CONCAT(_c1, '_', _c3) AS Dealer_Location_Key,

    _c2 AS Location_Name,
    _c3 AS Loc_Code,
    _c4 AS Item_Name,
    _c5 AS Item_Modl,
    _c6 AS Item_Desc,
    _c7 AS HSNSAC_Code,
    _c8 AS Igrp_Name,

    CAST(CAST(_c12 AS FLOAT) AS INT) AS OpenQty,
    CAST(_c13 AS FLOAT) AS Open_Rate,
    CAST(CAST(_c15 AS FLOAT) AS INT) AS Balance_Qty,
    CAST(_c17 AS FLOAT) AS CloseAmt,
    CAST(_c22 AS FLOAT) AS ITEM_RATE,

    _c24 AS State

FROM workspace.bronze.stock_report
WHERE _c0 != 'Company_Name'
""")

stock_report_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.stock_report")


# =========================================================
# 🔹 4. Wholesale VOC Bangalore
# =========================================================

wholesale_voc_df = spark.sql("""
SELECT 
    CONCAT(_c0, '_', _c4) AS Dealer_Location_Key,
    _c1 AS Dealer_Name, 
    _c3 AS Location_Name,

    -- ✅ Bill Date
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
    CAST(_c13 AS FLOAT) AS Qty,
    CAST(_c14 AS FLOAT) AS Rate,
    CAST(_c22 AS FLOAT) AS Discount,
    CAST(_c23 AS FLOAT) AS Total

FROM workspace.bronze.wholesale_voc_bangalore
WHERE _c1 != 'Dealer_Name'
""")

wholesale_voc_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.wholesale_voc_bangalore")


# =========================================================
# ✅ Completion Message
# =========================================================
print("✅ Silver Layer Transformation Completed Successfully!")
