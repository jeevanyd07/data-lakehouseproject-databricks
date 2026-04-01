# Silver Layer
-----------------------------------------------------------------------------------------------
##  Reading Table 1: bronze.dealer_invoices
-----------------------------------------------------------------------------------------------
df = spark.table("workspace.bronze.dealer_invoices")

----------------------------------------------------------------------------------------------
## Silver Transformations & Loading 1 : silver.dealer_invoices 
### 1. Convert the date format to SQL format for the columns **Date_Inv** and **Job Date**.
### 2. If **Insurance Party** is NULL, show it as **"NA"**.
### 3. Remove the unwanted columns.
### 4.Add an additional column named Network_Type.
-------------------------------------------------------------------------------------------------
df = spark.sql("""
SELECT
    _c0 AS Dealer_Code,
    _c1 AS Dealer_State,
    date_format(
        COALESCE(
            try_to_date(_c2, 'M/d/yy'),
            try_to_date(_c2, 'd/M/yy'),
            try_to_date(_c2, 'dd-MM-yyyy'),
            try_to_date(_c2, 'dd/MM/yyyy')
        ),
        'yyyy-MM-dd'
    ) AS Date_Inv,
    _c3 AS Bill_No,
    _c5 AS Party_Name,
    CASE 
      WHEN _c0 IN ('KA010001','TN310001','TS010011','TN310002','AP040001','AP020002') THEN 'FICOCO'
      ELSE 'FIFO'
    END AS Network_Type,
    _c6 AS Job_Number,
    date_format(
        COALESCE(
            try_to_date(_c7, 'M/d/yy'),
            try_to_date(_c7, 'd/M/yy'),
            try_to_date(_c7, 'dd-MM-yyyy'),
            try_to_date(_c7, 'dd/MM/yyyy')
        ),
        'yyyy-MM-dd'
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
    trim(_c17) AS Reg_No,
    _c19 AS Dealer_Location,
    _c20 AS Location_Code,
    _c21 AS Item,
    _c22 AS Description,
    _c23 AS Item_Type,
    _c24 AS Item_Group,
    _c26 AS Qty,
    _c27 AS Rate,
    _c28 AS Txbl_Total,
    _c35 AS Discount,
    _c36 AS Total
FROM workspace.bronze.dealer_invoices
WHERE _c2 != 'Date_Inv'
""")

df.write.mode("overwrite").saveAsTable("silver.dealer_invoices")

----------------------------------------------------------------------------------------------------------------
## Reading Bronze Table 2 bronze.dealer_workshop_performance 
-----------------------------------------------------------------------------------------------------------------
df = spark.table("workspace.bronze.dealer_workshop_performance")

----------------------------------------------------------------------------------------------------------------
## Silver Transformations & Loading 2: silver.dealer_workshop_performance  

### 1. Convert the date format to SQL format for the columns **Date_Inv** and **Job Date**.
### 2. If **Insurance Party** is NULL, show it as **"NA"**.
### 3. Remove the unwanted columns.
### 4.Add an additional column named Network_Type.  
-----------------------------------------------------------------------------------------------------------------
df = spark.sql("""
    SELECT
_c0 AS Fiscal_Year,
_c1 AS Day,
_c2 AS Month,
_c3 AS Job_Card_Number,
_c4 AS Invoice_Number,
 date_format(
        COALESCE(
            try_to_date(_c5, 'M/d/yy'),
            try_to_date(_c5, 'd/M/yy'),
            try_to_date(_c5, 'dd-MM-yyyy'),
            try_to_date(_c5, 'dd/MM/yyyy')
        ),
        'yyyy-MM-dd'
    ) AS Job_In_Date,
date_format(
        COALESCE(
            try_to_date(_c6, 'M/d/yy'),
            try_to_date(_c6, 'd/M/yy'),
            try_to_date(_c6, 'dd-MM-yyyy'),
            try_to_date(_c6, 'dd/MM/yyyy')
        ),
        'yyyy-MM-dd'
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
_c31 AS Labour_Revenue,
_c32 AS Parts_Revenue,
_c34 AS Oil_Revenue,
_c36 AS Accessories_Revenue,
_c40 AS Discount_Amount,
_c41 AS Total_Job_Card_Revenue,
CASE 
      WHEN _c42 IS NULL THEN 'NA'
      ELSE _c42
    END AS Insurance_Party
FROM workspace.bronze.dealer_workshop_performance
WHERE _c5 != 'Job_Card_Open_Date'
""")

df.write.mode("overwrite").saveAsTable("silver.dealer_workshop_performance")


------------------------------------------------------------------------------------------------------
# Reading Table 3: workspace.bronze.stock_report
------------------------------------------------------------------------------------------------------
df = spark.table("workspace.bronze.stock_report")

------------------------------------------------------------------------------------------------------
## Silver Transformations & Loading 3: silver.stock_report   
### No Transfermation Required Here
-------------------------------------------------------------------------------------------------------
df = spark.sql("""
SELECT 
_c0 AS Company_Name,
_c1 AS Dealer_Code,
_c2 AS Location_Name,
_c3 AS Loc_Code,
_c4 AS Item_Name,
_c5 AS Item_Modl,
_c6 AS Item_Desc,
_c7 AS HSNSAC_Code,
_c8 AS Igrp_Name,
_c12 AS OpenQty,
_c13 AS Open_Rate,
_c15 AS Balance_Qty,
_c17 AS CloseAmt,
_c22 AS ITEM_RATE,
_c24 AS State
FROM workspace.bronze.stock_report
WHERE _c0 != 'Company_Name'""")

df.write.mode("overwrite").saveAsTable("silver.stock_report")
