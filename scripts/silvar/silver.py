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
----------------------------------------------------------------------------------------------------------------




