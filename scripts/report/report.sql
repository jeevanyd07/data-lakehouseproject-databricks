-- =========================================================
-- 📊 Report Layer – Aggregated Business Reports
-- =========================================================


-- =========================================================
-- 🔹 1. Quarter-wise Revenue Report
-- =========================================================

CREATE OR REPLACE TABLE workspace.reports.quarter_month_revenue
USING DELTA
AS

SELECT
    Network_Name,
    Network_Location,
    Dealer_Location_Key,
    Dealer_State,
    Network_Type,

    -- Q1 (Apr–Jun)
    SUM(CASE WHEN MONTH(Job_Out_Date)=4 THEN Total_Job_Card_Revenue ELSE 0 END) AS April,
    SUM(CASE WHEN MONTH(Job_Out_Date)=5 THEN Total_Job_Card_Revenue ELSE 0 END) AS May,
    SUM(CASE WHEN MONTH(Job_Out_Date)=6 THEN Total_Job_Card_Revenue ELSE 0 END) AS June,
    SUM(CASE WHEN MONTH(Job_Out_Date) BETWEEN 4 AND 6 THEN Total_Job_Card_Revenue ELSE 0 END) AS Q1,

    -- Q2 (Jul–Sep)
    SUM(CASE WHEN MONTH(Job_Out_Date)=7 THEN Total_Job_Card_Revenue ELSE 0 END) AS July,
    SUM(CASE WHEN MONTH(Job_Out_Date)=8 THEN Total_Job_Card_Revenue ELSE 0 END) AS August,
    SUM(CASE WHEN MONTH(Job_Out_Date)=9 THEN Total_Job_Card_Revenue ELSE 0 END) AS September,
    SUM(CASE WHEN MONTH(Job_Out_Date) BETWEEN 7 AND 9 THEN Total_Job_Card_Revenue ELSE 0 END) AS Q2,

    -- Q3 (Oct–Dec)
    SUM(CASE WHEN MONTH(Job_Out_Date)=10 THEN Total_Job_Card_Revenue ELSE 0 END) AS October,
    SUM(CASE WHEN MONTH(Job_Out_Date)=11 THEN Total_Job_Card_Revenue ELSE 0 END) AS November,
    SUM(CASE WHEN MONTH(Job_Out_Date)=12 THEN Total_Job_Card_Revenue ELSE 0 END) AS December,
    SUM(CASE WHEN MONTH(Job_Out_Date) BETWEEN 10 AND 12 THEN Total_Job_Card_Revenue ELSE 0 END) AS Q3,

    -- Q4 (Jan–Mar)
    SUM(CASE WHEN MONTH(Job_Out_Date)=1 THEN Total_Job_Card_Revenue ELSE 0 END) AS January,
    SUM(CASE WHEN MONTH(Job_Out_Date)=2 THEN Total_Job_Card_Revenue ELSE 0 END) AS February,
    SUM(CASE WHEN MONTH(Job_Out_Date)=3 THEN Total_Job_Card_Revenue ELSE 0 END) AS March,
    SUM(CASE WHEN MONTH(Job_Out_Date) BETWEEN 1 AND 3 THEN Total_Job_Card_Revenue ELSE 0 END) AS Q4

FROM workspace.silver.dealer_workshop_performance

WHERE Job_Out_Date BETWEEN '2025-04-01' AND '2026-03-31'

GROUP BY
    Network_Name,
    Network_Location,
    Dealer_Location_Key,
    Dealer_State,
    Network_Type

ORDER BY Network_Name, Network_Location;



-- =========================================================
-- 🔹 2. Quarter-wise Job Count Report
-- =========================================================

CREATE OR REPLACE TABLE workspace.reports.quarter_month_jobcount
USING DELTA
AS

SELECT
    Network_Name,
    Network_Location,
    Dealer_State,
    Network_Type,

    -- Q1
    COUNT(CASE WHEN MONTH(Job_In_Date)=4 THEN 1 END) AS April,
    COUNT(CASE WHEN MONTH(Job_In_Date)=5 THEN 1 END) AS May,
    COUNT(CASE WHEN MONTH(Job_In_Date)=6 THEN 1 END) AS June,
    COUNT(CASE WHEN MONTH(Job_In_Date) BETWEEN 4 AND 6 THEN 1 END) AS Q1,

    -- Q2
    COUNT(CASE WHEN MONTH(Job_In_Date)=7 THEN 1 END) AS July,
    COUNT(CASE WHEN MONTH(Job_In_Date)=8 THEN 1 END) AS August,
    COUNT(CASE WHEN MONTH(Job_In_Date)=9 THEN 1 END) AS September,
    COUNT(CASE WHEN MONTH(Job_In_Date) BETWEEN 7 AND 9 THEN 1 END) AS Q2,

    -- Q3
    COUNT(CASE WHEN MONTH(Job_In_Date)=10 THEN 1 END) AS October,
    COUNT(CASE WHEN MONTH(Job_In_Date)=11 THEN 1 END) AS November,
    COUNT(CASE WHEN MONTH(Job_In_Date)=12 THEN 1 END) AS December,
    COUNT(CASE WHEN MONTH(Job_In_Date) BETWEEN 10 AND 12 THEN 1 END) AS Q3,

    -- Q4
    COUNT(CASE WHEN MONTH(Job_In_Date)=1 THEN 1 END) AS January,
    COUNT(CASE WHEN MONTH(Job_In_Date)=2 THEN 1 END) AS February,
    COUNT(CASE WHEN MONTH(Job_In_Date)=3 THEN 1 END) AS March,
    COUNT(CASE WHEN MONTH(Job_In_Date) BETWEEN 1 AND 3 THEN 1 END) AS Q4

FROM workspace.silver.dealer_workshop_performance

WHERE Job_In_Date BETWEEN '2025-04-01' AND '2026-03-31'

GROUP BY
    Network_Name,
    Network_Location,
    Dealer_State,
    Network_Type,
    Dealer_Location_Key

ORDER BY Network_Name, Network_Location;



-- =========================================================
-- 🔹 3. Retail vs Wholesale Report
-- =========================================================

CREATE OR REPLACE TABLE workspace.reports.Wholesale_VS_Retail
USING DELTA
AS

WITH Whole_sale AS (
    SELECT
        Party_Name,
        Party_Code,
        Item_Type,

        ROUND(SUM(CASE WHEN MONTH(Bill_Date)=4 THEN Total ELSE 0 END)) AS Whole_Sale_April,
        ROUND(SUM(CASE WHEN MONTH(Bill_Date)=5 THEN Total ELSE 0 END)) AS Whole_Sale_May,
        ROUND(SUM(CASE WHEN MONTH(Bill_Date)=6 THEN Total ELSE 0 END)) AS Whole_Sale_June,
        ROUND(SUM(CASE WHEN MONTH(Bill_Date) BETWEEN 4 AND 6 THEN Total ELSE 0 END)) AS Whole_Sale_Q1,

        ROUND(SUM(CASE WHEN MONTH(Bill_Date)=7 THEN Total ELSE 0 END)) AS Whole_Sale_July,
        ROUND(SUM(CASE WHEN MONTH(Bill_Date)=8 THEN Total ELSE 0 END)) AS Whole_Sale_August,
        ROUND(SUM(CASE WHEN MONTH(Bill_Date)=9 THEN Total ELSE 0 END)) AS Whole_Sale_September,
        ROUND(SUM(CASE WHEN MONTH(Bill_Date) BETWEEN 7 AND 9 THEN Total ELSE 0 END)) AS Whole_Sale_Q2,

        ROUND(SUM(CASE WHEN MONTH(Bill_Date)=10 THEN Total ELSE 0 END)) AS Whole_Sale_October,
        ROUND(SUM(CASE WHEN MONTH(Bill_Date)=11 THEN Total ELSE 0 END)) AS Whole_Sale_November,
        ROUND(SUM(CASE WHEN MONTH(Bill_Date)=12 THEN Total ELSE 0 END)) AS Whole_Sale_December,
        ROUND(SUM(CASE WHEN MONTH(Bill_Date) BETWEEN 10 AND 12 THEN Total ELSE 0 END)) AS Whole_Sale_Q3,

        ROUND(SUM(CASE WHEN MONTH(Bill_Date)=1 THEN Total ELSE 0 END)) AS Whole_Sale_January,
        ROUND(SUM(CASE WHEN MONTH(Bill_Date)=2 THEN Total ELSE 0 END)) AS Whole_Sale_February,
        ROUND(SUM(CASE WHEN MONTH(Bill_Date)=3 THEN Total ELSE 0 END)) AS Whole_Sale_March,
        ROUND(SUM(CASE WHEN MONTH(Bill_Date) BETWEEN 1 AND 3 THEN Total ELSE 0 END)) AS Whole_Sale_Q4

    FROM workspace.silver.wholesale_voc_bangalore
    WHERE Bill_Date BETWEEN '2025-04-01' AND '2026-03-31'
    AND Item_Type != 'Labour'
    GROUP BY Party_Name, Party_Code, Item_Type
),

Retail AS (
    SELECT 
        Dealer_Code,
        Network_Type,
        Item_Type,

        ROUND(SUM(CASE WHEN MONTH(Date_Inv)=4 THEN Total ELSE 0 END)) AS Retail_April,
        ROUND(SUM(CASE WHEN MONTH(Date_Inv)=5 THEN Total ELSE 0 END)) AS Retail_May,
        ROUND(SUM(CASE WHEN MONTH(Date_Inv)=6 THEN Total ELSE 0 END)) AS Retail_June,
        ROUND(SUM(CASE WHEN MONTH(Date_Inv) BETWEEN 4 AND 6 THEN Total ELSE 0 END)) AS Retail_Q1,

        ROUND(SUM(CASE WHEN MONTH(Date_Inv)=7 THEN Total ELSE 0 END)) AS Retail_July,
        ROUND(SUM(CASE WHEN MONTH(Date_Inv)=8 THEN Total ELSE 0 END)) AS Retail_August,
        ROUND(SUM(CASE WHEN MONTH(Date_Inv)=9 THEN Total ELSE 0 END)) AS Retail_September,
        ROUND(SUM(CASE WHEN MONTH(Date_Inv) BETWEEN 7 AND 9 THEN Total ELSE 0 END)) AS Retail_Q2,

        ROUND(SUM(CASE WHEN MONTH(Date_Inv)=10 THEN Total ELSE 0 END)) AS Retail_October,
        ROUND(SUM(CASE WHEN MONTH(Date_Inv)=11 THEN Total ELSE 0 END)) AS Retail_November,
        ROUND(SUM(CASE WHEN MONTH(Date_Inv)=12 THEN Total ELSE 0 END)) AS Retail_December,
        ROUND(SUM(CASE WHEN MONTH(Date_Inv) BETWEEN 10 AND 12 THEN Total ELSE 0 END)) AS Retail_Q3,

        ROUND(SUM(CASE WHEN MONTH(Date_Inv)=1 THEN Total ELSE 0 END)) AS Retail_January,
        ROUND(SUM(CASE WHEN MONTH(Date_Inv)=2 THEN Total ELSE 0 END)) AS Retail_February,
        ROUND(SUM(CASE WHEN MONTH(Date_Inv)=3 THEN Total ELSE 0 END)) AS Retail_March,
        ROUND(SUM(CASE WHEN MONTH(Date_Inv) BETWEEN 1 AND 3 THEN Total ELSE 0 END)) AS Retail_Q4

    FROM workspace.silver.dealer_invoices
    WHERE Item_Type != 'Labour'
    AND Date_Inv BETWEEN '2025-04-01' AND '2026-03-31'
    GROUP BY Dealer_Code, Network_Type, Item_Type
)

SELECT 
    r.Dealer_Code,

    CASE 
        WHEN w.Party_Name IS NULL THEN r.Dealer_Code
        ELSE w.Party_Name
    END AS Party_Name,

    r.Network_Type,
    COALESCE(w.Item_Type, r.Item_Type) AS Item_Type,

    -- Q1–Q4 (NULL safe)
    COALESCE(w.Whole_Sale_April,0), COALESCE(r.Retail_April,0),
    COALESCE(w.Whole_Sale_May,0), COALESCE(r.Retail_May,0),
    COALESCE(w.Whole_Sale_June,0), COALESCE(r.Retail_June,0),
    COALESCE(w.Whole_Sale_Q1,0), COALESCE(r.Retail_Q1,0),

    COALESCE(w.Whole_Sale_July,0), COALESCE(r.Retail_July,0),
    COALESCE(w.Whole_Sale_August,0), COALESCE(r.Retail_August,0),
    COALESCE(w.Whole_Sale_September,0), COALESCE(r.Retail_September,0),
    COALESCE(w.Whole_Sale_Q2,0), COALESCE(r.Retail_Q2,0),

    COALESCE(w.Whole_Sale_October,0), COALESCE(r.Retail_October,0),
    COALESCE(w.Whole_Sale_November,0), COALESCE(r.Retail_November,0),
    COALESCE(w.Whole_Sale_December,0), COALESCE(r.Retail_December,0),
    COALESCE(w.Whole_Sale_Q3,0), COALESCE(r.Retail_Q3,0),

    COALESCE(w.Whole_Sale_January,0), COALESCE(r.Retail_January,0),
    COALESCE(w.Whole_Sale_February,0), COALESCE(r.Retail_February,0),
    COALESCE(w.Whole_Sale_March,0), COALESCE(r.Retail_March,0),
    COALESCE(w.Whole_Sale_Q4,0), COALESCE(r.Retail_Q4,0)

FROM Retail r
LEFT JOIN Whole_sale w 
ON r.Dealer_Code = w.Party_Code 
AND w.Item_Type = r.Item_Type

WHERE r.Network_Type IS NOT NULL
ORDER BY Party_Name;
