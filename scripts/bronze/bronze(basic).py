# =========================================================
# 📦 Bronze Layer – ERP Excel Ingestion (PySpark / Databricks)
# =========================================================

# -------------------------------
# 🔧 Initialization
# -------------------------------
import inspect

print("Available Excel methods:")
methods = [m for m in dir(spark.read) if not m.startswith('_')]
print([m for m in methods if 'xl' in m.lower() or 'excel' in m.lower()])


# =========================================================
# 📥 1. Dealer Invoice Report (Multiple Files)
# =========================================================

# Read File 1
df1 = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .excel("/Volumes/workspace/bronze/source_system/ERP_FILES/Dealer_Invoice_Report.xlsx")
)

# Read File 2
df2 = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .excel("/Volumes/workspace/bronze/source_system/ERP_FILES/Dealer_Invoice_Report1.xlsx")
)

# Merge (Append)
dealer_invoices_df = df1.unionByName(df2)

# Display
dealer_invoices_df.display()

# Write to Bronze Table
dealer_invoices_df.write \
    .mode("overwrite") \
    .saveAsTable("bronze.dealer_invoices")


# =========================================================
# 📥 2. Dealer Workshop Performance
# =========================================================

dealer_workshop_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .excel("/Volumes/workspace/bronze/source_system/ERP_FILES/Dealer_Workshop_Performance.xlsx")
)

dealer_workshop_df.display()

dealer_workshop_df.write \
    .mode("overwrite") \
    .saveAsTable("bronze.dealer_workshop_performance")


# =========================================================
# 📥 3. Stock Report
# =========================================================

stock_report_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .excel("/Volumes/workspace/bronze/source_system/ERP_FILES/Stock_Report.xlsx")
)

stock_report_df.display()

stock_report_df.write \
    .mode("overwrite") \
    .saveAsTable("bronze.stock_report")


# =========================================================
# 📥 4. Wholesale VOC Bangalore
# =========================================================

wholesale_voc_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .excel("/Volumes/workspace/bronze/source_system/ERP_FILES/Wholesale_VOC_Bangalore.xlsx")
)

wholesale_voc_df.display()

wholesale_voc_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze.wholesale_voc_bangalore")


# =========================================================
# ✅ Completion Message
# =========================================================
print("✅ Bronze Layer Data Ingestion Completed Successfully!")
