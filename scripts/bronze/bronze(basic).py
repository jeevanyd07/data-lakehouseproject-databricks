# Bronze Layer
-----------------------------------------------------------------------------------------------------
## Init
import inspect
# Check what methods are available on DataFrameReader
print("Available methods on DataFrameReader:")
methods = [m for m in dir(spark.read) if not m.startswith('_')]
print([m for m in methods if 'xl' in m.lower() or 'excel' in m.lower()])

-------------------------------------------------------------------------------------------------------

## Reading ERP Files from Excle
### Reading 1.Dealer_Invoice_Report.excle
# Read File 1
df1 = (spark.read
       .option("header", "true")
       .option("inferSchema", "true")
       .excel("/Volumes/workspace/bronze/source_system/ERP_FILES/Dealer_Invoice_Report.xlsx"))

# Read File 2
df2 = (spark.read
       .option("header", "true")
       .option("inferSchema", "true")
       .excel("/Volumes/workspace/bronze/source_system/ERP_FILES/Dealer_Invoice_Report1.xlsx"))

# Merge (Append)
df_final = df1.unionByName(df2)

# Display result
df_final.display()

### Write to Bronze Layer table 1.bronze.dealer_invoices
df_final.write.mode("overwrite").saveAsTable("bronze.dealer_invoices")

-------------------------------------------------------------------------------------------------------

### Reading 2.Dealer_Workshop_performance.xlsx
df = (spark.read.option("header","true")
      .option("inferSchema","true")
      .excel("/Volumes/workspace/bronze/source_system/ERP_FILES/Dealer_Workshop_Performance.xlsx"))
df.display()

### Writing 2.Dealer_Workshop_performance.xlsx
df.write.mode("overwrite").saveAsTable("bronze.dealer_workshop_performance")

--------------------------------------------------------------------------------------------------------

### Reading 3.Stock_Report.xlsx
df = (spark.read.option("header","true")
      .option("inferSchema","true")
      .excel("/Volumes/workspace/bronze/source_system/ERP_FILES/Stock_Report.xlsx"))
df.display()

df.write.mode("overwrite").saveAsTable("bronze.stock_report")

---------------------------------------------------------------------------------------------------------
### Reading 4.Wholesale_VOC_Bangalore.xlsx
df = (spark.read.option("header","true")
      .option("inferSchema","true")
      .excel("/Volumes/workspace/bronze/source_system/ERP_FILES/Wholesale_VOC_Bangalore.xlsx"))
df.display()

### Writing 4.Wholesale_VOC_Bangalore.xlsx
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.wholesale_voc_bangalore")

---------------------------------------------------------------------------------------------------------
