# Bronze Layer
# Init

import inspect
# Check what methods are available on DataFrameReader
print("Available methods on DataFrameReader:")
methods = [m for m in dir(spark.read) if not m.startswith('_')]
print([m for m in methods if 'xl' in m.lower() or 'excel' in m.lower()])

----------------------------------------------------------------------------------------------------------

# Reading ERP Files from Excle
# Reading 1.Dealer_Invoice_Report.excle
df = (spark.read.option("header","true")
      .option("inferSchema","true")
      .excel("/Volumes/workspace/bronze/source_system/ERP_FILES/Dealer_Invoice_Report.xlsx"))
df.display()

# Write to Bronze Layer table 1.bronze.dealer_invoices
df.write.mode("overwrite").saveAsTable("bronze.dealer_invoices")

-------------------------------------------------------------------------------------------------------------


