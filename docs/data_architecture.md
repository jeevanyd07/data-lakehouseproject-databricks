create a data_architecture 
This data Architecture for this project follows Medallion Architecture Bronze,Silver,and Gold Layers
Source is ERP : xlsx file
Bronze Layer : Row Date, Object Type - Table, Load - Bactch Processing Full Load(Truncate & insert), No Transformation, Model - None.
Silver Layer : Clean Standard Data, Object Type - Table, Load - Bactch Processing Full Load(Truncate & insert), Transformation - Data Cleaning, Data Normalization, Data Standardization, Derived Columns, Data Enrichnment , Data Model - None (As-Is)
Gold Layer : Buisness Ready Data, 
