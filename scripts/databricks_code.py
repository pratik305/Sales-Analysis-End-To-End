# Transformation using databrick notebook
# We have to use this code databricks notebook 
# Processed csv file uploaded in databrick 
# Output: Delta tables for BI consumption

# Read Silver tables
fact_sales_raw = spark.table("fact_sales_raw")
fact_refunds_raw = spark.table("fact_refunds_raw")
dim_product_raw = spark.table("dim_product_raw")
dim_session_raw = spark.table("dim_session_raw")
dim_date_raw = spark.table("dim_date_raw")

# Write Gold Delta tables
fact_sales_raw.write.format("delta").mode("overwrite").saveAsTable("fact_sales")
fact_refunds_raw.write.format("delta").mode("overwrite").saveAsTable("fact_refunds")
dim_product_raw.write.format("delta").mode("overwrite").saveAsTable("dim_product")
dim_session_raw.write.format("delta").mode("overwrite").saveAsTable("dim_session")
dim_date_raw.write.format("delta").mode("overwrite").saveAsTable("dim_date")
