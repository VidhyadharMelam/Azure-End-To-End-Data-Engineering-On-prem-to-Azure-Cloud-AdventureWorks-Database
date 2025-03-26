# Databricks notebook source
# MAGIC %md
# MAGIC #### The Gold Layer in a data lakehouse or data architecture represents the final, refined, and business-ready data. It is optimized for consumption by end-users, such as analysts, data scientists, and business stakeholders. The transformations applied at this layer are focused on enriching data with additional context, aggregating data for insights, Optimizing data for performance and usability, Ensuring data quality and consistency, and structuring data to make it highly usable and performant for reporting, analytics, and machine learning.

# COMMAND ----------

from pyspark.sql import SparkSession  # To create a Spark session
from pyspark.sql import DataFrame  # To work with DataFrames
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col, to_date, when, lit
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.functions import regexp_replace


# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Reading - Silver

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.adventureworksadls0.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adventureworksadls0.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adventureworksadls0.dfs.core.windows.net", "326a7877-806a-4d58-b021-9d60117c06f8")
spark.conf.set("fs.azure.account.oauth2.client.secret.adventureworksadls0.dfs.core.windows.net", "mZ28Q~B0nmbhxccEDnFkJBmITvQmXq~Rug~Z0dyo")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adventureworksadls0.dfs.core.windows.net", "https://login.microsoftonline.com/26a6638c-cdb1-4861-8638-bb12bcd55165/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1- Read Table : Address

# COMMAND ----------

df_address = spark.read.format("delta")\
        .option("header", "True")\
        .option("inferSchema", "True")\
        .load("abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/Address")

display(df_address)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Transformed Address Table Data to Gold Layer/ SalesLT/ Address/ Address.delta

# COMMAND ----------

df_address.write.format('delta')\
            .mode('append')\
            .option("path", "abfss://gold@adventureworksadls0.dfs.core.windows.net/SalesLT/Address")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2- Read Table : Customer

# COMMAND ----------

df_customer = spark.read.format("delta")\
                            .option("header", "True")\
                            .option("inferSchema", "True")\
                            .load("abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/Customer")

display(df_customer)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Transformed Customer Table Data to Gold Layer/ SalesLT/ Customer/ Customer.delta

# COMMAND ----------

df_customer.write.format('delta')\
            .mode('overwrite')\
            .option("path", "abfss://gold@adventureworksadls0.dfs.core.windows.net/SalesLT/Customer")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3- Read Table : CustomerAddress

# COMMAND ----------

df_CustomerAddress = spark.read.format("delta")\
                            .option("header", "True")\
                            .option("inferSchema", "True")\
                            .load("abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/CustomerAddress")

display(df_CustomerAddress)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Transformed CustomerAddress Table Data to Gold Layer/ SalesLT/ CustomerAddress/ CustomerAddress.delta

# COMMAND ----------

df_CustomerAddress.write.format('delta')\
                    .mode('overwrite')\
                    .option("path", "abfss://gold@adventureworksadls0.dfs.core.windows.net/SalesLT/CustomerAddress")\
                    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4- Read Table : Product

# COMMAND ----------

df_product = spark.read.format("delta")\
                    .option("header", "True")\
                    .option("inferSchema", "True")\
                    .load("abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/Product")

display(df_product)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Transformed Product Table Data to Gold Layer/ SalesLT/ Product/ Product.delta

# COMMAND ----------

df_product.write.format('delta')\
            .mode('overwrite')\
            .option("path", "abfss://gold@adventureworksadls0.dfs.core.windows.net/SalesLT/Product")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5- Read Table : ProductCategory

# COMMAND ----------

df_ProductCategory = spark.read.format("delta")\
                            .option("header", "True")\
                            .option("inferSchema", "True")\
                            .load("abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/ProductCategory")

display(df_ProductCategory)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Transformed ProductCategory Table Data to Gold Layer/ SalesLT/ ProductCategory/ ProductCategory.delta
# MAGIC
# MAGIC

# COMMAND ----------

df_ProductCategory.write.format('delta')\
                    .mode('overwrite')\
                    .option("path", "abfss://gold@adventureworksadls0.dfs.core.windows.net/SalesLT/ProductCategory")\
                    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 - Read Table : ProductDescription

# COMMAND ----------

df_prod_desc = spark.read.format("delta")\
                    .option("header", "True")\
                    .option("inferSchema", "True")\
                    .load("abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/ProductDescription")

display(df_prod_desc)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Transformed ProductDescription Table Data to Gold Layer/ SalesLT/ ProductDescription/ProductDescription.delta

# COMMAND ----------

df_prod_desc.write.format('delta')\
                .mode('overwrite')\
                .option("path", "abfss://gold@adventureworksadls0.dfs.core.windows.net/SalesLT/ProductDescription")\
                .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 - Read Table : ProductModel

# COMMAND ----------

df_prod_model = spark.read.format("delta")\
                       .option("header", "True")\
                        .option("inferSchema", "True")\
                        .load("abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/ProductModel")

display(df_prod_model)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Transformed ProductModel Table Data to Gold Layer/ SalesLT/ ProductModel/ ProductModel.delta

# COMMAND ----------

df_prod_model.write.format('delta')\
                 .mode('overwrite')\
                 .option("path", "abfss://gold@adventureworksadls0.dfs.core.windows.net/SalesLT/ProductModel")\
                .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8- Read Table : ProductModelProductDescription

# COMMAND ----------

df_prod_mode_prod_desc = spark.read.format("delta")\
                                .option("header", "True")\
                                .option("inferSchema", "True")\
                                .load("abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/ProductModelProductDescription")

display(df_prod_mode_prod_desc)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Transformed ProductModelProductDescription Table Data to Gold Layer/ SalesLT/ ProductModelProductDescription/ ProductModelProductDescription.delta
# MAGIC

# COMMAND ----------

df_prod_mode_prod_desc.write.format('delta')\
                         .mode('overwrite')\
                        .option("path", "abfss://gold@adventureworksadls0.dfs.core.windows.net/SalesLT/ProductModelProductDescription")\
                        .save()

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## 9- Read Table : SalesOrderDetail

# COMMAND ----------

df_sales_order_details = spark.read.format("delta")\
                            .option("header", "True")\
                            .option("inferSchema", "True")\
                            .load("abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/SalesOrderDetail")

display(df_sales_order_details)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Transformed SalesOrderDetail Table Data to Gold Layer/ SalesLT/ SalesOrderDetail/ SalesOrderDetail.delta

# COMMAND ----------

df_sales_order_details.write.format('delta')\
                         .mode('overwrite')\
                        .option("path", "abfss://gold@adventureworksadls0.dfs.core.windows.net/SalesLT/SalesOrderDetail")\
                        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10- Read Table : SalesOrderHeader

# COMMAND ----------

df_sales_order_header = spark.read.format("delta")\
                                .option("header", "True")\
                                .option("inferSchema", "True") \
                                .load("abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/SalesOrderHeader")

display(df_sales_order_header)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Transformed SalesOrderHeader Table Data to Gold Layer/ SalesLT/ SalesOrderHeader/ SalesOrderHeader.delta
# MAGIC

# COMMAND ----------

df_sales_order_header.write.format('delta')\
                         .mode('overwrite')\
                         .option("path", "abfss://gold@adventureworksadls0.dfs.core.windows.net/SalesLT/SalesOrderHeader")\
                         .save()

# COMMAND ----------

# MAGIC %md
# MAGIC # GOLD TRANSFORMATIONS

# COMMAND ----------

# %md
#  Understanding Gold Layer Requirements
#  The gold layer should contain:

#  Dimension Tables: Master/reference data that provides context (slow-changing)

#  Fact Tables: Transactional data with business metrics (fast-changing)

#  Aggregate Tables: Pre-computed summaries for performance

#  Bridge Tables: For many-to-many relationships (if needed)

# COMMAND ----------

# %md
#  Star Schema Design:

#  Dimension Tables (Conformed Dimensions):

#  DimCustomer (from Customer, CustomerAddress, Address tables)

#  DimProduct (from Product, ProductCategory, ProductModel tables)

#  DimDate (generate date dimension)

#  Fact Tables:

#  FactSales (from SalesOrderHeader, SalesOrderDetail tables)

#  Aggregate Tables:

#  SalesSummaryByCustomer (customer performance metrics)

#  SalesSummaryByProduct (product performance metrics)

#  SalesTrendsByTime (time-based aggregations)


# COMMAND ----------

# MAGIC %md
# MAGIC # Dimensions

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 - DimCustomer

# COMMAND ----------

# %md
#  DimCustomer : Transforming for Gold Layer in PySpark

#  Create the DimCustomer dimension table from the Silver Layer tables:

#  1 - Silver.Customer

#  2 - Silver.CustomerAddress

#  3 - Silver.Address

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 : Load Data from Silver Layer

# COMMAND ----------

silver = "abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/"

# COMMAND ----------

df_customer = spark.read.format("delta").load("abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/Customer")
df_customer_address = spark.read.format("delta").load("abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/CustomerAddress")
df_address = spark.read.format("delta").load("abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/Address")


# COMMAND ----------

display(df_customer)
display(df_customer_address)
display(df_address)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 : Transformation : Join Tables to Create a Denormalized DimCustomer

# COMMAND ----------

df_dim_customer = df_customer \
    .join(df_customer_address, "CustomerID", "left") \
    .join(df_address, "AddressID", "left") \
    .select(
        df_customer["CustomerID"].alias("customer_key"),  # Surrogate Key
        df_customer["CompanyName"].alias("company_name"),
        df_customer["FullName"].alias("full_name"),
        df_customer["Phone"].alias("phone"),
        df_customer["EmailAddress"].alias("email"),
        df_address["Address"].alias("address"),
        df_address["City"].alias("city"),
        df_address["State"].alias("state"),
        df_address["Country"].alias("country"),
        df_address["PostalCode"].alias("postal_code"),
        df_customer["ModifiedDate"].alias("customer_modified_date")
    )


# COMMAND ----------

# %md
#  Explanation:

# 1 - Joins Customer with CustomerAddress using CustomerID.

# 2 - Joins the result with Address using AddressID.

# 3 - Selects only necessary columns for analytics.

# 4 - Renames CustomerID to CustomerKey (surrogate key).

# COMMAND ----------

display(df_dim_customer)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 : Generate Surrogate Key for Customer

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

df_dim_customer = df_dim_customer.withColumn("customer_key", monotonically_increasing_id())

# COMMAND ----------

# %md
# Explanation

# 1 - customer_key is a surrogate key used in fact tables.

# 2 - Avoids using primary keys from source systems.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Handle Missing & Null Values

# COMMAND ----------

df_dim_customer = df_dim_customer.fillna({
    "company_name": "Unknown",
    "phone": "Not Available",
    "email": "Not Provided",
    "city": "Unknown",
    "state": "Unknown",
    "country": "Unknown"
})

display(df_dim_customer)

# COMMAND ----------

# %md
# Why?

# 1 - Ensures data completeness in Gold Layer.

# 2 - Fills missing values with default placeholders.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Format Date Columns
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import to_date

df_dim_customer = df_dim_customer.withColumn("customer_modified_date", to_date("customer_modified_date", "yyyy-MM-dd"))

display(df_dim_customer)

# COMMAND ----------

# %md

# Why?

# Standardizes date formats for reporting.



# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Store DimCustomer as a Delta Table in the Gold Layer

# COMMAND ----------

df_dim_customer.write.format("delta")\
                .mode("overwrite")\
                .option("path", "abfss://gold@adventureworksadls0.dfs.core.windows.net/SalesLT/DimCustomer")\
                .save()

# COMMAND ----------

# MAGIC %md
# MAGIC # Dimensions

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 - DimProduct

# COMMAND ----------

# %md
# Transforming DimProduct for Gold Layer in PySpark

# Create the `DimProduct`` dimension table from the Silver Layer tables:

# 1 - Silver.Product

# 2 - Silver.ProductCategory

# 3 - Silver.ProductModel

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 : Load Data from Silver Layer

# COMMAND ----------

df_product = spark.read.format("delta").load("abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/Product")
df_product_category = spark.read.format("delta").load("abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/ProductCategory")
df_product_model = spark.read.format("delta").load("abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/ProductModel")

display(df_product)
display(df_product_category)
display(df_product_model)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Join Tables to Create a Denormalized DimProduct

# COMMAND ----------

df_dim_product = df_product \
    .join(df_product_category, "ProductCategoryID", "left") \
    .join(df_product_model, "ProductModelID", "left") \
    .select(
        df_product["ProductID"].alias("product_key"),  # Surrogate Key
        df_product["Name"].alias("product_name"),
        df_product_category["Name"].alias("category_name"),
        df_product_category["ProductCategoryID"].alias("product_category_key"),
        df_product_category["ParentProductCategoryID"].alias("parent_product_category_key"),
        df_product_category["ModifiedDate"].alias("product_category_modified_date"),
        df_product_model["Name"].alias("model_name"),
        df_product_model["ModifiedDate"].alias("product_model_modified_date"),
        df_product["ProductNumber"].alias("product_number"),
        df_product["Color"].alias("color"),
        df_product["Size"].alias("size"),
        df_product["StandardCost"].alias("standard_cost"),
        df_product["ListPrice"].alias("list_price"),
        df_product["Weight"].alias("weight"),
        df_product["SellStartDate"].alias("sell_start_date"),
        df_product["ModifiedDate"].alias("product_modified_date")
    )


# COMMAND ----------

display(df_dim_product)

# COMMAND ----------

# %md
# Explanation:

# Joins Product with ProductCategory using ProductCategoryID.

# Joins the result with ProductModel using ProductModelID.

# Selects only necessary columns for analytics.

# Renames ProductID to ProductKey (used in Fact tables).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Generate Surrogate Key for Product

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

df_dim_product = df_dim_product.withColumn("product_key", monotonically_increasing_id())

# COMMAND ----------

# %md

# Why?

# ProductKey is a surrogate key for fact tables.

# Helps maintain data integrity.



# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Handle Missing & Null Values

# COMMAND ----------

df_dim_product = df_dim_product.fillna({
    "category_name": "Unknown",
    "model_name": "Unknown",
    "color": "Not Specified",
    "size": "Not Specified",
    "weight": 0.0,
    "sell_start_date": "1900-01-01",
})


# COMMAND ----------

display(df_dim_product)

# COMMAND ----------

# Why?

# Ensures data completeness in the Gold Layer.

# Uses default values for missing data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Format Date Columns

# COMMAND ----------

from pyspark.sql.functions import to_date

df_dim_product = df_dim_product \
    .withColumn("sell_start_date", to_date("sell_start_date", "yyyy-MM-dd")) \
    .withColumn("product_modified_date", to_date("product_modified_date", "yyyy-MM-dd"))\
    .withColumn("product_category_modified_date", to_date("product_category_modified_date", "yyyy-MM-dd"))\
    .withColumn("product_model_modified_date", to_date("product_model_modified_date", "yyyy-MM-dd"))

# COMMAND ----------

# Why?

# Standardizes date formats for better analytics.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Store as a Delta Table in the Gold Layer

# COMMAND ----------

df_dim_product.write.format("delta")\
                 .mode("overwrite")\
                 .option("path", "abfss://gold@adventureworksadls0.dfs.core.windows.net/SalesLT/DimProduct")\
                 .save()

# COMMAND ----------

# MAGIC %md
# MAGIC # Facts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Sales

# COMMAND ----------

# Creating FactSales Fact Table for Gold Layer in PySpark

# Create the FactSales fact table from the Silver Layer tables:

# Silver.SalesOrderHeader

# Silver.SalesOrderDetail

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Data from Silver Layer

# COMMAND ----------

df_sales_order_header = spark.read.format("delta").load("abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/SalesOrderHeader")
df_sales_order_detail = spark.read.format("delta").load("abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/SalesOrderDetail")

display(df_sales_order_header)
display(df_sales_order_detail)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Join Tables to Create a Denormalized FactSales

# COMMAND ----------

df_fact_sales = df_sales_order_detail \
    .join(df_sales_order_header, "SalesOrderID", "inner") \
    .select(
        df_sales_order_detail["SalesOrderID"].alias("sales_order_key"),
        df_sales_order_detail["SalesOrderDetailID"].alias("sales_order_detail_key"),
        df_sales_order_header["CustomerID"].alias("customer_key"),
        df_sales_order_detail["ProductID"].alias("product_key"),
        df_sales_order_header["OrderDate"].alias("order_date"),
        df_sales_order_header["ShipDate"].alias("ship_date"),
        df_sales_order_header["DueDate"].alias("due_date"),
        df_sales_order_detail["OrderQty"].alias("order_quantity"),
        df_sales_order_detail["UnitPrice"].alias("unit_price"),
        df_sales_order_detail["UnitPriceDiscount"].alias("unit_price_discount"),
        df_sales_order_detail["LineTotal"].alias("gross_amount"),
        df_sales_order_header["SubTotal"].alias("total_amount"),
        df_sales_order_header["TaxAmt"].alias("tax_amount"),
        df_sales_order_header["Freight"].alias("freight_amount"),
        df_sales_order_header["TotalDue"].alias("total_due"),
        df_sales_order_header["ModifiedDate"].alias("sales_order_modified_date"),
    )


# COMMAND ----------

display(df_fact_sales)

# COMMAND ----------

# Explanation:

# Joins SalesOrderDetail with SalesOrderHeader using SalesOrderID.

# Selects only necessary columns for analytics.

# Uses CustomerID and ProductID as foreign keys linking to DimCustomer and DimProduct.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Handle Missing & Null Values

# COMMAND ----------

df_fact_sales = df_fact_sales.fillna({
    "ship_date": "1900-01-01",
    "due_date": "1900-01-01",
    "unit_price_discount": 0.0,
    "freight_amount": 0.0,
    "tax_amount": 0.0
})


# COMMAND ----------

# Why?

# Ensures data completeness in the Gold Layer.

# Uses default values for missing data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Format Date Columns

# COMMAND ----------

from pyspark.sql.functions import to_date

df_fact_sales = df_fact_sales \
    .withColumn("order_date", to_date("order_date", "yyyy-MM-dd")) \
    .withColumn("ship_date", to_date("ship_date", "yyyy-MM-dd")) \
    .withColumn("due_date", to_date("due_date", "yyyy-MM-dd")) \
    .withColumn("modified_date", to_date("sales_order_modified_date", "yyyy-MM-dd"))


# COMMAND ----------

# Why?

# Standardizes date formats for better analytics.


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Store as a Delta Table in the Gold Layer

# COMMAND ----------

df_fact_sales.write.format("delta")\
                .mode("overwrite")\
                .option("path", "abfss://gold@adventureworksadls0.dfs.core.windows.net/SalesLT/FactSales")\
                .save()

# COMMAND ----------

# MAGIC %md
# MAGIC # FactSalesAggregation

# COMMAND ----------

# Creating FactSalesAgg (Aggregated Fact Table) for Gold Layer in PySpark
# The FactSalesAgg table is an aggregated version of FactSales, optimized for faster analytics and reporting. It will summarize sales data at the Customer and Product level.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Data from Gold Layer (FactSales)

# COMMAND ----------

df_fact_sales = spark.read.format("delta")\
                         .load("abfss://gold@adventureworksadls0.dfs.core.windows.net/SalesLT/FactSales")

display(df_fact_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Perform Aggregations
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import sum, avg, count, round

df_fact_sales_agg = df_fact_sales.groupBy("customer_key", "product_key") \
    .agg(
        count("sales_order_key").alias("total_orders"),
        sum("order_quantity").alias("total_quantity"),
        sum("gross_amount").alias("total_sales_amount"),
        sum("tax_amount").alias("total_tax"),
        sum("freight_amount").alias("total_freight_amount"),
        round(avg("unit_price"), 2).alias("avg_unit_price"),
        round(avg("unit_price_discount"), 2).alias("avg_discount")
    )


# COMMAND ----------

# Explanation:

# Groups data by CustomerKey and ProductKey.

# Calculates:
# ✔ TotalOrders → Number of orders per customer-product
# ✔ TotalQuantity → Total units ordered
# ✔ TotalSalesAmount → Total revenue generated
# ✔ TotalTax → Total tax paid
# ✔ TotalFreight → Total shipping cost
# ✔ AvgUnitPrice → Average selling price per unit
# ✔ AvgDiscount → Average discount applied

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Optimize for Query Performance

# COMMAND ----------

df_fact_sales_agg = df_fact_sales_agg.repartition("customer_key")


# COMMAND ----------

# Why?

# Partitioning by CustomerKey ensures faster queries.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Store as a Delta Table in the Gold Layer

# COMMAND ----------

df_fact_sales_agg.write.format("delta")\
                    .mode("overwrite")\
                    .option("path", "abfss://gold@adventureworksadls0.dfs.core.windows.net/SalesLT/FactSalesAggregation")\
                    .save()

# COMMAND ----------

# What NOT to Load to Gold Layer:
    
# Raw transaction tables - These should remain in silver

# Intermediate tables - Used only for transformation pipelines

# Staging tables - Temporary tables used during ETL

# Audit/log tables - Should remain in silver or a separate audit layer

# Gold Layer Transformation Best Practices
# Business Naming Conventions:

# Use clear business terms (e.g., "net_sales" instead of "amt_net")

# Consistent naming across all tables

# Data Enrichment:

# Add calculated business metrics

# Create derived dimensions (e.g., customer segments)

# Add time intelligence (YTD, QTD, MTD flags)

# Data Quality:

# Add data quality checks as columns

# Implement surrogate keys

# Handle NULLs consistently

# Performance Optimization:

# Proper partitioning (typically by date)

# Z-ordering on frequently filtered columns

# Materialized aggregates for common queries

# SCD Handling:

# Type 2 for slowly changing dimensions

# Effective/expiration dates for historical tracking

# Implementation Roadmap
# First Load:

# Create dimension tables

# Create fact tables

# Generate date dimension

# Incremental Loads:

# Identify new/changed records in silver

# Apply SCD logic to dimensions

# Append new facts with proper keys

# Aggregates:

# Build after initial load

# Refresh based on business needs

# Optimization:

# Analyze query patterns

# Adjust partitioning and indexing

# Vacuum and optimize regularly

# COMMAND ----------

