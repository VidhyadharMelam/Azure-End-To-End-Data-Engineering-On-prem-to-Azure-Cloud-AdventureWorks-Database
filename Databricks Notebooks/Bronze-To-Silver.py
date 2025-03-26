# Databricks notebook source
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
# MAGIC ## Data Reading - Bronze

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

df_address = spark.read.format("parquet")\
        .option("header", "True")\
        .option("inferSchema", "True")\
        .load("abfss://bronze@adventureworksadls0.dfs.core.windows.net/SalesLT/Address")

display(df_address)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformation 1 - Transform "ModifiedDate" Column and keep only YYYY-MM-DD Format and Delete the Irrelavant Timestamp

# COMMAND ----------

df_address = df_address.withColumn("ModifiedDate", to_date(df_address["ModifiedDate"]))

display (df_address)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformation 2 - Drop AddressLine2 Column

# COMMAND ----------

# Drop the AddressLine2 column

df_address = df_address.drop("AddressLine2")
display(df_address)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformation 3 - Rename : CountryRegion to Country, StateProvince to State, and AddressLine1 to Address

# COMMAND ----------

df_address = df_address.withColumnRenamed("CountryRegion", "Country")\
        .withColumnRenamed("StateProvince", "State") \
     .withColumnRenamed("AddressLine1", "Address")

display(df_address)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Transformed Address Table Data to Silver Layer/ SalesLT/ Address/ Address.delta

# COMMAND ----------

df_address.write.format('delta')\
            .mode('overwrite')\
            .option("path", "abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/Address")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2- Read Table : Customer

# COMMAND ----------

df_customer = spark.read.format("parquet")\
                            .option("header", "True")\
                            .option("inferSchema", "True")\
                            .load("abfss://bronze@adventureworksadls0.dfs.core.windows.net/SalesLT/Customer")

display(df_customer)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformation 1 - Transform "ModifiedDate" Column and keep only YYYY-MM-DD Format and Delete the Irrelavant Timestamp

# COMMAND ----------

df_customer = df_customer.withColumn("ModifiedDate", to_date(df_customer["ModifiedDate"]))

display (df_customer)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformation 2 - Drop Suffix Column

# COMMAND ----------

df_customer = df_customer.drop("Suffix")

display(df_customer)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformation 3 - Combine : FirstName, MiddleName, and LastName into a FullName column. If MiddleName is NULL, it should be skipped to avoid extra spaces.

# COMMAND ----------

# Combine FirstName, MiddleName, LastName into FullName (skipping null MiddleName)

df_customer = df_customer.withColumn("FullName", concat_ws(" ", "FirstName", "MiddleName", "LastName"))

display(df_customer)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformation 4 - Extracting relevant columns from a DataFrame.

# COMMAND ----------

df_customer = df_customer.select("CustomerID", "CompanyName", "SalesPerson", "EmailAddress", "Phone", "ModifiedDate", "FullName")

display(df_customer)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Transformed Customer Table Data to Silver Layer/ SalesLT/ Customer/ Customer.delta

# COMMAND ----------

df_customer.write.format('delta')\
            .mode('overwrite')\
            .option("path", "abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/Customer")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3- Read Table : CustomerAddress

# COMMAND ----------

df_CustomerAddress = spark.read.format("parquet")\
                            .option("header", "True")\
                            .option("inferSchema", "True")\
                            .load("abfss://bronze@adventureworksadls0.dfs.core.windows.net/SalesLT/CustomerAddress")

display(df_CustomerAddress)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformation 1 - Transform "ModifiedDate" Column and keep only YYYY-MM-DD Format and Delete the Irrelavant Timestamp

# COMMAND ----------

df_CustomerAddress = df_CustomerAddress.withColumn("ModifiedDate", to_date(df_CustomerAddress["ModifiedDate"]))

display(df_CustomerAddress)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformation 2 - Drop Column : rowguid

# COMMAND ----------

df_CustomerAddress = df_CustomerAddress.drop("rowguid")

display(df_CustomerAddress)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Transformed CustomerAddress Table Data to Silver Layer/ SalesLT/ CustomerAddress/ CustomerAddress.delta

# COMMAND ----------

df_CustomerAddress.write.format('delta')\
                    .mode('overwrite')\
                    .option("path", "abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/CustomerAddress")\
                    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4- Read Table : Product

# COMMAND ----------

df_product = spark.read.format("parquet")\
                    .option("header", "True")\
                    .option("inferSchema", "True")\
                    .load("abfss://bronze@adventureworksadls0.dfs.core.windows.net/SalesLT/Product")

display(df_product)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformation 1 - Transform "ModifiedDate", SellStartDate, Columns and keep only YYYY-MM-DD Format and Delete the Irrelavant Timestamp

# COMMAND ----------

df_product = df_product.withColumn("ModifiedDate", to_date(df_product["ModifiedDate"]))\
                        .withColumn("SellStartDate", to_date(df_product["SellStartDate"]))\
                        .withColumn("SellEndDate", to_date(df_product["SellEndDate"]))\

display(df_product)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformation 2 - Fill n/a Columns with Unknown

# COMMAND ----------

df_product = df_product.fillna(0, subset=["Weight"])\
                        .fillna("Unknown", subset=["Color"])\
                        .fillna("Unknown", subset=["Size"])\
                        .fillna("Unknown", subset=["SellEndDate"])

display(df_product)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformation 3 - Drop Column : SellEndDate, DiscontinuedDate, ThumbNailPhoto, ThumbnailPhotoFileName, rowguid

# COMMAND ----------

df_product = df_product.drop("SellEndDate", "DiscontinuedDate", "ThumbNailPhoto", "ThumbnailPhotoFileName", "rowguid")

display(df_product)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Transformed Product Table Data to Silver Layer/ SalesLT/ Product/ Product.delta
# MAGIC

# COMMAND ----------

df_product.write.format('delta')\
            .mode('overwrite')\
            .option("path", "abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/Product")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5- Read Table : ProductCategory

# COMMAND ----------

df_ProductCategory = spark.read.format("parquet")\
                            .option("header", "True")\
                            .option("inferSchema", "True")\
                            .load("abfss://bronze@adventureworksadls0.dfs.core.windows.net/SalesLT/ProductCategory")

display(df_ProductCategory)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC #### Transformation 1 - Transform "ModifiedDate" Column and keep only YYYY-MM-DD Format and Delete the Irrelavant Timestamp
# MAGIC

# COMMAND ----------

df_ProductCategory = df_ProductCategory.withColumn("ModifiedDate", to_date(df_ProductCategory["ModifiedDate"]))

display(df_ProductCategory)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformation 2 - Fill n/a Columns

# COMMAND ----------

df_ProductCategory = df_ProductCategory.fillna(0, subset=["ParentProductCategoryID"])

display(df_ProductCategory)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformation 3 - Drop Column : rowguid

# COMMAND ----------

df_ProductCategory = df_ProductCategory.drop("rowguid")

display(df_ProductCategory)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Transformed ProductCategory Table Data to Silver Layer/ SalesLT/ ProductCategory/ ProductCategory.delta
# MAGIC
# MAGIC

# COMMAND ----------

df_ProductCategory.write.format('delta')\
                    .mode('overwrite')\
                    .option("path", "abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/ProductCategory")\
                    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 - Read Table : ProductDescription

# COMMAND ----------

df_prod_desc = spark.read.format("parquet")\
                    .option("header", "True")\
                    .option("inferSchema", "True")\
                    .load("abfss://bronze@adventureworksadls0.dfs.core.windows.net/SalesLT/ProductDescription")

display(df_prod_desc)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC #### Transformation 1 - Transform "ModifiedDate" Column and keep only YYYY-MM-DD Format and Delete the Irrelavant Timestamp

# COMMAND ----------

df_prod_desc = df_prod_desc.withColumn("ModifiedDate", to_date(df_prod_desc["ModifiedDate"]))

display(df_prod_desc)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformation 2 - Drop Column : rowguid

# COMMAND ----------

df_prod_desc = df_prod_desc.drop("rowguid")

display(df_prod_desc)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Transformed ProductDescription Table Data to Silver Layer/ SalesLT/ ProductDescription/ProductDescription.delta

# COMMAND ----------

df_prod_desc.write.format('delta')\
                .mode('overwrite')\
                .option("path", "abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/ProductDescription")\
                .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 - Read Table : ProductModel

# COMMAND ----------

df_prod_model = spark.read.format("parquet")\
                       .option("header", "True")\
                        .option("inferSchema", "True")\
                        .load("abfss://bronze@adventureworksadls0.dfs.core.windows.net/SalesLT/ProductModel")

display(df_prod_model)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformation 1 - Transform "ModifiedDate" Column and keep only YYYY-MM-DD Format and Delete the Irrelavant Timestamp

# COMMAND ----------

df_prod_model = df_prod_model.withColumn("ModifiedDate", to_date(df_prod_model["ModifiedDate"]))

display(df_prod_model)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformation 2 - Drop Column : CatalogDescription and rowguid
# MAGIC
# MAGIC

# COMMAND ----------

df_prod_model = df_prod_model.drop("CatalogDescription", "rowguid")

display(df_prod_model)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Transformed ProductModel Table Data to Silver Layer/ SalesLT/ ProductModel/ ProductModel.delta
# MAGIC

# COMMAND ----------

df_prod_model.write.format('delta')\
                 .mode('overwrite')\
                 .option("path", "abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/ProductModel")\
                .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8- Read Table : ProductModelProductDescription

# COMMAND ----------

df_prod_mode_prod_desc = spark.read.format("parquet")\
                                .option("header", "True")\
                                .option("inferSchema", "True")\
                                .load("abfss://bronze@adventureworksadls0.dfs.core.windows.net/SalesLT/ProductModelProductDescription")

display(df_prod_mode_prod_desc)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformation 1 - Transform "ModifiedDate" Column and keep only YYYY-MM-DD Format and Delete the Irrelavant Timestamp
# MAGIC

# COMMAND ----------

df_prod_mode_prod_desc = df_prod_mode_prod_desc.withColumn("ModifiedDate", to_date(df_prod_mode_prod_desc["ModifiedDate"]))

display(df_prod_mode_prod_desc)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformation 2 - Drop Column : rowguid

# COMMAND ----------

df_prod_mode_prod_desc = df_prod_mode_prod_desc.drop("rowguid")

display(df_prod_mode_prod_desc)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Transformed ProductModelProductDescription Table Data to Silver Layer/ SalesLT/ ProductModelProductDescription/ ProductModelProductDescription.delta
# MAGIC

# COMMAND ----------

df_prod_mode_prod_desc.write.format('delta')\
                         .mode('overwrite')\
                        .option("path", "abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/ProductModelProductDescription")\
                        .save()

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## 9- Read Table : SalesOrderDetail

# COMMAND ----------

df_sales_order_details = spark.read.format("parquet")\
                            .option("header", "True")\
                            .option("inferSchema", "True")\
                            .load("abfss://bronze@adventureworksadls0.dfs.core.windows.net/SalesLT/SalesOrderDetail")

display(df_sales_order_details)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC #### Transformation 1 - Transform "ModifiedDate" Column and keep only YYYY-MM-DD Format and Delete the Irrelavant Timestamp

# COMMAND ----------

df_sales_order_details = df_sales_order_details.withColumn("ModifiedDate", to_date(df_sales_order_details["ModifiedDate"]))

display(df_sales_order_details)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformation 2 - Drop Column : rowguid

# COMMAND ----------

df_sales_order_details = df_sales_order_details.drop("rowguid")

display(df_sales_order_details)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Transformed SalesOrderDetail Table Data to Silver Layer/ SalesLT/ SalesOrderDetail/ SalesOrderDetail.delta

# COMMAND ----------

df_sales_order_details.write.format('delta')\
                         .mode('overwrite')\
                        .option("path", "abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/SalesOrderDetail")\
                        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10- Read Table : SalesOrderHeader

# COMMAND ----------

df_sales_order_header = spark.read.format("parquet")\
                                .option("header", "True")\
                                .option("inferSchema", "True") \
                                .load("abfss://bronze@adventureworksadls0.dfs.core.windows.net/SalesLT/SalesOrderHeader")

display(df_sales_order_header)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformation 1 - Transform "OrderDate", "DueDate", "ShipDate", "ModifiedDate" Column and keep only YYYY-MM-DD Format and Delete the Irrelavant Timestamp

# COMMAND ----------

df_sales_order_header = df_sales_order_header.withColumn("ModifiedDate", to_date(df_sales_order_header["ModifiedDate"]))\
                                             .withColumn("OrderDate", to_date(df_sales_order_header["OrderDate"]))\
                                            .withColumn("DueDate", to_date(df_sales_order_header["DueDate"]))\
                                            .withColumn("ShipDate", to_date(df_sales_order_header["ShipDate"]))

display(df_sales_order_header)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformation 2 - Drop Column : rowguid

# COMMAND ----------

df_sales_order_header = df_sales_order_header.drop("rowguid", "Comment", "CreditCardApprovalCode")

display(df_sales_order_header)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Transformed SalesOrderHeader Table Data to Silver Layer/ SalesLT/ SalesOrderHeader/ SalesOrderHeader.delta
# MAGIC

# COMMAND ----------

df_sales_order_header.write.format('delta')\
                         .mode('overwrite')\
                         .option("path", "abfss://silver@adventureworksadls0.dfs.core.windows.net/SalesLT/SalesOrderHeader")\
                         .save()