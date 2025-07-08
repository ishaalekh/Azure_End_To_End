# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Script

# COMMAND ----------

# MAGIC %md
# MAGIC ### Accessing Data through the app

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.portfolioprojectdata.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.portfolioprojectdata.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.portfolioprojectdata.dfs.core.windows.net", "ca242c02-d863-4841-a7c9-e7f6d1c21654")
spark.conf.set("fs.azure.account.oauth2.client.secret.portfolioprojectdata.dfs.core.windows.net", "ec28Q~uD2hbOKbsC..jpdAdj6FZm9LaDvW2eeaBb")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.portfolioprojectdata.dfs.core.windows.net", "https://login.microsoftonline.com/2aeab805-d608-4ca7-b8bc-0323fbd04b00/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Loading
# MAGIC

# COMMAND ----------

df_Calender = spark.read.format('csv')\
.option('header',True)\
.option('inferSchema', True)\
.load("abfss://bronze@portfolioprojectdata.dfs.core.windows.net/Calendar")

# COMMAND ----------

df_Customers = spark.read.format('csv')\
.option('header',True)\
.option('inferSchema', True)\
.load("abfss://bronze@portfolioprojectdata.dfs.core.windows.net/Customers")

# COMMAND ----------

df_Product_Categories = spark.read.format('csv')\
.option('header',True)\
.option('inferSchema', True)\
.load("abfss://bronze@portfolioprojectdata.dfs.core.windows.net/Product_Categories")

# COMMAND ----------

df_Product_Subcategories = spark.read.format('csv')\
.option('header',True)\
.option('inferSchema', True)\
.load("abfss://bronze@portfolioprojectdata.dfs.core.windows.net/Product_Subcategories")

# COMMAND ----------

df_Products = spark.read.format('csv')\
.option('header',True)\
.option('inferSchema', True)\
.load("abfss://bronze@portfolioprojectdata.dfs.core.windows.net/Products")

# COMMAND ----------

df_Returns = spark.read.format('csv')\
.option('header',True)\
.option('inferSchema', True)\
.load("abfss://bronze@portfolioprojectdata.dfs.core.windows.net/Returns")

# COMMAND ----------

df_Sales = spark.read.format('csv')\
.option('header',True)\
.option('inferSchema', True)\
.load("abfss://bronze@portfolioprojectdata.dfs.core.windows.net/Sales*")

# COMMAND ----------

df_Territories = spark.read.format('csv')\
.option('header',True)\
.option('inferSchema', True)\
.load("abfss://bronze@portfolioprojectdata.dfs.core.windows.net/Territories")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation for Calender Table

# COMMAND ----------

df_Calender.display()

# COMMAND ----------

from pyspark.sql.functions import * 
df_Calender = df_Calender.withColumn('Year', year(col('Date')))\
                        .withColumn('Month', month(col('Date')))

# COMMAND ----------

df_Calender.display()

# COMMAND ----------

df_Calender.write.format('parquet')\
                .mode('append')\
                .option('path','abfss://silver@portfolioprojectdata.dfs.core.windows.net/Calendar')\
                .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation for Customers

# COMMAND ----------

df_Customers.display()

# COMMAND ----------

df_Customers.withColumn('Full Name', concat(col('Prefix'), lit(' '),  col('FirstName'), lit(' '), col('LastName'))).display()

# COMMAND ----------

df_Customers = df_Customers.withColumn('Full Name', concat_ws(' ', col('Prefix'), col('FirstName'), col('LastName')))

# COMMAND ----------

df_Customers.display()

# COMMAND ----------

df_Customers.write.format('parquet')\
            .mode('append')\
            .option('path','abfss://silver@portfolioprojectdata.dfs.core.windows.net/Customers')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation on Product Subcategories

# COMMAND ----------

df_Product_Subcategories.display()

# COMMAND ----------

df_Product_Subcategories.write.format('parquet')\
            .mode('append')\
            .option('path','abfss://silver@portfolioprojectdata.dfs.core.windows.net/Product_Subcategories')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation on Product

# COMMAND ----------

df_Products.display()

# COMMAND ----------

df_Products = df_Products.withColumn('ProductSKU', split(col('ProductSKU'),'-')[0])\
            .withColumn('ProductName', split(col('ProductName'),' ')[0])

# COMMAND ----------

df_Products.display()

# COMMAND ----------

df_Products.write.format('parquet')\
            .mode('append')\
            .option('path','abfss://silver@portfolioprojectdata.dfs.core.windows.net/Products')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### transformation on product categories
# MAGIC

# COMMAND ----------

df_Product_Categories.display()

# COMMAND ----------

df_Product_Categories.write.format('parquet')\
            .mode('append')\
            .option('path','abfss://silver@portfolioprojectdata.dfs.core.windows.net/Product_Categories')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### transformation on Returns

# COMMAND ----------

df_Returns.display()

# COMMAND ----------

df_Returns.write.format('parquet')\
            .mode('append')\
            .option('path','abfss://silver@portfolioprojectdata.dfs.core.windows.net/Returns')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### transformation on Territories

# COMMAND ----------

df_Territories.display()

# COMMAND ----------

df_Territories.write.format('parquet')\
            .mode('append')\
            .option('path','abfss://silver@portfolioprojectdata.dfs.core.windows.net/Territories')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### transformation on sales
# MAGIC

# COMMAND ----------

df_Sales.display()

# COMMAND ----------

df_Sales = df_Sales.withColumn('StockDate', to_timestamp('StockDate'))

# COMMAND ----------

df_Sales = df_Sales.withColumn('OrderNumber',regexp_replace(col('OrderNumber'),'S','T'))

# COMMAND ----------

df_Sales = df_Sales.withColumn('Multiply',col('OrderLineItem') * col('OrderQuantity'))

# COMMAND ----------

df_Sales.display()

# COMMAND ----------

df_Sales.write.format('parquet')\
            .mode('append')\
            .option('path','abfss://silver@portfolioprojectdata.dfs.core.windows.net/Sales')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales Analysis

# COMMAND ----------

df_Sales.groupBy('OrderDate').agg(count('OrderNumber').alias('OrderCount')).display()

# COMMAND ----------

df_Product_Categories.display()

# COMMAND ----------

df_Territories.display()

# COMMAND ----------

