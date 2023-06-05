# Databricks notebook source
df=(spark.read.format("csv") 
        .option("path","/FileStore/tables/Products.csv") 
        .option("header","True") 
        .option("inferSchema","True") 
        .load())

# COMMAND ----------

display(df)

# COMMAND ----------

df1=df.select("OrderDate","Country","EnglishProductName")

# COMMAND ----------

display(df1)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df2=df.select(col("OrderDate"),col("Country"),col("EnglishProductName"))

# COMMAND ----------

display(df2)

# COMMAND ----------

df3=df.selectExpr("OrderDate","Country","EnglishProductName")  #same as select but if we need to add 1 more column with expression we can add
display(df3)

# COMMAND ----------

df4=df.selectExpr("OrderDate","Country","SalesAmount") 
display(df4)

# COMMAND ----------

df5=df.selectExpr("OrderDate","Country","SalesAmount * 0.05 AS Tax ") #this cant be done via select
display(df5)

# COMMAND ----------

#if we want to use select as selectexpr
from pyspark.sql.functions import expr

df6=df5=df.select("OrderDate","Country","SalesAmount",expr("SalesAmount * 0.05 AS Tax "))
display(df6)

# COMMAND ----------

from pyspark.sql.functions import concat
df7 = df.selectExpr("OrderDate", "Country", "(SalesAmount * 0.05) AS Tax", "CONCAT(Country, '-ABC') AS NewCountry")

display(df7)

# COMMAND ----------

