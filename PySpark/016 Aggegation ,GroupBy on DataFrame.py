# Databricks notebook source
df=(spark.read.format("csv") 
        .option("path","/FileStore/tables/014_Data-2.csv") 
        .option("header","True") 
        .option("inferSchema","True") 
        .load())
display(df)

# COMMAND ----------

df1=df.select("SalesAmount")
display(df1)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_2=df.select(sum("SalesAmount"))
display(df_2) #naya column name sum("SalesAmount")

# COMMAND ----------

df_sum=df.select(sum("SalesAmount").alias("TotalSales"))
display(df_sum)

# COMMAND ----------

df_avg=df.select(avg("SalesAmount").alias("AvgSales"))
display(df_avg)

# COMMAND ----------

df_cnt_dist=df.select(countDistinct("Country"))
display(df_cnt_dist) #distinct contries ka count

# COMMAND ----------

df_sum_avg=df.select(sum("SalesAmount").alias("TotalSales"),avg("SalesAmount").alias("AvgSales"))
display(df_sum_avg)

# COMMAND ----------

#OR



df_expr=df.selectExpr("sum(SalesAmount) AS TotalSales")
display(df_expr)

# COMMAND ----------

#GROUPBY AGGREGATIONS
df_gb=df.groupBy("Country").sum("SalesAmount")
display(df_gb)

# COMMAND ----------

df_gb2=df.groupBy("Country").sum("SalesAmount","TaxAmt")
display(df_gb2)

# COMMAND ----------

df_gb3=df.groupBy("Country","ProductName").sum("SalesAmount")
display(df_gb3)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_gb_new=df.groupBy("Country").agg(sum("SalesAmount"))
display(df_gb_new)

# COMMAND ----------

df_gb_new1=df.groupBy("Country").agg(sum("SalesAmount").alias("TotalSales"))
display(df_gb_new1)

# COMMAND ----------

 