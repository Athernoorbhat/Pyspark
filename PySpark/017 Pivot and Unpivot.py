# Databricks notebook source
df=(spark.read.format("csv") 
        .option("path","/FileStore/tables/014_Data-2.csv") 
        .option("header","True") 
        .option("inferSchema","True") 
        .load())
display(df)

# COMMAND ----------

df_gb=df.groupBy("ProductName").sum("SalesAmount")
display(df_gb)

# COMMAND ----------

df_piv=df.groupBy("ProductName").pivot("Country").sum("SalesAmount")
display(df_piv)

# COMMAND ----------

df_piv2=df.groupBy("ProductKey","ProductName").pivot("Country").sum("SalesAmount")
display(df_piv2)

# COMMAND ----------

#UNPIVOT
df_unpiv=df_piv.unpivot("ProductName",["Australia","Canada"],"Country","Sales")
display(df_unpiv)

# COMMAND ----------

df_unpiv2=df_piv2.unpivot(["ProductKey","ProductName"],["Australia","Canada"],"Country","Sales")
display(df_unpiv2)

# COMMAND ----------

