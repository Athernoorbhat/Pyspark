# Databricks notebook source
df=(spark.read.format("csv") 
        .option("path","/FileStore/tables/EmployeesNew.csv") 
        .option("header","True") 
        .option("inferSchema","True") 
        .load())
display(df)

# COMMAND ----------

df_dist=df.distinct()
display(df_dist)

# COMMAND ----------

df_dupes=df.dropDuplicates(["city"]) #city mai jo dupes ha remove
display(df_dupes)

# COMMAND ----------

df_list_dist=df.select("city").distinct() #returns one column
display(df_list_dist)

# COMMAND ----------

