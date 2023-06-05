# Databricks notebook source
df_1=(spark.read.format("csv") 
        .option("path","/FileStore/tables/Employees-1.csv") 
        .option("header","True") 
        .option("inferSchema","True") 
        .load())
display(df_1)

# COMMAND ----------

#IN UNION COLUMN NO SHOULD BE SAME
df_union=df_1.union(df_1)
display(df_union)

# COMMAND ----------

df_2=(spark.read.format("csv") 
        .option("path","/FileStore/tables/EmployeesNew2.csv") 
        .option("header","True") 
        .option("inferSchema","True") 
        .load())
display(df_2)

# COMMAND ----------

#UNION FAILED
df_union2=df_1.union(df_2) #mixed columns 
display(df_union2)

# COMMAND ----------

df_union_name=df_1.unionByName(df_2) #mixed columns 
display(df_union_name)

# COMMAND ----------

