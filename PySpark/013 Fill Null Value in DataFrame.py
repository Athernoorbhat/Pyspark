# Databricks notebook source
df=(spark.read.format("csv") 
        .option("path","/FileStore/tables/EmployeesNull.csv") 
        .option("header","True") 
        .option("inferSchema","True") 
        .load())
display(df)

# COMMAND ----------

df_null=df.na.fill(0) #numeric column replacement
display(df_null)

# COMMAND ----------

df_null_2=df.na.fill("N/A")
display(df_null_2)

# COMMAND ----------

df1=df.na.fill("N/A").na.fill(0)
display(df1)

# COMMAND ----------

from pyspark.sql.functions import expr


# COMMAND ----------

df_new=df.withColumn("id",expr("coalesce(id,0)"))
display(df_new)

# COMMAND ----------

df_new2=df.withColumn("id",expr("coalesce(id,0)")).withColumn("name",expr("coalesce(name,'N/A')"))
display(df_new2)

# COMMAND ----------

