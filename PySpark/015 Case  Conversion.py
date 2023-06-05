# Databricks notebook source
df=(spark.read.format("csv") 
        .option("path","/FileStore/tables/014_Data-2.csv") 
        .option("header","True") 
        .option("inferSchema","True") 
        .load())
display(df)

# COMMAND ----------

from pyspark.sql.functions import col,lower,upper,initcap

# COMMAND ----------

df_lower=df.withColumn("ProductName",lower(col("ProductName")))
display(df_lower)

# COMMAND ----------

df_upper=df.withColumn("ProductName",upper(col("ProductName")))
display(df_upper)

# COMMAND ----------

df_init=df_upper.withColumn("ProductName",initcap(col("ProductName")))
display(df_init)

# COMMAND ----------

df.show(5)

# COMMAND ----------

df.take(5)

# COMMAND ----------

x=df.take(5)
display(x)
type(x) #list

# COMMAND ----------

rdd=sc.parallelize(x)
df_new=rdd.toDF()

# COMMAND ----------

df_new.head(4)

# COMMAND ----------

df_new.first()

# COMMAND ----------

df.tail(5)

# COMMAND ----------

