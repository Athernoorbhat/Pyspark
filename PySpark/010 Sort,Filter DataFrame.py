# Databricks notebook source
df=(spark.read.format("csv") 
        .option("path","/FileStore/tables/014_Data-1.csv") 
        .option("header","True") 
        .option("inferSchema","True") 
        .load())
display(df)

# COMMAND ----------

df_sort=df.sort("ProductKey") #ascending order
display(df_sort)

# COMMAND ----------

df_sort2=df.sort("Color","SalesAmount") #Ist sort color then sales sub sorted
display(df_sort2)

# COMMAND ----------

from pyspark.sql.functions import col


# COMMAND ----------

df2=df.sort(col("ProductKey").desc()) #descending order
display(df2)

# COMMAND ----------

df3=df.sort(col("Color").asc(),col("SalesAmount").desc()) #color asc then amount desc
display(df3)

# COMMAND ----------

 #FILTER
display(df)

# COMMAND ----------

df4=df.filter("ProductKey==310")
display(df4)

# COMMAND ----------

df6=df.filter("Country=='India'")
display(df6)

# COMMAND ----------

df7=df.filter("ProductKey!=310")
display(df7)

# COMMAND ----------

df8=df.filter("SalesAmount > 3000")
display(df8)

# COMMAND ----------

df9=df.filter(col("ProductKey")==310)
display(df9)

# COMMAND ----------

df10=df.filter(col("Country")=='India')
display(df10)

# COMMAND ----------

df11=df.filter(col("SalesAmount").between(3000,5000))
display(df11)

# COMMAND ----------

df12=df.filter("Color == 'Red' OR Color == 'Black'")
display(df12)

# COMMAND ----------

df13=df.filter("Color == 'Red' AND SalesAmount > 3000")
display(df13)

# COMMAND ----------

