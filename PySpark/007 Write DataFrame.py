# Databricks notebook source
df=(spark.read.format("csv") 
        .option("path","/FileStore/tables/Data.csv") 
        .option("header","True") 
        .option("inferSchema","True") 
        .load())

# COMMAND ----------

df.show() #display(df)

# COMMAND ----------

df.write.format("csv").save("/FileStore/tables/pySparkNew")

# COMMAND ----------

df.write.format("parquet").save("/FileStore/tables/pySparkNew2")

# COMMAND ----------

df.write.format("csv").option("path","/FileStore/tables/pySparkNew3").save()

# COMMAND ----------

#Additionally save the file to same folder without getting error
df.write.format("csv").option("path","/FileStore/tables/pySparkNew3").mode("append").save()

# COMMAND ----------

#delete the previos file and save this
df.write.format("csv").option("path","/FileStore/tables/pySparkNew3").mode("overwrite").save()

# COMMAND ----------

#if path exist give error
df.write.format("csv").option("path","/FileStore/tables/pySparkNew3").mode("error").save()

# COMMAND ----------

#if path exist ignore
df.write.format("csv").option("path","/FileStore/tables/pySparkNew3").mode("ignore").save()

# COMMAND ----------

