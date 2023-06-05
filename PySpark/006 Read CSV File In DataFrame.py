# Databricks notebook source
df=spark.read.csv("/FileStore/tables/Data.csv") 

# COMMAND ----------

df.display() #header not pushed --> becomes row 

# COMMAND ----------

df2=spark.read.csv("/FileStore/tables/Data.csv",header=True) #header sahe ha ab
display(df2)

# COMMAND ----------

df2.printSchema() #all columns string

# COMMAND ----------

df2.dtypes #list of data types

# COMMAND ----------

df3=spark.read.csv("/FileStore/tables/Data.csv",header=True,inferSchema=True) #2 spark job run huwai 
display(df3)

# COMMAND ----------

df3.printSchema() #data types theek ha

# COMMAND ----------

 df4=spark.read.format("csv") \
        .option("path","/FileStore/tables/Data.csv") \
        .option("header","True") \
        .option("inferSchema","True") \
        .load()

# COMMAND ----------

 df5=(spark.read.format("csv") 
        .option("path","/FileStore/tables/Data.csv") 
        .option("header","True") 
        .option("inferSchema","True") 
        .load())

# COMMAND ----------

display(df4)

# COMMAND ----------

df4.printSchema()

# COMMAND ----------

