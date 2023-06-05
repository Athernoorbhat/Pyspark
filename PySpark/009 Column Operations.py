# Databricks notebook source
df=(spark.read.format("csv") 
        .option("path","/FileStore/tables/Data.csv") 
        .option("header","True") 
        .option("inferSchema","True") 
        .load())

# COMMAND ----------

#display(df)
df.printSchema()


# COMMAND ----------

from pyspark.sql.functions import lit

#lit--> literal 
#lit takes strings and returns column object

# COMMAND ----------


#ADD COLUMN
df1=df.withColumn("Country",lit("India")) #india sab rows mai


# COMMAND ----------

display(df1)

# COMMAND ----------

#ADD COLUMNS WITH SOME CALC
df2=df1.withColumn("Tax",lit("Sales*0.05")) #sarai row mai same data

# COMMAND ----------

display(df2)

# COMMAND ----------

from pyspark.sql.functions import col
df3=df1.withColumn("Tax",col("Sales")*0.05) 

# COMMAND ----------

display(df3)

# COMMAND ----------

#CHANGING DATA TYPE OF COLUMN
df4=df2.withColumn("ProductKey",col("ProductKey").cast("string"))

# COMMAND ----------

df4.printSchema()

# COMMAND ----------

df5=df2.withColumn("ProductKey",col("ProductKey").cast("string")).withColumn("Sales",col("Sales").cast("double"))
df5.printSchema()

# COMMAND ----------

#DROP COLUMNS
df6=df.drop("Sales","ProducName") #select mai expression likh saktai ha

# COMMAND ----------

display(df6)

# COMMAND ----------

#RENAME COLUMN
display(df)

# COMMAND ----------

df7=df.withColumnRenamed("ProductKey","ProductId")
#old Column ,New Column
display(df7)

# COMMAND ----------

df8=df.withColumnRenamed("ProductKey","ProductId").withColumnRenamed("ProducName","Prod Name")
display(df8)

# COMMAND ----------

df9=df.selectExpr("ProductKey AS ProdId") #selected column displayed problematic
display(df9)

# COMMAND ----------

df10=df.selectExpr("ProductKey AS ProdId","ProducName AS  ProdName","Sales") 
display(df10)

# COMMAND ----------

#CONVERTING COLUMNS NAME TO UPPER/LOWER CASE
df.columns

# COMMAND ----------

new_col_name=map(lambda x: x.upper(),df.columns)
print(new_col_name)

# COMMAND ----------

new_col_name2=list(map(lambda x: x.upper(),df.columns))
print(new_col_name2)

# COMMAND ----------

df11=df.toDF(*new_col_name2)
display(df11)

# COMMAND ----------

