# Databricks notebook source
sparksc=spark.sparkContext
rdd = sparksc.parallelize([(1,"India"),(2,"US"),(3,"UK"),(4,"Turkey")])
rdd.collect() 

# COMMAND ----------

rdd.count()

# COMMAND ----------

df=rdd.toDF()
df.collect()

# COMMAND ----------

df.show() 

# COMMAND ----------

df.show(2) #shows 2 rows

# COMMAND ----------

display(df)

# COMMAND ----------

myschema=["id","country"] #Giving column names

# COMMAND ----------

df2=rdd.toDF(myschema)
df2.show()

# COMMAND ----------

df2.printSchema() #datatype , coloumn name

# COMMAND ----------

#OTHER METHOD OF NAMING COLUMNS

# COMMAND ----------

df4=rdd.toDF("id integer,country string")
df4.show()

# COMMAND ----------

df4.printSchema()

# COMMAND ----------

#OTHER METHOD OF NAMING COLUMNS

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

mydfSchema=StructType([
            StructField("id",LongType(),True),
            StructField("country",StringType(),True)   
])

# COMMAND ----------

df5=rdd.toDF(mydfSchema)
df5.show()

# COMMAND ----------

#OTHER METHOD OF CREATING DATA FRAME

# COMMAND ----------

df6=spark.createDataFrame(rdd)
df6.show()

# COMMAND ----------

df6.printSchema()

# COMMAND ----------

df7=spark.createDataFrame(rdd,myschema) #myschema=["id","country"]
df7.show()

# COMMAND ----------

df8=spark.createDataFrame(rdd,mydfSchema) #from struck
df8.show()

# COMMAND ----------

df9=spark.createDataFrame(rdd,"id long,country string") #dirctly schema pass
df9.show()

# COMMAND ----------

#OTHER METHOD

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

newdf=spark.createDataFrame([
    Row(1,"India"),
     Row(2,"UK"),
     Row(3,"US"),
     Row(4,"Turk")
    
],myschema)
newdf.show()

# COMMAND ----------

newdf.columns

# COMMAND ----------

len(newdf.columns) #count of columns

# COMMAND ----------

