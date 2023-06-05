# Databricks notebook source
sparksc=spark.sparkContext
rdd=sparksc.textFile("/FileStore/tables/001_Wordcount.txt") 
#takes txt file path and gives rdd
rdd.collect()
#gives the  list collection each line an item

# COMMAND ----------

#WORD COUNT PROBLEM

# COMMAND ----------

rdd2=rdd.flatMap(lambda x:x.split(" "))
#function name = lambda arguments : expression
#flatMap is an tanformation which take som e input and 
#operation that flattens the RDD/DataFrame
rdd2.collect()

# COMMAND ----------

rdd3=rdd2.map(lambda x: (x,1))
rdd3.collect() 

# COMMAND ----------

rdd4=rdd3.reduceByKey(lambda x,y:x+y)
#perform data aggregation
rdd4.collect()

# COMMAND ----------

