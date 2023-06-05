# Databricks notebook source
sparksc=spark.sparkContext
rdd=sparksc.textFile("/FileStore/tables/001_Wordcount-1.txt") #takes txt file path and gives rdd
rdd2=rdd.flatMap(lambda x: x.split(" ")) #operation that flattens the RDD/DataFrame
rdd3=rdd2.map(lambda x:(x,1)) #key value pair creation
rdd4=rdd3.reduceByKey(lambda x,y:x+y) ##perform data aggregation
rdd4.collect()

# COMMAND ----------

rdd4.saveAsTextFile("/FileStore/tables/PysparkFiles") #hdfs mai store -->data>dbfs>filestore>tables>PysparlFiles>_SUCESS

# COMMAND ----------

display(spark.read.text("/FileStore/tables/PysparkFiles"))

# COMMAND ----------

