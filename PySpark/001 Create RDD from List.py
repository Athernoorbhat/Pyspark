# Databricks notebook source
 lst=[1,2,3,4,5,6,7,8,9,10]

# COMMAND ----------

print(lst)

# COMMAND ----------

type(lst)

# COMMAND ----------

sparksc=spark.sparkContext
#spark context is the entry point spark.+ctrl+Spc

# COMMAND ----------

type(sparksc)

# COMMAND ----------

rdd=sparksc.parallelize(lst )
#parallelize is used to create an RDD(immutab) from a list collection

# COMMAND ----------

type(rdd)

# COMMAND ----------

rdd.collect()
#collect is an action to view o/p

# COMMAND ----------

rdd2=sc.parallelize(lst)

# COMMAND ----------

rdd2.collect()

# COMMAND ----------

rdd3=sc.parallelize([1,2,3,4,5])
rdd3.collect()

# COMMAND ----------

