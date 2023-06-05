# Databricks notebook source
sparksc=spark.sparkContext
rdd=sparksc.textFile("/FileStore/tables/001_Wordcount-1.txt") #takes txt file path and gives rdd
rdd2=rdd.flatMap(lambda x: x.split(" ")) #operation that flattens the RDD/DataFrame
rdd3=rdd2.map(lambda x:(x,1)) #key value pair creation
rdd4=rdd3.reduceByKey(lambda x,y:x+y) ##perform data aggregation
rdd4.collect()

# COMMAND ----------

help(rdd4.sortByKey)

# COMMAND ----------

rdd5=rdd4.sortByKey() #asc = True
rdd5.collect()

# COMMAND ----------

rdd6=rdd4.sortByKey(False) #asc = False
rdd6.collect()

# COMMAND ----------

rdd7=rdd4.sortBy(lambda x: x[1]) #x[0]=key ,x[1]=value
rdd7.collect()

# COMMAND ----------

rdd8=rdd4.sortBy(lambda x: x[1], False) #x[0]=key ,x[1]=value ,desc=False
rdd8.collect()

# COMMAND ----------

x=rdd4.first() #gives us first record

# COMMAND ----------

type(x)

# COMMAND ----------

x[0]

# COMMAND ----------

x[1]

# COMMAND ----------

rdd8.take(3) #gives us 3 most occuring word

# COMMAND ----------

rdd7.take(3) #bottom 3

# COMMAND ----------

rddkeys=rdd4.keys() #gives keys only
rddkeys.collect()

# COMMAND ----------

rddVal=rdd4.values() #gives keys only
rddVal.collect()

# COMMAND ----------

#FILTER

# COMMAND ----------

rddfil=rdd4.filter(lambda x: x[1]>30)
rddfil.collect()

# COMMAND ----------

rddfil2=rdd4.filter(lambda x: x[1]<5)
rddfil2.collect()

# COMMAND ----------

#EVEN CHECK
rdd_fil_Even=rdd4.filter(lambda x: x[1] % 2 ==0)
rdd_fil_Even.collect()

# COMMAND ----------

#ODD CHECK
rdd_fil_Odd=rdd4.filter(lambda x: x[1] % 2 ==1)
rdd_fil_Odd.collect()

# COMMAND ----------

#KEY CHECK
rddfil3=rdd4.filter(lambda x: x[0].startswith("h"))
rddfil3.collect()

# COMMAND ----------

rddfil4=rdd4.filter(lambda x:"e" in x[0])
rddfil4.collect()

# COMMAND ----------

#FIND
rddfil5=rdd4.filter(lambda x:x[0].find("e")!= -1 )
rddfil5.collect()

# COMMAND ----------

