# Databricks notebook source
df=(spark.read.format("csv") 
        .option("path","/FileStore/tables/014_Data-2.csv") 
        .option("header","True") 
        .option("inferSchema","True") 
        .load())
display(df)

# COMMAND ----------

df_fil=df.filter("ProductKey == 310")
display(df_fil)

# COMMAND ----------

#OR
df_fil2=df.where("ProductKey == 310")
display(df_fil2)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df1=df.where(col("ProductName").startswith("A"))
display(df1)

# COMMAND ----------

df2=df.where(col("ProductName").endswith("e"))
display(df2)

# COMMAND ----------

df3=df.where(col("ProductName").contains("a")) #bech mai 'a'
display(df3)

# COMMAND ----------

df4=df.where(col("ProductName").like("A%")) #start with A
display(df4)

# COMMAND ----------

df5=df.where(col("ProductName").like("%e")) #ends with 'e'
display(df5)

# COMMAND ----------

df6=df.where(col("ProductName").like("%a%")) #conatins 'a'
display(df6)

# COMMAND ----------

df7=df.where(col("ProductName").like("A_C%")) #conatins A_W
display(df7)

# COMMAND ----------

from pyspark.sql.functions import col , when

# COMMAND ----------

df_new_col=df.withColumn("Category",when(col("color")=="Red",1))
display(df_new_col)

# COMMAND ----------

df_new_col2=df.withColumn("Category",when(col("color")=="Red",1).otherwise(0))
display(df_new_col2)

# COMMAND ----------

df_new_col3=(df.withColumn("Category",when(col("color")=="Red",1)
                                    .when(col("color")=="Black",2)
                                      .otherwise(0)))
display(df_new_col3)

# COMMAND ----------

