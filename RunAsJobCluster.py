# Databricks notebook source
insurance_df = spark.read.format("csv") \
    .option("inferSchema", "True") \
    .option("header", "True") \
    .option("sep", ",") \
    .load("dbfs:/FileStore/shared_uploads/marc.gomez@microsoft.com/insurance.csv")

# COMMAND ----------

insurance_df.groupby('children')\
    .agg({'charges': 'avg', 'children': 'count'})\
    .withColumnRenamed('avg(charges)', 'average_charges')\
    .orderBy('average_charges')\
    .display()

# COMMAND ----------

children_5 = insurance_df.filter(insurance_df.children == 5)
children_5.display()

# COMMAND ----------

children_5.write.mode("overwrite").csv('dbfs:/FileStore/shared_uploads/marc.gomez@microsoft.com/output/children_5.csv')

# COMMAND ----------

children_5.write.mode("overwrite").json('dbfs:/FileStore/shared_uploads/marc.gomez@microsoft.com/output/children_5.json')

# COMMAND ----------


