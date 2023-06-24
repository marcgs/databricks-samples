# Databricks notebook source
import pandas as pd

# COMMAND ----------

loan_data = spark.read.format("csv")\
    .option("inferSchema", "true")\
    .option("header", "true")\
    .option("sep", ",")\
    .load("dbfs:/FileStore/shared_uploads/marc.gomez@microsoft.com/loan_data.csv")

# COMMAND ----------

loan_data.display()

# COMMAND ----------

loan_data.rdd.getNumPartitions()

# COMMAND ----------

loan_pandas_df = pd.read_csv("/dbfs/FileStore/shared_uploads/marc.gomez@microsoft.com/loan_data.csv")
loan_df = spark.createDataFrame(loan_pandas_df)

# COMMAND ----------

loan_df.rdd.getNumPartitions()

# COMMAND ----------

spark.sparkContext.defaultParallelism

# COMMAND ----------

loan_df.rdd.glom().collect()

# COMMAND ----------

len(loan_df.rdd.glom().collect())

# COMMAND ----------

loan_data.filter(loan_data.Degree == 'Graduate').display()

# COMMAND ----------

loan_df.filter(loan_df.Degree == 'Graduate').display()

# COMMAND ----------

spark.conf.get("spark.sql.files.maxPartitionBytes")

# COMMAND ----------

loan_df_8 = loan_df.repartition(8)

# COMMAND ----------

loan_df_8.filter(loan_df.Degree == 'Graduate').display()

# COMMAND ----------


