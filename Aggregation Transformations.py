# Databricks notebook source
insurance_df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/marc.gomez@microsoft.com/insurance.csv")

# COMMAND ----------

insurance_df.groupBy('sex')\
    .count()\
    .display()

# COMMAND ----------

from pyspark.sql.functions import round
gender_data_counts = insurance_df.groupBy('sex')\
                                    .count()\
                                    .withColumnRenamed('count', 'total')
gender_data_proportions = gender_data_counts\
    .withColumn('proportions', round(gender_data_counts.total/insurance_df.count() * 100, 2))\
        .drop('total')\
        .display()

# COMMAND ----------

charges_by_smoking = insurance_df.groupBy('smoker')\
            .agg({'charges': 'avg', 'bmi': 'average', 'sex': 'count'})\
            .withColumnRenamed('avg(charges)', 'average_charges')
charges_by_smoking.display()

# COMMAND ----------

insurance_df.count()

# COMMAND ----------


