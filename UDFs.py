# Databricks notebook source
from pyspark.sql.functions import udf, lit
from pyspark.sql.types import FloatType, ArrayType, LongType, StringType

# COMMAND ----------

superstore_df = spark.read.table("default.superstore")

# COMMAND ----------

def cost(sales, profit):
    return (sales - profit)

# COMMAND ----------

compute_cost_udf = udf(cost, FloatType())

superstore_df.withColumn('Cost', compute_cost_udf('Sales', 'Profit'))\
    .select('Product Name', 'Sales', 'Profit', 'Cost')\
    .display()

# COMMAND ----------



# COMMAND ----------


