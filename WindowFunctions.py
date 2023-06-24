# Databricks notebook source
loan_data = spark.table("default.loan_data")
loan_data.display()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, min, max
window_spec = Window.partitionBy(loan_data['Degree'])\
                    .orderBy(loan_data['Income'].desc())
ranking = loan_data.select("ID", "Degree", "Income")\
    .withColumn("Rank", rank().over(window_spec))
ranking.display()

# COMMAND ----------

ranking.where(ranking["Rank"] < 3).drop("Rank").display()

# COMMAND ----------

from pyspark.sql.functions import min, max

df_max = loan_data.groupBy("Degree").agg(max("Income").alias("Max_Income"))
df_min = loan_data.groupBy("Degree").agg(min("Income").alias("Min_Income"))
df_max.join(df_min, "Degree").display()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import min, max, row_number

windowSpec = Window.partitionBy("Degree").orderBy("Income")
df = loan_data.withColumn("rn", row_number().over(windowSpec))

windowSpec2 = Window.partitionBy("Degree")
df = df.withColumn("max_rn", max("rn").over(windowSpec2))

min_df = df.where(df.rn == 1).select("Degree", "Income").withColumnRenamed("Income", "Min_Income")
max_df = df.where(df.rn == df.max_rn).select("Degree", "Income").withColumnRenamed("Income", "Max_Income")

#df.select("Degree", "Income", "rn", "max_rn").display()
min_df.join(max_df, "Degree").display()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import min, max
window_spec = Window.partitionBy(loan_data['Gender'])\
                    .orderBy(loan_data['Age'].desc())\
                    .rowsBetween(-1, 0)
loan_data.select("ID", "Gender", "Age")\
    .withColumn("compare_age", max(loan_data['Age']).over(window_spec)).display()

# COMMAND ----------

import sys
from pyspark.sql.window import Window
from pyspark.sql.functions import avg
window_spec = Window.partitionBy(loan_data['Degree'])\
                    .orderBy(loan_data['Age'].asc())\
                    .rangeBetween(-sys.maxsize, 0)
loan_data.select("ID", "Degree", "Age")\
    .withColumn("avg_age_so_far", avg(loan_data['Age']).over(window_spec)).display()

# COMMAND ----------


