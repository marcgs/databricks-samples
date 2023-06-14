# Databricks notebook source
import pandas as pd

# COMMAND ----------

credit_train_pandas_df = pd.read_csv('/dbfs/FileStore/shared_uploads/marc.gomez@microsoft.com/credit_train.csv')
credit_train_df = spark.createDataFrame(credit_train_pandas_df)

credit_train_df2 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/marc.gomez@microsoft.com/credit_train.csv")

# Set df to desired dataframe
df = credit_train_df2


# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df.show()

# COMMAND ----------

df.count()

# COMMAND ----------

df.select('Home Ownership', 'Purpose', 'Monthly Debt').display(5)

# COMMAND ----------

df.describe().display()

# COMMAND ----------

df.select('Home Ownership').distinct().show()

# COMMAND ----------

df.crosstab("Purpose", "Term").display()

# COMMAND ----------

df.select('Loan Status', 'Current Loan Amount', 'Term', 'Annual Income')\
    .filter(df['Loan Status'] == "Fully Paid")\
    .limit(10)\
    .display()

# COMMAND ----------

df.select('Loan Status', 'Current Loan Amount', 'Term', 'Annual Income')\
    .filter((df['Loan Status'] == "Fully Paid") & (df['Annual Income'] > 0))\
    .orderBy('Annual Income', ascending=False)\
    .limit(10)\
    .display()

# COMMAND ----------


