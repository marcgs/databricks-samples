# Databricks notebook source
loan_df = spark.sql('select * from default.loan_data')

# COMMAND ----------

loan_df.display()

# COMMAND ----------

spark.sql("""select Loan_type as Type, Gender, Income, Credit_score as Score
          from loan_data
          where Income > 100000
          order by Income desc
          limit 10""").display()

# COMMAND ----------

spark.sql("""select count(*) as Count
          from loan_data
          where Credit_score > 600
          limit 10""").display()

# COMMAND ----------

spark.sql("""select Gender, avg(Income) as AvgIncome
          from loan_data
          where Default = 0
          group by Gender
          """).display()

# COMMAND ----------

spark.sql("""select Degree, avg(Income) as AvgIncome
          from loan_data
          group by Degree
          having AvgIncome > 70000
          """).display()

# COMMAND ----------

spark.table("default.loan_data").display()

# COMMAND ----------



# COMMAND ----------

spark.sql("""select Degree, Income from loan_data""")\
    .groupby('Degree')\
    .agg({'Income': 'avg'})\
    .display()

# COMMAND ----------

load_data_subset = spark.sql("""select trim(Loan_type) as Type, trim(Gender) as Gender, Age, Income, Credit_score from loan_data""")

# COMMAND ----------



# COMMAND ----------

load_data_subset.createOrReplaceTempView('load_data_subset')

# COMMAND ----------

spark.sql('select * from loan_data where Gender = "Female" and Age < 30').write.mode("overwrite")\
    .saveAsTable('loan_data_females_under_30')

# COMMAND ----------

spark.sql('select * from loan_data where Gender = "Female" and Age < 30').write.mode("overwrite")\
    .partitionBy("Loan_type")\
    .saveAsTable('partitioned_loan_data_females_under_30')

# COMMAND ----------

spark.sql('select * from loan_data_females_under_30').display()

# COMMAND ----------

spark.sql('select * from partitioned_loan_data_females_under_30').display()

# COMMAND ----------


