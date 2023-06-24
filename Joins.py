# Databricks notebook source
countries = spark.sql("SELECT * FROM default.countries")
countries.display()

# COMMAND ----------

scores = spark.table("default.happiness_scores")
scores.display()

# COMMAND ----------

result = scores.join(countries, ["Country_id"], "leftouter")
result.display()

# COMMAND ----------

result = scores.join(countries, ["Country_id"], "leftsemi")
result.display()

# COMMAND ----------

result = scores.join(countries, ["Country_id"], "inner")
result.display()

# COMMAND ----------

result = scores.join(countries, ["Country_id"], "leftanti")
result.display()

# COMMAND ----------


