# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

df= spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

display(df)

# COMMAND ----------

races_filtered_df = df.filter("race_year=2019 and round <=5")

# COMMAND ----------

race_filtered_df = df.filter((df["race_year"]==2019) & (df["round"]<=5))

# COMMAND ----------

display(races_filtered_df)
