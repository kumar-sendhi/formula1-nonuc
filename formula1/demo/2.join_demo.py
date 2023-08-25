# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df= spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

races_df= spark.read.parquet(f"{processed_folder_path}/races").filter("race_year=2019")

# COMMAND ----------

display(races_df)

# COMMAND ----------

circuits_race_df = circuits_df.join(races_df, circuits_df["circuit_id"]==races_df["circuit_id"],"inner")

# COMMAND ----------

display(circuits_race_df)

# COMMAND ----------

#right Outer Join
right_circuits_race_df = circuits_df.join(races_df, circuits_df["circuit_id"]==races_df["circuit_id"],"right")

# COMMAND ----------

display(right_circuits_race_df)

# COMMAND ----------

#full Outer Join
full_circuits_race_df = circuits_df.join(races_df, circuits_df["circuit_id"]==races_df["circuit_id"],"full")

# COMMAND ----------

display(full_circuits_race_df)

# COMMAND ----------

#Semi  Join
semi_circuits_race_df = circuits_df.join(races_df, circuits_df["circuit_id"]==races_df["circuit_id"],"semi")

# COMMAND ----------

display(semi_circuits_race_df)

# COMMAND ----------

#Anti  Join
anti_circuits_race_df = circuits_df.join(races_df, circuits_df["circuit_id"]==races_df["circuit_id"],"anti")

# COMMAND ----------

display(anti_circuits_race_df)

# COMMAND ----------

#Cross  Join
cross_circuits_race_df = circuits_df.crossJoin(races_df)

# COMMAND ----------

display(cross_circuits_race_df)

# COMMAND ----------

#Left Outer Join
circuits_race_df = circuits_df.join(races_df, circuits_df["circuit_id"]==races_df["circuit_id"],"outer")

# COMMAND ----------

display(circuits_race_df)

# COMMAND ----------



# COMMAND ----------


