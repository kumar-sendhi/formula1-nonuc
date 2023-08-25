# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date =dbutils.widgets.get("p_file_date")

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
.withColumnRenamed("number","driver_number") \
.withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
.withColumnRenamed("nationality","constructor_nationality") \
.withColumnRenamed("constructor_name","team")

# COMMAND ----------

circuits_df= spark.read.parquet(f"{processed_folder_path}/circuits") \
.withColumnRenamed("location","circuit_location")

# COMMAND ----------

races_df= spark.read.parquet(f"{processed_folder_path}/races") \
.withColumnRenamed("races_name","race_name") \
.withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------

results_df= spark.read.parquet(f"{processed_folder_path}/results") \
.filter(f"file_date='{v_file_date}'") \
.withColumnRenamed("time","race_time") \
.withColumnRenamed("race_id","result_race_id")

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join circuits to races

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id) \
.select(races_df.races_id,races_df.race_year, races_df.race_name, races_df.race_date,circuits_df.circuit_location)

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### join results to all other dataframes

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.result_race_id ==race_circuits_df.races_id) \
.join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
.join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp,lit

# COMMAND ----------

final_df =race_results_df.select("races_id","race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time","points","position").withColumn("created_date",current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

display(final_df.filter("race_year == 2011 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

#final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(final_df)

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

1
# final_df = re_arrange_partition_column(final_df, "races_id")

overwrite_partition(final_df,'f1_presentation','race_results','races_id')

# COMMAND ----------

df= spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(df)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


