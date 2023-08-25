# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest results.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the csv file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType,TimestampType,DateType,FloatType
from pyspark.sql.functions import col, current_timestamp,lit,to_timestamp,concat

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source =dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

results_schema = StructType(fields =[StructField("resultId",IntegerType(),False),
                                     StructField("raceId",IntegerType(),True),
                                     StructField("driverId",IntegerType(),True),
                                     StructField("constructorId",IntegerType(),True),
                                     StructField("number",IntegerType(),True),
                                     StructField("grid",IntegerType(),True),
                                     StructField("position",IntegerType(),True),
                                     StructField("positionText",StringType(),True),
                                     StructField("positionOrder",IntegerType(),True),
                                     StructField("points",FloatType(),True),
                                     StructField("laps",IntegerType(),True),
                                     StructField("time",StringType(),True),
                                     StructField("milliseconds",IntegerType(),True),
                                     StructField("fastestLap",IntegerType(),True),
                                     StructField("rank",IntegerType(),True),
                                     StructField("fasterLapTime",StringType(),True),
                                     StructField("fasterLapSpeed",FloatType(),True),
                                     StructField("statusId",StringType(),True)
                                     ])

# COMMAND ----------

# races_df = spark.read.option("header", "true") \
# .option("inferSchema", "true") \
# .json('/mnt/seyonformula1dls/raw/constructors.csv')

# COMMAND ----------

results_df = spark.read.option("header", "true") \
.schema(results_schema) \
.json(f'{raw_folder_path}/results.json')

# COMMAND ----------

type(results_df)

# COMMAND ----------

results_df.show()

# COMMAND ----------

display(results_df)

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### select the columns of interest

# COMMAND ----------

# display(drivers_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename the columns as required

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed("resultId","result_id") \
.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId","driver_id") \
.withColumnRenamed("constructorId","constructor_id") \
.withColumnRenamed("positionText","position_text") \
.withColumnRenamed("positionOrder","position_order") \
.withColumnRenamed("fastestLap","fastest_lap") \
.withColumnRenamed("fastestLapTime","fastest_lap_time") \
.withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
.withColumn("ingestion_date",current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 4 - add ingestion date to the dataframe

# COMMAND ----------

results_final_df = results_renamed_df.withColumn("env",lit("Production")).withColumn("data_source",lit(v_data_source)).drop('statusId')

# COMMAND ----------

results_final_df = add_ingestion_date(results_final_df)

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####### Write data to datalake as parquet

# COMMAND ----------

#results_final_df.write.mode("overwrite").partitionBy('race_id').parquet(f"{processed_folder_path}/results")

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

df= spark.read.parquet(f"{processed_folder_path}/results")

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
