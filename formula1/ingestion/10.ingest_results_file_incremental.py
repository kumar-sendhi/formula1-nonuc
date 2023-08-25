# Databricks notebook source
# MAGIC %sql
# MAGIC -- drop table f1_processed.results

# COMMAND ----------

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

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date =dbutils.widgets.get("p_file_date")

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
.json(f'{raw_folder_path}/{v_file_date}/results.json')

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

results_final_df = results_renamed_df.withColumn("env",lit("Production")).withColumn("data_source",lit(v_data_source)).drop('statusId').withColumn("file_date",lit(v_file_date))

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

# MAGIC %md
# MAGIC #####Method 1

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     print(race_id_list.race_id)
#     if(spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### end of method 1

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Method 2

# COMMAND ----------

# spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

# results_final_df = results_final_df.select("result_id", "driver_id","constructor_id","number","grid","position","position_text",
#                                           "position_order","points","laps","time","milliseconds","fastest_lap","rank","fasterLapTime",
#                                           "fasterLapSpeed","data_source","file_date","ingestion_date","race_id")

# COMMAND ----------

# if(spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
#     output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
# else:
#     output_df.write.mode("overwrite").partitionBy(f"{partition_column}").format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ####end of method 2

# COMMAND ----------

results_final_df = re_arrange_partition_column(results_final_df, "race_id")

# COMMAND ----------

overwrite_partition(results_final_df,'f1_processed','results','race_id')

# COMMAND ----------

df= spark.read.parquet(f"{processed_folder_path}/results")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


