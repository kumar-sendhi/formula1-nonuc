# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pitstops.json file

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

pitstops_schema = StructType(fields =[StructField("raceId",IntegerType(),False),
                                     StructField("driverId",IntegerType(),True),
                                     StructField("stop",StringType(),True),
                                     StructField("lap",IntegerType(),True),
                                     StructField("time",StringType(),True),
                                     StructField("duration",StringType(),True),
                                     StructField("milliseconds",IntegerType(),True)
                                     ])

# COMMAND ----------

# races_df = spark.read.option("header", "true") \
# .option("inferSchema", "true") \
# .json('/mnt/seyonformula1dls/raw/constructors.csv')

# COMMAND ----------

pitstops_df = spark.read.option("header", "true") \
.option("multiline", "true") \
.schema(pitstops_schema) \
.json(f'{raw_folder_path}/{v_file_date}/pit_stops.json')

# COMMAND ----------

type(pitstops_df)

# COMMAND ----------

pitstops_df.show()

# COMMAND ----------

display(pitstops_df)

# COMMAND ----------

pitstops_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename the columns as required

# COMMAND ----------

pitstops_renamed_df = pitstops_df.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId","driver_id") \
.withColumn("ingestion_date",current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 4 - add ingestion date to the dataframe

# COMMAND ----------

pitstops_final_df = pitstops_renamed_df.withColumn("env",lit("Production")).withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

pitstops_final_df = add_ingestion_date(pitstops_final_df)

# COMMAND ----------

display(pitstops_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####### Write data to datalake as parquet

# COMMAND ----------

#pitstops_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")

# COMMAND ----------

pitstops_final_df = re_arrange_partition_column(pitstops_final_df, "race_id")

# COMMAND ----------

overwrite_partition(pitstops_final_df,'f1_processed','pit_stops','race_id')

# COMMAND ----------

# pitstops_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

df= spark.read.parquet(f"{processed_folder_path}/pit_stops")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1)
# MAGIC from f1_processed.pit_stops
# MAGIC group by race_id

# COMMAND ----------

dbutils.notebook.exit("Success")
