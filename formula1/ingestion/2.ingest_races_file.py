# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the csv file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType,TimestampType,DateType
from pyspark.sql.functions import col, current_timestamp,lit,to_timestamp,concat

# COMMAND ----------

dbutils.widgets.text("p_data_source","Production")
v_data_source =dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date =dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

races_schema = StructType(fields =[StructField("racesId",IntegerType(),False),
                                     StructField("year",IntegerType(),True),
                                     StructField("round",IntegerType(),True),
                                     StructField("circuitId",IntegerType(),True),
                                     StructField("name",StringType(),True),
                                     StructField("date",DateType(),True),
                                     StructField("time",StringType(),True),
                                     StructField("url",StringType(),True)
                                     ])

# COMMAND ----------

# races_df = spark.read.option("header", "true") \
# .option("inferSchema", "true") \
# .csv('/mnt/seyonformula1dls/raw/races.csv')

# COMMAND ----------

races_df = spark.read.option("header", "true") \
.schema(races_schema) \
.csv(f'{raw_folder_path}/{v_file_date}/races.csv')

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/seyonformula1dls/raw

# COMMAND ----------

type(races_df)

# COMMAND ----------

races_df.show()

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### select the columns of interest

# COMMAND ----------

races_selected_df = races_df.select('racesId','year','round','circuitId','name','date','time')

# COMMAND ----------

races_selected_df = races_df.select(races_df.racesId,races_df.year,races_df.round,races_df.circuitId,races_df.name,races_df.date,races_df.time)

# COMMAND ----------

races_selected_df = races_df.select(col("racesId"),col("year"),col("round"),col("circuitId"),col("name"),col("date"),col("time"))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename the columns as required

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("racesId","races_id") \
.withColumnRenamed("circuitId","circuit_id") \
.withColumnRenamed("name","races_name") \
.withColumnRenamed("year","race_year") \
.withColumn("race_timestamp",to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

races_selected_df = races_renamed_df.select(col("races_id"),col("race_year"),col("round"),col("circuit_id"),col("races_name"),col("race_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 4 - add ingestion date to the dataframe

# COMMAND ----------

races_final_df = races_selected_df.withColumn("env",lit("Production")) \
.withColumn("data_source",lit(v_data_source)) \
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

races_final_df = add_ingestion_date(races_final_df)

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####### Write data to datalake as parquet

# COMMAND ----------

#races_final_df.write.mode("overwrite").partitionBy('race_year').parquet(f"{processed_folder_path}/races")

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

df= spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
