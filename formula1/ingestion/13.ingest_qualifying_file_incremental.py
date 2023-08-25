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

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date =dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

qualifying_schema = StructType(fields =[StructField("qualifyId",IntegerType(),False),
                                     StructField("raceId",IntegerType(),True),
                                     StructField("driverId",IntegerType(),True),
                                     StructField("constructorId",IntegerType(),True),
                                     StructField("number",IntegerType(),True),
                                     StructField("position",IntegerType(),True),
                                     StructField("q1",StringType(),True),
                                     StructField("q2",StringType(),True),
                                     StructField("q3",StringType(),True)
                                     ])

# COMMAND ----------

# races_df = spark.read.option("header", "true") \
# .option("inferSchema", "true") \
# .json('/mnt/seyonformula1dls/raw/constructors.csv')

# COMMAND ----------

qualifying_df = spark.read.option("header", "true") \
.option("multiline", "true") \
.schema(qualifying_schema) \
.json(f'{raw_folder_path}/{v_file_date}/qualifying')

# COMMAND ----------

type(qualifying_df)

# COMMAND ----------

qualifying_df.show()

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

qualifying_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename the columns as required

# COMMAND ----------

qualifying_renamed_df = qualifying_df.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("qualifyId","qualify_id") \
.withColumnRenamed("constructorId","constructor_id") \
.withColumnRenamed("driverId","driver_id") \
.withColumn("ingestion_date",current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 4 - add ingestion date to the dataframe

# COMMAND ----------

qualifying_final_df = qualifying_renamed_df.withColumn("env",lit("Production")).withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_final_df)

# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####### Write data to datalake as parquet

# COMMAND ----------

#qualifying_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")

# COMMAND ----------

qualifying_final_df = re_arrange_partition_column(qualifying_final_df, "race_id")

# COMMAND ----------

overwrite_partition(qualifying_final_df,'f1_processed','qualifying','race_id')

# COMMAND ----------

# qualifying_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

df= spark.read.parquet(f"{processed_folder_path}/qualifying")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1)
# MAGIC from f1_processed.qualifying
# MAGIC group by race_id

# COMMAND ----------

dbutils.notebook.exit("Success")
