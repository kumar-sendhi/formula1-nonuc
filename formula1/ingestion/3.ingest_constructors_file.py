# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the csv file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType,TimestampType,DateType
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

constructors_schema = 'constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING'

# COMMAND ----------

# races_df = spark.read.option("header", "true") \
# .option("inferSchema", "true") \
# .json('/mnt/seyonformula1dls/raw/constructors.csv')

# COMMAND ----------

constructors_df = spark.read.option("header", "true") \
.schema(constructors_schema) \
.json(f'{raw_folder_path}/{v_file_date}/constructors.json')

# COMMAND ----------

type(constructors_df)

# COMMAND ----------

constructors_df.show()

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### select the columns of interest

# COMMAND ----------

constructors_selected_df = constructors_df.drop('url')

# COMMAND ----------

display(constructors_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename the columns as required

# COMMAND ----------

constructors_renamed_df = constructors_selected_df.withColumnRenamed("constructorId","constructor_id") \
.withColumnRenamed("constructorRef","constructor_ref") \
.withColumnRenamed("name","constructor_name") 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 4 - add ingestion date to the dataframe

# COMMAND ----------

constructors_final_df = constructors_renamed_df.withColumn("data_source",lit(v_data_source)) \
.withColumn("env",lit("Production")) \
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

constructors_final_df = add_ingestion_date(constructors_final_df)

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####### Write data to datalake as parquet

# COMMAND ----------

#constructors_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

df= spark.read.parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
