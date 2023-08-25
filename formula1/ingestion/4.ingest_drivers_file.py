# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest drivers.json file

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

name_schema = StructType(fields =[StructField("forename",StringType(),False),
                                     StructField("surname",StringType(),True)
                                     ])

# COMMAND ----------

drivers_schema = StructType(fields =[StructField("driverId",IntegerType(),False),
                                     StructField("driverRef",StringType(),True),
                                     StructField("number",IntegerType(),True),
                                     StructField("code",StringType(),True),
                                     StructField("name",name_schema),
                                     StructField("dob",DateType(),True),
                                     StructField("nationality",StringType(),True),
                                     StructField("url",StringType(),True)
                                     ])

# COMMAND ----------

# races_df = spark.read.option("header", "true") \
# .option("inferSchema", "true") \
# .json('/mnt/seyonformula1dls/raw/constructors.csv')

# COMMAND ----------

drivers_df = spark.read.option("header", "true") \
.schema(drivers_schema) \
.json(f'{raw_folder_path}/{v_file_date}/drivers.json')

# COMMAND ----------

type(drivers_df)

# COMMAND ----------

drivers_df.show()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### select the columns of interest

# COMMAND ----------

drivers_selected_df = drivers_df.drop('url')

# COMMAND ----------

display(drivers_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename the columns as required

# COMMAND ----------

driversconstructors_renamed_df = drivers_selected_df.withColumnRenamed("driverId","driver_id") \
.withColumnRenamed("driverRef","driver_ref") \
.withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname"))) \
.withColumnRenamed("name","driver_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 4 - add ingestion date to the dataframe

# COMMAND ----------

drivers_final_df = driversconstructors_renamed_df.withColumn("data_source",lit(v_data_source)) \
.withColumn("env",lit("Production")) \
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

drivers_final_df = add_ingestion_date(drivers_final_df)

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####### Write data to datalake as parquet

# COMMAND ----------

#drivers_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

df= spark.read.parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
