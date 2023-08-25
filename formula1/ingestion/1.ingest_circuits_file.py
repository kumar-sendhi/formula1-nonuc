# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the csv file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source =dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date =dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, current_timestamp,lit

# COMMAND ----------

circuits_schema = StructType(fields =[StructField("circuitId",IntegerType(),False),
                                     StructField("circuitRef",StringType(),True),
                                     StructField("name",StringType(),True),
                                     StructField("location",StringType(),True),
                                     StructField("country",StringType(),True),
                                     StructField("lat",DoubleType(),True),
                                     StructField("lng",DoubleType(),True),
                                     StructField("alt",IntegerType(),True),
                                     StructField("url",StringType(),True)
                                     ])

# COMMAND ----------

# circuits_df = spark.read.option("header", "true") \
# .option("inferSchema", "true") \
# .csv(f'/mnt/seyonformula1dls/raw/circuits.csv')

# COMMAND ----------

circuits_df = spark.read.option("header", "true") \
.schema(circuits_schema) \
.csv(f'{raw_folder_path}/{v_file_date}/circuits.csv')

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/seyonformula1dls/raw

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### select the columns of interest

# COMMAND ----------

circuits_selected_df = circuits_df.select('circuitId','circuitRef','name','location','country','lat','lng','alt')

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df.circuitId,circuits_df.circuitRef,circuits_df.name,circuits_df.location,circuits_df.country,circuits_df.lat,circuits_df.lng,circuits_df.alt)

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df["circuitId"],circuits_df["circuitRef"],circuits_df["name"],circuits_df["location"],circuits_df["country"],circuits_df["lat"],circuits_df["lng"],circuits_df["alt"])

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country").alias("race_country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename the columns as required

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_id") \
.withColumnRenamed("circuitRef","circuit_ref") \
.withColumnRenamed("name","circuits_name") \
.withColumnRenamed("lat","latitude") \
.withColumnRenamed("lng","longitude") \
.withColumnRenamed("alt","altitude") 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 4 - add ingestion date to the dataframe

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("env",lit("Production")) \
.withColumn("data_source",lit(v_data_source)) \
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_final_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####### Write data to datalake as parquet

# COMMAND ----------

#circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

df= spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


