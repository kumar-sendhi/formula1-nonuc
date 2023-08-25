# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists f1_demo
# MAGIC location "/mnt/seyonformula1dls/demo"

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

results_df = spark.read.option("header", "true") \
.schema(results_schema) \
.json(f'{raw_folder_path}/{v_file_date}/results.json')

# COMMAND ----------

display(results_df)

# COMMAND ----------


results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------


results_df.write.format("delta").mode("overwrite").save("/mnt/seyonformula1dls/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table f1_demo.results_external
# MAGIC using delta
# MAGIC location '/mnt/seyonformula1dls/demo/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_external

# COMMAND ----------

df= spark.read.format("delta").load("/mnt/seyonformula1dls/demo/results_external")

# COMMAND ----------

display(df)

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy('raceId').saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed SET points = 11-position Where position <= 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, '/mnt/seyonformula1dls/demo/results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.update(
  condition = "position <= 10",
  set = { "points": "21-position" }
)

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed WHERE position >10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, '/mnt/seyonformula1dls/demo/results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.delete(
  condition = "points = 0"
)
