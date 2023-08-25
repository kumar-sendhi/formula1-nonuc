# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df= spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

demo_df  = race_results_df.filter("race_year = 2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

demo_df.select(count("*")).show()

# COMMAND ----------

demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_df.select(sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name='Lewis Hamilton'").select(sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name='Lewis Hamilton'").select(sum("points"), countDistinct("race_name")).\
withColumnRenamed("sum(points)", "total_points") \
.withColumnRenamed("count(DISTINCT race_name)","number_of_races").show()

# COMMAND ----------

df =demo_df.groupby("driver_name").sum("points").withColumnRenamed("sum(points)","total_points")
df1=df.orderBy(df.total_points.desc())

# COMMAND ----------

df2 =demo_df.groupby("driver_name").agg(sum("points"),countDistinct("race_name")).withColumnRenamed("sum(points)","total_points")

# COMMAND ----------

display(df1)

# COMMAND ----------

display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Window Function

# COMMAND ----------

demo_df = race_results_df.filter("race_year in (2019,2020)")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

df2 =demo_df.groupby("race_year","driver_name").agg(sum("points"),countDistinct("race_name").alias("number_of_races")).withColumnRenamed("sum(points)","total_points")

# COMMAND ----------

display(df2)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))

df2.withColumn("rank",rank().over(driverRankSpec)).show(100)

# COMMAND ----------



# COMMAND ----------

display(df2)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


