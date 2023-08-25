# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_result = dbutils.notebook.run("1.ingest_circuits_file",0,{"p_data_source":"Ergast API","p_file_date":"2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.ingest_races_file",0,{"p_data_source":"Ergast API","p_file_date":"2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3.ingest_constructors_file",0,{"p_data_source":"Ergast API","p_file_date":"2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4.ingest_drivers_file",0,{"p_data_source":"Ergast API","p_file_date":"2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("10.ingest_results_file_incremental",0,{"p_data_source":"Ergast API","p_file_date":"2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("11.ingest_pitstops_file_incremental",0,{"p_data_source":"Ergast API","p_file_date":"2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("12.ingest_lap_times_file_incremental",0,{"p_data_source":"Ergast API","p_file_date":"2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("13.ingest_qualifying_file_incremental",0,{"p_data_source":"Ergast API","p_file_date":"2021-03-28"})

# COMMAND ----------

v_result
