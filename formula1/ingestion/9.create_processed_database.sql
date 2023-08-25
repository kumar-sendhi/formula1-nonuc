-- Databricks notebook source
CREATE DATABASE IF NOT Exists f1_processed
Location "/mnt/seyonformula1dls/processed"

-- COMMAND ----------

DESC DATABASE f1_raw;

-- COMMAND ----------

DESC DATABASE f1_processed;

-- COMMAND ----------

drop database f1_processed

-- COMMAND ----------

CREATE DATABASE IF NOT Exists f1_presentation
Location "/mnt/seyonformula1dls/presentation"

-- COMMAND ----------


