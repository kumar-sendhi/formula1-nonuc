-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

show databases

-- COMMAND ----------

 describe database demo

-- COMMAND ----------

 describe database extended demo

-- COMMAND ----------

select current_database()

-- COMMAND ----------

show tables

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

use demo

-- COMMAND ----------

select current_database()

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df= spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

show tables


-- COMMAND ----------

desc extended race_results_python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mounts()

-- COMMAND ----------

select * from demo.race_results_python

-- COMMAND ----------

create table race_results_sql
as 
select *
from demo.race_results_python
where race_year=2020

-- COMMAND ----------

select *
from demo.race_results_sql

-- COMMAND ----------

drop table demo.race_results_sql

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

desc extended race_results_ext_py

-- COMMAND ----------

create table race_results_ext_sql
using parquet
Location "/mnt/seyonformula1dls/presentation/race_results_ext_sql"
as 
select *
from demo.race_results_ext_py
where race_year=2020


-- COMMAND ----------

create or replace temp view v_race_results
as 
select * from race_results_python
where race_year = 2018

-- COMMAND ----------

select * from v_race_results

-- COMMAND ----------

create or replace global temp view gv_race_results
as 
select * from race_results_python
where race_year = 2015

-- COMMAND ----------

select * from global_temp.gv_race_results

-- COMMAND ----------

create or replace view demo.pv_race_results
as 
select * from race_results_python
where race_year = 2014

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------


