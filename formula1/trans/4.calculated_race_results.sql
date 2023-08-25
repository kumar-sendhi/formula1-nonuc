-- Databricks notebook source
use f1_processed

-- COMMAND ----------

desc constructors

-- COMMAND ----------

create  table f1_presentation.calculated_race_results 
Using parquet
AS
select races.race_year,
constructors.constructor_name,
drivers.driver_name,
results.position,
results.points,
11 - results.position as calculated_points
from results join drivers on (results.driver_id = drivers.driver_id)
join constructors on (results.constructor_id = constructors.constructor_id)
join races on (results.race_id = races.races_id)
where results.position <= 10

-- COMMAND ----------

select * from f1_presentation.calculated_race_results

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

