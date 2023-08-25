-- Databricks notebook source
select constructor_name,
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
group by constructor_name
Having count(1) >= 100
order by avg_points desc

-- COMMAND ----------

select constructor_name,
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where race_year between 1990 and 2000
group by constructor_name
Having count(1) >= 100
order by avg_points desc

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------


