-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominanat Formula 1 Teams <h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

create or replace temp view v_dominant_teams
as
select constructor_name,
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points,
rank() over(order by avg(calculated_points) desc) team_rank
from f1_presentation.calculated_race_results
where race_year between 1990 and 2000
group by constructor_name
Having count(1) >= 100
order by avg_points desc

-- COMMAND ----------

select 
race_year,
constructor_name,
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where constructor_name in (select constructor_name from v_dominant_teams where team_rank <=10)
group by race_year,constructor_name
order by race_year 

-- COMMAND ----------

select 
race_year,
constructor_name,
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where constructor_name in (select constructor_name from v_dominant_teams where team_rank <=10)
group by race_year,constructor_name
order by race_year 

-- COMMAND ----------

select 
race_year,
constructor_name,
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where constructor_name in (select constructor_name from v_dominant_teams where team_rank <=10)
group by race_year,constructor_name
order by race_year 

-- COMMAND ----------



-- COMMAND ----------


