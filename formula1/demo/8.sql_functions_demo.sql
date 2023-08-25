-- Databricks notebook source
select current_database()

-- COMMAND ----------

use f1_processed

-- COMMAND ----------

select *, concat(driver_ref,'-', code) as new_driver_ref from drivers

-- COMMAND ----------

select *, SPLIT(driver_name, ' ') from drivers

-- COMMAND ----------

SELECT *, current_timestamp() from drivers

-- COMMAND ----------

select *, date_format(dob,'dd-MM-yyyy') from drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Aggregate functions

-- COMMAND ----------

select count(1) from drivers

-- COMMAND ----------

select max(dob) from drivers

-- COMMAND ----------

select * from drivers where dob = '2000-05-11'

-- COMMAND ----------

select count(*) from drivers where nationality = 'Indian'

-- COMMAND ----------

select nationality, count(*) from drivers group by nationality order by count(1) desc

-- COMMAND ----------

select nationality, count(*) from drivers 
group by nationality 
Having count(*) > 100 
order by nationality

-- COMMAND ----------

select nationality, driver_name, dob, Rank(dob) over(partition by nationality order by dob desc) as age_rank from drivers

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------


