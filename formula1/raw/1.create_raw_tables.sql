-- Databricks notebook source
-- MAGIC %sql
-- MAGIC CREATE DATABASE IF NOT Exists f1_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create Circuits Table

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC drop table if exists f1_raw.circuits

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC create table if not exists f1_raw.circuits(circuitId INT,
-- MAGIC circuitRef STRING,
-- MAGIC name STRING,
-- MAGIC location STRING,
-- MAGIC country STRING,
-- MAGIC lat DOUBLE,
-- MAGIC lng DOUBLE,
-- MAGIC alt INT,
-- MAGIC url STRING)
-- MAGIC using csv
-- MAGIC options (path "/mnt/seyonformula1dls/raw/circuits.csv", header true)

-- COMMAND ----------

-- MAGIC %sql 
-- MAGIC select * from f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Races Table

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC drop table if exists f1_raw.races

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC create table if not exists f1_raw.races(raceId INT,
-- MAGIC year INT,
-- MAGIC round INT,
-- MAGIC circuitId INT,
-- MAGIC name STRING,
-- MAGIC date DATE,
-- MAGIC time STRING,
-- MAGIC url STRING)
-- MAGIC using csv
-- MAGIC options (path "/mnt/seyonformula1dls/raw/races.csv", header true)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create constructors table
-- MAGIC ## Single Line Json

-- COMMAND ----------


drop table if exists f1_raw.constructors;
create table if not exists f1_raw.constructors(constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING)
using json
options (path "/mnt/seyonformula1dls/raw/constructors.json", header true)

-- COMMAND ----------

select * from f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create drivers table

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers(driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING)
using json
options (path "/mnt/seyonformula1dls/raw/drivers.json", header true)

-- COMMAND ----------

select * from f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Results Table

-- COMMAND ----------


drop table if exists f1_raw.results;
create table if not exists f1_raw.results(resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT, 
grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING)
using json
options (path "/mnt/seyonformula1dls/raw/results.json", header true)

-- COMMAND ----------

select * from f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create PitStops Table

-- COMMAND ----------

drop table if exists f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops(driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING)
using json
options (path "/mnt/seyonformula1dls/raw/pit_stops.json",multiLine true, header true)

-- COMMAND ----------

select * from f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create Lap Time Table

-- COMMAND ----------

drop table if exists f1_raw.lap_times;
create table if not exists f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
millisecnds INT)
using csv
options (path "/mnt/seyonformula1dls/raw/lap_times", header true)

-- COMMAND ----------

select * from f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ####Create Qualifying Table

-- COMMAND ----------

drop table if exists f1_raw.qualifying;
create table if not exists f1_raw.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT)
using json
options (path "/mnt/seyonformula1dls/raw/qualifying",multiLine true, header true)

-- COMMAND ----------

select * from f1_raw.qualifying

-- COMMAND ----------

desc extended f1_raw.qualifying

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------


