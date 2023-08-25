# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.seyonformula1dls.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.seyonformula1dls.dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.seyonformula1dls.dfs.core.windows.net", "sp=rl&st=2022-08-25T02:32:39Z&se=2022-08-26T10:32:39Z&spr=https&sv=2021-06-08&sr=c&sig=4WCvPXoL6z7SYxsHFszuBvjfN%2Bd64dI7eNrGFKvMNv4%3D")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@seyonformula1dls.dfs.core.windows.net/results_partitioned/raceId=1052")

# COMMAND ----------

spark.read.format("parquet").load("abfss://demo@seyonformula1dls.dfs.core.windows.net/results_partitioned/raceId=1052/part-00000-d16bb3da-9bf3-47de-969a-a03ff2d39a6d.c000.snappy.parquet")



# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.seyonformula1dls.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.seyonformula1dls.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.seyonformula1dls.dfs.core.windows.net", "?sv=2020-02-10&st=2022-08-23T14%3A53%3A00Z&se=2022-08-26T14%3A53%3A00Z&sr=d&sp=racwdlmeop&sig=38DKCWGU%2FDv6Jlck5%2BPv3HknJ53moe0gS2%2BHtyXG1os%3D&sdd=2")

# COMMAND ----------

https://seyonformula1dls.blob.core.windows.net/demo/results_partitioned/raceId%3D1052/part-00000-d16bb3da-9bf3-47de-969a-a03ff2d39a6d.c000.snappy.parquet
