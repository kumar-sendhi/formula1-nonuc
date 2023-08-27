# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('formula1-scope')

# COMMAND ----------

dbutils.secrets.get('formula1-scope','clientid')

# COMMAND ----------

for x in dbutils.secrets.get('formula1-scope','clientid'):
    print(x)

# COMMAND ----------

storage_account_name ="seyonstoragedlsfailover"
client_id=dbutils.secrets.get('formula1-scope','clientid')
tenant_id=dbutils.secrets.get('formula1-scope','tenantid')
client_secret=dbutils.secrets.get('formula1-scope','clientsecret')

# COMMAND ----------

storage_account_name ="seyonformula1dls"
client_id="91ad0da3-abec-44ae-8e94-332613ae72b3"
tenant_id="521ae675-49d9-4e53-9cfc-fd592a4d36c6"
client_secret="D-w8Q~c342dk-pYeU1~AVvuWdSKWP8hMlJqQXae0"

# COMMAND ----------



configs =  {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": f"{client_id}",
            "fs.azure.account.oauth2.client.secret": f"{client_secret}",
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

container_name = "raw"
dbutils.fs.mount(
source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs=configs)

# COMMAND ----------

dbutils.fs.unmount('/mnt/seyonstoragedlsfailover/raw')

# COMMAND ----------

dbutils.fs.ls("/mnt/seyonformula1dls/raw")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

container_name = "processed"
dbutils.fs.mount(
source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs=configs)

# COMMAND ----------

dbutils.fs.unmount('/mnt/seyonformula1dls/processed')

# COMMAND ----------

dbutils.fs.unmount('/mnt/seyonformula1dls/raw')

# COMMAND ----------

container_name = "presentation"
dbutils.fs.mount(
source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs=configs)

# COMMAND ----------

container_name = "demo"
dbutils.fs.mount(
source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs=configs)
