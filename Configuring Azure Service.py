# Databricks notebook source
# DBTITLE 1,Setting up variables

storage_account="moviegoers"
client_id = dbutils.secrets.get(scope="movie-scope", key="databricks-client-id")
tenant_id = dbutils.secrets.get(scope="movie-scope", key="databricks-tenant-id")
client_secret=dbutils.secrets.get(scope="movie-scope", key="databricks-client-secret")

# COMMAND ----------

# DBTITLE 1,Config Settings
configs = { "fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret":f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token" }



# COMMAND ----------

# DBTITLE 1,Mount the containers
def mountadlsgen(container_name):
dbutils.fs.mount(
source = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

mountadlsgen("raw")

# COMMAND ----------

mountadlsgen("processed")

# COMMAND ----------

# DBTITLE 1,Access the mount container
dbutils.fs.ls("/mnt/moviegoers/raw")

# COMMAND ----------

dbutils.fs.ls("/mnt/moviegoers/processed")

# COMMAND ----------


