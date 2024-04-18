# Databricks notebook source
# DBTITLE 1,Mount games Dataset from storage container 
dbutils.fs.mount(
    source='wasbs://games@moviesdataset.blob.core.windows.net',
    mount_point='/mnt/games',
    extra_configs = {'fs.azure.account.key.moviesdataset.blob.core.windows.net': dbutils.secrets.get('projectmoviesscope', 'storageAccountKey')}
)

# COMMAND ----------

# DBTITLE 1,Display path information 
# MAGIC %fs
# MAGIC ls "/mnt/games"

# COMMAND ----------

# DBTITLE 1,Reads file format
games = spark.read.format("csv").option("header","true").load("/mnt/games/raw-data/vgsales.csv")

# COMMAND ----------

# DBTITLE 1,Displays first 20 results 
games.show()
