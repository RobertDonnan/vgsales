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

# COMMAND ----------

# DBTITLE 1,Print Schema to show data type 
games.printSchema()

# COMMAND ----------

# DBTITLE 1,Import functions and types to convert data types 
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DataType

# COMMAND ----------

# DBTITLE 1,Convert Global Sales to Int
games = games.withColumn("Rank", col("Rank").cast(IntegerType()))

# COMMAND ----------

# DBTITLE 1,Reprint schema to ensure Global Sales has changed to Int 
games.printSchema()

# COMMAND ----------

# DBTITLE 1,Show results again to view change 
games.show()

# COMMAND ----------

# DBTITLE 1,Converts Year to int 
games = games.withColumn("Year", col("Year").cast(IntegerType()))

# COMMAND ----------

# DBTITLE 1,Cast global Sales to double 
#converted to double to avoid numbers rounding down if an int was used 
games = games.withColumn("Global_Sales", col("Global_Sales").cast(DoubleType()))

# COMMAND ----------

# DBTITLE 1,Displaying table to ensure numbers in global sales remain the same 
games.show()

# COMMAND ----------

# DBTITLE 1,Cast North America Sales to double 
games = games.withColumn("NA_Sales", col("NA_Sales").cast(DoubleType()))

# COMMAND ----------

# DBTITLE 1,Cast EU Sales to double 
games = games.withColumn("EU_Sales", col("EU_Sales").cast(DoubleType()))

# COMMAND ----------

# DBTITLE 1,Cast Japan Sales to double 
games = games.withColumn("JP_Sales", col("JP_Sales").cast(DoubleType()))

# COMMAND ----------

# DBTITLE 1,Cast Japan Sales to double
games = games.withColumn("Other_Sales", col("Other_Sales").cast(DoubleType()))

# COMMAND ----------

# DBTITLE 1,Display table to ensure numbers have changed to double
games.show()

# COMMAND ----------

# DBTITLE 1,Shows Schema to ensure correct and all changes have been made 
games.printSchema()

# COMMAND ----------

# DBTITLE 1,Creating new dataframe with updated column name 
gamesNameChange = games.withColumnRenamed("JP_Sales", "Japan_Sales")

# COMMAND ----------

# DBTITLE 1,Showing new column name change
gamesNameChange.show()
