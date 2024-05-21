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

# DBTITLE 1,Cast Other Sales to double
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

# COMMAND ----------

# DBTITLE 1,Creates transformed-data vgsales folder 
gamesNameChange.write.option("header", "true").csv("mnt/games/transformed-data/vgsales")

# COMMAND ----------

# DBTITLE 1,Overwrites transformed data to transformed-data folder 
gamesNameChange.write.mode("overwrite").option("header", "true").csv("/mnt/games/transformed-data/vgsales")

# COMMAND ----------

# DBTITLE 1,Importing Plotly Express for data visualisation 
import plotly.express as ps

# COMMAND ----------

# DBTITLE 1,Visualisation of Genre Popularity
# Read the CSV file as a Spark DataFrame
vgsales = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/mnt/games/raw-data/vgsales.csv") \
    .createOrReplaceTempView("temp_table")

# Create a Spark table from the temporary view
spark.sql("CREATE TABLE IF NOT EXISTS games USING parquet AS SELECT * FROM temp_table")

# Query for Genre Popularity based on sales
query_result = spark.sql("SELECT Genre, Global_Sales FROM games")

# Create a Pandas DataFrame for plotting
pandas_df = query_result.toPandas()

# Group by Genre and Global sales, and create a DataFrame with "genre" and "global sales" columns
grouped_df = pandas_df.groupby("Genre").size().to_frame(name="sales").reset_index()

# Create the bar chart using Plotly (with color customisation)
fig = ps.bar(grouped_df, x="Genre", y="sales", color="sales", color_continuous_scale="Viridis")

# Set plot size
fig.update_layout(width=900, height=600)

# Display the plot
fig.show()


# COMMAND ----------

# DBTITLE 1,Visualisation of platform sales
# Read the CSV file as a Spark DataFrame
vgsales = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/mnt/games/raw-data/vgsales.csv") \
    .createOrReplaceTempView("temp_table")

# Create a Spark table from the temporary view
spark.sql("CREATE TABLE IF NOT EXISTS games USING parquet AS SELECT * FROM temp_table")

# Query for Platform Popularity based on sales
query_result = spark.sql("SELECT Platform, Global_Sales FROM games")

# Create a Pandas DataFrame for plotting
pandas_df = query_result.toPandas()

# Group by Platform and Global sales, and create a DataFrame with "Platform" and "Global Sales" columns
grouped_df = pandas_df.groupby("Platform").size().to_frame(name="Sales").reset_index()

# Create the bar chart using Plotly (with color customisation)
fig = ps.bar(grouped_df, x="Platform", y="Sales", color="Sales", color_continuous_scale="Viridis")

# Set plot size
fig.update_layout(width=900, height=600)

# Display the plot
fig.show()

# COMMAND ----------

# DBTITLE 1,Rank based on titles and global sales
# Read the CSV file as a Spark DataFrame
vgsales = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/mnt/games/raw-data/vgsales.csv") \
    .createOrReplaceTempView("temp_table")

    
# Create a Spark table from the temporary view
spark.sql("CREATE TABLE IF NOT EXISTS games USING parquet AS SELECT * FROM temp_table")

# Query for Platform Popularity based on sales
query_result = spark.sql("SELECT Rank, Global_Sales, Name FROM games")

# Create a Pandas DataFrame for plotting
pandas_df = query_result.toPandas()

# Group by Rank and Name, and create a DataFrame with "Rank" and "Name" columns and finding the mean to calculate the rank of each game
grouped_df = pandas_df.groupby("Name")["Rank"].mean().reset_index().head(20)

# Create the bar chart using Plotly (with color customisation)
fig = ps.bar(grouped_df, x="Name", y="Rank", color="Name", color_continuous_scale="Viridis")

# Set plot size
fig.update_layout(width=900, height=600)

# Display the plot
fig.show()

# COMMAND ----------

# DBTITLE 1,Publishers that have sold the most games 
# Read the CSV file as a Spark DataFrame
vgsales = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/mnt/games/raw-data/vgsales.csv") \
    .createOrReplaceTempView("temp_table")

    
# Create a Spark table from the temporary view
spark.sql("CREATE TABLE IF NOT EXISTS games USING parquet AS SELECT * FROM temp_table")

# Query for Platform Popularity based on sales
query_result = spark.sql("SELECT Publisher, Global_Sales FROM games")

# Create a Pandas DataFrame for plotting
pandas_df = query_result.toPandas()

# Group by Publisher and global sales, and create a DataFrame with "Publisher" and "Global Sales" columns limited to 10
grouped_df = pandas_df.groupby("Publisher").size().to_frame(name="Sales").reset_index().head(15)

# Create the bar chart using Plotly (with color customisation)
fig = ps.bar(grouped_df, x="Publisher", y="Sales", color="Sales", color_continuous_scale="Viridis")

# Set plot size
fig.update_layout(width=900, height=600)

# Display the plot
fig.show()

# COMMAND ----------

# DBTITLE 1,Global Sales based on year
# Read the CSV file as a Spark DataFrame
vgsales = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/mnt/games/raw-data/vgsales.csv") \
    .createOrReplaceTempView("temp_table")

    
# Create a Spark table from the temporary view
spark.sql("CREATE TABLE IF NOT EXISTS games USING parquet AS SELECT * FROM temp_table")

# Query for Platform Popularity based on sales
query_result = spark.sql("SELECT Year, Global_Sales FROM games")

# Create a Pandas DataFrame for plotting
pandas_df = query_result.toPandas()

# Group by Year and Global Sales, and create a DataFrame with "Year" and "Global Sales" columns limited to 10
grouped_df = pandas_df.groupby("Year").size().to_frame(name="Sales").reset_index()

# Create the bar chart using Plotly (with color customisation)
fig = ps.bar(grouped_df, x="Year", y="Sales", color="Sales", color_continuous_scale="Viridis")

# Set plot size
fig.update_layout(width=900, height=600)

# Display the plot
fig.show()
