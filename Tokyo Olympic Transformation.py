# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "27a602ea-063d-4513-8f24-a22cb20bfd89",
"fs.azure.account.oauth2.client.secret": 'O5P8Q~tGHzYRtdqSiWDctNUu-u-beV2mg~2vibO4',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/41f88ecb-ca63-404d-97dd-ab0a169fd138/oauth2/token"}


dbutils.fs.mount(
source = "abfss://tokyo-olympic-data@assigmenttokyoolympic.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/assigmenttokyoolympic",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/assigmenttokyoolympic"

# COMMAND ----------

athletes = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/assigmenttokyoolympic/raw/athletes.csv")
coaches = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/assigmenttokyoolympic/raw/coaches.csv")
entriesgender = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/assigmenttokyoolympic/raw/entriesgender.csv")
medals = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/assigmenttokyoolympic/raw/medals.csv")
teams = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/assigmenttokyoolympic/raw/teams.csv")

# COMMAND ----------

medals.show()

# COMMAND ----------

entriesgender = entriesgender.withColumn("Female",col("Female").cast(IntegerType()))\
    .withColumn("Male",col("Male").cast(IntegerType()))\
    .withColumn("Total",col("Total").cast(IntegerType()))

# COMMAND ----------

# Find the top countries with the highest number of gold medals
top_gold_medal_countries = medals.orderBy("Gold", ascending=False).select("Team_Country","Gold").show()

# COMMAND ----------

# Calculate the average number of entries by gender for each discipline
average_entries_by_gender = entriesgender.withColumn(
    'Avg_Female', entriesgender['Female'] / entriesgender['Total']
).withColumn(
    'Avg_Male', entriesgender['Male'] / entriesgender['Total']
)
average_entries_by_gender.show()

# COMMAND ----------

athletes.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/assigmenttokyoolympic/processed/athletes")
coaches.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/assigmenttokyoolympic/processed/coaches")
entriesgender.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/assigmenttokyoolympic/processed/entriesgender")
medals.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/assigmenttokyoolympic/processed/medals")
teams.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/assigmenttokyoolympic/processed/teams")
     

# COMMAND ----------


