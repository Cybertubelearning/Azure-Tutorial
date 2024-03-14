# Databricks notebook source
from datetime import datetime

# COMMAND ----------

spark.conf.set("fs.azure.account.key.straceaccount.dfs.core.windows.net","L6uMb2H+JRZbOlIjVYsZUCvLA+KKeqDyhzR0N9G/dVt0bCNlceIpTnsVJCaZ4yelg4lXNaraYPV/+AStfgR0eA==")

# COMMAND ----------

spark.read.csv(path='abfss://inboundport@straceaccount.dfs.core.windows.net/RawFile.csv',header = True)

# COMMAND ----------

f_location = "abfss://inboundport@straceaccount.dfs.core.windows.net"

# COMMAND ----------

df = spark.read.format("csv").option("inferSchema","True").option("header","true").option("delimiter",",").load(f_location)

# COMMAND ----------

df.show(1)

# COMMAND ----------

df = df.withColumnRenamed("Order Date","Order_Date")

# COMMAND ----------

df.show(1)

# COMMAND ----------

df = df.withColumn('Current_Time',datetime.now())

# COMMAND ----------

now = datetime.now()

# COMMAND ----------

current_time = now.strftime("%Y:%M:%D")

# COMMAND ----------

print(current_time)

# COMMAND ----------


