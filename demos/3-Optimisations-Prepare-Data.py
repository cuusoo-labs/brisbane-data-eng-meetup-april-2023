# Databricks notebook source
# MAGIC %md
# MAGIC ## Prepare NYC taxi data
# MAGIC 
# MAGIC - Copies a sample delta table into tmp folder but remove records outside NYC
# MAGIC - Clones it into another folder and Z-Order it
# MAGIC - This notebooks needs a bit big compute-optimised cluster to run quickly (8 workers with 16 cores each if possible)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow

# COMMAND ----------

dbutils.fs.rm("dbfs:/tmp/delta-optimisation", True)

# COMMAND ----------

import pyspark.sql.functions as F
df = (
  spark.read.format('delta').load("dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow").where(""" 
  (pickup_longitude between -74.4 and -73.6) AND
  (pickup_latitude between 40 and 41) AND
  (dropoff_longitude between -74.4 and -73.6) AND
  (dropoff_latitude between 40 and 41)
""").repartition(64))

df.write.format("delta").mode("overwrite").save("dbfs:/tmp/delta-optimisation/nyctaxi_yellow")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE delta.`dbfs:/tmp/delta-optimisation/nyctaxi_yellow_optimised` 
# MAGIC DEEP CLONE delta.`dbfs:/tmp/delta-optimisation/nyctaxi_yellow`;

# COMMAND ----------

spark.conf.get("spark.databricks.delta.optimize.maxFileSize")

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`dbfs:/tmp/delta-optimisation/nyctaxi_yellow_optimised` ZORDER BY (pickup_longitude, pickup_latitude)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY delta.`dbfs:/tmp/delta-optimisation/nyctaxi_yellow_optimised`

# COMMAND ----------


