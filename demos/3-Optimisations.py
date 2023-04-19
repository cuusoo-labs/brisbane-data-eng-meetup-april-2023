# Databricks notebook source
# MAGIC %pip install databricks-mosaic

# COMMAND ----------

import mosaic as mos
spark.conf.set("spark.databricks.io.cache.enabled", "false")
spark.conf.set("spark.databricks.labs.mosaic.index.system", "H3") # Default
mos.enable_mosaic(spark, dbutils)

# COMMAND ----------

# MAGIC %sh head /dbfs/tmp/delta-optimisation/nyctaxi_yellow/_delta_log/00000000000000000000.json

# COMMAND ----------

import pyspark.sql.functions as F
def load_stats_from_commit(commit_file):
  df = spark.read.json(commit_file).where("add is not null")
  add_schema = """
  struct
  <
  numRecords:long,
  minValues: struct<pickup_latitude: double,pickup_longitude: double>,
  maxValues: struct<pickup_latitude: double,pickup_longitude: double>
  >
  """

  stats = (
    df
      .select("add.path", "add.size", F.from_json("add.stats", add_schema).alias("stats"), F.col("add.stats").alias("copy"))
      .selectExpr(
        "size", "stats.minValues.pickup_latitude as min_pickup_lat","stats.minValues.pickup_longitude as min_pickup_lon", 
        "stats.maxValues.pickup_latitude as max_pickup_lat","stats.maxValues.pickup_longitude as max_pickup_lon"
      )
  )

  stats = (
    stats
      .withColumn("rect", F.expr(
        """
          concat('POLYGON ((' , 
            min_pickup_lon, ' ', min_pickup_lat, ',' ,
            max_pickup_lon, ' ', min_pickup_lat, ',' ,
            max_pickup_lon, ' ', max_pickup_lat, ',' ,
            min_pickup_lon, ' ', max_pickup_lat, ',' ,
            min_pickup_lon, ' ', min_pickup_lat, 
          '))'
          )
      """))
  )

  return stats

# COMMAND ----------

stats = load_stats_from_commit("dbfs:/tmp/delta-optimisation/nyctaxi_yellow/_delta_log/00000000000000000000.json")
stats.display()

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC stats "rect" "geometry"

# COMMAND ----------

# DBTITLE 1,Note that we point to commit number 1
stats_optimised = load_stats_from_commit("dbfs:/tmp/delta-optimisation/nyctaxi_yellow_optimised/_delta_log/00000000000000000001.json")
stats_optimised.display()

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC stats_optimised "rect" "geometry"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`dbfs:/tmp/delta-optimisation/nyctaxi_yellow`
# MAGIC WHERE pickup_latitude = 40.702603 AND pickup_longitude = -74.012488

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`dbfs:/tmp/delta-optimisation/nyctaxi_yellow_optimised`
# MAGIC WHERE pickup_latitude = 40.702603 AND pickup_longitude = -74.012488

# COMMAND ----------


