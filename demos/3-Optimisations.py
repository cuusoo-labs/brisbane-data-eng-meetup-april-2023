# Databricks notebook source
# MAGIC %pip install databricks-mosaic

# COMMAND ----------



# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", "false")

# COMMAND ----------

import mosaic as mos
spark.conf.set("spark.databricks.labs.mosaic.index.system", "H3") # Default
mos.enable_mosaic(spark, dbutils)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow

# COMMAND ----------

dbutils.fs.rm("dbfs:/tmp/yousry/nyc_taxi", True)
dbutils.fs.rm("dbfs:/tmp/yousry/nyc_taxi_parquet", True)
dbutils.fs.rm("dbfs:/tmp/yousry/nyc_taxi_delta", True)

# COMMAND ----------

import pyspark.sql.functions as F
df = (spark.read.format('delta').load("dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow").where(""" 
pickup_latitude IS NOT NULL AND
pickup_longitude IS NOT NULL AND
dropoff_latitude IS NOT NULL AND
dropoff_longitude IS NOT NULL AND

(pickup_longitude between -74.4 and -73.6) AND
(pickup_latitude between 40 and 41) AND
(dropoff_longitude between -74.4 and -73.6) AND
(dropoff_latitude between 40 and 41)

""")
.withColumn("pickup_latitude", F.col("pickup_latitude").cast("float"))
.withColumn("pickup_longitude", F.col("pickup_longitude").cast("float"))
.withColumn("dropoff_latitude", F.col("dropoff_latitude").cast("float"))
.withColumn("dropoff_longitude", F.col("dropoff_longitude").cast("float"))

).repartition(64)

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("dbfs:/tmp/yousry/nyc_taxi_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM 
# MAGIC delta.`dbfs:/tmp/yousry/nyc_taxi_delta`

# COMMAND ----------

import pyspark.sql.functions as F
df = spark.read.json("dbfs:/tmp/yousry/nyc_taxi_delta/_delta_log/00000000000000000000.json").where("add is not null")
add_schema = """
struct
<
numRecords:long,
minValues: struct
  <
    pickup_latitude: double,
    pickup_longitude: double
  >,
maxValues: struct
  <
    pickup_latitude: double,
    pickup_longitude: double
  >
>
"""

stats = (df.select("add.path", "add.size", F.from_json("add.stats", add_schema).alias("stats"), F.col("add.stats").alias("copy"))
  .selectExpr("size", "stats.minValues.pickup_latitude as min_pickup_lat","stats.minValues.pickup_longitude as min_pickup_lon", "stats.maxValues.pickup_latitude as max_pickup_lat","stats.maxValues.pickup_longitude as max_pickup_lon")
)

stats = (
  stats
    .withColumn("width", F.expr("max_pickup_lat - min_pickup_lat"))
    .withColumn("height", F.expr("max_pickup_lon - min_pickup_lon"))
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

stats.display()
#struct<Zipcode:string,inner:struct<Zipcode:string,ZipCodeType:string,City:string,State:string>,City:string,State:string>

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC stats "rect" "geometry"

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE delta.`dbfs:/tmp/yousry/nyc_taxi_delta_clone` DEEP CLONE delta.`dbfs:/tmp/yousry/nyc_taxi_delta`

# COMMAND ----------

spark.conf.get("spark.databricks.delta.optimize.maxFileSize")

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`dbfs:/tmp/yousry/nyc_taxi_delta_clone` ZORDER BY (pickup_longitude, pickup_latitude)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC DETAIL delta.`dbfs:/tmp/yousry/nyc_taxi_delta_clone`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC DETAIL delta.`dbfs:/tmp/yousry/nyc_taxi_delta`

# COMMAND ----------

import pyspark.sql.functions as F

df2 = spark.read.json("dbfs:/tmp/yousry/nyc_taxi_delta_clone/_delta_log/00000000000000000001.json").where("add is not null")
add_schema = """
struct
<
numRecords:long,
minValues: struct
  <
    pickup_latitude: double,
    pickup_longitude: double
  >,
maxValues: struct
  <
    pickup_latitude: double,
    pickup_longitude: double
  >
>
"""

stats2 = (df2.select("add.path", "add.size", F.from_json("add.stats", add_schema).alias("stats"), F.col("add.stats").alias("copy"))
  .selectExpr("size", "stats.minValues.pickup_latitude as min_pickup_lat","stats.minValues.pickup_longitude as min_pickup_lon", "stats.maxValues.pickup_latitude as max_pickup_lat","stats.maxValues.pickup_longitude as max_pickup_lon")
)

stats2 = (
  stats2
    #.withColumn("width", F.expr("max_pickup_lat - min_pickup_lat"))
    #.withColumn("height", F.expr("max_pickup_lon - min_pickup_lon"))
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

stats2.display()


# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC stats2 "rect" "geometry"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`dbfs:/tmp/yousry/nyc_taxi_delta_clone`
# MAGIC WHERE pickup_latitude = CAST(40.70701 AS FLOAT) AND pickup_longitude = CAST(-74.01156 AS FLOAT)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`dbfs:/tmp/yousry/nyc_taxi_delta`
# MAGIC WHERE pickup_latitude = CAST(40.70701 AS FLOAT) AND pickup_longitude = CAST(-74.01156 AS FLOAT)

# COMMAND ----------


