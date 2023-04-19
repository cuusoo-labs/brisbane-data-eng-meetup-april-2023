# Databricks notebook source
# DBTITLE 1,Disable disk (parquet) cache
spark.conf.set("spark.databricks.io.cache.enabled", "false")

# COMMAND ----------

protocol = {
    "protocol": {
        "minReaderVersion": 1,
        "minWriterVersion": 1
    }
}
print(protocol)

# COMMAND ----------

from uuid import uuid4
from pprint import pprint
import time
import json
schema = {
    "type":"struct",
    "fields":[
        {"name":"id", "type":"long", "nullable":False, "metadata":{}},
        {"name":"name", "type":"string", "nullable":False, "metadata":{}},
    ]
}
metadata = {
    "metaData": {
        "id": str(uuid4()),
        "name": None,
        "description": None,
        "format": {
            "provider": "parquet",
            "options": {}
        },
        "schemaString": json.dumps(schema),
        "partitionColumns": [],
        "createdTime": int(time.time_ns() / 1000000),
        "configuration": {}
    }
}
pprint(metadata)

# COMMAND ----------

import pandas as pd
def write_parquet_file(data, path):
    df = pd.DataFrame(data)
    df.to_parquet(path.replace("dbfs:/", "/dbfs/"), )

# COMMAND ----------

import uuid
table_path = f"dbfs:/tmp/delta-lake/{uuid.uuid4()}"
dbutils.fs.rm(table_path, True)

protcol_str = json.dumps(protocol)
metadata_str = json.dumps(metadata)

content = protcol_str + "\n" + metadata_str

dbutils.fs.put(f"{table_path}/_delta_log/00000000000000000000.json", content)
print(f"table_path: {table_path}")
spark.read.format("delta").load(table_path).show()

# COMMAND ----------

write_parquet_file( {"id": [1,2], "name": ["first", "second"]}, f"{table_path}/file1.parquet")
add = {
    "add": {
        "path": "file1.parquet",
        "size": dbutils.fs.ls(f"{table_path}/file1.parquet")[0].size,
        "partitionValues": {},
        "modificationTime": int(time.time_ns() / 1000000),
        "dataChange": True,
        "tags": None
    }
}

add_content = json.dumps(add)
dbutils.fs.put(f"{table_path}/_delta_log/00000000000000000001.json", add_content, True)

spark.read.format("delta").load(table_path).display()

# COMMAND ----------

write_parquet_file( {"id": [1, 2], "name": ["first", "second updated"]}, f"{table_path}/file2.parquet")
add = {
    "add": {
        "path": "file2.parquet",
        "size": dbutils.fs.ls(f"{table_path}/file2.parquet")[0].size,
        "partitionValues": {},
        "modificationTime": int(time.time_ns() / 1000000),
        "dataChange": True,
        "tags": None
    }
}

remove = {
    "remove": {
        "path": "file1.parquet",
        "deletionTimestamp": int(time.time_ns() / 1000000),
        "dataChange": True,
        "size": dbutils.fs.ls(f"{table_path}/file1.parquet")[0].size
    }
}

update_content = json.dumps(remove) + "\n" + json.dumps(add)
dbutils.fs.put(f"{table_path}/_delta_log/00000000000000000002.json", update_content, True)

spark.read.format("delta").load(table_path).display()

# COMMAND ----------

display(dbutils.fs.ls(table_path))

# COMMAND ----------

spark.read.format("delta").option("versionAsOf", "1").load(table_path).display()

# COMMAND ----------

write_parquet_file( {"id": [11, 22], "name": ["eleven", "twenty second"]}, f"{table_path}/file-dummy.parquet")
display(dbutils.fs.ls(table_path))

# COMMAND ----------

spark.read.format("delta").load(table_path).display()

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled","false")
spark.sql(f"VACUUM delta.`{table_path}` RETAIN 0 HOURS")

# COMMAND ----------

display(dbutils.fs.ls(table_path))

# COMMAND ----------

spark.read.format("delta").load(table_path).display()

# COMMAND ----------

spark.read.format("delta").option("versionAsOf", "1").load(table_path).display()

# COMMAND ----------

spark.sql(f"DESC HISTORY delta.`{table_path}`").display()

# COMMAND ----------

for i in range(3, 500):
  write_parquet_file( {"id": [i, i +1], "name": [f"item {i}", f"item {i+ 1}"]}, f"{table_path}/new-file-{i}.parquet")
  version_str = f"{i:020d}"
  add = {
      "add": {
          "path": f"new-file-{i}.parquet",
          "size": dbutils.fs.ls(f"{table_path}/new-file-{i}.parquet")[0].size,
          "partitionValues": {},
          "modificationTime": int(time.time_ns() / 1000000),
          "dataChange": True,
          "tags": None
      }
  }

  add_content = json.dumps(add)
  dbutils.fs.put(f"{table_path}/_delta_log/{version_str}.json", add_content, True)

# COMMAND ----------

display(dbutils.fs.ls(table_path + "/_delta_log"))

# COMMAND ----------

spark.read.format("delta").load(table_path).display()

# COMMAND ----------

spark.sql(f"INSERT INTO delta.`{table_path}` VALUES (1000000, 'item 1000000')")

# COMMAND ----------

display(dbutils.fs.ls(table_path + "/_delta_log"))

# COMMAND ----------

spark.read.format("delta").load(table_path).display()

# COMMAND ----------

spark.sql(f"OPTIMIZE delta.`{table_path}`").display()

# COMMAND ----------

display(dbutils.fs.ls(table_path))

# COMMAND ----------

spark.read.format("delta").load(table_path).display()

# COMMAND ----------


