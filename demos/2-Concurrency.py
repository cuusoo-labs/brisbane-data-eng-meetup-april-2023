# Databricks notebook source
# MAGIC %sql
# MAGIC SET spark.databricks.io.cache.enabled = false;

# COMMAND ----------

table_path = "dbfs:/tmp/demo/2-concurrency"
table_append = "dbfs:/tmp/demo/2-concurrency/append"
table_merge = "dbfs:/tmp/demo/2-concurrency/merge"
dbutils.fs.rm(table_path, True)

# COMMAND ----------

import pyspark.sql.functions as F
df = spark.range(1000_000)
for i in range(1,9):
  df = df.withColumn(f"col{i}", F.expr(f"1000000 * {i} + id"))

df.write.format("delta").mode("overwrite").save(table_append)

# COMMAND ----------

from delta.tables import *

def append_table(col):
  dfUpdates = spark.range(10_000)
  for i in range(1,9):
      dfUpdates = dfUpdates.withColumn(f"col{i}", F.expr(f"1000000 * {i} + id"))

  dfUpdates.write.format("delta").mode("append").save(table_append)


def update_table(col):
  dfUpdates = spark.range(1000_000)
  for i in range(1,9):
    if f"col{i}" != col :
      dfUpdates = dfUpdates.withColumn(f"col{i}", F.expr(f"1000000 * {i} + id"))
    else:
      dfUpdates = dfUpdates.withColumn(col, F.expr(f"1000000 * {i} + id + 1"))

  src = DeltaTable.forPath(spark, table_update)
  src.alias('src') \
    .merge(
      dfUpdates.alias('updates'),
      'src.id = updates.id'
    ) \
    .whenMatchedUpdate(set =
      {
        f"{col}": f"updates.{col}",
      }
    ) \
    .execute()

# COMMAND ----------

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, List
from collections import namedtuple
AsyncJobResult = namedtuple("AsyncJobResult", "input result error")

# COMMAND ----------

def run_task_update(task: str) -> int:
  #spark.sparkContext.setLocalProperty("spark.scheduler.pool", f"pool-{task}")
  #spark.sparkContext.setJobDescription(f"job-for-{table}")
  print(f"Processing task: {task}")
  update_table(task)
  print(f"Processed task: {task}")
  #col1 = spark.table(table).columns[0]
  #return spark.sql(f"SELECT COUNT(DISTINCT {col1}) FROM {table}").first()[0]
  return f"Processed {task}"

def run_task_append(task: str) -> int:
  #spark.sparkContext.setLocalProperty("spark.scheduler.pool", f"pool-{task}")
  #spark.sparkContext.setJobDescription(f"job-for-{table}")
  print(f"Processing task: {task}")
  append_table(task)
  print(f"Processed task: {task}")
  #col1 = spark.table(table).columns[0]
  #return spark.sql(f"SELECT COUNT(DISTINCT {col1}) FROM {table}").first()[0]
  return f"Processed {task}"

# COMMAND ----------

tasks = [f"col{x}" for x in range(1,9)]

# COMMAND ----------

def run_parralell_jobs(handler, workers):
  if workers and len(tasks) < workers:
    workers = len(tasks)

  print(f"Number of processing threads: {workers}")

  results = []
  with ThreadPoolExecutor(max_workers=workers) as executor:
    futures = {
        executor.submit(handler, item): index
        for index, item in enumerate(tasks)
    }

    for future in as_completed(futures):
        index = futures[future]
        try:
            future_result = future.result()
        except Exception as exc:
            result = AsyncJobResult(tasks[index], None, exc)
        else:
            result = AsyncJobResult(tasks[index], future_result, None)
        finally:
            results.append(result)

  for x in results:
    print(x)

# COMMAND ----------

run_parralell_jobs(run_task_append, 8)

# COMMAND ----------

spark.read.format("delta").load(table_append).count()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY delta.`table_append`table_append
