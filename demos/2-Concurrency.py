# Databricks notebook source
import os
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, List
from collections import namedtuple
AsyncJobResult = namedtuple("AsyncJobResult", "input result error")

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.io.cache.enabled = false;

# COMMAND ----------

import pyspark.sql.functions as F

table_path_root = "dbfs:/tmp/delta-lake/2-concurrency"
table1 = f"{table_path_root}/append"
table2 = f"{table_path_root}/merge"
table3 = f"{table_path_root}/merge-retry"
dbutils.fs.rm(table_path_root, True)


def create_table(table_path):
  dbutils.fs.rm(table_path_root, True)
  df = spark.range(0, 1000_000)
  for i in range(1,9):
    df = df.withColumn(f"col{i}", F.expr(f"1000000 * {i} + id"))

  df.write.format("delta").mode("overwrite").save(table_path)

  df.display()

create_table(table1)

# COMMAND ----------

def run_parralell_jobs(tasks, handler):
  workers = len(tasks)
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

from delta.tables import *

def append_table(task):
  print(f"Starting task: {task}")
  dfUpdates = spark.range(10_000)
  for i in range(1,9):
      dfUpdates = dfUpdates.withColumn(f"col{i}", F.expr(f"1000000 * {i} + id"))

  dfUpdates.write.format("delta").option("userMetadata", f"commit from task {task}").mode("append").save(table1)
  print(f"Processed task: {task}")
  return f"Processed {task}"

run_parralell_jobs(range(4), append_table)

# COMMAND ----------

spark.read.format("delta").load(table1).count()

# COMMAND ----------

spark.sql(f"DESC HISTORY delta.`{table1}`").display()

# COMMAND ----------

create_table(table2)

# COMMAND ----------

def merge_into_table(task):
  print(f"Starting task: {task}")
  spark_clone = spark.newSession()
  spark_clone.conf.set("spark.databricks.delta.commitInfo.userMetadata", f"merge_into_table-task-{task}")
  dfUpdates = spark_clone.range(task, 1000_1000, 4)
  for i in range(1,9):
      dfUpdates = dfUpdates.withColumn(f"col{i}", F.expr(f"1000000 * {i} + id + {task}"))
  
  src = DeltaTable.forPath(spark_clone, table2)
  try:
    src.alias('src') \
    .merge(
      dfUpdates.alias('updates'),
      'src.id = updates.id'
    ) \
    .whenMatchedUpdate(set =
      {
        f"col{task+1}": f"updates.col{task+1}",
      }
    ) \
    .execute()
  except Exception as ex:
    ex_type = type(ex)
    print(f"Task {task} failed with {ex_type}")
  
  print(f"Processed task: {task}")
  return f"Processed {task}"

run_parralell_jobs(range(4), merge_into_table)


# COMMAND ----------

spark.sql(f"DESC HISTORY delta.`{table2}`").display()

# COMMAND ----------

create_table(table3)

# COMMAND ----------

import time
import random
from delta.exceptions import ConcurrentAppendException

def merge_into_table_with_retry(task):
  print(f"Starting task: {task}")

  def merge_it(task, trial):
    spark_clone = spark.newSession()
    commit_message = f"merge_into_table_with_retry-task-{task} : done in trial {trial}"
    spark_clone.conf.set("spark.databricks.delta.commitInfo.userMetadata", commit_message)

    dfUpdates = spark_clone.range(task, 1000_1000, 8)
    for i in range(1,9):
        dfUpdates = dfUpdates.withColumn(f"col{i}", F.expr(f"1000000 * {i} + id + {task}"))
    
    src = DeltaTable.forPath(spark_clone, table3)
    
    src.alias('src') \
      .merge(
        dfUpdates.alias('updates'),
        'src.id = updates.id'
      ) \
      .whenMatchedUpdate(set =
        {
          f"col{task+1}": f"updates.col{task+1}",
        }
      ) \
      .execute()

  counter = 0
  while counter < 5:
    print(f"Trial {counter} for task {task}")
    try:
      merge_it(task, counter)
      break
    except ConcurrentAppendException as ex: # TODO : use the right exception type 
      #ex_type = type(ex)
      print("=" * 100)
      print(f"Task {task} failed with ConcurrentAppendException, will try again in a few seconds")
      print("=" * 100)
      time.sleep(int(random.random() * 10))

    counter = counter + 1
  
  
  print(f"Processed task: {task}")
  return f"Processed {task}"

run_parralell_jobs(range(4), merge_into_table_with_retry)


# COMMAND ----------

spark.sql(f"DESC HISTORY delta.`{table3}`").display()

# COMMAND ----------


