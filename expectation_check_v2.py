# Databricks notebook source
# MAGIC %md
# MAGIC Please reaching out xxx if you face any issues while trying this private preview feature.
# MAGIC ## Behaviors:
# MAGIC 1. Table/monitor does not exist: throw `MONITOR_DOES_NOT_EXSIT` error
# MAGIC 2. Monitor not active: throw `MONITOR_NOT_ACTIVE` error
# MAGIC 3. Refresh metrics failure: throw `REFRESH_FAILURE`
# MAGIC 4. Any of the expectation checks fail on the table: throw `DOES_NOT_MEET_EXPECATION` error
# MAGIC 5. No expectations are configured: pass
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup

# COMMAND ----------

# DBTITLE 1,Install Lakehoue Monitoring Client Wheel
# MAGIC %pip install "databricks-sdk>=0.28.0"

# COMMAND ----------

# This step is necessary to reset the environment with our newly installed wheel.
dbutils.library.restartPython()

# COMMAND ----------



spark.conf.set("spark.databricks.sql.analyzeConstraints.enabled", True)

dbutils.widgets.text("table_name", "a.b.c")
TABLE_NAME = dbutils.widgets.get("table_name")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Check monitor exist

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorTimeSeries, MonitorInfoStatus, MonitorRefreshInfoState, MonitorMetric

w = WorkspaceClient()
try:
  info = w.quality_monitors.get(table_name=TABLE_NAME)
  if info.status != MonitorInfoStatus.MONITOR_STATUS_ACTIVE:
     raise Exception(f"[MONITOR_NOT_ACTIVE] Table does not have active monitor.")
except Exception as e:  
  raise Exception(f"[MONITOR_NOT_FOUND] Quality could not be checked due to missing monitor. Please enable the monitor and retry. Error message: {e}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Refresh metrics

# COMMAND ----------


import time
run_info = w.quality_monitors.run_refresh(table_name=TABLE_NAME)
while run_info.state in (MonitorRefreshInfoState.PENDING, MonitorRefreshInfoState.RUNNING):
  run_info = w.quality_monitors.get_refresh(table_name=TABLE_NAME, refresh_id=run_info.refresh_id)
  time.sleep(30)

if run_info.state != MonitorRefreshInfoState.SUCCESS:
  raise Exception(f"[SCAN_FAILED] Quality could not be checked due to a system error. Run Info: {run_info}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Analayze constraint and show results

# COMMAND ----------

df = spark.sql(f'''ANALYZE CONSTRAINTS FOR {TABLE_NAME}''')


# COMMAND ----------

# DBTITLE 1,Shows expectations and the expectation check results
display(df)


# COMMAND ----------

from pyspark.sql.functions import col
# if violated, throw expection
# Check if any row has "VIOLATED" in the status column
violated_count = df.filter(col("status") == "VIOLATED").count()

if violated_count > 0:
    raise Exception("[EXPECTATION_VIOLATED] Quality violations detected in data. Use debug_sql_query columns from the output dataframe to debug further.")

print("No violated statuses found.")