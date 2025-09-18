# Databricks notebook source
DEFAULT_WHL_URL = "https://ml-team-public-read.s3.us-west-2.amazonaws.com/wheels/data-monitoring/a4050ef7-b183-47a1-a145-e614628e3146/databricks_anomaly_detection-0.0.18-py3-none-any.whl"

try:
  from dbruntime.databricks_repl_context import get_context
  workspace_id = get_context().workspaceId
  if workspace_id in [
    "6051921418418893", # e2
    "5206439413157315",  # field eng demo workspace
    "984752964297111",  # field-eng workspace
    "2548836972759138", # clf
  ]:
    DEFAULT_WHL_URL = "https://ml-team-public-read.s3.us-west-2.amazonaws.com/wheels/data-monitoring/staging/databricks_anomaly_detection-0.0.18-py3-none-any.whl"
except Exception as e:
  print("Error while aquiring workspace id: {e}")

dbutils.widgets.text("whl_override", DEFAULT_WHL_URL)
WHL_URL = dbutils.widgets.get("whl_override").strip() or DEFAULT_WHL_URL

# COMMAND ----------

get_ipython().run_line_magic("pip", f"install \"{WHL_URL}\"")

# COMMAND ----------

# restart python so library can be installed
dbutils.library.restartPython()

# COMMAND ----------

import json

from databricks.data_monitoring.anomalydetection.detection import run_anomaly_detection
from databricks.data_monitoring.anomalydetection.metric_config import (
    FreshnessConfig, CompletenessConfig
)

dbutils.widgets.text("catalog_name", "my_catalog")
dbutils.widgets.text("schema_name", "my_schema")
dbutils.widgets.text("metric_configs", "[]")
dbutils.widgets.text("logging_table_name", "")

CATALOG_NAME = dbutils.widgets.get("catalog_name")
SCHEMA_NAME =  dbutils.widgets.get("schema_name")
LOGGING_TABLE_NAME = dbutils.widgets.get("logging_table_name")

# Convert metric_configs JSON to objects
dict_list = json.loads(dbutils.widgets.get("metric_configs"))
decoded_configs = []
for config in dict_list:
    metric_type = config.pop("metric_type", None)
    if metric_type == "FreshnessConfig":
        decoded_configs.append(FreshnessConfig.from_dict(config))
    elif metric_type == "CompletenessConfig":
        decoded_configs.append(CompletenessConfig.from_dict(config))
    else:
        raise ValueError(f"Unsupported metric_type: {metric_type}")

# COMMAND ----------

current_run_logging_table = run_anomaly_detection(
    catalog_name=CATALOG_NAME,
    schema_name=SCHEMA_NAME,
    metric_configs=decoded_configs,
    logging_table_name=LOGGING_TABLE_NAME if len(LOGGING_TABLE_NAME) > 0 else None
)

# COMMAND ----------

# Display current run's logging table for all checks enabled.
display(current_run_logging_table)