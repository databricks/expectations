# Databricks notebook source
dbutils.widgets.text("whl", "https://ml-team-public-read.s3.us-west-2.amazonaws.com/wheels/data-monitoring/a4050ef7-b183-47a1-a145-e614628e3146/databricks_anomaly_detection-0.0.10-py3-none-any.whl")
WHL_URL = dbutils.widgets.get("whl")

# COMMAND ----------

get_ipython().run_line_magic("pip", f"install \"{WHL_URL}\"")

# COMMAND ----------

# restart python so library can be installed
dbutils.library.restartPython()

# COMMAND ----------

from datetime import timedelta
import json

dbutils.widgets.text("logging_table_name", "my_logging_table")
dbutils.widgets.text("catalog_name", "my_catalog")
dbutils.widgets.text("schema_name", "my_schema")
dbutils.widgets.text("tables_to_skip", "bad_table1, bad_table2")
dbutils.widgets.text("table_threshold_overrides", "")
dbutils.widgets.text("static_table_threshold_override", "")
dbutils.widgets.text("tables_to_scan", "")
dbutils.widgets.text("event_timestamp_col_names", "")

LOGGING_TABLE_NAME = dbutils.widgets.get("logging_table_name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")
SCHEMA_NAME =  dbutils.widgets.get("schema_name")
TABLES_TO_SKIP = dbutils.widgets.get("tables_to_skip")
TABLES_TO_SCAN = dbutils.widgets.get("tables_to_scan")
TABLE_THRESHOLD_OVERRIDES = dbutils.widgets.get("table_threshold_overrides")
STATIC_TABLE_THRESHOLD_OVERRIDE = dbutils.widgets.get("static_table_threshold_override")
TABLES_TO_SCAN = dbutils.widgets.get("tables_to_scan")
EVENT_TIMESTAMP_COL_NAMES = dbutils.widgets.get("event_timestamp_col_names")

# Convert the comma-separated string to a list of strings
tables_to_skip_list = [table.strip() for table in TABLES_TO_SKIP.split(",") if table]
tables_to_scan_list = [table.strip() for table in TABLES_TO_SCAN.split(",") if table]
event_timestamp_col_names_list = [col.strip() for col in EVENT_TIMESTAMP_COL_NAMES.split(",") if col]

# Convert Json String of Table Override Dict into Dict[str, timedelta]. Assumption that thresholds are floats in seconds.
table_threshold_overrides = {key: timedelta(seconds=float(value)) for key, value in json.loads(TABLE_THRESHOLD_OVERRIDES).items()} if TABLE_THRESHOLD_OVERRIDES else None

# Convert static table threshold override into timedelta. Assumption that thresholds are floats in seconds.
static_table_threshold_override = timedelta(seconds=float(STATIC_TABLE_THRESHOLD_OVERRIDE)) if STATIC_TABLE_THRESHOLD_OVERRIDE else None

# COMMAND ----------

from databricks.data_monitoring.anomalydetection import FreshnessChecker

# Instantiate the freshness checker (add additional parameters to instantiation if customization above was performed)
freshness_checker = FreshnessChecker(
  catalog_name=CATALOG_NAME,
  schema_name=SCHEMA_NAME,
  logging_table_name=LOGGING_TABLE_NAME,
  tables_to_skip=tables_to_skip_list,
  tables_to_scan=tables_to_scan_list,
  table_threshold_overrides=table_threshold_overrides,
  static_table_threshold_override=static_table_threshold_override,
  event_timestamp_col_names=event_timestamp_col_names_list
)

# COMMAND ----------

# Run the checker
freshness_checker.run_freshness_checks()

# COMMAND ----------



