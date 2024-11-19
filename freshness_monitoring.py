# Databricks notebook source
# MAGIC %pip install "https://ml-team-public-read.s3.us-west-2.amazonaws.com/wheels/data-monitoring/a4050ef7-b183-47a1-a145-e614628e3146/databricks_anomaly_detection-0.0.1-py3-none-any.whl"

# COMMAND ----------

# restart python so library can be installed
dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("logging_table_name", "my_logging_table")
dbutils.widgets.text("catalog_name", "my_catalog")
dbutils.widgets.text("schema_name", "my_schema")
LOGGING_TABLE_NAME = dbutils.widgets.get("logging_table_name")
CATALOG_NAME = dbutils.widgets.get("catalog_name")
SCHEMA_NAME =  dbutils.widgets.get("schema_name")


# COMMAND ----------

from databricks.data_monitoring.anomalydetection import FreshnessChecker

# Instantiate the freshness checker (add additional parameters to instantiation if customization above was performed)
freshness_checker = FreshnessChecker(
  catalog_name=CATALOG_NAME,
  schema_name=SCHEMA_NAME,
  logging_table_name=LOGGING_TABLE_NAME
)

# COMMAND ----------

# Run the checker
freshness_checker.run_freshness_checks()

# COMMAND ----------



