# Databricks notebook source [THIS IS ON STAGING BRANCH]
# MAGIC %pip install "s3://ml-team-public-read/wheels/data-monitoring/staging/abcd1234-5678-90ef-ghij-klmnopqrstuv/databricks_anomaly_detection-staging-py3-none-any.whl"


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



