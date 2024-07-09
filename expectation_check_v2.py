# Databricks notebook source
# MAGIC %md
# MAGIC Please reach out to lakehouse-monitoring-feedback@databricks.com if you face any issues.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup

# COMMAND ----------

# DBTITLE 1,Install Lakehouse Monitoring Client Wheel
# MAGIC %pip install "databricks-sdk>=0.28.0" --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

spark.conf.set("spark.databricks.sql.analyzeConstraints.enabled", True)

dbutils.widgets.text("table_name", "a.b.c")
TABLE_NAME = dbutils.widgets.get("table_name")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Check monitor exist

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorInfoStatus, MonitorRefreshInfoState

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
# MAGIC ## Step 4: Analyze constraints and show results

# COMMAND ----------

df = spark.sql(f'''ANALYZE CONSTRAINTS FOR {TABLE_NAME}''')
display(df)

# COMMAND ----------

from pyspark.sql.functions import collect_list, concat_ws, col

# Collect all constraint expressions where status is "VIOLATED"
violated_df = df.filter(col("status") == "VIOLATED")
violated_count = violated_df.count()
violated_constraints_str = df.filter(col("status") == "VIOLATED") \
                            .agg(concat_ws(", ", collect_list("constraint_expr")).alias("violated_constraints")) \
                            .collect()[0]["violated_constraints"]

# COMMAND ----------

css = """
<style>
.expectations table {
    width: 50%;
    border-collapse: collapse;
    font-size: 18px;
    margin-top: 10px;
    table-layout: auto;
    text-align: left;
}
.expectations tbody th, .expectations thead th, .expectations tbody td {
    padding: 12px;
    border: 1px solid #ddd;
    text-align: center;
}
.expectations th {
    background-color: #f2f2f2;
}
.expectations tr:nth-child(even) {
    background-color: #f9f9f9;
}
</style>
"""

html_content = f"""
<!DOCTYPE html>
<html>
<head>
    {css}
</head>
<body class="expectations">
    <p><b>Debugging Information:</b></p>
    <ul>
"""
# Collect the violated rows
violated_rows = violated_df.collect()

# Process each violated row
for row in violated_rows:
    query = row["debug_sql_query"]
    constraint = row["constraint_expr"]
    try:
        result_df = spark.sql(query).limit(10)
        if "profile_metrics" in query:
            result_df = result_df.drop("quantiles")
            result_df = result_df.drop("frequent_items")
        result_html = result_df.toPandas().to_html(index=False, escape=False)
        
        # Append the result to the HTML content
        html_content += f"""
        <li>
            The quality expectation <strong><code>{constraint}</code></strong> has failed. Please review the details below:
            {result_html}
        </li>
        """
    except Exception as e:
        # Handle any SQL execution errors
        html_content += f"""
        <li>
            The quality expectation <strong><code>{constraint}</code></strong> has failed. Please review the details below:
            <pre>Error executing query: {str(e)}</pre>
        </li>
        """

# Close the HTML tags
html_content += """
    </ul>
</body>
</html>
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show results

# COMMAND ----------

if violated_count > 0:
  displayHTML(html_content)
  raise Exception(f"[EXPECTATION_VIOLATED] There are {violated_count} violations detected in the table from the following constraints: {violated_constraints_str}")
else:
  print("No violated statuses found.")
