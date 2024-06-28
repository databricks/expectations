# Databricks notebook source

dbutils.widgets.text("table_name", "a.b.c")
table_name = dbutils.widgets.get("table_name")


# COMMAND ----------

df = spark.sql(f'''ANALYZE CONSTRAINTS FOR {table_name}''')

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import col
# if violated, throw expection
# Check if any row has "VIOLATED" in the status column
violated_count = df.filter(col("status") == "VIOLATED").count()

if violated_count > 0:
    raise Exception("Exception: 'VIOLATED' status found!")

print("No violated statuses found. Continuing execution...")