# Freshness Moniorng
Note: This feature is in [Private Preview](https://docs.databricks.com/en/release-notes/release-types.html). To try it, reach out to your Databricks contact or lakehouse-monitoring-feedback@databricks.com.

The software and other materials referenced ("Copyrighted Materials") are protected by US and international copyright laws and are the property of Databricks, Inc. The Copyrighted Materials are not provided under a license for public or third-party use. Accordingly, you may not access, use, copy, modify, publish, and/or distribute the Copyrighted Materials unless you have received prior written authorization or a license from Databricks to do so.

This library is subject to the preview terms. You will not, and you will not permit your Authorized Users to, copy, modify, disassemble, decompile, reverse engineer, or attempt to view or discover the source code of the private preview, in whole or in part.

# Expectation 

The software and other materials included in this repo ("Copyrighted Materials") are protected by US and international copyright laws and are the property of Databricks, Inc. The Copyrighted Materials are not provided under a license for public or third-party use. Accordingly, you may not access, use, copy, modify, publish, and/or distribute the Copyrighted Materials unless you have received prior written authorization or a license from Databricks to do so.

# QuickStart
## Step 1: Set up Notebook Task
you can  create a notebook task to tie quality directly into your Databricks Workflow:
1. Add a new Task of type `Notebook`
2. Select notebook source as `Git provider`. Git repository URL: `https://github.com/databricks/expectations`, Git reference: `main`
3. Configure path as `expectation_check_v2`
4. Select DBR 15.2+ cluster in Compute
5. Add table_name in Parameters. Key: `table_name`, Value: `<three_level_table_name>`

## Step 2: Task fails on violation
When the workflow runs, the Data Quality task will execute `ANALYZE CONSTRAINTS` on the selected table. If any constraints are violated, the task will fail and display debug information.
