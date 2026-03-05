# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Root-cause Drill-down (click to explore)
# MAGIC Scene 4: open root cause #1 -> dimension breakdown + trend + sample logs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Input: anomaly time window (can be passed from RCA step)
# MAGIC Default 10:00-11:00 for demo

# COMMAND ----------

dbutils.widgets.text("window_start", "2025-03-04 10:00:00", "Window Start (UTC)")
dbutils.widgets.text("window_end", "2025-03-04 11:00:00", "Window End (UTC)")

# COMMAND ----------

ws = dbutils.widgets.get("window_start")
we = dbutils.widgets.get("window_end")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Slice by platform + version (root cause #1: iOS 1.2.7)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Platform x version: payment success rate
# MAGIC SELECT platform, app_version,
# MAGIC   sum(pay_attempts) AS attempts,
# MAGIC   sum(pay_success) AS success,
# MAGIC   round(sum(pay_success)*1.0/nullif(sum(pay_attempts),0), 4) AS pay_success_rate
# MAGIC FROM main.liveops_demo.gold_kpi_5m
# MAGIC WHERE bucket_5m >= cast('$window_start' AS timestamp)
# MAGIC   AND bucket_5m < cast('$window_end' AS timestamp)
# MAGIC GROUP BY platform, app_version
# MAGIC ORDER BY pay_success_rate;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Slice by region (e.g. SG)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT region, platform, app_version,
# MAGIC   sum(pay_attempts) AS attempts, sum(pay_success) AS success,
# MAGIC   round(sum(pay_success)*1.0/nullif(sum(pay_attempts),0), 4) AS pay_success_rate
# MAGIC FROM main.liveops_demo.gold_kpi_5m
# MAGIC WHERE bucket_5m >= cast('$window_start' AS timestamp)
# MAGIC   AND bucket_5m < cast('$window_end' AS timestamp)
# MAGIC GROUP BY region, platform, app_version
# MAGIC ORDER BY pay_success_rate;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top Error Codes (root cause #2: TOKEN_EXPIRED spike)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT coalesce(error_code, 'SUCCESS') AS error_code,
# MAGIC   count(*) AS cnt
# MAGIC FROM main.liveops_demo.silver_payments
# MAGIC WHERE event_ts >= cast('$window_start' AS timestamp)
# MAGIC   AND event_ts < cast('$window_end' AS timestamp)
# MAGIC GROUP BY error_code
# MAGIC ORDER BY cnt DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Slice by payment provider (root cause #3: Stripe iOS)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT provider, platform, app_version,
# MAGIC   sum(pay_attempts) AS attempts, sum(pay_success) AS success,
# MAGIC   round(sum(pay_success)*1.0/nullif(sum(pay_attempts),0), 4) AS pay_success_rate
# MAGIC FROM main.liveops_demo.gold_kpi_5m
# MAGIC WHERE bucket_5m >= cast('$window_start' AS timestamp)
# MAGIC   AND bucket_5m < cast('$window_end' AS timestamp)
# MAGIC GROUP BY provider, platform, app_version
# MAGIC ORDER BY pay_success_rate;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample payment/error logs (summary rows)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event_ts, user_id, provider, payment_status, error_code, amount
# MAGIC FROM main.liveops_demo.silver_payments
# MAGIC WHERE event_ts >= cast('$window_start' AS timestamp)
# MAGIC   AND event_ts < cast('$window_end' AS timestamp)
# MAGIC   AND payment_status = 'fail'
# MAGIC ORDER BY event_ts
# MAGIC LIMIT 20;
