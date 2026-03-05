# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Dashboard Data (for Lakeview)
# MAGIC Scenes 1&2: overview trend + anomaly windows (highlight)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 1: Revenue & Payment Success Rate (5-min granularity)
# MAGIC For line charts: time vs revenue, time vs pay_success_rate

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use in Lakeview as Revenue / Pay Success Rate data source
# MAGIC SELECT
# MAGIC   bucket_5m AS time_bucket,
# MAGIC   revenue,
# MAGIC   pay_success_rate * 100 AS pay_success_rate_pct,
# MAGIC   pay_attempts,
# MAGIC   pay_success,
# MAGIC   dau_5m
# MAGIC FROM (
# MAGIC   SELECT bucket_5m, sum(revenue) AS revenue, sum(pay_attempts) AS pay_attempts,
# MAGIC     sum(pay_success) AS pay_success, sum(dau_5m) AS dau_5m,
# MAGIC     sum(pay_success) * 1.0 / nullif(sum(pay_attempts), 0) AS pay_success_rate
# MAGIC   FROM cursor_gaming.gaming.gold_kpi_5m
# MAGIC   GROUP BY bucket_5m
# MAGIC ) t
# MAGIC ORDER BY time_bucket;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 2: Anomaly windows (for highlighting)
# MAGIC Mark anomaly intervals (e.g. 10:05-10:40) on the chart

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   bucket_5m AS anomaly_start,
# MAGIC   metric,
# MAGIC   actual,
# MAGIC   baseline,
# MAGIC   z_score,
# MAGIC   impact_estimated
# MAGIC FROM cursor_gaming.gaming.gold_anomaly
# MAGIC WHERE is_anomaly = true
# MAGIC ORDER BY bucket_5m;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 3: Anomaly summary (estimated loss, affected DAU share)
# MAGIC For Scene 2 display

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH anomaly_windows AS (
# MAGIC   SELECT bucket_5m, metric, actual, baseline, impact_estimated
# MAGIC   FROM cursor_gaming.gaming.gold_anomaly
# MAGIC   WHERE is_anomaly = true
# MAGIC ),
# MAGIC kpi AS (
# MAGIC   SELECT bucket_5m, sum(revenue) AS revenue, sum(dau_5m) AS dau_5m
# MAGIC   FROM cursor_gaming.gaming.gold_kpi_5m GROUP BY bucket_5m
# MAGIC )
# MAGIC SELECT
# MAGIC   min(a.bucket_5m) AS window_start,
# MAGIC   max(a.bucket_5m) AS window_end,
# MAGIC   sum(a.impact_estimated) AS estimated_loss,
# MAGIC   (SELECT sum(dau_5m) FROM kpi k WHERE k.bucket_5m IN (SELECT bucket_5m FROM cursor_gaming.gaming.gold_anomaly WHERE is_anomaly = true)) AS affected_dau
# MAGIC FROM anomaly_windows a;
