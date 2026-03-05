# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Build gold_kpi_5m and gold_anomaly
# MAGIC - 5-minute bucket aggregates: revenue, pay_success_rate, DAU
# MAGIC - Anomaly detection: rolling baseline same time-of-day (7d), z_score < -3 marks anomaly

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 5-minute KPI (by platform/version/region/provider)
# MAGIC CREATE OR REPLACE TABLE main.cursor_gaming.gold_kpi_5m AS
# MAGIC WITH pay_5m AS (
# MAGIC   SELECT
# MAGIC     date_trunc('minute', (floor(unix_timestamp(event_ts) / 300) * 300).cast('timestamp')) AS bucket_5m,
# MAGIC     platform,
# MAGIC     app_version,
# MAGIC     region,
# MAGIC     provider,
# MAGIC     count(*) AS pay_attempts,
# MAGIC     sum(case when payment_status = 'success' then 1 else 0 end) AS pay_success,
# MAGIC     sum(amount) AS revenue,
# MAGIC     count(distinct user_id) AS dau_5m
# MAGIC   FROM main.cursor_gaming.silver_payments
# MAGIC   GROUP BY 1, 2, 3, 4, 5
# MAGIC )
# MAGIC SELECT
# MAGIC   bucket_5m,
# MAGIC   platform,
# MAGIC   app_version,
# MAGIC   region,
# MAGIC   provider,
# MAGIC   revenue,
# MAGIC   pay_attempts,
# MAGIC   pay_success,
# MAGIC   round(pay_success * 1.0 / nullif(pay_attempts, 0), 4) AS pay_success_rate,
# MAGIC   dau_5m
# MAGIC FROM pay_5m;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold_anomaly: inline KPI + baseline (no temp views for job reliability)
# MAGIC CREATE OR REPLACE TABLE main.cursor_gaming.gold_anomaly AS
# MAGIC WITH kpi_overall AS (
# MAGIC   SELECT bucket_5m, sum(revenue) AS revenue, sum(pay_attempts) AS pay_attempts,
# MAGIC     sum(pay_success) AS pay_success, sum(dau_5m) AS dau_5m,
# MAGIC     sum(pay_success) * 1.0 / nullif(sum(pay_attempts), 0) AS pay_success_rate
# MAGIC   FROM main.cursor_gaming.gold_kpi_5m
# MAGIC   GROUP BY bucket_5m
# MAGIC ),
# MAGIC baseline AS (
# MAGIC   SELECT hour(bucket_5m) AS h, minute(bucket_5m) AS m,
# MAGIC     avg(pay_success_rate) AS baseline_rate, stddev(pay_success_rate) AS std_rate,
# MAGIC     avg(revenue) AS baseline_revenue, stddev(revenue) AS std_revenue
# MAGIC   FROM kpi_overall
# MAGIC   GROUP BY hour(bucket_5m), minute(bucket_5m)
# MAGIC ),
# MAGIC with_baseline AS (
# MAGIC   SELECT k.bucket_5m, k.revenue, k.pay_success_rate, k.pay_attempts, k.dau_5m,
# MAGIC     b.baseline_rate, b.std_rate, b.baseline_revenue, b.std_revenue
# MAGIC   FROM kpi_overall k
# MAGIC   JOIN baseline b ON hour(k.bucket_5m) = b.h AND minute(k.bucket_5m) = b.m
# MAGIC ),
# MAGIC rate_anomaly AS (
# MAGIC   SELECT bucket_5m, 'pay_success_rate' AS metric,
# MAGIC     pay_success_rate AS actual, baseline_rate AS baseline, coalesce(std_rate, 0) AS std,
# MAGIC     (pay_success_rate - baseline_rate) / greatest(coalesce(std_rate, 0), 1e-9) AS z_score,
# MAGIC     (pay_success_rate - baseline_rate) / greatest(coalesce(std_rate, 0), 1e-9) < -3 AS is_anomaly,
# MAGIC     (baseline_rate - pay_success_rate) * pay_attempts * 50 AS impact_estimated
# MAGIC   FROM with_baseline
# MAGIC ),
# MAGIC rev_anomaly AS (
# MAGIC   SELECT bucket_5m, 'revenue' AS metric,
# MAGIC     revenue AS actual, baseline_revenue AS baseline, coalesce(std_revenue, 0) AS std,
# MAGIC     (revenue - baseline_revenue) / greatest(coalesce(std_revenue, 0), 1e-9) AS z_score,
# MAGIC     (revenue - baseline_revenue) / greatest(coalesce(std_revenue, 0), 1e-9) < -3 AS is_anomaly,
# MAGIC     (baseline_revenue - revenue) AS impact_estimated
# MAGIC   FROM with_baseline
# MAGIC )
# MAGIC SELECT * FROM rate_anomaly UNION ALL SELECT * FROM rev_anomaly;
