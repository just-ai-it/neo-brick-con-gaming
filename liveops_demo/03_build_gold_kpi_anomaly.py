# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Build gold_kpi_5m and gold_anomaly
# MAGIC - 5-minute bucket aggregates: revenue, pay_success_rate, DAU
# MAGIC - Anomaly detection: rolling baseline same time-of-day (7d), z_score < -3 marks anomaly

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 5-minute KPI (by platform/version/region/provider)
# MAGIC CREATE OR REPLACE TABLE cursor_gaming.gaming.gold_kpi_5m AS
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
# MAGIC   FROM cursor_gaming.gaming.silver_payments
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
# MAGIC -- Overall 5-minute KPI (for anomaly detection)
# MAGIC CREATE OR REPLACE TEMP VIEW v_kpi_5m_overall AS
# MAGIC SELECT
# MAGIC   bucket_5m,
# MAGIC   sum(revenue) AS revenue,
# MAGIC   sum(pay_attempts) AS pay_attempts,
# MAGIC   sum(pay_success) AS pay_success,
# MAGIC   sum(pay_success) * 1.0 / nullif(sum(pay_attempts), 0) AS pay_success_rate,
# MAGIC   sum(dau_5m) AS dau_5m
# MAGIC FROM cursor_gaming.gaming.gold_kpi_5m
# MAGIC GROUP BY bucket_5m;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rolling baseline: same minute mean/std over past days (here 3 days simulates 7d)
# MAGIC CREATE OR REPLACE TEMP VIEW v_baseline AS
# MAGIC WITH kpi AS (
# MAGIC   SELECT bucket_5m, revenue, pay_success_rate, dau_5m
# MAGIC   FROM v_kpi_5m_overall
# MAGIC ),
# MAGIC with_minute AS (
# MAGIC   SELECT *,
# MAGIC     minute(bucket_5m) AS m,
# MAGIC     hour(bucket_5m) AS h
# MAGIC   FROM kpi
# MAGIC ),
# MAGIC baseline AS (
# MAGIC   SELECT
# MAGIC     h, m,
# MAGIC     avg(pay_success_rate) AS baseline_rate,
# MAGIC     stddev(pay_success_rate) AS std_rate,
# MAGIC     avg(revenue) AS baseline_revenue,
# MAGIC     stddev(revenue) AS std_revenue
# MAGIC   FROM with_minute
# MAGIC   GROUP BY h, m
# MAGIC )
# MAGIC SELECT * FROM baseline;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold_anomaly: each bucket vs baseline, z_score < -3 => anomaly
# MAGIC CREATE OR REPLACE TABLE cursor_gaming.gaming.gold_anomaly AS
# MAGIC WITH kpi AS (
# MAGIC   SELECT bucket_5m, revenue, pay_success_rate, pay_attempts, dau_5m
# MAGIC   FROM v_kpi_5m_overall
# MAGIC ),
# MAGIC with_baseline AS (
# MAGIC   SELECT k.bucket_5m, k.revenue, k.pay_success_rate, k.pay_attempts, k.dau_5m,
# MAGIC     b.baseline_rate, b.std_rate, b.baseline_revenue, b.std_revenue
# MAGIC   FROM kpi k
# MAGIC   JOIN v_baseline b ON hour(k.bucket_5m) = b.h AND minute(k.bucket_5m) = b.m
# MAGIC ),
# MAGIC rate_anomaly AS (
# MAGIC   SELECT
# MAGIC     bucket_5m, 'pay_success_rate' AS metric,
# MAGIC     pay_success_rate AS actual, baseline_rate AS baseline, coalesce(std_rate, 0) AS std,
# MAGIC     CASE WHEN coalesce(std_rate, 0) > 0 THEN (pay_success_rate - baseline_rate) / std_rate ELSE 0 END AS z_score,
# MAGIC     (pay_success_rate - baseline_rate) / nullif(std_rate, 0) < -3 AS is_anomaly,
# MAGIC     (baseline_rate - pay_success_rate) * pay_attempts * 50 AS impact_estimated
# MAGIC   FROM with_baseline
# MAGIC ),
# MAGIC rev_anomaly AS (
# MAGIC   SELECT
# MAGIC     bucket_5m, 'revenue' AS metric,
# MAGIC     revenue AS actual, baseline_revenue AS baseline, coalesce(std_revenue, 0) AS std,
# MAGIC     CASE WHEN coalesce(std_revenue, 0) > 0 THEN (revenue - baseline_revenue) / std_revenue ELSE 0 END AS z_score,
# MAGIC     (revenue - baseline_revenue) / nullif(std_revenue, 0) < -3 AS is_anomaly,
# MAGIC     (baseline_revenue - revenue) AS impact_estimated
# MAGIC   FROM with_baseline
# MAGIC )
# MAGIC SELECT * FROM rate_anomaly UNION ALL SELECT * FROM rev_anomaly;
