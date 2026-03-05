# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Create LiveOps Demo Schema and Tables
# MAGIC Unity Catalog: `main.cursor_gaming`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS main.cursor_gaming
# MAGIC COMMENT 'LiveOps demo: real-time monitoring + anomaly alerting + RCA';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Behavioral events
# MAGIC CREATE TABLE IF NOT EXISTS main.cursor_gaming.silver_game_events (
# MAGIC   event_ts TIMESTAMP NOT NULL,
# MAGIC   user_id STRING NOT NULL,
# MAGIC   region STRING,
# MAGIC   platform STRING,
# MAGIC   device_model STRING,
# MAGIC   app_version STRING,
# MAGIC   event_name STRING
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Events: login, level_start, purchase_attempt, purchase_success, purchase_fail';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Payment outcomes (with dimensions for drill-down)
# MAGIC CREATE TABLE IF NOT EXISTS main.cursor_gaming.silver_payments (
# MAGIC   event_ts TIMESTAMP NOT NULL,
# MAGIC   user_id STRING NOT NULL,
# MAGIC   amount DOUBLE,
# MAGIC   currency STRING,
# MAGIC   provider STRING,
# MAGIC   payment_status STRING,
# MAGIC   error_code STRING,
# MAGIC   platform STRING,
# MAGIC   app_version STRING,
# MAGIC   region STRING
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Payment result: success/fail, error_code; platform/version/region for drill-down';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 5-minute KPI aggregates (multi-dimensional)
# MAGIC CREATE TABLE IF NOT EXISTS main.cursor_gaming.gold_kpi_5m (
# MAGIC   bucket_5m TIMESTAMP NOT NULL,
# MAGIC   platform STRING,
# MAGIC   app_version STRING,
# MAGIC   region STRING,
# MAGIC   provider STRING,
# MAGIC   revenue DOUBLE,
# MAGIC   pay_attempts BIGINT,
# MAGIC   pay_success BIGINT,
# MAGIC   pay_success_rate DOUBLE,
# MAGIC   dau_5m BIGINT
# MAGIC ) USING DELTA
# MAGIC COMMENT '5-minute KPI aggregates, multi-dimensional';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Anomaly detection results
# MAGIC CREATE TABLE IF NOT EXISTS main.cursor_gaming.gold_anomaly (
# MAGIC   bucket_5m TIMESTAMP NOT NULL,
# MAGIC   metric STRING NOT NULL,
# MAGIC   actual DOUBLE,
# MAGIC   baseline DOUBLE,
# MAGIC   std DOUBLE,
# MAGIC   z_score DOUBLE,
# MAGIC   is_anomaly BOOLEAN,
# MAGIC   impact_estimated DOUBLE
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Anomaly detection: rolling baseline + z_score';

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN main.cursor_gaming;
