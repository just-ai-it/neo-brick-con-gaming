# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - GenAI Incident Detective (RCA Agent)
# MAGIC Scene 3: natural language input -> Agent runs 3 steps -> root-cause cards + evidence + actions
# MAGIC Principle: **Answers must cite data evidence (table/time window/compare scope)**; read-only, Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Preset question buttons (fallback)
# MAGIC - "Explain anomaly" / "Top drivers" / "Generate report"

# COMMAND ----------

# Preset questions (can be UI buttons)
PRESET_QUESTIONS = [
    "Explain why revenue dropped after 10:00 today. Give me top 3 root causes, evidence (dimensions/numbers/compare window), and recommended actions.",
    "What caused the revenue drop after 10:00? Top 3 root causes with evidence.",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Tool — anomaly time window

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tool 1: anomaly time window
# MAGIC SELECT bucket_5m, metric, actual, baseline, z_score, impact_estimated
# MAGIC FROM main.cursor_gaming.gold_anomaly
# MAGIC WHERE is_anomaly = true
# MAGIC ORDER BY bucket_5m;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Tool — dimension breakdown (version/platform/region/provider)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tool 2: platform+version pay success rate (incident day 10:00-11:00)
# MAGIC SELECT platform, app_version,
# MAGIC   sum(pay_attempts) AS attempts, sum(pay_success) AS success,
# MAGIC   round(sum(pay_success)*1.0/nullif(sum(pay_attempts),0), 4) AS pay_success_rate
# MAGIC FROM main.cursor_gaming.gold_kpi_5m
# MAGIC WHERE bucket_5m >= current_date() - interval 1 day + interval 10 hour
# MAGIC   AND bucket_5m < current_date() - interval 1 day + interval 11 hour
# MAGIC GROUP BY platform, app_version
# MAGIC ORDER BY pay_success_rate;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tool 2b: region slice (e.g. SG)
# MAGIC SELECT region, platform, app_version,
# MAGIC   sum(pay_attempts) AS attempts, sum(pay_success) AS success,
# MAGIC   round(sum(pay_success)*1.0/nullif(sum(pay_attempts),0), 4) AS pay_success_rate
# MAGIC FROM main.cursor_gaming.gold_kpi_5m
# MAGIC WHERE bucket_5m >= current_date() - interval 1 day + interval 10 hour
# MAGIC   AND bucket_5m < current_date() - interval 1 day + interval 11 hour
# MAGIC GROUP BY region, platform, app_version
# MAGIC ORDER BY pay_success_rate;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tool 3: Top error_code
# MAGIC SELECT coalesce(error_code, 'SUCCESS') AS error_code, count(*) AS cnt
# MAGIC FROM main.cursor_gaming.silver_payments
# MAGIC WHERE event_ts >= current_date() - interval 1 day + interval 10 hour
# MAGIC   AND event_ts < current_date() - interval 1 day + interval 11 hour
# MAGIC GROUP BY error_code ORDER BY cnt DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tool 4: payment provider x platform
# MAGIC SELECT provider, platform, app_version,
# MAGIC   sum(pay_attempts) AS attempts, sum(pay_success) AS success,
# MAGIC   round(sum(pay_success)*1.0/nullif(sum(pay_attempts),0), 4) AS pay_success_rate
# MAGIC FROM main.cursor_gaming.gold_kpi_5m
# MAGIC WHERE bucket_5m >= current_date() - interval 1 day + interval 10 hour
# MAGIC   AND bucket_5m < current_date() - interval 1 day + interval 11 hour
# MAGIC GROUP BY provider, platform, app_version
# MAGIC ORDER BY pay_success_rate;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: GenAI prompt template (fill query results above to generate root-cause cards)
# MAGIC In production: use Foundation Model API / Agent to run SQL tools, then feed results into prompt.

# COMMAND ----------

RCA_PROMPT_TEMPLATE = """
You are a LiveOps incident analysis assistant. Using the **read-only query results** below (from Unity Catalog tables), produce root-cause cards. Answers must cite data evidence (table name, time window, numbers).

## Anomaly time window (gold_anomaly)
{anomaly_windows}

## Platform x version payment success rate (gold_kpi_5m)
{platform_version}

## Region slice
{region_slice}

## Top error_code (silver_payments)
{error_codes}

## Payment provider slice
{provider_slice}

Output **root-cause cards** in this format:
- **Root cause #1**: [one sentence] Evidence: table/dimension/numbers/compare window
- **Root cause #2**: [one sentence] Evidence: ...
- **Root cause #3**: [one sentence] Evidence: ...
- **Recommended actions**: Rollback / canary off payment entry / force token refresh / contact provider (aligned to causes)
"""

# Usage: In Agent, run the 4 SQLs above, fill format(), then call LLM to generate text.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example: fetch evidence in Python and call Foundation Model (placeholder)
# MAGIC Configure Foundation Model or Mosaic endpoint in Databricks for production

# COMMAND ----------

def fetch_evidence(spark):
    """Run SQL to get evidence; return dict for prompt."""
    anomaly = spark.sql("SELECT bucket_5m, metric, actual, baseline, z_score FROM main.cursor_gaming.gold_anomaly WHERE is_anomaly = true ORDER BY bucket_5m").collect()
    return {
        "anomaly_windows": "\n".join([str(r) for r in anomaly]) if anomaly else "None",
        "platform_version": "(see Step 2 query result above)",
        "region_slice": "(see Step 2b above)",
        "error_codes": "(see Step 3 above)",
        "provider_slice": "(see Step 4 above)",
    }

# Example (configure ai_endpoint):
# from databricks_genie_integration import chat  # or your GenAI integration
# evidence = fetch_evidence(spark)
# prompt = RCA_PROMPT_TEMPLATE.format(**evidence)
# response = chat(prompt)
# display(response)  # show root-cause cards
