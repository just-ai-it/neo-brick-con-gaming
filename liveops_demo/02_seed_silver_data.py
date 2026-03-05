# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Generate Mock Data (3 days + incident at 10:05)
# MAGIC - From 10:05: iOS + app_version 1.2.7 + SG payment failure rate 2%->18%
# MAGIC - error_code TOKEN_EXPIRED spike
# MAGIC - Stripe on iOS new version failure rate anomaly

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

# COMMAND ----------

def make_event_ts(base_date, hour, minute):
    return datetime(base_date.year, base_date.month, base_date.day, hour, minute, 0)

# Generate 3 days of data, minute-level; incident day 10:05-10:40
def generate_events_and_payments(spark, base_dates, incident_date):
    """base_dates: list of date; incident_date: incident day (anomaly starts 10:05 that day)"""
    rows_events = []
    rows_payments = []
    regions = ["SG", "US", "EU", "JP"]
    platforms = ["iOS", "Android"]
    versions_ios = ["1.2.6", "1.2.7"]
    versions_android = ["1.2.6", "1.2.7"]
    providers = ["Stripe", "PayPal", "ApplePay"]
    events = ["login", "level_start", "purchase_attempt", "purchase_success", "purchase_fail"]
    errors = ["TOKEN_EXPIRED", "NETWORK_TIMEOUT", "PROVIDER_XX", "INSUFFICIENT_FUNDS", None]

    for base_date in base_dates:
        is_incident_day = base_date == incident_date
        for hour in range(6, 22):
            for minute in range(0, 60, 2):  # every 2 minutes to limit data size
                ts = make_event_ts(base_date, hour, minute)
                # Incident window 10:05-10:40
                in_incident_window = is_incident_day and (hour == 10 and 5 <= minute <= 40)
                n_users = random.randint(80, 150) if not in_incident_window else random.randint(100, 180)
                for i in range(n_users):
                    user_id = f"u_{base_date.strftime('%Y%m%d')}_{hour:02d}{minute:02d}_{i}"
                    region = random.choice(regions)
                    platform = random.choice(platforms)
                    app_version = random.choice(versions_ios if platform == "iOS" else versions_android)
                    device = "iPhone14" if platform == "iOS" else "Pixel7"
                    # Events
                    for _ in range(random.randint(1, 3)):
                        ev = random.choice(events)
                        rows_events.append((ts, user_id, region, platform, device, app_version, ev))
                    # Payments: higher failure rate in incident window
                    n_pay = random.randint(5, 25)
                    for _ in range(n_pay):
                        provider = random.choice(providers)
                        amount = round(random.uniform(0.99, 99.99), 2)
                        if in_incident_window:
                            # iOS 1.2.7 + SG failure rate 2% -> 18%
                            if platform == "iOS" and app_version == "1.2.7" and region == "SG":
                                fail = random.random() < 0.18
                            elif provider == "Stripe" and platform == "iOS":
                                fail = random.random() < 0.12
                            else:
                                fail = random.random() < 0.02
                        else:
                            fail = random.random() < 0.02
                        status = "fail" if fail else "success"
                        err = random.choice([e for e in errors if e]) if fail else None
                        if fail and in_incident_window and platform == "iOS" and app_version == "1.2.7":
                            err = "TOKEN_EXPIRED" if random.random() < 0.7 else err
                        rows_payments.append((ts, user_id, amount, "USD", provider, status, err, platform, app_version, region))
    return rows_events, rows_payments

# COMMAND ----------

# Last 3 days; incident day = yesterday (for demo)
today = datetime.utcnow().date()
base_dates = [today - timedelta(days=i) for i in range(2, -1, -1)]
incident_date = today - timedelta(days=1)

rows_events, rows_payments = generate_events_and_payments(spark, base_dates, incident_date)

schema_events = StructType([
    StructField("event_ts", TimestampType(), False),
    StructField("user_id", StringType(), False),
    StructField("region", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("device_model", StringType(), True),
    StructField("app_version", StringType(), True),
    StructField("event_name", StringType(), True),
])
schema_payments = StructType([
    StructField("event_ts", TimestampType(), False),
    StructField("user_id", StringType(), False),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("provider", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("error_code", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("app_version", StringType(), True),
    StructField("region", StringType(), True),
])

df_events = spark.createDataFrame(rows_events, schema_events)
df_payments = spark.createDataFrame(rows_payments, schema_payments)

# COMMAND ----------

df_events.write.mode("overwrite").saveAsTable("cursor_gaming.gaming.silver_game_events")
df_payments.write.mode("overwrite").saveAsTable("cursor_gaming.gaming.silver_payments")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date_trunc('hour', event_ts) AS h, count(*) AS cnt
# MAGIC FROM cursor_gaming.gaming.silver_game_events
# MAGIC GROUP BY 1 ORDER BY 1;
