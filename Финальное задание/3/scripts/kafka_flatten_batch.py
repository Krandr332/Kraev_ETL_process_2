#!/usr/bin/env python3

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, explode, from_json
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


BURNOUT_SCHEMA = StructType([
    StructField("user_id", IntegerType()),
    StructField("profile", StructType([
        StructField("age", IntegerType()),
        StructField("occupation", StringType()),
        StructField("work_mode", StringType()),
    ])),
    StructField("device_usage", StructType([
        StructField("device_usage_type", StringType()),
        StructField("daily_screen_time", DoubleType()),
        StructField("social_media_hours", DoubleType()),
        StructField("doomscrolling_duration", DoubleType()),
        StructField("app_switch_frequency", IntegerType()),
        StructField("notification_count", IntegerType()),
        StructField("smartphone_unlocks", IntegerType()),
        StructField("late_night_device_usage", IntegerType()),
    ])),
    StructField("work_patterns", StructType([
        StructField("focus_sessions", IntegerType()),
        StructField("deep_work_hours", DoubleType()),
        StructField("distraction_frequency", IntegerType()),
        StructField("task_completion_rate", IntegerType()),
        StructField("concentration_score", IntegerType()),
        StructField("meeting_hours", DoubleType()),
        StructField("remote_work_days", IntegerType()),
    ])),
    StructField("wellbeing", StructType([
        StructField("sleep_hours", DoubleType()),
        StructField("sleep_quality", IntegerType()),
        StructField("caffeine_intake", IntegerType()),
        StructField("physical_activity", DoubleType()),
        StructField("stress_level", IntegerType()),
        StructField("workspace_quality", IntegerType()),
        StructField("internet_stability", IntegerType()),
        StructField("motivation_level", DoubleType()),
        StructField("mental_fatigue", IntegerType()),
        StructField("emotional_exhaustion", IntegerType()),
        StructField("work_satisfaction", IntegerType()),
        StructField("mental_state", StringType()),
    ])),
    StructField("scores", ArrayType(StructType([
        StructField("metric", StringType()),
        StructField("value", IntegerType()),
    ]))),
    StructField("productivity_category", StringType()),
])


def kafka_options() -> dict:
    jaas = (
        "org.apache.kafka.common.security.scram.ScramLoginModule required "
        f'username="{os.environ["KAFKA_USERNAME"]}" '
        f'password="{os.environ["KAFKA_PASSWORD"]}";'
    )
    return {
        "kafka.bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "subscribe": os.environ["KAFKA_TOPIC"],
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "SCRAM-SHA-512",
        "kafka.sasl.jaas.config": jaas,
        "kafka.ssl.endpoint.identification.algorithm": "",
        "startingOffsets": "earliest",
        "endingOffsets": "latest",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("hw-final-kafka-flatten-burnout-batch").getOrCreate()

    raw = (
        spark.read.format("kafka")
        .options(**kafka_options())
        .load()
        .selectExpr("CAST(value AS STRING) AS json_payload")
        .filter(col("json_payload").isNotNull())
    )

    parsed = raw.withColumn("data", from_json(col("json_payload"), BURNOUT_SCHEMA)).select("data.*")

    flat = (
        parsed
        .withColumn("score", explode(col("scores")))
        .select(
            col("user_id"),
            col("profile.age").alias("age"),
            col("profile.occupation").alias("occupation"),
            col("profile.work_mode").alias("work_mode"),
            col("device_usage.device_usage_type").alias("device_usage_type"),
            col("device_usage.daily_screen_time").alias("daily_screen_time"),
            col("wellbeing.mental_state").alias("mental_state"),
            col("wellbeing.stress_level").alias("stress_level"),
            col("wellbeing.mental_fatigue").alias("mental_fatigue"),
            col("work_patterns.focus_sessions").alias("focus_sessions"),
            col("score.metric").alias("metric_name"),
            col("score.value").alias("metric_value"),
            col("productivity_category"),
            current_timestamp().alias("processed_at"),
        )
    )

    flat.write.mode("overwrite").partitionBy("work_mode", "productivity_category").parquet(args.output_path)

    print(f"Flattened rows written: {flat.count()}")
    spark.stop()


if __name__ == "__main__":
    main()
