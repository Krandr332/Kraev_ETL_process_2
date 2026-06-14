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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--checkpoint-path", required=True)
    args = parser.parse_args()

    jaas = (
        "org.apache.kafka.common.security.scram.ScramLoginModule required "
        f'username="{os.environ["KAFKA_USERNAME"]}" '
        f'password="{os.environ["KAFKA_PASSWORD"]}";'
    )

    spark = SparkSession.builder.appName("hw-final-kafka-flatten-burnout-stream").getOrCreate()

    stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP_SERVERS"])
        .option("subscribe", os.environ["KAFKA_TOPIC"])
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
        .option("kafka.sasl.jaas.config", jaas)
        .option("kafka.ssl.endpoint.identification.algorithm", "")
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING) AS json_payload")
        .filter(col("json_payload").isNotNull())
    )

    parsed = stream_df.withColumn("data", from_json(col("json_payload"), BURNOUT_SCHEMA)).select("data.*")

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

    query = (
        flat.writeStream
        .trigger(once=True)
        .format("parquet")
        .option("path", args.output_path)
        .option("checkpointLocation", args.checkpoint_path)
        .partitionBy("work_mode", "productivity_category")
        .start()
    )

    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()
