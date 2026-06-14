#!/usr/bin/env python3

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, current_timestamp, sum as spark_sum

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("exam-process-applications")
        .getOrCreate()
    )

    # Читаем CSV с вашими данными
    raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{args.input_path}/*.csv")
    )

    # Очистка данных (как в примере)
    cleaned = (
        raw
        .filter(col("application_id").isNotNull())
        .filter(col("requested_amount").between(1000, 100000))
        .filter(col("credit_score").between(300, 850))
    )

    # Сохраняем очищенные данные
    cleaned.write.mode("overwrite").parquet(f"{args.output_path}/cleaned")

    # Агрегация по статусу решения и уровню риска (ваша задача)
    summary = (
        cleaned
        .groupBy("decision_status", "risk_level")
        .agg(
            count("application_id").alias("applications_count"),
            avg("requested_amount").alias("avg_requested_amount"),
            avg("credit_score").alias("avg_credit_score"),
            spark_sum("approved_amount").alias("total_approved_amount"),
        )
        .withColumn("processed_at", current_timestamp())
    )

    summary.write.mode("overwrite").parquet(f"{args.output_path}/summary")

    print(f"Cleaned rows: {cleaned.count()}")
    print(f"Summary rows: {summary.count()}")

    spark.stop()

if __name__ == "__main__":
    main()