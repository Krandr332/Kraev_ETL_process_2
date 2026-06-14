#!/usr/bin/env python3

import argparse
import os
import ssl
import time

from kafka import KafkaProducer


def kafka_ssl_context() -> ssl.SSLContext:
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"].split(","),
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=os.environ["KAFKA_USERNAME"],
        sasl_plain_password=os.environ["KAFKA_PASSWORD"],
        ssl_context=kafka_ssl_context(),
        request_timeout_ms=30_000,
        linger_ms=50,
        batch_size=32_768,
    )

    topic = os.environ["KAFKA_TOPIC"]
    sent = 0
    batch = []

    with open(args.input, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            batch.append(line.encode("utf-8"))
            if len(batch) >= args.batch_size:
                for msg in batch:
                    producer.send(topic, value=msg)
                producer.flush()
                sent += len(batch)
                print(f"Sent {sent:,} messages...")
                batch = []
                time.sleep(0.1)

        if batch:
            for msg in batch:
                producer.send(topic, value=msg)
            producer.flush()
            sent += len(batch)

    producer.close()
    print(f"Done. Total messages sent: {sent:,}")


if __name__ == "__main__":
    main()
