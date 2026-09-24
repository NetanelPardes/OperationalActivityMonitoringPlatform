import json
import math
import os
from collections import defaultdict, deque
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import Consumer, KafkaException, Producer


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
READINGS_TOPIC = os.getenv("ACTIVITY_TOPIC", "activity-readings")
ANOMALIES_TOPIC = os.getenv("ANOMALIES_TOPIC", "anomalies")
GROUP_ID = os.getenv("ANALYTICS_GROUP_ID", "activity-analytics")

WINDOW_SIZE = int(os.getenv("WINDOW_SIZE", "20"))
Z_THRESHOLD = float(os.getenv("Z_THRESHOLD", "3.5"))
CRITICAL_THRESHOLD = float(os.getenv("CRITICAL_THRESHOLD", "4.5"))

if WINDOW_SIZE < 2:
    raise ValueError("WINDOW_SIZE must be at least 2")

if not 0 < Z_THRESHOLD < CRITICAL_THRESHOLD:
    raise ValueError("Thresholds must satisfy 0 < Z_THRESHOLD < CRITICAL_THRESHOLD")


def validate_reading(data):
    if not isinstance(data, dict):
        raise ValueError("Message must be a JSON object")

    for field in ("event_id", "source_id", "timestamp", "value"):
        if field not in data or data[field] is None:
            raise ValueError(f"Missing field: {field}")

    event_id = str(data["event_id"]).strip()
    source_id = str(data["source_id"]).strip()
    timestamp_text = str(data["timestamp"]).strip()

    if not event_id or not source_id or not timestamp_text:
        raise ValueError("event_id, source_id and timestamp cannot be empty")

    datetime.fromisoformat(timestamp_text.replace("Z", "+00:00"))

    if isinstance(data["value"], bool):
        raise ValueError("value must be a number")

    value = float(data["value"])

    if not math.isfinite(value):
        raise ValueError("value must be a finite number")

    return {
        "event_id": event_id,
        "source_id": source_id,
        "timestamp": timestamp_text,
        "value": value,
    }


def analyze_reading(reading, window):
    if len(window) < WINDOW_SIZE:
        return None

    values = pd.Series(list(window), dtype="float64")

    mean = float(values.mean())
    standard_deviation = float(values.std(ddof=1))

    if standard_deviation == 0:
        return None

    z_score = (reading["value"] - mean) / standard_deviation
    absolute_z = abs(z_score)

    if absolute_z >= CRITICAL_THRESHOLD:
        severity = "Critical"
    elif absolute_z >= Z_THRESHOLD:
        severity = "Warning"
    else:
        return None  

    detected_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

    return {
        "event_id": reading["event_id"],
        "source_id": reading["source_id"],
        "timestamp": reading["timestamp"],
        "value": reading["value"],
        "mean": mean,
        "standard_deviation": standard_deviation,
        "z_score": z_score,
        "severity": severity,
        "detected_at": detected_at,
    }


def publish_anomaly(producer, anomaly):
    delivery = {"confirmed": False, "error": None}

    def on_delivery(error, message):
        delivery["error"] = error
        delivery["confirmed"] = error is None

    producer.produce(
        topic=ANOMALIES_TOPIC,
        key=anomaly["source_id"].encode("utf-8"),
        value=json.dumps(anomaly, ensure_ascii=False).encode("utf-8"),
        on_delivery=on_delivery,
    )

    remaining_messages = producer.flush(timeout=30)

    if delivery["error"] is not None:
        raise RuntimeError(f"Failed to publish anomaly: {delivery['error']}")

    if remaining_messages or not delivery["confirmed"]:
        raise TimeoutError("Kafka did not confirm anomaly delivery")


def main():
    windows = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS,})

    consumer.subscribe([READINGS_TOPIC])
    print(f"Analytics is consuming {READINGS_TOPIC}", flush=True)

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                raise KafkaException(message.error())

            try:
                data = json.loads(message.value().decode("utf-8"))
                reading = validate_reading(data)
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
                print(f"Invalid reading skipped: {exc}", flush=True)
                consumer.commit(message=message, asynchronous=False)
                continue

            window = windows[reading["source_id"]]
            anomaly = analyze_reading(reading, window)

            if anomaly is not None:
                publish_anomaly(producer, anomaly)
                print(
                    f"Anomaly: {reading['event_id']} | "
                    f"{reading['source_id']} | "
                    f"{anomaly['severity']} | "
                    f"z={anomaly['z_score']:.2f}",
                    flush=True,
                )

            
            window.append(reading["value"])
            consumer.commit(message=message, asynchronous=False)

    except KeyboardInterrupt:
        print("Analytics stopped", flush=True)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()