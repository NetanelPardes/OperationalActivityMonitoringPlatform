import pandas as pd
from confluent_kafka import Producer
import json
import time

data = pd.read_csv("/app/Data/activity_readings.csv")
df = pd.DataFrame(data)

sort_data = df.sort_values("timestamp")

sort_data["timestamp"] = pd.to_datetime(sort_data["timestamp"] ,errors="coerce")

conf = {"bootstrap.servers": "kafka:19092"}

producer = Producer(conf)

mt_topic = "activity-readings"

sort_data = sort_data.to_dict("records")

for item in sort_data:
    message = json.dumps(item, default=str).encode("utf-8")
    producer.produce(mt_topic, value=message)  
    time.sleep(0.1)
    print(f"Sending event: {item['event_id']}")

producer.flush()

print(f"Successfully sent {len(sort_data)} readings")
