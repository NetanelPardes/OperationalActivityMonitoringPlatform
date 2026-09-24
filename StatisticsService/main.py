import pymongo
import pandas as pd
from datetime import datetime, timezone

import os

myclient = pymongo.MongoClient(os.getenv("MONGO_URL", "mongodb://localhost:27017/"))

mydb = myclient["activity_monitoring"]

mycol = mydb["raw_readings"]
statistics_collection = mydb["station_statistics"]

myData = mycol.find()

df = pd.DataFrame(myData)

stat = df.groupby("source_id").agg(
    measurements_count=("value", "count"),
        average_value=("value", "mean"),
        min_value=("value", "min"),
        max_value=("value", "max"),
        last_reading_at=("timestamp", "max"),
)
computed_at = datetime.now(timezone.utc)

for source_id, row in stat.iterrows():
        document = {
            "measurements_count": int(row["measurements_count"]),
            "average_value": round(float(row["average_value"]), 2),
            "min_value": float(row["min_value"]),
            "max_value": float(row["max_value"]),
            "last_reading_at": row["last_reading_at"].to_pydatetime(),
            "computed_at": computed_at,
        }
        statistics_collection.update_one(
            {"_id": source_id},
            {"$set": document},
            upsert=True,
        )
myclient.close()