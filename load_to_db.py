from pymongo import DESCENDING
import pandas as pd

def load_to_mongo(df, collection):
    # Convert df timestamp to UTC datetime objects
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    # Get the latest timestamp in MongoDB (UTC-aware)
    latest_entry = collection.find_one(
        sort=[("timestamp", DESCENDING)], projection={"timestamp": 1}
    )

    if latest_entry and latest_entry.get("timestamp"):
        latest_timestamp = pd.to_datetime(latest_entry["timestamp"], utc=True)

        # Filter rows strictly greater than the latest timestamp
        new_df = df[df["timestamp"] > latest_timestamp]
        print(f"🕓 Latest timestamp in DB: {latest_timestamp}")
    else:
        new_df = df
        print("ℹ️ No existing records found. Will insert all.")

    # Insert new rows if any
    if not new_df.empty:
        collection.insert_many(new_df.to_dict(orient="records"))
        print(f"✅ Inserted {len(new_df)} new records.")
    else:
        print("⚠️ No new data to insert.")

