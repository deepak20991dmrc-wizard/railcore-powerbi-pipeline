
import os
import requests
import psycopg2
from psycopg2.extras import Json
from datetime import datetime, timezone

TRAIN_NUMBER = "12309"

API_URL = f"https://ir.railcore.tech/v1/trains/{TRAIN_NUMBER}"

API_KEY = os.environ["RAILCORE_API_KEY"]
DATABASE_URL = os.environ["DATABASE_URL"]


def main():

    print(f"Fetching train details for {TRAIN_NUMBER}")

    response = requests.get(
        API_URL,
        headers={"X-RailCore-Key": API_KEY},
        timeout=30
    )

    response.raise_for_status()

    payload = response.json()

    if not payload.get("success"):
        raise Exception(f"RailCore API error: {payload}")

    data = payload["data"]

    retrieved_at = (
        payload.get("meta", {})
        .get("freshness", {})
        .get("retrieved_at")
        or payload.get("meta", {}).get("timestamp")
    )

    if retrieved_at:
        retrieved_at = datetime.fromisoformat(
            retrieved_at.replace("Z", "+00:00")
        )
    else:
        retrieved_at = datetime.now(timezone.utc)

    conn = psycopg2.connect(
        DATABASE_URL,
        sslmode="require"
    )

    try:
        with conn.cursor() as cur:

            cur.execute("""
                INSERT INTO train_master (
                    train_number,
                    train_name,
                    display_name,
                    source_station_code,
                    destination_station_code,
                    train_type,
                    running_days,
                    api_retrieved_at,
                    raw_payload,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())

                ON CONFLICT (train_number)
                DO UPDATE SET
                    train_name = EXCLUDED.train_name,
                    display_name = EXCLUDED.display_name,
                    source_station_code = EXCLUDED.source_station_code,
                    destination_station_code = EXCLUDED.destination_station_code,
                    train_type = EXCLUDED.train_type,
                    running_days = EXCLUDED.running_days,
                    api_retrieved_at = EXCLUDED.api_retrieved_at,
                    raw_payload = EXCLUDED.raw_payload,
                    updated_at = NOW()
            """, (
                data.get("train_number"),
                data.get("train_name"),
                data.get("display_name"),
                data.get("source_station_code"),
                data.get("destination_station_code"),
                data.get("train_type"),
                Json(data.get("running_days", [])),
                retrieved_at,
                Json(payload)
            ))

        conn.commit()

        print("Train master data successfully saved!")
        print("Train:", data.get("display_name"))
        print("Origin:", data.get("source_station_code"))
        print("Destination:", data.get("destination_station_code"))
        print("Category:", data.get("train_type"))

    finally:
        conn.close()


if __name__ == "__main__":
    main()
