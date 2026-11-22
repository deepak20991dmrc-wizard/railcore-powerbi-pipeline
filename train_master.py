
import os
import requests
import psycopg2
from psycopg2.extras import Json
from datetime import datetime, timezone

TRAIN_NUMBERS = [
    "12309",
    "12393",
    "12391",
    "20801",
    "12303",
    "12273",
    "22361",
    "13257",
    "12487",
    "12505"
]

API_KEY = os.environ["RAILCORE_API_KEY"]
DATABASE_URL = os.environ["DATABASE_URL"]


def fetch_train(train_number, cur):

    print(f"Fetching train {train_number}")

    url = f"https://ir.railcore.tech/v1/trains/{train_number}"

    response = requests.get(
        url,
        headers={"X-RailCore-Key": API_KEY},
        timeout=30
    )

    response.raise_for_status()

    payload = response.json()

    if not payload.get("success"):
        raise Exception(f"API error: {payload}")

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

    print(f"SUCCESS: {train_number} - {data.get('display_name')}")


def main():

    successful = 0
    failed = []

    conn = psycopg2.connect(
        DATABASE_URL,
        sslmode="require"
    )

    try:
        with conn.cursor() as cur:

            for train_number in TRAIN_NUMBERS:

                try:
                    fetch_train(train_number, cur)
                    conn.commit()
                    successful += 1

                except Exception as error:
                    conn.rollback()
                    failed.append(train_number)
                    print(f"FAILED: {train_number}: {error}")

        print("--------------------------------")
        print(f"Successful trains: {successful}")
        print(f"Failed trains: {len(failed)}")
        print(f"Failed train numbers: {failed}")

        if successful == 0:
            raise Exception("No train records were updated.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
