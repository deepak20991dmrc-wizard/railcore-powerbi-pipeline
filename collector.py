
import os
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
import psycopg2
from psycopg2.extras import Json


# -----------------------------
# Configuration
# -----------------------------

TRAIN_NUMBER = "12309"
FROM_STATION = "RJPB"
TO_STATION = "NDLS"

TIMEZONE = ZoneInfo("Asia/Kolkata")

API_KEY = os.environ["RAILCORE_API_KEY"]
DATABASE_URL = os.environ["DATABASE_URL"]

API_URL = (
    f"https://ir.railcore.tech/v1/"
    f"trains/{TRAIN_NUMBER}/live-status"
)


def parse_timestamp(value):
    if not value:
        return None

    try:
        return datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )
    except (ValueError, AttributeError):
        return None


def main():

    now = datetime.now(TIMEZONE)

    force_run = (
        os.getenv("FORCE_RUN", "false").lower() == "true"
    )

    # Normal collection window: 20:00 to 08:59 IST
    within_window = now.hour >= 20 or now.hour <= 8

    if not within_window and not force_run:
        print("Outside collection window. Skipping API call.")
        return

    # The journey date remains the departure date
    # even after midnight.
    if now.hour <= 8:
        journey_date = now.date() - timedelta(days=1)
    else:
        journey_date = now.date()

    # Hourly uniqueness key
    collection_slot = now.replace(
        minute=0,
        second=0,
        microsecond=0
    )

    print("Train:", TRAIN_NUMBER)
    print("Journey date:", journey_date)
    print("Collection time:", collection_slot)

    # -----------------------------
    # Extract: RailCore API
    # -----------------------------

    headers = {
        "X-RailCore-Key": API_KEY
    }

    params = {
        "date": journey_date.isoformat(),
        "from": FROM_STATION,
        "to": TO_STATION
    }

    response = requests.get(
        API_URL,
        headers=headers,
        params=params,
        timeout=30
    )

    if response.status_code != 200:
        print("API error:", response.status_code)
        print(response.text[:1000])
        sys.exit(1)

    payload = response.json()

    if payload.get("success") is not True:
        print("API returned an unsuccessful response.")
        print(payload)
        sys.exit(1)

    data = payload.get("data") or {}
    meta = payload.get("meta") or {}

    # -----------------------------
    # Transform: Extract fields
    # -----------------------------

    freshness = meta.get("freshness") or {}

    reported_at = (
        data.get("last_reported_at")
        or freshness.get("retrieved_at")
        or meta.get("timestamp")
    )

    api_reported_at = parse_timestamp(reported_at)

    train_status = data.get("status")
    status_text = data.get("status_text")

    current_station = data.get("current_station_code")
    previous_station = data.get("previous_station_code")
    next_station = data.get("next_station_code")

    approaching_station = data.get(
        "approaching_station_code"
    )

    delay_minutes = data.get("delay_minutes")

    distance_current = data.get(
        "distance_from_current_station_km"
    )

    distance_approaching = data.get(
        "distance_to_approaching_station_km"
    )

    confidence = data.get("confidence")

    # -----------------------------
    # Load: Supabase PostgreSQL
    # -----------------------------

    connection = psycopg2.connect(
        DATABASE_URL,
        sslmode="require"
    )

    try:
        with connection:
            with connection.cursor() as cursor:

                cursor.execute(
                    """
                    INSERT INTO train_observations (
                        train_number,
                        journey_date,
                        collection_slot,
                        api_reported_at,
                        train_status,
                        status_text,
                        current_station_code,
                        previous_station_code,
                        next_station_code,
                        approaching_station_code,
                        delay_minutes,
                        distance_from_current_station_km,
                        distance_to_approaching_station_km,
                        confidence,
                        raw_payload
                    )
                    VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (
                        train_number,
                        journey_date,
                        collection_slot
                    )
                    DO NOTHING
                    """,
                    (
                        TRAIN_NUMBER,
                        journey_date,
                        collection_slot,
                        api_reported_at,
                        train_status,
                        status_text,
                        current_station,
                        previous_station,
                        next_station,
                        approaching_station,
                        delay_minutes,
                        distance_current,
                        distance_approaching,
                        confidence,
                        Json(payload)
                    )
                )

                if cursor.rowcount == 1:
                    print("Observation successfully inserted.")
                else:
                    print("Duplicate hourly observation skipped.")

    finally:
        connection.close()

    print("ETL execution completed successfully.")


if __name__ == "__main__":
    main()
