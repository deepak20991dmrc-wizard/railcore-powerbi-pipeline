
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
import psycopg2
from psycopg2.extras import Json


# -----------------------------
# Configuration
# -----------------------------

TIMEZONE = ZoneInfo("Asia/Kolkata")

API_KEY = os.environ["RAILCORE_API_KEY"]
DATABASE_URL = os.environ["DATABASE_URL"]

API_BASE_URL = "https://ir.railcore.tech/v1/trains"


def parse_timestamp(value):
    if not value:
        return None

    try:
        return datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )
    except (ValueError, AttributeError):
        return None


def get_collection_slot(now):
    # Two-hour collection slots: 00:00, 02:00, 04:00 ... 22:00
    slot_hour = (now.hour // 2) * 2

    return now.replace(
        hour=slot_hour,
        minute=0,
        second=0,
        microsecond=0
    )


def collect_train(train_number, collection_slot, now, cursor):
    print(f"\nFetching train: {train_number}")

    url = f"{API_BASE_URL}/{train_number}/live"

    headers = {
        "X-RailCore-Key": API_KEY
    }

    try:
        # No common from/to station parameters.
        # RailCore determines the train's own journey.
        response = requests.get(
            url,
            headers=headers,
            timeout=30
        )

        if response.status_code != 200:
            print(
                f"API error for {train_number}: "
                f"{response.status_code}"
            )
            print(response.text[:500])
            return False

        payload = response.json()

    except requests.RequestException as error:
        print(f"Network error for {train_number}: {error}")
        return False

    except ValueError:
        print(f"Invalid JSON response for {train_number}")
        return False

    if payload.get("success") is not True:
        print(f"Unsuccessful API response for {train_number}")
        print(payload)
        return False

    data = payload.get("data") or {}
    meta = payload.get("meta") or {}
    freshness = meta.get("freshness") or {}

    # Use the journey date returned by RailCore.
    # Fall back to today's date if it is not supplied.
    journey_date_value = data.get("journey_date")

    if journey_date_value:
        try:
            journey_date = datetime.fromisoformat(
                str(journey_date_value)[:10]
            ).date()
        except ValueError:
            journey_date = now.date()
    else:
        journey_date = now.date()
        print(
            "Warning: API did not provide journey_date; "
            "using today's date."
        )

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
            train_number,
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
        print(
            f"Inserted: {train_number} | "
            f"Journey date: {journey_date} | "
            f"Slot: {collection_slot}"
        )
    else:
        print(
            f"Duplicate observation skipped: {train_number}"
        )

    return True


def main():

    now = datetime.now(TIMEZONE)
    collection_slot = get_collection_slot(now)

    print("RAILCORE MULTI-TRAIN COLLECTION")
    print("Current IST time:", now)
    print("Two-hour collection slot:", collection_slot)

    connection = psycopg2.connect(
        DATABASE_URL,
        sslmode="require"
    )

    successful = 0
    failed = 0

    try:
        with connection.cursor() as cursor:

            # Get all train numbers from train_master.
            cursor.execute(
                """
                SELECT train_number
                FROM train_master
                ORDER BY train_number
                """
            )

            trains = cursor.fetchall()

            if not trains:
                raise Exception(
                    "No trains found in train_master table."
                )

            train_numbers = [
                row[0] for row in trains
            ]

            print("Total trains:", len(train_numbers))
            print("Train numbers:", train_numbers)

            # Collect each train independently.
            for train_number in train_numbers:

                try:
                    result = collect_train(
                        train_number,
                        collection_slot,
                        now,
                        cursor
                    )

                    if result:
                        successful += 1
                    else:
                        failed += 1

                    # Commit each train independently.
                    connection.commit()

                except Exception as error:
                    connection.rollback()
                    failed += 1
                    print(
                        f"Database/processing error for "
                        f"{train_number}: {error}"
                    )

    finally:
        connection.close()

    print("\n-----------------------------")
    print("COLLECTION SUMMARY")
    print("Successful:", successful)
    print("Failed:", failed)
    print("-----------------------------")

    if failed > 0:
        sys.exit(1)

    print("Multi-train ETL completed successfully.")


if __name__ == "__main__":
    main()
