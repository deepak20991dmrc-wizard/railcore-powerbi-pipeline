
import os
import csv
import json
from datetime import date, datetime
from decimal import Decimal

import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ["DATABASE_URL"]
OUTPUT_FILE = "data/train_observations.csv"


def convert_value(value):
    if value is None:
        return ""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, Decimal):
        return str(value)
    return value


def main():
    os.makedirs("data", exist_ok=True)

    conn = psycopg2.connect(
        DATABASE_URL,
        sslmode="require"
    )

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT *
                FROM train_observations
                ORDER BY collected_at ASC
            """)

            rows = cur.fetchall()
            columns = [desc.name for desc in cur.description]

        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as file:
            writer = csv.DictWriter(file, fieldnames=columns)
            writer.writeheader()

            for row in rows:
                writer.writerow({
                    column: convert_value(row[column])
                    for column in columns
                })

        print(f"CSV exported successfully: {len(rows)} records")
        print(f"File: {OUTPUT_FILE}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
