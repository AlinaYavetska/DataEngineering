import csv
import json
import os
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC1 = os.getenv("TOPIC1", "Topic1")
TOPIC2 = os.getenv("TOPIC2", "Topic2")
CSV_PATH = os.getenv("CSV_PATH", "/app/data/Divvy_Trips_2019_Q4.csv")
DELAY = float(os.getenv("SEND_DELAY_MS", "5")) / 1000.0
MAX_ROWS = int(os.getenv("MAX_ROWS", "1000"))  # 0 = unlimited


def build_producer():
    for attempt in range(30):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=5,
                linger_ms=20,
                batch_size=32768,
            )
        except NoBrokersAvailable:
            print(f"[producer] Brokers not ready, retry {attempt + 1}/30")
            time.sleep(3)
    raise RuntimeError("Kafka brokers unavailable")


def _to_int(v):
    try:
        return int(float(v)) if v not in (None, "", "NA") else None
    except (TypeError, ValueError):
        return None


def row_to_event(row: dict) -> dict:
    """
    Divvy Q4 2019 schema:
      trip_id, start_time, end_time, bikeid, tripduration,
      from_station_id, from_station_name, to_station_id, to_station_name,
      usertype, gender, birthyear
    Output JSON keys kept the same as in Lab 4 streams app
    (trip_duration, start_time, stop_time, start_station_*, end_station_*).
    """
    return {
        "trip_id": row.get("trip_id"),
        "trip_duration": _to_int(row.get("tripduration")),
        "start_time": row.get("start_time"),
        "stop_time": row.get("end_time"),
        "start_station_id": row.get("from_station_id"),
        "start_station_name": row.get("from_station_name"),
        "end_station_id": row.get("to_station_id"),
        "end_station_name": row.get("to_station_name"),
        "bike_id": row.get("bikeid"),
        "user_type": row.get("usertype"),
        "gender": row.get("gender"),
        "birthyear": row.get("birthyear"),
    }


def main():
    print(f"[producer] bootstrap={BOOTSTRAP} csv={CSV_PATH} max_rows={MAX_ROWS or 'ALL'}")
    producer = build_producer()

    sent = 0
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            event = row_to_event(row)
            key = event.get("trip_id") or str(sent)
            producer.send(TOPIC1, key=key, value=event)
            producer.send(TOPIC2, key=key, value=event)
            sent += 1
            if sent % 500 == 0:
                print(f"[producer] sent {sent} messages to {TOPIC1} and {TOPIC2}")
            if MAX_ROWS and sent >= MAX_ROWS:
                break
            if DELAY > 0:
                time.sleep(DELAY)

    producer.flush()
    producer.close()
    print(f"[producer] DONE. Total messages: {sent} per topic")


if __name__ == "__main__":
    main()
