-- Виконати в trino CLI:
--   docker compose exec -it trino trino --server localhost:8080 --catalog iceberg

CREATE SCHEMA IF NOT EXISTS iceberg.trips_db;

USE iceberg.trips_db;

-- Сирі поїздки (з Topic1/Topic2)
CREATE TABLE IF NOT EXISTS trips (
    trip_id            VARCHAR,
    trip_duration      BIGINT,
    start_time         VARCHAR,
    stop_time          VARCHAR,
    start_station_id   VARCHAR,
    start_station_name VARCHAR,
    end_station_id     VARCHAR,
    end_station_name   VARCHAR,
    bike_id            VARCHAR,
    user_type          VARCHAR,
    trip_date          DATE
)
WITH (partitioning = ARRAY['trip_date']);

-- Результати агрегацій Kafka Streams (Lab 4)
CREATE TABLE IF NOT EXISTS avg_duration_per_day (
    trip_date             DATE,
    avg_duration_seconds  DOUBLE,
    trips                 BIGINT
);

CREATE TABLE IF NOT EXISTS trips_count_per_day (
    trip_date    DATE,
    trips_count  BIGINT
);

CREATE TABLE IF NOT EXISTS top_start_station_per_day (
    trip_date          DATE,
    top_start_station  VARCHAR,
    trips              BIGINT
);

CREATE TABLE IF NOT EXISTS top3_stations_per_day (
    trip_date  DATE,
    rank       INTEGER,
    station    VARCHAR,
    cnt        BIGINT
);

-- Перевірка
SELECT * FROM trips LIMIT 10;
