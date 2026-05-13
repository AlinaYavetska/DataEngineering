# Лабораторна робота №3 — Kafka Producer/Consumer

## Склад кластеру
- Zookeeper
- Broker1 (порт 9092)
- Broker2 (порт 9093)
- Kafka UI (порт 8080) — http://localhost:8080
- Python-продюсер (читає `producer/data/trips.csv`)

## Топіки
- `Topic1` (partitions=3, replication=2)
- `Topic2` (partitions=3, replication=2)

Кожне повідомлення публікується в обидва топіки.

## Запуск
```bash
docker compose up --build
```

## Перевірка
1. Відкрити Kafka UI: http://localhost:8080
2. Cluster `local` → Topics → `Topic1` / `Topic2` → Messages
3. Логи продюсера: `docker logs -f producer`

## Зупинка
```bash
docker compose down -v
```
