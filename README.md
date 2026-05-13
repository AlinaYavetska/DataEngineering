# Лабораторна робота №4 — Kafka Streams

Java/Maven застосунок підписується на `Topic1` (з Lab 3) і агрегує події **по даті поїздки** (YYYY-MM-DD з `start_time`).

## Обчислення → окремі топіки
| Завдання | Output-топік | Формат значення |
|---|---|---|
| (a) Середня тривалість поїздки на день | `avg-duration-per-day` | `{"avg_duration_seconds": X, "trips": N}` |
| (b) Кількість поїздок на день | `trips-count-per-day` | `{"trips_count": N}` |
| (c) Найпопулярніша початкова станція | `top-start-station-per-day` | `{"top_start_station": "...", "trips": N}` |
| (d) Top-3 станцій (start+end) на день | `top3-stations-per-day` | `{"top3":[{"station":"...","count":N}, ...]}` |

Ключ у всіх output-топіках — дата `YYYY-MM-DD`.

## Запуск
```bash
docker compose up --build
```
Kafka UI: http://localhost:8085 — у топіках `avg-duration-per-day`, `trips-count-per-day`, `top-start-station-per-day`, `top3-stations-per-day` побачите оновлювані KTable-стрім (нові значення під тим самим ключем при кожному оновленні).

## Зупинка
```bash
docker compose down -v
```
