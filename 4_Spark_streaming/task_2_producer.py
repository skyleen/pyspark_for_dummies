import json, pathlib, random, datetime as dt, time

random.seed(42)
SRC = pathlib.Path("./source_bikeshare_stream")
SRC.mkdir(exist_ok=True)

# Список станций
station_ids = [f"S{i:03d}" for i in range(1, 16)]

zones = {
    "North": ["S001", "S002", "S007", "S008"],
    "South": ["S003", "S005", "S009", "S013", "S014"],
    "Center": ["S004", "S006", "S010", "S011", "S012", "S015"]
}

start_time = dt.datetime(2026, 1, 19, 8, 0, 0, tzinfo=dt.timezone.utc)

print("Генератор потока запущен...")

try:
    for i in range(100):
        ts_base = start_time + dt.timedelta(seconds=i * 30)
        rows = []

        for s_id in station_ids:
            # Логика загруженности в зависимости от зоны
            current_zone = next((z for z, ids in zones.items() if s_id in ids), "North")

            if current_zone == "Center":
                bikes, scooters = random.randint(10, 25), random.randint(5, 15)
            else:
                bikes, scooters = random.randint(0, 8), random.randint(0, 5)

            ts_event = ts_base
            if random.random() < 0.15:
                # 15% сообщений опаздывают на 1-2.5 минуты
                ts_event -= dt.timedelta(seconds=random.randint(70, 150))

            rows.append({
                "ts": ts_event.isoformat(),
                "station_id": s_id,
                "bikes_available": bikes,
                "scooters_available": scooters
            })

        fname = SRC / f"batch_{i:03d}.json"
        fname.write_text("\n".join(json.dumps(r) for r in rows))

        print(f"Батч {i + 1}/100 записан. Время данных: {ts_base.strftime('%H:%M:%S')}")
        time.sleep(2)

except KeyboardInterrupt:
    print("Генерация остановлена.")