import json, pathlib, random, datetime as dt, time

random.seed(42)
SRC = pathlib.Path("source_crypto_stream")
SRC.mkdir(exist_ok=True)

start = dt.datetime(2026, 1, 19, 14, 0, 0, tzinfo=dt.timezone.utc)
EXCHANGES = ["Cryptojab", "CoinParovoz", "BitKotik"]
base_price = 95000.0

print(" Запуск потока...")

for sec in range(600):
    ts = start + dt.timedelta(seconds=sec)
    base_price += random.uniform(-5, 5)
    hot_exchange = EXCHANGES[(sec // 120) % len(EXCHANGES)]

    rows = []
    for _ in range(5):
        ex = random.choice(EXCHANGES)
        vol_mult = 5.0 if ex == hot_exchange else 1.0
        rows.append({
            "ts": ts.isoformat(),
            "exchange": ex,
            "price": round(base_price + (15.0 if ex == hot_exchange else 0), 2),
            "qty": round(random.uniform(0.01, 1.0) * vol_mult, 4)
        })

    fname = SRC / f"trade_{sec:06d}.json"
    fname.write_text("\n".join(json.dumps(r) for r in rows))

    time.sleep(1)
    if sec % 10 == 0:
        print(f" Событие {sec} отправлено в поток...")