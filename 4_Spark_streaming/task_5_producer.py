import os
import json
import time
import random
from datetime import datetime, timedelta


def generate_banking_stream_continuous():
    random.seed(42)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    stream_dir = os.path.join(base_dir, "source_bank_transactions")
    print(f"Генератор запущен. Данные будут поступать в '{stream_dir}' в течение 60 секунд.")

    start_time = datetime(2026, 4, 1, 10, 0, 0)
    categories = ["Food", "Travel", "Electronics", "Utilities", "Entertainment"]

    # Генерация в течение 1 минуты (30 файлов с интервалом 2 секунды)
    for i in range(30):
        current_time = start_time + timedelta(seconds=i * 2)
        transactions = []

        for _ in range(random.randint(5, 15)):
            amount = random.randint(100, 8000)
            txn = {
                "transaction_id": f"txn_{i}_{random.randint(10000, 99999)}",
                "timestamp": (current_time + timedelta(seconds=random.randint(0, 1))).strftime('%Y-%m-%d %H:%M:%S'),
                "amount": amount,
                "category": random.choice(categories),
                "card_id": f"card_{random.randint(1, 50)}",
                "card_type": random.choice(["Gold", "Platinum", "Standard"])
            }
            transactions.append(txn)

        file_path = os.path.join(stream_dir, f"transactions_{i + 1}.json")
        with open(file_path, 'w') as f:
            for t in transactions:
                f.write(json.dumps(t) + '\n')

        print(f"Создан файл: transactions_{i + 1}.json ({len(transactions)} транзакций)")
        time.sleep(2)

    print("\nГенерация завершена. Все файлы созданы.")


if __name__ == "__main__":
    generate_banking_stream_continuous()
