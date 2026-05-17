from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# Создаем Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Список возможных событий и категорий
event_types = ['view', 'click', 'purchase', 'login', 'search']
categories = ['electronics', 'books', 'clothing', 'home', 'sports']

user_ids = list(range(1, 6))  # 5 пользователей


def generate_simple_event():
    user_id = random.choice(user_ids)
    event_type = random.choice(event_types)
    category = random.choice(categories)

    # Генерируем данные
    event_data = {
        "user_id": user_id,
        "event_type": event_type,
        "category": category,
        "timestamp": datetime.now().isoformat(),  # Временная метка события
        "value": round(random.uniform(1.0, 100.0), 2)
    }

    return event_data


try:
    print("Запускаю генератор событий. Нажмите Ctrl+C для остановки.")
    while True:
        # Генерируем событие
        event = generate_simple_event()
        producer.send('user_events', value=event)
        print(f"Отправлено событие: {event['event_type']} для пользователя {event['user_id']}")

        # Добавляем задержку от 1 до 3 секунд перед отправкой следующего события
        time.sleep(random.uniform(1, 3))

except KeyboardInterrupt:
    print("Останавливаю генератор данных.")
finally:
    producer.close()
    print("Продюсер Kafka закрыт.")