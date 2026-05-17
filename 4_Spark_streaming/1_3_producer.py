import time
import requests
import json
import random
from kafka import KafkaProducer

bootstrap_servers = 'localhost:9092'
topic_name = 'test_data'

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Отправка данных в топик '{topic_name}'...")

while True:
    try:
        # Генерируем случайный ID поста от 1 до 100
        random_id = random.randint(1, 100)
        api_url = f"https://jsonplaceholder.typicode.com/posts/{random_id}"

        response = requests.get(api_url)
        data = response.json()

        producer.send(topic_name, value=data)
        print(f"Отправлены данные: ID {data['id']}, title: {data['title']}")

    except Exception as e:
        print(f"Ошибка: {e}")

    time.sleep(5)