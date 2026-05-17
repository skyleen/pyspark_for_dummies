import socket
import time
from datetime import datetime, timedelta
import random

random.seed(42)


def send_data():
    host = 'localhost'
    port = 9999

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((host, port))
    s.listen(1)

    print(f"Генератор запущен на {host}:{port}. Ожидание подключения Spark...")
    conn, addr = s.accept()
    print(f"Подключено: {addr}")

    users = ["user_A", "user_B", "user_C"]
    actions = ["view", "add_to_cart", "purchase"]

    # Начальное виртуальное время
    virtual_time = datetime(2026, 2, 1, 10, 0, 0)

    try:
        print("--- Активность user_A (одна сессия, 30 минутные интервалы) ---")
        for i in range(3):
            ts_str = virtual_time.strftime('%Y-%m-%d %H:%M:%S')
            data = f"{ts_str},user_A,{random.choice(actions)}\n"
            conn.send(data.encode('utf-8'))
            print(f"Отправлено (время {ts_str}): {data.strip()}")

            # В реальности ждем 5 секунд, чтобы не перегружать сокетный источник Spark
            virtual_time += timedelta(minutes=10)
            time.sleep(5)

        # Сценарий 2: Разрыв сессии (user_B)
        print("\n--- Активность user_B с разрывом сессии ---")
        # Первое действие
        ts_str = virtual_time.strftime('%Y-%m-%d %H:%M:%S')
        data = f"{ts_str},user_B,view\n"
        conn.send(data.encode('utf-8'))
        print(f"Отправлено (время {ts_str}): {data.strip()}")

        # Делаем скачок во времени на 40 минут (gap > 30 min)
        virtual_time += timedelta(minutes=40)
        time.sleep(5)

        # Второе действие (уже новая сессия)
        ts_str = virtual_time.strftime('%Y-%m-%d %H:%M:%S')
        data = f"{ts_str},user_B,purchase\n"
        conn.send(data.encode('utf-8'))
        print(f"Отправлено (время {ts_str}): {data.strip()} [Должна начаться новая сессия]")

        # Поток случайных событий
        print("\n--- Сценарий 3: Короткая серия случайных событий ---")
        for _ in range(10):
            u = random.choice(users)
            a = random.choice(actions)
            ts_str = virtual_time.strftime('%Y-%m-%d %H:%M:%S')
            data = f"{ts_str},{u},{a}\n"
            conn.send(data.encode('utf-8'))
            print(f"Отправлено: {data.strip()}")

            virtual_time += timedelta(minutes=5)
            time.sleep(5)

        # Отправляем сообщение из будущего, чтобы закрыть все открытые окна.
        print("\n--- Закрываем все сессии ---")
        final_ts = (virtual_time + timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
        flush_data = f"{final_ts},system,flush\n"
        conn.send(flush_data.encode('utf-8'))
        print(f"Отправлен финальный сигнал: {flush_data.strip()}")

        print("Генерация завершена.")
        time.sleep(5)

    except (ConnectionResetError, BrokenPipeError):
        print("Spark приложение остановило соединение.")
    finally:
        conn.close()
        s.close()


if __name__ == "__main__":
    send_data()
