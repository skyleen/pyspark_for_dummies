import socket
import time
import datetime as dt
import random

random.seed(42)
# Настройки сервера
HOST = 'localhost'
PORT = 9999


def send_batch(conn, start_time, duration_sec, hosts, duplicate_msg=None):
    for sec in range(duration_sec):
        current_ts = (start_time + dt.timedelta(seconds=sec)).strftime('%Y-%m-%d %H:%M:%S')
        for host in hosts:
            cpu = random.randint(30, 90)
            mem = random.randint(40, 80)
            msg = f"{current_ts},{host},{cpu},{mem}\n"
            conn.sendall(msg.encode('utf-8'))

    # Если передан дубликат, отправляем его в конце батча
    if duplicate_msg:
        conn.sendall(duplicate_msg.encode('utf-8'))
        print(f"   >>> ОТПРАВЛЕН ДУБЛИКАТ: {duplicate_msg.strip()}")
        print("       (Проверьте, что в финальном отчете нет лишних строк)")

    time.sleep(15)


def run_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen()
        print("Ожидание Spark-приложения...")

        conn, addr = s.accept()
        with conn:
            print(f"Spark подключен.\n")

            start_ts = dt.datetime(2026, 2, 1, 10, 0, 0, tzinfo=dt.timezone.utc)

            # Сохраняем одно конкретное сообщение для последующего дублирования.
            # берем +5 секунд, чтобы показать, что дедупликация сработает для любой записи внутри окна, а не только для самого начала
            dup_ts = (start_ts + dt.timedelta(seconds=5)).strftime('%Y-%m-%d %H:%M:%S')
            special_msg = f"{dup_ts},srv-1,99,99\n"
            hosts_3 = [f"srv-{i}" for i in range(1, 4)]
            # Отправляем данные за 10:00
            print("[1/4] Отправляем данные за 10:00:00 (3 хоста)")
            send_batch(conn, start_ts, 60, hosts_3)

            # Отправляем данные за 10:01 + ДУБЛИКАТ из 10:00 (5 хостов)
            print("[2/4] Отправляем данные за 10:01:00 (5 хостов) + дубликат")
            hosts_5 = [f"srv-{i}" for i in range(1, 6)]
            send_batch(conn, start_ts + dt.timedelta(minutes=1), 60, hosts_5, duplicate_msg=special_msg)

            # Отправляем ОПОЗДАВШИЕ данные
            print("[3/4] ТЕСТ: Отправляем пакет из прошлого (09:55:00)")
            late_ts = dt.datetime(2026, 2, 1, 9, 55, 0, tzinfo=dt.timezone.utc)
            send_batch(conn, late_ts, 10, hosts_3)
            print("   -> Проверьте: появится ли в Spark окно за 09:55?\n")

            # Отправляем данные за 10:02 (2 хоста)
            print("[4/4] Отправляем данные за 10:02:00 (2 хоста)")
            hosts_2 = [f"srv-{i}" for i in range(1, 3)]
            send_batch(conn, start_ts + dt.timedelta(minutes=2), 60, hosts_2)
            print("   -> СЕЙЧАС в Spark должно появится окно за 10:00\n")

            # Финальный сигнал для закрытия всех окон
            # Отправляем время 10:05, чтобы Watermark (10:05 - 1 мин = 10:04) закрыл окно за 10:02.
            print("[5/5] Отправляем финальный сигнал (10:05:00) ")
            flush_ts = (start_ts + dt.timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
            flush_msg = f"{flush_ts},srv-flush,0,0\n"
            conn.sendall(flush_msg.encode('utf-8'))
            print("   -> СЕЙЧАС в Spark должны появиться все оставшиеся окна.\n")

            # Завершение
            print("Поток завершен.")
            time.sleep(5)


if __name__ == "__main__":
    try:
        run_server()
    except KeyboardInterrupt:
        print("\nОстановка.")
    except Exception as e:
        print(f"Ошибка: {e}")
