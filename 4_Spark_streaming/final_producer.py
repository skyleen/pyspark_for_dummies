import requests
import json
import time
import os
from datetime import datetime, timezone

API_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
OUTPUT_DIR = "source_wiki_stream"
BATCH_INTERVAL_SEC = 10
EVENTS_PER_FILE = 50

os.makedirs(OUTPUT_DIR, exist_ok=True)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) Gecko/20100101 Firefox/117.0"
]

def stream_wikimedia_changes():
    """
    Читает события из Wikimedia RecentChange API (SSE)
    и сохраняет их в виде JSON-файлов для Structured Streaming.
    Переподключается автоматически при обрыве.
    """
    buffer = []

    while True:
        headers = {
            "User-Agent": USER_AGENTS[int(time.time()) % len(USER_AGENTS)],
            "Accept": "text/event-stream"
        }

        try:
            response = requests.get(API_URL, headers=headers, stream=True, timeout=60)
            response.raise_for_status()

            for line in response.iter_lines():
                if not line:
                    continue

                decoded_line = line.decode("utf-8")
                if decoded_line.startswith("data:"):
                    event_json = decoded_line.replace("data:", "").strip()
                    try:
                        event = json.loads(event_json)
                        buffer.append(event)
                    except json.JSONDecodeError:
                        continue

                if len(buffer) >= EVENTS_PER_FILE:
                    write_batch(buffer)
                    buffer.clear()
                    time.sleep(BATCH_INTERVAL_SEC)

        except (requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            print(f"[WARN] Connection lost: {e}. Reconnecting in 5 seconds...")
            time.sleep(5)
        except requests.exceptions.HTTPError as e:
            print(f"[ERROR] HTTP error: {e}. Sleeping 10s before retry...")
            time.sleep(10)
        except Exception as e:
            print(f"[ERROR] Unexpected error: {e}. Sleeping 10s before retry...")
            time.sleep(10)


def write_batch(events):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    file_path = os.path.join(OUTPUT_DIR, f"wiki_events_{timestamp}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")
    print(f"Saved {len(events)} events to {file_path}")


if __name__ == "__main__":
    print("Starting Wikimedia RecentChange API")
    stream_wikimedia_changes()
