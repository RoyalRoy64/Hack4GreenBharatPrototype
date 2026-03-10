import threading
import time

def worker_loop():
    while True:
        # We will add the actual LLM schedule logic here later
        time.sleep(60)

def start_worker_thread():
    t = threading.Thread(target=worker_loop, daemon=True)
    t.start()
    print("[WORKER] Maintenance background thread started.")