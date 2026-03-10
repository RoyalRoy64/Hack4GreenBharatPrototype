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

# # workers/maintenance_worker.py
# import time
# import threading
# from datetime import datetime, timedelta
# from database.db import get_manual, get_maintenance_tasks, add_maintenance_task, get_manual_parsed_thresholds
# import csv
# import os

# SENSOR_CSV = "data/sensors.csv"  # your existing CSV path; adjust if necessary
# POLL_INTERVAL = 10  # seconds - frequency to run the worker

# def estimate_runtime_from_csv(machine_id):
#     """
#     Simple heuristic: count entries in CSV for machine_id -> treat as minutes (demo).
#     Replace with real runtime aggregator if available.
#     """
#     if not os.path.exists(SENSOR_CSV):
#         return 0
#     runtime = 0
#     with open(SENSOR_CSV, "r") as f:
#         reader = csv.DictReader(f)
#         for row in reader:
#             if row.get("machine_id") == machine_id:
#                 runtime += 1  # each row = 1 tick
#     return runtime

# def schedule_from_manual(machine_id):
#     m = get_manual(machine_id)
#     if not m:
#         return
#     parsed = m.get("parsed_json", {})
#     maintenance_list = parsed.get("maintenance", [])
#     # For each maintenance task, create an entry in maintenance_tasks table if not present
#     for item in maintenance_list:
#         task = item.get("task") or item.get("value") or "maintenance"
#         interval = item.get("interval") or item.get("notes") or None
#         # naive scheduled_date: set to now + 1 day if interval unknown
#         scheduled_date = (datetime.utcnow() + timedelta(days=1)).isoformat()
#         add_maintenance_task(machine_id, task, interval, scheduled_date)

# def worker_loop(stop_event):
#     while not stop_event.is_set():
#         # find all manuals, schedule tasks if not already scheduled
#         # Simple approach: for demo, schedule tasks for each manual if no tasks exist
#         # to avoid duplicate scheduling, this can be improved
#         # Here we use get_maintenance_tasks to check if tasks exist for a machine
#         from database.db import get_maintenance_tasks, get_manual
#         # fetch machines with manuals
#         conn = None
#         # naive: read manual records directly by querying DB functions: we'll call parse results
#         # get manual list by reusing get_maintenance_tasks (it can return all tasks)
#         # For speed: load all manuals from DB by reading files in data/manuals folder
#         manuals_dir = "data/manuals"
#         if os.path.exists(manuals_dir):
#             for fname in os.listdir(manuals_dir):
#                 if fname.lower().endswith(".pdf"):
#                     # extract machine_id from filename prefix until first underscore
#                     mid = fname.split("_")[0]
#                     # if no tasks exist for this machine, create schedule
#                     tasks = get_maintenance_tasks(mid)
#                     if not tasks:
#                         schedule_from_manual(mid)
#         # sleep until next poll
#         for _ in range(int(POLL_INTERVAL)):
#             if stop_event.is_set():
#                 break
#             time.sleep(1)

# def start_worker_thread():
#     stop = threading.Event()
#     t = threading.Thread(target=worker_loop, args=(stop,), daemon=True)
#     t.start()
#     return stop