# app_runner.py
import asyncio
import csv
import random
import time
import os
import importlib
import sys
import uvicorn
from pathlib import Path


# ==============================
# CONFIG
DATA_DIR = Path("data")
DATA_FILE = DATA_DIR / "sensors.csv"

HEADERS = [
    "timestamp",
    "machine_id",
    "temperature",
    "vibration",
    "energy_consumption"
]


# ==============================
# ASYNC DATA GENERATOR
async def async_data_generator():
    print("🚀 EcoSync Async Data Generator started")

    DATA_DIR.mkdir(exist_ok=True)

    # Write headers once
    if not DATA_FILE.exists():
        with open(DATA_FILE, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(HEADERS)

    while True:
        is_anomaly = random.random() > 0.9

        temp = random.uniform(95, 110) if is_anomaly else random.uniform(60, 80)
        vib = random.uniform(0.8, 1.0) if is_anomaly else random.uniform(0.1, 0.4)

        row = [
            time.time(),
            random.choice(["M1", "M2", "M3"]),
            round(temp, 2),
            round(vib, 2),
            round(random.uniform(10, 30), 2),
        ]

        # File writing is blocking → offload to thread
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, append_row, row)

        print(f"[DATA_GEN] Sent: {row}")

        await asyncio.sleep(1)


def append_row(row):
    with open(DATA_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(row)


# ==============================
# PATHWAY STARTER
def resolve_callable(module_name, candidates=("main", "run")):
    try:
        mod = importlib.import_module(module_name)
    except Exception as e:
        print(f"[app_runner] failed to import {module_name}: {e}")
        return None

    for name in candidates:
        if hasattr(mod, name) and callable(getattr(mod, name)):
            return getattr(mod, name)

    print(f"[app_runner] no callable found in {module_name}")
    return None


async def run_blocking_function(func, label):
    loop = asyncio.get_running_loop()
    print(f"[app_runner] starting {label} in executor")
    try:
        await loop.run_in_executor(None, func)
    except Exception as e:
        print(f"[app_runner] {label} crashed: {e}", file=sys.stderr)


# ==============================
# SERVER STARTER
async def start_server():
    config = uvicorn.Config(
        "server:app",
        host="0.0.0.0",
        port=8000,
        log_level="info",
        loop="asyncio",
    )
    server = uvicorn.Server(config)
    await server.serve()


# ==============================
# MAIN ENTRY
async def main():
    print("⚡ Booting EcoSync Unified Runtime")

    tasks = []

    # Start Pathway
    pathway_func = resolve_callable("main", candidates=("main", "run"))
    if pathway_func:
        tasks.append(asyncio.create_task(
            run_blocking_function(pathway_func, "Pathway Main")
        ))

    # Start async data generator
    tasks.append(asyncio.create_task(async_data_generator()))

    # Start web server (blocks until shutdown)
    await start_server()

    # Cancel background tasks on shutdown
    for t in tasks:
        t.cancel()


if __name__ == "__main__":
    asyncio.run(main())