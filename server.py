# server.py
# FastAPI app: serves dashboard.html, exposes SSE, and JSON endpoints.
# Run with: uvicorn server:app --host 0.0.0.0 --port 8000
import asyncio
import csv
import json
from collections import defaultdict
from datetime import datetime
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path

DATA_FILE = Path("./data/sensors.csv")
ROOT_HTML = Path("./dashboard.html")  # adjust if your file is in another folder
POLL_INTERVAL = 1.0  # seconds

app = FastAPI(title="Pathway Dashboard Server")

# mount static if you have assets folder (optional)
if Path("static").exists():
    app.mount("/static", StaticFiles(directory="static"), name="static")

# in-memory state
state = {
    "alerts": [],
    "emissions": [],
    "status": {},
    "last_update": None,
}

# SSE subscribers (queues)
subscribers = set()


def read_csv_rows(path: Path):
    rows = []
    if not path.exists():
        return rows
    try:
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            for r in reader:
                # try to convert numeric fields safely
                for k in ("timestamp", "temperature", "vibration", "energy_consumption"):
                    if k in r and r[k] != "":
                        try:
                            r[k] = float(r[k])
                        except Exception:
                            # keep original if can't parse
                            pass
                rows.append(r)
    except Exception:
        pass
    return rows


def compute_snapshots(rows):
    # heuristic rules — tweak to match your pipeline/config
    TEMPERATURE_MULTIPLIER = 1.2
    VIBRATION_THRESHOLD = 0.8
    CO2_EMISSION_FACTOR = 0.475

    by_machine = defaultdict(list)
    for r in rows:
        mid = r.get("machine_id", "unknown")
        by_machine[mid].append(r)

    emissions = []
    alerts = []
    total_co2 = 0.0
    total_anomalies = 0

    for m, recs in by_machine.items():
        # sum co2
        cumulative_co2 = sum((r.get("energy_consumption") or 0.0) * CO2_EMISSION_FACTOR for r in recs)
        emissions.append({"machine_id": m, "cumulative_co2_kg": cumulative_co2})
        total_co2 += cumulative_co2

        temps = [r.get("temperature", 0.0) for r in recs if isinstance(r.get("temperature", None), (int, float))]
        vibs = [r.get("vibration", 0.0) for r in recs if isinstance(r.get("vibration", None), (int, float))]
        if temps:
            avg_temp = sum(temps) / len(temps)
            max_temp = max(temps)
        else:
            avg_temp = max_temp = 0.0
        if vibs:
            avg_vib = sum(vibs) / len(vibs)
            max_vib = max(vibs)
        else:
            avg_vib = max_vib = 0.0

        if (temps and max_temp > avg_temp * TEMPERATURE_MULTIPLIER) or (vibs and max_vib > VIBRATION_THRESHOLD):
            total_anomalies += 1
            latest = max(recs, key=lambda x: x.get("timestamp", 0))
            kind = "TEMP_SPIKE" if (temps and max_temp > avg_temp * TEMPERATURE_MULTIPLIER) else "HIGH_VIBRATION"
            alerts.append({
                "machine_id": m,
                "anomaly_type": kind,
                "current_temperature": latest.get("temperature"),
                "avg_temperature": avg_temp,
                "current_vibration": latest.get("vibration"),
                "alert_time": datetime.fromtimestamp(latest.get("timestamp", datetime.utcnow().timestamp())).isoformat(),
                "severity": "HIGH" if kind == "TEMP_SPIKE" or max_vib > (VIBRATION_THRESHOLD * 1.2) else "MEDIUM"
            })

    active_machines = len(by_machine)
    total_records = len(rows)
    if total_anomalies == 0:
        system_health = "HEALTHY"
    elif total_anomalies < max(1, active_machines // 4):
        system_health = "DEGRADED"
    else:
        system_health = "CRITICAL"

    status = {
        "system_health": system_health,
        "total_records_processed": total_records,
        "active_machines": active_machines,
        "total_anomalies_detected": total_anomalies,
        "avg_co2_per_machine": (total_co2 / active_machines) if active_machines else 0.0,
    }

    emissions_sorted = sorted(emissions, key=lambda x: x["cumulative_co2_kg"], reverse=True)
    return alerts, emissions_sorted, status


async def publisher_loop():
    # broadcast only on change to reduce UI flicker
    last_snapshots = {"alerts": None, "emissions": None, "status": None}
    while True:
        rows = read_csv_rows(DATA_FILE)
        alerts, emissions, status_snapshot = compute_snapshots(rows)
        now = datetime.utcnow().isoformat()

        state["alerts"] = alerts
        state["emissions"] = emissions
        state["status"] = status_snapshot
        state["last_update"] = now

        payload = {"alerts": alerts, "emissions": emissions, "status": status_snapshot, "last_update": now}

        for key in ("alerts", "emissions", "status"):
            if json.dumps(payload[key], sort_keys=True) != json.dumps(last_snapshots.get(key), sort_keys=True):
                last_snapshots[key] = payload[key]
                data = json.dumps({"type": key, "data": payload[key], "last_update": now})
                dead = []
                for q in list(subscribers):
                    try:
                        q.put_nowait(data)
                    except Exception:
                        dead.append(q)
                for d in dead:
                    subscribers.discard(d)

        await asyncio.sleep(POLL_INTERVAL)


@app.on_event("startup")
async def on_startup():
    asyncio.create_task(publisher_loop())


# Serve HTML at root (single file)
@app.get("/", include_in_schema=False)
async def root():
    if ROOT_HTML.exists():
        return FileResponse(str(ROOT_HTML))
    return JSONResponse({"error": "dashboard.html not found"}, status_code=404)


# Optional JSON endpoints (keeps backward compatibility if other code fetches)
@app.get("/api/v1/alerts")
async def get_alerts():
    return JSONResponse(state["alerts"])


@app.get("/api/v1/emissions")
async def get_emissions():
    return JSONResponse(state["emissions"])


@app.get("/api/v1/status")
async def get_status():
    return JSONResponse(state["status"])


@app.get("/api/v1/stream")
async def sse_stream(request: Request):
    """
    Server-sent events endpoint that pushes JSON messages:
      data: {"type":"alerts","data":[...],"last_update":"..."}
    Browser:
      const es = new EventSource("/api/v1/stream");
      es.onmessage = e => { const obj = JSON.parse(e.data); ... }
    """
    async def event_generator(q):
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await q.get()
                except asyncio.CancelledError:
                    break
                yield f"data: {msg}\n\n"
        finally:
            pass

    q = asyncio.Queue()
    subscribers.add(q)
    return StreamingResponse(event_generator(q), media_type="text/event-stream")