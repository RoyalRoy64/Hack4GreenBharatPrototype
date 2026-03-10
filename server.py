import asyncio
import json
import csv
from pathlib import Path
from fastapi import FastAPI, UploadFile, Request, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sse_starlette.sse import EventSourceResponse
from database.db import get_db
from services.gemini_llm import extract_manual_data # The script we wrote earlier

app = FastAPI(title="EcoSync Sentinel")

# Mount static files (your CSS and JS)
# Assuming your html, css, and js are in a "frontend" folder or root
@app.get("/app.js")
def serve_js():
    return FileResponse("app.js")

@app.get("/style.css")
def serve_css():
    return FileResponse("style.css")

# ==========================================
# 1. PAGE ROUTERS
# ==========================================
@app.get("/")
# @app.get("/index.html") 
def serve_dashboard():
    return FileResponse("index.html")

@app.get("/diagnostics")
def serve_diagnostics():
    return FileResponse("diagnostics.html")

@app.get("/maintenance")
def serve_maintenance():
    return FileResponse("maintenance.html")

# ==========================================
# 2. REST API ENDPOINTS (For app.js)
# ==========================================
@app.get("/api/machines")
def get_machines():
    conn = get_db()
    machines = conn.execute("SELECT id, name, status FROM machines").fetchall()
    return [dict(m) for m in machines]

@app.get("/api/machines/{machine_id}")
def get_machine_details(machine_id: str):
    conn = get_db()
    machine = conn.execute("SELECT * FROM machines WHERE id = ?", (machine_id,)).fetchone()
    if not machine:
        raise HTTPException(status_code=404, detail="Machine not found")
    
    data = dict(machine)
    # Parse the JSON string back into a dict for the frontend
    if data.get("operating_limits"):
        data["operating_limits"] = json.loads(data["operating_limits"])
    return data

@app.put("/api/machines/{machine_id}/limits")
async def update_limits(machine_id: str, request: Request):
    limits = await request.json()
    conn = get_db()
    conn.execute(
        "UPDATE machines SET operating_limits = ? WHERE id = ?", 
        (json.dumps(limits), machine_id)
    )
    conn.commit()
    return {"status": "success"}

@app.get("/api/maintenance/schedule")
def get_schedule():
    conn = get_db()
    # Map SQL columns to exactly what app.js expects: machine, task, date, severity
    tasks = conn.execute('''
        SELECT machine_id as machine, task, scheduled_date as date, severity 
        FROM maintenance_tasks
    ''').fetchall()
    return [dict(t) for t in tasks]

@app.get("/api/inventory")
def get_inventory():
    conn = get_db()
    items = conn.execute('''
        SELECT machine_id, name, part_number as partNumber, description, 
               current_stock as currentStock, minimum_required as minimumRequired
        FROM spare_parts
    ''').fetchall()
    return [dict(i) for i in items]

# ==========================================
# 3. GEMINI LLM INTEGRATION
# ==========================================
@app.post("/api/manuals/upload")
async def upload_manual(file: UploadFile):
    # 1. Save the uploaded PDF temporarily
    file_path = f"data/manuals/{file.filename}"
    Path("data/manuals").mkdir(parents=True, exist_ok=True)
    
    with open(file_path, "wb") as f:
        f.write(await file.read())
        
    # 2. Pass to Gemini 2.5 Pro (from gemini_llm.py)
    extracted_data = extract_manual_data(file_path)
    
    # 3. Save the results to the SQLite Database
    conn = get_db()
    machine_id = extracted_data["machine_detail"]["machine_name"].replace(" ", "_")
    
    conn.execute('''
        INSERT OR REPLACE INTO machines (id, name, type, description, operating_limits)
        VALUES (?, ?, ?, ?, ?)
    ''', (
        machine_id,
        extracted_data["machine_detail"]["machine_name"],
        extracted_data["machine_detail"]["machine_type"],
        extracted_data["machine_detail"]["machine_description"],
        json.dumps({
            "temperature_max": extracted_data["operating_temperature"].get("max"),
            "sound_max": extracted_data.get("sound_level_db")
        })
    ))
    conn.commit()
    
    # (Optional: Add logic here to loop through extracted_data["spare_parts"] 
    # and insert them into the spare_parts table)
    
    return {"status": "success", "machine_id": machine_id, "data": extracted_data}

# ==========================================
# 4. SERVER SENT EVENTS (SSE) STREAMS
# ==========================================
@app.get("/api/stream/sensors")
async def stream_sensors(request: Request):
    """Tails the sensors.csv file and pushes live data to the UI."""
    async def event_generator():
        data_file = Path("data/sensors.csv")
        last_pos = 0
        
        while True:
            if await request.is_disconnected():
                break
                
            if data_file.exists():
                with open(data_file, "r") as f:
                    f.seek(last_pos)
                    lines = f.readlines()
                    last_pos = f.tell()
                    
                    for line in lines:
                        if "timestamp" in line: continue # Skip header
                        parts = line.strip().split(",")
                        if len(parts) >= 4:
                            machine_id = parts[1]
                            # Fetch LLM limits to draw the red line on the charts
                            conn = get_db()
                            limits_row = conn.execute("SELECT operating_limits FROM machines WHERE id=?", (machine_id,)).fetchone()
                            temp_max = None
                            if limits_row and limits_row[0]:
                                temp_max = json.loads(limits_row[0]).get("temperature_max")

                            payload = {
                                "machineId": machine_id,
                                "temperature": float(parts[2]),
                                "vibration": float(parts[3]),
                                "temperatureMax": temp_max, # App.js uses this for the red chart line
                                "systemHealth": 98,
                                "machinesOnline": 3
                            }
                            yield {"data": json.dumps(payload)}
                            
            await asyncio.sleep(1) # Send data every second
            
    return EventSourceResponse(event_generator())

@app.get("/api/stream/alerts")
async def stream_alerts(request: Request):
    """Pushes alerts generated by Pathway to the UI."""
    async def event_generator():
        last_id = 0
        while True:
            if await request.is_disconnected():
                break
            
            # Check DB for new alerts created by Pathway
            conn = get_db()
            new_alerts = conn.execute("SELECT * FROM alerts WHERE id > ?", (last_id,)).fetchall()
            
            for alert in new_alerts:
                last_id = alert["id"]
                payload = {
                    "machine": alert["machine_id"],
                    "severity": alert["level"],
                    "message": alert["message"]
                }
                yield {"data": json.dumps(payload)}
                
            await asyncio.sleep(2)
            
    return EventSourceResponse(event_generator())

# # server.py
# # FastAPI app: serves dashboard.html, exposes SSE, and JSON endpoints.
# # Run with: uvicorn server:app --host 0.0.0.0 --port 8000
# import asyncio
# import csv
# import json
# from collections import defaultdict
# from datetime import datetime
# from fastapi import FastAPI, Request
# from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
# from fastapi.staticfiles import StaticFiles
# from pathlib import Path

# from api.manuals import router as manuals_router
# from api.maintenance import router as maintenance_router


# DATA_FILE = Path("./data/sensors.csv")
# ROOT_HTML = Path("./dashboard.html")  # adjust if your file is in another folder
# POLL_INTERVAL = 1.0  # seconds

# app = FastAPI(title="Pathway Dashboard Server")

# app.include_router(manuals_router)
# app.include_router(maintenance_router)

# # mount static if you have assets folder (optional)
# if Path("static").exists():
#     app.mount("/static", StaticFiles(directory="static"), name="static")

# # in-memory state
# state = {
#     "alerts": [],
#     "emissions": [],
#     "status": {},
#     "last_update": None,
# }

# # SSE subscribers (queues)
# subscribers = set()


# def read_csv_rows(path: Path):
#     rows = []
#     if not path.exists():
#         return rows
#     try:
#         with open(path, newline="") as f:
#             reader = csv.DictReader(f)
#             for r in reader:
#                 # try to convert numeric fields safely
#                 for k in ("timestamp", "temperature", "vibration", "energy_consumption"):
#                     if k in r and r[k] != "":
#                         try:
#                             r[k] = float(r[k])
#                         except Exception:
#                             # keep original if can't parse
#                             pass
#                 rows.append(r)
#     except Exception:
#         pass
#     return rows


# def compute_snapshots(rows):
#     # heuristic rules — tweak to match your pipeline/config
#     TEMPERATURE_MULTIPLIER = 1.2
#     VIBRATION_THRESHOLD = 0.8
#     CO2_EMISSION_FACTOR = 0.475

#     by_machine = defaultdict(list)
#     for r in rows:
#         mid = r.get("machine_id", "unknown")
#         by_machine[mid].append(r)

#     emissions = []
#     alerts = []
#     total_co2 = 0.0
#     total_anomalies = 0

#     for m, recs in by_machine.items():
#         # sum co2
#         cumulative_co2 = sum((r.get("energy_consumption") or 0.0) * CO2_EMISSION_FACTOR for r in recs)
#         emissions.append({"machine_id": m, "cumulative_co2_kg": cumulative_co2})
#         total_co2 += cumulative_co2

#         temps = [r.get("temperature", 0.0) for r in recs if isinstance(r.get("temperature", None), (int, float))]
#         vibs = [r.get("vibration", 0.0) for r in recs if isinstance(r.get("vibration", None), (int, float))]
#         if temps:
#             avg_temp = sum(temps) / len(temps)
#             max_temp = max(temps)
#         else:
#             avg_temp = max_temp = 0.0
#         if vibs:
#             avg_vib = sum(vibs) / len(vibs)
#             max_vib = max(vibs)
#         else:
#             avg_vib = max_vib = 0.0

#         if (temps and max_temp > avg_temp * TEMPERATURE_MULTIPLIER) or (vibs and max_vib > VIBRATION_THRESHOLD):
#             total_anomalies += 1
#             latest = max(recs, key=lambda x: x.get("timestamp", 0))
#             kind = "TEMP_SPIKE" if (temps and max_temp > avg_temp * TEMPERATURE_MULTIPLIER) else "HIGH_VIBRATION"
#             alerts.append({
#                 "machine_id": m,
#                 "anomaly_type": kind,
#                 "current_temperature": latest.get("temperature"),
#                 "avg_temperature": avg_temp,
#                 "current_vibration": latest.get("vibration"),
#                 "alert_time": datetime.fromtimestamp(latest.get("timestamp", datetime.utcnow().timestamp())).isoformat(),
#                 "severity": "HIGH" if kind == "TEMP_SPIKE" or max_vib > (VIBRATION_THRESHOLD * 1.2) else "MEDIUM"
#             })

#     active_machines = len(by_machine)
#     total_records = len(rows)
#     if total_anomalies == 0:
#         system_health = "HEALTHY"
#     elif total_anomalies < max(1, active_machines // 4):
#         system_health = "DEGRADED"
#     else:
#         system_health = "CRITICAL"

#     status = {
#         "system_health": system_health,
#         "total_records_processed": total_records,
#         "active_machines": active_machines,
#         "total_anomalies_detected": total_anomalies,
#         "avg_co2_per_machine": (total_co2 / active_machines) if active_machines else 0.0,
#     }

#     emissions_sorted = sorted(emissions, key=lambda x: x["cumulative_co2_kg"], reverse=True)
#     return alerts, emissions_sorted, status


# async def publisher_loop():
#     # broadcast only on change to reduce UI flicker
#     last_snapshots = {"alerts": None, "emissions": None, "status": None}
#     while True:
#         rows = read_csv_rows(DATA_FILE)
#         alerts, emissions, status_snapshot = compute_snapshots(rows)
#         now = datetime.utcnow().isoformat()

#         state["alerts"] = alerts
#         state["emissions"] = emissions
#         state["status"] = status_snapshot
#         state["last_update"] = now

#         payload = {"alerts": alerts, "emissions": emissions, "status": status_snapshot, "last_update": now}

#         for key in ("alerts", "emissions", "status"):
#             if json.dumps(payload[key], sort_keys=True) != json.dumps(last_snapshots.get(key), sort_keys=True):
#                 last_snapshots[key] = payload[key]
#                 data = json.dumps({"type": key, "data": payload[key], "last_update": now})
#                 dead = []
#                 for q in list(subscribers):
#                     try:
#                         q.put_nowait(data)
#                     except Exception:
#                         dead.append(q)
#                 for d in dead:
#                     subscribers.discard(d)

#         await asyncio.sleep(POLL_INTERVAL)


# from workers.maintenance_worker import start_worker_thread
# @app.on_event("startup")
# async def on_startup():
#     start_worker_thread()
#     asyncio.create_task(publisher_loop())


# # Serve HTML at root (single file)
# @app.get("/", include_in_schema=False)
# async def root():
#     if ROOT_HTML.exists():
#         return FileResponse(str(ROOT_HTML))
#     return JSONResponse({"error": "dashboard.html not found"}, status_code=404)


# # Optional JSON endpoints (keeps backward compatibility if other code fetches)
# @app.get("/api/v1/alerts")
# async def get_alerts():
#     return JSONResponse(state["alerts"])


# @app.get("/api/v1/emissions")
# async def get_emissions():
#     return JSONResponse(state["emissions"])


# @app.get("/api/v1/status")
# async def get_status():
#     return JSONResponse(state["status"])


# @app.get("/api/v1/stream")
# async def sse_stream(request: Request):
#     """
#     Server-sent events endpoint that pushes JSON messages:
#       data: {"type":"alerts","data":[...],"last_update":"..."}
#     Browser:
#       const es = new EventSource("/api/v1/stream");
#       es.onmessage = e => { const obj = JSON.parse(e.data); ... }
#     """
#     async def event_generator(q):
#         try:
#             while True:
#                 if await request.is_disconnected():
#                     break
#                 try:
#                     msg = await q.get()
#                 except asyncio.CancelledError:
#                     break
#                 yield f"data: {msg}\n\n"
#         finally:
#             pass

#     q = asyncio.Queue()
#     subscribers.add(q)
#     return StreamingResponse(event_generator(q), media_type="text/event-stream")