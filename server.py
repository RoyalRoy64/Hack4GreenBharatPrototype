import asyncio
import json
import csv
import datetime
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

from fastapi.responses import RedirectResponse

@app.get("/diagnostics")
def serve_diagnostics():
    return RedirectResponse(url="/#diagnostics")

@app.get("/maintenance")
def serve_maintenance():
    return RedirectResponse(url="/#maintenance")

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
    # print(file_path)
    with open(file_path, "wb") as f:
        f.write(await file.read())

    json_dir = Path("data/manuals/manuals_json")
    json_dir.mkdir(parents=True, exist_ok=True)
    json_path = json_dir / f"{Path(file.filename).stem}.json"

    if json_path.exists():
        print(f"Skipping extraction, reading from existing JSON: {json_path}")
        await asyncio.sleep(3)
        with open(json_path, "r", encoding="utf-8") as f:
            extracted_data = json.load(f)
    else:
        # 2. Pass to Gemini 2.5 Pro (from gemini_llm.py)
        extracted_data = extract_manual_data(file_path)

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(extracted_data, f, indent=2)
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
    
    # Clear existing tasks and parts for this machine to avoid duplicates from multiple uploads
    conn.execute("DELETE FROM maintenance_tasks WHERE machine_id = ?", (machine_id,))
    conn.execute("DELETE FROM spare_parts WHERE machine_id = ?", (machine_id,))
    
    today = datetime.date.today()
    for task_data in extracted_data.get("maintenance", []):
        task_desc = task_data.get("task", "")
        interval = task_data.get("interval", "")
        period_days = task_data.get("period", 30)
        scheduled_date = (today + datetime.timedelta(days=period_days)).strftime("%Y-%m-%d")
        
        conn.execute('''
            INSERT INTO maintenance_tasks (machine_id, task, interval, scheduled_date, severity)
            VALUES (?, ?, ?, ?, ?)
        ''', (machine_id, task_desc, interval, scheduled_date, 'INFO'))
        
    for part_data in extracted_data.get("spare_parts", []):
        conn.execute('''
            INSERT INTO spare_parts (machine_id, name, part_number, description, current_stock, minimum_required)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            machine_id,
            part_data.get("name", ""),
            part_data.get("part_number", ""),
            part_data.get("description", ""),
            0, # Default stock
            part_data.get("minimum_required_parts", 1)
        ))
        
    conn.commit()
    
    return {"status": "success", "machine_id": machine_id, "data": extracted_data}

# ==========================================
# 4. SERVER SENT EVENTS (SSE) STREAMS
# ==========================================
@app.get("/api/stream/sensors")
async def stream_sensors(request: Request):
    """Tails the sensors.csv file and pushes live data to the UI using a lightweight memory cache."""
    async def event_generator():
        data_file = Path("data/sensors.csv")
        last_pos = 0
        machine_limits_cache = {}
        last_cache_update = 0
        
        # In-memory trackers
        system_health = 100.0 
        cumulative_co2 = 0.0  # Starts at zero and permanently adds up
        
        while True:
            if await request.is_disconnected():
                break
                
            now = asyncio.get_event_loop().time()
            
            # Refresh SQLite cache every 5 seconds
            if now - last_cache_update > 5:
                try:
                    conn = get_db()
                    rows = conn.execute("SELECT id, operating_limits FROM machines").fetchall()
                    for r in rows:
                        if r["operating_limits"]:
                            parsed = json.loads(r["operating_limits"])
                            machine_limits_cache[r["id"]] = parsed.get("temperature_max")
                except Exception as e:
                    pass
                finally:
                    conn.close() 
                last_cache_update = now
                
            lines_read = False
            if data_file.exists():
                with open(data_file, "r") as f:
                    f.seek(last_pos)
                    lines = f.readlines()
                    last_pos = f.tell()
                    
                    for line in lines:
                        lines_read = True
                        if "timestamp" in line: continue # Skip header
                        
                        parts = line.strip().split(",")
                        if len(parts) >= 5: 
                            machine_id = parts[1]
                            temp = float(parts[2])
                            vib = float(parts[3])
                            energy_kw = float(parts[4])
                            
                            temp_max = machine_limits_cache.get(machine_id)
                            
                            # 1. Cumulative CO2 Footprint (kg)
                            # Assuming energy_kw is power, we divide by 3600 to get kWh for a 1-second ping.
                            # Multiply by 0.475 grid factor to get the tiny kg increment.
                            co2_increment = (energy_kw / 3600.0) * 0.475
                            cumulative_co2 += co2_increment
                            
                            # 2. Dynamic System Health Logic
                            effective_temp_max = temp_max if temp_max else 80.0
                            
                            if temp > effective_temp_max:
                                system_health -= 2.0  
                            elif vib > 0.8:
                                system_health -= 1.0  
                            else:
                                system_health += 0.5  
                                
                            system_health = max(0.0, min(100.0, system_health))
                            
                            payload = {
                                "machineId": machine_id,
                                "temperature": temp,
                                "vibration": vib,
                                "temperatureMax": temp_max, 
                                "systemHealth": round(system_health, 1),
                                # Send the cumulative total, rounded to 4 decimals so it ticks smoothly
                                "co2Footprint": round(cumulative_co2, 4), 
                                "machinesOnline": 3
                            }
                            yield {"data": json.dumps(payload)}
                            
            if not lines_read:
                await asyncio.sleep(0.5)
            
    return EventSourceResponse(event_generator())
# @app.get("/api/stream/sensors")
# async def stream_sensors(request: Request):
#     """Tails the sensors.csv file and pushes live data to the UI using a lightweight memory cache."""
#     async def event_generator():
#         data_file = Path("data/sensors.csv")
#         last_pos = 0
#         machine_limits_cache = {}
#         last_cache_update = 0
        
#         while True:
#             if await request.is_disconnected():
#                 break
                
#             now = asyncio.get_event_loop().time()
#             # Refresh SQLite cache every 5 seconds instead of every row
#             if now - last_cache_update > 5:
#                 conn = get_db()
#                 try:
#                     rows = conn.execute("SELECT id, operating_limits FROM machines").fetchall()
#                     for r in rows:
#                         if r["operating_limits"]:
#                             parsed = json.loads(r["operating_limits"])
#                             machine_limits_cache[r["id"]] = parsed.get("temperature_max")
#                 except Exception:
#                     pass
#                 last_cache_update = now
                
#             if data_file.exists():
#                 with open(data_file, "r") as f:
#                     f.seek(last_pos)
#                     lines = f.readlines()
#                     last_pos = f.tell()
                    
#                     for line in lines:
#                         if "timestamp" in line: continue # Skip header
#                         parts = line.strip().split(",")
#                         if len(parts) >= 4:
#                             machine_id = parts[1]
#                             temp_max = machine_limits_cache.get(machine_id)
                            
#                             payload = {
#                                 "machineId": machine_id,
#                                 "temperature": float(parts[2]),
#                                 "vibration": float(parts[3]),
#                                 "temperatureMax": temp_max, # App.js uses this for the red chart line
#                                 "systemHealth": 98,
#                                 "machinesOnline": 3
#                             }
#                             yield {"data": json.dumps(payload)}
                            
#             # Don't sleep if we just processed lines, loop again. 
#             # Only sleep if we actually hit EOF
#             if not lines:
#                 await asyncio.sleep(0.5)
            
#     return EventSourceResponse(event_generator())

@app.get("/api/stream/alerts")
async def stream_alerts(request: Request):
    """Tails the alerts.jsonl file created by Pathway safely without DB locks."""
    async def event_generator():
        data_file = Path("data/alerts.jsonl")
        last_pos = 0
        
        while True:
            if await request.is_disconnected():
                break
                
            if data_file.exists():
                with open(data_file, "r", encoding="utf-8") as f:
                    f.seek(last_pos)
                    lines = f.readlines()
                    last_pos = f.tell()
                    
                    for line in lines:
                        if not line.strip(): continue
                        try:
                            alert = json.loads(line)
                            payload = {
                                "machine": alert["machine_id"],
                                "severity": alert["level"],
                                "message": alert["message"]
                            }
                            yield {"data": json.dumps(payload)}
                        except json.JSONDecodeError:
                            continue
                            
            if not getattr(locals(), 'lines', None):
                await asyncio.sleep(0.5)
            
    return EventSourceResponse(event_generator())

# # server.py