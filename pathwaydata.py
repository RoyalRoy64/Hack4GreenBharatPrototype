import pathway as pw
import sqlite3
import json

# --- CONFIG ---
DATA_FILE = "./data/sensors.csv"
DB_PATH = "data/ecosync.db"
VIBRATION_THRESHOLD = 0.8

# --- SCHEMA ---
class SensorData(pw.Schema):
    timestamp: float
    machine_id: str
    temperature: float
    vibration: float
    energy_consumption: float

# --- 1. MEMORY CACHE ---
import time
machines_cache = {}
last_cache_update = 0

def get_machine_limit(machine_id: str) -> float:
    global last_cache_update
    now = time.time()
    if now - last_cache_update > 5:
        try:
            conn = sqlite3.connect(DB_PATH, timeout=5)
            rows = conn.execute("SELECT id, operating_limits FROM machines").fetchall()
            for r in rows:
                if r[1]:
                    limits = json.loads(r[1])
                    if limits.get("temperature_max") is not None:
                        machines_cache[r[0]] = float(limits["temperature_max"])
            conn.close()
        except Exception:
            pass
        last_cache_update = now
        
    return machines_cache.get(machine_id, 80.0)

# --- 1. LLM LIMIT CHECKER ---
@pw.udf
def check_anomalies(machine_id: str, temperature: float, vibration: float) -> str:
    temp_max = get_machine_limit(machine_id)
        
    alerts = []
    if temperature > temp_max:
        alerts.append(f"CRITICAL: Temp {temperature:.1f}°C > LLM limit of {temp_max}°C")
    if vibration > VIBRATION_THRESHOLD:
        alerts.append(f"WARNING: High vibration ({vibration:.2f})")
        
    return " | ".join(alerts)

# --- 2. FILE QUEUE SINK ---
def save_alerts_to_file(key, row, time, is_addition):
    if not is_addition or not row.get("alert_msg"):
        return
        
    try:
        with open("data/alerts.jsonl", "a", encoding="utf-8") as f:
            for msg in row["alert_msg"].split(" | "):
                if msg.strip():
                    level = "CRITICAL" if "CRITICAL" in msg else "WARNING"
                    payload = {
                        "machine_id": row["machine_id"],
                        "level": level,
                        "message": msg.strip(),
                        "timestamp": time
                    }
                    f.write(json.dumps(payload) + "\n")
    except Exception:
        pass

# --- 3. STREAM PIPELINE ---
def run():
    print("[INFO] Starting Minimal Pathway Engine...")
    
    # Read stream
    stream = pw.io.csv.read(DATA_FILE, schema=SensorData, mode="streaming")
    
    # Analyze and filter down to anomalies only
    alerts_table = stream.select(
        *pw.this,
        alert_msg=check_anomalies(pw.this.machine_id, pw.this.temperature, pw.this.vibration)
    ).filter(pw.this.alert_msg != "")

    # Output to JSONL
    pw.io.subscribe(alerts_table, save_alerts_to_file)

    # Run silently in the background
    pw.run(monitoring_level=pw.MonitoringLevel.NONE)

if __name__ == "__main__":
    run()