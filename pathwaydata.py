from typing import Tuple
from datetime import datetime
import pathway as pw
import sqlite3
import json
from pathlib import Path

# ================= CONFIG =================
# DATA_DIRECTORY = "./data/"
DATA_FILE = "./data/sensors.csv"
DB_PATH = Path("data/ecosync.db")
VIBRATION_THRESHOLD = 0.8
CO2_EMISSION_FACTOR = 0.475

# ================= SCHEMA =================
class SensorData(pw.Schema):
    timestamp: float
    machine_id: str
    temperature: float
    vibration: float
    energy_consumption: float

# ================= 1. AI THRESHOLD CHECKER =================
@pw.udf
def check_anomalies(machine_id: str, temperature: float, vibration: float) -> str:
    """
    This Pathway User Defined Function runs on every row.
    It fetches the LLM-extracted limits from SQLite to evaluate the sensor.
    """
    # Default fallback if the LLM hasn't processed the manual yet
    temp_max = 80.0 
    
    # 1. Connect to DB to get Gemini's limits
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT operating_limits FROM machines WHERE id = ?", (machine_id,)).fetchone()
        
        if row and row['operating_limits']:
            limits = json.loads(row['operating_limits'])
            if "temperature_max" in limits and limits["temperature_max"] is not None:
                temp_max = float(limits["temperature_max"])
        conn.close()
    except Exception:
        pass # If DB is locked momentarily, fallback to 80.0
        
    alerts = []
    
    # 2. Evaluate against LLM Logic
    if temperature > temp_max:
        alerts.append(f"CRITICAL: Temp {temperature:.1f}°C > LLM limit of {temp_max}°C")
        
    # 3. Evaluate against Static Logic
    if vibration > VIBRATION_THRESHOLD:
        alerts.append(f"WARNING: High vibration ({vibration:.2f})")
        
    # Return as a delimited string so Pathway can handle it cleanly
    return " | ".join(alerts)

# ================= 2. SQLITE SINK =================
def save_alerts_to_db(key, row, time, is_addition):
    """
    This callback catches any row that triggered an alert in Pathway 
    and writes it directly to the database for the frontend to see.
    """
    if not is_addition or not row.get("alert_msg"):
        return
        
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5)
        msgs = row["alert_msg"].split(" | ")
        
        for msg in msgs:
            if msg.strip():
                level = "CRITICAL" if "CRITICAL" in msg else "WARNING"
                conn.execute(
                    "INSERT INTO alerts (machine_id, level, message) VALUES (?, ?, ?)",
                    (row["machine_id"], level, msg.strip())
                )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error saving alert to DB: {e}")

# ================= 3. STREAM PROCESSING =================
def process_sensor_stream():
    # Read the streaming CSV
    sensor_data = pw.io.csv.read(
        DATA_FILE,
        schema=SensorData,
        mode="streaming",
    )

    # Apply our LLM anomaly detector to every row
    analyzed = sensor_data.select(
        *pw.this,
        alert_msg=check_anomalies(pw.this.machine_id, pw.this.temperature, pw.this.vibration)
    )

    # Filter down to ONLY the rows that generated an alert
    alerts_table = analyzed.filter(pw.this.alert_msg != "")

    # Subscribe to the filtered table to push alerts into SQLite
    pw.io.subscribe(alerts_table, save_alerts_to_db)

    # Calculate CO2 emissions for the Dashboard KPI tiles
    emissions = (
        sensor_data
        .groupby(pw.this.machine_id)
        .reduce(
            machine_id=pw.this.machine_id,
            total_co2_kg=pw.reducers.sum(
                pw.this.energy_consumption * CO2_EMISSION_FACTOR
            ),
        )
    )

    return alerts_table, emissions

# ================= MAIN RUNNER =================
def run():
    print("=" * 80)
    print("EcoSync Sentinel: Real-time AI for Green Manufacturing")
    print("=" * 80)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Initializing Pathway Engine...")

    alerts, emissions = process_sensor_stream()

    print("[INFO] Running streaming engine...")
    pw.run()

if __name__ == "__main__":
    run()