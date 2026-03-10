from typing import Tuple
from datetime import datetime
import pathway as pw
from database.db import get_manual_parsed_thresholds, save_alert

# inside your sensor processing / alerting function (pseudocode):
def evaluate_sensor_row(machine_id, temperature, vibration, sound_db, row_meta=None):
    # fetch any manual-derived thresholds
    t = get_manual_parsed_thresholds(machine_id) or {}
    temp_max = t.get("temp_max")
    temp_min = t.get("temp_min")
    sound_th = t.get("sound_db")

    # Normalize None fallback to your original static thresholds (configured in config.py)
    from config import DEFAULT_TEMP_MAX, DEFAULT_TEMP_MIN, DEFAULT_SOUND_DB

    if temp_max is None:
        temp_max = DEFAULT_TEMP_MAX
    if temp_min is None:
        temp_min = DEFAULT_TEMP_MIN
    if sound_th is None:
        sound_th = DEFAULT_SOUND_DB

    # now evaluate
    alerts = []
    if temperature is not None and temp_max is not None and temperature > temp_max:
        alerts.append(("critical", f"temperature {temperature} > {temp_max}"))
    if temperature is not None and temp_min is not None and temperature < temp_min:
        alerts.append(("warning", f"temperature {temperature} < {temp_min}"))
    if sound_db is not None and sound_th is not None and sound_db > sound_th:
        alerts.append(("warning", f"sound {sound_db} dB > {sound_th} dB"))

    for level, msg in alerts:
        save_alert(machine_id, level, msg, meta={"row": row_meta})
    return alerts

# ================= CONFIG =================

TEMPERATURE_MULTIPLIER = 1.2
VIBRATION_THRESHOLD = 0.8
CO2_EMISSION_FACTOR = 0.475
DATA_DIRECTORY = "./data/"


# ================= SCHEMA =================

class SensorData(pw.Schema):
    timestamp: float
    machine_id: str
    temperature: float
    vibration: float
    energy_consumption: float


# ================= PROCESS =================

def process_sensor_stream() -> Tuple[pw.Table, pw.Table, pw.Table]:

    sensor_data = pw.io.csv.read(
        DATA_DIRECTORY,
        schema=SensorData,
        mode="streaming",
    )

    # ALERTS
    alerts = (
        sensor_data
        .groupby(pw.this.machine_id)
        .reduce(
            machine_id=pw.this.machine_id,
            avg_temperature=pw.reducers.avg(pw.this.temperature),
            max_temperature=pw.reducers.max(pw.this.temperature),
            avg_vibration=pw.reducers.avg(pw.this.vibration),
            max_vibration=pw.reducers.max(pw.this.vibration),
        )
        .filter(
            (pw.this.max_temperature > pw.this.avg_temperature * TEMPERATURE_MULTIPLIER)
            | (pw.this.max_vibration > VIBRATION_THRESHOLD)
        )
    )

    # EMISSIONS
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

    # STATUS
    status = sensor_data.groupby().reduce(
        total_records_processed=pw.reducers.count(),
        active_machines=pw.reducers.count_distinct(pw.this.machine_id),
    )

    return alerts, emissions, status


# ================= MAIN =================

def main():
    print("=" * 80)
    print("EcoSync Sentinel: Real-time AI for Green Manufacturing")
    print("=" * 80)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Initializing...")

    alerts, emissions, status = process_sensor_stream()

    # # Print outputs to console
    # pw.io.print(alerts)
    # pw.io.print(emissions)
    # pw.io.print(status)

    print("[INFO] Running streaming engine...")
    pw.run()

if __name__ == "__main__":
    main()

