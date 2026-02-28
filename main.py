from typing import Tuple
from datetime import datetime
import pathway as pw


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
    pw.io.print(alerts)
    pw.io.print(emissions)
    pw.io.print(status)

    print("[INFO] Running streaming engine...")
    pw.run()

if __name__ == "__main__":
    main()

