import os
import json
from google import genai
from google.genai import types
from pydantic import BaseModel
from typing import List, Optional

# --- Configuration ---
PROJECTz = "machine-data-extractor" 
MODEL = "gemini-2.5-pro"

# --- Schema Definitions ---
class MachineDetail(BaseModel):
    machine_name: str
    machine_type: str
    machine_description: str

class OperatingTemp(BaseModel):
    min: Optional[float]
    max: Optional[float]
    unit: str = "C"

class MiscParameter(BaseModel):
    parameter_name: str
    min_value: Optional[float]
    max_value: Optional[float]
    unit: Optional[str]

class MaintenanceTask(BaseModel):
    task: str
    interval: str
    period: int  # Added: Number of days

class SparePart(BaseModel):
    name: str
    part_number: str
    description: Optional[str]
    minimum_required_parts: int  # Added: Inventory count

class MachineSpecs(BaseModel):
    machine_detail: MachineDetail
    operating_temperature: OperatingTemp
    sound_level_db: Optional[float]
    miscellaneous_parameters: List[MiscParameter]  # Added: Catch-all for extra specs
    maintenance: List[MaintenanceTask]
    spare_parts: List[SparePart]

# --- Client Setup ---
PROJECT_ID = os.getenv("GCLOUD_PROJECT", PROJECTz)
client = genai.Client(vertexai=True, project=PROJECT_ID, location="us-central1")

def extract_manual_data(pdf_file_path: str) -> dict:
    """Ingests the raw PDF utilizing Gemini's native document understanding."""
    
    with open(pdf_file_path, "rb") as f:
        pdf_bytes = f.read()

    pdf_part = types.Part.from_bytes(
        data=pdf_bytes, 
        mime_type="application/pdf"
    )

    prompt = """
    You are an expert industrial maintenance engineer and technical documentation analyzer. 
    Analyze the provided manufacturing machine manual and extract the structured data. 

    Follow these strict rules for extraction and generation:

    1. MACHINE DETAILS:
    - Extract or infer the specific machine name, its general industrial type (e.g., "CNC Lathe", "Rotary Compressor"), and write a brief operational description.

    2. OPERATING LIMITS & MISCELLANEOUS PARAMETERS:
    - Extract the safe operating temperature range (min and max) and maximum safe sound level (dB).
    - VALIDATION: Evaluate if the extracted values are logically sound for this specific type of machine. If illogical (e.g., 5000°C), provide standard baseline values.
    - MISCELLANEOUS: Extract any other relevant operational parameters mentioned (e.g., coolant temperature range, air duct flow, pneumatic pressure limits). If none exist, leave the array empty.

    3. MAINTENANCE SCHEDULE & PROCEDURES:
    - Extract the recommended maintenance schedule and step-by-step procedures.
    - FALLBACK: If missing or lacking, generate a logical, industry-standard schedule.
    - PERIOD CALCULATION: You must convert the textual interval into an estimated integer number of days (`period`). Do NOT assume a light 8-hour workday. Instead, apply standard industrial duty cycles (e.g., 24/7 continuous operation or heavy 16-hour shifts) based on the specific machine type. For example, if an industrial pump requires maintenance every 2000 hours, assume heavy/continuous operation and output a realistic timeframe like 83 days. For explicit calendar terms, use standard conversions (e.g., "Monthly" = 30, "Annually" = 365).

    4. SPARE PARTS INVENTORY:
    - Provide a list of spare parts for routine maintenance and emergency preparedness.
    - INVENTORY COUNT: Estimate a sensible `minimum_required_parts` integer to keep in stock based on the part's wear rate and criticality.
    - RULE A (Explicit): If parts and numbers are mentioned, extract them exactly.
    - RULE B (Probable): If probable but lack numbers, write "to be specified manually" for the part_number.
    - RULE C (LLM Suggestion): If you generate the part suggestion entirely, include this exact phrase in the description: "suggested by llm, needs review and specificaiton manually".

    Return the data strictly in the requested JSON format matching this exact structure:

    EXAMPLE JSON OUTPUT:
    {
    "machine_detail": {
        "machine_name": "Becker KVT 3.140",
        "machine_type": "Rotary Vane Vacuum Pump",
        "machine_description": "Oil-free rotary vane displacement pump used for industrial vacuum generation."
    },
    "operating_temperature": {
        "min": 5.0,
        "max": 45.0,
        "unit": "C"
    },
    "sound_level_db": 75.0,
    "miscellaneous_parameters": [
        {
        "parameter_name": "Cooling Air Flow",
        "min_value": null,
        "max_value": 150.0,
        "unit": "m3/h"
        }
    ],
    "maintenance": [
        {
        "task": "Clean or replace the air intake filter.",
        "interval": "Every 250 hours",
        "period": 31
        },
        {
        "task": "Inspect carbon vanes for wear and measure minimum width.",
        "interval": "Annually",
        "period": 365
        }
    ],
    "spare_parts": [
        {
        "name": "Carbon Vane Set",
        "part_number": "90133000004",
        "description": "Explicitly listed in manual.",
        "minimum_required_parts": 1
        },
        {
        "name": "Intake Filter Cartridge",
        "part_number": "to be specified manually",
        "description": "Standard wear part, missing exact number in text.",
        "minimum_required_parts": 3
        },
        {
        "name": "Motor Bearings",
        "part_number": "to be specified manually",
        "description": "suggested by llm, needs review and specificaiton manually",
        "minimum_required_parts": 2
        }
    ]
    }
    """

    response = client.models.generate_content(
        model=MODEL, # Variable used here
        contents=[pdf_part, prompt],
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=MachineSpecs,
            temperature=0.1
        )
    )
    
    return response.parsed.model_dump()

# --- Execution and Saving Logic ---
if __name__ == "__main__":
    pdf_filename = "Maintenance-Guide-for-KVT_KDT_DVT.pdf"
    output_filename = "parsed_becker_pump_data02.json"

    print(f"Starting extraction for {pdf_filename}. This may take 10-20 seconds...")

    try:
        extracted_data = extract_manual_data(pdf_filename)
        
        with open(output_filename, "w", encoding="utf-8") as json_file:
            json.dump(extracted_data, json_file, indent=4)
            
        print(f"✅ Success! Data perfectly extracted and saved to {output_filename}")

    except Exception as e:
        print(f"❌ An error occurred during extraction: {e}")