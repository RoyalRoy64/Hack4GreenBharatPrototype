import os
from google import genai
from google.genai import types
from pydantic import BaseModel
from typing import List, Optional

PROJECT_ID = "machine-data-extractor" 
MODEL = "gemini-2.5-pro"

# 1. Exact Schema Matching Your Output Requirement
class OperatingTemp(BaseModel):
    min: Optional[float]
    max: Optional[float]
    unit: str = "C"

class MaintenanceTask(BaseModel):
    task: str
    interval: str

class SparePart(BaseModel):
    name: str
    part_number: str

class MachineSpecs(BaseModel):
    operating_temperature: OperatingTemp
    sound_level_db: Optional[float]
    maintenance: List[MaintenanceTask]
    spare_parts: List[SparePart]

# 2. Client Setup (Assumes gcloud auth application-default login is active)
PROJECT_ID = os.getenv("GCLOUD_PROJECT", PROJECT_ID)
client = genai.Client(vertexai=True, project=PROJECT_ID, location="us-central1")

def extract_manual_data(pdf_file_path: str) -> dict:
    """
    Ingests the raw PDF at HIGH resolution for maximum table extraction accuracy.
    """
    # Read the PDF as raw bytes
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

    1. OPERATING LIMITS (Temperature & Sound):
    - Extract the safe operating temperature range (min and max) and maximum safe sound level (dB).
    - VALIDATION: Evaluate if the extracted values are logically sound for this specific type of machine. If the manual's values are missing, or if they seem completely unreasonable/illogical (e.g., obvious typos like 5000°C for a standard motor), provide standard, logical baseline values for this machine type instead.

    2. MAINTENANCE SCHEDULE & PROCEDURES:
    - Extract the manufacturer's recommended maintenance schedule and step-by-step procedures.
    - FALLBACK: If the manual does not provide a schedule or procedure, or if the extracted one is severely lacking, you MUST generate a logical, industry-standard maintenance schedule and procedure based on the specific type of machine.

    3. SPARE PARTS INVENTORY (Scheduled & Emergency):
    - Provide a comprehensive list of spare parts needed for BOTH scheduled routine maintenance and preparedness for accidental malfunctions. Apply these rules strictly:
    * RULE A (Explicitly in PDF): If parts and numbers are mentioned, extract them exactly.
    * RULE B (Probable but no part number): If specific parts are highly probable for this machine but lack part numbers in the PDF, list the probable part name and strictly write "to be specified manually" for the part_number.
    * RULE C (General LLM Suggestion): If the manual lacks spare parts guidance and you must suggest a general inventory list to be prepared for malfunctions, you MUST include this exact phrase in the part's description: "suggested by llm, needs review and specificaiton manually".

    Return the data strictly in the requested JSON format matching this exact structure:

    EXAMPLE JSON OUTPUT:
    {
    "operating_temperature": {
        "min": 15.0,
        "max": 80.0,
        "unit": "C"
    },
    "sound_level_db": 85.0,
    "maintenance": [
        {
        "task": "Lubricate main spindle bearings using ISO VG 68 oil.",
        "interval": "Every 500 hours"
        },
        {
        "task": "Inspect drive belts for tension and wear (Generated standard procedure).",
        "interval": "Monthly"
        }
    ],
    "spare_parts": [
        {
        "name": "Main Spindle Bearing",
        "part_number": "SKF-6205",
        "description": "Explicitly listed in manual."
        },
        {
        "name": "Coolant Pump Seal",
        "part_number": "to be specified manually",
        "description": "Standard wear part, missing exact number in text."
        },
        {
        "name": "Emergency Stop Relay",
        "part_number": "to be specified manually",
        "description": "suggested by llm, needs review and specificaiton manually"
        }
    ]
    }
    """

    response = client.models.generate_content(
        model= MODEL, # The best model for complex reasoning
        contents=[pdf_part, prompt],
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=MachineSpecs,
            temperature=0.1, # Keep it strictly factual
            # Setting the highest resolution for complex engineering diagrams/tables
            pdf_resolution="HIGH" 
        )
    )
    
    # Returns a validated Python dictionary matching your frontend needs
    return response.parsed.model_dump()


import json

# 1. Define the input PDF and the output JSON file name
pdf_filename = "Maintenance-Guide-for-KVT_KDT_DVT.pdf"
output_filename = "parsed_becker_pump_data.json"

print(f"Starting extraction for {pdf_filename}. This may take 10-20 seconds...")

try:
    # 2. Run the Gemini extraction
    extracted_data = extract_manual_data(pdf_filename)
    
    # 3. Save the result to a file with nice formatting (indent=4)
    with open(output_filename, "w", encoding="utf-8") as json_file:
        json.dump(extracted_data, json_file, indent=4)
        
    print(f"✅ Success! Data perfectly extracted and saved to {output_filename}")

except Exception as e:
    print(f"❌ An error occurred during extraction: {e}")