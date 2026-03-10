from google import genai
from pydantic import BaseModel
from typing import List, Optional

from google.genai import types  # Add this import

# 1. Define your professional schema
class MaintenanceTask(BaseModel):
    task: str
    interval: Optional[str]
    notes: Optional[str]

class MachineData(BaseModel):
    machine_name: Optional[str]
    operating_temp_max: Optional[float]
    maintenance_schedule: List[MaintenanceTask]



# 1. REPLACE THIS with your actual ID from the Cloud Console top-bar
# Example: "my-green-bharat-prototype-4455"
ACTUAL_PROJECT_ID = "machine-data-extractor" 

# MODEL = 'gemini-2.0-flash-lite-001'
# MODEL = 'gemini-3-flash-preview'
MODEL = 'gemini-2.5-pro'

client = genai.Client(
    vertexai=True, 
    project=ACTUAL_PROJECT_ID, 
    location="us-central1"
)

# 2. Using '3-flash' from your provided list
response = client.models.generate_content(
    model= MODEL,
    contents="Confirming access to v3 architecture.",
    config=types.GenerateContentConfig(
        # Note: 'thinking_level' is for Gemini 3.1+ 
        # For '3 Flash', use standard confisg
        temperature=0.1 
    )
    )  

print(f"Success! Response: {response.text}")
