# api/manuals.py
from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
import os
import uuid
import json
from typing import Optional

from services.manual_parser_advanced import parse_manual
from services.gemini_llm import extract_machine_data
from database.db import init_db, save_manual, get_manual, mark_manual_approved

router = APIRouter(prefix="/api/manuals")

# ensure DB created
init_db()

MANUALS_DIR = "data/manuals"
os.makedirs(MANUALS_DIR, exist_ok=True)

@router.post("/upload")
async def upload_manual(file: UploadFile = File(...), machine_id: str = Form(...), use_llm: Optional[bool] = Form(False)):
    """
    Upload a machine manual PDF.
    Params:
      - machine_id: string identifier you use for that machine (e.g., "CNC_12")
      - file: PDF file
      - use_llm: optional boolean (if true and GEMINI key present, we'll run Gemini on the snippets)
    Returns: parsed structured JSON
    """
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files accepted")

    uid = str(uuid.uuid4())[:8]
    out_path = os.path.join(MANUALS_DIR, f"{machine_id}_{uid}.pdf")
    contents = await file.read()
    with open(out_path, "wb") as f:
        f.write(contents)

    # 1) parse PDF to get candidate snippets and table findings
    parsed_candidates = parse_manual(out_path, use_llm=False)  # returns aggregated rule-based result (candidates)
    # parsed_candidates includes source_locations and raw finds

    # 2) prepare concise snippets to send to LLM (if chosen)
    snippets = []
    for s in parsed_candidates.get("source_locations", []):
        snippets.append(f"page {s.get('page')}: {s.get('text')} (purpose: {s.get('purpose')})")
    snippet_blob = "\n\n".join(snippets)[:6000]  # keep size sane

    structured = parsed_candidates
    if use_llm:
        try:
            llm_result = extract_machine_data(snippet_blob)
            # If the LLM returns a JSON (preferred), use it to fill in missing fields
            if isinstance(llm_result, dict) and any(k in llm_result for k in ("operating_temperature", "maintenance", "spare_parts")):
                # merge heuristically, prefer LLM fields if present
                for k, v in llm_result.items():
                    if v:
                        structured[k] = v
        except Exception as e:
            # fallback: keep rule-based structured
            structured["llm_error"] = str(e)

    # persist to DB (store parsed JSON and provenance)
    manual_id = save_manual(machine_id=machine_id, pdf_path=out_path, parsed_json=structured)

    return JSONResponse({"manual_id": manual_id, "machine_id": machine_id, "parsed": structured})

@router.get("/{machine_id}")
def get_manual_for_machine(machine_id: str):
    doc = get_manual(machine_id)
    if not doc:
        raise HTTPException(status_code=404, detail="manual not found")
    return JSONResponse(doc)

@router.post("/{machine_id}/approve")
def approve_manual(machine_id: str, approved: bool = True):
    ok = mark_manual_approved(machine_id, approved)
    if not ok:
        raise HTTPException(status_code=404, detail="manual not found")
    return JSONResponse({"machine_id": machine_id, "approved": approved})