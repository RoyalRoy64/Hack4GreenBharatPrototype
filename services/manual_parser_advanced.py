"""
services/manual_parser_advanced.py

Advanced PDF/manual parser for industrial machine manuals.

Features:
- Table-first strategy with semantic table classification (parameter/value, maintenance schedule, parts list).
- Text scanning fallback for missing info.
- Unit normalization: Fahrenheit -> Celsius, dBA -> dB.
- Maintenance schedule extraction (full task list).
- Spare parts extraction (name, part number, description).
- Optional LLM verification using OpenAI (if OPENAI_API_KEY set).
- Source tracking: page, excerpt text, and purpose for every extracted item.

Usage:
>>> from services.manual_parser_advanced import parse_manual
>>> out = parse_manual("CNC_manual.pdf", use_llm=True)
>>> print(json.dumps(out, indent=2))
"""

import re
import math
import json
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict
import os
import logging

import pandas as pd

# PyMuPDF import (module name is fitz)
try:
    import fitz
except Exception as e:
    raise ImportError("PyMuPDF (fitz) is required. Install with: pip install pymupdf") from e

# Optional OpenAI integration
OPENAI_AVAILABLE = False
try:
    import openai
    OPENAI_AVAILABLE = True
except Exception:
    OPENAI_AVAILABLE = False

# configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("manual_parser_advanced")

# -----------------------
# Regex patterns
# -----------------------
# Temperature patterns: single value or range (5-60°C, 5 to 60 °C, 60 °F)
TEMP_SINGLE = re.compile(r'(-?\d+(?:\.\d+)?)\s*(?:°\s*)?(C|F|c|f|deg\s*C|deg\s*F|Celsius|Fahrenheit)\b', re.I)
TEMP_RANGE = re.compile(
    r'(-?\d+(?:\.\d+)?)\s*(?:to|-|–)\s*(-?\d+(?:\.\d+)?)\s*(?:°\s*)?(C|F|c|f|deg\s*C|deg\s*F|Celsius|Fahrenheit)\b',
    re.I
)

# Noise
NOISE_PATTERN = re.compile(r'(\d+(?:\.\d+)?)\s*(dB|dBA|dB\(A\)|dB A)\b', re.I)

# Maintenance: expressions like "every 500 hours", "at 1000 h", "inspect every month"
MAINT_INTERVAL = re.compile(r'(every|at|after)\s+(\d+(?:\.\d+)?)\s*(hours|hrs|h|days|day|months|month|years|year|cycles)\b', re.I)
MAINT_VERB = re.compile(r'\b(inspect|replace|lubricate|check|clean|service|tighten|adjust|calibrate|overhaul)\b', re.I)

# Part names/keywords (expandable)
PART_KEYWORDS = re.compile(r'\b(bearing|filter|belt|motor|fan|seal|valve|sensor|pump|shaft|coupling|bearing assembly|gearbox)\b', re.I)

# Part number heuristic (alphanumeric codes, may include -, /, etc.)
PART_NUMBER = re.compile(r'\b([A-Z0-9\-_/]{3,})\b')

# Table header keywords
PARAM_HEADER_KEYWORDS = {"parameter", "specification", "value", "operating", "temperature", "noise", "sound", "range", "rating"}
MAINT_HEADER_KEYWORDS = {"maintenance", "interval", "task", "action", "schedule", "period"}
PARTS_HEADER_KEYWORDS = {"part", "part no", "part number", "item", "description", "qty", "quantity", "spare"}

# -----------------------
# Unit normalization
# -----------------------
def f_to_c(f: float) -> float:
    return (float(f) - 32.0) * 5.0 / 9.0

def normalize_temperature_value(value: float, unit_hint: str) -> float:
    unit_hint = (unit_hint or "").lower()
    try:
        v = float(value)
    except Exception:
        return None
    if "f" in unit_hint:
        return round(f_to_c(v), 2)
    # treat as Celsius by default
    return round(v, 2)

def normalize_noise_value(value: float, unit_hint: Optional[str]) -> float:
    # Accept dBA/dB as equivalent for now; return numeric dB
    try:
        return round(float(value), 2)
    except Exception:
        return None

# -----------------------
# Helpers: text excerpt
# -----------------------
def excerpt_text(text: str, max_chars: int = 200) -> str:
    t = text.strip().replace("\n", " ")
    return (t[:max_chars] + "...") if len(t) > max_chars else t

# -----------------------
# PDF extraction helpers
# -----------------------
def extract_pages(doc_path: str) -> List[fitz.Page]:
    doc = fitz.open(doc_path)
    return [doc[i] for i in range(len(doc))]

def get_page_text_dict(page: fitz.Page) -> Dict[str, Any]:
    # get structured dict (blocks / lines / spans)
    return page.get_text("dict")

# -----------------------
# Table extraction & semantic classification
# -----------------------
def extract_tables_with_context(page: fitz.Page, max_distance: int = 60) -> List[Dict[str, Any]]:
    """
    Return list of dicts: { 'table_df': pd.DataFrame, 'bbox': bbox, 'title_candidates': [texts], 'page': num }
    Uses page.find_tables() (PyMuPDF) and collects nearby text blocks to use as title/context.
    """
    raw_tables = page.find_tables()  # returns Table instances
    results = []

    if not raw_tables:
        return results

    text_dict = page.get_text("dict")
    blocks = text_dict.get("blocks", [])

    def collect_nearby_text(bbox, direction):
        ref_y = bbox[1] if direction == "above" else bbox[3]
        nearby = []
        for block in blocks:
            if "lines" not in block:
                continue
            block_y_top = block["bbox"][1]
            block_y_bottom = block["bbox"][3]
            block_y = block_y_bottom if direction == "above" else block_y_top
            dist = (ref_y - block_y) if direction == "above" else (block_y - ref_y)
            if dist >= 0 and dist <= max_distance:
                # collect spans
                text = []
                for line in block["lines"]:
                    for span in line["spans"]:
                        s = span.get("text", "").strip()
                        if s:
                            text.append(s)
                if text:
                    nearby.append(" ".join(text))
        return nearby

    for table in raw_tables:
        try:
            data = table.extract()
            df = pd.DataFrame(data)
        except Exception:
            # fallback: attempt to extract using page.get_text("blocks") as poor-man's table
            continue
        bbox = table.bbox
        above = collect_nearby_text(bbox, "above")
        below = collect_nearby_text(bbox, "below")
        results.append({"table_df": df, "bbox": bbox, "title_candidates": above + below})
    return results

def classify_table_semantics(df: pd.DataFrame, title_candidates: List[str]) -> str:
    """
    Classify as one of: 'parameters', 'maintenance', 'parts', 'unknown'
    Based on header row keywords and title candidates.
    """
    # Inspect header-like row candidates: assume first row might be header if many non-numeric
    header_row = None
    # identify row with many text items
    for r in range(min(2, len(df))):
        row = [str(x).strip().lower() for x in list(df.iloc[r])]
        non_null = sum([1 for c in row if c and c not in ("nan", "none")])
        if non_null >= 1:
            header_row = row
            break

    header_text = " ".join(title_candidates).lower() if title_candidates else ""
    header_words = set()
    if header_row:
        for cell in header_row:
            cols = re.split(r'[\s/,_\-]+', cell or "")
            header_words.update([c for c in cols if c])

    # heuristics
    hw = header_words
    ht = set([w for w in header_text.split() if w])

    # If header_text contains maintenance-like words -> maintenance
    if any(k in header_text for k in MAINT_HEADER_KEYWORDS):
        return "maintenance"
    # If header has 'part' or 'part no' etc. -> parts
    if any(any(pk in cell for pk in PARTS_HEADER_KEYWORDS) for cell in hw) or any(k in header_text for k in PARTS_HEADER_KEYWORDS):
        return "parts"
    # If headers contain parameter/spec/value etc -> parameters
    if any(any(ph in cell for ph in PARAM_HEADER_KEYWORDS) for cell in hw) or any(k in header_text for k in PARAM_HEADER_KEYWORDS):
        return "parameters"
    # fallback: try to detect numeric ranges - parameters likely
    # if table contains cells with degree symbols / dB -> parameters
    flattened = " ".join(df.fillna("").astype(str).values.flatten()).lower()
    if "°c" in flattened or "°f" in flattened or "db" in flattened or "dba" in flattened:
        return "parameters"
    # otherwise unknown
    return "unknown"

def parse_parameters_table(df: pd.DataFrame, page_num: int) -> List[Dict[str, Any]]:
    """
    Attempt to extract parameter-value pairs from df.
    Returns list of findings with source info.
    """
    findings = []
    # Try to find likely columns: parameter name and value
    cols = list(df.columns)
    # normalize column text to lowercases of header row if present
    # create candidate pairs by scanning all cells; heuristics: if a cell contains 'temperature' or 'noise' or 'db' -> pair with neighbor
    for r in range(len(df)):
        for c in range(len(df.columns)):
            cell = str(df.iat[r, c]).strip()
            if not cell or cell.lower() in ("nan", "none"):
                continue
            lc = cell.lower()
            # temperature cell
            if re.search(r'temp(erature)?|°c|°f|celsius|fahrenheit', lc):
                # look for value in same row other columns
                val = None
                for cc in range(len(df.columns)):
                    if cc == c:
                        continue
                    candidate = str(df.iat[r, cc]).strip()
                    if candidate and candidate.lower() not in ("nan", "none"):
                        val = candidate
                        break
                findings.append({
                    "type": "temperature",
                    "raw": cell + (" | " + val if val else ""),
                    "value_raw": val,
                    "page": page_num,
                    "source_text": excerpt_text(" | ".join(filter(None, [cell, val])))
                })
            # noise / dB
            if re.search(r'noise|sound|dba|db', lc):
                val = None
                for cc in range(len(df.columns)):
                    if cc == c:
                        continue
                    candidate = str(df.iat[r, cc]).strip()
                    if candidate and candidate.lower() not in ("nan", "none"):
                        val = candidate
                        break
                findings.append({
                    "type": "noise",
                    "raw": cell + (" | " + val if val else ""),
                    "value_raw": val,
                    "page": page_num,
                    "source_text": excerpt_text(" | ".join(filter(None, [cell, val])))
                })
            # generic numeric ranges in value-like cells
            if re.search(r'\d+\s*(?:to|-|–)\s*\d+\s*(?:°\s*C|°\s*F|\bC\b|\bF\b)', cell, re.I):
                # treat as temperature range
                findings.append({
                    "type": "temperature",
                    "raw": cell,
                    "value_raw": cell,
                    "page": page_num,
                    "source_text": excerpt_text(cell)
                })
            # generic dB in any cell
            if re.search(r'\d+\s*(dB|dBA)', cell, re.I):
                findings.append({
                    "type": "noise",
                    "raw": cell,
                    "value_raw": cell,
                    "page": page_num,
                    "source_text": excerpt_text(cell)
                })
    return findings

def parse_parts_table(df: pd.DataFrame, page_num: int) -> List[Dict[str, Any]]:
    """
    Attempt to extract parts list with columns: name, part number, description, qty.
    """
    findings = []
    # Normalize headers if present
    headers = [str(c).strip().lower() for c in list(df.columns)]
    candidate_cols = {"name": None, "part_no": None, "desc": None, "qty": None}
    # find header indices heuristically
    for i, h in enumerate(headers):
        if any(k in h for k in ["part", "item", "component", "spare"]):
            candidate_cols["name"] = i
        if any(k in h for k in ["part no", "part_no", "part number", "pn", "partno"]):
            candidate_cols["part_no"] = i
        if any(k in h for k in ["description", "desc", "details"]):
            candidate_cols["desc"] = i
        if any(k in h for k in ["qty", "quantity", "count"]):
            candidate_cols["qty"] = i

    # fallback heuristics: try to guess by content
    if candidate_cols["part_no"] is None:
        # scan columns for patterns that match part numbers
        for i in range(len(df.columns)):
            col_text = " ".join(df[i].astype(str).fillna("").values)
            if PART_NUMBER.search(col_text):
                candidate_cols["part_no"] = i
                break

    # now iterate rows
    for r in range(len(df)):
        name = None
        part_no = None
        desc = None
        qty = None
        if candidate_cols["name"] is not None:
            name = str(df.iat[r, candidate_cols["name"]]).strip()
        # try fallback: first column as name
        if not name:
            name = str(df.iat[r, 0]).strip()
        if candidate_cols["part_no"] is not None:
            part_no = str(df.iat[r, candidate_cols["part_no"]]).strip()
        else:
            # search within row for part number
            row_flat = " ".join([str(df.iat[r, c]) for c in range(len(df.columns))])
            pn = PART_NUMBER.search(row_flat)
            if pn:
                part_no = pn.group(1)
        if candidate_cols["desc"] is not None:
            desc = str(df.iat[r, candidate_cols["desc"]]).strip()
        else:
            # use concatenation of other cells as description
            desc_cells = []
            for c in range(len(df.columns)):
                if c not in (candidate_cols.get("name"), candidate_cols.get("part_no")):
                    desc_cells.append(str(df.iat[r, c]).strip())
            desc = " | ".join([d for d in desc_cells if d and d.lower() not in ("nan", "none")])
        if candidate_cols["qty"] is not None:
            qty = str(df.iat[r, candidate_cols["qty"]]).strip()

        # if row appears empty, skip
        if not any([name, part_no, desc]):
            continue

        findings.append({
            "type": "part",
            "name": name or None,
            "part_number": part_no or None,
            "description": excerpt_text(desc or ""),
            "page": page_num,
            "source_text": excerpt_text(" | ".join(filter(None, [name, part_no, desc])))
        })
    return findings

def parse_maintenance_table(df: pd.DataFrame, page_num: int) -> List[Dict[str, Any]]:
    """
    Extract rows mapping tasks to intervals or schedule.
    Returns list of dicts with 'task', 'interval', 'notes', 'page', 'source_text'.
    """
    findings = []
    headers = [str(c).strip().lower() for c in list(df.columns)]
    candidate_cols = {"task": None, "interval": None, "notes": None}
    for i, h in enumerate(headers):
        if any(k in h for k in ["task", "action", "activity", "work"]):
            candidate_cols["task"] = i
        if any(k in h for k in ["interval", "period", "frequency", "every", "schedule"]):
            candidate_cols["interval"] = i
        if any(k in h for k in ["notes", "details", "description"]):
            candidate_cols["notes"] = i

    # fallback: if first column looks like an interval (e.g., "100h", "every 500 hours") treat appropriately
    for r in range(len(df)):
        task = None
        interval = None
        notes = None
        if candidate_cols["task"] is not None:
            task = str(df.iat[r, candidate_cols["task"]]).strip()
        else:
            # if first column doesn't look like interval, assume it's a task
            col0 = str(df.iat[r, 0]).strip()
            if not MAINT_INTERVAL.search(col0):
                task = col0
        if candidate_cols["interval"] is not None:
            interval = str(df.iat[r, candidate_cols["interval"]]).strip()
        else:
            # search entire row for interval pattern
            row_flat = " ".join([str(df.iat[r, c]) for c in range(len(df.columns))])
            m = MAINT_INTERVAL.search(row_flat)
            if m:
                interval = m.group(0)
        if candidate_cols["notes"] is not None:
            notes = str(df.iat[r, candidate_cols["notes"]]).strip()
        else:
            # aggregate non-task, non-interval cells as notes
            notes_cells = []
            for c in range(len(df.columns)):
                if c not in (candidate_cols.get("task"), candidate_cols.get("interval")):
                    notes_cells.append(str(df.iat[r, c]).strip())
            notes = " | ".join([nc for nc in notes_cells if nc and nc.lower() not in ("nan", "none")])

        if not any([task, interval, notes]):
            continue

        findings.append({
            "type": "maintenance",
            "task": task or notes or None,
            "interval": interval or None,
            "notes": excerpt_text(notes or ""),
            "page": page_num,
            "source_text": excerpt_text(" | ".join(filter(None, [task, interval, notes])))
        })
    return findings

# -----------------------
# Text scanning (fallback & complementary)
# -----------------------
def scan_text_for_candidates(text: str, page_num: int) -> List[Dict[str, Any]]:
    """
    Scan free text for temperature, noise, maintenance tasks, parts.
    Returns a list of finding dicts similar to those from table parsing.
    """
    findings = []

    # Temperature ranges first
    for m in TEMP_RANGE.finditer(text):
        v1, v2, unit = m.group(1), m.group(2), m.group(3)
        try:
            v1n = normalize_temperature_value(v1, unit)
            v2n = normalize_temperature_value(v2, unit)
        except Exception:
            v1n = v2n = None
        findings.append({
            "type": "temperature",
            "value_min": v1n,
            "value_max": v2n,
            "raw": m.group(0),
            "page": page_num,
            "source_text": excerpt_text(m.group(0)),
        })

    # Single temperature values
    for m in TEMP_SINGLE.finditer(text):
        val, unit = m.group(1), m.group(2)
        valn = normalize_temperature_value(val, unit)
        findings.append({
            "type": "temperature",
            "value": valn,
            "raw": m.group(0),
            "page": page_num,
            "source_text": excerpt_text(m.group(0))
        })

    # Noise
    for m in NOISE_PATTERN.finditer(text):
        val, unit = m.group(1), m.group(2)
        valn = normalize_noise_value(val, unit)
        findings.append({
            "type": "noise",
            "value": valn,
            "unit": "dB",
            "raw": m.group(0),
            "page": page_num,
            "source_text": excerpt_text(m.group(0))
        })

    # Maintenance (verbs + interval)
    # find sentences containing maintenance verbs or explicit intervals
    sentences = re.split(r'[.\n;]\s*', text)
    for s in sentences:
        if len(s.strip()) < 5:
            continue
        if MAINT_VERB.search(s) or MAINT_INTERVAL.search(s) or "maintenance" in s.lower():
            # extract interval if possible
            m = MAINT_INTERVAL.search(s)
            interval = m.group(0) if m else None
            findings.append({
                "type": "maintenance",
                "task": excerpt_text(s),
                "interval": interval,
                "page": page_num,
                "source_text": excerpt_text(s)
            })

    # Parts: search for typical keywords and adjacent part numbers
    # scan lines for PART_KEYWORDS or "Part No."
    for s in sentences:
        if PART_KEYWORDS.search(s) or "part no" in s.lower() or "part number" in s.lower() or "spare" in s.lower():
            pn = PART_NUMBER.search(s)
            pn_val = pn.group(1) if pn else None
            # try to extract a probable name from phrase
            name_match = PART_KEYWORDS.search(s)
            name = name_match.group(0) if name_match else None
            findings.append({
                "type": "part",
                "name": name or None,
                "part_number": pn_val,
                "description": excerpt_text(s),
                "page": page_num,
                "source_text": excerpt_text(s)
            })

    return findings

# -----------------------
# Aggregation & Deduplication
# -----------------------
def aggregate_findings(findings: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Turn raw findings into final structured output (without LLM verification).
    Returns dict with keys: operating_temperature, sound_level_db, maintenance, spare_parts, source_locations
    """
    temps = []
    noises = []
    maintenance_list = []
    parts_list = []
    sources = []

    for f in findings:
        sources.append({
            "page": f.get("page"),
            "text": excerpt_text(f.get("source_text") or f.get("raw") or ""),
            "purpose": f.get("type") if f.get("type") else "unknown"
        })

        if f.get("type") == "temperature":
            # handle range vs single
            if "value_min" in f and f.get("value_min") is not None:
                temps.append(float(f.get("value_min")))
            if "value_max" in f and f.get("value_max") is not None:
                temps.append(float(f.get("value_max")))
            if "value" in f and f.get("value") is not None:
                temps.append(float(f.get("value")))
            # sometimes value_raw contains text like "5-60 C"
            if f.get("value_raw"):
                vr = f.get("value_raw")
                # try range parse
                m = re.search(r'(-?\d+(?:\.\d+)?)\s*(?:to|-|–)\s*(-?\d+(?:\.\d+)?)\s*(°\s*[CF]|C|F|c|f)?', vr, re.I)
                if m:
                    try:
                        a = float(m.group(1)); b = float(m.group(2)); unit = m.group(3) or 'C'
                        # normalize
                        a_n = normalize_temperature_value(a, unit)
                        b_n = normalize_temperature_value(b, unit)
                        if a_n is not None: temps.append(a_n)
                        if b_n is not None: temps.append(b_n)
                    except Exception:
                        pass

        elif f.get("type") == "noise":
            if f.get("value") is not None:
                noises.append(float(f.get("value")))
            elif f.get("raw"):
                m = re.search(r'(\d+(?:\.\d+)?)', f.get("raw"))
                if m:
                    noises.append(float(m.group(1)))

        elif f.get("type") == "maintenance":
            maintenance_list.append({
                "task": f.get("task") or f.get("value") or "",
                "interval": f.get("interval") or None,
                "notes": f.get("notes") or "",
                "source": {"page": f.get("page"), "text": excerpt_text(f.get("source_text") or "")}
            })

        elif f.get("type") == "part":
            parts_list.append({
                "name": f.get("name") or None,
                "part_number": f.get("part_number") or None,
                "description": f.get("description") or excerpt_text(f.get("source_text") or ""),
                "source": {"page": f.get("page"), "text": excerpt_text(f.get("source_text") or "")}
            })

    # compute final temperature min/max
    temp_min = min(temps) if temps else None
    temp_max = max(temps) if temps else None

    # compute average noise (rounded)
    noise_avg = round(sum(noises) / len(noises), 2) if noises else None

    # deduplicate maintenance and parts (simple dedupe)
    def dedupe_list_by_key(items, key):
        seen = set()
        out = []
        for it in items:
            val = it.get(key) or json.dumps(it, sort_keys=True)
            if val not in seen:
                seen.add(val)
                out.append(it)
        return out

    maintenance_list = dedupe_list_by_key(maintenance_list, "task")
    parts_list = dedupe_list_by_key(parts_list, "name")

    structured = {
        "machine_name": None,
        "operating_temperature": {
            "min": temp_min,
            "max": temp_max,
            "unit": "C" if (temp_min is not None or temp_max is not None) else None
        },
        "sound_level_db": noise_avg,
        "maintenance": maintenance_list,
        "spare_parts": parts_list,
        "source_locations": sources
    }

    return structured

# -----------------------
# Optional LLM verification/cleaning
# -----------------------
def llm_verify_and_normalize(extracted: Dict[str, Any], model: str = "gpt-4o-mini") -> Dict[str, Any]:
    """
    If OpenAI key present and library installed, call LLM to tidy ambiguous outputs.
    Prepare a prompt summarizing extracted items and ask model to return cleaned JSON in the given schema.
    If OpenAI is not available, return extracted unchanged.
    """
    if not OPENAI_AVAILABLE or not os.getenv("OPENAI_API_KEY"):
        logger.info("OpenAI not available or OPENAI_API_KEY not set; skipping LLM verification.")
        return extracted

    # build prompt
    prompt = []
    prompt.append("You are a helpful extractor that converts raw parsed findings from a machine manual into a clean structured JSON with fields:")
    prompt.append("machine_name, operating_temperature (min, max, unit=C), sound_level_db (numeric), maintenance (list of {task, interval, notes, source}), spare_parts (list of {name, part_number, description, source}), source_locations (list of {page, text, purpose}).")
    prompt.append("Here are the extracted raw fields (do not invent new specific numbers unless inferred):")
    prompt.append(json.dumps(extracted, indent=2))
    prompt.append("Return ONLY the JSON object in the exact schema. Normalize temperatures to Celsius (if Fahrenheit present convert). Normalize noise to dB numeric. For intervals, keep text like 'every 500 hours' if numeric parsing is ambiguous. For source fields include page numbers and short text excerpts. If a field is empty, use null or empty list appropriately.")

    system_prompt = "You are a JSON generator for technical documents."
    user_prompt = "\n\n".join(prompt)

    openai.api_key = os.getenv("OPENAI_API_KEY")
    try:
        # Using chat completion
        resp = openai.ChatCompletion.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.0,
            max_tokens=800
        )
        text = resp["choices"][0]["message"]["content"].strip()
        # Attempt to parse JSON from response (tolerant)
        first_brace = text.find("{")
        last_brace = text.rfind("}")
        if first_brace != -1 and last_brace != -1:
            json_text = text[first_brace:last_brace+1]
        else:
            json_text = text
        parsed = json.loads(json_text)
        return parsed
    except Exception as e:
        logger.exception("LLM verification failed, falling back to rule-based result: %s", e)
        return extracted

# -----------------------
# Main entry: parse_manual
# -----------------------
def parse_manual(pdf_path: str, use_llm: bool = False) -> Dict[str, Any]:
    """
    Main function to parse a machine manual PDF and produce structured JSON.
    Parameters:
      - pdf_path: path to PDF file
      - use_llm: if True, attempt LLM-based verification (OPENAI_API_KEY must be set)
    """
    logger.info("Opening document: %s", pdf_path)
    doc = fitz.open(pdf_path)
    raw_findings = []

    for page_index in range(len(doc)):
        page = doc[page_index]
        page_num = page_index + 1

        # 1) Table-first extraction
        tables = extract_tables_with_context(page)
        for tbl in tables:
            df = tbl["table_df"]
            title_candidates = tbl.get("title_candidates", [])
            sem = classify_table_semantics(df, title_candidates)
            logger.debug("Page %d detected table semantic: %s", page_num, sem)
            if sem == "parameters":
                raw_findings.extend(parse_parameters_table(df, page_num))
            elif sem == "parts":
                raw_findings.extend(parse_parts_table(df, page_num))
            elif sem == "maintenance":
                raw_findings.extend(parse_maintenance_table(df, page_num))
            else:
                # Unknown: still scan table cells for candidates
                raw_findings.extend(parse_parameters_table(df, page_num))
                raw_findings.extend(parse_parts_table(df, page_num))
                raw_findings.extend(parse_maintenance_table(df, page_num))

        # 2) Then page text scanning
        page_text = page.get_text() or ""
        raw_findings.extend(scan_text_for_candidates(page_text, page_num))

    # 3) Aggregate and normalize
    aggregated = aggregate_findings(raw_findings)

    # 4) LLM verification (optional)
    if use_llm:
        verified = llm_verify_and_normalize(aggregated)
        return verified

    return aggregated

# -----------------------
# CLI usage example
# -----------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Parse a machine manual PDF into structured JSON.")
    parser.add_argument("pdf", help="path to PDF file")
    parser.add_argument("--llm", action="store_true", help="Use LLM (OpenAI) to verify/clean output if API key is set")
    args = parser.parse_args()
    result = parse_manual(args.pdf, use_llm=args.llm)
    print(json.dumps(result, indent=2))