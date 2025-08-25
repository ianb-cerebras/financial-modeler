import os
import json
import argparse
from typing import Dict, Any, List, Optional
import openpyxl
from cerebras.cloud.sdk import Cerebras
from docx import Document  # pip install python-docx
import time
import logging
import random

# ---------------- User configurable paths ----------------
SOURCE_FILE = "test.xlsx"  # primary data workbook to read and write

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Global Cerebras client & model configuration
# ---------------------------------------------------------------------------
CLIENT = Cerebras(api_key=os.environ.get("CEREBRAS_API_KEY"))
MODEL = "qwen-3-coder-480b"

# ---------------------------------------------------------------------------
# Spreadsheet helpers
# ---------------------------------------------------------------------------
def load_excel_data(filepath: str) -> Dict[str, List[List[Any]]]:
    """Load every worksheet in an Excel workbook into a matrix list-of-lists."""
    wb = openpyxl.load_workbook(filepath, data_only=True)
    data: Dict[str, List[List[Any]]] = {}
    for ws in wb.worksheets:
        sheet_matrix = [list(row) for row in ws.iter_rows(values_only=True)]
        data[ws.title] = sheet_matrix
        logger.debug("Loaded sheet '%s' with %d rows", ws.title, len(sheet_matrix))
    return data

# ---------------------------------------------------------------------------
# Cerebras wrapper with retry/backoff
# ---------------------------------------------------------------------------
def _chat_with_retries(messages: List[Dict[str, Any]], *, purpose: str, max_retries: int = 3, base_delay: float = 0.5) -> str:
    """Call Cerebras chat API with exponential-backoff retries."""
    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            logger.info("Cerebras call (%s) attempt %d/%d", purpose, attempt, max_retries)
            response = CLIENT.chat.completions.create(model=MODEL, messages=messages)
            return response.choices[0].message.content
        except Exception as e:
            last_error = e
            if attempt == max_retries:
                break
            sleep_s = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 0.25)
            logger.warning("Cerebras call failed (%s) on attempt %d: %s – retrying in %.2fs", purpose, attempt, e, sleep_s)
            time.sleep(sleep_s)
    logger.error("Cerebras call failed after %d attempts (%s)", max_retries, purpose)
    raise last_error if last_error else RuntimeError("Unknown Cerebras error")

# ---------------------------------------------------------------------------
# AI-assisted analytics steps
# ---------------------------------------------------------------------------
def identify_assumptions(workbook_data: Dict[str, Any], core_elements: Optional[str] = None, question: Optional[str] = None) -> str:
    """Cerebras call – extract numeric data & assumptions from raw workbook."""
    system_prompt = (
        "You are a financial analyst. Given the core elements and raw workbook data, "
        "extract specific financial information and all underlying assumptions required for modelling."
    )
    context = {
        "workbook": workbook_data,
        "core_elements": core_elements,
        "question": question or "Analyze the financial performance and provide key insights",
    }
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": json.dumps(context)},
    ]
    return _chat_with_retries(messages, purpose="identify_assumptions")


def make_formulas(workbook_data: Dict[str, Any], core_elements: Optional[str] = None, question: Optional[str] = None) -> str:
    """Cerebras call – return numeric literals per cell, no cell references."""
    system_prompt = (
        "You are a senior financial analyst. Using ONLY numeric literals, provide the values to populate the template. "
        "Return a JSON array where each object has keys: sheet, cell, value.  Example: "
        "[{\"sheet\": \"Income Statement\", \"cell\": \"C12\", \"value\": 22}]. "
        "Do NOT reference any other cells or sheets; compute everything beforehand."
    )
    context = {
        "workbook": workbook_data,
        "core_elements": core_elements,
        "question": question or "Analyze the financial performance and provide key insights",
    }
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": json.dumps(context)},
    ]
    return _chat_with_retries(messages, purpose="generate_formulas")


# ---------------------------------------------------------------------------
# Formula evaluation engine
# ---------------------------------------------------------------------------
def values_from_json(json_text: str) -> Dict[str, Dict[str, float]]:
    """Convert model JSON (sheet, cell, value) into nested dict."""
    import json, numbers
    data = json.loads(json_text)
    out: Dict[str, Dict[str, float]] = {}
    for obj in data:
        sheet = obj["sheet"]
        cell = obj["cell"]
        val = obj["value"]
        if isinstance(val, str):
            # Allow simple literal expressions like "1000*0.1"
            val = float(eval(val, {"__builtins__": {}}))
        elif isinstance(val, numbers.Number):
            val = float(val)
        else:
            continue
        out.setdefault(sheet, {})[cell] = val
    return out


def fill_template(values: Dict[str, Dict[str, float]], template_path: str, output_path: Optional[str] = None) -> str:
    """Insert evaluated numbers into an Excel template.

    Args:
        values: Nested dict from evaluate_formulas – sheet -> cell -> number.
        template_path: Path to the template workbook (e.g. incomestatementformat.xlsx).
        output_path: Where to save the filled workbook. If None, writes alongside template with suffix _filled.xlsx.

    Returns:
        The path to the written workbook.
    """
    from pathlib import Path

    wb = openpyxl.load_workbook(template_path)

    missing_refs = []
    for sheet_name, cell_map in values.items():
        if sheet_name not in wb.sheetnames:
            missing_refs.append((sheet_name, None))
            continue  # skip unknown sheet
        ws = wb[sheet_name]
        for cell, num in cell_map.items():
            try:
                ws[cell]  # trigger coordinate parsing
            except ValueError:
                missing_refs.append((sheet_name, cell))
                continue
            ws[cell] = num

    if missing_refs:
        logger.warning("Skipped %d references not present in template: %s", len(missing_refs), missing_refs[:20])

    if output_path is None:
        p = Path(template_path)
        output_path = str(p.with_name(p.stem + "_filled" + p.suffix))

    wb.save(output_path)
    logger.info("Template filled and saved to %s", output_path)
    return output_path


# ---------------------------------------------------------------------------
# Template sheet handling
# ---------------------------------------------------------------------------
def copy_template_sheet(template_path: str, dest_path: str, new_sheet_name: Optional[str] = None) -> str:
    """Copy the first sheet from template workbook into destination workbook.

    Overwrites any existing sheet with the same name.
    Returns the sheet name used.
    """
    tpl_wb = openpyxl.load_workbook(template_path, data_only=True)
    tpl_ws = tpl_wb.worksheets[0]
    target_name = new_sheet_name or tpl_ws.title

    dest_wb = openpyxl.load_workbook(dest_path)
    # Remove existing sheet with that name to ensure fresh copy
    if target_name in dest_wb.sheetnames:
        std = dest_wb[target_name]
        dest_wb.remove(std)

    new_ws = dest_wb.create_sheet(title=target_name)

    # Copy cell values (styles not critical for basic numbers)
    for row in tpl_ws.iter_rows(values_only=False):
        for cell in row:
            new_ws[cell.coordinate].value = cell.value

    dest_wb.save(dest_path)
    logger.info("Copied template sheet '%s' into %s", target_name, dest_path)
    return target_name


def populate_template(values: Dict[str, float], dest_path: str, sheet_name: str) -> None:
    """Write numbers into existing sheet, only where coordinates already exist."""
    wb = openpyxl.load_workbook(dest_path)
    if sheet_name not in wb.sheetnames:
        raise ValueError(f"Sheet {sheet_name} not found in {dest_path}")
    ws = wb[sheet_name]

    for cell, num in values.items():
        if cell in ws:
            ws[cell].value = num
        else:
            logger.debug("Skipping value for non-existent cell %s", cell)

    wb.save(dest_path)
    logger.info("Populated %d values into sheet '%s'", len(values), sheet_name)


# ---------------------------------------------------------------------------
# Formula evaluation engine
# ---------------------------------------------------------------------------


# new function for inserting into any workbook
def insert_values_sheet(values: Dict[str, float], workbook_path: str, sheet_title: str) -> None:
    """Create a new sheet and write cell->value mappings.

    Args:
        values: Flat dict of cell address -> number (e.g. {"A1": 10}).
        workbook_path: Existing workbook file to modify.
        sheet_title: Title for the new sheet.
    """
    wb = openpyxl.load_workbook(workbook_path)
    if sheet_title in wb.sheetnames:
        ws = wb[sheet_title]
        logger.info("Overwriting existing sheet '%s' in %s", sheet_title, workbook_path)
    else:
        ws = wb.create_sheet(title=sheet_title)

    for cell, num in values.items():
        ws[cell] = num

    wb.save(workbook_path)
    logger.info("Inserted %d values into sheet '%s' of %s", len(values), sheet_title, workbook_path)
    

# ---------------------------------------------------------------------------
# Final AI review helper
# ---------------------------------------------------------------------------
def last_check(workbook_values: Dict[str, Dict[str, float]], summary_question: Optional[str] = None) -> str:
    """Ask the model to review the filled data and flag any issues.

    Args:
        workbook_values: Nested dict of the numbers we plan to write – typically the result of evaluate_formulas.
        summary_question: Optional extra question/prompts for the model.
    Returns:
        Model response containing either 'All good' or suggestions/corrections.
    """
    system_prompt = (
        "You are a formatting auditor. Examine the structured workbook data intended for presentation. "
        "Check for alignment with standard financial-model formatting: correct headers, year columns, sourceline totals, consistent decimal places, and logical ordering. "
        "If everything is presentation-ready, respond with 'PASS'. Otherwise, list the formatting issues and specify the exact cell edits required to fix them (sheet, cell, new_value or action)."
    )
    context = {
        "values": workbook_values,
        "question": summary_question or "Perform a final sanity check on the financial model output."
    }
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": json.dumps(context)},
    ]
    return _chat_with_retries(messages, purpose="final_review")
    

# ---------------------------------------------------------------------------
# Command-line entrypoint
# ---------------------------------------------------------------------------
def main() -> None:
    """Run the forecasting pipeline writing results back into the source workbook."""
    user_question = input("Enter analysis question (or press Enter for default): ") or None

    wb_data = load_excel_data(SOURCE_FILE)

    print("\n--- Calling identify_assumptions ---")
    assumptions = identify_assumptions(wb_data, None, user_question)
    print("AI assumptions response:\n", assumptions)

    print("\n--- Calling make_formulas ---")
    formulas_json = make_formulas(wb_data, None, user_question)
    print("Raw formulas JSON:\n", formulas_json)

    values = values_from_json(formulas_json)
    print("\nParsed numeric values:")
    print(values)

    review = last_check(values)
    print("\nAI review:\n", review)

    # Step: ensure template sheet exists in source workbook
    tmpl_sheet = copy_template_sheet("incomestatementformat.xlsx", SOURCE_FILE, "Income Statement")

    # Merge all values and populate
    combined: Dict[str, float] = {}
    for sheet_dict in values.values():
        combined.update(sheet_dict)

    populate_template(combined, SOURCE_FILE, tmpl_sheet)
    print("\nUpdated", SOURCE_FILE, "with formatted sheet", tmpl_sheet)


if __name__ == "__main__":
    main()