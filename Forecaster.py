import os
import json
import argparse
from typing import Dict, Any, List
import openpyxl
from cerebras.cloud.sdk import Cerebras
from docx import Document  # pip install python-docx
import time
import logging
import random

# ---------------- User configurable paths ----------------
SOURCE_FILE = "test.xlsx"
TEMPLATE_FILE = "incomestatementformat.xlsx"
OUTPUT_FILE = "incomestatementformat_filled.xlsx"

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
def identify_assumptions(workbook_data: Dict[str, Any], core_elements: str | None = None, question: str | None = None) -> str:
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


def make_formulas(workbook_data: Dict[str, Any], core_elements: str | None = None, question: str | None = None) -> str:
    """Cerebras call – generate spreadsheet-style formulas or literal numbers for template filling."""
    system_prompt = (
        "You are a senior financial analyst. Based on the extracted core elements, generate **numeric** Excel-style "
        "formulas or literal values that populate the target template. Return **only** a JSON array of objects with "
        "keys: sheet, cell, formula. Do not include commentary. Use expressions like 'A12-B12', not words."
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
def evaluate_formulas(workbook_data: Dict[str, List[List[Any]]], formulas_json: str) -> Dict[str, Dict[str, float]]:
    """Turn JSON formulas into numeric values using in-memory evaluation."""
    import re, string
    value_map: Dict[tuple[str, str], float] = {}
    for sheet, matrix in workbook_data.items():
        for r, row in enumerate(matrix, start=1):
            for c, val in enumerate(row, start=1):
                if isinstance(val, (int, float)) and val is not None:
                    col_letter = string.ascii_uppercase[c - 1]
                    value_map[(sheet, f"{col_letter}{r}")] = float(val)

    cell_re = re.compile(r"\b([A-Z]+[0-9]+)\b")

    formulas = json.loads(formulas_json)

    remaining = formulas.copy()
    progress = True
    while remaining and progress:
        progress = False
        for f in remaining[:]:
            key = (f["sheet"], f["cell"])
            expr = f["formula"]
            unresolved = False

            def sub(m):
                ref = (f["sheet"], m.group(1))
                nonlocal unresolved
                if ref in value_map:
                    return str(value_map[ref])
                unresolved = True
                return m.group(0)

            expr_sub = cell_re.sub(sub, expr)
            if unresolved:
                continue
            try:
                value_map[key] = float(eval(expr_sub, {"__builtins__": {}}, {}))
                remaining.remove(f)
                progress = True
            except Exception as e:
                logger.warning("Failed to eval formula %s=%s: %s", key, expr_sub, e)
                remaining.remove(f)

    if remaining:
        logger.warning("%d formulas could not be resolved due to missing dependencies.", len(remaining))

    out: Dict[str, Dict[str, float]] = {}
    for (sheet, cell), val in value_map.items():
        out.setdefault(sheet, {})[cell] = val
    return out


def fill_template(values: Dict[str, Dict[str, float]], template_path: str, output_path: str | None = None) -> str:
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

    for sheet_name, cell_map in values.items():
        ws = wb[sheet_name] if sheet_name in wb.sheetnames else wb.create_sheet(sheet_name)
        for cell, num in cell_map.items():
            ws[cell] = num

    if output_path is None:
        p = Path(template_path)
        output_path = str(p.with_name(p.stem + "_filled" + p.suffix))

    wb.save(output_path)
    logger.info("Template filled and saved to %s", output_path)
    return output_path


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
def last_check(workbook_values: Dict[str, Dict[str, float]], summary_question: str | None = None) -> str:
    """Ask the model to review the filled data and flag any issues.

    Args:
        workbook_values: Nested dict of the numbers we plan to write – typically the result of evaluate_formulas.
        summary_question: Optional extra question/prompts for the model.
    Returns:
        Model response containing either 'All good' or suggestions/corrections.
    """
    system_prompt = (
        "You are an auditing financial analyst. Review the following structured workbook data. "
        "Identify any glaring inconsistencies, missing pieces, or calculation errors. "
        "If everything is reasonable, respond with 'PASS'. Otherwise, list the issues found and recommend corrections."
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
    """Run the forecasting pipeline using hard-coded file paths above."""
    wb_data = load_excel_data(SOURCE_FILE)
    formulas_json = make_formulas(wb_data, None)
    values = evaluate_formulas(wb_data, formulas_json)
    review = last_check(values)
    print("AI review:\n", review)
    filled_path = fill_template(values, TEMPLATE_FILE, OUTPUT_FILE)
    print("Filled workbook written to", filled_path)


if __name__ == "__main__":
    main()
    
