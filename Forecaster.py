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

# Hard-coded row mapping for Income Statement template
ROW_MAPPING = {
    "Revenue": "6",
    "COGS": "7",
    "Gross Profit": "8",
    "Gross Margin %": "9",
    "SG&A": "10",
    "EBIT": "11",
    "Interest Income": "12",
    "Interest Expense": "13",
    "Profit Before Tax": "14",
    "Tax Expense": "15",
    "Net Income": "16",
}

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Template structure extraction
# ---------------------------------------------------------------------------
def extract_template_structure(template_path: str) -> Dict[str, str]:
    """Extract row labels and their cell coordinates from template."""
    wb = openpyxl.load_workbook(template_path)
    ws = wb.worksheets[0]
    structure = {}

    # Look for row labels in column A (assuming labels are in column A)
    for row in range(1, 50):  # Check first 50 rows
        cell = ws[f"A{row}"]
        if cell.value and isinstance(cell.value, str) and cell.value.strip():
            label = cell.value.strip()
            # Map to the data column (E) for the same row
            structure[label] = f"E{row}"

    logger.info("Extracted template structure: %s", structure)
    return structure

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
        "You are a financial analyst. Given the core elements and raw workbook data for P&L forecasting. "
        "extract specific financial information and all underlying assumptions required for modelling."
        "Do not do any modeling, and prepare all potentially relevant information for modeling (P&L)"
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


def make_formulas(workbook_data: Dict[str, Any], template_structure: Dict[str, str], core_elements: Optional[str] = None, question: Optional[str] = None) -> str:
    """Cerebras call – return row-based calculations using template structure."""
    system_prompt = (
        "You are a senior financial analyst creating a 5-year financial model. "
        f"Use this template structure: {template_structure}\n\n"
        "Return a JSON array where each object has: row_label, value.\n\n"
        "• If the value is a direct input (e.g., historical revenue) you may put the literal number.\n"
        "• If the value is **derived** (e.g., COGS = Revenue * 0.6, Gross Profit = Revenue – COGS, margins, etc.) "
        "return an arithmetic expression built ONLY from numeric literals and the four operators + - * / and parentheses.\n\n"
        "Examples:\n"
        "  {\"row_label\":\"Revenue\",\"value\":1050}\n"
        "  {\"row_label\":\"COGS\",\"value\":\"1050*0.6\"}\n"
        "  {\"row_label\":\"Gross Profit\",\"value\":\"1050-630\"}\n\n"
        "IMPORTANT: Include all rows: Revenue, COGS, Gross Profit, Gross Margin %, SG&A, D&A, EBIT, EBIT Margin %, Interest Income, Interest Expense, Profit Before Tax, Tax Expense, Net Income.\n"
        "Do NOT evaluate the expressions."
    )
    context = {
        "workbook": workbook_data,
        "template_structure": template_structure,
        "core_elements": core_elements,
        "question": question or "Create a 5-year financial forecast",
    }
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": json.dumps(context)},
    ]
    return _chat_with_retries(messages, purpose="generate_formulas")


# ---------------------------------------------------------------------------
# Formula evaluation engine
# ---------------------------------------------------------------------------
def values_from_json(json_text: str, row_mapping: Dict[str, str]) -> Dict[str, Dict[str, float]]:
    """Convert row-based JSON into cell-based nested dict using row mapping."""
    import json, numbers
    data = json.loads(json_text)
    out: Dict[str, Dict[str, float]] = {}
    
    # Define the columns for 5-year forecast (E, F, G, H, I for 2023-2027)
    forecast_columns = ['E', 'F', 'G', 'H', 'I']
    
    for obj in data:
        row_label = obj["row_label"]
        val = obj["value"]
        
        # Map row label to row number
        if row_label not in row_mapping:
            print(f"Skipped unknown row label: {row_label}")
            continue
        
        row_num = row_mapping[row_label]
        sheet = "Income Statement"  # Default sheet name
        
        if isinstance(val, str):
            print(f"Evaluating expression for {row_label} (row {row_num}): {val}")
            val = float(eval(val, {"__builtins__": {}}))
            print(f"Result -> {val}")
        elif isinstance(val, numbers.Number):
            print(f"Literal number for {row_label} (row {row_num}): {val}")
            val = float(val)
        else:
            print(f"Skipped unsupported value for {row_label}: {val}")
            continue
        
        # Populate across all forecast columns with the same value
        for col in forecast_columns:
            cell = f"{col}{row_num}"
            out.setdefault(sheet, {})[cell] = val
    
    print(f"\nCompleted values_from_json; total cells processed: {sum(len(d) for d in out.values())}")
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

    # Copy dimensions (column widths)
    for col_dim in tpl_ws.column_dimensions.values():
        new_ws.column_dimensions[col_dim.column_letter].width = col_dim.width
    for row_dim in tpl_ws.row_dimensions.values():
        new_ws.row_dimensions[row_dim.index].height = row_dim.height

    # Copy cell values and basic styles
    from copy import copy as _copy
    for row in tpl_ws.iter_rows():
        for cell in row:
            new_cell = new_ws[cell.coordinate]
            new_cell.value = cell.value
            if cell.has_style:
                new_cell.font = _copy(cell.font)
                new_cell.border = _copy(cell.border)
                new_cell.fill = _copy(cell.fill)
                new_cell.number_format = _copy(cell.number_format)
                new_cell.protection = _copy(cell.protection)
                new_cell.alignment = _copy(cell.alignment)

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
        try:
            ws[cell].value = num
        except ValueError:
            logger.debug("Skipping invalid cell coordinate %s", cell)

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
    template_structure = extract_template_structure("incomestatementformat.xlsx")
    formulas_json = make_formulas(wb_data, template_structure, None, user_question)
    print("Raw formulas JSON:\n", formulas_json)

    values = values_from_json(formulas_json, ROW_MAPPING)
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