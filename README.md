# Excel AI Financial Forecaster

A Python-driven workflow that turns a raw Excel workbook into a five-year projected Income Statement in seconds using a Cerebras LLM for assumption extraction and a deterministic formula engine for all math.

---

## 1.  Key Features

* **LLM-assisted assumption extraction**  – No hard-coding of cell addresses. The model scans the workbook and returns a JSON dict of drivers (`base_revenue`, `sg&a_percent`, etc.).
* **Deterministic calculation layer**  – All math is executed locally in Python, guaranteeing replicable numbers and full auditability.
* **Template-agnostic cell routing**  – `LABEL_TO_ROW` maps semantic labels ("revenue", "cogs", …) to row numbers, while `FORECAST_COLUMNS` picks the start column. Change either to fit any template.
* **One-command run**  – `python forecaster.py` loads the workbook, talks to the model, evaluates numbers, writes results, and prints a runtime summary.
* **Non-destructive**  – Before writing, the script copies the formatted template sheet (`incomestatementformat.xlsx`) into the source workbook, so original data remain intact.

---

## 2.  Repository Layout

```text
excel/
├── Forecaster.py           # Main pipeline
├── incomestatementformat.xlsx  # Clean template sheet copied into every run
├── dummydata.xlsx          # Sample input workbook
├── instructions.json       # Domain rules / formula guidance for the LLM
├── venv/ …
└── README.md               # You are here
```

---

## 3.  Quick-Start

### 3-1  Prerequisites

* Python ≥ 3.9
* `pip install -r requirements.txt` (see below)
* Environment variable `CEREBRAS_API_KEY` containing your cloud API key

### 3-2  Install deps

```bash
python -m venv venv && source venv/bin/activate
pip install openpyxl python-docx cerebras-cloud-sdk
```

### 3-3  Run

```bash
cd excel
python Forecaster.py            # Prompts for a question (press Enter for default)
```

You will see console output like:

```
--- Calling identify_assumptions ---
AI assumptions raw response: …
Parsed assumptions dict: …
--- Computing 5-year forecast locally ---
Forecast values dict: …
Updated dummydata.xlsx with formatted sheet Income Statement
Total AI + projection runtime: 7.42 seconds
```

Open `dummydata.xlsx` → sheet **Income Statement** to see the filled numbers.

---

## 4.  How It Works

1. **Load Workbook**  All sheets are converted to Python lists (`load_excel_data`).
2. **Identify Assumptions**  `identify_assumptions()` sends workbook JSON + rules to the LLM → returns a flat dict of drivers.
3. **Parse & Compute**  `compute_forecast()` turns the drivers into five-year arrays for every P&L line.
4. **Route to Cells**  Rows picked via `LABEL_TO_ROW`; columns via `FORECAST_COLUMNS` (currently B–F → Years 1-5).
5. **Copy Template**  First sheet of `incomestatementformat.xlsx` is cloned into the source workbook (name: *Income Statement*).
6. **Populate & Review**  Numbers are written; a final LLM pass (`last_check`) flags any presentation issues.
7. **Timing**  Total runtime is printed at the end.

---

## 5.  Customising

### 5-1  Change Template Rows / Columns

* Update `LABEL_TO_ROW` at the top of **Forecaster.py** – shift row numbers or add aliases.
* Update `FORECAST_COLUMNS` (default `["B","C","D","E","F"]`).

### 5-2  Add a New Line Item

1. Add a calculation in `compute_forecast()` that returns a 5-element list.
2. Insert it into `series_map` with an appropriate label.
3. Map that label to a row in `LABEL_TO_ROW`.

### 5-3  Extra Drivers

Add keys to the assumptions prompt and extend `compute_forecast()` accordingly. Anything not returned defaults to zero, so iterating is safe.

---

## 6.  Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|--------------|-----|
| Interest rows all zeros | No cash / debt balance found | Add labelled balances or let model back-solve from Interest Expense. |
| Numbers in wrong rows | Row not in `LABEL_TO_ROW` | Add mapping or correct label spelling. |
| Template missing | Ensure `incomestatementformat.xlsx` exists and has the desired formatting. |
| Runtime error in LLM call | Check `CEREBRAS_API_KEY`, internet, or retry. |

---

## 7.  Security & Privacy

No workbook data leave your machine except the JSON payload sent to the Cerebras API. If your workbook is highly sensitive, redact or anonymise before use.

---

## 8.  Roadmap

* Balance-Sheet & Cash-Flow projections
* Scenario manager (best / base / downside)
* CLI flags for headless / batch processing
* CI test harness with sample workbooks

Contributions welcome – open an issue or PR!