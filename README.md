## Excel AI Finance Runner

A finance analysis tool powered by Cererbas. It reads your Excel workbook, calls an AI planner, deterministically evaluates numbers, and can write results back to the model and export a polished report.

### Status
- Early working version. A release with stronger output validation and nicer model formatting is coming soon.

### What it does (today)
- Load all sheets from an `.xlsx` workbook
- AI planning via Cerebras (e.g., formulas or numeric plans)
- Deterministic evaluation in Python
- Optional write-back to the "Operating Model" sheet
- Optional logging and console output

### Coming soon
- Automatic validation of outputs (balance checks, sanity rules)
- Cleaner, more consistent model formatting and layout
- Richer document/report generation
- Adaptive 

## Quickstart
1) Requirements
- Python 3.9+
- An environment variable `CEREBRAS_API_KEY` set with a valid key

2) Install dependencies
```bash
pip install openpyxl python-docx cerebras-cloud-sdk pandas numpy
```

3) Run (AI-driven finance runner)
```bash
python ai_finance_runner.py /path/to/workbook.xlsx "Your question here" --debug --log
# Add --write to write computed numbers back into the workbook
```

Notes:
- Avoid committing secrets. Use `CEREBRAS_API_KEY` from your shell or a secrets manager.
- Large or sensitive workbooks should be sanitized before sharing.

## Repo hygiene (suggested)
- Exclude virtual envs, caches, and temp files:
```
venv/
__pycache__/
~$*
*.env
```

## Roadmap highlights
- Validation: more robust balance-sheet checks and cross-statement consistency
- Formatting: improved Excel write-back (styles, headers, alignment) and better .docx reports
- Performance: tighter prompts and faster evaluation

If you have a tricky model you want supported, open an issue with a redacted sample.