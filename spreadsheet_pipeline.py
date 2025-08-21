"""End-to-end pipeline for offline spreadsheet processing.

1. Loader – reads Excel or CSV into a matrix and an address map.
2. AiParser – calls Cerebras Cloud SDK to convert natural-language instructions into
   structured operations that reference addresses.
3. LogicEngine – executes operations on the matrix.
4. Exporter – writes the updated matrix back to CSV or Excel.

This file implements thin, testable components so the heavy lifting happens in Python
rather than inside Excel.
"""

from __future__ import annotations

import os
import csv
import json
from typing import List, Dict, Tuple, Any, Iterable, Union

import openpyxl

try:
    # Cerebras SDK is optional at import time – only required when AiParser is used.
    from cerebras.cloud.sdk import Cerebras  # type: ignore
except ImportError:  # pragma: no cover
    Cerebras = None  # type: ignore

Matrix = List[List[Any]]
AddressMap = Dict[Tuple[int, int], str]  # (row_idx, col_idx) -> "A1"


class SpreadsheetLoader:
    """Load a spreadsheet (CSV or Excel) into a matrix + address map."""

    def __init__(self, path: str):
        self.path = path
        self.matrix: Matrix = []
        self.address_map: AddressMap = {}

    def load(self) -> None:
        if self.path.lower().endswith(".csv"):
            self._load_csv()
        else:
            self._load_excel()

    # ---------------------------------------------------------------------
    # Internal helpers
    # ---------------------------------------------------------------------
    def _load_csv(self) -> None:
        """Simple CSV reader. Addresses are computed manually (A1 style)."""
        with open(self.path, newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            for r_idx, row in enumerate(reader):
                self.matrix.append(row)
                for c_idx, _ in enumerate(row):
                    self.address_map[(r_idx, c_idx)] = self._idx_to_address(r_idx, c_idx)

    def _load_excel(self) -> None:
        """Use openpyxl so we get accurate cell coordinates."""
        wb = openpyxl.load_workbook(self.path, data_only=True)
        ws = wb.active  # default to first sheet for now
        for r_idx, row in enumerate(ws.iter_rows(values_only=True)):
            row_vals: List[Any] = []
            for c_idx, value in enumerate(row):
                cell_address = ws.cell(row=r_idx + 1, column=c_idx + 1).coordinate
                row_vals.append(value)
                self.address_map[(r_idx, c_idx)] = cell_address
                print(value)
            self.matrix.append(row_vals)

    # Static ----------------------------------------------------------------
    @staticmethod
    def _idx_to_address(r: int, c: int) -> str:
        """Convert zero-based (row, col) to Excel address like A1."""
        col = ""
        n = c
        while True:
            n, remainder = divmod(n, 26)
            col = chr(65 + remainder) + col
            if n == 0:
                break
            n -= 1
        return f"{col}{r + 1}"


class AiParser:
    """Thin wrapper around Cerebras Cloud SDK call."""

    MODEL_NAME = "qwen-3-coder-480b"

    def __init__(self, api_key: str | None = None):
        if Cerebras is None:
            raise ImportError("cerebras-cloud-sdk not installed")
        self.client = Cerebras(api_key=api_key or os.environ.get("CEREBRAS_API_KEY"))

    def parse(self, instruction: str) -> List[dict]:
        """Convert instruction to a list of structured operations."""
        msg = [{"role": "user", "content": instruction}]
        resp = self.client.chat.completions.create(
            model=self.MODEL_NAME,
            messages=msg,
        )
        content = resp.choices[0].message.content  # type: ignore[attr-defined]
        try:
            return json.loads(content)
        except Exception:  # pragma: no cover
            # Fallback: wrap raw text
            return [{"op": "raw", "content": content}]


class LogicEngine:
    """Execute structured operations on a matrix."""

    def __init__(self, matrix: Matrix, address_map: AddressMap):
        self.matrix = matrix
        self.addr_map = address_map

    def apply(self, ops: Iterable[dict]) -> None:
        for op in ops:
            self._apply_single(op)

    # ------------------------------------------------------------------
    def _apply_single(self, op: dict) -> None:  # noqa: C901 (simple placeholder)
        name = op.get("op")
        if name == "raw":
            # placeholder – nothing to do
            return
        if name == "add":
            self._binary(op, lambda a, b: a + b)
        elif name == "subtract":
            self._binary(op, lambda a, b: a - b)
        else:
            raise ValueError(f"Unsupported op: {name}")

    def _binary(self, op: dict, fn):
        tgt = op["target"]
        left = self._resolve(op["left"])
        right = self._resolve(op["right"])
        res = fn(left, right)
        r, c = self._address_to_idx(tgt)
        # ensure row exists
        while r >= len(self.matrix):
            self.matrix.append([])
        row = self.matrix[r]
        while c >= len(row):
            row.append(None)
        row[c] = res

    # ------------------------------------------------------------------
    def _resolve(self, ref: Union[str, float, int]) -> Any:
        if isinstance(ref, (int, float)):
            return ref
        r, c = self._address_to_idx(ref)
        return self.matrix[r][c]

    def _address_to_idx(self, addr: str) -> Tuple[int, int]:
        # very simple conversion (supports AA, AB …)
        col_part = "".join(filter(str.isalpha, addr)).upper()
        row_part = "".join(filter(str.isdigit, addr))
        col_num = 0
        for ch in col_part:
            col_num = col_num * 26 + (ord(ch) - 64)
        return int(row_part) - 1, col_num - 1


class Exporter:
    """Write matrix back to CSV."""

    def __init__(self, matrix: Matrix):
        self.matrix = matrix

    def to_csv(self, path: str) -> None:
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerows(self.matrix)


__all__ = [
    "SpreadsheetLoader",
    "AiParser",
    "LogicEngine",
    "Exporter",
    "read_workbook_as_matrices",
    "ask_question_about_workbook",
]

def read_workbook_as_matrices(path: str) -> Dict[str, Matrix]:
    """Load all sheets from an Excel workbook into a dict of 2-D matrices."""
    wb = openpyxl.load_workbook(path, data_only=True)
    data: Dict[str, Matrix] = {}
    for ws in wb.worksheets:
        sheet_matrix: Matrix = []
        for row in ws.iter_rows(values_only=True):
            sheet_matrix.append(list(row))
        data[ws.title] = sheet_matrix
    return data


def ask_question_about_workbook(
    question: str,
    sheets: Dict[str, Matrix],
    api_key: str | None = None,
) -> str:
    """Ask the Cerebras model a question about all sheets' data.

    The entire workbook (sheet -> matrix) is serialized to JSON and provided as
    context. Keep questions focused to avoid overly large prompts.
    """
    if Cerebras is None:
        raise ImportError("cerebras-cloud-sdk not installed")

    client = Cerebras(api_key=api_key or os.environ.get("CEREBRAS_API_KEY"))
    system_ctx = (
        "You are a data analyst. Answer questions about this spreadsheet. "
        "Be concise and perform calculations as needed. Data:\n" + json.dumps(sheets)
    )
    messages = [
        {"role": "system", "content": system_ctx},
        {"role": "user", "content": question},
    ]
    resp = client.chat.completions.create(
        model=AiParser.MODEL_NAME,
        messages=messages,
    )
    return resp.choices[0].message.content  # type: ignore[attr-defined]

