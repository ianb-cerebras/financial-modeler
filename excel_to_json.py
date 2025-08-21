import argparse
import json
import pandas as pd
import os

def excel_to_dict(filepath: str) -> dict:
    """Read all sheets from the Excel file and return a dictionary.
    The keys are sheet names and the values are lists of rows, where each row is a list of cell dictionaries containing 'value' and 'address'.
    """
    xl = pd.ExcelFile(filepath)
    data = {}
    for sheet_name in xl.sheet_names:
        df = xl.parse(sheet_name)
        rows = []
        for row_idx, row in df.iterrows():
            row_data = []
            for col_idx, value in enumerate(row):
                # Convert to Excel-style column letter and row number (1-based)
                col_letter = chr(ord('A') + col_idx)
                address = f"{col_letter}{row_idx + 2}"  # +2 because row_idx is 0-based and we're adding the header row
                row_data.append({"value": value if pd.notna(value) else None, "address": address})
            rows.append(row_data)
        
        # Add header row
        header_row = []
        for col_idx, column_name in enumerate(df.columns):
            col_letter = chr(ord('A') + col_idx)
            address = f"{col_letter}1"
            header_row.append({"value": column_name, "address": address})
        rows.insert(0, header_row)
        
        data[sheet_name] = rows
    return data

def excel_to_matrix(filepath: str) -> dict:
    """Read all sheets from the Excel file and return a dictionary of 2D arrays.
    Each sheet maps to a list of rows, where the first row is the header and
    subsequent rows are the sheet values with NaNs converted to None.
    """
    xl = pd.ExcelFile(filepath)
    data = {}
    for sheet_name in xl.sheet_names:
        df = xl.parse(sheet_name)
        header_row = list(df.columns)
        values = df.where(pd.notna(df), None).values.tolist()
        data[sheet_name] = [header_row] + values
    return data

def main():
    parser = argparse.ArgumentParser(description="Convert an Excel workbook to JSON with cell addresses.")
    parser.add_argument("excel_path", help="Path to the input Excel file")
    parser.add_argument("-o", "--output", help="Path to output JSON file (defaults to a file in the current directory)")
    parser.add_argument("--format", choices=["cells", "matrix"], default="cells", help="Output structure: 'cells' (value+address dicts) or 'matrix' (2D values with header row)")
    args = parser.parse_args()

    if args.format == "matrix":
        result = excel_to_matrix(args.excel_path)
    else:
        result = excel_to_dict(args.excel_path)
    json_data = json.dumps(result, indent=4)

    if args.output:
        output_path = args.output
    else:
        # Default: same directory as the script's current working directory, filename based on input
        base_name = os.path.splitext(os.path.basename(args.excel_path))[0]
        output_path = os.path.join(os.getcwd(), f"{base_name}.json")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(json_data)
    print(f"JSON data written to {output_path}")

if __name__ == "__main__":
    main()
