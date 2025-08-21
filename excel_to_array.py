import argparse
import json
import pandas as pd
import numpy as np
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

def excel_to_simple_arrays(filepath: str) -> dict:
    """Convert Excel sheets to simple arrays without address information.
    Returns a dictionary where keys are sheet names and values are 2D arrays (lists of lists) of just the cell values.
    """
    xl = pd.ExcelFile(filepath)
    data = {}
    for sheet_name in xl.sheet_names:
        df = xl.parse(sheet_name)
        # Convert to list of lists, replacing NaN with None
        data[sheet_name] = df.where(pd.notnull(df), None).values.tolist()
    return data

def excel_to_numpy_matrices(filepath: str) -> dict:
    """Convert Excel sheets to NumPy matrices where possible.
    Returns a dictionary where keys are sheet names and values are NumPy arrays.
    Non-numeric sheets will be converted to arrays of strings.
    """
    xl = pd.ExcelFile(filepath)
    data = {}
    for sheet_name in xl.sheet_names:
        df = xl.parse(sheet_name)
        # Try to convert to numeric matrix
        try:
            # This will work for sheets with all numeric data
            numeric_df = df.select_dtypes(include=[np.number])
            if not numeric_df.empty and numeric_df.shape == df.shape:
                data[sheet_name] = numeric_df.values  # Returns NumPy array
            else:
                # If not all numeric, convert to object array with strings
                data[sheet_name] = df.astype(str).where(pd.notnull(df), None).values
        except Exception as e:
            # If conversion fails, keep as object array
            data[sheet_name] = df.astype(object).where(pd.notnull(df), None).values
    return data

def excel_to_csv_files(filepath: str, output_dir: str) -> None:
    """Convert each Excel sheet to a separate CSV file.
    This provides the simplest format for model processing.
    """
    xl = pd.ExcelFile(filepath)
    os.makedirs(output_dir, exist_ok=True)
    
    for sheet_name in xl.sheet_names:
        df = xl.parse(sheet_name)
        # Clean sheet name for use as filename
        clean_sheet_name = "".join(c for c in sheet_name if c.isalnum() or c in (' ', '_')).rstrip()
        csv_path = os.path.join(output_dir, f"{clean_sheet_name}.csv")
        df.to_csv(csv_path, index=False)
        print(f"Sheet '{sheet_name}' written to {csv_path}")

def main():
    parser = argparse.ArgumentParser(description="Convert Excel workbook to various data structures for model processing.")
    parser.add_argument("excel_path", help="Path to the input Excel file")
    parser.add_argument("-o", "--output", help="Path to output file (defaults to a file in the current directory)")
    parser.add_argument("-f", "--format", choices=["dict", "array", "numpy", "csv"], default="dict",
                        help="Output format: dict (with addresses), array (values only), numpy (NumPy matrices), or csv (separate CSV files)")
    parser.add_argument("-d", "--csv-dir", help="Directory for CSV output files (required if format is csv)")
    args = parser.parse_args()

    if args.format == "dict":
        result = excel_to_dict(args.excel_path)
        json_data = json.dumps(result, indent=4, default=str)  # default=str handles NumPy types
    elif args.format == "array":
        result = excel_to_simple_arrays(args.excel_path)
        json_data = json.dumps(result, indent=4)
    elif args.format == "numpy":
        result = excel_to_numpy_matrices(args.excel_path)
        # Convert NumPy arrays to lists for JSON serialization
        json_result = {}
        for sheet_name, matrix in result.items():
            json_result[sheet_name] = matrix.tolist()
        json_data = json.dumps(json_result, indent=4)
    elif args.format == "csv":
        if not args.csv_dir:
            print("Error: --csv-dir is required when format is csv")
            return
        excel_to_csv_files(args.excel_path, args.csv_dir)
        return
    
    if args.output:
        output_path = args.output
    else:
        # Default: same directory as the script's current working directory, filename based on input
        base_name = os.path.splitext(os.path.basename(args.excel_path))[0]
        output_path = os.path.join(os.getcwd(), f"{base_name}_{args.format}.json")
    
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(json_data)
    print(f"Data written to {output_path}")

if __name__ == "__main__":
    main()
