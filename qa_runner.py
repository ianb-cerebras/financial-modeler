import argparse
import os
from spreadsheet_pipeline import read_workbook_as_matrices, ask_question_about_workbook


def main():
    parser = argparse.ArgumentParser(description="Interactive Q&A about an Excel workbook (Ctrl-C or 'exit' to quit).")
    parser.add_argument("excel_path", help="Path to the input .xlsx file")
    parser.add_argument("--api-key", dest="api_key", default=None, help="Cerebras API key (defaults to CEREBRAS_API_KEY env var)")
    args = parser.parse_args()

    sheets = read_workbook_as_matrices(args.excel_path)
    print("Loaded workbook. Type your questions about the data. Type 'exit' to quit.\n")

    while True:
        try:
            question = input("Q> ").strip()
            if not question:
                continue
            if question.lower() in ("exit", "quit", "q", ":q"):
                print("Bye.")
                break
            answer = ask_question_about_workbook(question, sheets, api_key=args.api_key)
            print(f"A> {answer}\n")
        except KeyboardInterrupt:
            print("\nBye.")
            break


if __name__ == "__main__":
    main()