from pathlib import Path
if Path("/opt/airflow/project").exists():
    BASE_DIR = Path("/opt/airflow/project")
else:
    BASE_DIR = Path(r"C:\Users\timot\Desktop")
import pdfplumber
import pandas as pd
from datetime import datetime

PDFS = [
    {
        "academic_year": "2023-24",
        "path": BASE_DIR / "Calendar23-24.pdf"
    },
    {
        "academic_year": "2024-25",
        "path": BASE_DIR / "Calendar24-25.pdf"
    },
    {
        "academic_year": "2025-26",
        "path": BASE_DIR / "Calendar25-26.pdf"
    }
]

OUTPUT_FILE = BASE_DIR / "sjsu_academic_calendar_raw.csv"

rows = []

for pdf_info in PDFS:
    academic_year = pdf_info["academic_year"]
    path = pdf_info["path"]

    print(f"Processing {academic_year}: {path}")

    with pdfplumber.open(path) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):
            text = page.extract_text()

            if not text:
                continue

            for line_number, line in enumerate(text.splitlines(), start=1):
                line = line.strip()

                if not line:
                    continue

                rows.append({
                    "academic_year": academic_year,
                    "source_file": path,
                    "page_number": page_number,
                    "line_number": line_number,
                    "raw_text": line,
                    "scraped_at": datetime.now().isoformat(timespec="seconds")
                })

df = pd.DataFrame(rows)

df.to_csv(
    OUTPUT_FILE,
    index=False,
    encoding="utf-8-sig"
)

print("\nDone.")
print("Rows saved:", len(df))
print("CSV file:", OUTPUT_FILE)
print(df.head(30))