from pathlib import Path
if Path("/opt/airflow/project").exists():
    BASE_DIR = Path("/opt/airflow/project")
else:
    BASE_DIR = Path(r"C:\Users\timot\Desktop")
import requests
import pandas as pd
import time
import os
from datetime import date, datetime, timedelta
from tqdm import tqdm

BASE_URL = "https://booking.sjlibrary.org/spaces/bookings/search"

HEADERS = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "user-agent": "Mozilla/5.0 (SJSU MSADI DATA 226 student project; public booking data analysis)",
    "x-requested-with": "XMLHttpRequest",
    "referer": "https://booking.sjlibrary.org/spaces/bookings?lid=23137&gid=49099"
}

TODAY = date.today()

START_DATE = date(2023, 8, 1)
END_DATE = TODAY + timedelta(days=4)

REFRESH_START_DATE = TODAY - timedelta(days=1)
REFRESH_END_DATE = TODAY + timedelta(days=4)

OUTPUT_FILE = BASE_DIR / "sjlibrary_bookings.csv"
COMPLETED_DATES_FILE = BASE_DIR / "sjlibrary_completed_dates.txt"

REQUEST_SLEEP = 0.5
DAY_SLEEP = 1.0

def load_completed_dates():
    if not os.path.exists(COMPLETED_DATES_FILE):
        return set()

    with open(COMPLETED_DATES_FILE, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())

def mark_date_completed(target_date):
    completed_dates = load_completed_dates()
    target_date_str = target_date.isoformat()

    if target_date_str not in completed_dates:
        with open(COMPLETED_DATES_FILE, "a", encoding="utf-8") as f:
            f.write(target_date_str + "\n")

def append_rows_to_csv(rows, output_file):
    if not rows:
        return

    df = pd.DataFrame(rows)
    file_exists = os.path.exists(output_file)

    df.to_csv(
        output_file,
        mode="a",
        header=not file_exists,
        index=False,
        encoding="utf-8-sig"
    )

def remove_existing_rows_for_date(target_date, output_file):
    if not os.path.exists(output_file):
        return

    target_date_str = target_date.isoformat()

    df = pd.read_csv(output_file, dtype=str)

    if "_query_date" not in df.columns:
        print(f"WARNING: '_query_date' column not found in {output_file}. Cannot remove old rows.")
        return

    original_count = len(df)
    df = df[df["_query_date"] != target_date_str]
    removed_count = original_count - len(df)

    df.to_csv(output_file, index=False, encoding="utf-8-sig")

    if removed_count > 0:
        print(f"Removed {removed_count} old rows for {target_date_str} before re-scraping.")

def fetch_bookings_for_date(target_date):
    all_rows = []
    start = 0
    length = 25

    while True:
        params = {
            "lid": "0",
            "gid": "0",
            "eid": "0",
            "seat": "0",
            "d": "custom",
            "customDate": target_date.strftime("%Y-%m-%d"),
            "q": "",
            "daily": "0",
            "draw": "1",
            "order[0][column]": "1",
            "order[0][dir]": "asc",
            "order[0][name]": "",
            "start": str(start),
            "length": str(length),
            "search[value]": "",
            "_": int(time.time() * 1000)
        }

        response = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()

        data = response.json()
        rows = data.get("data", [])
        records_filtered = int(data.get("recordsFiltered", 0))

        for row in rows:
            row["_query_date"] = target_date.isoformat()
            row["_scraped_at"] = datetime.now().isoformat(timespec="seconds")

        all_rows.extend(rows)

        if len(rows) == 0 or start + length >= records_filtered:
            break

        start += length
        time.sleep(REQUEST_SLEEP)

    return all_rows

def date_range(start_date, end_date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)

completed_dates = load_completed_dates()

all_dates = list(date_range(START_DATE, END_DATE))

refresh_dates = set(
    d.isoformat()
    for d in date_range(REFRESH_START_DATE, REFRESH_END_DATE)
)

remaining_dates = [
    d for d in all_dates
    if d.isoformat() not in completed_dates or d.isoformat() in refresh_dates
]

print("Completed dates already recorded:", len(completed_dates))
print("Total dates in range:", len(all_dates))
print("Refresh window:", REFRESH_START_DATE, "through", REFRESH_END_DATE)
print("Remaining dates to scrape:", len(remaining_dates))
print(f"Scraping from {START_DATE} through {END_DATE}")
print("Output file:", OUTPUT_FILE)
print("Completed dates file:", COMPLETED_DATES_FILE)

progress_bar = tqdm(
    remaining_dates,
    desc="Scraping dates",
    unit="day"
)

total_rows_saved_this_run = 0

for current_date in progress_bar:
    try:
        progress_bar.set_postfix_str(f"current={current_date}")

        is_refresh_date = current_date.isoformat() in refresh_dates

        if is_refresh_date:
            remove_existing_rows_for_date(current_date, OUTPUT_FILE)

        rows = fetch_bookings_for_date(current_date)

        append_rows_to_csv(rows, OUTPUT_FILE)
        mark_date_completed(current_date)

        total_rows_saved_this_run += len(rows)

        progress_bar.set_postfix_str(
            f"current={current_date}, rows={len(rows)}, total_rows={total_rows_saved_this_run}"
        )

        time.sleep(DAY_SLEEP)

    except Exception as e:
        print(f"\nERROR on {current_date}: {e}")
        print("Stopping safely. Rerun the script and it will skip completed dates, except refresh dates.")
        break

print("\nDone or safely stopped.")
print("Rows saved this run:", total_rows_saved_this_run)
print("CSV file:", OUTPUT_FILE)
print("Completed dates file:", COMPLETED_DATES_FILE)