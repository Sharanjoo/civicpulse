from pathlib import Path
if Path("/opt/airflow/project").exists():
    BASE_DIR = Path("/opt/airflow/project")
else:
    BASE_DIR = Path(r"C:\Users\timot\Desktop")
import requests
import pandas as pd
import sys
import time
import os
from io import StringIO
from datetime import date, datetime, timedelta
from tqdm import tqdm

BASE_URL = "https://booking.sjlibrary.org/widget/hours/grid"

HEADERS = {
    "accept": "text/html, */*; q=0.01",
    "user-agent": "Mozilla/5.0 (SJSU MSADI DATA 226 student project; public library hours analysis)",
    "x-requested-with": "XMLHttpRequest",
    "referer": "https://library.sjsu.edu/library-hours"
}

OUTPUT_FILE = BASE_DIR / "sjlibrary_hours.csv"
COMPLETED_WEEKS_FILE = BASE_DIR / "sjlibrary_completed_hours_weeks.txt"

REQUEST_SLEEP = 0.5

def week_start_sunday(target_date):
    days_since_sunday = (target_date.weekday() + 1) % 7
    return target_date - timedelta(days=days_since_sunday)

def week_range(start_date, end_date):
    current = week_start_sunday(start_date)
    final_week = week_start_sunday(end_date)

    while current <= final_week:
        yield current
        current += timedelta(days=7)

def load_completed_weeks():
    if not os.path.exists(COMPLETED_WEEKS_FILE):
        return set()

    with open(COMPLETED_WEEKS_FILE, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())

def mark_week_completed(week_start_date):
    completed_weeks = load_completed_weeks()
    week_start_str = week_start_date.isoformat()

    if week_start_str not in completed_weeks:
        with open(COMPLETED_WEEKS_FILE, "a", encoding="utf-8") as f:
            f.write(week_start_str + "\n")

def remove_existing_rows_for_week(week_start_date, output_file):
    if not os.path.exists(output_file):
        return

    week_start_str = week_start_date.isoformat()

    df = pd.read_csv(output_file, dtype=str)

    if "week_start_date" not in df.columns:
        print(f"WARNING: 'week_start_date' column not found in {output_file}. Cannot remove old rows.")
        return

    original_count = len(df)
    df = df[df["week_start_date"] != week_start_str]
    removed_count = original_count - len(df)

    df.to_csv(output_file, index=False, encoding="utf-8-sig")

    if removed_count > 0:
        print(f"Removed {removed_count} old rows for week {week_start_str} before re-scraping.")

def append_df_to_csv(df, output_file):
    if df.empty:
        return

    file_exists = os.path.exists(output_file)

    df.to_csv(
        output_file,
        mode="a",
        header=not file_exists,
        index=False,
        encoding="utf-8-sig"
    )

def fetch_hours_for_week(week_start_date):
    params = {
        "id": "3284",
        "lid": "19569",
        "date": week_start_date.isoformat()
    }

    response = requests.get(
        BASE_URL,
        params=params,
        headers=HEADERS,
        timeout=30
    )
    response.raise_for_status()

    html = response.text
    tables = pd.read_html(StringIO(html))

    if not tables:
        return pd.DataFrame()

    df = tables[0]
    first_col = df.columns[0]

    df_long = df.melt(
        id_vars=[first_col],
        var_name="date_day",
        value_name="hours_text"
    )

    df_long = df_long.rename(columns={first_col: "unit_name"})
    df_long = df_long[df_long["unit_name"] == "SJSU Library Hours"].copy()

    df_long["week_start_date"] = week_start_date.isoformat()
    df_long["scraped_at"] = datetime.now().isoformat(timespec="seconds")

    return df_long

TODAY = date.today()

START_DATE = date(2023, 8, 1)
END_DATE = TODAY + timedelta(days=7)

CURRENT_WEEK = week_start_sunday(TODAY)
NEXT_WEEK = week_start_sunday(TODAY + timedelta(days=7))

refresh_weeks = {
    CURRENT_WEEK.isoformat(),
    NEXT_WEEK.isoformat()
}

completed_weeks = load_completed_weeks()
all_weeks = list(week_range(START_DATE, END_DATE))

remaining_weeks = [
    w for w in all_weeks
    if w.isoformat() not in completed_weeks or w.isoformat() in refresh_weeks
]

print(f"Scraping library hours from {START_DATE} through {END_DATE}")
print("Completed weeks already recorded:", len(completed_weeks))
print("Total weeks in range:", len(all_weeks))
print("Refresh weeks:", sorted(refresh_weeks))
print("Remaining weeks to scrape:", len(remaining_weeks))
print("Output file:", OUTPUT_FILE)
print("Completed weeks file:", COMPLETED_WEEKS_FILE)

total_rows_saved_this_run = 0
had_error = False

for week_start in tqdm(remaining_weeks, desc="Scraping weeks", unit="week"):
    try:
        is_refresh_week = week_start.isoformat() in refresh_weeks

        if is_refresh_week:
            remove_existing_rows_for_week(week_start, OUTPUT_FILE)

        df_week = fetch_hours_for_week(week_start)

        append_df_to_csv(df_week, OUTPUT_FILE)
        mark_week_completed(week_start)

        total_rows_saved_this_run += len(df_week)

        time.sleep(REQUEST_SLEEP)

    except Exception as e:
        print(f"\nERROR on week starting {week_start}: {e}")
        print("Stopping safely. Rerun the script and it will skip completed weeks, except refresh weeks.")
        had_error = True
        break

print("\nDone or safely stopped.")
print("Rows saved this run:", total_rows_saved_this_run)
print("CSV file:", OUTPUT_FILE)
print("Completed weeks file:", COMPLETED_WEEKS_FILE)

if had_error:
    sys.exit(1)