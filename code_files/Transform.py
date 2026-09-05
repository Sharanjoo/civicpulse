from pathlib import Path
if Path("/opt/airflow/project").exists():
    PROJECT_ROOT = Path("/opt/airflow/project")
else:
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
import pandas as pd
from datetime import date, datetime, timedelta, time
import re

DATA_DIR = PROJECT_ROOT / "csv_txt_files"
CALENDAR_RAW_FILE = DATA_DIR / "sjsu_academic_calendar_raw.csv"
BOOKINGS_FILE = DATA_DIR / "sjlibrary_bookings.csv"
HOURS_FILE = DATA_DIR / "sjlibrary_hours.csv"
OUTPUT_DIR = PROJECT_ROOT / "warehouse_output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

FACT_BOOKINGS_OUT = OUTPUT_DIR / "fact_bookings.csv"
DIM_DATE_OUT = OUTPUT_DIR / "dim_date.csv"
DIM_TIME_OUT = OUTPUT_DIR / "dim_time.csv"
DIM_ROOM_OUT = OUTPUT_DIR / "dim_room.csv"

START_DATE = pd.to_datetime("2023-08-01").date()
END_DATE = date.today() + timedelta(days=4)

def clean_text(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    return s if s else None

def make_time_key(t):
    return int(f"{t.hour:02d}{t.minute:02d}")

def singularize_category(category):
    c = clean_text(category)
    if c is None:
        return "Room"
    if c.endswith("Rooms"):
        return c[:-1]
    return c

def parse_hour_token(token):
    token = token.strip().lower().replace(".", "")
    m = re.match(r"^(\d{1,2})(?::(\d{2}))?\s*(am|pm)$", token)
    if not m:
        return None

    hour = int(m.group(1))
    minute = int(m.group(2) or 0)
    am_pm = m.group(3)

    if am_pm == "am":
        hour = 0 if hour == 12 else hour
    else:
        hour = 12 if hour == 12 else hour + 12

    return time(hour, minute)

def parse_hours_text(hours_text):
    s = clean_text(hours_text)
    if s is None:
        return pd.Series([None, None, None, None])

    s_norm = s.replace("–", "-").replace("—", "-").strip()

    if s_norm.lower().startswith("24 hours"):
        return pd.Series(["00:00:00", "00:00:00", 24.0, False])

    parts = [p.strip() for p in s_norm.split("-", 1)]
    if len(parts) != 2:
        return pd.Series([None, None, None, None])

    open_t = parse_hour_token(parts[0])
    close_t = parse_hour_token(parts[1])

    if open_t is None or close_t is None:
        return pd.Series([None, None, None, None])

    open_dt = datetime.combine(date(2000, 1, 1), open_t)
    close_dt = datetime.combine(date(2000, 1, 1), close_t)

    if close_dt <= open_dt:
        close_dt += timedelta(days=1)

    hours_open = round((close_dt - open_dt).total_seconds() / 3600, 2)

    return pd.Series([
        open_t.strftime("%H:%M:%S"),
        close_t.strftime("%H:%M:%S"),
        hours_open,
        False
    ])

def part_of_day(hour_24):
    if 6 <= hour_24 <= 11:
        return "Morning"
    if 12 <= hour_24 <= 16:
        return "Afternoon"
    if 17 <= hour_24 <= 20:
        return "Evening"
    return "Late Night"

def build_dim_time():
    rows = []
    current = datetime.combine(date(2000, 1, 1), time(0, 0))

    for _ in range(1440):
        t = current.time()
        hour_24 = t.hour
        hour_12 = hour_24 % 12 or 12
        am_pm = "AM" if hour_24 < 12 else "PM"
        pod = part_of_day(hour_24)

        rows.append({
            "time_key": make_time_key(t),
            "time_value": t.strftime("%H:%M:%S"),
            "hour_24": hour_24,
            "hour_12": hour_12,
            "minute": t.minute,
            "am_pm": am_pm,
            "time_label": current.strftime("%I:%M %p").lstrip("0"),
            "part_of_day": pod,
            "is_morning": pod == "Morning",
            "is_afternoon": pod == "Afternoon",
            "is_evening": pod == "Evening",
            "is_late_night": pod == "Late Night"
        })

        current += timedelta(minutes=1)

    return pd.DataFrame(rows)

def build_dim_room(bookings):
    rooms = bookings[
        ["itemId", "itemName", "categoryName", "categoryUrl", "locationName", "seatName"]
    ].drop_duplicates().copy()

    rooms = rooms.rename(columns={
        "itemId": "item_id",
        "itemName": "room_name",
        "categoryName": "category_name",
        "categoryUrl": "category_url",
        "locationName": "location_name",
        "seatName": "seat_name"
    })

    for col in ["room_name", "category_name", "category_url", "location_name", "seat_name"]:
        rooms[col] = rooms[col].apply(clean_text)

    rooms["room_label"] = rooms.apply(
        lambda r: f"{singularize_category(r['category_name'])} {r['room_name']}",
        axis=1
    )

    rooms["is_study_room"] = rooms["category_name"].str.contains(
        "Study", case=False, na=False
    )

    rooms = rooms.sort_values(
        ["category_name", "room_name", "item_id"]
    ).reset_index(drop=True)

    rooms.insert(0, "room_key", range(1, len(rooms) + 1))

    return rooms[
        [
            "room_key",
            "item_id",
            "room_name",
            "category_name",
            "category_url",
            "location_name",
            "seat_name",
            "room_label",
            "is_study_room"
        ]
    ]

def add_event(events, start_date, end_date, name, event_type, academic_year, semester=None):
    events.append({
        "start_date": pd.to_datetime(start_date).date(),
        "end_date": pd.to_datetime(end_date).date(),
        "academic_event_name": name,
        "academic_event_type": event_type,
        "academic_year": academic_year,
        "semester": semester
    })

def build_academic_events():
    events = []

    for dt, name in [
        ("2023-09-04", "Labor Day"),
        ("2023-11-10", "Veteran’s Day Observed"),
        ("2023-11-23", "Thanksgiving Holiday"),
        ("2023-11-24", "Rescheduled Holiday"),
        ("2023-12-25", "Christmas Holiday"),
        ("2024-01-01", "New Year’s Day"),
        ("2024-01-15", "Dr. Martin Luther King Jr. Day"),
        ("2024-04-01", "Cesar Chavez Day Observed"),
        ("2024-05-27", "Memorial Day"),
        ("2024-06-19", "Juneteenth")
    ]:
        add_event(events, dt, dt, name, "holiday", "2023-24")

    add_event(events, "2023-11-22", "2023-11-22", "Non-Instructional Day", "non_instructional", "2023-24", "Fall 2023")
    add_event(events, "2023-12-07", "2023-12-07", "Study/Conference Day", "study_day", "2023-24", "Fall 2023")
    add_event(events, "2023-12-08", "2023-12-15", "Final Examinations", "exam_period", "2023-24", "Fall 2023")
    add_event(events, "2023-12-20", "2024-01-21", "Winter Recess", "recess", "2023-24")
    add_event(events, "2024-04-01", "2024-04-05", "Spring Recess", "recess", "2023-24", "Spring 2024")
    add_event(events, "2024-05-14", "2024-05-14", "Study/Conference Day", "study_day", "2023-24", "Spring 2024")
    add_event(events, "2024-05-15", "2024-05-22", "Final Examinations", "exam_period", "2023-24", "Spring 2024")

    for dt, name in [
        ("2024-07-04", "Independence Day"),
        ("2024-09-02", "Labor Day"),
        ("2024-11-11", "Veteran’s Day"),
        ("2024-11-28", "Thanksgiving Holiday"),
        ("2024-11-29", "Rescheduled Holiday"),
        ("2024-12-25", "Christmas Holiday"),
        ("2025-01-01", "New Year’s Day"),
        ("2025-01-20", "Dr. Martin Luther King Jr. Day"),
        ("2025-03-31", "Cesar Chavez Day"),
        ("2025-05-26", "Memorial Day"),
        ("2025-06-19", "Juneteenth")
    ]:
        add_event(events, dt, dt, name, "holiday", "2024-25")

    add_event(events, "2024-11-27", "2024-11-27", "Non-Instructional Day", "non_instructional", "2024-25", "Fall 2024")
    add_event(events, "2024-12-10", "2024-12-10", "Study/Conference Day", "study_day", "2024-25", "Fall 2024")
    add_event(events, "2024-12-11", "2024-12-18", "Final Examinations", "exam_period", "2024-25", "Fall 2024")
    add_event(events, "2024-12-23", "2025-01-20", "Winter Recess", "recess", "2024-25")
    add_event(events, "2025-03-31", "2025-04-04", "Spring Recess", "recess", "2024-25", "Spring 2025")
    add_event(events, "2025-05-13", "2025-05-13", "Study/Conference Day", "study_day", "2024-25", "Spring 2025")
    add_event(events, "2025-05-14", "2025-05-21", "Final Examinations", "exam_period", "2024-25", "Spring 2025")

    for dt, name in [
        ("2025-07-04", "Independence Day"),
        ("2025-09-01", "Labor Day"),
        ("2025-11-11", "Veteran’s Day"),
        ("2025-11-27", "Thanksgiving Holiday"),
        ("2025-11-28", "Rescheduled Holiday"),
        ("2025-12-25", "Christmas Holiday"),
        ("2026-01-01", "New Year’s Day"),
        ("2026-01-19", "Dr. Martin Luther King Jr. Day"),
        ("2026-03-31", "Cesar Chavez Day"),
        ("2026-05-25", "Memorial Day"),
        ("2026-06-19", "Juneteenth")
    ]:
        add_event(events, dt, dt, name, "holiday", "2025-26")

    add_event(events, "2025-11-26", "2025-11-26", "Non-Instructional Day", "non_instructional", "2025-26", "Fall 2025")
    add_event(events, "2025-12-09", "2025-12-09", "Study/Conference Day", "study_day", "2025-26", "Fall 2025")
    add_event(events, "2025-12-10", "2025-12-17", "Culminating Activities and Final Examinations", "exam_period", "2025-26", "Fall 2025")
    add_event(events, "2025-12-25", "2026-01-16", "Winter Recess", "recess", "2025-26")
    add_event(events, "2026-03-30", "2026-04-03", "Spring Recess", "recess", "2025-26", "Spring 2026")
    add_event(events, "2026-05-12", "2026-05-12", "Study/Conference Day", "study_day", "2025-26", "Spring 2026")
    add_event(events, "2026-05-13", "2026-05-20", "Culminating Activities and Final Examinations", "exam_period", "2025-26", "Spring 2026")

    return events

SEMESTER_RANGES = [
    ("2023-24", "Fall 2023", "2023-08-17", "2023-12-19", "2023-08-21", "2023-12-06"),
    ("2023-24", "Spring 2024", "2024-01-22", "2024-05-24", "2024-01-24", "2024-05-13"),
    ("2024-25", "Fall 2024", "2024-08-19", "2024-12-20", "2024-08-21", "2024-12-09"),
    ("2024-25", "Spring 2025", "2025-01-21", "2025-05-23", "2025-01-23", "2025-05-12"),
    ("2025-26", "Fall 2025", "2025-08-18", "2025-12-19", "2025-08-20", "2025-12-08"),
    ("2025-26", "Spring 2026", "2026-01-20", "2026-05-22", "2026-01-22", "2026-05-11"),
]

SEMESTER_RANGES = [
    (ay, sem, pd.to_datetime(start).date(), pd.to_datetime(end).date(),
     pd.to_datetime(instr_start).date(), pd.to_datetime(instr_end).date())
    for ay, sem, start, end, instr_start, instr_end in SEMESTER_RANGES
]

def infer_academic_year(d):
    if date(2023, 7, 1) <= d <= date(2024, 6, 30):
        return "2023-24"
    if date(2024, 7, 1) <= d <= date(2025, 6, 30):
        return "2024-25"
    if date(2025, 7, 1) <= d <= date(2026, 6, 30):
        return "2025-26"
    return None

def infer_semester(d):
    for ay, sem, start, end, instr_start, instr_end in SEMESTER_RANGES:
        if start <= d <= end:
            return sem
    return "Break/Recess"

def in_instruction_window(d):
    for ay, sem, start, end, instr_start, instr_end in SEMESTER_RANGES:
        if instr_start <= d <= instr_end:
            return True
    return False

def build_clean_hours(hours):
    h = hours.copy()
    h["week_start_date"] = pd.to_datetime(h["week_start_date"]).dt.date

    h["_original_order"] = range(len(h))

    h = h.sort_values(["week_start_date", "_original_order"]).reset_index(drop=True)

    h["day_offset"] = h.groupby("week_start_date").cumcount()

    h["calendar_date"] = h.apply(
        lambda r: r["week_start_date"] + timedelta(days=int(r["day_offset"])),
        axis=1
    )

    h = h[
        (h["calendar_date"] >= START_DATE) &
        (h["calendar_date"] <= END_DATE)
    ].copy()

    parsed = h["hours_text"].apply(parse_hours_text)
    parsed.columns = [
        "library_open_time",
        "library_close_time",
        "library_hours_open",
        "is_library_closed"
    ]

    h = pd.concat([h, parsed], axis=1)

    return h[
        [
            "calendar_date",
            "hours_text",
            "library_open_time",
            "library_close_time",
            "library_hours_open",
            "is_library_closed"
        ]
    ].rename(columns={"hours_text": "library_hours_text"})

def build_dim_date(hours):
    date_df = pd.DataFrame({
        "calendar_date": pd.date_range(START_DATE, END_DATE, freq="D")
    })

    date_df["calendar_date"] = date_df["calendar_date"].dt.date
    ts = pd.to_datetime(date_df["calendar_date"])

    date_df["date_key"] = ts.dt.strftime("%Y%m%d").astype(int)
    date_df["year"] = ts.dt.year
    date_df["month"] = ts.dt.month
    date_df["month_name"] = ts.dt.month_name()
    date_df["day"] = ts.dt.day
    date_df["day_of_week"] = ts.dt.dayofweek + 1
    date_df["day_name"] = ts.dt.day_name()
    date_df["week_start_date"] = (
        ts - pd.to_timedelta((ts.dt.dayofweek + 1) % 7, unit="D")
    ).dt.date
    date_df["week_of_year"] = ts.dt.isocalendar().week.astype(int)
    date_df["quarter"] = ts.dt.quarter
    date_df["is_weekend"] = date_df["day_of_week"].isin([6, 7])

    date_df["academic_year"] = date_df["calendar_date"].apply(infer_academic_year)
    date_df["semester"] = date_df["calendar_date"].apply(infer_semester)

    event_rows = []

    for ev in build_academic_events():
        current = ev["start_date"]

        while current <= ev["end_date"]:
            event_rows.append({
                "calendar_date": current,
                "academic_event_name": ev["academic_event_name"],
                "academic_event_type": ev["academic_event_type"]
            })
            current += timedelta(days=1)

    ev_df = pd.DataFrame(event_rows)

    ev_agg = ev_df.groupby("calendar_date").agg({
        "academic_event_name": lambda x: "; ".join(dict.fromkeys(x)),
        "academic_event_type": lambda x: "; ".join(dict.fromkeys(x))
    }).reset_index()

    date_df = date_df.merge(ev_agg, on="calendar_date", how="left")

    etype = date_df["academic_event_type"].fillna("")

    date_df["is_holiday"] = etype.str.contains("holiday")
    date_df["is_recess"] = etype.str.contains("recess")
    date_df["is_exam_period"] = etype.str.contains("exam_period")
    date_df["is_study_day"] = etype.str.contains("study_day")

    non_instructional = etype.str.contains(
        "holiday|recess|study_day|non_instructional|exam_period"
    )

    date_df["is_instruction_day"] = (
        date_df["calendar_date"].apply(in_instruction_window)
        & ~date_df["is_weekend"]
        & ~non_instructional
    )

    hours_clean = build_clean_hours(hours)
    date_df = date_df.merge(hours_clean, on="calendar_date", how="left")

    return date_df[
        [
            "date_key",
            "calendar_date",
            "year",
            "month",
            "month_name",
            "day",
            "day_of_week",
            "day_name",
            "week_start_date",
            "week_of_year",
            "quarter",
            "is_weekend",
            "academic_year",
            "semester",
            "is_instruction_day",
            "is_holiday",
            "is_recess",
            "is_exam_period",
            "is_study_day",
            "academic_event_name",
            "academic_event_type",
            "library_hours_text",
            "library_open_time",
            "library_close_time",
            "library_hours_open",
            "is_library_closed"
        ]
    ]

def build_fact_bookings(bookings, dim_room):
    b = bookings.copy()

    b["start_datetime"] = pd.to_datetime(b["from"], errors="coerce")
    b["end_datetime"] = pd.to_datetime(b["to"], errors="coerce")

    b = b.dropna(subset=["start_datetime", "end_datetime"]).copy()

    b = b.drop_duplicates(
    subset=["start_datetime", "end_datetime", "itemId", "nickname"],
    keep="first"
    ).copy()

    b["duration_minutes"] = (
        (b["end_datetime"] - b["start_datetime"]).dt.total_seconds() / 60
    ).round().astype(int)

    b["duration_hours"] = (b["duration_minutes"] / 60).round(2)
    b["booking_count"] = 1

    b["date_key"] = b["start_datetime"].dt.strftime("%Y%m%d").astype(int)
    b["start_time_key"] = b["start_datetime"].dt.strftime("%H%M").astype(int)
    b["end_time_key"] = b["end_datetime"].dt.strftime("%H%M").astype(int)

    b["booking_name"] = b["nickname"].apply(clean_text)

    room_lookup = dim_room[["room_key", "item_id"]].drop_duplicates()

    b = b.merge(
        room_lookup,
        left_on="itemId",
        right_on="item_id",
        how="left"
    )

    b = b.sort_values(
        ["start_datetime", "end_datetime", "itemId", "booking_name"]
    ).reset_index(drop=True)

    b.insert(0, "booking_key", range(1, len(b) + 1))

    out = b[
        [
            "booking_key",
            "date_key",
            "start_time_key",
            "end_time_key",
            "room_key",
            "booking_name",
            "start_datetime",
            "end_datetime",
            "duration_minutes",
            "duration_hours",
            "booking_count"
        ]
    ].copy()

    out["start_datetime"] = out["start_datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
    out["end_datetime"] = out["end_datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")

    

    return out

def main():
    bookings = pd.read_csv(BOOKINGS_FILE)
    hours = pd.read_csv(HOURS_FILE)

    if Path(CALENDAR_RAW_FILE).exists():
        cal_raw = pd.read_csv(CALENDAR_RAW_FILE)
        print(f"Academic calendar raw rows available: {len(cal_raw):,}")

    print(f"Raw bookings rows: {len(bookings):,}")
    print(f"Raw hours rows: {len(hours):,}")

    dim_time = build_dim_time()
    dim_room = build_dim_room(bookings)
    dim_date = build_dim_date(hours)
    fact_bookings = build_fact_bookings(bookings, dim_room)

    dim_time.to_csv(DIM_TIME_OUT, index=False, encoding="utf-8-sig")
    dim_room.to_csv(DIM_ROOM_OUT, index=False, encoding="utf-8-sig")
    dim_date.to_csv(DIM_DATE_OUT, index=False, encoding="utf-8-sig")
    fact_bookings.to_csv(FACT_BOOKINGS_OUT, index=False, encoding="utf-8-sig")

    print("\nTransformation complete.")
    print(f"dim_time rows: {len(dim_time):,} -> {DIM_TIME_OUT}")
    print(f"dim_room rows: {len(dim_room):,} -> {DIM_ROOM_OUT}")
    print(f"dim_date rows: {len(dim_date):,} -> {DIM_DATE_OUT}")
    print(f"fact_bookings rows: {len(fact_bookings):,} -> {FACT_BOOKINGS_OUT}")

if __name__ == "__main__":
    main()