"""Stage 4 — Push the shortlist to Google Sheets."""
import os, json, pandas as pd, gspread
from google.oauth2.service_account import Credentials
from datetime import date

SHEET_NAME = os.environ.get("SHEET_NAME", "Job Tracker")

# Load service account from env (GitHub Secret) or local file
creds_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT")
if creds_json:
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",],
        
    )
else:
    creds = Credentials.from_service_account_file(
        "service_account.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",],
    )

gc = gspread.authorize(creds)

try:
    sh = gc.open(SHEET_NAME)
except gspread.SpreadsheetNotFound:
    print(f"✗ Sheet '{SHEET_NAME}' not found.")
    print("  → Make sure the sheet exists AND is shared with your service account email.")
    exit(1)

ws = sh.sheet1

# Add headers if sheet is empty
existing = ws.get_all_values()
if not existing:
    ws.append_row([
        "Date", "Score", "Title", "Company", "Location",
        "Site", "Highlights", "Concerns", "Reason", "Link", "Status",
    ])
    print("✓ Created header row")

# Load shortlist
shortlist = pd.read_csv("output/shortlist.csv")

if shortlist.empty:
    print("⚠ Shortlist is empty — nothing to push today.")
    exit(0)

# Build rows
today    = str(date.today())
new_rows = []
existing_links = {row[9] for row in existing[1:] if len(row) > 9}  # column J = Link

skipped = 0
for _, job in shortlist.iterrows():
    link = str(job.get("job_url", ""))
    if link in existing_links:
        skipped += 1
        continue

    new_rows.append([
        today,
        int(job.get("score", 0)),
        str(job.get("title", "")),
        str(job.get("company", "")),
        str(job.get("location", "")),
        str(job.get("site", "")),
        str(job.get("highlights", "")),
        str(job.get("concerns", "")),
        str(job.get("reason", "")),
        link,
        "To Apply",
    ])

if new_rows:
    ws.append_rows(new_rows, value_input_option="USER_ENTERED")
    print(f"✓ Pushed {len(new_rows)} new jobs to '{SHEET_NAME}'")
else:
    print("ℹ All shortlisted jobs already in sheet")

if skipped:
    print(f"  ({skipped} duplicates skipped)")

print(f"\nSheet URL: {sh.url}")