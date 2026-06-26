
"""
Germany AI/Data Job Scraper — strict last-24-hours.

Sources:
  • LinkedIn   (via jobspy)        — native hours_old support
  • Indeed     (via jobspy)        — native hours_old support
  • Adzuna     (REST API + key)    — filtered by max_days_old + post-filter
  • Remotive   (open API)          — post-filtered by publication_date
  • Arbeitnow  (open API)          — post-filtered by created_at unix ts

Strategy:
  • LinkedIn + Indeed: sequential (they share a rate limiter)
  • Adzuna + Remotive + Arbeitnow: parallel (independent APIs)
  • 15s cooldown between search terms

Setup:
  1. pip install -r requirements.txt
  2. Create .env file with:
        ADZUNA_APP_ID=your_id
        ADZUNA_APP_KEY=your_key
  3. python scraper.py

Output: jobs_today.csv
"""

from dotenv import load_dotenv
load_dotenv()

import os
import time
import requests
import pandas as pd
import dateutil.parser
from datetime import date, datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from jobspy import scrape_jobs


# ── Config ────────────────────────────────────────────────────────────

SEARCH_TERMS = [
    "Data Scientist",
    "AI Engineer",
    "Data Engineer",
]

HOURS_WINDOW = 24    # change to 48 / 72 for wider window

ADZUNA_APP_ID  = os.environ.get("ADZUNA_APP_ID",  "")
ADZUNA_APP_KEY = os.environ.get("ADZUNA_APP_KEY", "")

CUTOFF = datetime.now(timezone.utc) - timedelta(hours=HOURS_WINDOW)


# ── Helpers ───────────────────────────────────────────────────────────

def is_recent(date_str: str) -> bool:
    """True if date is within HOURS_WINDOW. Keep job if date is unparseable."""
    if not date_str:
        return True
    try:
        dt = dateutil.parser.parse(str(date_str))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt >= CUTOFF
    except Exception:
        return True


def is_recent_unix(ts: int) -> bool:
    """True if Unix timestamp is within HOURS_WINDOW."""
    if not ts:
        return True
    try:
        dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
        return dt >= CUTOFF
    except Exception:
        return True


# ── Scrapers ──────────────────────────────────────────────────────────

def scrape_jobspy_sites(term: str) -> pd.DataFrame:
    """LinkedIn + Indeed via jobspy. Run together to respect their shared rate limiter."""
    try:
        df = scrape_jobs(
            site_name=["linkedin", "indeed"],
            search_term=term,
            location="Germany",
            results_wanted=50,
            hours_old=HOURS_WINDOW,
            country_indeed="Germany",
            linkedin_fetch_description=False,
            verbose=0,
        )
        if not df.empty and "site" in df.columns:
            for site, count in df["site"].value_counts().items():
                print(f"  ✓ {site}: {count} jobs for '{term}'")
        else:
            print(f"  ✗ LinkedIn/Indeed: 0 jobs for '{term}'")
        return df
    except Exception as e:
        print(f"  ✗ LinkedIn/Indeed error for '{term}': {e}")
        return pd.DataFrame()


def scrape_adzuna(term: str) -> pd.DataFrame:
    if not (ADZUNA_APP_ID and ADZUNA_APP_KEY):
        return pd.DataFrame()
    rows = []
    for page in range(1, 4):
        try:
            r = requests.get(
                f"https://api.adzuna.com/v1/api/jobs/de/search/{page}",
                params={
                    "app_id":           ADZUNA_APP_ID,
                    "app_key":          ADZUNA_APP_KEY,
                    "what":             term,
                    "results_per_page": 25,
                    "max_days_old":     1,
                    "sort_by":          "date",
                },
                timeout=15,
            )
            for j in r.json().get("results", []):
                if not is_recent(j.get("created", "")):
                    continue
                rows.append({
                    "title":       j.get("title", ""),
                    "company":     j.get("company", {}).get("display_name", ""),
                    "location":    j.get("location", {}).get("display_name", "Germany"),
                    "job_url":     j.get("redirect_url", ""),
                    "description": j.get("description", ""),
                    "date_posted": j.get("created", ""),
                    "site":        "adzuna",
                })
        except Exception as e:
            print(f"  ✗ Adzuna error: {e}")
    print(f"  ✓ Adzuna: {len(rows)} jobs for '{term}'")
    return pd.DataFrame(rows)


def scrape_remotive(term: str) -> pd.DataFrame:
    try:
        r = requests.get(
            "https://remotive.com/api/remote-jobs",
            params={"search": term, "limit": 50},
            timeout=15,
        )
        rows = []
        for j in r.json().get("jobs", []):
            if not is_recent(j.get("publication_date", "")):
                continue
            loc = j.get("candidate_required_location", "") or ""
            if any(x in loc for x in ["Germany", "Europe", "Worldwide", ""]):
                rows.append({
                    "title":       j.get("title", ""),
                    "company":     j.get("company_name", ""),
                    "location":    loc or "Remote",
                    "job_url":     j.get("url", ""),
                    "description": (j.get("description", "") or "")[:1500],
                    "date_posted": j.get("publication_date", ""),
                    "site":        "remotive",
                })
        print(f"  ✓ Remotive: {len(rows)} jobs for '{term}'")
        return pd.DataFrame(rows)
    except Exception as e:
        print(f"  ✗ Remotive error: {e}")
        return pd.DataFrame()


def scrape_arbeitnow(term: str) -> pd.DataFrame:
    try:
        r = requests.get(
            "https://www.arbeitnow.com/api/job-board-api",
            params={"search": term},
            timeout=15,
        )
        rows = []
        for j in r.json().get("data", []):
            if not is_recent_unix(j.get("created_at", 0)):
                continue
            rows.append({
                "title":       j.get("title", ""),
                "company":     j.get("company_name", ""),
                "location":    j.get("location", "Germany"),
                "job_url":     j.get("url", ""),
                "description": (j.get("description", "") or "")[:1500],
                "date_posted": str(j.get("created_at", "")),
                "site":        "arbeitnow",
            })
        print(f"  ✓ Arbeitnow: {len(rows)} jobs for '{term}'")
        return pd.DataFrame(rows)
    except Exception as e:
        print(f"  ✗ Arbeitnow error: {e}")
        return pd.DataFrame()


# ── Pipeline ──────────────────────────────────────────────────────────

def process_term(term: str) -> list:
    """For each term:
       1. LinkedIn + Indeed sequentially (shared rate limiter)
       2. Adzuna + Remotive + Arbeitnow in parallel (independent APIs)
    """
    print(f"\n[Scraping: '{term}']")
    results = []

    # Step 1: LinkedIn + Indeed
    results.append(scrape_jobspy_sites(term))

    # Step 2: independent APIs in parallel
    independent = [scrape_adzuna, scrape_remotive, scrape_arbeitnow]
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {pool.submit(api, term): api.__name__ for api in independent}
        for fut in as_completed(futures):
            results.append(fut.result())

    return results


def main() -> None:
    start = time.time()

    print(f"⏱ Cutoff: jobs posted after {CUTOFF.strftime('%Y-%m-%d %H:%M UTC')} ({HOURS_WINDOW}h window)")
    if not (ADZUNA_APP_ID and ADZUNA_APP_KEY):
        print("⚠ Adzuna keys not set in .env — skipping Adzuna")

    all_jobs = []

    for i, term in enumerate(SEARCH_TERMS):
        all_jobs.extend(process_term(term))
        if i < len(SEARCH_TERMS) - 1:
            print("  ⏳ Cooling down 15s before next term...")
            time.sleep(15)

    # Combine
    combined = pd.concat(all_jobs, ignore_index=True)

    # Drop empty titles
    combined = combined[combined["title"].notna() & (combined["title"] != "")]

    # Deduplicate by (title, company) — fall back to title alone if no company
    combined["company"] = combined["company"].fillna("").astype(str)
    combined.drop_duplicates(subset=["title", "company"], keep="first", inplace=True)

    # Also dedupe by URL (safety net for cross-source duplicates)
    if "job_url" in combined.columns:
        combined = combined[combined["job_url"].notna() & (combined["job_url"] != "")]
        combined.drop_duplicates(subset=["job_url"], keep="first", inplace=True)

    combined["scraped_date"] = str(date.today())

    out_path = "jobs_today.csv"
    combined.to_csv(out_path, index=False)

    elapsed = int(time.time() - start)
    print(f"\n{'─' * 50}")
    print(f"✓ {len(combined)} unique jobs saved → {out_path}  ({elapsed}s)")

    if "site" in combined.columns and not combined.empty:
        print("\nBreakdown by site:")
        print(combined.groupby("site")["title"].count().sort_values(ascending=False).to_string())


if __name__ == "__main__":
    main()







# """
# Germany AI/Data Job Scraper
# Scrapes LinkedIn, Indeed, StepStone, and Bundesagentur für Arbeit.
# Searches both "Germany" and "Deutschland" location strings to maximise coverage.
# Output: jobs_today.csv

# Install deps first:
#     pip install jobspy pandas requests
# """

# from jobspy import scrape_jobs
# import pandas as pd
# import requests
# import xml.etree.ElementTree as ET
# import time
# from datetime import date

# # ── Search terms ─────────────────────────────────────────────
# # SEARCH_TERMS = [
# #     # General AI / ML
# #     "Artificial Intelligence Engineer",
# #     "AI Engineer",
# #     "Machine Learning Engineer",
# #     "ML Engineer",
# #     "Deep Learning Engineer",
# #     "Generative AI Engineer",
# #     "LLM Engineer",
# #     "Applied AI Engineer",
# #     "AI Research Engineer",
# #     "AI Research Scientist",
# #     "AI Scientist",
# #     "AI Developer",
# #     "AI Software Engineer",
# #     "AI Architect",
# #     "AI Consultant",
# #     # Data Science
# #     "Data Scientist",
# #     "Senior Data Scientist",
# #     "Junior Data Scientist",
# #     "Lead Data Scientist",
# #     "Principal Data Scientist",
# #     "Applied Scientist",
# #     "Research Scientist",
# #     "Quantitative Data Scientist",
# #     # Data Analysis
# #     "Data Analyst",
# #     "Business Data Analyst",
# #     "BI Analyst",
# #     "Business Intelligence Analyst",
# #     "Analytics Engineer",
# #     "Product Analyst",
# #     "Marketing Analyst",
# #     "Financial Analyst",
# #     "Reporting Analyst",
# #     # Data Engineering
# #     "Data Engineer",
# #     "Big Data Engineer",
# #     "ETL Developer",
# #     "ETL Engineer",
# #     "Data Platform Engineer",
# #     "Data Warehouse Engineer",
# #     "Cloud Data Engineer",
# #     "Pipeline Engineer",
# #     # Machine Learning Specializations
# #     "Machine Learning Scientist",
# #     "Machine Learning Researcher",
# #     "MLOps Engineer",
# #     "ML Ops Engineer",
# #     "AI Ops Engineer",
# #     "Model Engineer",
# #     "Inference Engineer",
# #     "Recommendation Systems Engineer",
# #     # NLP / LLM
# #     "NLP Engineer",
# #     "Natural Language Processing Engineer",
# #     "Computational Linguist",
# #     "LLM Developer",
# #     "Prompt Engineer",
# #     "Conversational AI Engineer",
# #     "Chatbot Developer",
# #     # Computer Vision
# #     "Computer Vision Engineer",
# #     "Computer Vision Scientist",
# #     "Image Processing Engineer",
# #     "Vision AI Engineer",
# #     # Robotics / Autonomous Systems
# #     "Robotics Engineer",
# #     "Autonomous Systems Engineer",
# #     "Perception Engineer",
# #     # Deep Learning
# #     "Deep Learning Researcher",
# #     "Neural Network Engineer",
# #     # AI Infrastructure
# #     "AI Infrastructure Engineer",
# #     "GPU Engineer",
# #     "Distributed ML Engineer",
# #     # Statistics / Quant
# #     "Statistician",
# #     "Quantitative Analyst",
# #     "Quant Analyst",
# #     "Decision Scientist",
# #     # Cloud + AI
# #     "Azure AI Engineer",
# #     "AWS Machine Learning Engineer",
# #     "GCP AI Engineer",
# #     # Specialized Domains
# #     "Fraud Detection Scientist",
# #     "Recommendation Engineer",
# #     "Speech Engineer",
# #     "Speech Recognition Engineer",
# #     "Audio ML Engineer",
# #     "Time Series Forecasting Engineer",
# #     # Entry-level / internship
# #     "Junior Machine Learning Engineer",
# #     "Junior Data Scientist",
# #     # Related software roles
# #     "Python Developer",
# #     "Backend AI Engineer",
# #     "Software Engineer AI",
# #     "AI Platform Engineer",
# # ]

# SEARCH_TERMS = [
#     "Data Scientist",
#     "AI Engineer",
#     "Data Engineer",
# ]

# # Location strings to try for LinkedIn/Indeed and StepStone
# LOCATIONS = ["Germany", "Deutschland"]


# # ── Helpers ───────────────────────────────────────────────────
# def scrape_stepstone(keyword: str, limit_per_location: int = 30) -> pd.DataFrame:
#     """Query StepStone with both 'Deutschland' and 'Germany' location strings,
#     deduplicating by job URL across both queries."""
#     kw = keyword.replace(" ", "+")
#     headers = {
#         "User-Agent": (
#             "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#             "AppleWebKit/537.36 (KHTML, like Gecko) "
#             "Chrome/124.0.0.0 Safari/537.36"
#         ),
#         "Accept-Language": "de-DE,de;q=0.9",
#         "Accept": "application/rss+xml, application/xml, text/xml, */*",
#     }
#     rows = []
#     seen_urls: set = set()

#     for where in LOCATIONS:
#         url = (
#             f"https://www.stepstone.de/5/ergebnisliste.html"
#             f"?what={kw}&where={where}&rssfeeds=1"
#         )
#         try:
#             r = requests.get(url, headers=headers, timeout=30)
#             r.raise_for_status()
#             root = ET.fromstring(r.content)
#             new_count = 0
#             for item in root.iter("item"):
#                 job_url = item.findtext("link", "")
#                 if job_url in seen_urls:
#                     continue
#                 seen_urls.add(job_url)
#                 rows.append({
#                     "title":       item.findtext("title", ""),
#                     "job_url":     job_url,
#                     "description": item.findtext("description", ""),
#                     "date_posted": item.findtext("pubDate", ""),
#                     "company":     "",
#                     "location":    "Germany",
#                     "site":        "stepstone",
#                 })
#                 new_count += 1
#                 if new_count >= limit_per_location:
#                     break
#             print(f"    → {new_count} new jobs from StepStone [{where}]")
#         except requests.exceptions.Timeout:
#             print(f"    ✗ StepStone timed out [{where}] — skipping")
#         except ET.ParseError:
#             print(f"    ✗ StepStone returned non-XML [{where}] — may be blocking, skipping")
#         except Exception as e:
#             print(f"    ✗ StepStone error [{where}]: {e}")

#     return pd.DataFrame(rows)


# def scrape_arbeitsagentur(keyword: str, size: int = 50) -> pd.DataFrame:
#     """Bundesagentur für Arbeit — single national endpoint, no location variant needed."""
#     url = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v3/jobs"
#     params = {
#         "was":         keyword,
#         "size":        size,
#         "page":        0,
#         "angebotsart": 1,  # full-time jobs
#     }
#     headers = {
#         "X-API-Key":  "jobboerse-jobsuche-ui",
#         "User-Agent": "Mozilla/5.0",
#         "Accept":     "application/json",
#     }
#     try:
#         r = requests.get(url, params=params, headers=headers, timeout=15)
#         if r.status_code != 200:
#             print(f"    ✗ Arbeitsagentur HTTP {r.status_code}: {r.text[:120]}")
#             return pd.DataFrame()
#         jobs = r.json().get("stellenangebote", [])
#         rows = [
#             {
#                 "title":       j.get("titel", ""),
#                 "company":     j.get("arbeitgeber", ""),
#                 "location":    j.get("arbeitsort", {}).get("ort", "Germany"),
#                 "job_url":     (
#                     "https://www.arbeitsagentur.de/jobsuche/jobdetail/"
#                     + j.get("hashId", "")
#                 ),
#                 "description": j.get("stellenbeschreibung", "") or "",
#                 "site":        "arbeitsagentur",
#             }
#             for j in jobs
#         ]
#         print(f"    → {len(rows)} jobs from Arbeitsagentur")
#         return pd.DataFrame(rows)
#     except Exception as e:
#         print(f"    ✗ Arbeitsagentur error: {e}")
#         return pd.DataFrame()


# def check_endpoints() -> None:
#     print("Checking Arbeitsagentur endpoint…")
#     try:
#         r = requests.get(
#             "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v3/jobs",
#             params={"was": "test", "size": 1, "angebotsart": 1},
#             headers={"X-API-Key": "jobboerse-jobsuche-ui", "Accept": "application/json"},
#             timeout=10,
#         )
#         status = "✓ OK" if r.status_code == 200 else f"✗ {r.status_code}"
#         print(f"  Arbeitsagentur: {status}")
#     except Exception as e:
#         print(f"  Arbeitsagentur: ✗ {e}")


# # ── Main ──────────────────────────────────────────────────────
# def main() -> None:
#     check_endpoints()

#     all_jobs: list = []
#     total = len(SEARCH_TERMS)

#     for idx, term in enumerate(SEARCH_TERMS, 1):
#         print(f"\n[{idx}/{total}] '{term}'")

#         # LinkedIn + Indeed — try both "Germany" and "Deutschland"
#         for location in LOCATIONS:
#             try:
#                 jobs = scrape_jobs(
#                     site_name=["linkedin", "indeed"],
#                     search_term=term,
#                     location=location,
#                     results_wanted=50,
#                     hours_old=24,
#                     country_indeed="Germany",
#                     linkedin_fetch_description=True,
#                     verbose=0,
#                 )
#                 all_jobs.append(jobs)
#                 print(f"    → {len(jobs)} jobs from LinkedIn/Indeed [{location}]")
#             except Exception as e:
#                 print(f"    ✗ jobspy error [{location}]: {e}")
#             time.sleep(2)  # brief pause between location variants

#         # StepStone — both location strings handled inside the function
#         all_jobs.append(scrape_stepstone(term))

#         # Bundesagentur für Arbeit — national API, no location variant needed
#         all_jobs.append(scrape_arbeitsagentur(term))

#         # Polite delay before next search term
#         time.sleep(3)

#     # ── Combine & deduplicate ─────────────────────────────────
#     combined = pd.concat(all_jobs, ignore_index=True)
#     combined.drop_duplicates(subset=["title", "company"], keep="first", inplace=True)
#     combined = combined[combined["title"].notna() & (combined["title"] != "")]
#     combined["scraped_date"] = str(date.today())

#     out_path = "jobs_today.csv"
#     combined.to_csv(out_path, index=False)

#     print(f"\n{'─'*50}")
#     print(f"✓ {len(combined)} unique jobs saved → {out_path}")
#     print("\nBreakdown by site:")
#     if "site" in combined.columns:
#         print(combined.groupby("site")["title"].count().to_string())
#     else:
#         print("  (no 'site' column found — jobspy may use different column names)")


# if __name__ == "__main__":
#     main()
