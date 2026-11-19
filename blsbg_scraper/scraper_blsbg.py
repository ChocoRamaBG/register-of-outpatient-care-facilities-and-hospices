#!/usr/bin/env python3
"""Polite, restartable HTML scraper for the public BLSBG regional directory.

Uses one HTTP session instead of Chrome. Keeps the original Excel schema and
checkpoint filenames. It does NOT skip pages when the site is unresponsive.

Dependencies: pip install requests beautifulsoup4 pandas openpyxl

Environment overrides:
  BLS_START_REGION=1       Region to use when not resuming (1..29)
  BLS_FORCE_START=1        Ignore resume_state.txt (keep existing Excel records)
  BLS_PAGE_DELAY=2.0       Seconds between successfully fetched pages
"""

import os
import random
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup


SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = SCRIPT_DIR / "blsbg_outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = OUTPUT_DIR / "bg_medics_dynamic_2029.xlsx"  # Preserve existing name.
PROCESSED_UIN_FILE = OUTPUT_DIR / "processed_blsbg_uin_codes.txt"
RESUME_STATE_FILE = OUTPUT_DIR / "resume_state.txt"
CONTINUE_FLAG_FILE = OUTPUT_DIR / "CONTINUE_FLAG_BLSBG"
LAST_ERROR_FILE = OUTPUT_DIR / "last_blsbg_error.txt"

START_REGION = int(os.getenv("BLS_START_REGION", "1"))
LAST_REGION = 29
FORCE_START = os.getenv("BLS_FORCE_START", "0") == "1"
PAGE_DELAY = max(1.0, float(os.getenv("BLS_PAGE_DELAY", "2.0")))
USER_AGENT = os.getenv("BLS_USER_AGENT", "BLSBG-Public-Directory-Reader/1.0")
PAGE_SIZE = 25
SAVE_EVERY_N_PAGES = 10
MAX_RUNTIME_SECONDS = 20400
SAFETY_BUFFER_SECONDS = 180
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 60
MAX_PAGE_ATTEMPTS = 5
BACKOFF_SECONDS = (20, 45, 90, 150)

COLUMNS = [
    "Region Code", "Region Name", "UIN", "Address (Hidden)",
    "G Address (Hidden)", "Phone", "Workplace", "Specialty (Hidden)",
    "Name", "Specialty (Visible)", "Source URL", "Summary Info",
]

NO_RESULTS_BG = "\u041d\u044f\u043c\u0430 \u043d\u0430\u043c\u0435\u0440\u0435\u043d\u0438"
REGION_HEADER_BG = (
    "\u0421\u043f\u0438\u0441\u044a\u043a \u0441 \u0447\u043b\u0435\u043d\u043e\u0432\u0435"
    " - \u0420\u041a \u043d\u0430 \u0411\u041b\u0421 -"
)


class PageError(Exception):
    """HTTP or HTML does not contain a reliable requested page."""


class SiteAccessDenied(Exception):
    """The site explicitly refused access (do not repeatedly retry)."""


class RuntimeBudgetReached(Exception):
    """Stop while there is enough time left to save progress."""


@dataclass
class ParsedPage:
    rows: List[dict]
    is_last: bool
    first_uin: Optional[str]
    summary: str


def clean_text(value) -> str:
    if value is None:
        return "-"
    result = re.sub(r"[\x00-\x1f\x7f]+", "", str(value)).strip()
    return result or "-"


def clean_attribute(value) -> str:
    return clean_text(value).replace('\\"', '"')


def normalize_uin(value) -> str:
    """Keep UIN as text, including zeros lost by numeric Excel cells."""
    text = clean_text(value)
    if re.fullmatch(r"\d+\.0", text):
        text = text[:-2]
    if re.fullmatch(r"\d{1,10}", text):
        return text.zfill(10)
    return text


def atomic_text_write(path: Path, content: str) -> None:
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(content)
    os.replace(temporary, path)


def load_records() -> Dict[str, dict]:
    if not OUTPUT_FILE.exists():
        print("[INFO] No prior Excel dataset found; beginning with an empty dataset.")
        return {}

    # Do not silently discard old Excel data if reading it fails.
    df = pd.read_excel(OUTPUT_FILE, dtype=str, keep_default_na=False)
    data = {}
    for row in df.to_dict("records"):
        uin = normalize_uin(row.get("UIN", ""))
        if uin == "-":
            raise ValueError("Existing Excel contains an empty UIN; refusing to rewrite it.")
        row["UIN"] = uin
        if "Region Code" in row:
            region_code = clean_text(row["Region Code"])
            if region_code.isdigit():
                row["Region Code"] = region_code.zfill(2)
        data[uin] = row

    print(f"[INFO] Loaded {len(df)} Excel rows ({len(data)} unique UINs).")
    return data


def report_tracker_discrepancies(records: Dict[str, dict]) -> None:
    """The Excel file is authoritative; a stale tracker must not hide missing data."""
    if not PROCESSED_UIN_FILE.exists():
        return
    with PROCESSED_UIN_FILE.open(encoding="utf-8") as handle:
        tracked = {normalize_uin(line) for line in handle if line.strip()}
    extra = tracked - set(records)
    if extra:
        print(
            f"[WARN] Tracking file has {len(extra)} UINs missing from Excel. "
            "They will NOT be skipped. The tracker is rebuilt at checkpoints."
        )


def get_start_position() -> Tuple[int, int]:
    if not 1 <= START_REGION <= LAST_REGION:
        raise ValueError("BLS_START_REGION must be between 1 and 29.")
    if FORCE_START:
        print(f"[INFO] Forced fresh traversal from Region {START_REGION:02d}, Page 1.")
        return START_REGION, 1
    if RESUME_STATE_FILE.exists():
        raw = RESUME_STATE_FILE.read_text(encoding="utf-8").strip()
        try:
            a, b = raw.split(",", 1)
            region, page = int(a), int(b)
            if not (1 <= region <= LAST_REGION and page >= 1):
                raise ValueError("out of range")
            print(f"[INFO] Resuming Region {region:02d}, Page {page}.")
            return region, page
        except ValueError as exc:
            raise ValueError(
                f"Invalid {RESUME_STATE_FILE}: {raw!r}. "
                "Fix it, delete it, or set BLS_FORCE_START=1."
            ) from exc
    return START_REGION, 1


def save_checkpoint(records: Dict[str, dict], next_position: Optional[Tuple[int, int]]) -> None:
    """Publish Excel first, then tracking set, then the matching resume position."""
    df = pd.DataFrame(list(records.values()))
    for column in COLUMNS:
        if column not in df.columns:
            df[column] = ""
    ordered = COLUMNS + [col for col in df.columns if col not in COLUMNS]
    df = df[ordered]

    # Using .xlsx suffix is required by pandas' Excel writer.
    temporary_excel = OUTPUT_FILE.with_name(OUTPUT_FILE.stem + ".writing.xlsx")
    df.to_excel(temporary_excel, index=False)
    os.replace(temporary_excel, OUTPUT_FILE)

    atomic_text_write(PROCESSED_UIN_FILE, "".join(f"{uin}\n" for uin in records))
    if next_position is None:
        RESUME_STATE_FILE.unlink(missing_ok=True)
    else:
        region, page = next_position
        atomic_text_write(RESUME_STATE_FILE, f"{region},{page}")
    print(f"[SAVE] Excel checkpoint: {len(records)} UINs." + (
        " Completed all regions." if next_position is None
        else f" Resume: {next_position[0]:02d}, page {next_position[1]}."
    ))


def parse_summary(text: str) -> Tuple[int, int, int]:
    # Examples: 'Showing 1-25 of 845 results.' / '... 1-25 \u043e\u0442 11290.'
    pattern = r"(\d+)\s*[-\u2013]\s*(\d+)\s*(?:\u043e\u0442|of)\s*([\d\s\u00a0]+)"
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if not match:
        raise PageError(f"Missing or changed pagination summary: {text[:150]!r}")
    first, last = int(match.group(1)), int(match.group(2))
    total = int(re.sub(r"\D", "", match.group(3)))
    if first < 1 or last < first or total < last:
        raise PageError(f"Inconsistent pagination summary: {text[:150]!r}")
    return first, last, total


def parse_page(content: bytes, region_code: str, page: int, url: str) -> ParsedPage:
    soup = BeautifulSoup(content, "html.parser")
    heading = soup.select_one("h2.light.bordered")
    region_name = (clean_text(heading.get_text(" ", strip=True))
                   .replace(REGION_HEADER_BG, "").strip() if heading else "")
    region_name = region_name or f"Region {region_code}"

    summary_node = soup.select_one("div.summary")
    summary = clean_text(summary_node.get_text(" ", strip=True)) if summary_node else "-"
    table_rows = soup.select("table.items tbody tr")
    if table_rows and all(
        "empty" in tr.get("class", []) or NO_RESULTS_BG in tr.get_text(" ", strip=True)
        or "No results found" in tr.get_text(" ", strip=True)
        for tr in table_rows
    ):
        if page == 1:
            return ParsedPage([], True, None, summary)
        raise PageError("Unexpected empty table on a later page.")
    if not table_rows:
        # Only an EXPLICIT no-results notice on the first page means empty region.
        body_text = soup.get_text(" ", strip=True)
        if page == 1 and (NO_RESULTS_BG in body_text or "No results found" in body_text):
            return ParsedPage([], True, None, summary)
        raise PageError("No table rows found (possibly a timeout, error page, or changed HTML).")

    if summary == "-":
        raise PageError("Table was found, but pagination summary is missing.")
    first_number, last_number, total = parse_summary(summary)
    expected_first = (page - 1) * PAGE_SIZE + 1
    if first_number != expected_first:
        raise PageError(
            f"Requested page {page} but page summary begins at {first_number}; "
            f"expected {expected_first}. Server may have returned a stale page."
        )

    entries = []
    for tr in table_rows:
        if "empty" in tr.get("class", []) or NO_RESULTS_BG in tr.get_text(" ", strip=True):
            continue
        cells = tr.select("td")
        if len(cells) < 4:
            raise PageError(f"A record row on page {page} has only {len(cells)} columns.")
        uin = normalize_uin(cells[0].get_text(" ", strip=True))
        if not re.fullmatch(r"\d{10}", uin):
            raise PageError(f"Invalid or absent UIN in page {page}: {uin!r}")

        img = tr.select_one("img.expand")
        attrs = img.attrs if img else {}
        entries.append({
            "Region Code": region_code,
            "Region Name": clean_text(region_name),
            "UIN": uin,
            "Address (Hidden)": clean_attribute(attrs.get("adr")),
            "G Address (Hidden)": clean_attribute(attrs.get("gadr")),
            "Phone": clean_attribute(attrs.get("tel")),
            "Workplace": clean_attribute(attrs.get("wrk")),
            "Specialty (Hidden)": clean_attribute(attrs.get("spec")),
            "Name": clean_text(cells[2].get_text(" ", strip=True)),
            "Specialty (Visible)": clean_text(cells[3].get_text(" ", strip=True)),
            "Source URL": url,
            "Summary Info": summary,
        })

    expected_count = last_number - first_number + 1
    if len(entries) != expected_count:
        raise PageError(
            f"Partial page: parsed {len(entries)} rows but summary expects "
            f"{expected_count} (page {page})."
        )
    if len({item["UIN"] for item in entries}) != len(entries):
        raise PageError(f"Duplicate UINs found within page {page}.")
    return ParsedPage(entries, last_number >= total, entries[0]["UIN"], summary)


def seconds_available(start_clock: float) -> float:
    return MAX_RUNTIME_SECONDS - SAFETY_BUFFER_SECONDS - (time.monotonic() - start_clock)


def fetch_page(
    session: requests.Session, region: int, page: int, start_clock: float
) -> Tuple[ParsedPage, float]:
    region_code = f"{region:02d}"
    url = f"https://blsbg.eu/bg/medics/unionlist/{region_code}?UIN_page={page}"
    last_error = "Unknown error"
    for attempt in range(1, MAX_PAGE_ATTEMPTS + 1):
        if seconds_available(start_clock) < CONNECT_TIMEOUT + READ_TIMEOUT + 30:
            raise RuntimeBudgetReached("Not enough time for another safe HTTP attempt.")
        response = None
        started = time.monotonic()
        try:
            response = session.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            if response.status_code in (401, 403):
                raise SiteAccessDenied(f"HTTP {response.status_code} at {url}")
            if response.status_code != 200:
                raise PageError(f"HTTP {response.status_code} at {url}")
            parsed = parse_page(response.content, region_code, page, url)
            return parsed, time.monotonic() - started
        except SiteAccessDenied:
            raise
        except (requests.RequestException, PageError) as exc:
            last_error = str(exc)
            if attempt == MAX_PAGE_ATTEMPTS:
                break
            delay = BACKOFF_SECONDS[attempt - 1] + random.uniform(0, 4)
            if response is not None and response.status_code == 429:
                retry_after = response.headers.get("Retry-After", "")
                if retry_after.isdigit():
                    delay = max(delay, min(int(retry_after), 300))
            if seconds_available(start_clock) < delay + CONNECT_TIMEOUT + READ_TIMEOUT + 30:
                raise RuntimeBudgetReached("Run budget near limit during retry/backoff.")
            print(
                f"[RETRY] Region {region_code}, Page {page}, attempt "
                f"{attempt}/{MAX_PAGE_ATTEMPTS}: {last_error[:180]}. "
                f"Pausing {delay:.0f}s."
            )
            time.sleep(delay)
    raise PageError(
        f"Region {region_code}, Page {page} failed after {MAX_PAGE_ATTEMPTS} "
        f"attempts. Last error: {last_error}"
    )


def main() -> int:
    start_clock = time.monotonic()
    print("[INFO] Initializing HTTP-based BLSBG scraper (no Selenium/Chrome).")
    records = load_records()
    report_tracker_discrepancies(records)
    start_region, start_page = get_start_position()
    CONTINUE_FLAG_FILE.unlink(missing_ok=True)
    LAST_ERROR_FILE.unlink(missing_ok=True)

    session = requests.Session()  # Connection reuse; do not run concurrent requests.
    session.headers.update({
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "bg-BG,bg;q=0.9",
        "User-Agent": USER_AGENT,
    })

    pending_pages = 0
    current = (start_region, start_page)
    try:
        for region in range(start_region, LAST_REGION + 1):
            region_code = f"{region:02d}"
            page = current[1] if region == start_region else 1
            print(f"\n[REGION] {region_code} / 29 (starting page {page})")
            previous_first_uin = None
            while True:
                current = (region, page)
                parsed, duration = fetch_page(session, region, page, start_clock)
                if parsed.first_uin and parsed.first_uin == previous_first_uin:
                    raise PageError(
                        f"Page {page} repeats the previous page's first UIN. "
                        "Stopping rather than silently skipping records."
                    )
                previous_first_uin = parsed.first_uin

                new_count = 0
                for row in parsed.rows:
                    if row["UIN"] not in records:
                        records[row["UIN"]] = row
                        new_count += 1
                print(
                    f"[PAGE] {region_code}/{page}: {len(parsed.rows)} rows, "
                    f"{new_count} new, HTTP {duration:.1f}s, "
                    f"total unique {len(records)}."
                )
                pending_pages += 1
                if parsed.is_last:
                    next_position = (region + 1, 1) if region < LAST_REGION else None
                    save_checkpoint(records, next_position)
                    pending_pages = 0
                    print(f"[REGION DONE] {region_code}")
                    if region < LAST_REGION and seconds_available(start_clock) > PAGE_DELAY + 30:
                        time.sleep(PAGE_DELAY)
                    break

                page += 1
                current = (region, page)
                if pending_pages >= SAVE_EVERY_N_PAGES:
                    save_checkpoint(records, current)
                    pending_pages = 0
                if seconds_available(start_clock) < CONNECT_TIMEOUT + READ_TIMEOUT + 30:
                    raise RuntimeBudgetReached("Run budget exhausted between pages.")
                time.sleep(PAGE_DELAY)

        print(f"[SUCCESS] Completed all 29 regions. {len(records)} unique UINs saved.")
        return 0

    except RuntimeBudgetReached as exc:
        print(f"[PAUSED] {exc}")
        save_checkpoint(records, current)
        atomic_text_write(CONTINUE_FLAG_FILE, "CONTINUE_REQUIRED\n")
        print(f"[PAUSED] Continuation requested from Region {current[0]:02d}, Page {current[1]}.")
        return 0
    except (PageError, SiteAccessDenied, requests.RequestException) as exc:
        print(f"[STOPPED] {exc}")
        save_checkpoint(records, current)
        atomic_text_write(LAST_ERROR_FILE, f"{type(exc).__name__}: {exc}\n")
        print("[STOPPED] No pages were skipped. Re-run the workflow later to retry this page.")
        return 1
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Saving progress...")
        save_checkpoint(records, current)
        return 130
    finally:
        session.close()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError) as error:
        print(f"[FATAL] {error}", file=sys.stderr)
        sys.exit(2)
