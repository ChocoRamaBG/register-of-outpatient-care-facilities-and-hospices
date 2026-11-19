#!/usr/bin/env python3
"""Full BLS regional member scraper, without a 249-record / page cap.

Uses the regional MEMBER LIST (unionlist/2 .. unionlist/29), not DocSearch.
The search form can return only 249 entries per region; the member list
publishes the full region pagination (e.g. Sofia has over 11,000 entries).

Existing Excel, tracker and resume filenames are preserved. Sequential, polite
HTTP requests with rate limiting, bounded retries, and durable checkpoints.

Dependencies: requests beautifulsoup4 pandas openpyxl
Optional environment variables:
  BLS_START_REGION=1       1=Blagoevgrad, ... 28=Yambol, for a new scan
  BLS_FORCE_START=1        Ignore resume checkpoint; keep existing Excel rows
  BLS_PAGE_DELAY=2.0       Delay between successful pages (minimum 2 seconds)
  BLS_SAVE_EVERY=10        Excel checkpoint interval in pages
  BLS_REQUIRE_EXISTING_EXCEL=1  Stop if previously-collected XLSX is missing
"""

import json
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlsplit

import pandas as pd
import requests
from bs4 import BeautifulSoup

SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = SCRIPT_DIR / "blsbg_outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "bg_medics_dynamic_2029.xlsx"  # Keep historical filename.
PROCESSED_UIN_FILE = OUTPUT_DIR / "processed_blsbg_uin_codes.txt"
RESUME_STATE_FILE = OUTPUT_DIR / "resume_state.txt"
CONTINUE_FLAG_FILE = OUTPUT_DIR / "CONTINUE_FLAG_BLSBG"
LAST_ERROR_FILE = OUTPUT_DIR / "last_blsbg_error.txt"
DEBUG_HTML_FILE = SCRIPT_DIR / "blsbg_last_bad_response.html"

START_REGION = int(os.getenv("BLS_START_REGION", "1"))
FORCE_START = os.getenv("BLS_FORCE_START", "0") == "1"
REQUIRE_EXISTING_EXCEL = os.getenv("BLS_REQUIRE_EXISTING_EXCEL", "0") == "1"
PAGE_DELAY = max(2.0, float(os.getenv("BLS_PAGE_DELAY", "2.0")))
SAVE_EVERY_N_PAGES = max(1, int(os.getenv("BLS_SAVE_EVERY", "10")))
MAX_RUNTIME_SECONDS = 20400             # Stay under a six-hour GitHub Actions job.
SAFETY_BUFFER_SECONDS = 180
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 75
MAX_PAGE_ATTEMPTS = 5
BACKOFF_SECONDS = (20, 45, 90, 150)
CHECKPOINT_SOURCE = "blsbg-unionlist-v1"
BASE_URL = "https://www.blsbg.eu/bg/medics/unionlist"
ALT_BASE_URL = "https://blsbg.eu/bg/medics/unionlist"
USER_AGENT = os.getenv(
    "BLS_USER_AGENT",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
)

# Order from the BLS region dropdown and member directory. Each public member
# directory ID is the ordinal position PLUS ONE: BLG is /2, Yambol is /29.
REGION_INFO = (
    ("BLG", "Благоевград"), ("BGS", "Бургас"), ("VAR", "Варна"),
    ("VTR", "Велико Търново"), ("VID", "Видин"), ("VRC", "Враца"),
    ("GAB", "Габрово"), ("DOB", "Добрич"), ("KRZ", "Кърджали"),
    ("KNL", "Кюстендил"), ("LOV", "Ловеч"), ("MON", "Монтана"),
    ("PAZ", "Пазарджик"), ("PER", "Перник"), ("PVN", "Плевен"),
    ("PDV", "Пловдив"), ("RAZ", "Разград"), ("RSE", "Русе"),
    ("SLS", "Силистра"), ("SLV", "Сливен"), ("SML", "Смолян"),
    ("SOF", "София"), ("SFO", "София Област"), ("SZR", "Стара Загора"),
    ("TGV", "Търговище"), ("HKV", "Хасково"), ("SHU", "Шумен"),
    ("JAM", "Ямбол"),
)
COLUMNS = (
    "Region Code", "Region Name", "UIN", "Address (Hidden)",
    "G Address (Hidden)", "Phone", "Workplace", "Specialty (Hidden)",
    "Name", "Specialty (Visible)", "Source URL", "Summary Info",
)
SUMMARY_RE = re.compile(
    r"(\d+)\s*[-\u2013\u2014]\s*(\d+)\s*(?:от|of)\s*([\d\s\u00a0]+)",
    flags=re.I,
)
HEADER_BG = "Списък с членове - РК на БЛС -"


class PageError(Exception):
    """Page is missing, incomplete, mismatched or otherwise untrustworthy."""


class SiteAccessDenied(Exception):
    """Website has refused access; do not try to bypass this."""


class RuntimeBudgetReached(Exception):
    """Stop in time to save a continuation checkpoint."""


@dataclass(frozen=True)
class Region:
    index: int                     # 1 .. 28, sequential processing order
    id: int                        # 2 .. 29, numeric website directory ID
    code: str                      # BLG, BGS, etc.
    name: str                      # Expected heading


REGIONS = [Region(i, i + 1, *info) for i, info in enumerate(REGION_INFO, start=1)]


@dataclass
class ParsedPage:
    rows: List[dict]
    first: int
    last: int
    total: int
    is_last: bool


def clean_text(value) -> str:
    if value is None:
        return "-"
    return re.sub(r"[\x00-\x1f\x7f]+", "", str(value)).strip() or "-"


def clean_attribute(value) -> str:
    return clean_text(value).replace('\\"', '"')


def normalize_uin(value) -> str:
    text = clean_text(value)
    if re.fullmatch(r"\d+\.0", text):
        text = text[:-2]
    if re.fullmatch(r"\d{1,10}", text):
        return text.zfill(10)
    return text


def atomic_text_write(path: Path, text: str) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def load_records() -> Dict[str, dict]:
    if not OUTPUT_FILE.exists():
        if REQUIRE_EXISTING_EXCEL:
            raise ValueError(f"Historical Excel file missing: {OUTPUT_FILE}")
        if PROCESSED_UIN_FILE.exists() and PROCESSED_UIN_FILE.stat().st_size > 0:
            raise ValueError(
                "Excel is missing but the UIN tracker is nonempty. Restore the "
                "existing workbook before running, or move the old tracker aside "
                "if you intentionally want an entirely new dataset."
            )
        print("[WARN] No existing Excel found in this GitHub checkout. "
              "This run cannot preserve older data that is not checked in.")
        return {}
    df = pd.read_excel(OUTPUT_FILE, dtype=str, keep_default_na=False)
    records = {}
    for row in df.to_dict("records"):
        uin = normalize_uin(row.get("UIN", ""))
        if not re.fullmatch(r"\d{10}", uin):
            raise ValueError(f"Historical Excel contains invalid UIN {uin!r}; refusing to overwrite it.")
        row["UIN"] = uin
        region_code = clean_text(row.get("Region Code"))
        if region_code.isdigit():
            row["Region Code"] = region_code.zfill(2)
        records[uin] = row
    print(f"[INFO] Loaded {len(df)} Excel rows ({len(records)} unique UINs).")
    if PROCESSED_UIN_FILE.exists():
        tracked = {
            normalize_uin(v.strip())
            for v in PROCESSED_UIN_FILE.read_text(encoding="utf-8").splitlines()
            if v.strip()
        }
        if tracked - set(records):
            print(f"[WARN] {len(tracked - set(records))} tracked UINs are absent "
                  "from Excel; they will be scraped again if encountered.")
    return records


def load_start() -> Tuple[int, int, int]:
    """Old DocSearch checkpoints are not valid for the full member-list source."""
    if not 1 <= START_REGION <= len(REGIONS):
        raise ValueError(f"BLS_START_REGION must be between 1 and {len(REGIONS)}.")
    if FORCE_START:
        print(f"[INFO] Forced start: Region {START_REGION:02d}, Page 1; keeping Excel.")
        return START_REGION, 1, 1
    if not RESUME_STATE_FILE.exists():
        return START_REGION, 1, 1
    raw = RESUME_STATE_FILE.read_text(encoding="utf-8").strip()
    if raw.startswith("{"):
        try:
            state = json.loads(raw)
            if state.get("source") != CHECKPOINT_SOURCE:
                print("[MIGRATION] Old DocSearch checkpoint detected. "
                      "Restarting full directory scan at Region 01, Page 1; "
                      "existing Excel data stays intact.")
                return 1, 1, 1
            region, page = int(state["region"]), int(state["page"])
            expected = int(state["expected_first"])
            if not 1 <= region <= len(REGIONS) or page < 1 or expected < 1:
                raise ValueError("Invalid checkpoint region, page or row offset")
            if page == 1 and expected != 1:
                raise ValueError("Page 1 checkpoint must start at record 1")
            print(f"[INFO] Resuming Region {region:02d}, Page {page}, "
                  f"expected row {expected}.")
            return region, page, expected
        except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
            raise ValueError(f"Invalid unionlist resume checkpoint: {exc}") from exc
    # Old scraper wrote '1,1' after finishing; previous non-starting page
    # offsets are incompatible with the now-unlimited unionlist pagination.
    if raw == "1,1":
        return START_REGION, 1, 1
    print(f"[MIGRATION] Legacy checkpoint {raw!r} cannot be used for "
          "unionlist pages. Restarting at Region 01, Page 1, preserving Excel.")
    return 1, 1, 1


def save_checkpoint(records: Dict[str, dict], position: Optional[Tuple[int, int, int]]) -> None:
    df = pd.DataFrame(list(records.values()))
    for key in COLUMNS:
        if key not in df.columns:
            df[key] = ""
    df = df[list(COLUMNS) + [col for col in df.columns if col not in COLUMNS]]
    temp = OUTPUT_FILE.with_name(OUTPUT_FILE.stem + ".writing.xlsx")
    df.to_excel(temp, index=False)
    os.replace(temp, OUTPUT_FILE)
    atomic_text_write(PROCESSED_UIN_FILE, "".join(f"{uin}\n" for uin in records))
    if position is None:
        # Keep a committed checkpoint in Git, since the user's YAML stages
        # this file if present but does not commit deletion of it.
        atomic_text_write(RESUME_STATE_FILE, "1,1\n")
        print(f"[SAVE] {len(records)} UINs. All 28 member lists complete; "
              "next monthly run starts at Region 01.")
    else:
        region, page, expected = position
        state = {
            "source": CHECKPOINT_SOURCE,
            "region": region, "page": page, "expected_first": expected,
        }
        atomic_text_write(RESUME_STATE_FILE, json.dumps(state, indent=2) + "\n")
        print(f"[SAVE] {len(records)} UINs; resume Region {region:02d}, "
              f"Page {page}, row {expected}.")


def seconds_available(clock: float) -> float:
    return MAX_RUNTIME_SECONDS - SAFETY_BUFFER_SECONDS - (time.monotonic() - clock)


def get_url(region: Region, page: int, base_url: str = BASE_URL) -> str:
    url = f"{base_url}/{region.id}"  # Never zero-pad IDs: 2, not 02.
    if page > 1:
        url += f"?UIN_page={page}"
    return url


def valid_final_url(url: str, region: Region) -> bool:
    parsed = urlsplit(url)
    return (parsed.scheme == "https"
            and parsed.hostname in ("blsbg.eu", "www.blsbg.eu")
            and parsed.path.rstrip("/").lower() ==
            f"/bg/medics/unionlist/{region.id}")


def parse_summary(text: str) -> Tuple[int, int, int]:
    m = SUMMARY_RE.search(text)
    if not m:
        raise PageError(f"Missing pagination range in summary: {text[:150]!r}")
    first, last = int(m.group(1)), int(m.group(2))
    total = int(re.sub(r"\D", "", m.group(3)))
    if first < 1 or first > last or last > total:
        raise PageError(f"Invalid pagination range: {text[:150]!r}")
    return first, last, total


def normalized_name(value: str) -> str:
    return re.sub(r"\W+", "", value.casefold(), flags=re.UNICODE)


def parse_page(content: bytes, region: Region, page: int,
               expected_first: int, source_url: str) -> ParsedPage:
    soup = BeautifulSoup(content, "html.parser")
    h2 = soup.select_one("h2.light.bordered")
    if not h2:
        raise PageError("Missing regional directory heading (possibly a server error page).")
    heading = clean_text(h2.get_text(" ", strip=True))
    if not heading.startswith(HEADER_BG):
        raise PageError(f"Received a non-regional-list page: {heading[:120]!r}")
    region_name = clean_text(heading[len(HEADER_BG):])
    actual, expected = normalized_name(region_name), normalized_name(region.name)
    if not (actual == expected or actual.startswith(expected) or expected.startswith(actual)):
        raise PageError(f"Wrong region heading for {region.code}: {region_name!r} "
                        f"(expected {region.name!r})")

    table = soup.select_one("table.items")
    summary_node = soup.select_one("div.summary")
    if table is None or summary_node is None:
        raise PageError("Missing regional results table or pagination summary.")
    summary = clean_text(summary_node.get_text(" ", strip=True))
    first, last, total = parse_summary(summary)
    if first != expected_first:
        raise PageError(f"Requested Page {page}, expected row {expected_first}, "
                        f"but server returned {first}-{last} of {total}. "
                        "Possible stale/incorrect page.")
    entries = []
    rows = table.select("tbody tr")
    if not rows:
        raise PageError("No table rows returned for a page with reported records.")
    for tr in rows:
        if "empty" in tr.get("class", []):
            raise PageError("Directory returned an empty/error row before the expected end.")
        cells = tr.select("td")
        if len(cells) < 4:
            raise PageError(f"Page {page} has an incomplete row ({len(cells)} cells).")
        uin = normalize_uin(cells[0].get_text(" ", strip=True))
        if not re.fullmatch(r"\d{10}", uin):
            raise PageError(f"Page {page} returned an invalid UIN: {uin!r}")
        img = tr.select_one("img.expand")
        attrs = img.attrs if img else {}
        entries.append({
            "Region Code": f"{region.id:02d}",
            "Region Name": region_name,
            "UIN": uin,
            "Address (Hidden)": clean_attribute(attrs.get("adr")),
            "G Address (Hidden)": clean_attribute(attrs.get("gadr")),
            "Phone": clean_attribute(attrs.get("tel")),
            "Workplace": clean_attribute(attrs.get("wrk")),
            "Specialty (Hidden)": clean_attribute(attrs.get("spec")),
            "Name": clean_text(cells[2].get_text(" ", strip=True)),
            "Specialty (Visible)": clean_text(cells[3].get_text(" ", strip=True)),
            "Source URL": source_url,
            "Summary Info": summary,
        })
    if len(entries) != (last - first + 1):
        raise PageError(f"Page {page} incomplete: {len(entries)} parsed, "
                        f"but server reports {last - first + 1} rows.")
    if len({row["UIN"] for row in entries}) != len(entries):
        raise PageError(f"Page {page} contains duplicate UINs; cannot trust it.")
    return ParsedPage(entries, first, last, total, last >= total)


def save_failed_html(content: bytes) -> None:
    if content:
        try:
            DEBUG_HTML_FILE.write_bytes(content[:300_000])
        except OSError:
            pass


def fetch_page(session: requests.Session, region: Region, page: int,
               expected_first: int, clock: float) -> Tuple[ParsedPage, float]:
    primary_url = get_url(region, page)
    alternate_url = get_url(region, page, ALT_BASE_URL)
    last_error = "No successful response"
    for attempt in range(1, MAX_PAGE_ATTEMPTS + 1):
        if seconds_available(clock) < CONNECT_TIMEOUT + READ_TIMEOUT + 30:
            raise RuntimeBudgetReached("Not enough time left for a safe HTTP fetch.")
        started = time.monotonic()
        response = None
        try:
            response = session.get(primary_url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            if response.status_code == 404:
                # This domain has occasionally returned a 404 on one hostname
                # but not the other. Try one alternate hostname, not another ID.
                time.sleep(2.0)
                response = session.get(alternate_url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            if response.status_code in (401, 403):
                raise SiteAccessDenied(f"HTTP {response.status_code} for {primary_url}")
            if response.status_code != 200:
                raise PageError(f"HTTP {response.status_code} for {response.url}")
            if not valid_final_url(response.url, region):
                raise PageError(f"Redirected to unexpected URL: {response.url}")
            parsed = parse_page(response.content, region, page, expected_first, response.url)
            DEBUG_HTML_FILE.unlink(missing_ok=True)
            return parsed, time.monotonic() - started
        except SiteAccessDenied:
            raise
        except (requests.RequestException, PageError) as exc:
            last_error = str(exc)
            status = response.status_code if response is not None else "network error"
            print(f"[FETCH WARN] {region.code} Page {page}, attempt "
                  f"{attempt}/{MAX_PAGE_ATTEMPTS}, status {status}: {last_error[:180]}")
            if response is not None and response.status_code == 200:
                save_failed_html(response.content)
            if attempt == MAX_PAGE_ATTEMPTS:
                break
            delay = BACKOFF_SECONDS[attempt - 1] + random.uniform(0, 4)
            if response is not None and response.status_code == 429:
                retry_after = response.headers.get("Retry-After", "")
                if retry_after.isdigit():
                    delay = max(delay, min(300, int(retry_after)))
            if seconds_available(clock) < delay + CONNECT_TIMEOUT + READ_TIMEOUT + 30:
                raise RuntimeBudgetReached("Run budget near limit while backing off.")
            print(f"[RETRY] {region.code} Page {page}: waiting {delay:.0f}s "
                  "before another request to the slow BLS server.")
            time.sleep(delay)
    raise PageError(f"Region {region.code} Page {page} failed after "
                    f"{MAX_PAGE_ATTEMPTS} attempts: {last_error}")


def main() -> int:
    clock = time.monotonic()
    print("[INFO] Initializing FULL BLS regional scraper (no 249-result limit).")
    records = load_records()
    start_region, start_page, expected_first = load_start()
    CONTINUE_FLAG_FILE.unlink(missing_ok=True)
    LAST_ERROR_FILE.unlink(missing_ok=True)

    session = requests.Session()
    session.headers.update({
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "bg-BG,bg;q=0.9",
        "User-Agent": USER_AGENT,
    })
    position = (start_region, start_page, expected_first)
    current_total = None
    try:
        for index in range(start_region, len(REGIONS) + 1):
            region = REGIONS[index - 1]
            page = start_page if index == start_region else 1
            row_expected = expected_first if index == start_region else 1
            prior_page_uins = set()
            pages_since_save = 0
            current_total = None
            print(f"\n[REGION] {index:02d}/{len(REGIONS)} {region.name} "
                  f"({region.code}), full directory /unionlist/{region.id}")
            while True:
                position = (index, page, row_expected)
                parsed, duration = fetch_page(session, region, page, row_expected, clock)
                uins = {row["UIN"] for row in parsed.rows}
                if prior_page_uins & uins:
                    raise PageError(f"Page {page} overlaps the previous page for {region.code}; "
                                    "database may have shifted during scraping.")
                prior_page_uins = uins
                if current_total is not None and current_total != parsed.total:
                    print(f"[WARN] {region.code} reported total changed from "
                          f"{current_total} to {parsed.total} during this scan.")
                current_total = parsed.total
                added = 0
                for row in parsed.rows:
                    if row["UIN"] not in records:
                        records[row["UIN"]] = row
                        added += 1
                print(f"[PAGE] {region.code} {page}: records {parsed.first}-{parsed.last} "
                      f"of {parsed.total}, {len(parsed.rows)} rows, {added} new, "
                      f"{len(records)} unique overall; HTTP {duration:.1f}s.")
                pages_since_save += 1
                if parsed.is_last:
                    next_position = (index + 1, 1, 1) if index < len(REGIONS) else None
                    save_checkpoint(records, next_position)
                    position = next_position or (1, 1, 1)
                    print(f"[REGION DONE] {region.code}: processed through record {parsed.last} "
                          f"of {parsed.total}, Page {page}.")
                    if next_position and seconds_available(clock) > PAGE_DELAY + 30:
                        time.sleep(PAGE_DELAY)
                    break
                page += 1   # NO page-number limit; continue until actual last record.
                row_expected = parsed.last + 1
                position = (index, page, row_expected)
                if pages_since_save >= SAVE_EVERY_N_PAGES:
                    save_checkpoint(records, position)
                    pages_since_save = 0
                if seconds_available(clock) < CONNECT_TIMEOUT + READ_TIMEOUT + 30:
                    raise RuntimeBudgetReached("CI runtime nearly exhausted between pages.")
                time.sleep(PAGE_DELAY)
        print(f"[SUCCESS] Completed all requested region member lists; "
              f"{len(records)} unique UINs saved.")
        return 0
    except RuntimeBudgetReached as exc:
        print(f"[PAUSED] {exc}")
        save_checkpoint(records, position)
        atomic_text_write(CONTINUE_FLAG_FILE, "CONTINUE_REQUIRED\n")
        return 0
    except (PageError, SiteAccessDenied, requests.RequestException) as exc:
        print(f"[STOPPED] {exc}")
        save_checkpoint(records, position)
        atomic_text_write(LAST_ERROR_FILE, f"{type(exc).__name__}: {exc}\n")
        print("[STOPPED] Progress saved. No missing page was silently skipped.")
        return 1
    except KeyboardInterrupt:
        print("[INTERRUPTED] Saving progress.")
        save_checkpoint(records, position)
        return 130
    finally:
        session.close()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError) as exc:
        print(f"[FATAL] {exc}", file=sys.stderr)
        sys.exit(2)
