#!/usr/bin/env python3
"""Restartable, low-rate BLSBG scraper using the working DocSearch region filter.

Scrapes publicly displayed member tables without Selenium. Discovers region keys
(such as BLG) from the live search dropdown; region 01 means the first dropdown
region, not /unionlist/01. Continues from checkpoints and preserves old Excel data.

Dependencies: requests beautifulsoup4 pandas openpyxl
Environment: BLS_START_REGION=1, BLS_FORCE_START=1, BLS_PAGE_DELAY=2.0
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
from urllib.parse import parse_qs, quote, urlencode, urljoin, urlsplit

import pandas as pd
import requests
from bs4 import BeautifulSoup

SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = SCRIPT_DIR / "blsbg_outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = OUTPUT_DIR / "bg_medics_dynamic_2029.xlsx"
PROCESSED_UIN_FILE = OUTPUT_DIR / "processed_blsbg_uin_codes.txt"
RESUME_STATE_FILE = OUTPUT_DIR / "resume_state.txt"
CONTINUE_FLAG_FILE = OUTPUT_DIR / "CONTINUE_FLAG_BLSBG"
LAST_ERROR_FILE = OUTPUT_DIR / "last_blsbg_error.txt"
DEBUG_HTML_FILE = SCRIPT_DIR / "blsbg_last_bad_response.html"

START_REGION = int(os.getenv("BLS_START_REGION", "1"))
FORCE_START = os.getenv("BLS_FORCE_START", "0") == "1"
PAGE_DELAY = max(2.0, float(os.getenv("BLS_PAGE_DELAY", "2.0")))
USER_AGENT = os.getenv(
    "BLS_USER_AGENT",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
)
SEARCH_URL = "https://blsbg.eu/bg/medics/search"
SEARCH_SUBMIT = "Търсене"
# Page length is discovered from live summary ranges (e.g., 1-35), not hard-coded.
SAVE_EVERY_N_PAGES = 10
MAX_RUNTIME_SECONDS = 20400
SAFETY_BUFFER_SECONDS = 180
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 75
MAX_PAGE_ATTEMPTS = 5
BACKOFF_SECONDS = (20, 45, 90, 150)

COLUMNS = [
    "Region Code", "Region Name", "UIN", "Address (Hidden)",
    "G Address (Hidden)", "Phone", "Workplace", "Specialty (Hidden)",
    "Name", "Specialty (Visible)", "Source URL", "Summary Info",
]
NO_RESULTS_TEXT = ("Няма намерени", "No results found")
SUMMARY_RE = re.compile(
    r"(\d+)\s*[-–—]\s*(\d+)\s*(?:от|of)\s*([\d\s\u00a0]+)",
    flags=re.IGNORECASE,
)


class PageError(Exception):
    """An HTTP response or page cannot be trusted as a complete result."""


class SiteAccessDenied(Exception):
    """BLS returned an explicit access denial; stop without retrying."""


class RuntimeBudgetReached(Exception):
    """Stop early enough to checkpoint before the CI job times out."""


@dataclass(frozen=True)
class Region:
    code: str  # For example, BLG
    name: str  # Display label from the website


@dataclass
class ParsedPage:
    rows: List[dict]
    is_last: bool
    first_uin: Optional[str]
    summary: str
    next_url: Optional[str]
    first_row: int
    last_row: int
    total_rows: int


def clean_text(value) -> str:
    if value is None:
        return "-"
    cleaned = re.sub(r"[\x00-\x1f\x7f]+", "", str(value)).strip()
    return cleaned or "-"


def clean_attribute(value) -> str:
    return clean_text(value).replace('\\"', '"')


def normalize_uin(value) -> str:
    value = clean_text(value)
    if re.fullmatch(r"\d+\.0", value):
        value = value[:-2]
    if re.fullmatch(r"\d{1,10}", value):
        return value.zfill(10)
    return value


def atomic_text_write(path: Path, content: str) -> None:
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as f:
        f.write(content)
    os.replace(temporary, path)


def load_records() -> Dict[str, dict]:
    if not OUTPUT_FILE.exists():
        print("[INFO] No existing Excel; starting a new dataset.")
        return {}
    df = pd.read_excel(OUTPUT_FILE, dtype=str, keep_default_na=False)
    records = {}
    for row in df.to_dict("records"):
        uin = normalize_uin(row.get("UIN", ""))
        if not re.fullmatch(r"\d{10}", uin):
            raise ValueError(f"Existing Excel has an invalid UIN: {uin!r}; refusing to overwrite it.")
        row["UIN"] = uin
        region_code = clean_text(row.get("Region Code"))
        if region_code.isdigit():
            row["Region Code"] = region_code.zfill(2)
        records[uin] = row
    print(f"[INFO] Loaded {len(df)} Excel rows ({len(records)} unique UINs).")
    return records


def check_tracker(records: Dict[str, dict]) -> None:
    # Excel is authoritative: never skip a UIN only because it is in the txt file.
    if PROCESSED_UIN_FILE.exists():
        with PROCESSED_UIN_FILE.open(encoding="utf-8") as f:
            old_uins = {normalize_uin(line.strip()) for line in f if line.strip()}
        missing = old_uins - set(records)
        if missing:
            print(f"[WARN] Tracker has {len(missing)} UINs not in Excel; they will be recollected if found.")


def is_allowed_url(value: str) -> bool:
    u = urlsplit(value)
    return (
        u.scheme == "https"
        and u.hostname in ("blsbg.eu", "www.blsbg.eu")
        and u.path.lower().startswith("/bg/medics/search")
    )


def get_resume_state() -> Tuple[int, int, Optional[List[Region]], Optional[str], Optional[int]]:
    if not (1 <= START_REGION <= 29):
        raise ValueError("BLS_START_REGION must be between 1 and 29.")
    if FORCE_START:
        print(f"[INFO] Forced restart from Region {START_REGION:02d}, Page 1; keeping existing Excel.")
        return START_REGION, 1, None, None, 1
    if not RESUME_STATE_FILE.exists():
        return START_REGION, 1, None, None, 1

    raw = RESUME_STATE_FILE.read_text(encoding="utf-8").strip()
    try:
        if raw.startswith("{"):
            state = json.loads(raw)
            region, page = int(state["region"]), int(state["page"])
            saved_regions = [Region(str(item["code"]), str(item["name"]))
                             for item in state.get("regions", [])]
            if not saved_regions:
                saved_regions = None
            next_url = state.get("next_url")
            if next_url and not is_allowed_url(next_url):
                raise ValueError("Unrecognized next-page URL in resume state")
            expected_first = state.get("expected_first")
            if expected_first is not None:
                expected_first = int(expected_first)
                if expected_first < 1:
                    raise ValueError("Invalid first-row checkpoint")
        else:
            # Accept existing checkpoints from the old scraper: '1,1'.
            region, page = (int(part) for part in raw.split(",", 1))
            saved_regions, next_url, expected_first = None, None, (1 if page == 1 else None)
        if not (1 <= region <= 29 and page >= 1):
            raise ValueError("Invalid region or page")
        print(f"[INFO] Resuming Region {region:02d}, Page {page}.")
        return region, page, saved_regions, next_url, expected_first
    except (KeyError, ValueError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"Invalid resume_state.txt; fix it or set BLS_FORCE_START=1. Details: {exc}"
        ) from exc


def save_checkpoint(
    records: Dict[str, dict], position: Optional[Tuple[int, int]],
    regions: List[Region], next_url: Optional[str] = None,
    expected_first: Optional[int] = None,
) -> None:
    df = pd.DataFrame(list(records.values()))
    for column in COLUMNS:
        if column not in df.columns:
            df[column] = ""
    df = df[COLUMNS + [col for col in df.columns if col not in COLUMNS]]
    temporary_excel = OUTPUT_FILE.with_name(OUTPUT_FILE.stem + ".writing.xlsx")
    df.to_excel(temporary_excel, index=False)
    os.replace(temporary_excel, OUTPUT_FILE)
    atomic_text_write(PROCESSED_UIN_FILE, "".join(f"{uin}\n" for uin in records))

    if position is None:
        # The user's YAML stages existing resume_state.txt but not its deletion.
        # Writing 1,1 ensures the next monthly run actually begins from the start.
        atomic_text_write(RESUME_STATE_FILE, "1,1\n")
        print(f"[SAVE] {len(records)} unique UINs; finished. Next scheduled scan restarts at Region 01.")
    else:
        region, page = position
        state = {
            "region": region,
            "page": page,
            "regions": [{"code": r.code, "name": r.name} for r in regions],
            "next_url": next_url if next_url and is_allowed_url(next_url) else None,
            "expected_first": expected_first,
        }
        atomic_text_write(RESUME_STATE_FILE, json.dumps(state, ensure_ascii=False, indent=2) + "\n")
        print(f"[SAVE] {len(records)} unique UINs; resume Region {region:02d}, Page {page}.")


def seconds_available(start_clock: float) -> float:
    return MAX_RUNTIME_SECONDS - SAFETY_BUFFER_SECONDS - (time.monotonic() - start_clock)


def debug_bad_page(content: Optional[bytes]) -> None:
    if content:
        try:
            DEBUG_HTML_FILE.write_bytes(content[:300_000])
        except OSError:
            pass


def get_request_url(region_key: str, page: int = 1) -> str:
    params = [
        ("DocSearch[username]", ""),
        ("DocSearch[uin]", ""),
        ("DocSearch[region]", region_key),
        ("yt0", SEARCH_SUBMIT),
    ]
    if page > 1:
        params.append(("UIN_page", str(page)))
    return SEARCH_URL + "?" + urlencode(params)


def get_path_style_url(region_key: str, page: int) -> str:
    # Yii's old pager also uses links like /DocSearch%5Bregion%5D/BLG/UIN_page/2.
    segments = [
        "DocSearch[username]", "DocSearch[uin]",
        "DocSearch[region]", region_key, "yt0", SEARCH_SUBMIT,
        "UIN_page", str(page),
    ]
    return SEARCH_URL + "/" + "/".join(quote(s, safe="") for s in segments)


def parse_region_options(content: bytes) -> List[Region]:
    soup = BeautifulSoup(content, "html.parser")
    dropdown = soup.select_one('select[name="DocSearch[region]"]')
    if dropdown is None:
        dropdown = soup.select_one("select#DocSearch_region")
    if dropdown is None:
        raise PageError("Could not find the DocSearch[region] dropdown in the search form.")

    result = []
    seen = set()
    for option in dropdown.select("option"):
        code = option.get("value", "").strip().upper()
        if not re.fullmatch(r"[A-Z]{2,6}", code) or code in seen:
            continue  # Blank / 0 / All regions option.
        seen.add(code)
        result.append(Region(code, clean_text(option.get_text(" ", strip=True))))

    # BLG is the known code for Blagoevgrad supplied by the user.
    if "BLG" not in seen:
        raise PageError("The region dropdown did not include BLG; stopping to avoid wrong mapping.")
    result.sort(key=lambda r: (r.code != "BLG",))  # Move BLG to index 01; retain other site order.
    if not (25 <= len(result) <= 30):
        raise PageError(f"Expected roughly 28/29 region options; found {len(result)}. Refusing to guess.")
    return result


def parse_summary(text: str) -> Tuple[int, int, int]:
    m = SUMMARY_RE.search(text)
    if not m:
        raise PageError(f"Missing or changed pagination summary: {text[:160]!r}")
    first, last = int(m.group(1)), int(m.group(2))
    total = int(re.sub(r"\D", "", m.group(3)))
    if first < 1 or last < first or last > total:
        raise PageError(f"Inconsistent pagination summary: {text[:160]!r}")
    return first, last, total


def get_page_from_href(url: str) -> Optional[int]:
    """Recognize common Yii page parameters without assuming one widget ID."""
    parsed = urlsplit(url)
    for key, values in parse_qs(parsed.query).items():
        if (key.lower() == "page" or key.lower().endswith("_page")) and values:
            if values[0].isdigit():
                return int(values[0])
    from urllib.parse import unquote
    m = re.search(r"(?:^|/)(?:[A-Za-z0-9_-]*_page|page)/(\d+)(?:/|$)",
                  unquote(parsed.path), flags=re.I)
    return int(m.group(1)) if m else None


def region_filter_matches_url(url: str, code: str) -> bool:
    """Reject a pager link explicitly filtered to a different region."""
    split = urlsplit(url)
    form_region = parse_qs(split.query).get("DocSearch[region]")
    if form_region:
        return form_region[0].upper() == code.upper()
    from urllib.parse import unquote
    route = unquote(split.path)
    region_match = re.search(r"/DocSearch\[region\]/([^/]+)", route, flags=re.I)
    return region_match is None or unquote(region_match.group(1)).upper() == code.upper()


def find_next_page_url(
    soup: BeautifulSoup, current_url: str, page: int, region_code: str
) -> Optional[str]:
    # The site's own Yii pager knows which parameters it requires. Do not
    # reconstruct its links if a valid, region-matching href is available.
    selectors = (
        "ul.yiiPager a[href], .pagination a[href], "
        "a[rel='next'][href], .pager a[href], a[href]"
    )
    for a in soup.select(selectors):
        target = urljoin(current_url, a.get("href", ""))
        if (is_allowed_url(target)
                and get_page_from_href(target) == page + 1
                and region_filter_matches_url(target, region_code)):
            return target
    # Fallback for a Yii installation with an unusual pager parameter:
    # only trust anchors from the actual pager whose visible label is next page.
    for a in soup.select("ul.yiiPager a[href], .pagination a[href], .pager a[href]"):
        if a.get_text(" ", strip=True) != str(page + 1):
            continue
        target = urljoin(current_url, a.get("href", ""))
        if is_allowed_url(target) and region_filter_matches_url(target, region_code):
            return target
    return None


def parse_page(
    content: bytes, region: Region, index: int, page: int, url: str,
    expected_first: Optional[int] = None,
) -> ParsedPage:
    soup = BeautifulSoup(content, "html.parser")
    table = soup.select_one("table.items")
    summary_element = soup.select_one("div.summary")
    summary = clean_text(summary_element.get_text(" ", strip=True)) if summary_element else "-"
    rows = table.select("tbody tr") if table else []
    body_text = soup.get_text(" ", strip=True)
    if not rows or all(
        "empty" in tr.get("class", []) or any(x in tr.get_text(" ", strip=True) for x in NO_RESULTS_TEXT)
        for tr in rows
    ):
        if any(x in body_text for x in NO_RESULTS_TEXT):
            # All known BLS regional colleges have doctors. Treat 'no results'
            # as a potentially broken filter rather than recording an empty region.
            raise PageError(f"Search returned no results for {region.code}; verify the region filter.")
        raise PageError("Missing result table; received a form, partial page, or server error.")
    if summary == "-":
        raise PageError("Result table exists but pagination summary is missing.")

    first, last, total = parse_summary(summary)
    # Dynamic pagination: BLS is returning 35 rows per page in the user's
    # current run, not the 25 assumed by the earlier scraper.
    if page == 1:
        expected_first = 1
    if expected_first is None:
        raise PageError(
            f"Missing validated starting row for Page {page}. "
            "Re-read Page 1 to establish this region's actual page length."
        )
    if first != expected_first:
        raise PageError(
            f"Requested page {page}, but summary starts at row {first}, expected {expected_first}. "
            "The site may have ignored pagination or served a cached page."
        )

    table_headers = [clean_text(th.get_text(" ", strip=True)).lower()
                     for th in table.select("thead tr th")]
    entries = []
    for tr in rows:
        if "empty" in tr.get("class", []) or any(x in tr.get_text(" ", strip=True) for x in NO_RESULTS_TEXT):
            continue
        cells = tr.select("td")
        if len(cells) < 4:
            raise PageError(f"Only {len(cells)} cells found in a result row on page {page}.")
        uin = normalize_uin(cells[0].get_text(" ", strip=True))
        if not re.fullmatch(r"\d{10}", uin):
            raise PageError(f"Invalid or missing UIN on page {page}: {uin!r}")

        # Search has FIVE columns: UIN, profile, name, region, specialty.
        # unionlist had FOUR. Never confuse region with specialty.
        has_region_col = len(cells) >= 5 or any(
            h in ("регион", "област", "region") for h in table_headers
        )
        if has_region_col and len(cells) < 5:
            raise PageError("Search table header implies 5 columns, but data has fewer.")
        specialty_index = 4 if has_region_col else 3
        region_name = clean_text(cells[3].get_text(" ", strip=True)) if has_region_col else region.name
        if region_name == "-":
            region_name = region.name
        if has_region_col:
            expected = re.sub(r"\W+", "", region.name, flags=re.UNICODE).casefold()
            actual = re.sub(r"\W+", "", region_name, flags=re.UNICODE).casefold()
            region_key = region.code.casefold()
            if not (expected in actual or actual in expected or region_key == actual):
                raise PageError(
                    f"Results are not filtered to {region.code} ({region.name}): "
                    f"encountered {region_name!r}. Refusing to mislabel rows."
                )

        img = tr.select_one("img.expand")
        attrs = img.attrs if img else {}
        entries.append({
            # Existing unionlist/2..29 files identify BLG as 02, not 01.
            # Preserve that legacy region-code convention in the Excel output.
            "Region Code": f"{index + 1:02d}",
            "Region Name": region_name,
            "UIN": uin,
            "Address (Hidden)": clean_attribute(attrs.get("adr")),
            "G Address (Hidden)": clean_attribute(attrs.get("gadr")),
            "Phone": clean_attribute(attrs.get("tel")),
            "Workplace": clean_attribute(attrs.get("wrk")),
            "Specialty (Hidden)": clean_attribute(attrs.get("spec")),
            "Name": clean_text(cells[2].get_text(" ", strip=True)),
            "Specialty (Visible)": clean_text(cells[specialty_index].get_text(" ", strip=True)),
            "Source URL": url,
            "Summary Info": summary,
        })

    expected_count = last - first + 1
    if len(entries) != expected_count:
        raise PageError(
            f"Partial result page: parsed {len(entries)} rows, expected {expected_count} on page {page}."
        )
    if len({row["UIN"] for row in entries}) != len(entries):
        raise PageError(f"Repeated UIN in a single page {page}.")
    is_last = last >= total
    next_url = find_next_page_url(soup, url, page, region.code) if not is_last else None
    return ParsedPage(entries, is_last, entries[0]["UIN"], summary, next_url,
                      first, last, total)


def fetch_verified(session: requests.Session, candidates: List[str], parser,
                   start_clock: float, label: str):
    # parser(content, final_url) must confirm that the response is the right page.
    last_error = "No valid response"
    for attempt in range(1, MAX_PAGE_ATTEMPTS + 1):
        if seconds_available(start_clock) < CONNECT_TIMEOUT + READ_TIMEOUT + 30:
            raise RuntimeBudgetReached("Not enough time for a safe HTTP request.")
        overload = False
        for candidate_number, candidate in enumerate(dict.fromkeys(candidates), start=1):
            if not is_allowed_url(candidate):
                raise PageError(f"Unexpected target URL {candidate}")
            response = None
            started = time.monotonic()
            try:
                response = session.get(candidate, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
                if response.status_code in (401, 403):
                    raise SiteAccessDenied(f"HTTP {response.status_code} at {candidate}")
                if response.status_code != 200:
                    raise PageError(f"HTTP {response.status_code} at {candidate}")
                parsed = parser(response.content, response.url)
                if label.endswith("Page 1") and getattr(parsed, "rows", None) and not parsed.is_last:
                    try:
                        (SCRIPT_DIR / "blsbg_last_good_first_page.html").write_bytes(
                            response.content[:300_000])
                    except OSError:
                        pass
                if DEBUG_HTML_FILE.exists():
                    DEBUG_HTML_FILE.unlink(missing_ok=True)
                return parsed, time.monotonic() - started
            except SiteAccessDenied:
                raise
            except (requests.RequestException, PageError) as exc:
                last_error = str(exc)
                status = response.status_code if response is not None else "network error"
                print(f"[FETCH WARN] {label}: {status}, URL={candidate[:220]}, "
                      f"issue={last_error[:130]}")
                if response is not None and response.status_code == 200:
                    debug_bad_page(response.content)
                    if attempt == 1 and "Page " in label:
                        try:
                            (SCRIPT_DIR / f"blsbg_failed_candidate_{candidate_number}.html").write_bytes(
                                response.content[:300_000])
                        except OSError:
                            pass
                if response is not None and response.status_code in (429, 500, 502, 503, 504):
                    overload = True
                    break  # Do not immediately send another request to an overloaded server.
                if len(candidates) > 1:
                    time.sleep(PAGE_DELAY)  # Alternate URL only for bad path or stale page.

        if attempt == MAX_PAGE_ATTEMPTS:
            break
        delay = BACKOFF_SECONDS[attempt - 1] + random.uniform(0, 4)
        if response is not None and response.status_code == 429:
            retry_after = response.headers.get("Retry-After", "")
            if retry_after.isdigit():
                delay = max(delay, min(int(retry_after), 300))
        if seconds_available(start_clock) < delay + CONNECT_TIMEOUT + READ_TIMEOUT + 30:
            raise RuntimeBudgetReached("Time limit near while waiting for BLS to recover.")
        print(f"[RETRY] {label}, attempt {attempt}/{MAX_PAGE_ATTEMPTS}: "
              f"{last_error[:160]}. Waiting {delay:.0f}s.")
        time.sleep(delay)
    raise PageError(f"{label} failed after {MAX_PAGE_ATTEMPTS} attempts: {last_error}")


def discover_regions(session: requests.Session, start_clock: float) -> List[Region]:
    print("[INFO] Reading the BLS region dropdown (DocSearch[region]).")
    regions, duration = fetch_verified(
        session, [SEARCH_URL], lambda body, url: parse_region_options(body),
        start_clock, "region dropdown"
    )
    print(f"[INFO] Discovered {len(regions)} searchable regions in {duration:.1f}s: "
          + ", ".join(r.code for r in regions))
    if len(regions) != 29:
        print(f"[NOTE] The website exposes {len(regions)} regional options, not 29. "
              "Using its actual list; no nonexistent region will be fabricated.")
    return regions


def fetch_page(session: requests.Session, region: Region, index: int, page: int,
               next_url: Optional[str], start_clock: float,
               expected_first: Optional[int] = None) -> Tuple[ParsedPage, float]:
    candidates = []
    if (next_url and is_allowed_url(next_url)
            and region_filter_matches_url(next_url, region.code)):
        candidates.append(next_url)
    candidates.append(get_request_url(region.code, page))
    if page > 1 and not next_url:
        # Only try an invented legacy path if the website exposed no pager URL.
        candidates.append(get_path_style_url(region.code, page))
    return fetch_verified(
        session, candidates,
        lambda body, final_url: parse_page(
            body, region, index, page, final_url, expected_first=expected_first),
        start_clock, f"Region {index:02d} ({region.code}), Page {page}"
    )


def main() -> int:
    clock = time.monotonic()
    print("[INFO] Initializing BLSBG DocSearch scraper (no Selenium/Chrome).")
    records = load_records()
    check_tracker(records)
    start_region, start_page, saved_regions, resume_url, resume_expected_first = get_resume_state()
    CONTINUE_FLAG_FILE.unlink(missing_ok=True)
    LAST_ERROR_FILE.unlink(missing_ok=True)

    session = requests.Session()
    session.headers.update({
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "bg-BG,bg;q=0.9",
        "User-Agent": USER_AGENT,
    })

    regions = saved_regions or []
    position = (start_region, start_page)
    current_url = resume_url
    expected_first = resume_expected_first
    try:
        if not regions:
            regions = discover_regions(session, clock)
        if start_region > len(regions):
            raise PageError(
                f"Checkpoint wants Region {start_region}, but the site has {len(regions)}. "
                "Set BLS_FORCE_START=1 to begin again from Region 01."
            )
        print(f"[INFO] Region 01 is {regions[0].name} ({regions[0].code}).")
        for index in range(start_region, len(regions) + 1):
            region = regions[index - 1]
            page = start_page if index == start_region else 1
            next_url = current_url if index == start_region else None
            expected_first = resume_expected_first if index == start_region else 1
            previous_first_uin = None
            pending_pages = 0
            print(f"\n[REGION] {index:02d}/{len(regions):02d} ({region.code}: {region.name}), "
                  f"starting Page {page}")
            if page > 1 and expected_first is None:
                # Old checkpoints saved page number but not true page length.
                # Re-read page 1 once to learn what BLS actually returned.
                print(f"[INFO] Legacy resume for {region.code}, Page {page}: "
                      "re-reading Page 1 to establish real page boundaries.")
                first_page, _ = fetch_page(session, region, index, 1, None, clock, 1)
                observed_page_size = first_page.last_row - first_page.first_row + 1
                expected_first = (page - 1) * observed_page_size + 1
                if page == 2 and first_page.next_url:
                    next_url = first_page.next_url
                print(f"[INFO] Page 1 holds {observed_page_size} records; "
                      f"Page {page} must begin at record {expected_first}.")
                time.sleep(PAGE_DELAY)
            while True:
                position = (index, page)
                current_url = next_url
                parsed, duration = fetch_page(
                    session, region, index, page, next_url, clock, expected_first)
                if parsed.first_uin and parsed.first_uin == previous_first_uin:
                    raise PageError(f"Page {page} repeats first UIN from the previous page.")
                previous_first_uin = parsed.first_uin
                new_count = 0
                for row in parsed.rows:
                    if row["UIN"] not in records:
                        records[row["UIN"]] = row
                        new_count += 1
                print(f"[PAGE] {index:02d}/{page}: {len(parsed.rows)} rows, "
                      f"{new_count} new, {duration:.1f}s, total {len(records)} UINs, "
                      f"summary={parsed.summary!r}.")
                if page == 1 and not parsed.is_last:
                    print(f"[PAGER] {region.code} next URL from HTML: "
                          f"{parsed.next_url or 'NOT FOUND (query fallback only)'}")
                pending_pages += 1
                if parsed.is_last:
                    following = (index + 1, 1) if index < len(regions) else None
                    save_checkpoint(records, following, regions)
                    position, current_url = following or (1, 1), None
                    print(f"[REGION DONE] {index:02d} ({region.code})")
                    if following and seconds_available(clock) > PAGE_DELAY + 30:
                        time.sleep(PAGE_DELAY)
                    break
                page += 1
                next_url = parsed.next_url  # Follow the actual BLS pager instead of guessing.
                expected_first = parsed.last_row + 1
                position = (index, page)
                current_url = next_url
                if pending_pages >= SAVE_EVERY_N_PAGES:
                    save_checkpoint(records, position, regions, next_url, expected_first)
                    pending_pages = 0
                if seconds_available(clock) < CONNECT_TIMEOUT + READ_TIMEOUT + 30:
                    raise RuntimeBudgetReached("CI runtime nearly exhausted between pages.")
                time.sleep(PAGE_DELAY)

        print(f"[SUCCESS] Scanned all {len(regions)} searchable regions. "
              f"Saved {len(records)} unique UINs.")
        return 0
    except RuntimeBudgetReached as exc:
        print(f"[PAUSED] {exc}")
        save_checkpoint(records, position, regions, current_url, expected_first)
        atomic_text_write(CONTINUE_FLAG_FILE, "CONTINUE_REQUIRED\n")
        print(f"[PAUSED] Requested continuation at Region {position[0]:02d}, Page {position[1]}.")
        return 0
    except (PageError, SiteAccessDenied, requests.RequestException) as exc:
        print(f"[STOPPED] {exc}")
        # If region discovery failed, don't write an empty region map checkpoint.
        save_checkpoint(records, position, regions, current_url, expected_first)
        atomic_text_write(LAST_ERROR_FILE, f"{type(exc).__name__}: {exc}\n")
        print("[STOPPED] No pages skipped. Re-run when BLS responds.")
        return 1
    except KeyboardInterrupt:
        print("[INTERRUPTED] Saving progress.")
        save_checkpoint(records, position, regions, current_url, expected_first)
        return 130
    finally:
        session.close()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError) as exc:
        print(f"[FATAL] {exc}", file=sys.stderr)
        sys.exit(2)
