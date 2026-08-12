import time
import os
import urllib.parse
import sys
import csv
import json
import re
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import (
    WebDriverException,
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
)


# ============================================================
# CONFIGURATION
# ============================================================

START_TIME = time.time()

# Keep your original ~5.4 hour execution limit
TIME_LIMIT_SECONDS = 5.4 * 60 * 60

BASE_URL = "https://zdraven-arhiv.com/doctors/"

# How long we allow a page to dynamically populate
PAGE_MAX_WAIT_SECONDS = 25

# Number of seconds the card count must remain unchanged
# before we consider the listing stable.
PAGE_STABLE_SECONDS = 3.0

# Small polling interval while waiting for cards
PAGE_POLL_INTERVAL = 0.5

# Number of complete page-loading attempts
MAX_PAGE_LOAD_RETRIES = 4

# Number of profile scraping attempts
MAX_PROFILE_RETRIES = 3

# Give the profile a little time after navigation
PROFILE_INITIAL_WAIT = 1.5

# Delay before retrying problematic pages/profiles
RETRY_DELAY_SECONDS = 2

# How many pages to keep trying after a suspicious failure
# before moving to phase 2.
MAX_CONSECUTIVE_PAGE_FAILURES = 5


# ============================================================
# PATHS
# ============================================================

try:
    base_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    base_dir = os.getcwd()

output_dir = os.path.join(base_dir, "zdraven_arhiv_outputs")
os.makedirs(output_dir, exist_ok=True)

state_file = os.path.join(
    output_dir,
    "savegame_zdraven_arhiv.json"
)

memory_file = os.path.join(
    output_dir,
    "parsed_urls_zdraven_arhiv.txt"
)

failed_pages_file = os.path.join(
    output_dir,
    "failed_pages_zdraven_arhiv.json"
)

failed_profiles_file = os.path.join(
    output_dir,
    "failed_profiles_zdraven_arhiv.json"
)

page_discovered_urls_file = os.path.join(
    output_dir,
    "page_discovered_urls.json"
)

current_batch_filename = os.path.join(
    output_dir,
    "zdraven_arhiv_data_mega.csv"
)

CONTINUE_FLAG_FILE = os.path.join(
    output_dir,
    "CONTINUE_FLAG_ZDRAVEN_ARHIV"
)


# ============================================================
# DATA SCHEMA
# ============================================================

fieldnames = [
    "Име",
    "URL",
    "Описание (Лист)",
    "Телефони",
    "Email",
    "Адрес (Текст)",
    "Адрес (Google Maps Pin)",
    "Google Maps Link",
    "Breadcrumb (Текст)",
    "Биография",
    "Note",
    "Timestamp",
]


# ============================================================
# TIME LIMIT HELPERS
# ============================================================

def time_limit_reached():
    return (time.time() - START_TIME) >= TIME_LIMIT_SECONDS


def remaining_time_seconds():
    return max(
        0,
        TIME_LIMIT_SECONDS - (time.time() - START_TIME)
    )


# ============================================================
# STATE MANAGEMENT
# ============================================================

state = {
    "page": 1,
    "phase": 1,
    "consecutive_fails": 0,
}

if os.path.exists(state_file):
    try:
        with open(state_file, "r", encoding="utf-8") as f:
            state = json.load(f)

        print(
            f"[INFO] Resuming from "
            f"Phase {state.get('phase', 1)}, "
            f"Page {state.get('page', 1)}."
        )

    except Exception as e:
        print(
            f"[WARN] Could not load state file: {e}"
        )

        state = {
            "page": 1,
            "phase": 1,
            "consecutive_fails": 0,
        }


def save_state(
    page,
    phase=1,
    consecutive_fails=0,
):
    temp_file = state_file + ".tmp"

    payload = {
        "page": page,
        "phase": phase,
        "consecutive_fails": consecutive_fails,
        "saved_at": datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
    }

    try:
        with open(
            temp_file,
            "w",
            encoding="utf-8"
        ) as f:
            json.dump(
                payload,
                f,
                ensure_ascii=False,
                indent=2,
            )

        os.replace(temp_file, state_file)

    except Exception as e:
        print(
            f"[ERROR] Failed to save state: {e}"
        )


# ============================================================
# PARSED URL MEMORY
# ============================================================

parsed_urls = set()

if os.path.exists(memory_file):
    try:
        with open(
            memory_file,
            "r",
            encoding="utf-8"
        ) as f:

            for line in f:
                url = line.strip()

                if not url:
                    continue

                decoded = urllib.parse.unquote(url)

                parsed_urls.add(decoded)
                parsed_urls.add(url)

    except Exception as e:
        print(
            f"[WARN] Could not load parsed URL memory: {e}"
        )


print(
    f"[INFO] Loaded {len(parsed_urls)} URL cache entries."
)


def normalize_url(url):
    if not url:
        return ""

    url = url.strip()

    decoded = urllib.parse.unquote(url)

    # Remove fragment
    decoded = decoded.split("#", 1)[0]

    # Remove trailing slash for consistency
    if decoded.endswith("/") and decoded != BASE_URL:
        decoded = decoded.rstrip("/")

    return decoded


def is_already_parsed(raw_url):
    decoded = normalize_url(raw_url)

    return (
        decoded in parsed_urls
        or raw_url in parsed_urls
    )


def mark_as_parsed(raw_url):
    decoded_url = normalize_url(raw_url)

    parsed_urls.add(decoded_url)
    parsed_urls.add(raw_url)

    try:
        with open(
            memory_file,
            "a",
            encoding="utf-8"
        ) as f:
            f.write(decoded_url + "\n")

    except Exception as e:
        print(
            f"[ERROR] Failed to write parsed URL: {e}"
        )


# ============================================================
# FAILED PROFILE MANAGEMENT
# ============================================================

def load_failed_profiles():
    if not os.path.exists(failed_profiles_file):
        return []

    try:
        with open(
            failed_profiles_file,
            "r",
            encoding="utf-8"
        ) as f:
            data = json.load(f)

        if not isinstance(data, list):
            return []

        return data

    except Exception:
        return []


def save_failed_profiles(profiles):
    try:
        with open(
            failed_profiles_file,
            "w",
            encoding="utf-8"
        ) as f:
            json.dump(
                profiles,
                f,
                ensure_ascii=False,
                indent=2,
            )

    except Exception as e:
        print(
            f"[ERROR] Failed to save failed profiles: {e}"
        )


def add_failed_profile(
    name,
    raw_url,
    page,
    reason=""
):
    profiles = load_failed_profiles()

    normalized = normalize_url(raw_url)

    for existing in profiles:
        if normalize_url(
            existing.get("URL", "")
        ) == normalized:
            return

    profiles.append(
        {
            "Име": name,
            "URL": normalized,
            "Page": page,
            "Reason": reason,
            "Timestamp": datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        }
    )

    save_failed_profiles(profiles)


# ============================================================
# FAILED PAGE MANAGEMENT
# ============================================================

def load_failed_pages():
    if not os.path.exists(failed_pages_file):
        return []

    try:
        with open(
            failed_pages_file,
            "r",
            encoding="utf-8"
        ) as f:
            data = json.load(f)

        if not isinstance(data, list):
            return []

        return sorted(
            set(
                int(x)
                for x in data
                if str(x).isdigit()
            )
        )

    except Exception:
        return []


def save_failed_pages(pages):
    try:
        pages = sorted(set(int(x) for x in pages))

        with open(
            failed_pages_file,
            "w",
            encoding="utf-8"
        ) as f:
            json.dump(
                pages,
                f,
                ensure_ascii=False,
                indent=2,
            )

    except Exception as e:
        print(
            f"[ERROR] Failed to save failed pages: {e}"
        )


def add_failed_page(page_num):
    pages = load_failed_pages()

    page_num = int(page_num)

    if page_num not in pages:
        pages.append(page_num)

    save_failed_pages(pages)


def remove_failed_page(page_num):
    pages = load_failed_pages()

    if page_num in pages:
        pages.remove(page_num)

    save_failed_pages(pages)


# ============================================================
# PAGE DISCOVERY MEMORY
# ============================================================

def load_page_discovered_urls():
    if not os.path.exists(page_discovered_urls_file):
        return {}

    try:
        with open(
            page_discovered_urls_file,
            "r",
            encoding="utf-8"
        ) as f:
            data = json.load(f)

        return data if isinstance(data, dict) else {}

    except Exception:
        return {}


page_discovered_urls = load_page_discovered_urls()


def save_page_discovered_urls():
    try:
        temp_file = page_discovered_urls_file + ".tmp"

        with open(
            temp_file,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                page_discovered_urls,
                f,
                ensure_ascii=False,
                indent=2,
            )

        os.replace(
            temp_file,
            page_discovered_urls_file
        )

    except Exception as e:
        print(
            f"[ERROR] Could not save page URL map: {e}"
        )


# ============================================================
# CSV INITIALIZATION
# ============================================================

if not os.path.exists(current_batch_filename):

    try:
        with open(
            current_batch_filename,
            "w",
            newline="",
            encoding="utf-8-sig"
        ) as f:

            writer = csv.DictWriter(
                f,
                fieldnames=fieldnames
            )

            writer.writeheader()

    except Exception as e:
        print(
            f"[ERROR] Failed to initialize CSV: {e}"
        )


# ============================================================
# WEBDRIVER
# ============================================================

def create_driver():

    print("[INFO] Booting Chrome...")

    options = Options()

    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "--disable-features=site-per-process"
    )

    options.add_argument(
        "--user-agent=Mozilla/5.0 "
        "(Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 "
        "(KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    # Reduce unnecessary browser overhead
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-extensions")

    try:

        service = Service(
            ChromeDriverManager().install()
        )

        drv = webdriver.Chrome(
            service=service,
            options=options
        )

        drv.set_page_load_timeout(30)

        return drv

    except Exception as e:

        print(
            f"[CRITICAL] Failed to start Chrome: {e}"
        )

        raise


driver = create_driver()


# ============================================================
# CSV SAVE
# ============================================================

def save_single_record(record):

    if not record:
        return False

    try:

        with open(
            current_batch_filename,
            "a",
            newline="",
            encoding="utf-8-sig"
        ) as f:

            writer = csv.DictWriter(
                f,
                fieldnames=fieldnames,
                extrasaction="ignore"
            )

            writer.writerow(record)

        return True

    except Exception as e:

        print(
            f"[ERROR] Failed to save CSV record: {e}"
        )

        return False


# ============================================================
# ERROR CLASSIFICATION
# ============================================================

def is_browser_crash_error(exception):
    text = str(exception).lower()

    keywords = [
        "crashed",
        "disconnected",
        "out of memory",
        "chrome not reachable",
        "invalid session id",
        "tab crashed",
        "session deleted",
    ]

    return any(
        keyword in text
        for keyword in keywords
    )


# ============================================================
# PAGE URL BUILDER
# ============================================================

def build_page_url(page):

    page = int(page)

    if page == 1:
        return BASE_URL

    return (
        f"{BASE_URL}"
        f"?jsf=jet-engine"
        f"&pagenum={page}"
    )


# ============================================================
# END-OF-PAGE DETECTION
# ============================================================

def page_is_not_found(driver):

    try:

        title = (
            driver.title
            or ""
        ).lower()

        if (
            "404" in title
            or "страницата не е намерена" in title
        ):
            return True

    except Exception:
        pass

    try:

        source = (
            driver.page_source
            or ""
        ).lower()

        if (
            "404 not found" in source
            or "страницата не е намерена" in source
        ):
            return True

    except Exception:
        pass

    return False


def page_has_no_data(driver):

    try:

        elements = driver.find_elements(
            By.CLASS_NAME,
            "jet-listing-not-found"
        )

        for element in elements:

            try:

                if not element.is_displayed():
                    continue

                text = (
                    element.text
                    or ""
                ).strip().lower()

                if (
                    "no data was found"
                    in text
                    or "няма намерени данни"
                    in text
                ):
                    return True

            except Exception:
                continue

    except Exception:
        pass

    return False


# ============================================================
# LISTING CARD LOCATOR
# ============================================================

CARD_XPATH = (
    "//div[contains("
    "@class, "
    "'jet-listing-grid__item'"
    ")]"
)


def get_listing_cards(driver):

    return driver.find_elements(
        By.XPATH,
        CARD_XPATH
    )


# ============================================================
# WAIT FOR LISTING TO STABILIZE
# ============================================================

def wait_for_stable_listing(driver):

    start = time.time()

    last_count = -1
    stable_since = None

    best_cards = []

    while (
        time.time() - start
        < PAGE_MAX_WAIT_SECONDS
    ):

        if time_limit_reached():
            break

        if page_is_not_found(driver):
            return [], "404"

        if page_has_no_data(driver):
            return [], "NO_DATA"

        try:

            cards = get_listing_cards(driver)

            count = len(cards)

        except (
            StaleElementReferenceException,
            WebDriverException
        ):

            time.sleep(
                PAGE_POLL_INTERVAL
            )

            continue

        if count > 0:

            best_cards = cards

            if count == last_count:

                if stable_since is None:
                    stable_since = time.time()

                elif (
                    time.time()
                    - stable_since
                    >= PAGE_STABLE_SECONDS
                ):
                    print(
                        f"      [INFO] "
                        f"Listing stabilized at "
                        f"{count} cards."
                    )

                    return best_cards, "OK"

            else:

                print(
                    f"      [DEBUG] "
                    f"Card count changed: "
                    f"{last_count} -> {count}"
                )

                last_count = count
                stable_since = None

        else:

            stable_since = None

        time.sleep(
            PAGE_POLL_INTERVAL
        )

    if best_cards:

        print(
            f"      [WARN] Listing did not "
            f"fully stabilize within "
            f"{PAGE_MAX_WAIT_SECONDS}s. "
            f"Using best observed count: "
            f"{len(best_cards)}."
        )

        return best_cards, "UNSTABLE_WITH_CARDS"

    return [], "EMPTY"


# ============================================================
# EXTRACT DOCTOR FROM CARD
# ============================================================

LINK_SELECTORS = [
    "a.jet-listing-dynamic-link__link",
    "a[href*='/doctor']",
    "a[href*='/doctors']",
    "a[href]",
]


def extract_doctor_from_card(card):

    last_error = None

    for selector in LINK_SELECTORS:

        try:

            links = card.find_elements(
                By.CSS_SELECTOR,
                selector
            )

            for link_el in links:

                raw_url = (
                    link_el.get_attribute("href")
                    or ""
                ).strip()

                if not raw_url:
                    continue

                # Ignore javascript / mail / tel links
                lower_url = raw_url.lower()

                if (
                    lower_url.startswith("javascript:")
                    or lower_url.startswith("mailto:")
                    or lower_url.startswith("tel:")
                    or lower_url.startswith("#")
                ):
                    continue

                # Prefer doctor-profile-looking links
                if (
                    "/doctor" not in lower_url
                    and
                    "/lek" not in lower_url
                    and
                    "/лекар" not in lower_url
                ):
                    # We may still accept it as a fallback
                    # only when it's clearly an absolute URL.
                    if not lower_url.startswith(
                        "https://zdraven-arhiv.com/"
                    ):
                        continue

                name = (
                    link_el.text
                    or ""
                ).strip()

                if not name:
                    name = (
                        card.text
                        or ""
                    ).split("\n")[0].strip()

                decoded_url = normalize_url(
                    raw_url
                )

                return {
                    "Име": name or "Unknown",
                    "RAW_URL": raw_url,
                    "URL": decoded_url,
                    "Описание (Лист)": "-",
                }

        except Exception as e:

            last_error = e

    raise NoSuchElementException(
        f"Could not extract doctor link "
        f"from card. Last error: {last_error}"
    )


# ============================================================
# EXTRACT ALL DOCTORS FROM PAGE
# ============================================================

def extract_doctors_from_page(
    cards,
    page_number
):

    doctors = []

    seen_urls = set()

    failed_cards = 0

    for index, card in enumerate(
        cards,
        start=1
    ):

        try:

            doc = extract_doctor_from_card(
                card
            )

            url = doc["URL"]

            if not url:
                raise ValueError(
                    "Doctor URL is empty"
                )

            # Deduplicate on the page itself
            if url in seen_urls:
                print(
                    f"      [DEBUG] "
                    f"Duplicate doctor on page "
                    f"{page_number}: "
                    f"{doc['Име']}"
                )
                continue

            seen_urls.add(url)

            doctors.append(doc)

        except (
            Exception
        ) as e:

            failed_cards += 1

            card_text = (
                card.text
                or ""
            ).strip().replace(
                "\n",
                " | "
            )

            print(
                f"      [WARN] Failed to "
                f"extract card #{index} "
                f"on page {page_number}: "
                f"{e}"
            )

            print(
                f"             Card text: "
                f"{card_text[:250]}"
            )

    print(
        f"      [INFO] Extracted "
        f"{len(doctors)} unique doctors "
        f"from {len(cards)} cards. "
        f"Failed cards: {failed_cards}"
    )

    return doctors, failed_cards


# ============================================================
# SAVE PAGE DISCOVERY
# ============================================================

def save_page_discovery(
    page_number,
    doctors,
    card_count,
    failed_card_count,
    status
):

    page_key = str(page_number)

    page_discovered_urls[page_key] = {
        "timestamp": datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        "card_count": card_count,
        "extracted_count": len(doctors),
        "failed_card_count": failed_card_count,
        "status": status,
        "urls": [
            normalize_url(
                d["URL"]
            )
            for d in doctors
        ],
    }

    save_page_discovered_urls()


# ============================================================
# LOAD A PAGE
# ============================================================

def load_page(
    page_number,
    retry_number=0
):

    target_url = build_page_url(
        page_number
    )

    print(
        f"  > Loading PAGE {page_number}: "
        f"{target_url}"
    )

    try:

        driver.get(
            target_url
        )

        # First let the document load
        try:

            WebDriverWait(
                driver,
                15
            ).until(
                lambda d:
                d.execute_script(
                    "return document.readyState"
                )
                in (
                    "interactive",
                    "complete"
                )
            )

        except Exception:
            pass

        # Small initial delay
        time.sleep(1.5)

        if page_is_not_found(driver):

            print(
                f"      [INFO] Page {page_number} "
                f"is 404."
            )

            return {
                "status": "404",
                "cards": [],
                "doctors": [],
            }

        if page_has_no_data(driver):

            print(
                f"      [INFO] Page {page_number} "
                f"contains no data."
            )

            return {
                "status": "NO_DATA",
                "cards": [],
                "doctors": [],
            }

        cards, wait_status = (
            wait_for_stable_listing(
                driver
            )
        )

        if not cards:

            return {
                "status": "EMPTY",
                "cards": [],
                "doctors": [],
            }

        doctors, failed_cards = (
            extract_doctors_from_page(
                cards,
                page_number
            )
        )

        save_page_discovery(
            page_number,
            doctors,
            len(cards),
            failed_cards,
            wait_status
        )

        # A page with zero extracted doctors is suspicious.
        if not doctors:

            print(
                f"      [WARN] Page {page_number} "
                f"had {len(cards)} cards but "
                f"0 doctor URLs were extracted."
            )

            return {
                "status": "BROKEN",
                "cards": cards,
                "doctors": [],
            }

        # A significant number of failed cards is suspicious.
        failure_ratio = (
            failed_cards / len(cards)
            if cards
            else 1
        )

        if failure_ratio >= 0.25:

            print(
                f"      [WARN] Page {page_number} "
                f"has a high card extraction "
                f"failure rate: "
                f"{failed_cards}/{len(cards)}"
            )

            return {
                "status": "SUSPICIOUS",
                "cards": cards,
                "doctors": doctors,
            }

        return {
            "status": "OK",
            "cards": cards,
            "doctors": doctors,
        }

    except WebDriverException as e:

        print(
            f"      [ERROR] WebDriver error "
            f"on page {page_number}: {e}"
        )

        if is_browser_crash_error(e):

            print(
                "      [CRITICAL] Browser/session "
                "appears dead. Recreating driver..."
            )

            recreate_driver()

        return {
            "status": "ERROR",
            "cards": [],
            "doctors": [],
        }

    except Exception as e:

        print(
            f"      [ERROR] Failed loading "
            f"page {page_number}: {e}"
        )

        return {
            "status": "ERROR",
            "cards": [],
            "doctors": [],
        }


# ============================================================
# DRIVER RESTART
# ============================================================

def recreate_driver():

    global driver

    try:
        driver.quit()
    except Exception:
        pass

    time.sleep(2)

    driver = create_driver()


# ============================================================
# PROFILE SCRAPER
# ============================================================

def scrape_inner_profile(
    url,
    basic_info
):

    profile_url = url

    try:

        driver.get(
            profile_url
        )

        # Wait for document
        try:

            WebDriverWait(
                driver,
                15
            ).until(
                lambda d:
                d.execute_script(
                    "return document.readyState"
                )
                in (
                    "interactive",
                    "complete"
                )
            )

        except Exception:
            pass

        time.sleep(
            PROFILE_INITIAL_WAIT
        )

        # ----------------------------------------------------
        # PHONE / EMAIL / ADDRESS
        # ----------------------------------------------------

        phones = []
        emails = []
        possible_addresses = []

        try:

            WebDriverWait(
                driver,
                6
            ).until(
                EC.presence_of_element_located(
                    (
                        By.CSS_SELECTOR,
                        ".elementor-widget-icon-box"
                    )
                )
            )

        except Exception:
            pass

        try:

            box_titles = driver.find_elements(
                By.CSS_SELECTOR,
                ".elementor-widget-icon-box "
                ".elementor-icon-box-title span"
            )

            for title_el in box_titles:

                try:

                    text = (
                        title_el.text
                        or ""
                    ).strip()

                    if not text:
                        continue

                    # Email
                    if "@" in text:

                        if text not in emails:
                            emails.append(text)

                        continue

                    # Bulgarian phone patterns
                    normalized_phone = re.sub(
                        r"[\s\-\(\)]",
                        "",
                        text
                    )

                    if (
                        re.search(
                            r"(\+359|00359|08[789]|02)",
                            normalized_phone
                        )
                        and len(normalized_phone) < 25
                    ):

                        if text not in phones:
                            phones.append(text)

                        continue

                    # Candidate addresses
                    if len(text) > 10:

                        if text not in possible_addresses:
                            possible_addresses.append(text)

                except StaleElementReferenceException:
                    continue

        except Exception:
            pass

        # ----------------------------------------------------
        # GOOGLE MAP
        # ----------------------------------------------------

        map_pin_address = "-"
        clickable_map_link = "-"

        try:

            iframe = driver.find_element(
                By.CSS_SELECTOR,
                "iframe[src*='maps.google.com']"
            )

            raw_address = (
                iframe.get_attribute("title")
                or
                iframe.get_attribute("aria-label")
                or
                iframe.get_attribute("data-address")
            )

            if raw_address:

                map_pin_address = (
                    raw_address.strip()
                )

                clickable_map_link = (
                    "https://www.google.com/maps/search/"
                    "?api=1&query="
                    + urllib.parse.quote(
                        map_pin_address
                    )
                )

        except Exception:
            pass

        # ----------------------------------------------------
        # TEXT ADDRESS
        # ----------------------------------------------------

        text_address = (
            map_pin_address
            if map_pin_address != "-"
            else (
                possible_addresses[0]
                if possible_addresses
                else "-"
            )
        )

        # ----------------------------------------------------
        # BIOGRAPHY
        # ----------------------------------------------------

        full_bio = "-"

        try:

            bio_candidates = driver.find_elements(
                By.XPATH,
                "//div[contains("
                "@class, "
                "'jet-listing-dynamic-field__content'"
                ")]"
            )

            bio_parts = []

            for element in bio_candidates:

                try:

                    text = (
                        element.get_attribute(
                            "innerText"
                        )
                        or ""
                    ).strip()

                    if text:
                        bio_parts.append(text)

                except Exception:
                    continue

            if bio_parts:

                # Remove duplicates while preserving order
                unique_bio = list(
                    dict.fromkeys(bio_parts)
                )

                full_bio = " || ".join(
                    unique_bio
                ).replace(
                    "\n",
                    " || "
                )

        except Exception:
            pass

        # ----------------------------------------------------
        # BREADCRUMB
        # ----------------------------------------------------

        breadcrumb_info = "-"

        try:

            breadcrumb_el = driver.find_element(
                By.ID,
                "breadcrumbs"
            )

            breadcrumb_info = (
                breadcrumb_el.text
                or ""
            ).strip()

        except Exception:
            pass

        # ----------------------------------------------------
        # FINAL DATA
        # ----------------------------------------------------

        basic_info.update(
            {
                "Телефони": (
                    ", ".join(phones)
                    if phones
                    else "-"
                ),

                "Email": (
                    ", ".join(emails)
                    if emails
                    else "-"
                ),

                "Адрес (Текст)":
                    text_address,

                "Адрес (Google Maps Pin)":
                    map_pin_address,

                "Google Maps Link":
                    clickable_map_link,

                "Breadcrumb (Текст)":
                    breadcrumb_info,

                "Биография":
                    full_bio,

                "Note":
                    "-",

                "Timestamp":
                    datetime.now().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
            }
        )

        return basic_info

    except WebDriverException as e:

        if is_browser_crash_error(e):
            raise

        basic_info.update(
            {
                "Note":
                    "Profile Scrape Failed"
            }
        )

        return basic_info

    except Exception:

        basic_info.update(
            {
                "Note":
                    "Profile Scrape Failed"
            }
        )

        return basic_info


# ============================================================
# SCRAPE ONE PROFILE WITH RETRIES
# ============================================================

def scrape_profile_with_retries(
    doc,
    page_number
):

    raw_url = doc["RAW_URL"]
    decoded_url = doc["URL"]

    for attempt in range(
        1,
        MAX_PROFILE_RETRIES + 1
    ):

        if time_limit_reached():

            print(
                "[WARN] Time limit reached "
                "while scraping profile."
            )

            return False

        print(
            f"      [PROFILE] "
            f"{doc['Име']} "
            f"(attempt {attempt}/"
            f"{MAX_PROFILE_RETRIES})"
        )

        try:

            basic_info = {
                "Име": doc["Име"],
                "URL": decoded_url,
                "Описание (Лист)": "-"
            }

            full_data = (
                scrape_inner_profile(
                    raw_url,
                    basic_info
                )
            )

            # ------------------------------------------------
            # IMPORTANT:
            # Don't mark the profile as parsed if
            # profile scraping completely failed.
            # ------------------------------------------------

            if (
                full_data.get("Note")
                == "Profile Scrape Failed"
            ):

                raise RuntimeError(
                    "Profile returned "
                    "Profile Scrape Failed"
                )

            saved = save_single_record(
                full_data
            )

            if not saved:

                raise RuntimeError(
                    "CSV save failed"
                )

            # Only now mark as parsed
            mark_as_parsed(
                raw_url
            )

            print(
                f"      [SUCCESS] "
                f"Saved: {doc['Име']}"
            )

            return True

        except WebDriverException as e:

            print(
                f"      [WARN] Profile WebDriver "
                f"error: {e}"
            )

            if is_browser_crash_error(e):

                recreate_driver()

            if attempt < MAX_PROFILE_RETRIES:

                time.sleep(
                    RETRY_DELAY_SECONDS
                )

        except Exception as e:

            print(
                f"      [WARN] Profile scrape "
                f"failed: {e}"
            )

            if attempt < MAX_PROFILE_RETRIES:

                time.sleep(
                    RETRY_DELAY_SECONDS
                )

    # --------------------------------------------------------
    # All profile attempts failed
    # --------------------------------------------------------

    print(
        f"      [FAILED] Could not scrape "
        f"{doc['Име']} after "
        f"{MAX_PROFILE_RETRIES} attempts."
    )

    add_failed_profile(
        doc["Име"],
        raw_url,
        page_number,
        "All scrape attempts failed"
    )

    return False


# ============================================================
# PROCESS DOCTORS FROM PAGE
# ============================================================

def process_doctors(
    doctors,
    page_number
):

    processed = 0
    skipped = 0
    failed = 0

    total = len(doctors)

    for index, doc in enumerate(
        doctors,
        start=1
    ):

        if time_limit_reached():
            break

        print(
            f"      [{index}/{total}] "
            f"{doc['Име']}"
        )

        raw_url = doc["RAW_URL"]
        decoded_url = doc["URL"]

        if is_already_parsed(
            decoded_url
        ):

            print(
                "          [SKIP] "
                "Already parsed."
            )

            skipped += 1

            continue

        success = (
            scrape_profile_with_retries(
                doc,
                page_number
            )
        )

        if success:
            processed += 1
        else:
            failed += 1

    return {
        "processed": processed,
        "skipped": skipped,
        "failed": failed,
    }


# ============================================================
# PROCESS ONE PAGE
# ============================================================

def process_page(
    page_number,
    phase_name
):

    print()
    print("=" * 70)
    print(
        f"{phase_name} | PAGE {page_number}"
    )
    print("=" * 70)

    for page_attempt in range(
        1,
        MAX_PAGE_LOAD_RETRIES + 1
    ):

        if time_limit_reached():
            return {
                "success": False,
                "end": False,
                "retry": True,
                "reason": "TIME_LIMIT",
            }

        print(
            f"  [PAGE ATTEMPT "
            f"{page_attempt}/"
            f"{MAX_PAGE_LOAD_RETRIES}]"
        )

        result = load_page(
            page_number,
            page_attempt
        )

        status = result["status"]

        # ----------------------------------------------------
        # Legitimate end of pagination
        # ----------------------------------------------------

        if status in (
            "404",
            "NO_DATA"
        ):

            print(
                f"  [END] Page {page_number} "
                f"signals end of database."
            )

            return {
                "success": True,
                "end": True,
                "retry": False,
                "reason": status,
            }

        doctors = result.get(
            "doctors",
            []
        )

        # ----------------------------------------------------
        # Successful / suspicious page with doctors
        # ----------------------------------------------------

        if doctors:

            # Save all discovered URLs,
            # even if some are already parsed.
            page_discovered_urls[
                str(page_number)
            ] = {
                "timestamp":
                    datetime.now().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),

                "card_count":
                    len(result.get(
                        "cards",
                        []
                    )),

                "extracted_count":
                    len(doctors),

                "status":
                    status,

                "urls":
                    [
                        normalize_url(
                            d["URL"]
                        )
                        for d in doctors
                    ],
            }

            save_page_discovered_urls()

            stats = process_doctors(
                doctors,
                page_number
            )

            print()
            print(
                f"  [PAGE SUMMARY] "
                f"Page {page_number}"
            )

            print(
                f"      Extracted: "
                f"{len(doctors)}"
            )

            print(
                f"      Processed: "
                f"{stats['processed']}"
            )

            print(
                f"      Skipped: "
                f"{stats['skipped']}"
            )

            print(
                f"      Failed profiles: "
                f"{stats['failed']}"
            )

            # If page itself was suspicious,
            # keep it for Phase 2.
            if status in (
                "BROKEN",
                "SUSPICIOUS"
            ):

                print(
                    f"  [WARN] Page "
                    f"{page_number} is marked "
                    f"for Phase 2 verification."
                )

                add_failed_page(
                    page_number
                )

                return {
                    "success": True,
                    "end": False,
                    "retry": False,
                    "reason": "SUSPICIOUS",
                }

            remove_failed_page(
                page_number
            )

            return {
                "success": True,
                "end": False,
                "retry": False,
                "reason": "OK",
            }

        # ----------------------------------------------------
        # No doctors extracted
        # ----------------------------------------------------

        print(
            f"  [WARN] Page {page_number} "
            f"returned no doctor URLs."
        )

        if page_attempt < MAX_PAGE_LOAD_RETRIES:

            print(
                "  [INFO] Retrying page..."
            )

            time.sleep(
                RETRY_DELAY_SECONDS
            )

        else:

            print(
                f"  [FAILED PAGE] "
                f"Page {page_number} "
                f"will be retried in Phase 2."
            )

            add_failed_page(
                page_number
            )

            return {
                "success": False,
                "end": False,
                "retry": True,
                "reason": status,
            }

    return {
        "success": False,
        "end": False,
        "retry": True,
        "reason": "UNKNOWN",
    }


# ============================================================
# PHASE 2
# ============================================================

def run_phase_2():

    print()
    print("=" * 70)
    print("PHASE 2 | RECHECK PROBLEMATIC PAGES")
    print("=" * 70)

    while True:

        if time_limit_reached():

            print(
                "[WARN] Time limit reached "
                "during Phase 2."
            )

            return False

        failed_pages = (
            load_failed_pages()
        )

        if not failed_pages:

            print(
                "[SUCCESS] Phase 2 complete. "
                "No failed pages remain."
            )

            return True

        print(
            f"[INFO] Pages awaiting "
            f"verification: {failed_pages}"
        )

        target_page = failed_pages[0]

        result = process_page(
            target_page,
            "PHASE 2"
        )

        if result["end"]:

            remove_failed_page(
                target_page
            )

            continue

        # If page succeeded, process_page()
        # may already have removed it.
        if (
            target_page
            not in load_failed_pages()
        ):
            continue

        # If it still failed, rotate it to
        # the end of the queue.
        failed_pages = (
            load_failed_pages()
        )

        if target_page in failed_pages:

            failed_pages.remove(
                target_page
            )

            failed_pages.append(
                target_page
            )

            save_failed_pages(
                failed_pages
            )

        time.sleep(1)


# ============================================================
# MAIN PIPELINE
# ============================================================

if os.path.exists(
    CONTINUE_FLAG_FILE
):
    try:
        os.remove(
            CONTINUE_FLAG_FILE
        )
    except Exception:
        pass


current_phase = state.get(
    "phase",
    1
)

page = int(
    state.get(
        "page",
        1
    )
)

consecutive_fails = int(
    state.get(
        "consecutive_fails",
        0
    )
)


print()
print("=" * 70)
print("ZDRAVEN ARHIV DOCTORS SCRAPER")
print("=" * 70)
print(
    f"Starting phase: {current_phase}"
)
print(
    f"Starting page: {page}"
)
print(
    f"Cached parsed URLs: "
    f"{len(parsed_urls)}"
)
print(
    f"Time limit: "
    f"{TIME_LIMIT_SECONDS / 3600:.2f} hours"
)
print("=" * 70)


try:

    # ========================================================
    # PHASE 1
    # ========================================================

    if current_phase == 1:

        while True:

            if time_limit_reached():

                print(
                    "[WARN] Time limit reached. "
                    "Saving state."
                )

                save_state(
                    page,
                    phase=1,
                    consecutive_fails=(
                        consecutive_fails
                    )
                )

                with open(
                    CONTINUE_FLAG_FILE,
                    "w",
                    encoding="utf-8"
                ) as f:
                    f.write(
                        "CONTINUE_REQUIRED"
                    )

                break

            result = process_page(
                page,
                "PHASE 1"
            )

            # ------------------------------------------------
            # End of database
            # ------------------------------------------------

            if result["end"]:

                print()
                print(
                    "[INFO] Phase 1 reached "
                    "the end of the database."
                )

                current_phase = 2

                save_state(
                    1,
                    phase=2,
                    consecutive_fails=0
                )

                break

            # ------------------------------------------------
            # Successful page
            # ------------------------------------------------

            if result["success"]:

                consecutive_fails = 0

            else:

                consecutive_fails += 1

                print(
                    f"[WARN] Consecutive page "
                    f"failure count: "
                    f"{consecutive_fails}"
                )

            # ------------------------------------------------
            # Move to next page
            # ------------------------------------------------

            page += 1

            save_state(
                page,
                phase=1,
                consecutive_fails=(
                    consecutive_fails
                )
            )

            # ------------------------------------------------
            # Do not blindly assume 10 broken pages
            # means end of database.
            #
            # We only move on when we've actually reached
            # an explicit 404 / no-data response.
            # ------------------------------------------------

    # ========================================================
    # PHASE 2
    # ========================================================

    if current_phase == 2:

        completed = run_phase_2()

        if not completed:

            print(
                "[WARN] Phase 2 did not "
                "finish before time limit."
            )

            save_state(
                page,
                phase=2,
                consecutive_fails=0
            )

            with open(
                CONTINUE_FLAG_FILE,
                "w",
                encoding="utf-8"
            ) as f:
                f.write(
                    "CONTINUE_REQUIRED"
                )

        else:

            print()
            print(
                "=" * 70
            )

            print(
                "[SUCCESS] SCRAPING COMPLETE."
            )

            print(
                f"Parsed URL cache size: "
                f"{len(parsed_urls)}"
            )

            print(
                f"Output CSV: "
                f"{current_batch_filename}"
            )

            print(
                f"Failed profile log: "
                f"{failed_profiles_file}"
            )

            print(
                f"Page discovery log: "
                f"{page_discovered_urls_file}"
            )

            print(
                "=" * 70
            )


except KeyboardInterrupt:

    print()
    print(
        "[WARN] Keyboard interrupt received. "
        "Saving current state..."
    )

    save_state(
        page,
        phase=current_phase,
        consecutive_fails=consecutive_fails
    )

    try:
        with open(
            CONTINUE_FLAG_FILE,
            "w",
            encoding="utf-8"
        ) as f:
            f.write(
                "CONTINUE_REQUIRED"
            )
    except Exception:
        pass


except Exception as e:

    print()
    print(
        f"[CRITICAL] Global pipeline failure: "
        f"{e}"
    )

    save_state(
        page,
        phase=current_phase,
        consecutive_fails=consecutive_fails
    )

    try:
        with open(
            CONTINUE_FLAG_FILE,
            "w",
            encoding="utf-8"
        ) as f:
            f.write(
                "CONTINUE_REQUIRED"
            )
    except Exception:
        pass


finally:

    try:
        driver.quit()
    except Exception:
        pass

    print()
    print(
        "[INFO] WebDriver closed."
    )

    print(
        "[INFO] Execution block concluded."
    )
