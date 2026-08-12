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
    StaleElementReferenceException,
)


# ============================================================
# CONFIGURATION
# ============================================================

START_TIME = time.time()

# Same time limit as your previous scraper
TIME_LIMIT_SECONDS = 5.4 * 60 * 60

BASE_URL = "https://zdraven-arhiv.com/doctors/"

# Old scraper proved these wait times work well.
PAGE1_WAIT_SECONDS = 10
OTHER_PAGE_WAIT_SECONDS = 5

# Profile wait
PROFILE_WAIT_SECONDS = 5
PROFILE_INITIAL_SLEEP = 1.5

# Retries
MAX_PAGE_RETRIES = 3
MAX_PROFILE_RETRIES = 3

# Small delay between retries
RETRY_DELAY_SECONDS = 2


# ============================================================
# PATHS
# ============================================================

try:
    base_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    base_dir = os.getcwd()

output_dir = os.path.join(
    base_dir,
    "zdraven_arhiv_outputs"
)

os.makedirs(
    output_dir,
    exist_ok=True
)

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
# OUTPUT SCHEMA
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
# TIME HELPERS
# ============================================================

def time_limit_reached():
    return (
        time.time() - START_TIME
    ) >= TIME_LIMIT_SECONDS


# ============================================================
# STATE
# ============================================================

state = {
    "page": 1,
    "phase": 1,
    "consecutive_fails": 0
}


if os.path.exists(state_file):

    try:

        with open(
            state_file,
            "r",
            encoding="utf-8"
        ) as f:

            state = json.load(f)

        print(
            f"[INFO] Resuming from "
            f"Phase {state.get('phase', 1)}, "
            f"Page {state.get('page', 1)}."
        )

    except Exception as e:

        print(
            f"[WARN] State file could not be loaded: {e}"
        )

        state = {
            "page": 1,
            "phase": 1,
            "consecutive_fails": 0
        }


def save_state(
    page,
    phase=1,
    consecutive_fails=0
):

    payload = {
        "page": page,
        "phase": phase,
        "consecutive_fails": consecutive_fails,
        "saved_at": datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    }

    temp_file = state_file + ".tmp"

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
                indent=2
            )

        os.replace(
            temp_file,
            state_file
        )

    except Exception as e:

        print(
            f"[ERROR] Could not save state: {e}"
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

                parsed_urls.add(
                    urllib.parse.unquote(url)
                )

                parsed_urls.add(url)

    except Exception as e:

        print(
            f"[WARN] Could not load parsed URL memory: {e}"
        )


print(
    f"[INFO] Loaded "
    f"{len(parsed_urls)} cached URLs."
)


def normalize_url(url):

    if not url:
        return ""

    url = url.strip()

    return urllib.parse.unquote(url)


def is_already_parsed(url):

    decoded = normalize_url(url)

    return (
        decoded in parsed_urls
        or url in parsed_urls
    )


def mark_as_parsed(url):

    decoded = normalize_url(url)

    parsed_urls.add(decoded)
    parsed_urls.add(url)

    try:

        with open(
            memory_file,
            "a",
            encoding="utf-8"
        ) as f:

            f.write(
                decoded + "\n"
            )

    except Exception as e:

        print(
            f"[ERROR] Could not save parsed URL: {e}"
        )


# ============================================================
# FAILED PAGE MANAGEMENT
# ============================================================

def load_failed_pages():

    if not os.path.exists(
        failed_pages_file
    ):
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

        pages = sorted(
            set(
                int(x)
                for x in pages
            )
        )

        with open(
            failed_pages_file,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                pages,
                f,
                ensure_ascii=False,
                indent=2
            )

    except Exception as e:

        print(
            f"[ERROR] Could not save failed pages: {e}"
        )


def add_failed_page(page):

    pages = load_failed_pages()

    if page not in pages:
        pages.append(page)

    save_failed_pages(pages)


def remove_failed_page(page):

    pages = load_failed_pages()

    if page in pages:
        pages.remove(page)

    save_failed_pages(pages)


# ============================================================
# FAILED PROFILE MANAGEMENT
# ============================================================

def load_failed_profiles():

    if not os.path.exists(
        failed_profiles_file
    ):
        return []

    try:

        with open(
            failed_profiles_file,
            "r",
            encoding="utf-8"
        ) as f:

            data = json.load(f)

        if isinstance(data, list):
            return data

    except Exception:
        pass

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
                indent=2
            )

    except Exception as e:

        print(
            f"[ERROR] Could not save failed profiles: {e}"
        )


def add_failed_profile(
    name,
    url,
    page,
    reason=""
):

    profiles = load_failed_profiles()

    decoded_url = normalize_url(url)

    for existing in profiles:

        if normalize_url(
            existing.get("URL", "")
        ) == decoded_url:

            return

    profiles.append(
        {
            "Име": name,
            "URL": decoded_url,
            "Page": page,
            "Reason": reason,
            "Timestamp": datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        }
    )

    save_failed_profiles(profiles)


# ============================================================
# PAGE DISCOVERY LOG
# ============================================================

def load_page_discovered_urls():

    if not os.path.exists(
        page_discovered_urls_file
    ):
        return {}

    try:

        with open(
            page_discovered_urls_file,
            "r",
            encoding="utf-8"
        ) as f:

            data = json.load(f)

        if isinstance(data, dict):
            return data

    except Exception:
        pass

    return {}


page_discovered_urls = (
    load_page_discovered_urls()
)


def save_page_discovered_urls():

    try:

        temp_file = (
            page_discovered_urls_file
            + ".tmp"
        )

        with open(
            temp_file,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                page_discovered_urls,
                f,
                ensure_ascii=False,
                indent=2
            )

        os.replace(
            temp_file,
            page_discovered_urls_file
        )

    except Exception as e:

        print(
            f"[ERROR] Could not save page discovery log: {e}"
        )


# ============================================================
# CSV INITIALIZATION
# ============================================================

if not os.path.exists(
    current_batch_filename
):

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
            f"[ERROR] Could not create CSV: {e}"
        )


# ============================================================
# WEBDRIVER
# ============================================================

def create_driver():

    print(
        "[INFO] Starting Chrome..."
    )

    options = Options()

    options.add_argument(
        "--headless=new"
    )

    options.add_argument(
        "--no-sandbox"
    )

    options.add_argument(
        "--disable-dev-shm-usage"
    )

    options.add_argument(
        "--disable-gpu"
    )

    options.add_argument(
        "--window-size=1920,1080"
    )

    options.add_argument(
        "--user-agent="
        "Mozilla/5.0 "
        "(Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 "
        "(KHTML, like Gecko) "
        "Chrome/120.0.0.0 "
        "Safari/537.36"
    )

    try:

        service = Service(
            ChromeDriverManager().install()
        )

        drv = webdriver.Chrome(
            service=service,
            options=options
        )

        drv.set_page_load_timeout(30)

        print(
            "[INFO] Chrome started."
        )

        return drv

    except Exception as e:

        print(
            f"[CRITICAL] Chrome failed to start: {e}"
        )

        raise


driver = create_driver()


# ============================================================
# DRIVER RESTART
# ============================================================

def restart_driver():

    global driver

    print(
        "[INFO] Restarting Chrome..."
    )

    try:
        driver.quit()
    except Exception:
        pass

    time.sleep(2)

    driver = create_driver()


# ============================================================
# CSV WRITER
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

        print(
            f"💾 Saved: "
            f"{record.get('Име', '-')}"
        )

        return True

    except Exception as e:

        print(
            f"❌ CSV save error: {e}"
        )

        return False


# ============================================================
# PAGE URL
# ============================================================

def build_page_url(page):

    if page == 1:

        return BASE_URL

    return (
        f"{BASE_URL}"
        f"?jsf=jet-engine"
        f"&pagenum={page}"
    )


# ============================================================
# EXACT OLD PAGE PARSING LOGIC
# ============================================================
#
# This deliberately follows your older working scraper:
#
#     driver.get()
#     wait for jet-listing-grid__item
#     find all cards
#     extract a.jet-listing-dynamic-link__link
#
# No "wait until card count stabilizes".
# No alternate card selector roulette.
# No aggressive DOM heuristics.
#
# Because the old logic works.
#
# ============================================================

def parse_listing_page(page):

    target_url = build_page_url(page)

    print()
    print("=" * 70)
    print(
        f"📄 PAGE {page}"
    )
    print(
        target_url
    )
    print("=" * 70)

    driver.get(
        target_url
    )

    # Same behavior as old scraper:
    # page 1 gets slightly longer initial wait.
    wait_time = (
        PAGE1_WAIT_SECONDS
        if page == 1
        else OTHER_PAGE_WAIT_SECONDS
    )

    # --------------------------------------------------------
    # 404 detection
    # --------------------------------------------------------

    try:

        if (
            "404"
            in (driver.title or "")
        ):

            print(
                "⛔ 404 detected."
            )

            return {
                "status": "END",
                "doctors": [],
                "cards": []
            }

        if (
            "Страницата не е намерена"
            in (driver.page_source or "")
        ):

            print(
                "⛔ 'Страницата не е намерена' detected."
            )

            return {
                "status": "END",
                "doctors": [],
                "cards": []
            }

    except Exception:
        pass

    # --------------------------------------------------------
    # Wait exactly like old scraper
    # --------------------------------------------------------

    try:

        WebDriverWait(
            driver,
            wait_time
        ).until(
            EC.presence_of_element_located(
                (
                    By.CLASS_NAME,
                    "jet-listing-grid__item"
                )
            )
        )

    except TimeoutException:

        print(
            f"⛔ No listing cards appeared "
            f"within {wait_time} seconds."
        )

        return {
            "status": "FAILED",
            "doctors": [],
            "cards": []
        }

    # --------------------------------------------------------
    # Find cards exactly like old scraper
    # --------------------------------------------------------

    cards = driver.find_elements(
        By.XPATH,
        "//div[contains(@class, "
        "'jet-listing-grid__item')]"
    )

    if not cards:

        print(
            "⛔ No cards found."
        )

        return {
            "status": "FAILED",
            "doctors": [],
            "cards": []
        }

    print(
        f"🔎 Found {len(cards)} cards."
    )

    doctors_on_page = []

    # ========================================================
    # CARD PARSING
    # ========================================================

    for card_index, card in enumerate(
        cards,
        start=1
    ):

        try:

            # ------------------------------------------------
            # EXACT working selector from old scraper
            # ------------------------------------------------

            link_el = card.find_element(
                By.CSS_SELECTOR,
                "a.jet-listing-dynamic-link__link"
            )

            raw_url = (
                link_el.get_attribute(
                    "href"
                )
            )

            name = (
                link_el.text
                or ""
            ).strip()

            if not raw_url:
                continue

            # ------------------------------------------------
            # DESCRIPTION FROM LISTING CARD
            # ------------------------------------------------
            #
            # This is the important fix.
            #
            # The old code put "-"
            # here, even though the listing card
            # clearly contains description text.
            #
            # First, try JetEngine's own dynamic
            # field content inside the card.
            #
            # Then fallback to card text.
            #
            # ------------------------------------------------

            description = extract_listing_description(
                card,
                name
            )

            doc_data = {
                "Име": name,
                "RAW_URL": raw_url,
                "URL": normalize_url(raw_url),
                "Описание (Лист)": description
            }

            doctors_on_page.append(
                doc_data
            )

        except Exception as e:

            print(
                f"⚠️ Card #{card_index} "
                f"could not be parsed: {e}"
            )

            continue

    # --------------------------------------------------------
    # Page discovery logging
    # --------------------------------------------------------

    page_discovered_urls[
        str(page)
    ] = {
        "timestamp":
            datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        "card_count":
            len(cards),
        "doctor_count":
            len(doctors_on_page),
        "urls":
            [
                x["URL"]
                for x in doctors_on_page
            ]
    }

    save_page_discovered_urls()

    print(
        f"✅ Extracted "
        f"{len(doctors_on_page)} doctors "
        f"from {len(cards)} cards."
    )

    return {
        "status": "OK",
        "doctors": doctors_on_page,
        "cards": cards
    }


# ============================================================
# DESCRIPTION EXTRACTION
# ============================================================

def clean_text(text):

    if not text:
        return ""

    text = text.replace(
        "\xa0",
        " "
    )

    text = re.sub(
        r"\s+",
        " ",
        text
    )

    return text.strip()


def extract_listing_description(
    card,
    doctor_name
):

    # ========================================================
    # METHOD 1
    # JetEngine dynamic field content
    # ========================================================

    try:

        elements = card.find_elements(
            By.XPATH,
            ".//*[contains("
            "@class,"
            "'jet-listing-dynamic-field__content'"
            ")]"
        )

        candidates = []

        for element in elements:

            try:

                text = (
                    element.get_attribute(
                        "innerText"
                    )
                    or ""
                )

                text = clean_text(text)

                if not text:
                    continue

                candidates.append(text)

            except Exception:
                continue

        # Remove exact duplicates
        candidates = list(
            dict.fromkeys(candidates)
        )

        # ----------------------------------------------------
        # Pick the most description-like field.
        #
        # Description normally contains considerably more
        # text than the address field.
        # ----------------------------------------------------

        filtered = []

        for text in candidates:

            if (
                text == clean_text(
                    doctor_name
                )
            ):
                continue

            if text == "Разгледай":
                continue

            filtered.append(text)

        # Strong candidate:
        # a reasonably long text which isn't just an address.
        description_candidates = []

        for text in filtered:

            # Skip obvious addresses
            if re.match(
                r"^(гр\.|с\.|ул\.|бул\.|кв\.|"
                r"\d{4}|ж\.к\.)",
                text,
                re.IGNORECASE
            ):
                continue

            if len(text) >= 30:
                description_candidates.append(
                    text
                )

        if description_candidates:

            # Usually the first dynamic text field
            # after the title is the listing description.
            return description_candidates[0]

        # If there isn't a long candidate,
        # return the first non-title candidate.
        if filtered:

            return filtered[0]

    except Exception:
        pass

    # ========================================================
    # METHOD 2
    # Card text fallback
    # ========================================================

    try:

        card_text = (
            card.text
            or ""
        ).strip()

        if card_text:

            lines = [
                clean_text(x)
                for x in card_text.splitlines()
                if clean_text(x)
            ]

            doctor_name_clean = clean_text(
                doctor_name
            )

            # Remove name
            remaining = []

            for line in lines:

                if line == doctor_name_clean:
                    continue

                if line == "Разгледай":
                    continue

                remaining.append(line)

            # Description is usually the first substantial
            # sentence after the name.
            for line in remaining:

                if len(line) < 25:
                    continue

                if re.match(
                    r"^(гр\.|с\.|ул\.|бул\.|кв\.|"
                    r"\d{4}|ж\.к\.)",
                    line,
                    re.IGNORECASE
                ):
                    continue

                return line

    except Exception:
        pass

    return "-"


# ============================================================
# PROFILE SCRAPER
# ============================================================

def scrape_inner_profile(
    url,
    basic_info
):

    print(
        f"   👉 Visiting: {url}"
    )

    try:

        driver.get(
            url
        )

        # Same basic profile behavior as your old,
        # working scraper.
        time.sleep(
            PROFILE_INITIAL_SLEEP
        )

        try:

            WebDriverWait(
                driver,
                PROFILE_WAIT_SECONDS
            ).until(
                EC.presence_of_element_located(
                    (
                        By.CLASS_NAME,
                        "elementor-widget-icon-box"
                    )
                )
            )

        except Exception:
            pass

        # ====================================================
        # PHONES / EMAILS / ADDRESSES
        # ====================================================

        phones = []
        emails = []
        possible_addresses = []

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

                    # Phone
                    if (
                        re.search(
                            r"(\+359|08[789]|02)",
                            text
                        )
                        and len(text) < 20
                    ):

                        if text not in phones:
                            phones.append(text)

                        continue

                    # Address
                    if len(text) > 10:

                        if (
                            text
                            not in
                            possible_addresses
                        ):

                            possible_addresses.append(
                                text
                            )

                except (
                    StaleElementReferenceException
                ):
                    continue

        except Exception as e:

            print(
                f"⚠️ Could not parse "
                f"icon boxes: {e}"
            )

        # ====================================================
        # GOOGLE MAP
        # ====================================================

        map_pin_address = "-"
        clickable_map_link = "-"

        try:

            iframe = driver.find_element(
                By.CSS_SELECTOR,
                "iframe[src*='maps.google.com']"
            )

            raw_address = (
                iframe.get_attribute(
                    "title"
                )
                or
                iframe.get_attribute(
                    "aria-label"
                )
            )

            if raw_address:

                map_pin_address = (
                    raw_address.strip()
                )

                encoded_address = (
                    urllib.parse.quote(
                        raw_address
                    )
                )

                clickable_map_link = (
                    "https://www.google.com/maps/"
                    "search/?api=1&query="
                    + encoded_address
                )

        except Exception:
            pass

        # ====================================================
        # TEXT ADDRESS
        # ====================================================

        text_address = (
            map_pin_address
            if map_pin_address != "-"
            else (
                possible_addresses[0]
                if possible_addresses
                else "-"
            )
        )

        # ====================================================
        # BIOGRAPHY
        # ====================================================

        full_bio = "-"

        try:

            bio_el = driver.find_element(
                By.XPATH,
                "//div[contains("
                "@class, "
                "'jet-listing-dynamic-field__content'"
                ")]"
            )

            full_bio = (
                bio_el.get_attribute(
                    "innerText"
                )
                or ""
            ).strip().replace(
                "\n",
                " || "
            )

        except Exception:
            pass

        # ====================================================
        # BREADCRUMB
        # ====================================================

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

        # ====================================================
        # FINAL RECORD
        # ====================================================

        basic_info.update(
            {
                # NOTE:
                # "Описание (Лист)" is NOT overwritten here.
                # It comes from the listing page.

                "Телефони":
                    ", ".join(phones)
                    if phones
                    else "-",

                "Email":
                    ", ".join(emails)
                    if emails
                    else "-",

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
                    )
            }
        )

        return basic_info

    except WebDriverException as e:

        # Browser completely crashed
        if any(
            x in str(e).lower()
            for x in [
                "crashed",
                "disconnected",
                "out of memory",
                "chrome not reachable",
                "invalid session id"
            ]
        ):
            raise

        print(
            f"💀 Profile WebDriver error: {e}"
        )

        basic_info.update(
            {
                "Note":
                    "Profile Scrape Failed"
            }
        )

        return basic_info

    except Exception as e:

        print(
            f"💀 Profile error: {e}"
        )

        basic_info.update(
            {
                "Note":
                    "Profile Scrape Failed"
            }
        )

        return basic_info


# ============================================================
# PROFILE RETRY
# ============================================================

def scrape_profile_with_retries(
    doc,
    page
):

    for attempt in range(
        1,
        MAX_PROFILE_RETRIES + 1
    ):

        if time_limit_reached():
            return False

        try:

            print(
                f"   [{attempt}/"
                f"{MAX_PROFILE_RETRIES}] "
                f"{doc['Име']}"
            )

            # IMPORTANT:
            # Create fresh basic_info every attempt
            # so previous failed data cannot leak.
            basic_info = {
                "Име":
                    doc["Име"],

                "URL":
                    doc["URL"],

                "Описание (Лист)":
                    doc.get(
                        "Описание (Лист)",
                        "-"
                    )
            }

            full_data = scrape_inner_profile(
                doc["RAW_URL"],
                basic_info
            )

            # A real scrape failure should not
            # be marked as successfully parsed.
            if (
                full_data.get("Note")
                ==
                "Profile Scrape Failed"
            ):

                raise RuntimeError(
                    "Profile scrape returned failure."
                )

            if not save_single_record(
                full_data
            ):

                raise RuntimeError(
                    "Failed to save record."
                )

            # Only mark parsed AFTER successful save.
            mark_as_parsed(
                doc["RAW_URL"]
            )

            return True

        except WebDriverException as e:

            print(
                f"⚠️ WebDriver error on "
                f"{doc['Име']}: {e}"
            )

            try:

                restart_driver()

            except Exception:
                pass

            if attempt < MAX_PROFILE_RETRIES:
                time.sleep(
                    RETRY_DELAY_SECONDS
                )

        except Exception as e:

            print(
                f"⚠️ Profile failed: {e}"
            )

            if attempt < MAX_PROFILE_RETRIES:
                time.sleep(
                    RETRY_DELAY_SECONDS
                )

    # --------------------------------------------------------
    # Completely failed
    # --------------------------------------------------------

    print(
        f"❌ FAILED PROFILE: "
        f"{doc['Име']}"
    )

    add_failed_profile(
        doc["Име"],
        doc["URL"],
        page,
        "All profile attempts failed"
    )

    return False


# ============================================================
# PROCESS PAGE'S DOCTORS
# ============================================================

def process_doctors(
    doctors,
    page
):

    processed = 0
    skipped = 0
    failed = 0

    for index, doc in enumerate(
        doctors,
        start=1
    ):

        if time_limit_reached():
            break

        print()
        print(
            f"👨‍⚕️ [{index}/{len(doctors)}] "
            f"{doc['Име']}"
        )

        # ----------------------------------------------------
        # Resume logic
        # ----------------------------------------------------

        if is_already_parsed(
            doc["URL"]
        ):

            print(
                "   ⏭️ Already parsed."
            )

            skipped += 1

            continue

        # ----------------------------------------------------
        # Profile
        # ----------------------------------------------------

        success = (
            scrape_profile_with_retries(
                doc,
                page
            )
        )

        if success:
            processed += 1
        else:
            failed += 1

    return {
        "processed":
            processed,

        "skipped":
            skipped,

        "failed":
            failed
    }


# ============================================================
# PROCESS SINGLE PAGE
# ============================================================

def process_page(
    page,
    phase="Phase 1"
):

    for attempt in range(
        1,
        MAX_PAGE_RETRIES + 1
    ):

        if time_limit_reached():

            return {
                "status":
                    "TIME_LIMIT"
            }

        print(
            f"\n🔄 {phase} | "
            f"Page {page} | "
            f"Attempt {attempt}/"
            f"{MAX_PAGE_RETRIES}"
        )

        try:

            result = parse_listing_page(
                page
            )

            status = result["status"]

            # ------------------------------------------------
            # REAL END
            # ------------------------------------------------

            if status == "END":

                return {
                    "status":
                        "END"
                }

            # ------------------------------------------------
            # FAILED PAGE
            # ------------------------------------------------

            if status == "FAILED":

                if (
                    attempt
                    <
                    MAX_PAGE_RETRIES
                ):

                    print(
                        "🔁 Retrying page..."
                    )

                    time.sleep(
                        RETRY_DELAY_SECONDS
                    )

                    continue

                print(
                    f"❌ Page {page} "
                    f"failed after "
                    f"{MAX_PAGE_RETRIES} attempts."
                )

                add_failed_page(
                    page
                )

                return {
                    "status":
                        "FAILED"
                }

            # ------------------------------------------------
            # PAGE LOADED
            # ------------------------------------------------

            doctors = (
                result["doctors"]
            )

            if not doctors:

                print(
                    "⚠️ Page loaded but "
                    "no doctors were extracted."
                )

                if (
                    attempt
                    <
                    MAX_PAGE_RETRIES
                ):

                    time.sleep(
                        RETRY_DELAY_SECONDS
                    )

                    continue

                add_failed_page(
                    page
                )

                return {
                    "status":
                        "FAILED"
                }

            # ------------------------------------------------
            # PROCESS DOCTORS
            # ------------------------------------------------

            stats = process_doctors(
                doctors,
                page
            )

            print()
            print(
                f"📊 PAGE {page} SUMMARY"
            )

            print(
                f"   Cards found: "
                f"{len(result['cards'])}"
            )

            print(
                f"   Doctors extracted: "
                f"{len(doctors)}"
            )

            print(
                f"   Newly processed: "
                f"{stats['processed']}"
            )

            print(
                f"   Already parsed: "
                f"{stats['skipped']}"
            )

            print(
                f"   Failed profiles: "
                f"{stats['failed']}"
            )

            # Page itself worked.
            remove_failed_page(
                page
            )

            return {
                "status":
                    "OK"
            }

        except WebDriverException as e:

            print(
                f"💥 WebDriver failure on "
                f"page {page}: {e}"
            )

            try:
                restart_driver()
            except Exception:
                pass

            if (
                attempt
                <
                MAX_PAGE_RETRIES
            ):

                time.sleep(
                    RETRY_DELAY_SECONDS
                )

                continue

        except Exception as e:

            print(
                f"🤬 Error on page {page}: {e}"
            )

            if (
                attempt
                <
                MAX_PAGE_RETRIES
            ):

                time.sleep(
                    RETRY_DELAY_SECONDS
                )

                continue

    add_failed_page(
        page
    )

    return {
        "status":
            "FAILED"
    }


# ============================================================
# PHASE 2: FAILED PAGES
# ============================================================

def run_phase_2():

    failed_pages = (
        load_failed_pages()
    )

    if not failed_pages:

        print(
            "[INFO] No failed pages."
        )

        return

    print()
    print("=" * 70)
    print(
        "🔁 PHASE 2 - RETRY FAILED PAGES"
    )
    print("=" * 70)

    # Work through a copy.
    for page in failed_pages:

        if time_limit_reached():

            print(
                "[WARN] Time limit reached "
                "during Phase 2."
            )

            return

        print(
            f"\n🔁 Retrying failed page "
            f"{page}"
        )

        result = process_page(
            page,
            phase="Phase 2"
        )

        if result["status"] == "OK":

            remove_failed_page(
                page
            )

        elif result["status"] == "END":

            remove_failed_page(
                page
            )


# ============================================================
# MAIN
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


current_phase = int(
    state.get(
        "phase",
        1
    )
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
print(
    "🚀 ZDRAVEN ARHIV SCRAPER"
)
print("=" * 70)
print(
    f"Starting phase: "
    f"{current_phase}"
)
print(
    f"Starting page: "
    f"{page}"
)
print(
    f"Cached URLs: "
    f"{len(parsed_urls)}"
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
                    "\n⏰ Time limit reached."
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
                phase="Phase 1"
            )

            # ------------------------------------------------
            # End of database
            # ------------------------------------------------

            if result["status"] == "END":

                print()
                print(
                    "🏁 Reached the end "
                    "of the database."
                )

                current_phase = 2

                save_state(
                    1,
                    phase=2,
                    consecutive_fails=0
                )

                break

            # ------------------------------------------------
            # Page success/failure
            # ------------------------------------------------

            if result["status"] == "OK":

                consecutive_fails = 0

            else:

                consecutive_fails += 1

                print(
                    f"⚠️ Page failure count: "
                    f"{consecutive_fails}"
                )

            # ------------------------------------------------
            # IMPORTANT:
            # Move sequentially exactly like the old
            # working scraper.
            # ------------------------------------------------

            page += 1

            save_state(
                page,
                phase=1,
                consecutive_fails=(
                    consecutive_fails
                )
            )

    # ========================================================
    # PHASE 2
    # ========================================================

    if current_phase == 2:

        run_phase_2()

        remaining_failed = (
            load_failed_pages()
        )

        if remaining_failed:

            print()
            print(
                "⚠️ Some pages still failed:"
            )

            print(
                remaining_failed
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
            print("=" * 70)
            print(
                "✅ SCRAPING COMPLETE"
            )
            print("=" * 70)

            print(
                f"Cached URLs: "
                f"{len(parsed_urls)}"
            )

            print(
                f"CSV: "
                f"{current_batch_filename}"
            )

            print(
                f"Failed profiles: "
                f"{failed_profiles_file}"
            )

            print(
                f"Page discovery: "
                f"{page_discovered_urls_file}"
            )

            print("=" * 70)


except KeyboardInterrupt:

    print(
        "\n🛑 Keyboard interrupt."
    )

    save_state(
        page,
        phase=current_phase,
        consecutive_fails=(
            consecutive_fails
        )
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

    print(
        f"\n💀 GLOBAL ERROR: {e}"
    )

    save_state(
        page,
        phase=current_phase,
        consecutive_fails=(
            consecutive_fails
        )
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
        "🛑 Chrome closed."
    )

    print(
        "🏁 Scraping session finished."
    )
