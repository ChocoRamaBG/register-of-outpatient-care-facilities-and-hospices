import csv
import json
import os
import re
import sys
import time
from datetime import datetime
from urllib.parse import unquote, urljoin, urlparse

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ============================================================
# CONFIG
# ============================================================

START_TIME = time.time()

# GitHub Actions normally allows up to 6 hours per job.
# Leave a safety margin so state can be committed cleanly.
TIME_LIMIT_SECONDS = float(os.getenv("TIME_LIMIT_SECONDS", str(5.4 * 60 * 60)))

BASE_SEARCH_URL = os.getenv(
    "BASE_SEARCH_URL",
    "https://myhealth.bg/search/?page="
)

# Your local scraper was running from page 164.
DEFAULT_START_PAGE = int(os.getenv("START_PAGE", "164"))

MAX_PAGE_RETRIES = 4
MAX_PROFILE_RETRIES = 3
PAGE_WAIT_SECONDS = 30
PROFILE_WAIT_SECONDS = 20
POST_LOAD_SLEEP = 1.5

BLOCKED_MARKERS = (
    "too many requests",
    "access denied",
    "cloudflare",
    "just a moment",
    "temporarily unavailable",
    "429",
)


# ============================================================
# PATHS
# ============================================================

try:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    SCRIPT_DIR = os.getcwd()

OUTPUT_DIR = os.path.join(SCRIPT_DIR, "myhealth_outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)

STATE_FILE = os.path.join(OUTPUT_DIR, "savegame_myhealth.json")
MEMORY_FILE = os.path.join(OUTPUT_DIR, "parsed_urls_myhealth.txt")
CSV_FILE = os.path.join(OUTPUT_DIR, "myhealth_doctors_full.csv")
CONTINUE_FLAG_FILE = os.path.join(OUTPUT_DIR, "CONTINUE_FLAG_MYHEALTH")


# ============================================================
# CSV SCHEMA
# ============================================================

FIELDNAMES = [
    "Име",
    "Специалност",
    "Рейтинг_Инфо",
    "Първи свободен час (Общо)",
    "Телефони",
    "НЗОК",
    "Биография",
    "URL",
    "Timestamp",
    "Цени",
    "Застрахователи",
]

for i in range(1, 5):
    FIELDNAMES.extend(
        [
            f"Hospital_{i}",
            f"Address_{i}",
            f"First_Free_{i}",
            f"Coords_{i}",
        ]
    )


# ============================================================
# STATE / MEMORY
# ============================================================

START_PAGE = DEFAULT_START_PAGE

state = {
    "page": START_PAGE,
    "consecutive_fails": 0,
}


def load_state():
    global state

    if not os.path.exists(STATE_FILE):
        print(f"[INFO] No state file. Starting from page {state['page']}.")
        return

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            loaded = json.load(f)

        loaded_page = int(loaded.get("page", state["page"]))
        state.update(loaded)
        state["page"] = max(1, loaded_page)

        print(f"[INFO] Resuming from page {state['page']}.")
    except Exception as exc:
        print(f"[WARN] Could not load state: {exc}")
        print(f"[INFO] Falling back to page {state['page']}.")


def save_state():
    temp_file = STATE_FILE + ".tmp"

    try:
        state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)

        os.replace(temp_file, STATE_FILE)
        return True

    except Exception as exc:
        print(f"[ERROR] Could not save state: {exc}")
        return False


parsed_urls = set()


def load_memory():
    if not os.path.exists(MEMORY_FILE):
        print("[INFO] Parsed URL memory does not exist yet.")
        return

    try:
        with open(MEMORY_FILE, "r", encoding="utf-8") as f:
            for line in f:
                url = line.strip()
                if not url:
                    continue

                parsed_urls.add(url)
                parsed_urls.add(unquote(url))

        print(f"[INFO] Loaded {len(parsed_urls)} URL memory entries.")

    except Exception as exc:
        print(f"[WARN] Could not load URL memory: {exc}")


def canonicalize_url(url):
    if not url:
        return ""

    url = url.strip()

    if url.startswith("/"):
        url = urljoin("https://myhealth.bg", url)

    # Remove fragment only. Keep query parameters if the site needs them.
    parsed = urlparse(url)
    clean = parsed._replace(fragment="").geturl()

    return clean.rstrip("/")


def is_parsed(url):
    canonical = canonicalize_url(url)
    decoded = unquote(canonical)

    return canonical in parsed_urls or decoded in parsed_urls


def mark_as_parsed(url):
    canonical = canonicalize_url(url)

    if not canonical:
        return

    decoded = unquote(canonical)

    parsed_urls.add(canonical)
    parsed_urls.add(decoded)

    try:
        with open(MEMORY_FILE, "a", encoding="utf-8") as f:
            f.write(canonical + "\n")
    except Exception as exc:
        print(f"[ERROR] Could not update URL memory: {exc}")


# ============================================================
# CSV INIT
# ============================================================

def init_csv():
    if os.path.exists(CSV_FILE) and os.path.getsize(CSV_FILE) > 0:
        return

    with open(CSV_FILE, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=FIELDNAMES,
            extrasaction="ignore",
        )
        writer.writeheader()

    print(f"[INFO] Created CSV: {CSV_FILE}")


def append_doctor_to_csv(details):
    with open(CSV_FILE, "a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=FIELDNAMES,
            extrasaction="ignore",
        )
        writer.writerow(details)
        f.flush()
        os.fsync(f.fileno())


# ============================================================
# SELENIUM
# ============================================================

driver = None


def init_driver():
    global driver

    options = webdriver.ChromeOptions()

    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--log-level=3")

    # Selenium Manager is preferred in GitHub Actions.
    # It automatically finds/downloads the matching browser driver.
    driver = webdriver.Chrome(options=options)

    driver.set_page_load_timeout(PAGE_WAIT_SECONDS)
    driver.set_script_timeout(PAGE_WAIT_SECONDS)

    print("[INFO] Chrome driver started successfully.")


def restart_driver():
    global driver

    print("[INFO] Restarting browser...")

    try:
        if driver is not None:
            driver.quit()
    except Exception:
        pass

    driver = None
    time.sleep(2)
    init_driver()


def close_driver():
    global driver

    try:
        if driver is not None:
            driver.quit()
    except Exception:
        pass

    driver = None


# ============================================================
# HELPERS
# ============================================================

def time_limit_reached():
    return (time.time() - START_TIME) >= TIME_LIMIT_SECONDS


def get_page_text():
    try:
        return driver.find_element(By.TAG_NAME, "body").text.strip().lower()
    except Exception:
        return ""


def page_looks_blocked():
    text = get_page_text()

    if not text:
        return False

    return any(marker in text for marker in BLOCKED_MARKERS)


def get_text_safe(xpath, search_context=None, default="-"):
    try:
        ctx = search_context if search_context else driver
        element = ctx.find_element(By.XPATH, xpath)
        return element.text.strip().replace("\n", " ")
    except Exception:
        return default


# ============================================================
# EXTRACTION
# ============================================================

def scrape_insurances_myhealth():
    try:
        logos = driver.find_elements(
            By.XPATH,
            "//div[contains(@class, 'practice__insurance-logos')]//img",
        )

        insurances = []

        for img in logos:
            alt = img.get_attribute("alt")
            if alt:
                alt = alt.strip()
                if alt:
                    insurances.append(alt)

        # Preserve order while removing duplicates.
        insurances = list(dict.fromkeys(insurances))

        return ", ".join(insurances) if insurances else "-"

    except Exception:
        return "-"


def scrape_prices_myhealth():
    try:
        price_items = driver.find_elements(
            By.XPATH,
            "//div[contains(@class, 'practice__pricing-text--item')]",
        )

        found_prices = []

        for item in price_items:
            try:
                name = item.find_element(
                    By.XPATH,
                    ".//p[contains(@class, 'dummy--reason__name')]",
                ).text.strip()

                value = item.find_element(
                    By.XPATH,
                    ".//p[contains(@class, 'dummy--reason__price')]",
                ).text.strip()

                if name or value:
                    found_prices.append(f"{name}: {value}")

            except Exception:
                continue

        return " | ".join(found_prices) if found_prices else "-"

    except Exception:
        return "-"


def get_coordinates_from_map_link(context=None):
    try:
        ctx = context if context else driver

        links = ctx.find_elements(
            By.XPATH,
            ".//a[contains(@href, 'google.com/maps') and contains(@href, 'daddr')]",
        )

        for link in links:
            href = link.get_attribute("href") or ""

            match = re.search(
                r"(?:[?&])daddr=([+-]?\d+(?:\.\d+)?),([+-]?\d+(?:\.\d+)?)",
                href,
            )

            if match:
                return f"{match.group(1)}, {match.group(2)}"

        return "-"

    except Exception:
        return "-"


def get_full_biography():
    try:
        hidden = driver.find_elements(By.ID, "hidden-profile-resume")

        if hidden:
            text = driver.execute_script(
                "return arguments[0].textContent || '';",
                hidden[0],
            ).strip()

            if text:
                return text

        try:
            read_more_btn = driver.find_element(
                By.CSS_SELECTOR,
                "button[data-hidden-text-id='profile-resume']",
            )

            if read_more_btn.is_displayed():
                driver.execute_script(
                    "arguments[0].click();",
                    read_more_btn,
                )
                time.sleep(0.5)

        except Exception:
            pass

        bio = driver.find_element(By.ID, "profile-resume")
        return bio.text.strip()

    except Exception:
        return "-"


def scrape_practices_detailed():
    practices_data = []

    try:
        free_dates_map = {}

        # ----------------------------------------------------
        # Available dates
        # ----------------------------------------------------
        try:
            dates_container = driver.find_element(
                By.CLASS_NAME,
                "dummy--detailed-profile-card__practices",
            )

            titles = dates_container.find_elements(
                By.CLASS_NAME,
                "dummy--detailed-profile-card__practices-title",
            )

            dates = dates_container.find_elements(
                By.CLASS_NAME,
                "dummy--detailed-profile-card__practices-fa",
            )

            if len(titles) == len(dates):
                for title, date_el in zip(titles, dates):
                    title_text = title.text.strip()
                    raw_date = date_el.get_attribute("data-date")
                    visible_date = date_el.text.strip()

                    final_date = (
                        raw_date.replace("T", " ").split("+")[0]
                        if raw_date
                        else visible_date
                    )

                    key = re.sub(
                        r"\s+",
                        "",
                        title_text.lower(),
                    )

                    if key:
                        free_dates_map[key] = final_date

        except Exception:
            pass

        # ----------------------------------------------------
        # Workplaces
        # ----------------------------------------------------
        workplaces = driver.find_elements(
            By.CLASS_NAME,
            "doctor-details__workplace-item",
        )

        for workplace in workplaces:
            try:
                hospital_name = workplace.find_element(
                    By.CLASS_NAME,
                    "doctor-details__workplace-item-title",
                ).text.strip()

                address = workplace.find_element(
                    By.CLASS_NAME,
                    "doctor-details__workplace-item-address",
                ).text.strip()

                coords = get_coordinates_from_map_link(workplace)

                first_date = "Няма свободни часове"

                combined = re.sub(
                    r"\s+",
                    "",
                    f"{hospital_name}{address}".lower(),
                )

                address_key = re.sub(
                    r"\s+",
                    "",
                    address.lower(),
                )

                # Exact/contains matching against normalized strings.
                for key, value in free_dates_map.items():

                    if key and (
                        key in combined or combined in key
                    ):
                        first_date = value
                        break

                    if (
                        address_key
                        and len(address_key) > 5
                        and address_key in key
                    ):
                        first_date = value
                        break

                practices_data.append(
                    {
                        "Hospital": hospital_name,
                        "Address": address,
                        "First_Date": first_date,
                        "Coords": coords,
                    }
                )

            except Exception:
                continue

    except Exception as exc:
        print(f"[WARN] Practice scrape error: {exc}")

    return practices_data


def get_all_first_available_dates_summary():
    dates_found = []

    try:
        date_elements = driver.find_elements(
            By.CLASS_NAME,
            "dummy--detailed-profile-card__practices-fa",
        )

        for date_el in date_elements:
            raw_date = date_el.get_attribute("data-date")

            if raw_date:
                clean_date = raw_date.replace("T", " ").split("+")[0]
                dates_found.append(clean_date)
            else:
                txt = date_el.text.strip()
                if txt:
                    dates_found.append(txt)

    except Exception:
        pass

    # Fallback
    if not dates_found:
        try:
            buttons = driver.find_elements(
                By.CLASS_NAME,
                "dummy--booking-component__first_available",
            )

            for button in buttons:
                raw_date = button.get_attribute(
                    "data-dummy-first-available"
                )

                if raw_date:
                    clean_date = raw_date.replace("T", " ").split("+")[0]
                    dates_found.append(clean_date)

        except Exception:
            pass

    # Preserve order and remove duplicates.
    dates_found = list(dict.fromkeys(dates_found))

    return (
        " | ".join(dates_found)
        if dates_found
        else "Няма свободни часове"
    )


# ============================================================
# DOCTOR URL DISCOVERY
# ============================================================

DOCTOR_PATH_MARKERS = (
    "/lekar/",
    "/practices/lekar/",
)


def extract_doctor_urls():
    urls = []

    try:
        links = driver.find_elements(By.TAG_NAME, "a")

        for link in links:
            href = link.get_attribute("href")

            if not href:
                continue

            absolute = canonicalize_url(href)
            parsed = urlparse(absolute)

            if parsed.netloc != "myhealth.bg":
                continue

            path = parsed.path.lower()

            if any(marker in path for marker in DOCTOR_PATH_MARKERS):
                if "/search" not in path:
                    urls.append(absolute)

    except Exception as exc:
        print(f"[WARN] Could not collect doctor links: {exc}")

    # Preserve order while deduplicating.
    return list(dict.fromkeys(urls))


# ============================================================
# DOCTOR PROFILE
# ============================================================

def scrape_doctor_profile_myhealth(url):
    for attempt in range(1, MAX_PROFILE_RETRIES + 1):

        if time_limit_reached():
            return None

        try:
            print(f"    [PROFILE {attempt}/{MAX_PROFILE_RETRIES}] {url}")

            driver.get(url)

            WebDriverWait(driver, PROFILE_WAIT_SECONDS).until(
                EC.presence_of_element_located(
                    (By.CLASS_NAME, "doctor-header")
                )
            )

            # Give dynamically populated practice/booking elements a moment.
            time.sleep(POST_LOAD_SLEEP)

            if page_looks_blocked():
                raise RuntimeError("Blocked / rate-limited page detected")

            doc_name = get_text_safe(
                "//div[contains(@class, 'doctor-header')]//h2/a"
            )

            specialty = get_text_safe(
                "//div[contains(@class, 'doctor-speciality')]"
            )

            rating_text = get_text_safe(
                "//span[contains(@class, 'doctor-rating-score_count')]"
            )

            bio = get_full_biography()

            nzok = "Не"

            try:
                if driver.find_elements(
                    By.XPATH,
                    "//span[contains(@class, 'ww-nzok')]",
                ):
                    nzok = "Да"
            except Exception:
                pass

            phone_values = []

            try:
                phone_links = driver.find_elements(
                    By.XPATH,
                    "//a[contains(@href, 'tel:')]",
                )

                for link in phone_links:
                    href = link.get_attribute("href") or ""

                    if href.startswith("tel:"):
                        phone_values.append(href[4:])

            except Exception:
                pass

            phone_values = list(dict.fromkeys(phone_values))

            practices = scrape_practices_detailed()

            if not practices:
                practices = [
                    {
                        "Hospital": "-",
                        "Address": "-",
                        "First_Date": "-",
                        "Coords": "-",
                    }
                ]

            doc_info = {
                "Име": doc_name,
                "Специалност": specialty,
                "Рейтинг_Инфо": rating_text,
                "Първи свободен час (Общо)": get_all_first_available_dates_summary(),
                "Телефони": ", ".join(phone_values) if phone_values else "-",
                "НЗОК": nzok,
                "Биография": bio[:1000],
                "URL": canonicalize_url(url),
                "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Цени": scrape_prices_myhealth(),
                "Застрахователи": scrape_insurances_myhealth(),
            }

            for i in range(1, 5):
                p = practices[i - 1] if i <= len(practices) else None

                doc_info[f"Hospital_{i}"] = (
                    p["Hospital"] if p else "-"
                )
                doc_info[f"Address_{i}"] = (
                    p["Address"] if p else "-"
                )
                doc_info[f"First_Free_{i}"] = (
                    p["First_Date"] if p else "-"
                )
                doc_info[f"Coords_{i}"] = (
                    p["Coords"] if p else "-"
                )

            if not doc_name or doc_name == "-":
                print(f"    [WARN] No doctor name found: {url}")
                return None

            return doc_info

        except (TimeoutException, WebDriverException, RuntimeError) as exc:
            print(f"    [WARN] Profile attempt failed: {exc}")

            if attempt < MAX_PROFILE_RETRIES:
                restart_driver()
                continue

        except Exception as exc:
            print(f"    [ERROR] Unexpected profile error: {exc}")

            if attempt < MAX_PROFILE_RETRIES:
                restart_driver()
                continue

        return None

    return None


# ============================================================
# SEARCH PAGE
# ============================================================

def load_search_page(page_number):
    url = f"{BASE_SEARCH_URL}{page_number}"

    for attempt in range(1, MAX_PAGE_RETRIES + 1):

        if time_limit_reached():
            return None

        try:
            print(
                f"\n[PAGE {page_number}] "
                f"Attempt {attempt}/{MAX_PAGE_RETRIES}: {url}"
            )

            driver.get(url)

            # Critical wait:
            # wait specifically for doctor profile links, not generic <a>.
            WebDriverWait(driver, PAGE_WAIT_SECONDS).until(
                EC.presence_of_element_located(
                    (
                        By.XPATH,
                        "//a[contains(@href, '/lekar/') "
                        "or contains(@href, '/practices/lekar/')]",
                    )
                )
            )

            time.sleep(POST_LOAD_SLEEP)

            if page_looks_blocked():
                raise RuntimeError("Blocked / rate-limited search page detected")

            doctor_urls = extract_doctor_urls()

            if doctor_urls:
                return doctor_urls

            print("[WARN] Page loaded, but no doctor URLs were found.")

        except Exception as exc:
            print(f"[WARN] Search page attempt failed: {exc}")

        if attempt < MAX_PAGE_RETRIES:
            restart_driver()

    return None


# ============================================================
# CONTINUE FLAG
# ============================================================

def flag_for_continuation():
    try:
        with open(CONTINUE_FLAG_FILE, "w", encoding="utf-8") as f:
            f.write("CONTINUE\n")
    except Exception as exc:
        print(f"[ERROR] Could not create continuation flag: {exc}")


def clear_continuation_flag():
    try:
        if os.path.exists(CONTINUE_FLAG_FILE):
            os.remove(CONTINUE_FLAG_FILE)
    except Exception as exc:
        print(f"[WARN] Could not remove continuation flag: {exc}")


# ============================================================
# MAIN
# ============================================================

def main():
    clear_continuation_flag()
    init_csv()
    load_memory()
    load_state()

    init_driver()

    try:
        while True:

            if time_limit_reached():
                print("[INFO] Time limit reached. Saving state.")
                save_state()
                flag_for_continuation()
                break

            page_number = int(state["page"])

            doctor_urls = load_search_page(page_number)

            if doctor_urls is None:
                print(
                    "[ERROR] Could not load search page after retries. "
                    "Stopping so the next run can retry the same page."
                )

                state["consecutive_fails"] = int(
                    state.get("consecutive_fails", 0)
                ) + 1

                save_state()
                flag_for_continuation()
                break

            state["consecutive_fails"] = 0

            print(
                f"[INFO] Page {page_number}: "
                f"found {len(doctor_urls)} doctor profile URLs."
            )

            for index, doctor_url in enumerate(doctor_urls, start=1):

                if time_limit_reached():
                    print("[INFO] Time limit reached during profile parsing.")
                    save_state()
                    flag_for_continuation()
                    return

                if is_parsed(doctor_url):
                    print(
                        f"  [{index}/{len(doctor_urls)}] "
                        f"[SKIP] Already parsed: {doctor_url}"
                    )
                    continue

                print(
                    f"  [{index}/{len(doctor_urls)}] "
                    f"Scraping: {doctor_url}"
                )

                details = scrape_doctor_profile_myhealth(doctor_url)

                if details:
                    append_doctor_to_csv(details)
                    mark_as_parsed(doctor_url)

                    # Save state after EVERY successful doctor.
                    # If Actions dies, we lose almost nothing.
                    save_state()

                    print(
                        f"    [+] Saved: {details['Име']} "
                        f"| {doctor_url}"
                    )
                else:
                    print(
                        f"    [FAIL] Could not parse profile: "
                        f"{doctor_url}"
                    )

            # Move only after the whole page has been inspected.
            state["page"] = page_number + 1
            save_state()

            print(
                f"[INFO] Finished page {page_number}. "
                f"Next page: {state['page']}"
            )

    finally:
        close_driver()

    print("\n[INFO] Scraper session finished.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("[INFO] Interrupted by user.")
        close_driver()
