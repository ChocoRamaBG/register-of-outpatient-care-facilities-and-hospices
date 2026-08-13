import time
import os
import urllib.parse
import csv
import json
import re
from datetime import datetime

from playwright.sync_api import (
    sync_playwright,
    TimeoutError as PlaywrightTimeoutError,
    Error as PlaywrightError
)

# ============================================================
# CONFIGURATION
# ============================================================

START_TIME = time.time()

TIME_LIMIT_SECONDS = 5.4 * 60 * 60

BASE_URL = "https://zdraven-arhiv.com/doctors/"

PAGE1_WAIT_SECONDS = 10
OTHER_PAGE_WAIT_SECONDS = 5

PROFILE_WAIT_SECONDS = 5
PROFILE_INITIAL_SLEEP = 1.5

MAX_PAGE_RETRIES = 3
MAX_PROFILE_RETRIES = 3

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

state_file = os.path.join(output_dir, "savegame_zdraven_arhiv.json")
memory_file = os.path.join(output_dir, "parsed_urls_zdraven_arhiv.txt")
failed_pages_file = os.path.join(output_dir, "failed_pages_zdraven_arhiv.json")
failed_profiles_file = os.path.join(output_dir, "failed_profiles_zdraven_arhiv.json")
page_discovered_urls_file = os.path.join(output_dir, "page_discovered_urls.json")
current_batch_filename = os.path.join(output_dir, "zdraven_arhiv_data_mega.csv")
CONTINUE_FLAG_FILE = os.path.join(output_dir, "CONTINUE_FLAG_ZDRAVEN_ARHIV")

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
    return (time.time() - START_TIME) >= TIME_LIMIT_SECONDS


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
        with open(state_file, "r", encoding="utf-8") as f:
            state = json.load(f)
        print(
            f"[INFO] Resuming from "
            f"Phase {state.get('phase', 1)}, "
            f"Page {state.get('page', 1)}."
        )
    except Exception as e:
        print(f"[WARN] State file could not be loaded: {e}")
        state = {"page": 1, "phase": 1, "consecutive_fails": 0}

def save_state(page, phase=1, consecutive_fails=0):
    payload = {
        "page": page,
        "phase": phase,
        "consecutive_fails": consecutive_fails,
        "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    temp_file = state_file + ".tmp"
    try:
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(temp_file, state_file)
    except Exception as e:
        print(f"[ERROR] Could not save state: {e}")

# ============================================================
# PARSED URL MEMORY
# ============================================================

parsed_urls = set()

if os.path.exists(memory_file):
    try:
        with open(memory_file, "r", encoding="utf-8") as f:
            for line in f:
                url = line.strip()
                if not url:
                    continue
                parsed_urls.add(urllib.parse.unquote(url))
                parsed_urls.add(url)
    except Exception as e:
        print(f"[WARN] Could not load parsed URL memory: {e}")

print(f"[INFO] Loaded {len(parsed_urls)} cached URLs.")

def normalize_url(url):
    if not url:
        return ""
    url = url.strip()
    return urllib.parse.unquote(url)

def is_already_parsed(url):
    decoded = normalize_url(url)
    return (decoded in parsed_urls or url in parsed_urls)

def mark_as_parsed(url):
    decoded = normalize_url(url)
    parsed_urls.add(decoded)
    parsed_urls.add(url)
    try:
        with open(memory_file, "a", encoding="utf-8") as f:
            f.write(decoded + "\n")
    except Exception as e:
        print(f"[ERROR] Could not save parsed URL: {e}")

# ============================================================
# FAILED PAGE MANAGEMENT
# ============================================================
def load_failed_pages():
    if not os.path.exists(failed_pages_file):
        return []
    try:
        with open(failed_pages_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            return []
        return sorted(set(int(x) for x in data if str(x).isdigit()))
    except Exception:
        return []

def save_failed_pages(pages):
    try:
        pages = sorted(set(int(x) for x in pages))
        with open(failed_pages_file, "w", encoding="utf-8") as f:
            json.dump(pages, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[ERROR] Could not save failed pages: {e}")

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
    if not os.path.exists(failed_profiles_file):
        return []
    try:
        with open(failed_profiles_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
    except Exception:
        pass
    return []

def save_failed_profiles(profiles):
    try:
        with open(failed_profiles_file, "w", encoding="utf-8") as f:
            json.dump(profiles, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[ERROR] Could not save failed profiles: {e}")

def add_failed_profile(name, url, page, reason=""):
    profiles = load_failed_profiles()
    decoded_url = normalize_url(url)
    for existing in profiles:
        if normalize_url(existing.get("URL", "")) == decoded_url:
            return
    profiles.append({
        "Име": name,
        "URL": decoded_url,
        "Page": page,
        "Reason": reason,
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })
    save_failed_profiles(profiles)

# ============================================================
# PAGE DISCOVERY LOG
# ============================================================

def load_page_discovered_urls():
    if not os.path.exists(page_discovered_urls_file):
        return {}
    try:
        with open(page_discovered_urls_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {}

page_discovered_urls = load_page_discovered_urls()

def save_page_discovered_urls():
    try:
        temp_file = page_discovered_urls_file + ".tmp"
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(page_discovered_urls, f, ensure_ascii=False, indent=2)
        os.replace(temp_file, page_discovered_urls_file)
    except Exception as e:
        print(f"[ERROR] Could not save page discovery log: {e}")

# ============================================================
# CSV INITIALIZATION
# ============================================================
if not os.path.exists(current_batch_filename):
    try:
        with open(current_batch_filename, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
    except Exception as e:
        print(f"[ERROR] Could not create CSV: {e}")


# ============================================================
# PLAYWRIGHT WEBDRIVER
# ============================================================

_pw_instance = None
_browser = None
_context = None
_page = None

def create_driver():
    global _pw_instance, _browser, _context, _page

    print("[INFO] Starting Playwright Chrome...")
    
    if _pw_instance is None:
        _pw_instance = sync_playwright().start()

    try:
        _browser = _pw_instance.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu"
            ]
        )
        
        _context = _browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        
        _page = _context.new_page()
        # Equivalent of driver.set_page_load_timeout(30)
        _page.set_default_navigation_timeout(30000)
        _page.set_default_timeout(30000)

        print("[INFO] Playwright Chrome started.")
        return _page
    except Exception as e:
        print(f"[CRITICAL] Playwright Chrome failed to start: {e}")
        raise

# Initialize the global driver page
driver_page = create_driver()


# ============================================================
# DRIVER RESTART
# ============================================================

def restart_driver():
    global driver_page, _browser, _context

    print("[INFO] Restarting Playwright Chrome...")
    try:
        if _page:
            _page.close()
        if _context:
            _context.close()
        if _browser:
            _browser.close()
    except Exception:
        pass

    time.sleep(2)
    driver_page = create_driver()

def close_driver():
    global _pw_instance, _browser, _context, _page
    try:
        if _page: _page.close()
        if _context: _context.close()
        if _browser: _browser.close()
        if _pw_instance: _pw_instance.stop()
    except Exception:
        pass


# ============================================================
# CSV WRITER
# ============================================================
def save_single_record(record):
    if not record:
        return False
    try:
        with open(current_batch_filename, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writerow(record)
        print(f"💾 Saved: {record.get('Име', '-')}")
        return True
    except Exception as e:
        print(f"❌ CSV save error: {e}")
        return False

# ============================================================
# PAGE URL
# ============================================================

def build_page_url(page):
    if page == 1:
        return BASE_URL
    return f"{BASE_URL}?jsf=jet-engine&pagenum={page}"

# ============================================================
# JETENGINE EMPTY-PAGE DETECTION
# ============================================================

def page_has_no_data():
    """
    Detect JetEngine's real end-of-pagination response using Playwright.
    """
    try:
        elements = driver_page.locator(".jet-listing-not-found").all()
        for element in elements:
            if not element.is_visible():
                continue
            text = (element.inner_text() or "").strip().lower()
            if "no data was found" in text:
                return True

        source = (driver_page.content() or "").lower()
        if "jet-listing-not-found" in source and "no data was found" in source:
            return True
    except Exception:
        pass
    return False

# ============================================================
# EXACT OLD PAGE PARSING LOGIC - PLAYWRIGHT
# ============================================================

def parse_listing_page(page_num):
    target_url = build_page_url(page_num)

    print()
    print("=" * 70)
    print(f"📄 PAGE {page_num}")
    print(target_url)
    print("=" * 70)

    try:
        driver_page.goto(target_url)
    except Exception as e:
        print(f"⛔ Page load error: {e}")
        return {"status": "FAILED", "doctors": [], "cards": []}

    wait_time = PAGE1_WAIT_SECONDS if page_num == 1 else OTHER_PAGE_WAIT_SECONDS

    # --------------------------------------------------------
    # END-OF-DATABASE DETECTION
    # --------------------------------------------------------
    try:
        if "404" in (driver_page.title() or ""):
            print("⛔ 404 detected. End of database.")
            return {"status": "END", "doctors": [], "cards": []}

        if "Страницата не е намерена" in (driver_page.content() or ""):
            print("⛔ 'Страницата не е намерена' detected. End of database.")
            return {"status": "END", "doctors": [], "cards": []}

        if page_has_no_data():
            print("🏁 JetEngine returned 'No data was found'. End of database.")
            return {"status": "END", "doctors": [], "cards": []}
    except Exception as e:
        print(f"⚠️ End-of-page detection error: {e}")

    # --------------------------------------------------------
    # Wait exactly like old scraper
    # --------------------------------------------------------
    try:
        driver_page.wait_for_selector(
            ".jet-listing-grid__item", 
            timeout=wait_time * 1000
        )
    except PlaywrightTimeoutError:
        print(f"⛔ No listing cards appeared within {wait_time} seconds.")
        return {"status": "FAILED", "doctors": [], "cards": []}

    # --------------------------------------------------------
    # Find cards exactly like old scraper
    # --------------------------------------------------------
    cards = driver_page.locator(".jet-listing-grid__item").all()

    if not cards and page_has_no_data():
        print("🏁 Empty JetEngine listing detected after card lookup. End of database.")
        return {"status": "END", "doctors": [], "cards": []}

    if not cards:
        print("⛔ No cards found.")
        return {"status": "FAILED", "doctors": [], "cards": []}

    print(f"🔎 Found {len(cards)} cards.")
    doctors_on_page = []

    # ========================================================
    # CARD PARSING
    # ========================================================
    for card_index, card in enumerate(cards, start=1):
        try:
            link_el = card.locator("a.jet-listing-dynamic-link__link").first
            
            # Using evaluate to get exact href securely in case DOM structure changes slightly
            raw_url = link_el.get_attribute("href")
            name = (link_el.inner_text() or "").strip()

            if not raw_url:
                continue

            description = extract_listing_description(card, name)

            doc_data = {
                "Име": name,
                "RAW_URL": raw_url,
                "URL": normalize_url(raw_url),
                "Описание (Лист)": description
            }
            doctors_on_page.append(doc_data)

        except Exception as e:
            print(f"⚠️ Card #{card_index} could not be parsed: {e}")
            continue

    # --------------------------------------------------------
    # Page discovery logging
    # --------------------------------------------------------
    page_discovered_urls[str(page_num)] = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "card_count": len(cards),
        "doctor_count": len(doctors_on_page),
        "urls": [x["URL"] for x in doctors_on_page]
    }
    save_page_discovered_urls()

    print(f"✅ Extracted {len(doctors_on_page)} doctors from {len(cards)} cards.")
    return {"status": "OK", "doctors": doctors_on_page, "cards": cards}


# ============================================================
# DESCRIPTION EXTRACTION
# ============================================================
def clean_text(text):
    if not text:
        return ""
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def extract_listing_description(card, doctor_name):
    # ========================================================
    # METHOD 1: JetEngine dynamic field content
    # ========================================================
    try:
        elements = card.locator(".jet-listing-dynamic-field__content").all()
        candidates = []

        for element in elements:
            try:
                text = (element.inner_text() or "")
                text = clean_text(text)
                if not text:
                    continue
                candidates.append(text)
            except Exception:
                continue

        candidates = list(dict.fromkeys(candidates))
        filtered = []

        for text in candidates:
            if text == clean_text(doctor_name):
                continue
            if text == "Разгледай":
                continue
            filtered.append(text)

        description_candidates = []
        for text in filtered:
            if re.match(r"^(гр\.|с\.|ул\.|бул\.|кв\.|" r"\d{4}|ж\.к\.)", text, re.IGNORECASE):
                continue
            if len(text) >= 30:
                description_candidates.append(text)

        if description_candidates:
            return description_candidates[0]
        if filtered:
            return filtered[0]
    except Exception:
        pass

    # ========================================================
    # METHOD 2: Card text fallback
    # ========================================================
    try:
        card_text = (card.inner_text() or "").strip()
        if card_text:
            lines = [clean_text(x) for x in card_text.splitlines() if clean_text(x)]
            doctor_name_clean = clean_text(doctor_name)
            
            remaining = []
            for line in lines:
                if line == doctor_name_clean or line == "Разгледай":
                    continue
                remaining.append(line)

            for line in remaining:
                if len(line) < 25:
                    continue
                if re.match(r"^(гр\.|с\.|ул\.|бул\.|кв\.|" r"\d{4}|ж\.к\.)", line, re.IGNORECASE):
                    continue
                return line
    except Exception:
        pass

    return "-"


# ============================================================
# PROFILE SCRAPER - PLAYWRIGHT
# ============================================================
def scrape_inner_profile(url, basic_info):
    print(f"   👉 Visiting: {url}")
    try:
        driver_page.goto(url)

        time.sleep(PROFILE_INITIAL_SLEEP)

        try:
            driver_page.wait_for_selector(
                ".elementor-widget-icon-box",
                timeout=PROFILE_WAIT_SECONDS * 1000
            )
        except PlaywrightTimeoutError:
            pass

        # ====================================================
        # PHONES / EMAILS / ADDRESSES
        # ====================================================
        phones = []
        emails = []
        possible_addresses = []

        try:
            box_titles = driver_page.locator(".elementor-widget-icon-box .elementor-icon-box-title span").all()
            for title_el in box_titles:
                try:
                    text = (title_el.inner_text() or "").strip()
                    if not text:
                        continue
                    
                    if "@" in text:
                        if text not in emails:
                            emails.append(text)
                        continue
                        
                    if re.search(r"(\+359|08[789]|02)", text) and len(text) < 20:
                        if text not in phones:
                            phones.append(text)
                        continue

                    if len(text) > 10:
                        if text not in possible_addresses:
                            possible_addresses.append(text)
                except Exception:
                    continue
        except Exception as e:
            print(f"⚠️ Could not parse icon boxes: {e}")

        # ====================================================
        # GOOGLE MAP
        # ====================================================
        map_pin_address = "-"
        clickable_map_link = "-"

        try:
            iframe = driver_page.locator("iframe[src*='maps.google.com']").first
            # Playwright handles counting nicely before interacting
            if iframe.count() > 0:
                raw_address = iframe.get_attribute("title") or iframe.get_attribute("aria-label")
                if raw_address:
                    map_pin_address = raw_address.strip()
                    encoded_address = urllib.parse.quote(raw_address)
                    clickable_map_link = "https://www.google.com/maps/search/?api=1&query=" + encoded_address
        except Exception:
            pass

        # ====================================================
        # TEXT ADDRESS
        # ====================================================
        text_address = (
            map_pin_address if map_pin_address != "-"
            else (possible_addresses[0] if possible_addresses else "-")
        )

        # ====================================================
        # BIOGRAPHY
        # ====================================================
        full_bio = "-"
        try:
            bio_el = driver_page.locator(".jet-listing-dynamic-field__content").first
            if bio_el.count() > 0:
                full_bio = (bio_el.inner_text() or "").strip().replace("\n", " || ")
        except Exception:
            pass

        # ====================================================
        # BREADCRUMB
        # ====================================================
        breadcrumb_info = "-"
        try:
            breadcrumb_el = driver_page.locator("#breadcrumbs").first
            if breadcrumb_el.count() > 0:
                breadcrumb_info = (breadcrumb_el.inner_text() or "").strip()
        except Exception:
            pass

        # ====================================================
        # FINAL RECORD
        # ====================================================
        basic_info.update({
            "Телефони": ", ".join(phones) if phones else "-",
            "Email": ", ".join(emails) if emails else "-",
            "Адрес (Текст)": text_address,
            "Адрес (Google Maps Pin)": map_pin_address,
            "Google Maps Link": clickable_map_link,
            "Breadcrumb (Текст)": breadcrumb_info,
            "Биография": full_bio,
            "Note": "-",
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        save_single_record(basic_info)
        return True

    except Exception as e:
        print(f"❌ Failed to parse profile {url}: {e}")
        return False


# ============================================================
# MAIN EXECUTION LOOP
# ============================================================
# ============================================================
# GITHUB ACTIONS FLAG MANAGEMENT
# ============================================================
def flag_for_continuation():
    """Creates the flag file so GitHub Actions knows to restart."""
    try:
        with open(CONTINUE_FLAG_FILE, 'w') as f:
            f.write("CONTINUE")
    except Exception as e:
        print(f"[ERROR] Could not write continue flag: {e}")

def clear_continuation_flag():
    """Removes the flag file if it exists (for clean completion)."""
    if os.path.exists(CONTINUE_FLAG_FILE):
        try:
            os.remove(CONTINUE_FLAG_FILE)
        except Exception:
            pass

# ============================================================
# MAIN EXECUTION LOOP
# ============================================================
def main():
    print("[INFO] Scraper Started.")
    clear_continuation_flag() # Ensure we start fresh
    
    while True:
        if time_limit_reached():
            print("[INFO] Time limit reached. Triggering continue flag and shutting down.")
            flag_for_continuation()
            break
            
        current_page = state["page"]
        
        # Parse Listing Page
        result = parse_listing_page(current_page)
        
        if result["status"] == "END":
            print(f"🎉 Scraping complete! Reached the end at page {current_page}.")
            clear_continuation_flag()
            break
            
        elif result["status"] == "FAILED":
            state["consecutive_fails"] += 1
            print(f"[WARN] Failed to load page {current_page}. Consecutive fails: {state['consecutive_fails']}")
            
            if state["consecutive_fails"] >= MAX_PAGE_RETRIES:
                print(f"[ERROR] Max retries reached for page {current_page}. Logging and moving to next page.")
                add_failed_page(current_page)
                state["page"] += 1
                state["consecutive_fails"] = 0
            
            save_state(state["page"], state["phase"], state["consecutive_fails"])
            restart_driver()
            time.sleep(RETRY_DELAY_SECONDS)
            continue
            
        # Success state on listing page
        state["consecutive_fails"] = 0
        doctors = result.get("doctors", [])
        time_limit_hit_in_profiles = False
        
        # Scrape Individual Profiles
        for doctor in doctors:
            if time_limit_reached():
                print("[INFO] Time limit reached during profile loop. Triggering continue flag.")
                flag_for_continuation()
                time_limit_hit_in_profiles = True
                break
                
            doc_url = doctor["URL"]
            
            if is_already_parsed(doc_url):
                print(f"   ⏭️ Already parsed: {doc_url}")
                continue
                
            success = scrape_inner_profile(doc_url, doctor)
            
            if success:
                mark_as_parsed(doc_url)
            else:
                add_failed_profile(doctor["Име"], doc_url, current_page)
        
        # If we hit the time limit, DO NOT increment the page. 
        # On the next run, it will reload the same page, skip the already 
        # parsed doctors, and continue exactly where it left off.
        if time_limit_hit_in_profiles:
            break
            
        # Proceed to next page ONLY if we successfully finished this page
        state["page"] += 1
        save_state(state["page"], state["phase"], state["consecutive_fails"])

    # Cleanup
    close_driver()
    print("[INFO] Scraper session ended cleanly.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("[INFO] Interrupted by user.")
        close_driver()
