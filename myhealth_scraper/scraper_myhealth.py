import os
import sys
import time
import json
import csv
import re
from urllib.parse import unquote, urlparse
from datetime import datetime

from playwright.sync_api import (
    sync_playwright,
    TimeoutError as PlaywrightTimeoutError
)

# ============================================================
# CONFIGURATION
# ============================================================
START_TIME = time.time()
TIME_LIMIT_SECONDS = 5.4 * 60 * 60  # ~5 hours and 24 minutes

BASE_SEARCH_URL = "https://myhealth.bg/search/?page="
MAX_PAGE_RETRIES = 3
RETRY_DELAY_SECONDS = 2

# ============================================================
# PATHS AND DIRECTORIES
# ============================================================
try:
    output_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    output_dir = os.getcwd()

output_dir = os.path.join(output_dir, "myhealth_outputs")
os.makedirs(output_dir, exist_ok=True)

state_file = os.path.join(output_dir, "savegame_myhealth.json")
memory_file = os.path.join(output_dir, "parsed_urls_myhealth.txt")
failed_profiles_file = os.path.join(output_dir, "failed_profiles_myhealth.json")
csv_file_path = os.path.join(output_dir, "myhealth_doctors_full.csv")
CONTINUE_FLAG_FILE = os.path.join(output_dir, "CONTINUE_FLAG_MYHEALTH")

# ============================================================
# DATA SCHEMA (CSV)
# ============================================================
fieldnames = [
    "Име", "Специалност", "Рейтинг_Инфо", "Първи свободен час (Общо)", 
    "Телефони", "НЗОК", "Биография", "URL", "Timestamp", "Цени", "Застрахователи"
]
for i in range(1, 6):
    fieldnames.extend([f"Hospital_{i}", f"Address_{i}", f"First_Free_{i}", f"Coords_{i}"])

if not os.path.exists(csv_file_path):
    with open(csv_file_path, mode="w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

# ============================================================
# TIME AND STATE MANAGEMENT
# ============================================================
def time_limit_reached():
    return (time.time() - START_TIME) >= TIME_LIMIT_SECONDS

state = {
    "page": 1,
    "consecutive_fails": 0
}

if os.path.exists(state_file):
    try:
        with open(state_file, "r", encoding="utf-8") as f:
            loaded_state = json.load(f)
            state.update(loaded_state)
        print(f"[INFO] Session restored: Starting from page {state['page']}.")
    except Exception as e:
        print(f"[WARN] Failed to load state: {e}")

def save_state():
    temp_file = state_file + ".tmp"
    try:
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        os.replace(temp_file, state_file)
    except Exception as e:
        print(f"[ERROR] Failed to save state file: {e}")

# ============================================================
# URL MEMORY (PROCESSED PROFILES)
# ============================================================
parsed_urls = set()

if os.path.exists(memory_file):
    with open(memory_file, "r", encoding="utf-8") as f:
        for line in f:
            url = line.strip()
            if url:
                parsed_urls.add(unquote(url))
                parsed_urls.add(url)
print(f"[INFO] Loaded {len(parsed_urls)} already processed URLs.")

def mark_as_parsed(url):
    decoded = unquote(url)
    parsed_urls.add(decoded)
    parsed_urls.add(url)
    with open(memory_file, "a", encoding="utf-8") as f:
        f.write(decoded + "\n")

# ============================================================
# PLAYWRIGHT INSTANCE
# ============================================================
_pw_instance = None
_browser = None
_context = None
_page = None

def create_driver():
    global _pw_instance, _browser, _context, _page
    if _pw_instance is None:
        _pw_instance = sync_playwright().start()

    _browser = _pw_instance.chromium.launch(
        headless=True,
        args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
    )
    _context = _browser.new_context(
        viewport={'width': 1920, 'height': 1080},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    # Block heavy assets for performance optimization
    _context.route("**/*.{png,jpg,jpeg,webp,svg,css,woff,woff2}", lambda route: route.abort())
    
    _page = _context.new_page()
    _page.set_default_navigation_timeout(30000)
    _page.set_default_timeout(30000)
    return _page

def restart_driver():
    global _page, _context, _browser
    print("[INFO] Restarting browser context...")
    try:
        if _page: _page.close()
        if _context: _context.close()
        if _browser: _browser.close()
    except: pass
    time.sleep(2)
    return create_driver()

def close_driver():
    try:
        if _page: _page.close()
        if _context: _context.close()
        if _browser: _browser.close()
        if _pw_instance: _pw_instance.stop()
    except: pass

driver_page = create_driver()

# ============================================================
# EXTRACTION HELPERS
# ============================================================
def get_text_safe(locator_selector, default="-"):
    try:
        locator = driver_page.locator(locator_selector).first
        if locator.count() > 0:
            return locator.inner_text().strip().replace('\n', ' ')
    except: pass
    return default

def extract_doctor_details(url):
    try:
        driver_page.goto(url, wait_until="domcontentloaded")
        time.sleep(0.5) 
    except Exception as e:
        print(f"[ERROR] Loading failed for {url}: {e}")
        return None

    # Base Info Extraction
    doc_name = get_text_safe(".doctor-header h2 a")
    if doc_name == "-":
        return None  # Invalid profile page

    specialty = get_text_safe(".doctor-speciality")
    rating_text = get_text_safe("span.doctor-rating-score_count")

    # Biography
    bio = "-"
    try:
        hidden_bio = driver_page.locator("#hidden-profile-resume").first
        if hidden_bio.count() > 0:
            bio = hidden_bio.text_content().strip()
        else:
            bio = get_text_safe("#profile-resume")
    except: pass

    # NZOK Status
    nzok = "Да" if driver_page.locator("span.ww-nzok").count() > 0 else "Не"

    # Phone Numbers
    phones = []
    try:
        phone_links = driver_page.locator("a[href^='tel:']").all()
        phones = list(set([lnk.get_attribute("href").replace("tel:", "") for lnk in phone_links if lnk.get_attribute("href")]))
    except: pass
    phone_str = ", ".join(phones) if phones else "-"

    # Insurances
    insurances = "-"
    try:
        logo_imgs = driver_page.locator(".practice__insurance-logos img").all()
        ins_list = [img.get_attribute("alt").strip() for img in logo_imgs if img.get_attribute("alt")]
        if ins_list: insurances = ", ".join(ins_list)
    except: pass

    # Prices
    prices = "-"
    try:
        price_items = driver_page.locator(".practice__pricing-text--item").all()
        found_prices = []
        for item in price_items:
            name = item.locator(".dummy--reason__name").inner_text().strip()
            val = item.locator(".dummy--reason__price").inner_text().strip()
            found_prices.append(f"{name}: {val}")
        if found_prices: prices = " | ".join(found_prices)
    except: pass

    # Main first available dates summary
    dates_found = []
    try:
        date_elements = driver_page.locator(".dummy--detailed-profile-card__practices-fa").all()
        for el in date_elements:
            raw_date = el.get_attribute("data-date")
            if raw_date:
                dates_found.append(raw_date.replace("T", " ").split("+")[0])
            else:
                txt = el.inner_text().strip()
                if txt: dates_found.append(txt)
    except: pass
    first_available_summary = " | ".join(dates_found) if dates_found else "Няма свободни часове"

    # Construct primary dictionary
    doc_info = {
        "Име": doc_name,
        "Специалност": specialty,
        "Рейтинг_Инфо": rating_text,
        "Първи свободен час (Общо)": first_available_summary,
        "Телефони": phone_str,
        "НЗОК": nzok,
        "Биография": bio[:1000] if bio else "-",
        "URL": url,
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Цени": prices,
        "Застрахователи": insurances
    }

    # Practices & Coordinates Logic
    practices = []
    try:
        workplaces = driver_page.locator(".doctor-details__workplace-item").all()
        for wp in workplaces:
            h_name = wp.locator(".doctor-details__workplace-item-title").inner_text().strip()
            h_addr = wp.locator(".doctor-details__workplace-item-address").inner_text().strip()
            
            h_coords = "-"
            try:
                map_link = wp.locator("a[href*='google.com/maps'][href*='daddr']").first
                if map_link.count() > 0:
                    href = map_link.get_attribute("href")
                    match = re.search(r"daddr=([\d\.]+),([\d\.]+)", href)
                    if match: h_coords = f"{match.group(1)}, {match.group(2)}"
            except: pass
            
            practices.append({
                "Hospital": h_name, "Address": h_addr, 
                "First_Date": "-", "Coords": h_coords
            })
    except: pass

    if not practices:
        practices = [{"Hospital": "-", "Address": "-", "First_Date": "-", "Coords": "-"}]

    for i, p in enumerate(practices):
        idx = i + 1
        if idx > 5: break
        doc_info[f"Hospital_{idx}"] = p["Hospital"]
        doc_info[f"Address_{idx}"] = p["Address"]
        doc_info[f"First_Free_{idx}"] = p["First_Date"]
        doc_info[f"Coords_{idx}"] = p["Coords"]

    for i in range(len(practices) + 1, 6):
        doc_info[f"Hospital_{i}"] = "-"
        doc_info[f"Address_{i}"] = "-"
        doc_info[f"First_Free_{i}"] = "-"
        doc_info[f"Coords_{i}"] = "-"

    return doc_info

# ============================================================
# MAIN LOGIC
# ============================================================
def flag_for_continuation():
    with open(CONTINUE_FLAG_FILE, 'w') as f:
        f.write("CONTINUE")

def clear_continuation_flag():
    if os.path.exists(CONTINUE_FLAG_FILE):
        os.remove(CONTINUE_FLAG_FILE)

def main():
    global driver_page, BASE_SEARCH_URL
    clear_continuation_flag()

    # Allow dynamic URL input via console/terminal, skip if running non-interactively (e.g., GitHub Actions)
    if sys.stdin.isatty():
        try:
            custom_url = input(f"[INPUT] Provide target URL or press Enter to keep default ({BASE_SEARCH_URL}): ").strip()
            if custom_url:
                BASE_SEARCH_URL = custom_url
        except EOFError:
            pass

    # Extract base domain to correctly map relative URLs
    parsed_base = urlparse(BASE_SEARCH_URL)
    base_domain = f"{parsed_base.scheme}://{parsed_base.netloc}"

    while True:
        if time_limit_reached():
            print("\n[INFO] Time limit reached. Setting continuation flag.")
            flag_for_continuation()
            break

        current_url = f"{BASE_SEARCH_URL}{state['page']}"
        print(f"\n--- Processing Page: {state['page']} ---")

        try:
            driver_page.goto(current_url, wait_until="domcontentloaded")
            driver_page.wait_for_selector("a", timeout=10000)
        except Exception as e:
            print(f"[WARN] Error loading search page: {e}")
            state["consecutive_fails"] += 1
            if state["consecutive_fails"] >= MAX_PAGE_RETRIES:
                print("[ERROR] Max retries reached. Assuming end of pagination.")
                break
            driver_page = restart_driver()
            continue
        
        state["consecutive_fails"] = 0

        # Extract doctor profile links
        links = driver_page.locator("a").all()
        doctor_urls = []
        for link in links:
            href = link.get_attribute("href")
            if href and ("/lekar/" in href or "/practices/lekar/" in href) and "search" not in href:
                # Handle relative URLs correctly
                if href.startswith("/"):
                    href = f"{base_domain}{href}"
                doctor_urls.append(href)

        doctor_urls = list(set(doctor_urls))

        if not doctor_urls:
            print("[INFO] No profile links found on this page. Pagination complete.")
            break

        time_limit_hit_in_profiles = False

        for doc_url in doctor_urls:
            if time_limit_reached():
                print("[INFO] Time limit reached during profile extraction.")
                flag_for_continuation()
                time_limit_hit_in_profiles = True
                break

            if unquote(doc_url) in parsed_urls or doc_url in parsed_urls:
                continue

            details = extract_doctor_details(doc_url)
            if details:
                with open(csv_file_path, mode="a", encoding="utf-8-sig", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writerow(details)
                
                mark_as_parsed(doc_url)
                print(f"  [+] Saved: {details['Име']} | {unquote(doc_url)}")
            else:
                print(f"  [-] Failed extraction for: {doc_url}")

        if time_limit_hit_in_profiles:
            break

        state["page"] += 1
        save_state()

    close_driver()
    print("\n[INFO] Scraping session completed successfully.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        close_driver()
        print("\n[INFO] Interrupted by user.")
