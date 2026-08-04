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

# --- CONFIGURATION & PATHS ---
START_TIME = time.time()
TIME_LIMIT_SECONDS = 5.4 * 60 * 60

try:
    base_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    base_dir = os.getcwd()

output_dir = os.path.join(base_dir, "zdraven_arhiv_outputs")
os.makedirs(output_dir, exist_ok=True)

state_file = os.path.join(output_dir, "savegame_zdraven_arhiv.json")
memory_file = os.path.join(output_dir, "parsed_urls_zdraven_arhiv.txt")
current_batch_filename = os.path.join(output_dir, "zdraven_arhiv_data_mega.csv")
CONTINUE_FLAG_FILE = os.path.join(output_dir, "CONTINUE_FLAG_ZDRAVEN_ARHIV")

# --- STATE AND MEMORY MANAGEMENT ---
state = {"page": 1}
if os.path.exists(state_file):
    try:
        with open(state_file, "r") as f:
            state = json.load(f)
            print(f"[INFO] Resuming execution from Page {state['page']}.")
    except Exception:
        print("[WARN] Corrupted state file detected. Initializing new state.")
        state = {"page": 1}

def save_state(page):
    with open(state_file, "w") as f:
        json.dump({"page": page}, f)

parsed_urls = set()
if os.path.exists(memory_file):
    with open(memory_file, "r", encoding="utf-8") as f:
        for line in f:
            url = line.strip()
            if url: parsed_urls.add(url)
print(f"[INFO] Initialized memory cache with {len(parsed_urls)} processed URLs.")

def mark_as_parsed(url):
    parsed_urls.add(url)
    with open(memory_file, "a", encoding="utf-8") as f:
        f.write(url + "\n")

# --- DATA SCHEMA CONFIGURATION ---
fieldnames = [
    "Име", "URL", "Описание (Лист)", "Телефони", "Email",
    "Адрес (Текст)", "Адрес (Google Maps Pin)", "Google Maps Link",
    "Breadcrumb (Текст)", "Биография", "Note", "Timestamp"
]

if not os.path.exists(current_batch_filename):
    try:
        with open(current_batch_filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
    except Exception as e:
        print(f"[ERROR] Failed to initialize CSV structure: {e}")

# --- WEBDRIVER INITIALIZATION ---
print("[INFO] Configuring WebDriver instance...")
options = Options()
options.add_argument('--headless=new') 
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--disable-gpu')
options.add_argument('--window-size=1920,1080')
# ПРЕМАХНАТО: options.page_load_strategy = 'eager' (Причиняваше зареждане само на 5 записа)
options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

try:
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    print("[SUCCESS] WebDriver instantiated successfully.")
except Exception as e:
    print(f"[CRITICAL] Failed to initiate WebDriver: {e}")
    sys.exit(1)

# --- CORE FUNCTIONS ---
def save_single_record(record):
    if not record: return
    try:
        with open(current_batch_filename, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writerow(record)
    except Exception as e:
        print(f"[ERROR] Failed to append record to CSV: {e}")

def scrape_inner_profile(url, basic_info):
    try:
        driver.get(url)
        time.sleep(1.5) 
        
        try:
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CLASS_NAME, "elementor-widget-icon-box")))
        except: pass

        phones = []
        emails = []
        possible_addresses = []
        
        try:
            box_titles = driver.find_elements(By.CSS_SELECTOR, ".elementor-widget-icon-box .elementor-icon-box-title span")
            
            for title_el in box_titles:
                text = title_el.text.strip()
                if not text: continue
                
                if "@" in text:
                    if text not in emails: emails.append(text)
                elif re.search(r"(\+359|08[789]|02)", text) and len(text) < 20:
                    if text not in phones: phones.append(text)
                elif len(text) > 10:
                    if text not in possible_addresses: possible_addresses.append(text)
                    
        except Exception:
            pass

        map_pin_address = "-"
        clickable_map_link = "-"
        
        try:
            iframe = driver.find_element(By.CSS_SELECTOR, "iframe[src*='maps.google.com']")
            raw_address = iframe.get_attribute("title") or iframe.get_attribute("aria-label")
            
            if raw_address:
                map_pin_address = raw_address
                encoded_address = urllib.parse.quote(raw_address)
                clickable_map_link = f"https://www.google.com/maps/search/?api=1&query={encoded_address}"
        except: pass

        text_address = map_pin_address if map_pin_address != "-" else (possible_addresses[0] if possible_addresses else "-")

        full_bio = "-"
        try:
            bio_el = driver.find_element(By.XPATH, "//div[contains(@class, 'jet-listing-dynamic-field__content')]")
            full_bio = bio_el.get_attribute("innerText").strip().replace('\n', ' || ')
        except: pass

        breadcrumb_info = "-"
        try:
            breadcrumb_el = driver.find_element(By.ID, "breadcrumbs")
            breadcrumb_info = breadcrumb_el.text.strip()
        except: pass

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
        
    except Exception as e:
        print(f"[ERROR] Profile extraction failure: {e}")
        basic_info.update({"Note": "Profile Scrape Failed"})
    
    return basic_info

# --- PIPELINE EXECUTION ---
if os.path.exists(CONTINUE_FLAG_FILE):
    os.remove(CONTINUE_FLAG_FILE)

page = state["page"]
print(f"[INFO] Initializing sequence starting at page {page}.")
timeout_reached = False

try:
    while True:
        if (time.time() - START_TIME) > TIME_LIMIT_SECONDS:
            print("\n[WARN] Execution time limit threshold reached. Initiating graceful shutdown...")
            with open(CONTINUE_FLAG_FILE, 'w') as f:
                f.write("CONTINUE_REQUIRED")
            timeout_reached = True
            break

        target_url = "https://zdraven-arhiv.com/doctors/" if page == 1 else f"https://zdraven-arhiv.com/doctors/?jsf=jet-engine&pagenum={page}"
            
        print(f"  > Processing PAGE {page}...")
        driver.get(target_url)
        
        try:
            if "404" in driver.title or "Страницата не е намерена" in driver.page_source:
                 print("  [INFO] 404 Not Found. Pagination complete.")
                 break

            wait_time = 10 if page == 1 else 5
            try:
                WebDriverWait(driver, wait_time).until(EC.presence_of_element_located((By.CLASS_NAME, "jet-listing-grid__item")))
                # FIX: Изчакваме допълнително, за да може JS да рендерира всички записи в решетката
                time.sleep(3)
            except:
                print("  [INFO] Exhausted records. Concluding.")
                break

            cards = driver.find_elements(By.XPATH, "//div[contains(@class, 'jet-listing-grid__item')]")
            if not cards: break

            print(f"  [INFO] Found {len(cards)} doctors on the page.")

            doctors_on_page = []
            for card in cards:
                try:
                    link_el = card.find_element(By.CSS_SELECTOR, "a.jet-listing-dynamic-link__link")
                    url = link_el.get_attribute("href")
                    name = link_el.text.strip()
                    
                    if not url: continue
                    
                    doc_data = {
                        "Име": name,
                        "URL": url,
                        "Описание (Лист)": "-" 
                    }
                    doctors_on_page.append(doc_data)
                except: continue

            for doc in doctors_on_page:
                doc_url = doc['URL']
                
                if doc_url in parsed_urls:
                    continue

                full_data = scrape_inner_profile(doc_url, doc)
                save_single_record(full_data)
                mark_as_parsed(doc_url)

            page += 1
            save_state(page)
            
        except Exception as e:
            print(f"[CRITICAL] Page iteration failure on page {page}: {e}")
            break

except Exception as e:
    print(f"[CRITICAL] Global pipeline failure: {e}")
finally:
    try: driver.quit()
    except: pass
    print(f"\n[SUCCESS] Execution block concluded.")
    sys.exit(0)
