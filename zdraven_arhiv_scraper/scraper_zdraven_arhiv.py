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
from selenium.common.exceptions import WebDriverException

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
failed_pages_file = os.path.join(output_dir, "failed_pages_zdraven_arhiv.json")
current_batch_filename = os.path.join(output_dir, "zdraven_arhiv_data_mega.csv")
CONTINUE_FLAG_FILE = os.path.join(output_dir, "CONTINUE_FLAG_ZDRAVEN_ARHIV")

# --- STATE AND MEMORY MANAGEMENT ---
state = {"page": 1, "phase": 1, "consecutive_fails": 0}
if os.path.exists(state_file):
    try:
        with open(state_file, "r") as f:
            state = json.load(f)
            print(f"[INFO] Resuming execution from Phase {state.get('phase', 1)}, Page {state.get('page', 1)}.")
    except Exception:
        print("[WARN] Corrupted state file detected. Initializing new state.")
        state = {"page": 1, "phase": 1, "consecutive_fails": 0}

def save_state(page, phase=1, consecutive_fails=0):
    with open(state_file, "w") as f:
        json.dump({"page": page, "phase": phase, "consecutive_fails": consecutive_fails}, f)

parsed_urls = set()
if os.path.exists(memory_file):
    with open(memory_file, "r", encoding="utf-8") as f:
        for line in f:
            url = line.strip()
            if url: 
                # Добавяме и декодирания, и суровия вариант, за да сме сигурни
                parsed_urls.add(urllib.parse.unquote(url))
                parsed_urls.add(url)
print(f"[INFO] Initialized memory cache with {len(parsed_urls)} processed URLs.")

def mark_as_parsed(raw_url):
    decoded_url = urllib.parse.unquote(raw_url)
    parsed_urls.add(decoded_url)
    parsed_urls.add(raw_url) # Кешираме и двата варианта за сигурност
    with open(memory_file, "a", encoding="utf-8") as f:
        f.write(decoded_url + "\n") # Записваме само красивата кирилица!

# --- FAILED PAGES MANAGEMENT ---
def load_failed_pages():
    if os.path.exists(failed_pages_file):
        try:
            with open(failed_pages_file, "r") as f:
                return json.load(f)
        except: return []
    return []

def save_failed_pages(failed_list):
    with open(failed_pages_file, "w") as f:
        json.dump(failed_list, f)

def add_failed_page(page_num):
    pages = load_failed_pages()
    if page_num not in pages:
        pages.append(page_num)
        save_failed_pages(pages)

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

# --- AUTO-RESPAWN WEBDRIVER INITIALIZATION ---
def create_driver():
    print("[INFO] Booting up Chrome...")
    options = Options()
    options.add_argument('--headless=new') 
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-features=site-per-process')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

    try:
        service = Service(ChromeDriverManager().install())
        drv = webdriver.Chrome(service=service, options=options)
        return drv
    except Exception as e:
        print(f"[CRITICAL] Failed to initiate WebDriver: {e}")
        sys.exit(1)

driver = create_driver()

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

        phones, emails, possible_addresses = [], [], []
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
        except: pass

        map_pin_address = clickable_map_link = "-"
        try:
            iframe = driver.find_element(By.CSS_SELECTOR, "iframe[src*='maps.google.com']")
            raw_address = iframe.get_attribute("title") or iframe.get_attribute("aria-label")
            if raw_address:
                map_pin_address = raw_address
                clickable_map_link = f"https://www.google.com/maps/search/?api=1&query={urllib.parse.quote(raw_address)}"
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
        
    except WebDriverException as we:
        if "crashed" in str(we).lower() or "disconnected" in str(we).lower() or "out of memory" in str(we).lower():
            raise we
        basic_info.update({"Note": "Profile Scrape Failed"})
    except Exception as e:
        basic_info.update({"Note": "Profile Scrape Failed"})
    
    return basic_info

# --- PIPELINE EXECUTION ---
if os.path.exists(CONTINUE_FLAG_FILE):
    os.remove(CONTINUE_FLAG_FILE)

current_phase = state.get("phase", 1)
page = state.get("page", 1)
consecutive_fails = state.get("consecutive_fails", 0)

print(f"[INFO] Initializing sequence starting at Phase {current_phase}, Page {page}.")

try:
    while True:
        if (time.time() - START_TIME) > TIME_LIMIT_SECONDS:
            print("\n[WARN] Execution time limit threshold reached. Initiating graceful shutdown...")
            with open(CONTINUE_FLAG_FILE, 'w') as f:
                f.write("CONTINUE_REQUIRED")
            break

        # ==========================================
        # PHASE 1: БЪРЗА МЕТЛА (Нормален обход)
        # ==========================================
        if current_phase == 1:
            target_url = "https://zdraven-arhiv.com/doctors/" if page == 1 else f"https://zdraven-arhiv.com/doctors/?jsf=jet-engine&pagenum={page}"
            print(f"  > [Phase 1] Processing PAGE {page}...")
            
            page_loaded = False
            retries = 0
            cards = []
            end_of_records = False
            
            while not page_loaded:
                if (time.time() - START_TIME) > TIME_LIMIT_SECONDS: break
                
                try:
                    driver.get(target_url)
                    time.sleep(4)
                    
                    if "404" in driver.title or "Страницата не е намерена" in driver.page_source:
                         print("  [INFO] 404 Not Found. Phase 1 complete.")
                         page_loaded = True
                         end_of_records = True
                         break

                    not_found_els = driver.find_elements(By.CLASS_NAME, "jet-listing-not-found")
                    if not_found_els and not_found_els[0].is_displayed() and "No data was found" in not_found_els[0].text:
                         print("  [INFO] End of database detected (No data was found). Phase 1 complete.")
                         page_loaded = True
                         end_of_records = True
                         break

                    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "jet-listing-grid__item")))
                    time.sleep(3) # Чакаме 3 секунди твърдо да се рендират всички карти
                    
                    cards = driver.find_elements(By.XPATH, "//div[contains(@class, 'jet-listing-grid__item')]")
                    if cards:
                        page_loaded = True
                        print(f"  [INFO] Successfully loaded {len(cards)} doctors on page {page}.")
                        consecutive_fails = 0 
                    else:
                        raise Exception("Cards array empty")
                        
                except WebDriverException as we:
                    if "crashed" in str(we).lower() or "disconnected" in str(we).lower() or "out of memory" in str(we).lower():
                        print(f"  [CRITICAL] Chrome tab crashed! Rebooting WebDriver...")
                        try: driver.quit()
                        except: pass
                        driver = create_driver()
                        continue
                    retries += 1
                    time.sleep(2)
                except Exception:
                    retries += 1
                    time.sleep(2)

                if retries >= 3:
                    print(f"  [WARN] Page {page} is unresponsive (PHP OOM/Timeout). Skipping to next page (will retry in Phase 2).")
                    add_failed_page(page)
                    consecutive_fails += 1
                    page_loaded = True
                    break

            if (time.time() - START_TIME) > TIME_LIMIT_SECONDS:
                save_state(page, phase=1, consecutive_fails=consecutive_fails)
                with open(CONTINUE_FLAG_FILE, 'w') as f: f.write("CONTINUE_REQUIRED")
                break
                
            if consecutive_fails >= 10:
                print(f"[CRITICAL INFO] Hit {consecutive_fails} consecutive broken pages. Assuming end of database!")
                end_of_records = True

            if end_of_records:
                print("[INFO] Phase 1 finished! Transitioning to Phase 2 (Retry failed pages)...")
                current_phase = 2
                consecutive_fails = 0
                save_state(page=1, phase=2, consecutive_fails=0)
                continue

            if cards:
                doctors_on_page = []
                for card in cards:
                    try:
                        link_el = card.find_element(By.CSS_SELECTOR, "a.jet-listing-dynamic-link__link")
                        raw_url = link_el.get_attribute("href")
                        name = link_el.text.strip()
                        if raw_url:
                            decoded_url = urllib.parse.unquote(raw_url)
                            doctors_on_page.append({
                                "Име": name, 
                                "RAW_URL": raw_url, 
                                "URL": decoded_url, 
                                "Описание (Лист)": "-"
                            })
                    except: continue

                for doc in doctors_on_page:
                    raw_url = doc['RAW_URL']
                    decoded_url = doc['URL']
                    
                    if decoded_url in parsed_urls or raw_url in parsed_urls:
                        continue
                        
                    # НОВАТА МЕХАНИКА: Опитваме до 3 пъти да изстържем СЪЩИЯ доктор, ако крашне!
                    for attempt in range(3):
                        try:
                            # Правим копие на doc, за да не се зацапа речника при retry
                            basic_info = {"Име": doc["Име"], "URL": decoded_url, "Описание (Лист)": "-"}
                            full_data = scrape_inner_profile(raw_url, basic_info)
                            save_single_record(full_data)
                            mark_as_parsed(raw_url)
                            break # Успех, излизаме от retry цикъла за този доктор
                        except WebDriverException as we:
                            if "crashed" in str(we).lower() or "disconnected" in str(we).lower() or "out of memory" in str(we).lower():
                                print(f"  [CRITICAL] Chrome crashed inside profile {decoded_url}! Rebooting and retrying (Attempt {attempt+1}/3)...")
                                try: driver.quit()
                                except: pass
                                driver = create_driver()
                                time.sleep(2)
                            else:
                                break # Друга грешка, прескачаме го

            page += 1
            save_state(page, phase=1, consecutive_fails=consecutive_fails)

        # ==========================================
        # PHASE 2: БЕЗКРАЕН ТЕРОР (Счупените страници)
        # ==========================================
        elif current_phase == 2:
            failed_pages = load_failed_pages()
            if not failed_pages:
                print("[SUCCESS] All failed pages successfully recovered! Scraping is 100% complete.")
                break

            target_page = failed_pages[0]
            
            target_url = "https://zdraven-arhiv.com/doctors/" if target_page == 1 else f"https://zdraven-arhiv.com/doctors/?jsf=jet-engine&pagenum={target_page}"
            print(f"  > [Phase 2 - Retry] Processing missed PAGE {target_page}...")

            page_loaded = False
            cards = []
            
            while not page_loaded:
                if (time.time() - START_TIME) > TIME_LIMIT_SECONDS: break
                
                try:
                    driver.get(target_url)
                    time.sleep(4)
                    
                    if "404" in driver.title or "Страницата не е намерена" in driver.page_source:
                         print(f"  [INFO] Page {target_page} is a 404. Removing from retry list.")
                         page_loaded = True
                         break

                    not_found_els = driver.find_elements(By.CLASS_NAME, "jet-listing-not-found")
                    if not_found_els and not_found_els[0].is_displayed() and "No data was found" in not_found_els[0].text:
                         print(f"  [INFO] Page {target_page} has no data. Removing from retry list.")
                         page_loaded = True
                         break

                    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "jet-listing-grid__item")))
                    time.sleep(3)
                    
                    cards = driver.find_elements(By.XPATH, "//div[contains(@class, 'jet-listing-grid__item')]")
                    if cards:
                        page_loaded = True
                        print(f"  [INFO] Recovered {len(cards)} doctors on missed page {target_page}.")
                    else:
                        raise Exception("Cards array empty")
                        
                except WebDriverException as we:
                    if "crashed" in str(we).lower() or "disconnected" in str(we).lower() or "out of memory" in str(we).lower():
                        print(f"  [CRITICAL] Chrome tab crashed! Rebooting WebDriver...")
                        try: driver.quit()
                        except: pass
                        driver = create_driver()
                        continue
                    time.sleep(3)
                except Exception:
                    time.sleep(3)

            if (time.time() - START_TIME) > TIME_LIMIT_SECONDS:
                with open(CONTINUE_FLAG_FILE, 'w') as f: f.write("CONTINUE_REQUIRED")
                break
                
            if cards:
                doctors_on_page = []
                for card in cards:
                    try:
                        link_el = card.find_element(By.CSS_SELECTOR, "a.jet-listing-dynamic-link__link")
                        raw_url = link_el.get_attribute("href")
                        name = link_el.text.strip()
                        if raw_url:
                            decoded_url = urllib.parse.unquote(raw_url)
                            doctors_on_page.append({
                                "Име": name, 
                                "RAW_URL": raw_url, 
                                "URL": decoded_url, 
                                "Описание (Лист)": "-"
                            })
                    except: continue

                for doc in doctors_on_page:
                    raw_url = doc['RAW_URL']
                    decoded_url = doc['URL']
                    
                    if decoded_url in parsed_urls or raw_url in parsed_urls:
                        continue
                        
                    # СЪЩАТА ЗАЩИТА СРЕЩУ КРАШ И ТУК ВЪВ ФАЗА 2
                    for attempt in range(3):
                        try:
                            basic_info = {"Име": doc["Име"], "URL": decoded_url, "Описание (Лист)": "-"}
                            full_data = scrape_inner_profile(raw_url, basic_info)
                            save_single_record(full_data)
                            mark_as_parsed(raw_url)
                            break 
                        except WebDriverException as we:
                            if "crashed" in str(we).lower() or "disconnected" in str(we).lower() or "out of memory" in str(we).lower():
                                print(f"  [CRITICAL] Chrome crashed inside profile {decoded_url}! Rebooting and retrying (Attempt {attempt+1}/3)...")
                                try: driver.quit()
                                except: pass
                                driver = create_driver()
                                time.sleep(2)
                            else:
                                break

            failed_pages.pop(0)
            save_failed_pages(failed_pages)
            save_state(page=target_page, phase=2)

except Exception as e:
    print(f"[CRITICAL] Global pipeline failure: {e}")
finally:
    try: driver.quit()
    except: pass
    print(f"\n[SUCCESS] Execution block concluded.")
    sys.exit(0)
