import time
import os
import sys
import csv
import json
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, WebDriverException

# --- CONFIGURATION & PATHS ---
START_TIME = time.time()
TIME_LIMIT_SECONDS = 5.4 * 60 * 60

try:
    base_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    base_dir = os.getcwd()

output_dir = os.path.join(base_dir, "lekaribg_outputs")
os.makedirs(output_dir, exist_ok=True)

state_file = os.path.join(output_dir, "savegame_lekaribg.json")
memory_file = os.path.join(output_dir, "parsed_urls_lekaribg.txt")
current_batch_filename = os.path.join(output_dir, "lekaribg_data_mega.csv")
CONTINUE_FLAG_FILE = os.path.join(output_dir, "CONTINUE_FLAG_LEKARIBG")

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
    "Име", "URL", "Телефон", "Email", "Работно време", 
    "Адрес", "Специалност", "Visits", "Last Updated"
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
options.add_argument('--start-maximized') 
options.add_argument('--no-sandbox')    
options.add_argument('--disable-dev-shm-usage') 
options.add_argument('--disable-gpu')   
options.add_argument('--disable-blink-features=AutomationControlled')
options.page_load_strategy = 'eager' 
options.add_argument('--log-level=3')
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

try:
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(30)
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

def scrape_details_from_profile(url, basic_info):
    try:
        driver.get(url)
        try:
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        except:
            return basic_info

        try:
            full_name = driver.find_element(By.XPATH, "//h1//span[@itemprop='name']").text.strip()
            basic_info["Име"] = full_name
        except: pass

        found_email = False
        try:
            email_row = driver.find_element(By.CLASS_NAME, "rowwemail")
            email_link = email_row.find_element(By.TAG_NAME, "a")
            email_text = email_link.text.strip()
            if email_text:
                basic_info["Email"] = email_text
                found_email = True
        except: pass 

        try:
            table = driver.find_element(By.ID, "TableCustomFieldsBig")
            rows = table.find_elements(By.TAG_NAME, "tr")
            
            for row in rows:
                try:
                    th = row.find_element(By.TAG_NAME, "th").text.strip()
                    td_el = row.find_element(By.TAG_NAME, "td")
                    td = td_el.text.strip()
                    
                    if "Работно време" in th:
                        basic_info["Работно време"] = td
                    elif "Телефон" in th:
                        basic_info["Телефон"] = td
                    elif "Адрес" in th:
                        basic_info["Адрес"] = td
                    elif "Специалност" in th:
                        basic_info["Специалност"] = td
                    elif not found_email and ("Имейл" in th or "Email" in th):
                        basic_info["Email"] = td
                        found_email = True
                except: continue
        except: pass

        basic_info["Last Updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return basic_info

    except Exception as e:
        print(f"[ERROR] Profile extraction failure: {e}")
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

        target_url = f"https://lekaribg.net/listing-category/lekari/page/{page}/"
        print(f"  > Processing PAGE {page}...")
        driver.get(target_url)
        
        try:
            try:
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".wlt_search_results"))
                )
            except:
                print("  [INFO] Exhausted records. Pagination complete.")
                break

            items = driver.find_elements(By.CSS_SELECTOR, ".wlt_search_results .itemdata")
            
            if not items:
                print("  [INFO] Zero results found on page. Concluding.")
                break

            doctors_on_page = []
            for item in items:
                try:
                    link_el = item.find_element(By.CSS_SELECTOR, "h4 a")
                    name = link_el.text.strip()
                    url = link_el.get_attribute("href")
                    
                    phone_backup = "-"
                    try:
                        phone_backup = item.find_element(By.CSS_SELECTOR, ".wlt_shortcode_phone").text.strip()
                    except: pass

                    visits = "0"
                    try:
                        visits_el = item.find_element(By.CSS_SELECTOR, ".wlt_shortcode_hits")
                        visits = visits_el.text.strip().replace(",", "") 
                    except: 
                        visits = "N/A"

                    doc_data = {
                        "Име": name,
                        "URL": url,
                        "Телефон": phone_backup,
                        "Email": "-",
                        "Работно време": "-",
                        "Адрес": "-",
                        "Специалност": "-",
                        "Visits": visits,
                        "Last Updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    doctors_on_page.append(doc_data)
                except: continue

            for doc in doctors_on_page:
                doc_url = doc['URL']
                
                if doc_url in parsed_urls:
                    continue

                full_data = scrape_details_from_profile(doc_url, doc)
                save_single_record(full_data)
                mark_as_parsed(doc_url)

            page += 1
            save_state(page)
            
        except Exception as e:
            print(f"[CRITICAL] Page iteration failure on page {page}: {e}")
            page += 1
            if page > 1000: 
                print("[WARN] Hard safety limit reached.")
                break
            continue

except Exception as e:
    print(f"[CRITICAL] Global pipeline failure: {e}")
finally:
    try: driver.quit()
    except: pass
    print(f"\n[SUCCESS] Execution block concluded.")
    sys.exit(0)
