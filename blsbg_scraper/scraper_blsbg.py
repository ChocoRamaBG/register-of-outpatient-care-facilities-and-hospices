import time
import pandas as pd
import os
import re
import sys
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# --- CONFIGURATION & PATHS ---
try:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    SCRIPT_DIR = os.getcwd()

OUTPUT_DIR = os.path.join(SCRIPT_DIR, "blsbg_outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)

PROCESSED_UIN_FILE = os.path.join(OUTPUT_DIR, "processed_blsbg_uin_codes.txt")
CONTINUE_FLAG_FILE = os.path.join(OUTPUT_DIR, "CONTINUE_FLAG_BLSBG")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "bg_medics_dynamic_2029.xlsx")

MAX_RUNTIME_SECONDS = 20400  # Enforce limit for CI/CD environments
START_TIME = time.time()

# --- UTILITIES ---
def clean_text(text):
    if not isinstance(text, str):
        return text
    return re.sub(r'[\x00-\x1F\x7F]+', '', text).strip()

def clean_attribute(text):
    if not isinstance(text, str):
        return "-"
    cleaned = re.sub(r'[\x00-\x1F\x7F]+', '', text).strip()
    return cleaned.replace('\\"', '"')

def get_processed_uins():
    if not os.path.exists(PROCESSED_UIN_FILE):
        return set()
    with open(PROCESSED_UIN_FILE, 'r', encoding='utf-8') as f:
        return set(line.strip() for line in f if line.strip())

def save_processed_uin(uin):
    with open(PROCESSED_UIN_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{uin}\n")

def save_to_excel(data, filepath):
    if not data: 
        return
    try:
        df = pd.DataFrame(data)
        if 'UIN' in df.columns:
            df = df.drop_duplicates(subset=['UIN'], keep='last')
        df.to_excel(filepath, index=False)
    except Exception as e:
        print(f"  [ERROR] I/O Exception during file save: {e}")

def get_text_safe(element, xpath):
    try:
        val = element.find_element(By.XPATH, xpath).text.strip()
        return val if val else "-"
    except:
        return "-"

# --- CORE PIPELINE ---
def main_loop():
    print("[INFO] Initializing UIN-based data extraction pipeline...")

    if os.path.exists(CONTINUE_FLAG_FILE):
        os.remove(CONTINUE_FLAG_FILE)

    all_data = []

    if os.path.exists(OUTPUT_FILE):
        print("[INFO] Locating existing dataset...")
        try:
            df_existing = pd.read_excel(OUTPUT_FILE).fillna("-")
            all_data = df_existing.to_dict('records')
            print(f"[INFO] Successfully loaded {len(all_data)} records from prior sessions.")
        except Exception as e:
            print(f"[WARN] Failed to parse existing file. Starting with empty dataset. Error: {e}")

    processed_uins = get_processed_uins()
    print(f"[INFO] Index loaded: {len(processed_uins)} unique UINs previously processed.")

    print("[INFO] Configuring WebDriver instance...")
    options = webdriver.ChromeOptions()
    options.page_load_strategy = 'eager'
    options.add_argument('--headless=new') 
    options.add_argument('--no-sandbox') 
    options.add_argument('--disable-dev-shm-usage') 
    options.add_argument('--start-maximized') 
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-blink-features=AutomationControlled') 
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--disable-backgrounding-occluded-windows')
    options.add_argument('--disable-renderer-backgrounding')
    options.add_argument('--disable-background-timer-throttling')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36')

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    for r in range(1, 29): 
        region_code = f"{r:02d}"
        page_num = 1 
        last_first_row_data = None  
        
        print(f"\n========================================")
        print(f" PROCESSING REGION: {region_code}")
        print(f"========================================")
        
        while True:
            elapsed = time.time() - START_TIME
            if elapsed > MAX_RUNTIME_SECONDS:
                print("\n[WARN] Execution runtime limit approaching. Initiating graceful shutdown...")
                with open(CONTINUE_FLAG_FILE, 'w') as f:
                    f.write("CONTINUE_REQUIRED")
                save_to_excel(all_data, OUTPUT_FILE)
                driver.quit()
                sys.exit(0)

            target_url = f"https://blsbg.eu/bg/medics/unionlist/{region_code}?UIN_page={page_num}"
            
            rows = []
            page_loaded = False
            
            while not page_loaded:
                elapsed = time.time() - START_TIME
                if elapsed > MAX_RUNTIME_SECONDS:
                    print("\n[WARN] Runtime limit reached while waiting for server. Shutting down...")
                    with open(CONTINUE_FLAG_FILE, 'w') as f:
                        f.write("CONTINUE_REQUIRED")
                    save_to_excel(all_data, OUTPUT_FILE)
                    driver.quit()
                    sys.exit(0)
                
                try:
                    driver.get(target_url)
                    time.sleep(3)
                except Exception as e:
                    print(f"  [WARN] Server is dead. Relaxing for 5 minutes before retry... Error: {e}")
                    time.sleep(300)
                    continue

                if "404" in driver.title or "Page not found" in driver.page_source:
                    break

                try:
                    region_name_element = driver.find_element(By.CSS_SELECTOR, "h2.light.bordered")
                    region_name = region_name_element.text.replace("Списък с членове - РК на БЛС -", "").strip()
                except NoSuchElementException:
                    region_name = f"Region {region_code}"

                try:
                    rows = WebDriverWait(driver, 30).until(
                        EC.presence_of_all_elements_located((By.XPATH, "//table[contains(@class, 'items')]/tbody/tr"))
                    )
                    page_loaded = True
                except TimeoutException:
                    if "Няма намерени" in driver.page_source:
                        page_loaded = True
                        rows = []
                    else:
                        print("  [WARN] DOM Timeout. Page is hanging. Refreshing in 30 secs...")
                        time.sleep(30) 
            
            if not page_loaded and not rows:
                break

            if not rows:
                break
            
            current_first_row_data = get_text_safe(rows[0], "./td[1]")
            if current_first_row_data == last_first_row_data and current_first_row_data != "-":
                break
            last_first_row_data = current_first_row_data

            summary_text = "-"
            is_last_page = False
            
            try:
                summary_element = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.summary"))
                )
                summary_text = clean_text(summary_element.text)
                
                match = re.search(r'(\d+)\s+от\s+(\d+)', summary_text)
                if match:
                    current_end = int(match.group(1))
                    total_records = int(match.group(2))
                    if current_end >= total_records:
                        is_last_page = True
            except TimeoutException:
                pass

            valid_records_this_page = 0
            new_records_this_page = 0

            for row_index, row in enumerate(rows):
                try:
                    if "empty" in row.get_attribute("class") or "Няма намерени" in row.text:
                        continue

                    uin = get_text_safe(row, "./td[1]")
                    if uin == "-": continue 
                    
                    valid_records_this_page += 1

                    if uin in processed_uins:
                        continue 
                    
                    try:
                        img = row.find_element(By.CSS_SELECTOR, "img.expand")
                        adr = clean_attribute(img.get_attribute("adr"))
                        gadr = clean_attribute(img.get_attribute("gadr"))
                        tel = clean_attribute(img.get_attribute("tel"))
                        wrk = clean_attribute(img.get_attribute("wrk"))
                        spec_attr = clean_attribute(img.get_attribute("spec"))
                    except NoSuchElementException:
                        adr = gadr = tel = wrk = spec_attr = "-"

                    name = get_text_safe(row, "./td[3]")
                    spec_text = get_text_safe(row, "./td[4]")

                    data_row = {
                        "Region Code": clean_text(region_code),
                        "Region Name": clean_text(region_name),
                        "UIN": clean_text(uin),
                        "Address (Hidden)": adr,
                        "G Address (Hidden)": gadr,
                        "Phone": tel,
                        "Workplace": wrk, 
                        "Specialty (Hidden)": spec_attr,
                        "Name": clean_text(name),
                        "Specialty (Visible)": clean_text(spec_text),
                        "Source URL": target_url,
                        "Summary Info": summary_text
                    }
                    all_data.append(data_row)
                    new_records_this_page += 1
                    
                    processed_uins.add(uin)
                    save_processed_uin(uin)
                
                except Exception as e:
                    print(f"    [ERROR] Failed to parse row {row_index} on page {page_num}: {e}")
                    continue
            
            if new_records_this_page > 0:
                print(f"  [INFO] Extracted {new_records_this_page} NEW records on page {page_num}.")
                save_to_excel(all_data, OUTPUT_FILE)
            else:
                print(f"  [INFO] Page {page_num} checked. No new records found.")

            if valid_records_this_page == 0:
                break

            if is_last_page:
                print(f"  [SUCCESS] End of dataset reached for Region {region_code}.")
                break 
            
            page_num += 1

    save_to_excel(all_data, OUTPUT_FILE)
    driver.quit()
    print(f"\n[SUCCESS] Pipeline execution complete. Extracted {len(all_data)} records to {OUTPUT_FILE}.")

if __name__ == "__main__":
    main_loop()
