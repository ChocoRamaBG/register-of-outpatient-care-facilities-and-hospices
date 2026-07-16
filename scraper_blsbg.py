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

# State management files
PROCESSED_LOG_FILE = os.path.join(SCRIPT_DIR, "processed_blsbg_pages.txt")
CONTINUE_FLAG_FILE = os.path.join(SCRIPT_DIR, "CONTINUE_FLAG_BLSBG")
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "bg_medics_dynamic_2029.xlsx")

# Workflow limits
MAX_RUNTIME_SECONDS = 20400  # 5 hours and 40 minutes limit
START_TIME = time.time()

# --- DATA CLEANING HELPER ---
def clean_text(text):
    if not isinstance(text, str):
        return text
    return re.sub(r'[\x00-\x1F\x7F]+', '', text).strip()

# --- STATE MANAGEMENT ---
def get_processed_pages():
    if not os.path.exists(PROCESSED_LOG_FILE):
        return set()
    with open(PROCESSED_LOG_FILE, 'r', encoding='utf-8') as f:
        return set(line.strip() for line in f if line.strip())

def save_processed_page(region_code, page_num):
    with open(PROCESSED_LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{region_code}_{page_num}\n")

def save_to_excel(data, filepath):
    if not data: 
        return
    try:
        df = pd.DataFrame(data)
        df.to_excel(filepath, index=False)
    except Exception as e:
        print(f"   [ERROR] Failed to save file to {filepath}. Exception: {e}")

# --- ELEMENT EXTRACTION ---
def get_text_safe(element, xpath):
    try:
        val = element.find_element(By.XPATH, xpath).text.strip()
        return val if val else "-"
    except:
        return "-"

def get_attr_safe(element, attr):
    try:
        val = element.get_attribute(attr)
        return val if val else "-"
    except:
        return "-"

def main_loop():
    # Remove old flag if present
    if os.path.exists(CONTINUE_FLAG_FILE):
        os.remove(CONTINUE_FLAG_FILE)

    all_data = []

    # --- LOAD EXISTING DATA ---
    if os.path.exists(OUTPUT_FILE):
        print("[INFO] Existing output file found. Loading previous data to prevent overwrite...")
        try:
            df_existing = pd.read_excel(OUTPUT_FILE).fillna("-")
            all_data = df_existing.to_dict('records')
            print(f"[INFO] Successfully loaded {len(all_data)} records from previous session.")
        except Exception as e:
            print(f"[WARN] Error reading old file. Starting fresh. Exception: {e}")

    processed_pages = get_processed_pages()
    print(f"[INFO] Found {len(processed_pages)} previously processed pages.")

    # --- WEBDRIVER CONFIGURATION ---
    print("[INFO] Initializing web driver...")
    options = webdriver.ChromeOptions()
    options.add_argument('--headless=new') 
    options.add_argument('--start-maximized') 
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-blink-features=AutomationControlled') 
    options.add_argument('--no-sandbox') 
    options.add_argument('--disable-dev-shm-usage') 
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--disable-gpu') 
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    # --- DATA COLLECTION PROCESS ---
    for r in range(2, 29): 
        region_code = f"{r:02d}"
        page_num = 1 
        
        while True:
            page_id = f"{region_code}_{page_num}"
            
            # Skip if already processed in a previous workflow run
            if page_id in processed_pages:
                page_num += 1
                continue

            # Enforce execution time limit to allow graceful workflow re-trigger
            elapsed = time.time() - START_TIME
            if elapsed > MAX_RUNTIME_SECONDS:
                print("\n[WARN] Execution time limit reached. Initiating graceful shutdown sequence.")
                with open(CONTINUE_FLAG_FILE, 'w') as f:
                    f.write("CONTINUE_REQUIRED")
                
                save_to_excel(all_data, OUTPUT_FILE)
                driver.quit()
                sys.exit(0)

            target_url = f"https://web.archive.org/web/20201027092646/https://blsbg.eu/bg/medics/unionlist/{region_code}?UIN_page={page_num}"
            print(f"  > Accessing region {region_code}, page {page_num}...")
            
            try:
                driver.get(target_url)
            except Exception:
                time.sleep(2)
                try:
                    driver.get(target_url)
                except:
                    print("  [ERROR] Failed to load page. Skipping.")
                    break 

            if "404" in driver.title or "Page not found" in driver.page_source:
                print(f"  [INFO] Region {region_code} data collection concluded.")
                break

            try:
                rows = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.XPATH, "//table//tr[td]"))
                )
            except TimeoutException:
                if "Няма намерени" in driver.page_source:
                    print(f"  [INFO] No records found for region {region_code}.")
                    break
                else:
                    print("  [WARNING] Timeout detected. Attempting page refresh...")
                    driver.refresh()
                    try:
                        rows = WebDriverWait(driver, 10).until(
                            EC.presence_of_all_elements_located((By.XPATH, "//table//tr[td]"))
                        )
                    except:
                        print("  [ERROR] Refresh failed. Skipping current iteration.")
                        break

            # --- PARSE PAGE DATA ---
            summary_text = "-"
            is_last_page = False
            
            try:
                summary_element = driver.find_element(By.CSS_SELECTOR, "div.summary")
                summary_text = clean_text(summary_element.text)
                
                match = re.search(r'-(\d+)\s+от\s+(\d+)', summary_text)
                if match:
                    current_end = int(match.group(1))
                    total_records = int(match.group(2))
                    if current_end >= total_records:
                        is_last_page = True
            except NoSuchElementException:
                pass

            for row in rows:
                try:
                    uin = get_text_safe(row, "./td[1]")
                    
                    try:
                        img = row.find_element(By.CSS_SELECTOR, "img.expand")
                        adr = get_attr_safe(img, "adr")
                        gadr = get_attr_safe(img, "gadr")
                        tel = get_attr_safe(img, "tel")
                        wrk = get_attr_safe(img, "wrk")
                        spec_attr = get_attr_safe(img, "spec")
                    except NoSuchElementException:
                        adr = gadr = tel = wrk = spec_attr = "-"

                    name = get_text_safe(row, "./td[3]")
                    spec_text = get_text_safe(row, "./td[4]")

                    data_row = {
                        "Region Code": clean_text(region_code),
                        "UIN": clean_text(uin),
                        "Address (Hidden)": clean_text(adr),
                        "G Address (Hidden)": clean_text(gadr),
                        "Phone": clean_text(tel),
                        "Workplace": clean_text(wrk), 
                        "Specialty (Hidden)": clean_text(spec_attr),
                        "Name": clean_text(name),
                        "Specialty (Visible)": clean_text(spec_text),
                        "Source URL": target_url,
                        "Summary Info": summary_text
                    }
                    all_data.append(data_row)
                except Exception:
                    continue
            
            # Log the page as successfully processed and save the state
            save_processed_page(region_code, page_num)
            save_to_excel(all_data, OUTPUT_FILE)

            if is_last_page:
                print(f"  [INFO] Reached the final page for Region {region_code}.")
                break 
            
            page_num += 1

    # Final teardown
    save_to_excel(all_data, OUTPUT_FILE)
    driver.quit()
    print(f"[SUCCESS] Scraping completed. Total records extracted: {len(all_data)}.")

if __name__ == "__main__":
    main_loop()
