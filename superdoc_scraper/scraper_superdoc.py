import time
import os
import urllib.parse
import re
import sys
import csv
import json
import random
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException, TimeoutException

# --- CONFIGURATION & PATHS ---
START_TIME = time.time()
TIME_LIMIT_SECONDS = 5.4 * 60 * 60

try:
    base_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    base_dir = os.getcwd()

output_dir = os.path.join(base_dir, "superdoc_outputs")
os.makedirs(output_dir, exist_ok=True)

state_file = os.path.join(output_dir, "savegame_superdoc.json")
memory_file = os.path.join(output_dir, "parsed_urls_superdoc.txt")
current_batch_filename = os.path.join(output_dir, "superdoc_data_mega.csv")
CONTINUE_FLAG_FILE = os.path.join(output_dir, "CONTINUE_FLAG_SUPERDOC")

# --- DATA SCHEMA CONFIGURATION ---
main_cols = [
    "Име", "Специалност", "Рейтинг", "Брой оценки", "Онлайн Консултация",
    "Опит", "Работи с деца", "URL", "Дата на скрейпване"
]
hospital_cols = []
for i in range(1, 4):
    sfx = f"_{i}"
    hospital_cols.extend([
        f"Болница{sfx}", f"Град{sfx}", f"Адрес{sfx}", 
        f"Телефони{sfx}", f"НЗОК{sfx}", f"График{sfx}",
        f"Latitude{sfx}", f"Longitude{sfx}", f"Цени{sfx}", f"Застрахователи{sfx}"
    ])
extra_cols = [
    "Кратко резюме", "Образование", "Квалификации", 
    "Биография", "Чужди езици", "Специалности", "Допълнителна информация"
]
fieldnames = main_cols + hospital_cols + extra_cols

if not os.path.exists(current_batch_filename):
    try:
        with open(current_batch_filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
    except Exception as e:
        print(f"[ERROR] Failed to initialize CSV structure: {e}")

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

# --- UTILS (ASCII SANITIZER) ---
def clean_excel_text(text):
    if not isinstance(text, str):
        return text
    return re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', text)

def get_text_safe(xpath, default="-"):
    try:
        element = driver.find_element(By.XPATH, xpath)
        return element.text.strip().replace('\n', ' ')
    except: return default

def get_visible_element_text(by, value):
    try:
        elements = driver.find_elements(by, value)
        for el in elements:
            if el.is_displayed() and el.text.strip():
                return el
        for el in elements:
            if el.text.strip():
                return el
    except: pass
    return None

def save_single_record(record):
    if not record: return
    try:
        cleaned_record = {k: clean_excel_text(v) for k, v in record.items()}
        for col in fieldnames:
            if col not in cleaned_record:
                cleaned_record[col] = "-"

        with open(current_batch_filename, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writerow(cleaned_record)
        print(f"  [SUCCESS] Saved: {cleaned_record.get('Име', 'Unknown')}")
    except Exception as e:
        print(f"  [ERROR] Failed to append record to CSV: {e}")

# --- AUTO-RESPAWN WEBDRIVER INITIALIZATION ---
def create_driver():
    print("[INFO] Booting up Chrome...")
    options = Options()
    options.add_argument('--headless=new') 
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-features=site-per-process') # FIX OOM: Помага за освобождаване на памет
    options.add_argument('--log-level=3') 
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    try:
        service = Service(ChromeDriverManager().install())
        drv = webdriver.Chrome(service=service, options=options)
        return drv
    except Exception as e:
        print(f"[CRITICAL] Failed to initiate WebDriver: {e}")
        sys.exit(1)

global driver
driver = create_driver()

# --- SCRAPING FUNCTIONS ---
def scrape_calendar():
    try:
        try: WebDriverWait(driver, 4).until(EC.presence_of_element_located((By.CLASS_NAME, "calendar-app")))
        except: return "Няма календар"

        visible_calendar = get_visible_element_text(By.CLASS_NAME, "calendar-app")
        search_context = visible_calendar if visible_calendar else driver

        if not visible_calendar: days = driver.find_elements(By.CSS_SELECTOR, ".days-holder .day")
        else: days = visible_calendar.find_elements(By.CSS_SELECTOR, ".days-holder .day")

        summary = []
        for day in days:
            try:
                if not day.is_displayed(): continue
                header = day.find_element(By.CLASS_NAME, "day-header").text.replace('\n', ' ').strip()
                slots = [s.text.strip() for s in day.find_elements(By.CSS_SELECTOR, ".calendar-slot.free .slot-start-time")]
                if slots: summary.append(f"{header}: {', '.join(slots)}")
            except: continue
        
        if summary: return " | ".join(summary)
            
        try:
            next_msg_el = search_context.find_element(By.CLASS_NAME, "calendar-available-date")
            if next_msg_el and next_msg_el.is_displayed(): return next_msg_el.text.strip()
        except: pass
            
        return "Няма свободни слотове"
    except: return "-"

def scrape_insurances():
    try:
        active_container = None
        name_el = get_visible_element_text(By.XPATH, "//*[@itemprop='memberOf']")
        
        if not name_el:
             visible_cal = get_visible_element_text(By.CLASS_NAME, "calendar-app")
             if visible_cal:
                 try: active_container = visible_cal.find_element(By.XPATH, "./parent::div")
                 except: pass
        else:
             try:
                 cal_type = name_el.find_element(By.XPATH, "./ancestor::div[contains(@class, 'calendar-type')]")
                 active_container = cal_type.find_element(By.XPATH, "./parent::div")
             except: pass

        if not active_container: return "-" 

        try:
            ins_block = active_container.find_element(By.CSS_SELECTOR, ".calendar-insurances")
            chips = ins_block.find_elements(By.CSS_SELECTOR, ".chip.neutral")
        except: return "-"
        
        if not chips: return "-"

        ins_list = []
        for chip in chips:
            txt = chip.text.strip()
            if not txt: txt = driver.execute_script("return arguments[0].textContent;", chip).strip()
            if txt: ins_list.append(txt)

        return ", ".join(ins_list) if ins_list else "-"
    except: return "-"

def scrape_prices():
    try:
        active_container = None
        name_el = get_visible_element_text(By.XPATH, "//*[@itemprop='memberOf']")
        
        if not name_el:
             visible_cal = get_visible_element_text(By.CLASS_NAME, "calendar-app")
             if visible_cal:
                 try: active_container = visible_cal.find_element(By.XPATH, "./parent::div")
                 except: pass
        else:
             try:
                 cal_type = name_el.find_element(By.XPATH, "./ancestor::div[contains(@class, 'calendar-type')]")
                 active_container = cal_type.find_element(By.XPATH, "./parent::div")
             except: pass

        if not active_container: return "-"

        try:
            prices_block = active_container.find_element(By.CSS_SELECTOR, ".calendar-prices")
            price_items = prices_block.find_elements(By.CSS_SELECTOR, ".list-group-item")
        except: return "Няма цени"
        
        if not price_items: return "Няма цени"

        prices_list = []
        for item in price_items:
            try:
                p_name = driver.execute_script("return arguments[0].querySelector('.price-name') ? arguments[0].querySelector('.price-name').textContent : '';", item).strip()
                p_val = driver.execute_script("return arguments[0].querySelector('.price-value') ? arguments[0].querySelector('.price-value').textContent : '';", item).strip()
                
                if p_name or p_val: clean_text = f"{p_name}: {p_val}"
                else:
                    raw_text = driver.execute_script("return arguments[0].textContent;", item)
                    clean_text = " ".join(raw_text.split())
                prices_list.append(clean_text)
            except: continue

        return " | ".join(prices_list)
    except: return "-"

def get_active_location_data():
    h_name = h_city = h_addr = h_phone = h_nzok = h_lat = h_lng = "-"
    try:
        name_el = get_visible_element_text(By.XPATH, "//*[@itemprop='memberOf']")
        if name_el: h_name = name_el.text.strip()
        else:
            alt_name = get_visible_element_text(By.CSS_SELECTOR, ".calendar-type h4")
            if alt_name: h_name = alt_name.text.strip()

        target_addr_el = None
        if name_el:
            try:
                parent_cal_type = name_el.find_element(By.XPATH, "./ancestor::div[contains(@class, 'calendar-type')]")
                candidates = parent_cal_type.find_elements(By.CSS_SELECTOR, ".text-muted.small")
                for c in candidates:
                    if c.is_displayed():
                        target_addr_el = c
                        break
            except: pass
        
        if not target_addr_el:
             addr_elements = driver.find_elements(By.CSS_SELECTOR, ".text-muted.small")
             for el in addr_elements:
                if el.is_displayed() and ('·' in el.text or ',' in el.text):
                    target_addr_el = el
                    break

        full_addr_text = "-"
        found_phones = []

        if target_addr_el:
            raw_text = driver.execute_script("return arguments[0].textContent;", target_addr_el)
            full_addr_text = " ".join(raw_text.split())
            matches = re.findall(r'(?:\+359|0)(?:[\s-]*\d){8,}', full_addr_text)
            if matches: found_phones.extend([m.strip() for m in matches])
        
        clean_addr_text = full_addr_text
        for ph in found_phones: clean_addr_text = clean_addr_text.replace(ph, "")
            
        clean_addr_text = re.sub(r'(?i)Телефон:.*', '', clean_addr_text).strip()
        clean_addr_text = re.sub(r'(?:\+359|0)8[0-9\s]{8,}.*$', '', clean_addr_text).strip()
        
        if clean_addr_text != "-" and clean_addr_text:
            if '·' in clean_addr_text:
                parts = clean_addr_text.split('·')
                h_city = parts[0].strip()
                h_addr = " ".join(parts[1:]).strip()
            else:
                h_city = clean_addr_text.split(',')[0].strip() if ',' in clean_addr_text else "Виж адреса"
                h_addr = clean_addr_text

        try:
             if name_el:
                 parent = name_el.find_element(By.XPATH, "./ancestor::div[contains(@class, 'calendar-type')]")
                 h_lat = parent.find_element(By.XPATH, ".//meta[@itemprop='latitude']").get_attribute("content")
                 h_lng = parent.find_element(By.XPATH, ".//meta[@itemprop='longitude']").get_attribute("content")
             else:
                 h_lat = driver.find_element(By.XPATH, "//meta[@itemprop='latitude']").get_attribute("content")
                 h_lng = driver.find_element(By.XPATH, "//meta[@itemprop='longitude']").get_attribute("content")
        except: pass

        try:
            phone_els = driver.find_elements(By.XPATH, "//span[@itemprop='telephone']")
            for p in phone_els:
                if p.is_displayed():
                    ptxt = p.text.strip()
                    if ptxt: found_phones.append(ptxt)
            
            if found_phones:
                clean_phones = [p.strip() for p in found_phones]
                h_phone = ", ".join(list(dict.fromkeys(clean_phones)))
        except: pass

        try:
            h_nzok = "Не"
            if name_el:
                try:
                    cal_type_container = name_el.find_element(By.XPATH, "./ancestor::div[contains(@class, 'calendar-type')]")
                    hospital_wrapper = cal_type_container.find_element(By.XPATH, "./parent::div")
                    candidates = hospital_wrapper.find_elements(By.XPATH, ".//*[contains(@class, 'chip') and (contains(., 'Работи с НЗОК') or .//img[contains(@src, 'nhp.png')])]")
                    for c in candidates:
                        if c.is_displayed():
                            h_nzok = "Да"
                            break
                except: pass
        except: pass

    except Exception as e:
        print(f"[WARN] Location Extraction Error: {e}")

    return h_name, h_city, h_addr, h_phone, h_nzok, h_lat, h_lng

def scrape_doctor_profile(url):
    try:
        driver.get(url)
        if "403 Forbidden" in driver.title:
            print(f"[CRITICAL] ACCESS DENIED (403) for {url}")
            return None

        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "h1")))
        time.sleep(1.0) 
        
        rating_val, rev_count = "-", "0"
        try:
            rev_div = driver.find_element(By.CLASS_NAME, "review-number")
            raw_js = " ".join(driver.execute_script("return arguments[0].textContent;", rev_div).split())
            rm = re.search(r"(\d+(?:\.\d+)?)", raw_js)
            if rm: rating_val = rm.group(1)
            cm = re.search(r"(\d+)\s*оценки", raw_js)
            if cm: rev_count = cm.group(1)
        except: pass

        doc_info = {
            "Име": get_text_safe("//h1[@itemprop='name']"),
            "Специалност": get_text_safe("//h2[contains(@class, 'doctor-specialties')]"),
            "Рейтинг": rating_val,
            "Брой оценки": rev_count,
            "Онлайн Консултация": "Да" if driver.find_elements(By.XPATH, "//a[contains(@data-scrollto, 'mobile-app')]") else "Не",
            "Опит": get_text_safe("//span[contains(@class, 'text-muted') and contains(text(), 'опит')]"),
            "Работи с деца": "Да" if driver.find_elements(By.XPATH, "//span[contains(., 'Работи с деца')]") else "Не",
            "URL": url,
            "Дата на скрейпване": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Кратко резюме": get_text_safe("//div[contains(@class, 'col-lg-11')]//p[1]"),
            "Образование": get_text_safe("//h5[contains(text(), 'Образование')]/following-sibling::p[1]"),
            "Квалификации": get_text_safe("//h5[contains(text(), 'Квалификации')]/following-sibling::ul[1]"),
            "Биография": get_text_safe("//h5[contains(text(), 'Биография')]/following-sibling::p[1]"),
            "Чужди езици": get_text_safe("//h5[contains(text(), 'чужди езици')]/following-sibling::p[1]"),
            "Специалности": get_text_safe("//h5[contains(text(), 'Специалности')]/following-sibling::ul[1]"),
            "Допълнителна информация": get_text_safe("//h5[contains(text(), 'Допълнителна информация')]/following-sibling::*[1]")
        }

        practices = driver.find_elements(By.CSS_SELECTOR, "label.form-check")
        
        if practices:
            for i in range(3):
                sfx = f"_{i+1}"
                if i < len(practices):
                    try:
                        curr_practices = driver.find_elements(By.CSS_SELECTOR, "label.form-check")
                        p_elem = curr_practices[i]
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", p_elem)
                        driver.execute_script("arguments[0].click();", p_elem)
                        time.sleep(1.5) 
                        
                        h_name, h_city, h_addr, h_phone, h_nzok, h_lat, h_lng = get_active_location_data()
                        h_prices = scrape_prices()
                        h_sched = scrape_calendar()
                        h_insurances = scrape_insurances()
                    except Exception as e:
                        h_name = f"Error: {e}"
                        # FIX 1: Верижно присвояване, за да няма проблем с разопаковането!
                        h_city = h_addr = h_phone = h_nzok = h_sched = h_lat = h_lng = h_prices = h_insurances = "-"
                else:
                     h_name = h_city = h_addr = h_phone = h_nzok = h_sched = h_lat = h_lng = h_prices = h_insurances = "-"
                
                doc_info[f"Болница{sfx}"] = h_name
                doc_info[f"Град{sfx}"] = h_city
                doc_info[f"Адрес{sfx}"] = h_addr
                doc_info[f"Телефони{sfx}"] = h_phone
                doc_info[f"НЗОК{sfx}"] = h_nzok
                doc_info[f"График{sfx}"] = h_sched
                doc_info[f"Latitude{sfx}"] = h_lat
                doc_info[f"Longitude{sfx}"] = h_lng
                doc_info[f"Цени{sfx}"] = h_prices
                doc_info[f"Застрахователи{sfx}"] = h_insurances
        else:
             sfx = "_1"
             try:
                h_name, h_city, h_addr, h_phone, h_nzok, h_lat, h_lng = get_active_location_data()
                h_prices = scrape_prices()
                h_sched = scrape_calendar()
                h_insurances = scrape_insurances()
             except Exception as e:
                h_name = f"Error: {e}"
                h_city = h_addr = h_phone = h_nzok = h_sched = h_lat = h_lng = h_prices = h_insurances = "-"
            
             doc_info[f"Болница{sfx}"] = h_name
             doc_info[f"Град{sfx}"] = h_city
             doc_info[f"Адрес{sfx}"] = h_addr
             doc_info[f"Телефони{sfx}"] = h_phone
             doc_info[f"НЗОК{sfx}"] = h_nzok
             doc_info[f"График{sfx}"] = h_sched
             doc_info[f"Latitude{sfx}"] = h_lat
             doc_info[f"Longitude{sfx}"] = h_lng
             doc_info[f"Цени{sfx}"] = h_prices
             doc_info[f"Застрахователи{sfx}"] = h_insurances
             
             for i in range(2, 4):
                sfx = f"_{i}"
                for field in ["Болница", "Град", "Адрес", "Телефони", "НЗОК", "График", "Latitude", "Longitude", "Цени", "Застрахователи"]:
                     doc_info[f"{field}{sfx}"] = "-"

        return doc_info

    # FIX 2: Прихващаме OOM крашовете вътре в профила и ги хвърляме към главния цикъл, за да рестартира браузъра
    except WebDriverException as we:
        error_msg = str(we).lower()
        if "crashed" in error_msg or "disconnected" in error_msg:
            raise we
        print(f"[ERROR] WebDriver Exception on profile {url}: {we}")
        return None
    except Exception as e:
        print(f"[ERROR] Profile extraction failure: {e}")
        return None


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

        target_url = f"https://superdoc.bg/lekari?sort=latest&page={page}"
        print(f"  > Processing PAGE {page}...")
        
        try:
            driver.get(target_url)
            
            try:
                WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CLASS_NAME, "search-result-link")))
            except:
                print("  [INFO] Exhausted records. Pagination complete.")
                break

            urls = list(dict.fromkeys([l.get_attribute("href") for l in driver.find_elements(By.CLASS_NAME, "search-result-link") if l.get_attribute("href")]))
            
            if not urls: 
                print("  [INFO] No URLs found on page. Concluding.")
                break

            for u in urls:
                if u in parsed_urls:
                    continue

                time.sleep(random.uniform(1.5, 2.5)) 
                
                full_data = scrape_doctor_profile(u)
                if full_data:
                    save_single_record(full_data)
                    mark_as_parsed(u)
            
            page += 1
            save_state(page)

        # ХВАЩАМЕ КРАША ТУК И ПРАВИМ RESPAWN
        except WebDriverException as we:
            error_msg = str(we).lower()
            if "crashed" in error_msg or "disconnected" in error_msg or "out of memory" in error_msg:
                print(f"  [CRITICAL] Chrome tab crashed on page {page}! Rebooting WebDriver to recover...")
                try: driver.quit()
                except: pass
                time.sleep(3)
                driver = create_driver() 
                # Използваме continue, за да не увеличаваме страницата, а да я завъртим наново
                continue
            else:
                print(f"[CRITICAL] Page iteration failure on page {page}: {we}")
                break

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
