import time
import os
import sys
import csv
import json
import urllib.parse
import re
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

output_dir = os.path.join(base_dir, "hapche_outputs")
os.makedirs(output_dir, exist_ok=True)

state_file = os.path.join(output_dir, "savegame_v5_auto.json") 
memory_file = os.path.join(output_dir, "parsed_urls.txt") 
current_batch_filename = os.path.join(output_dir, "hapche_data_mega.csv")
CONTINUE_FLAG_FILE = os.path.join(output_dir, "CONTINUE_FLAG_HAPCHE")

# --- STATE AND MEMORY MANAGEMENT ---
state = {"cat_idx": 0, "page": 1}
if os.path.exists(state_file):
    try:
        with open(state_file, "r") as f:
            state = json.load(f)
            print(f"[INFO] Resuming execution from Category Index {state['cat_idx']}, Page {state['page']}.")
    except Exception:
        print("[WARN] Corrupted state file detected. Initializing new state.")
        state = {"cat_idx": 0, "page": 1}

def save_state(cat_idx, page):
    with open(state_file, "w") as f:
        json.dump({"cat_idx": cat_idx, "page": page}, f)

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
    "Категория", "Име", "URL", "Град", "Специалност", 
    "Описание", "Съобщение/Ваканция",
    "Посещения", "Отметки", "Резервации", "Консултации", 
    "Рейтинг", "Оценки", "Коментари", "Препоръки",
    "Университет", "Година на дипломиране", 
    "Услуги", "Цени", "Партньори", "Апаратура",
    "Адрес", "Координати/Локация", "Телефони", 
    "Работно време", "Email", "Website", "Други координати", 
    "Timestamp"
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

prefs = {
    "profile.managed_default_content_settings.images": 2,
    "profile.managed_default_content_settings.stylesheets": 2,
}
options.add_experimental_option("prefs", prefs)

try:
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(30)
    print("[SUCCESS] WebDriver instantiated successfully.")
except Exception as e:
    print(f"[CRITICAL] Failed to initiate WebDriver: {e}")
    sys.exit(1)

# --- CORE FUNCTIONS ---
def nuke_cookie_popups(driver):
    try:
        btns = driver.find_elements(By.CSS_SELECTOR, "button.fc-cta-consent, button.cc-nb-okagree")
        for btn in btns:
            if btn.is_displayed():
                driver.execute_script("arguments[0].click();", btn)
    except: pass 

def fast_get(driver, url, wait_element_locator, max_retries=3):
    for attempt in range(max_retries):
        try:
            driver.get(url)
            WebDriverWait(driver, 7).until(EC.presence_of_element_located(wait_element_locator))
            driver.execute_script("window.stop();") 
            return True
        except Exception as e:
            print(f"[WARN] Connection timeout (Attempt {attempt+1}/{max_retries})")
            time.sleep(2)
    return False

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
        if not fast_get(driver, url, (By.XPATH, "//h1[contains(@class, 'title')]")):
            return basic_info

        try: basic_info["Име"] = driver.find_element(By.XPATH, "//h1[contains(@class, 'title')]").text.strip()
        except: pass
        
        try: basic_info["Специалност"] = driver.find_element(By.CSS_SELECTOR, ".subtitle--category").text.strip()
        except: pass

        try: basic_info["Град"] = driver.find_element(By.CSS_SELECTOR, ".subtitle--settlement").text.strip()
        except: pass 

        try: basic_info["Описание"] = driver.find_element(By.CSS_SELECTOR, "p.lead-paragraph").text.strip()
        except: basic_info["Описание"] = "-"
        
        try: basic_info["Съобщение/Ваканция"] = driver.find_element(By.CSS_SELECTOR, ".message--attention p").text.strip()
        except: basic_info["Съобщение/Ваканция"] = "-"

        stats_map = {
            "Посещения": "visits", "Отметки": "bookmarks", "Резервации": "reservations",
            "Консултации": "consultations", "Рейтинг": "rating", "Оценки": "votes",
            "Коментари": "comments", "Препоръки": "recommendations"
        }
        for key, id_prefix in stats_map.items():
            try: basic_info[key] = driver.find_element(By.CSS_SELECTOR, f"#{id_prefix}-statistics-metadata-value span").text.strip()
            except: basic_info[key] = "-"

        def get_label_value(label_text):
            try: return driver.find_element(By.XPATH, f"//div[contains(@class, 'label') and contains(text(), '{label_text}')]/following-sibling::div[contains(@class, 'value')]").text.replace('\n', ' | ').strip()
            except: return "-"

        basic_info["Университет"] = get_label_value("Университет")
        basic_info["Година на дипломиране"] = get_label_value("Година на дипломиране")
        basic_info["Телефони"] = get_label_value("Телефон")
        basic_info["Работно време"] = get_label_value("Работно време")
        
        try:
            email_el = driver.find_element(By.XPATH, "//div[contains(@class, 'label') and contains(text(), 'Електронна поща')]/following-sibling::div[contains(@class, 'value')]")
            email_html = email_el.get_attribute('innerHTML')
            email_clean = re.sub(r'<i[^>]*fa-at[^>]*></i>', '@', email_html)
            email_clean = re.sub(r'<[^>]+>', '', email_clean).strip()
            basic_info["Email"] = email_clean if email_clean else "-"
        except: basic_info["Email"] = "-"

        try: basic_info["Website"] = driver.find_element(By.XPATH, "//div[contains(@class, 'label') and contains(text(), 'Интернет страница')]/following-sibling::div[contains(@class, 'value')]//a").get_attribute("href")
        except: basic_info["Website"] = "-"
        
        try:
            addr_elements = driver.find_elements(By.XPATH, "//div[@id='address-label']/following-sibling::div[contains(@class, 'value')]//p/span")
            basic_info["Адрес"] = " | ".join([a.text.strip() for a in addr_elements if a.text.strip()]) or "-"
        except: basic_info["Адрес"] = "-"

        try:
            maps_elements = driver.find_elements(By.XPATH, "//div[@id='address-label']/following-sibling::div[contains(@class, 'value')]//a[contains(@href, 'maps.google.com')]")
            all_maps = []
            for m_el in maps_elements:
                href = m_el.get_attribute("href")
                if href:
                    parsed = urllib.parse.urlparse(href)
                    qs = urllib.parse.parse_qs(parsed.query)
                    if 'q' in qs: all_maps.append(urllib.parse.unquote_plus(qs['q'][0]))
                    else: all_maps.append(href)
            basic_info["Координати/Локация"] = " | ".join(all_maps) if all_maps else "-"
        except: basic_info["Координати/Локация"] = "-"
        
        try:
            other_coords = driver.find_elements(By.XPATH, "//div[contains(@class, 'label') and contains(text(), 'Други координати')]/following-sibling::div[contains(@class, 'value')]//a")
            basic_info["Други координати"] = " | ".join([f"{a.text}: {a.get_attribute('href')}" for a in other_coords]) or "-"
        except: basic_info["Други координати"] = "-"

        def get_base_content(header_text):
            try: return driver.find_element(By.XPATH, f"//h2[text()='{header_text}']/following-sibling::div[@class='base-content']").text.strip()
            except: return "-"

        basic_info["Услуги"] = get_base_content("Услуги")
        basic_info["Цени"] = get_base_content("Цени")
        basic_info["Партньори"] = get_base_content("Партньори")
        basic_info["Апаратура"] = get_base_content("Апаратура")

        basic_info["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return basic_info

    except Exception:
        return basic_info

# --- PIPELINE EXECUTION ---
if os.path.exists(CONTINUE_FLAG_FILE):
    os.remove(CONTINUE_FLAG_FILE)

print("[INFO] Fetching core categories index...")
if not fast_get(driver, "https://www.rating.hapche.bg", (By.ID, "type"), max_retries=5):
    print("[CRITICAL] Unable to load base index URL. Terminating process.")
    driver.quit()
    sys.exit(1)
    
nuke_cookie_popups(driver)

categories = []
try:
    select_element = driver.find_element(By.ID, "type")
    options_els = select_element.find_elements(By.TAG_NAME, "option")
    for opt in options_els:
        val = opt.get_attribute("value")
        text = opt.text.strip()
        if val and val != "-":
            categories.append({"slug": val, "name": text})
    print(f"[SUCCESS] Extracted {len(categories)} operational categories.")
except Exception as e:
    print(f"[CRITICAL] Category extraction failed: {e}")
    driver.quit()
    sys.exit(1)

timeout_reached = False

try:
    for cat_idx in range(state["cat_idx"], len(categories)):
        if timeout_reached: break
        
        category = categories[cat_idx]
        cat_slug = category["slug"]
        cat_name = category["name"]
        
        page = state["page"] if cat_idx == state["cat_idx"] else 1
        
        print(f"\n[INFO] Initializing sequence for Category: {cat_name} ({cat_slug})")

        while True:
            if (time.time() - START_TIME) > TIME_LIMIT_SECONDS:
                print("\n[WARN] Execution time limit threshold reached. Initiating graceful shutdown...")
                with open(CONTINUE_FLAG_FILE, 'w') as f:
                    f.write("CONTINUE_REQUIRED")
                timeout_reached = True
                break

            target_url = f"https://www.rating.hapche.bg/search/{cat_slug}/-/-?page={page}"
            print(f"  > Processing {cat_name} | PAGE {page}...")
            
            try:
                if not fast_get(driver, target_url, (By.CSS_SELECTOR, "body"), max_retries=2):
                    page += 1
                    save_state(cat_idx, page)
                    continue
                
                nuke_cookie_popups(driver)

                links = driver.find_elements(By.CSS_SELECTOR, ".pretty-list h3 a, table.mr-table td.name a")
                
                if not links:
                    print(f"  [INFO] Exhausted records for {cat_name}.")
                    break
                
                doctors_on_page = []
                for link in links:
                    try:
                        url = link.get_attribute("href")
                        if "search" not in url:
                            doctors_on_page.append({
                                "Категория": cat_name,
                                "URL": url
                            })
                    except: continue

                for doc in doctors_on_page:
                    doc_url = doc['URL']
                    
                    if doc_url in parsed_urls:
                        continue

                    full_data = scrape_details_from_profile(doc_url, doc)
                    save_single_record(full_data)
                    mark_as_parsed(doc_url) 

                page += 1
                save_state(cat_idx, page)

            except Exception as e:
                print(f"  [ERROR] Page iteration failure: {e}")
                page += 1 
                save_state(cat_idx, page)

        if not timeout_reached:
            save_state(cat_idx + 1, 1)

except Exception as e:
    print(f"[CRITICAL] Global pipeline failure: {e}")
finally:
    try: driver.quit()
    except: pass
    print(f"\n[SUCCESS] Execution block concluded.")
    sys.exit(0)
