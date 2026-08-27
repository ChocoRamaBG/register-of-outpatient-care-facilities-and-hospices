import os
import sys
import time
import json
import csv
import re
from urllib.parse import unquote, urlparse
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# ============================================================
# КОНФИГУРАЦИЯ
# ============================================================
START_TIME = time.time()
TIME_LIMIT_SECONDS = 5.4 * 60 * 60  # ~5 часа и 24 минути

BASE_SEARCH_URL = "https://myhealth.bg/search/?page="
MAX_PAGE_RETRIES = 3

# ============================================================
# ПЪТИЩА И ДИРЕКТОРИИ
# ============================================================
try:
    output_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    output_dir = os.getcwd()

output_dir = os.path.join(output_dir, "myhealth_outputs")
os.makedirs(output_dir, exist_ok=True)

state_file = os.path.join(output_dir, "savegame_myhealth.json")
memory_file = os.path.join(output_dir, "parsed_urls_myhealth.txt")
csv_file_path = os.path.join(output_dir, "myhealth_doctors_full.csv")
CONTINUE_FLAG_FILE = os.path.join(output_dir, "CONTINUE_FLAG_MYHEALTH")

# ============================================================
# СХЕМА ЗА ЗАПИС НА ДАННИ (CSV)
# ============================================================
fieldnames = [
    "Име", "Специалност", "Рейтинг_Инфо", "Първи свободен час (Общо)", 
    "Телефони", "НЗОК", "Биография", "URL", "Timestamp", "Цени", "Застрахователи"
]
for i in range(1, 5): # Според вашия код, практиките се запълват до 4
    fieldnames.extend([f"Hospital_{i}", f"Address_{i}", f"First_Free_{i}", f"Coords_{i}"])

if not os.path.exists(csv_file_path):
    with open(csv_file_path, mode="w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

# ============================================================
# УПРАВЛЕНИЕ НА ВРЕМЕТО И СЪСТОЯНИЕТО
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
        print(f"[INFO] Възстановяване на сесията: Стартиране от страница {state['page']}.")
    except Exception as e:
        print(f"[WARN] Грешка при зареждане на състоянието: {e}")

def save_state():
    temp_file = state_file + ".tmp"
    try:
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        os.replace(temp_file, state_file)
    except Exception as e:
        print(f"[ERROR] Неуспешен запис на state файл: {e}")

# ============================================================
# ПАМЕТ ЗА ОБРАБОТЕНИ URL АДРЕСИ
# ============================================================
parsed_urls = set()

if os.path.exists(memory_file):
    with open(memory_file, "r", encoding="utf-8") as f:
        for line in f:
            url = line.strip()
            if url:
                parsed_urls.add(unquote(url))
                parsed_urls.add(url)
print(f"[INFO] Заредени {len(parsed_urls)} вече обработени адреса.")

def mark_as_parsed(url):
    decoded = unquote(url)
    parsed_urls.add(decoded)
    parsed_urls.add(url)
    with open(memory_file, "a", encoding="utf-8") as f:
        f.write(decoded + "\n")

# ============================================================
# SELENIUM ИНСТАНЦИЯ
# ============================================================
driver = None

def init_driver():
    global driver
    options = webdriver.ChromeOptions()
    options.add_argument('--start-maximized')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--log-level=3')
    
    # Задължителни флагове за работа в GitHub Actions (Headless Linux)
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
    except Exception as e:
        print(f"[ERROR] Грешка при инициализация на драйвъра: {e}")
        raise e

def restart_driver():
    global driver
    print("[INFO] Рестартиране на браузъра...")
    try:
        if driver: driver.quit()
    except: pass
    time.sleep(2)
    init_driver()

def close_driver():
    global driver
    try:
        if driver: driver.quit()
    except: pass

# ============================================================
# ФУНКЦИИ ЗА ЕКСТРАКЦИЯ (БЕЗ ПРОМЯНА ОТ ВАШИЯ КОД)
# ============================================================
def get_text_safe(xpath, search_context=None, default="-"):
    try:
        ctx = search_context if search_context else driver
        element = ctx.find_element(By.XPATH, xpath)
        return element.text.strip().replace('\n', ' ')
    except:
        return default

def scrape_insurances_myhealth():
    try:
        logos = driver.find_elements(By.XPATH, "//div[contains(@class, 'practice__insurance-logos')]//img")
        insurances = [img.get_attribute("alt").strip() for img in logos if img.get_attribute("alt")]
        return ", ".join(insurances) if insurances else "-"
    except:
        return "-"

def scrape_prices_myhealth():
    try:
        price_items = driver.find_elements(By.XPATH, "//div[contains(@class, 'practice__pricing-text--item')]")
        found_prices = []
        for item in price_items:
            try:
                name = item.find_element(By.XPATH, ".//p[contains(@class, 'dummy--reason__name')]").text.strip()
                val = item.find_element(By.XPATH, ".//p[contains(@class, 'dummy--reason__price')]").text.strip()
                found_prices.append(f"{name}: {val}")
            except:
                continue
        return " | ".join(found_prices) if found_prices else "-"
    except:
        return "Няма кинти, брат"

def get_coordinates_from_map_link(context=None):
    try:
        ctx = context if context else driver
        map_link = ctx.find_element(By.XPATH, ".//a[contains(@href, 'google.com/maps') and contains(@href, 'daddr')]")
        href = map_link.get_attribute("href")
        match = re.search(r"daddr=([\d\.]+),([\d\.]+)", href)
        if match:
            return f"{match.group(1)}, {match.group(2)}"
        return "-"
    except:
        return "-"

def get_full_biography():
    try:
        hidden_bio_el = driver.find_elements(By.ID, "hidden-profile-resume")
        if hidden_bio_el:
            text = driver.execute_script("return arguments[0].textContent;", hidden_bio_el[0]).strip()
            if text: return text
        try:
            read_more_btn = driver.find_element(By.CSS_SELECTOR, "button[data-hidden-text-id='profile-resume']")
            if read_more_btn.is_displayed():
                driver.execute_script("arguments[0].click();", read_more_btn)
                time.sleep(0.5) 
        except: pass 
        bio_el = driver.find_element(By.ID, "profile-resume")
        return bio_el.text.strip()
    except:
        return "-"

def scrape_practices_detailed():
    practices_data = []
    try:
        free_dates_map = {}
        try:
            dates_container = driver.find_element(By.CLASS_NAME, "dummy--detailed-profile-card__practices")
            titles = dates_container.find_elements(By.CLASS_NAME, "dummy--detailed-profile-card__practices-title")
            dates = dates_container.find_elements(By.CLASS_NAME, "dummy--detailed-profile-card__practices-fa")
            if len(titles) == len(dates):
                for i in range(len(titles)):
                    t_text = titles[i].text.strip()
                    d_raw = dates[i].get_attribute("data-date")
                    d_text = dates[i].text.strip()
                    final_date = d_raw.replace("T", " ").split("+")[0] if d_raw else d_text
                    key = re.sub(r'\s+', '', t_text.lower())[:50] 
                    free_dates_map[key] = final_date
        except: pass

        workplaces = driver.find_elements(By.CLASS_NAME, "doctor-details__workplace-item")
        for wp in workplaces:
            try:
                h_name = wp.find_element(By.CLASS_NAME, "doctor-details__workplace-item-title").text.strip()
                h_addr = wp.find_element(By.CLASS_NAME, "doctor-details__workplace-item-address").text.strip()
                h_coords = get_coordinates_from_map_link(wp)
                h_date = "Няма свободни часове"
                
                check_str_full = re.sub(r'\s+', '', (h_name + h_addr).lower())
                check_str_addr = re.sub(r'\s+', '', h_addr.lower())
                
                for k, v in free_dates_map.items():
                    if k in check_str_full or check_str_full in k:
                        h_date = v
                        break
                    if check_str_addr and len(check_str_addr) > 5 and check_str_addr in k:
                        h_date = v
                        break
                
                practices_data.append({
                    "Hospital": h_name, "Address": h_addr, "First_Date": h_date, "Coords": h_coords
                })
            except: continue
    except Exception as e:
        print(f"[ERROR] Practice Scrape Error: {e}")
    return practices_data

def get_all_first_available_dates_summary():
    dates_found = []
    try:
        date_elements = driver.find_elements(By.CLASS_NAME, "dummy--detailed-profile-card__practices-fa")
        for date_el in date_elements:
            raw_date = date_el.get_attribute("data-date")
            if raw_date:
                clean_date = raw_date.replace("T", " ").split("+")[0]
                dates_found.append(clean_date)
            else:
                txt = date_el.text.strip()
                if txt: dates_found.append(txt)
    except: pass
    if not dates_found:
        try:
            btns = driver.find_elements(By.CLASS_NAME, "dummy--booking-component__first_available")
            for btn in btns:
                raw_date = btn.get_attribute("data-dummy-first-available")
                if raw_date:
                    clean_date = raw_date.replace("T", " ").split("+")[0]
                    dates_found.append(clean_date)
        except: pass
    return " | ".join(dates_found) if dates_found else "Няма свободни часове"

def scrape_doctor_profile_myhealth(url):
    driver.get(url)
    try:
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CLASS_NAME, "doctor-header")))
        time.sleep(1.0) 
        
        doc_name = get_text_safe("//div[contains(@class, 'doctor-header')]//h2/a")
        specialty = get_text_safe("//div[contains(@class, 'doctor-speciality')]")
        rating_text = get_text_safe("//span[contains(@class, 'doctor-rating-score_count')]")
        bio = get_full_biography()

        nzok = "Не"
        try:
            if driver.find_elements(By.XPATH, "//span[contains(@class, 'ww-nzok')]"): nzok = "Да"
        except: pass

        phones = []
        try:
            phone_links = driver.find_elements(By.XPATH, "//a[contains(@href, 'tel:')]")
            phones = [lnk.get_attribute("href").replace("tel:", "") for lnk in phone_links]
        except: pass
        phone_str = ", ".join(list(set(phones))) if phones else "-"

        doc_info = {
            "Име": doc_name,
            "Специалност": specialty,
            "Рейтинг_Инфо": rating_text,
            "Телефони": phone_str,
            "НЗОК": nzok,
            "Биография": bio[:1000], 
            "URL": url,
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Цени": scrape_prices_myhealth(),
            "Застрахователи": scrape_insurances_myhealth(),
            "Първи свободен час (Общо)": get_all_first_available_dates_summary()
        }
        
        practices = scrape_practices_detailed()
        if not practices:
             practices = [{"Hospital": "-", "Address": "-", "First_Date": "-", "Coords": "-"}]

        for i, p in enumerate(practices):
            idx = i + 1
            if idx > 4: break 
            doc_info[f"Hospital_{idx}"] = p["Hospital"]
            doc_info[f"Address_{idx}"] = p["Address"]
            doc_info[f"First_Free_{idx}"] = p["First_Date"]
            doc_info[f"Coords_{idx}"] = p["Coords"]

        for i in range(len(practices) + 1, 5):
             doc_info[f"Hospital_{i}"] = "-"
             doc_info[f"Address_{i}"] = "-"
             doc_info[f"First_Free_{i}"] = "-"
             doc_info[f"Coords_{i}"] = "-"

        if doc_name == "-" or not doc_name:
            print(f"  [WARN] Не намерих име за {url}. Пропускане.")
            return None

        return doc_info

    except Exception as e:
        print(f"  [ERROR] Проблем при профил {url}: {e}")
        return None

# ============================================================
# ОСНОВЕН ЦИКЪЛ И УПРАВЛЕНИЕ
# ============================================================
def flag_for_continuation():
    with open(CONTINUE_FLAG_FILE, 'w') as f:
        f.write("CONTINUE")

def clear_continuation_flag():
    if os.path.exists(CONTINUE_FLAG_FILE):
        os.remove(CONTINUE_FLAG_FILE)

def main():
    global BASE_SEARCH_URL
    clear_continuation_flag()

    # Динамично подаване на URL през терминала
    if sys.stdin.isatty():
        try:
            custom_url = input(f"[INPUT] Provide target URL or press Enter to keep default ({BASE_SEARCH_URL}): ").strip()
            if custom_url:
                BASE_SEARCH_URL = custom_url
        except EOFError:
            pass

    parsed_base = urlparse(BASE_SEARCH_URL)
    base_domain = f"{parsed_base.scheme}://{parsed_base.netloc}"

    init_driver()

    while True:
        if time_limit_reached():
            print("\n[INFO] Лимитът на времето е достигнат. Активиране на флаг за продължение.")
            flag_for_continuation()
            break

        current_url = f"{BASE_SEARCH_URL}{state['page']}"
        print(f"\n--- Обработка на страница: {state['page']} ---")

        try:
            driver.get(current_url)
            WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.TAG_NAME, "a")))
        except Exception as e:
            print(f"[WARN] Грешка при зареждане на търсачката: {e}")
            state["consecutive_fails"] += 1
            if state["consecutive_fails"] >= MAX_PAGE_RETRIES:
                print("[ERROR] Максимален брой опити. Приемаме край на пагинацията.")
                break
            restart_driver()
            continue
        
        state["consecutive_fails"] = 0

        all_links = driver.find_elements(By.TAG_NAME, "a")
        doctor_urls = []
        for link in all_links:
            href = link.get_attribute("href")
            if href and ("/lekar/" in href or "/practices/lekar/" in href) and "search" not in href:
                if href.startswith("/"):
                    href = f"{base_domain}{href}"
                doctor_urls.append(href)

        doctor_urls = list(set(doctor_urls))

        if not doctor_urls:
            print("[INFO] Няма намерени линкове на страницата. Край на обхождането.")
            break

        time_limit_hit_in_profiles = False

        for doc_url in doctor_urls:
            if time_limit_reached():
                print("[INFO] Лимитът на времето е достигнат по време на екстракция.")
                flag_for_continuation()
                time_limit_hit_in_profiles = True
                break

            if unquote(doc_url) in parsed_urls or doc_url in parsed_urls:
                continue

            details = scrape_doctor_profile_myhealth(doc_url)
            if details:
                with open(csv_file_path, mode="a", encoding="utf-8-sig", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writerow(details)
                
                mark_as_parsed(doc_url)
                print(f"  [+] Записан: {details['Име']} | {unquote(doc_url)}")

        if time_limit_hit_in_profiles:
            break

        state["page"] += 1
        save_state()

    close_driver()
    print("\n[INFO] Сесията приключи успешно.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        close_driver()
        print("\n[INFO] Прекъснато от потребител.")
