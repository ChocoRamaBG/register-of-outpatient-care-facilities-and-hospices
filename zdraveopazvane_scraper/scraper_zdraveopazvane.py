import os
import time
import json
import csv
import re
from urllib.parse import unquote
from datetime import datetime

from playwright.sync_api import (
    sync_playwright,
    TimeoutError as PlaywrightTimeoutError
)

# ============================================================
# КОНФИГУРАЦИЯ
# ============================================================
START_TIME = time.time()
TIME_LIMIT_SECONDS = 5.4 * 60 * 60  # ~5 часа и 24 минути

BASE_URL = ""
while not BASE_URL:
    BASE_URL = input("Моля, въведете базовия URL адрес (напр. https://.../?page=): ").strip()

MAX_PAGE_RETRIES = 3
RETRY_DELAY_SECONDS = 2

# ============================================================
# ПЪТИЩА И ДИРЕКТОРИИ
# ============================================================
try:
    output_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    output_dir = os.getcwd()

output_dir = os.path.join(output_dir, "zdraveopazvane_outputs")
os.makedirs(output_dir, exist_ok=True)

state_file = os.path.join(output_dir, "savegame_zdraveopazvane.json")
memory_file = os.path.join(output_dir, "parsed_urls_zdraveopazvane.txt")
failed_profiles_file = os.path.join(output_dir, "failed_profiles_zdraveopazvane.json")
csv_file_path = os.path.join(output_dir, "zdraveopazvane_doctors_full.csv")
CONTINUE_FLAG_FILE = os.path.join(output_dir, "CONTINUE_FLAG_ZDRAVEOPAZVANE")

# ============================================================
# СХЕМА ЗА ЗАПИС НА ДАННИ (CSV)
# ============================================================
fieldnames = [
    "Име", "Адрес", "Телефон", "Имейл", "Отрасъл", 
    "Дейност", "Ключови думи", "Latitude", "Longitude", 
    "Описание", "Линк"
]

if not os.path.exists(csv_file_path):
    with open(csv_file_path, mode="w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

# ============================================================
# УПРАВЛЕНИЕ НА ВРЕМЕТО
# ============================================================
def time_limit_reached():
    return (time.time() - START_TIME) >= TIME_LIMIT_SECONDS

# ============================================================
# УПРАВЛЕНИЕ НА СЪСТОЯНИЕТО (STATE)
# ============================================================
state = {
    "page": 1,
    "consecutive_fails": 0,
    "previous_first_doc": None
}

if os.path.exists(state_file):
    try:
        with open(state_file, "r", encoding="utf-8") as f:
            loaded_state = json.load(f)
            state.update(loaded_state)
        print(f"[INFO] Възстановяване на сесията: Страница {state['page']}.")
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
# ЛОГОВЕ ЗА ГРЕШКИ
# ============================================================
def add_failed_profile(url, page, error_msg=""):
    profiles = []
    if os.path.exists(failed_profiles_file):
        try:
            with open(failed_profiles_file, "r", encoding="utf-8") as f:
                profiles = json.load(f)
        except json.JSONDecodeError:
            pass
    profiles.append({
        "URL": unquote(url),
        "Page": page,
        "Error": str(error_msg),
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })
    with open(failed_profiles_file, "w", encoding="utf-8") as f:
        json.dump(profiles, f, ensure_ascii=False, indent=2)

def retry_failed_profiles():
    if not os.path.exists(failed_profiles_file):
        return False
        
    try:
        with open(failed_profiles_file, "r", encoding="utf-8") as f:
            failed_profiles = json.load(f)
    except Exception:
        return False

    if not failed_profiles:
        return False

    print(f"\n[INFO] Опит за възстановяване на {len(failed_profiles)} неуспешни профила...")
    still_failed = []
    time_limit_hit = False

    for profile in failed_profiles:
        if time_limit_hit or time_limit_reached():
            if not time_limit_hit:
                print("[INFO] Лимитът на времето е достигнат по време на възстановяването на профили.")
                flag_for_continuation()
                time_limit_hit = True
            still_failed.append(profile)
            continue
            
        url = profile["URL"]
        if unquote(url) in parsed_urls or url in parsed_urls:
            continue

        details = extract_doctor_details(url)
        if details:
            with open(csv_file_path, mode="a", encoding="utf-8-sig", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writerow(details)
            
            mark_as_parsed(url)
            print(f"  [+] Възстановен и записан: {details['Име']} | Lat: {details['Latitude']}, Lon: {details['Longitude']} | {unquote(url)}")
        else:
            profile["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            still_failed.append(profile)

    with open(failed_profiles_file, "w", encoding="utf-8") as f:
        json.dump(still_failed, f, ensure_ascii=False, indent=2)

    return time_limit_hit

# ============================================================
# PLAYWRIGHT ИНСТАНЦИЯ
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
    # Блокиране на изображения и стилове за по-бързо зареждане
    _context.route("**/*.{png,jpg,jpeg,webp,svg,css,woff,woff2}", lambda route: route.abort())
    
    _page = _context.new_page()
    _page.set_default_navigation_timeout(30000)
    _page.set_default_timeout(30000)
    return _page

def restart_driver():
    global _page, _context, _browser
    print("[INFO] Рестартиране на браузъра...")
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
# ЕКСТРАКЦИЯ НА ПРОФИЛ
# ============================================================
def extract_doctor_details(url):
    try:
        driver_page.goto(url, wait_until="domcontentloaded")
    except Exception as e:
        print(f"[ERROR] Грешка при зареждане на {url}: {e}")
        return None

    decoded_url = unquote(url)
    details = {
        "Име": "", "Адрес": "", "Телефон": "", "Имейл": "", 
        "Отрасъл": "", "Дейност": "", "Ключови думи": "", 
        "Latitude": "", "Longitude": "", "Описание": "", "Линк": decoded_url
    }

    # Име
    try:
        name_loc = driver_page.locator(".bgr1_title h1").first
        if name_loc.count() > 0:
            details["Име"] = name_loc.inner_text().strip()
    except: pass

    # Координати
    try:
        map_loc = driver_page.locator(".partitions_map_wrap iframe").first
        if map_loc.count() > 0:
            src = map_loc.get_attribute("src")
            if src:
                match = re.search(r'q=([-+]?\d+\.\d+)[^\d]+([-+]?\d+\.\d+)', src)
                if match:
                    details["Latitude"] = match.group(1)
                    details["Longitude"] = match.group(2)
    except Exception as e:
        print(f"[ERROR] Грешка при извличане на координати за {url}: {e}")

    # Описание
    try:
        desc_loc = driver_page.locator(".txt_about_us").first
        if desc_loc.count() > 0:
            details["Описание"] = desc_loc.inner_text().strip().replace('\n', ' ')
    except: 
        pass

    # Детайлни полета
    try:
        rows = driver_page.locator(".w100").all()
        for row in rows:
            const_loc = row.locator(".partitions_const").first
            val_loc = row.locator(".partitions_value").first
            
            if const_loc.count() > 0 and val_loc.count() > 0:
                c_text = const_loc.inner_text().strip()
                v_text = val_loc.inner_text().strip()
                
                if "Адрес:" in c_text:
                    details["Адрес"] = v_text
                elif "Телефони:" in c_text:
                    raw_p = v_text.replace(" ", "")
                    if raw_p: 
                        # Форматираме като текстова формула, за да форсираме Excel да запази нулата
                        details["Телефон"] = f'="{raw_p}"'
                elif "E-mail:" in c_text:
                    details["Имейл"] = v_text
                elif "Отрасъл:" in c_text:
                    details["Отрасъл"] = v_text.replace('\n', ' ').replace('\t', '')
                elif "Дейност:" in c_text:
                    details["Дейност"] = v_text
                elif "Ключови думи:" in c_text:
                    clean_kw = v_text.replace('[виж още]', '').replace('[скрий]', '').replace('…', '').strip()
                    details["Ключови думи"] = clean_kw
    except Exception as e:
        print(f"[ERROR] Грешка при парсване на полетата за {url}: {e}")

    return details

# ============================================================
# ОСНОВНА ЛОГИКА
# ============================================================
def flag_for_continuation():
    with open(CONTINUE_FLAG_FILE, 'w') as f:
        f.write("CONTINUE")

def clear_continuation_flag():
    if os.path.exists(CONTINUE_FLAG_FILE):
        os.remove(CONTINUE_FLAG_FILE)

def main():
    global driver_page
    clear_continuation_flag()

    # Първо опит за извличане на предишни неуспешни профили
    if retry_failed_profiles():
        close_driver()
        return

    while True:
        if time_limit_reached():
            print("\n[INFO] Лимитът на времето е достигнат. Флагът за продължение е активиран.")
            flag_for_continuation()
            break

        page = state["page"]
        current_url = f"{BASE_URL}{page}"
        print(f"\n--- Обработка на страница: {page} ---")

        try:
            driver_page.goto(current_url, wait_until="domcontentloaded")
        except Exception as e:
            print(f"[WARN] Грешка при зареждане на страница {page}: {e}")
            state["consecutive_fails"] += 1
            if state["consecutive_fails"] >= MAX_PAGE_RETRIES:
                print(f"[ERROR] Страница {page} не се зарежда. Край на обхождането.")
                break
            save_state()
            driver_page = restart_driver()
            continue

        state["consecutive_fails"] = 0

        doc_links = driver_page.locator(".box_firm_wrap a.title_firm").all()
        doctor_urls = []
        for el in doc_links:
            href = el.get_attribute("href")
            if href:
                # Гарантиране на абсолютен URL адрес
                if href.startswith("/"):
                    href = f"https://www.zdraveopazvaneto.bg{href}"
                doctor_urls.append(href)

        if not doctor_urls:
            print("[INFO] Няма повече профили на тази страница. Край на обхождането.")
            break

        if state["previous_first_doc"] == doctor_urls[0]:
            print("[WARN] Засечено повторение на резултатите. Край на пагинацията.")
            break

        state["previous_first_doc"] = doctor_urls[0]
        time_limit_hit_in_profiles = False

        for doc_url in doctor_urls:
            if time_limit_reached():
                print("[INFO] Лимитът на времето е достигнат по време на обхождане на профили.")
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
                print(f"  [+] Записан: {details['Име']} | Lat: {details['Latitude']}, Lon: {details['Longitude']} | {unquote(doc_url)}")
            else:
                add_failed_profile(doc_url, page, "Неуспешно извличане")

        if time_limit_hit_in_profiles:
            break

        state["page"] += 1
        save_state()

    close_driver()
    print("\n[INFO] Операцията приключи.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        close_driver()
        print("\n[INFO] Прекъснато от потребител.")
