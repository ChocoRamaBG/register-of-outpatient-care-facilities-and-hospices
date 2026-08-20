import os
import time
import json
import csv
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

# Забележка: В оригиналния ви GitHub скрипт URL адресът беше за "здравни заведения".
# За да съвпада с данните за лекари, го промених на съответния линк от работещия скрипт.
BASE_URL = "https://spravochnik.framar.bg/%D0%BC%D0%B5%D0%B4%D0%B8%D1%86%D0%B8%D0%BD%D1%81%D0%BA%D0%B8-%D1%81%D0%BF%D0%B5%D1%86%D0%B8%D0%B0%D0%BB%D0%B8%D1%81%D1%82%D0%B8"

MAX_PAGE_RETRIES = 3
RETRY_DELAY_SECONDS = 2

# ============================================================
# ПЪТИЩА И ДИРЕКТОРИИ
# ============================================================
try:
    output_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    output_dir = os.getcwd()

# Добавена е подпапка за запазване на чистотата в основната директория
output_dir = os.path.join(output_dir, "framar_outputs")
os.makedirs(output_dir, exist_ok=True)

state_file = os.path.join(output_dir, "savegame_framar.json")
memory_file = os.path.join(output_dir, "parsed_urls_framar.txt")
failed_pages_file = os.path.join(output_dir, "failed_pages_framar.json")
failed_profiles_file = os.path.join(output_dir, "failed_profiles_framar.json")
csv_file_path = os.path.join(output_dir, "framar_doctors_full_playwright.csv")
CONTINUE_FLAG_FILE = os.path.join(output_dir, "CONTINUE_FLAG_FRAMAR")

# ============================================================
# СХЕМА ЗА ЗАПИС НА ДАННИ (CSV)
# ============================================================
fieldnames = [
    "Name", "Specialty", "Region", "Address", "Phone", "Email", "Website",
    "Dates", "Rating", "Education", "Experience", "Qualifications", 
    "Memberships", "Additional_Info", "Path", "Source_URL"
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
    "region_index": 0,
    "page": 1,
    "regions": [],
    "consecutive_fails": 0,
    "previous_first_doc": None
}

if os.path.exists(state_file):
    try:
        with open(state_file, "r", encoding="utf-8") as f:
            loaded_state = json.load(f)
            state.update(loaded_state)
        print(f"[INFO] Възстановяване на сесията: Регион индекс {state['region_index']}, Страница {state['page']}.")
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
def add_failed_profile(url, region, page, error_msg=""):
    profiles = []
    if os.path.exists(failed_profiles_file):
        with open(failed_profiles_file, "r", encoding="utf-8") as f:
            profiles = json.load(f)
    profiles.append({
        "URL": unquote(url),
        "Region_Index": region,
        "Page": page,
        "Error": str(error_msg),
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })
    with open(failed_profiles_file, "w", encoding="utf-8") as f:
        json.dump(profiles, f, ensure_ascii=False, indent=2)

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
# ПОМОЩНИ ФУНКЦИИ
# ============================================================
def decline_cookies():
    try:
        driver_page.locator("button.cky-btn-reject").first.click(timeout=5000)
    except PlaywrightTimeoutError:
        pass

def scrape_regions():
    print("[INFO] Извличане на региони...")
    driver_page.goto(BASE_URL)
    decline_cookies()
    
    links = driver_page.locator("//a[contains(@href, '-%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82')]").all()
    regions = sorted(list(set([el.get_attribute("href") for el in links if el.get_attribute("href")])))
    
    if not regions:
        raise ValueError("Не са открити региони. Проверете селекторите или базовата страница.")
        
    state["regions"] = regions
    save_state()
    print(f"[INFO] Открити {len(regions)} региона.")

# ============================================================
# ЕКСТРАКЦИЯ НА ПРОФИЛ (БЪРЗ DOM ПАРСИНГ)
# ============================================================
def extract_doctor_details(url):
    try:
        driver_page.goto(url, wait_until="domcontentloaded")
    except Exception as e:
        print(f"[ERROR] Грешка при зареждане на {url}: {e}")
        return None

    decoded_url = unquote(url)
    details = {
        "Name": "N/A", "Specialty": "N/A", "Region": "N/A", "Address": "N/A", 
        "Phone": "N/A", "Email": "N/A", "Website": "N/A", "Dates": "N/A", 
        "Rating": "N/A", "Education": "N/A", "Experience": "N/A",
        "Qualifications": "N/A", "Memberships": "N/A", "Additional_Info": "N/A",
        "Path": "N/A", "Source_URL": decoded_url
    }

    try:
        name_loc = driver_page.locator("h1").first
        if name_loc.count() > 0:
            details["Name"] = name_loc.inner_text().strip()
    except: pass

    try:
        rating_elements = driver_page.locator("span.fl").all_inner_texts()
        for text in rating_elements:
            if "оценки" in text or "/" in text:
                details["Rating"] = text.strip()
                break
    except: pass

    try:
        time_tag = driver_page.locator("time.subheader.last").first
        if time_tag.count() > 0:
            details["Dates"] = time_tag.inner_text().strip()
    except: pass

    try:
        crumbs = driver_page.locator("#breadcrumbs .section").all_inner_texts()
        details["Path"] = " > ".join([c.strip() for c in crumbs if c.strip().lower() != "назад"])
    except: pass

    try:
        info_elements = driver_page.locator("#info p").all_inner_texts()
        for text in info_elements:
            text = text.strip()
            if "Специалист:" in text: details["Specialty"] = text.replace("Специалист:", "").strip()
            elif "Населено място:" in text: details["Region"] = text.replace("Населено място:", "").strip()
            elif "Адрес:" in text: details["Address"] = text.replace("Адрес:", "").strip()
            elif "Телефон:" in text: details["Phone"] = text.replace("Телефон:", "").strip()
            elif "E-mail:" in text: details["Email"] = text.replace("E-mail:", "").strip()
            elif "Сайт:" in text: details["Website"] = text.replace("Сайт:", "").strip()
    except Exception as e:
        print(f"[ERROR] Грешка при парсване на инфо секцията за {url}: {e}")

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

    if not state["regions"]:
        scrape_regions()

    total_regions = len(state["regions"])

    while state["region_index"] < total_regions:
        if time_limit_reached():
            print("\n[INFO] Лимитът на времето е достигнат. Флагът за продължение е активиран.")
            flag_for_continuation()
            break

        current_region_url = state["regions"][state["region_index"]]
        page = state["page"]

        print(f"\n--- Обработка на регион ({state['region_index'] + 1}/{total_regions}): {unquote(current_region_url)} | Страница: {page} ---")

        p_segment = f"/стр-{page}" if page > 1 else ""
        current_url = f"{current_region_url.split('?')[0]}{p_segment}?vars=10000,1,0,0"

        try:
            driver_page.goto(current_url, wait_until="domcontentloaded")
            decline_cookies()
        except Exception as e:
            print(f"[WARN] Грешка при зареждане на страницата на региона: {e}")
            state["consecutive_fails"] += 1
            if state["consecutive_fails"] >= MAX_PAGE_RETRIES:
                print("[ERROR] Достигнат лимит за презареждане. Преминаване към следваща страница.")
                state["page"] += 1
                state["consecutive_fails"] = 0
            save_state()
            driver_page = restart_driver()
            continue

        state["consecutive_fails"] = 0

        doc_links = driver_page.locator("article.item h2.header a").all()
        doctor_urls = [el.get_attribute("href") for el in doc_links if el.get_attribute("href")]

        if not doctor_urls:
            print("[INFO] Няма повече профили в този регион. Преминаване към следващия.")
            state["region_index"] += 1
            state["page"] = 1
            state["previous_first_doc"] = None
            save_state()
            continue

        if state["previous_first_doc"] == doctor_urls[0]:
            print("[WARN] Засечено повторение на резултатите (край на пагинацията). Преминаване към следващ регион.")
            state["region_index"] += 1
            state["page"] = 1
            state["previous_first_doc"] = None
            save_state()
            continue

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
                print(f"  [+] Записан: {details['Name']} | {unquote(doc_url)}")
            else:
                add_failed_profile(doc_url, state["region_index"], page, "Неуспешно извличане")

        if time_limit_hit_in_profiles:
            break

        state["page"] += 1
        save_state()

    close_driver()
    if state["region_index"] >= total_regions:
        print("\n[INFO] Обхождането на всички региони приключи успешно!")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        close_driver()
        print("\n[INFO] Прекъснато от потребител.")
