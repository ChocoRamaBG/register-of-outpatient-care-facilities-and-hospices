import os
import time
import json
import csv
from urllib.parse import unquote, urljoin
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

BASE_URL = "https://www.credoweb.bg/search?cat=101"
BASE_DOMAIN = "https://www.credoweb.bg"

MAX_PAGE_RETRIES = 3
RETRY_DELAY_SECONDS = 2

# ============================================================
# ПЪТИЩА И ДИРЕКТОРИИ
# ============================================================
try:
    output_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    output_dir = os.getcwd()

output_dir = os.path.join(output_dir, "credoweb_outputs")
os.makedirs(output_dir, exist_ok=True)

state_file = os.path.join(output_dir, "savegame_credoweb.json")
memory_file = os.path.join(output_dir, "parsed_urls_credoweb.txt")
failed_pages_file = os.path.join(output_dir, "failed_pages_credoweb.json")
failed_profiles_file = os.path.join(output_dir, "failed_profiles_credoweb.json")
csv_file_path = os.path.join(output_dir, "credoweb_doctors_full.csv")
CONTINUE_FLAG_FILE = os.path.join(output_dir, "CONTINUE_FLAG_CREDOWEB")

# ============================================================
# СХЕМА ЗА ЗАПИС НА ДАННИ (CSV)
# ============================================================
fieldnames = [
    "Name", "Specialty", "Other_Specialties", "Address", "Phone", "Email", 
    "Workplace", "Education", "CV_Bio", "Source_URL"
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
    "previous_first_doc": None,
    "finished": False
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
    
    # Блокиране на изображения и стилове за по-бързо зареждане на Angular
    _context.route("**/*.{png,jpg,jpeg,webp,svg,css,woff,woff2}", lambda route: route.abort())
    
    _page = _context.new_page()
    _page.set_default_navigation_timeout(40000)
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
        driver_page.locator("cookie-policy-popup button:has-text('Приемам')").first.click(timeout=3000)
    except PlaywrightTimeoutError:
        pass

# ============================================================
# ЕКСТРАКЦИЯ НА ПРОФИЛ
# ============================================================
def extract_doctor_details(url):
    try:
        driver_page.goto(url, wait_until="domcontentloaded")
        # Изчакване на основния профилен компонент да се рендерира
        driver_page.wait_for_selector("profile-main-section", timeout=15000)
    except Exception as e:
        print(f"[ERROR] Грешка при зареждане на {url}: {e}")
        return None

    decoded_url = unquote(url)
    
    details = {
        "Name": "", "Specialty": "", "Other_Specialties": "", "Address": "", 
        "Phone": "", "Email": "", "Workplace": "", "Education": "", 
        "CV_Bio": "", "Source_URL": decoded_url
    }

    try:
        # Име
        name_loc = driver_page.locator(".personal-information h1 span").first
        if name_loc.count() > 0:
            details["Name"] = name_loc.inner_text().strip()

        # Основна специалност
        spec_loc = driver_page.locator(".personal-information .fs-16.fw-semibold").first
        if spec_loc.count() > 0:
            details["Specialty"] = spec_loc.inner_text().strip()

        # Други специалности
        other_spec_loc = driver_page.locator(".personal-information .other-specialties").first
        if other_spec_loc.count() > 0:
            details["Other_Specialties"] = other_spec_loc.inner_text().strip()

        # Адрес (обикновено се намира под университета/лечебното заведение)
        addr_loc = driver_page.locator(".personal-information .fs-14.fw-normal.text-gray-500").first
        if addr_loc.count() > 0:
            details["Address"] = addr_loc.inner_text().strip()

        # Месторабота (Работно място)
        workplaces = driver_page.locator("experience .link").all_inner_texts()
        if workplaces:
            details["Workplace"] = " | ".join([w.strip() for w in workplaces if w.strip()])

        # Образование
        education = driver_page.locator("education .fs-16").all_inner_texts()
        if education:
            details["Education"] = " | ".join([e.strip() for e in education if e.strip()])

        # Имейл
        email_loc = driver_page.locator("a[href^='mailto:']").first
        if email_loc.count() > 0:
            details["Email"] = email_loc.inner_text().strip()

        # Телефон
        phone_loc = driver_page.locator("a[href^='tel:']").first
        if phone_loc.count() > 0:
            details["Phone"] = phone_loc.inner_text().strip()

        # CV / Биография
        cv_loc = driver_page.locator("cv-description").first
        if cv_loc.count() > 0:
            details["CV_Bio"] = cv_loc.inner_text().strip().replace('\n', '  ')

    except Exception as e:
        print(f"[ERROR] Грешка при парсване на данните за {url}: {e}")

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

    if state.get("finished", False):
        print("[INFO] Скрейпингът е вече маркиран като завършен.")
        return

    while not state["finished"]:
        if time_limit_reached():
            print("\n[INFO] Лимитът на времето е достигнат. Флагът за продължение е активиран.")
            flag_for_continuation()
            break

        page = state["page"]
        print(f"\n--- Обработка на Страница: {page} ---")

        current_url = f"{BASE_URL}&page={page}"

        try:
            driver_page.goto(current_url, wait_until="domcontentloaded")
            decline_cookies()
            # Изчакване на резултатите да се заредят
            driver_page.wait_for_selector(".search-result", timeout=15000)
        except Exception as e:
            print(f"[WARN] Грешка при зареждане на страница {page}: {e}")
            state["consecutive_fails"] += 1
            if state["consecutive_fails"] >= MAX_PAGE_RETRIES:
                print(f"[ERROR] Достигнат лимит за грешки на стр. {page}. Маркиране като завършен.")
                state["finished"] = True
            save_state()
            driver_page = restart_driver()
            continue

        state["consecutive_fails"] = 0

        # Извличане на линкове към профилите
        doc_links = driver_page.locator(".search-result a.search-list-title").all()
        doctor_urls = []
        for el in doc_links:
            href = el.get_attribute("href")
            if href:
                full_url = urljoin(BASE_DOMAIN, href)
                doctor_urls.append(full_url)

        if not doctor_urls:
            print("[INFO] Няма повече профили намерени на тази страница. Край на пагинацията.")
            state["finished"] = True
            save_state()
            break

        # Проверка за повтаряща се пагинация (защитен механизъм)
        if state["previous_first_doc"] == doctor_urls[0]:
            print("[WARN] Засечено повторение на резултатите (вероятно край на пагинацията).")
            state["finished"] = True
            save_state()
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
                print(f"  [+] Записан: {details['Name']} | {unquote(doc_url)}")
            else:
                add_failed_profile(doc_url, page, "Неуспешно извличане")

        if time_limit_hit_in_profiles:
            break

        state["page"] += 1
        save_state()

    close_driver()
    if state.get("finished", False):
        print("\n[INFO] Обхождането на всички профили приключи успешно!")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        close_driver()
        print("\n[INFO] Прекъснато от потребител.")
