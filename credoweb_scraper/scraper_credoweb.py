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
# КОНФИГУРАЦИЯ И ЦЕЛИ (TARGETS)
# ============================================================
START_TIME = time.time()
TIME_LIMIT_SECONDS = 5.4 * 60 * 60  # ~5 часа и 24 минути

BASE_DOMAIN = "https://www.credoweb.bg"

MAX_PAGE_RETRIES = 3
RETRY_DELAY_SECONDS = 2

TARGETS = [
    {
        "cat": "101",
        "name": "doctors",
        "url": "https://www.credoweb.bg/search?cat=101",
        "csv": "credoweb_doctors_full.csv",
        "fields": [
            "Name", "Profession_Tags", "Specialty", "Other_Specialties", 
            "Address", "Workplace", "Education", "Organizations",
            "Phone", "Email", "Followers", "Following", "CV_Bio", "Source_URL"
        ]
    },
    {
        "cat": "103",
        "name": "hospitals",
        "url": "https://www.credoweb.bg/search?cat=103",
        "csv": "credoweb_hospitals_full.csv",
        "fields": [
            "Name", "Type", "Address", "Website", "Phone", "Email", 
            "Followers", "Following", "About", "Structures", "Team", "Source_URL"
        ]
    }
]

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
CONTINUE_FLAG_FILE = os.path.join(output_dir, "CONTINUE_FLAG_CREDOWEB")

# Инициализиране на CSV файловете, ако не съществуват
for t in TARGETS:
    csv_path = os.path.join(output_dir, t["csv"])
    if not os.path.exists(csv_path):
        with open(csv_path, mode="w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=t["fields"])
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
    "101": {"page": 1, "finished": False, "previous_first_doc": None},
    "103": {"page": 1, "finished": False, "previous_first_doc": None},
    "consecutive_fails": 0
}

if os.path.exists(state_file):
    try:
        with open(state_file, "r", encoding="utf-8") as f:
            loaded_state = json.load(f)
            # Миграция от стария формат към новия
            if "101" not in loaded_state:
                state["101"]["page"] = loaded_state.get("page", 1)
                state["101"]["finished"] = loaded_state.get("finished", False)
                state["101"]["previous_first_doc"] = loaded_state.get("previous_first_doc", None)
                state["consecutive_fails"] = loaded_state.get("consecutive_fails", 0)
                print(f"[INFO] Успешна миграция на state файла към multi-target формат.")
            else:
                state.update(loaded_state)
        print(f"[INFO] Заредено състояние. Лекари стр: {state['101']['page']}, Болници стр: {state['103']['page']}.")
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
def add_failed_profile(url, cat, page, error_msg=""):
    profiles = []
    if os.path.exists(failed_profiles_file):
        try:
            with open(failed_profiles_file, "r", encoding="utf-8") as f:
                profiles = json.load(f)
        except json.JSONDecodeError:
            pass
    profiles.append({
        "URL": unquote(url),
        "Category": cat,
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

def decline_cookies():
    try:
        driver_page.locator("cookie-policy-popup button:has-text('Приемам')").first.click(timeout=3000)
    except PlaywrightTimeoutError:
        pass

# ============================================================
# ЕКСТРАКЦИЯ СПОРЕД КАТЕГОРИЯТА
# ============================================================
def extract_doctor(url):
    decoded_url = unquote(url)
    details = {
        "Name": "", "Profession_Tags": "", "Specialty": "", "Other_Specialties": "", 
        "Address": "", "Workplace": "", "Education": "", "Organizations": "",
        "Phone": "", "Email": "", "Followers": "0", "Following": "0", 
        "CV_Bio": "", "Source_URL": decoded_url
    }

    try:
        name_loc = driver_page.locator(".personal-information h1 span").first
        if name_loc.count() > 0: details["Name"] = name_loc.inner_text().strip()
            
        prof_tags = driver_page.locator(".personal-information cw-tags a span").all_inner_texts()
        if prof_tags: details["Profession_Tags"] = ", ".join([t.strip() for t in prof_tags if t.strip()])

        spec_loc = driver_page.locator(".personal-information .fs-16.fw-semibold").first
        if spec_loc.count() > 0: details["Specialty"] = spec_loc.inner_text().strip()

        other_spec_loc = driver_page.locator(".personal-information .other-specialties").first
        if other_spec_loc.count() > 0: details["Other_Specialties"] = other_spec_loc.inner_text().strip()

        addr_loc = driver_page.locator(".personal-information .fs-14.fw-normal.text-gray-500").first
        if addr_loc.count() > 0: details["Address"] = addr_loc.inner_text().strip()

        social_stats = driver_page.locator(".social-information .bs-btn").all()
        for stat in social_stats:
            num = stat.locator(".number").inner_text().strip() if stat.locator(".number").count() > 0 else "0"
            txt = stat.locator(".text").inner_text().strip().lower() if stat.locator(".text").count() > 0 else ""
            if "последователи" in txt: details["Followers"] = num
            elif "следвани" in txt: details["Following"] = num

        workplaces = driver_page.locator("experience .link").all_inner_texts()
        if workplaces: details["Workplace"] = " | ".join([w.strip() for w in workplaces if w.strip()])

        education = driver_page.locator("education .fs-18.fw-bold").all_inner_texts()
        if education: details["Education"] = " | ".join([e.strip() for e in education if e.strip()])
            
        organizations = driver_page.locator("organisation .fs-16.fw-semibold").all_inner_texts()
        if organizations: details["Organizations"] = " | ".join([o.strip().replace('\n', ' ') for o in organizations if o.strip()])

        email_loc = driver_page.locator("contacts a[href^='mailto:']").first
        if email_loc.count() > 0: details["Email"] = email_loc.inner_text().strip()

        phone_loc = driver_page.locator("contacts a[href^='tel:']").first
        if phone_loc.count() > 0: details["Phone"] = phone_loc.inner_text().strip()

        cv_loc = driver_page.locator("cv-description").first
        if cv_loc.count() > 0: details["CV_Bio"] = cv_loc.inner_text().strip().replace('\n', '  ')

    except Exception as e:
        print(f"[ERROR] Грешка при парсване на данните (Лекар) за {url}: {e}")

    return details

def extract_hospital(url):
    decoded_url = unquote(url)
    details = {
        "Name": "", "Type": "", "Address": "", "Website": "", "Phone": "", "Email": "", 
        "Followers": "0", "Following": "0", "About": "", "Structures": "", "Team": "", "Source_URL": decoded_url
    }

    try:
        name_loc = driver_page.locator(".personal-information h1").first
        if name_loc.count() > 0: details["Name"] = name_loc.inner_text().strip()
        
        type_loc = driver_page.locator(".personal-information .fs-18.text-gray-500").first
        if type_loc.count() > 0: details["Type"] = type_loc.inner_text().strip()

        addr_loc = driver_page.locator(".personal-information .address").first
        if addr_loc.count() > 0:
            details["Address"] = addr_loc.inner_text().strip()
        else:
            addr_alt = driver_page.locator("page-contacts .contact-address .text-dark-blue-500").first
            if addr_alt.count() > 0: details["Address"] = addr_alt.inner_text().strip()

        web_loc = driver_page.locator(".personal-information a.website, .personal-information a[target='_blank']").first
        if web_loc.count() > 0: details["Website"] = web_loc.inner_text().strip()

        phone_loc = driver_page.locator("contacts a[href^='tel:']").first
        if phone_loc.count() > 0: details["Phone"] = phone_loc.inner_text().strip()

        email_loc = driver_page.locator("contacts a[href^='mailto:']").first
        if email_loc.count() > 0: details["Email"] = email_loc.inner_text().strip()

        social_stats = driver_page.locator(".social-information .bs-btn").all()
        for stat in social_stats:
            num = stat.locator(".number").inner_text().strip() if stat.locator(".number").count() > 0 else "0"
            txt = stat.locator(".text").inner_text().strip().lower() if stat.locator(".text").count() > 0 else ""
            if "последователи" in txt: details["Followers"] = num
            elif "следвани" in txt: details["Following"] = num

        about_loc = driver_page.locator("cv-description").first
        if about_loc.count() > 0: details["About"] = about_loc.inner_text().strip().replace('\n', '  ')

        structures = driver_page.locator("profile-structures a.bg-light-blue span").all_inner_texts()
        if structures: details["Structures"] = " | ".join([s.strip() for s in structures if s.strip()])

        team = driver_page.locator("team .team-member h3").all_inner_texts()
        if team: details["Team"] = " | ".join([t.strip() for t in team if t.strip()])

    except Exception as e:
        print(f"[ERROR] Грешка при парсване на данните (Болница) за {url}: {e}")

    return details

# ============================================================
# ОСНОВЕН СКРЕЙПЪР КОНТРОЛЕР
# ============================================================
def flag_for_continuation():
    with open(CONTINUE_FLAG_FILE, 'w') as f:
        f.write("CONTINUE")

def check_and_clear_continuation_flag():
    if os.path.exists(CONTINUE_FLAG_FILE):
        os.remove(CONTINUE_FLAG_FILE)
        return True
    return False

def main():
    global driver_page
    is_continuation = check_and_clear_continuation_flag()

    # Инициализация при ново стартиране (спрямо график)
    if not is_continuation:
        print("[INFO] Ново стартиране (не е продължение). Рестартиране на пагинацията за всички цели.")
        for t in TARGETS:
            cat = t["cat"]
            state[cat]["page"] = 1
            state[cat]["finished"] = False
            state[cat]["previous_first_doc"] = None
        state["consecutive_fails"] = 0
        save_state()

    all_finished = all(state[t["cat"]]["finished"] for t in TARGETS)
    if all_finished:
        print("[INFO] Скрейпингът на всички категории е вече маркиран като завършен.")
        return

    for target in TARGETS:
        cat = target["cat"]
        cat_name = target["name"]
        cat_url = target["url"]
        csv_file = os.path.join(output_dir, target["csv"])

        if state[cat]["finished"]:
            print(f"[INFO] Цел [{cat_name}] е вече завършена. Преминаване към следващата...")
            continue

        print(f"\n=======================================================")
        print(f" ЗАПОЧВАНЕ ОБХОЖДАНЕ НА ЦЕЛ: {cat_name.upper()} (CAT: {cat})")
        print(f"=======================================================")

        while not state[cat]["finished"]:
            if time_limit_reached():
                print("\n[INFO] Лимитът на времето е достигнат. Флагът за продължение е активиран.")
                flag_for_continuation()
                return

            page = state[cat]["page"]
            print(f"\n--- [{cat_name.upper()}] Обработка на Страница: {page} ---")

            if page == 1:
                current_url = cat_url
            else:
                current_url = f"{cat_url}&page={page - 1}"

            try:
                driver_page.goto(current_url, wait_until="domcontentloaded")
                decline_cookies()
                driver_page.wait_for_selector(".search-result", timeout=15000)
            except Exception as e:
                print(f"[WARN] Грешка при зареждане на страница {page} ({cat_name}): {e}")
                state["consecutive_fails"] += 1
                if state["consecutive_fails"] >= MAX_PAGE_RETRIES:
                    print(f"[ERROR] Достигнат лимит за грешки на стр. {page}. Край на обхождането за тази цел.")
                    state[cat]["finished"] = True
                save_state()
                driver_page = restart_driver()
                continue

            state["consecutive_fails"] = 0

            # Извличане на линковете
            doc_links = driver_page.locator(".search-result a.search-list-title").all()
            profile_urls = []
            for el in doc_links:
                href = el.get_attribute("href")
                if href:
                    full_url = urljoin(BASE_DOMAIN, href)
                    profile_urls.append(full_url)

            if not profile_urls:
                print(f"[INFO] Няма намерени профили. Край на пагинацията за {cat_name}.")
                state[cat]["finished"] = True
                save_state()
                break

            if state[cat]["previous_first_doc"] == profile_urls[0]:
                print(f"[WARN] Засечено повторение на резултатите (край на пагинацията) за {cat_name}.")
                state[cat]["finished"] = True
                save_state()
                break

            state[cat]["previous_first_doc"] = profile_urls[0]
            time_limit_hit_in_profiles = False

            for url in profile_urls:
                if time_limit_reached():
                    print("[INFO] Лимитът на времето е достигнат по време на обхождане на профили.")
                    flag_for_continuation()
                    time_limit_hit_in_profiles = True
                    break

                if unquote(url) in parsed_urls or url in parsed_urls:
                    continue

                try:
                    driver_page.goto(url, wait_until="domcontentloaded")
                    driver_page.wait_for_selector(".personal-information", timeout=15000)
                except Exception as e:
                    print(f"[ERROR] Грешка при зареждане на детайли за {url}: {e}")
                    add_failed_profile(url, cat, page, "Мрежова/DOM грешка при отваряне")
                    continue

                details = extract_doctor(url) if cat == "101" else extract_hospital(url)

                if details and details.get("Name"):
                    with open(csv_file, mode="a", encoding="utf-8-sig", newline="") as f:
                        writer = csv.DictWriter(f, fieldnames=target["fields"])
                        writer.writerow(details)
                    
                    mark_as_parsed(url)
                    print(f"  [+] Записан: {details['Name']} | {unquote(url)}")
                else:
                    add_failed_profile(url, cat, page, "Неуспешно извличане на структурата")

            if time_limit_hit_in_profiles:
                return

            state[cat]["page"] += 1
            save_state()

    close_driver()
    print("\n[INFO] Обхождането на всички профили и категории приключи успешно!")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        close_driver()
        print("\n[INFO] Прекъснато от потребител.")
