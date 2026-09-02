import csv
import json
import os
import re
import sys
import time
from datetime import datetime
from urllib.parse import unquote, urljoin, urlparse

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

START_TIME = time.time()
TIME_LIMIT_SECONDS = float(os.getenv('TIME_LIMIT_SECONDS', str(5.4 * 60 * 60)))
DEFAULT_START_PAGE = int(os.getenv('START_PAGE', '1'))
MAX_PAGE_RETRIES = 5
MAX_PROFILE_RETRIES = 3
PAGE_WAIT_SECONDS = 45
PROFILE_WAIT_SECONDS = 15

BASE_SEARCH_URL = ""

try:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    SCRIPT_DIR = os.getcwd()

OUTPUT_DIR = os.path.join(SCRIPT_DIR, 'myhealth_outputs')
os.makedirs(OUTPUT_DIR, exist_ok=True)
STATE_FILE = os.path.join(OUTPUT_DIR, 'savegame_myhealth.json')
MEMORY_FILE = os.path.join(OUTPUT_DIR, 'parsed_urls_myhealth.txt')
CSV_FILE = os.path.join(OUTPUT_DIR, 'myhealth_doctors_full.csv')
CONTINUE_FLAG_FILE = os.path.join(OUTPUT_DIR, 'CONTINUE_FLAG_MYHEALTH')

FIELDNAMES = [
    'Име', 'Специалност', 'Рейтинг_Инфо', 'Първи свободен час (Общо)',
    'Телефони', 'НЗОК', 'Биография', 'URL', 'Timestamp', 'Цени', 'Застрахователи'
]
# Корекция: Увеличен обхват до 6, за да покрива индекси от 1 до 5 (включително)
for i in range(1, 6):
    FIELDNAMES += [f'Hospital_{i}', f'Address_{i}', f'First_Free_{i}', f'Coords_{i}']

state = {'page': DEFAULT_START_PAGE, 'consecutive_fails': 0}
parsed_urls = set()
driver = None


def save_state():
    tmp = STATE_FILE + '.tmp'
    try:
        state['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        print(f'[WARN] Could not save state: {e}')


def load_state():
    if not os.path.exists(STATE_FILE):
        print(f"[INFO] No state file. Starting from page {state['page']}.")
        return
    try:
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            loaded = json.load(f)
        state.update(loaded)
        state['page'] = max(1, int(state.get('page', DEFAULT_START_PAGE)))
        print(f"[INFO] Resuming from page {state['page']}.")
    except Exception as e:
        print(f'[WARN] Could not load state: {e}')


def load_memory():
    if not os.path.exists(MEMORY_FILE):
        print('[INFO] URL memory does not exist yet.')
        return
    try:
        with open(MEMORY_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                u = line.strip()
                if u:
                    parsed_urls.add(u)
                    parsed_urls.add(unquote(u))
        print(f'[INFO] Loaded {len(parsed_urls)} URL memory entries.')
    except Exception as e:
        print(f'[WARN] Could not load URL memory: {e}')


def canonicalize_url(url):
    if not url:
        return ''
    url = url.strip()
    if url.startswith('/'):
        url = urljoin('https://myhealth.bg', url)
    return urlparse(url)._replace(fragment='').geturl().rstrip('/')


def is_parsed(url):
    u = canonicalize_url(url)
    return u in parsed_urls or unquote(u) in parsed_urls


def mark_as_parsed(url):
    u = canonicalize_url(url)
    if not u:
        return
    parsed_urls.add(u)
    parsed_urls.add(unquote(u))
    with open(MEMORY_FILE, 'a', encoding='utf-8') as f:
        f.write(u + '\n')


def time_limit_reached():
    return time.time() - START_TIME >= TIME_LIMIT_SECONDS


def init_csv():
    if os.path.exists(CSV_FILE) and os.path.getsize(CSV_FILE) > 0:
        return
    with open(CSV_FILE, 'w', encoding='utf-8-sig', newline='') as f:
        csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction='ignore').writeheader()


def append_csv(row):
    with open(CSV_FILE, 'a', encoding='utf-8-sig', newline='') as f:
        csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction='ignore').writerow(row)
        f.flush()
        os.fsync(f.fileno())


def init_driver():
    global driver
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    
    # Корекция: Специфични опции за по-добра стабилност в GitHub Actions (Xvfb)
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--log-level=3')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(30)
        print('[INFO] Chrome driver started successfully with webdriver-manager.')
    except Exception as e:
        print(f'[ERROR] Failed to start Chrome driver: {e}')
        raise e


def restart_driver():
    global driver
    print('[INFO] Restarting browser...')
    try:
        if driver:
            driver.quit()
    except Exception:
        pass
    driver = None
    time.sleep(3)
    init_driver()


def close_driver():
    global driver
    try:
        if driver:
            driver.quit()
    except Exception:
        pass
    driver = None


def body_text():
    try:
        return driver.find_element(By.TAG_NAME, 'body').text.lower()
    except Exception:
        return ''


def blocked_page():
    text = body_text()
    markers = ('too many requests', 'access denied', 'cloudflare', 'just a moment', 'temporarily unavailable')
    return any(m in text for m in markers)


def get_text_safe(xpath, context=None, default='-'):
    try:
        el = (context or driver).find_element(By.XPATH, xpath)
        return el.text.strip().replace('\n', ' ')
    except Exception:
        return default


def scrape_insurances_myhealth():
    try:
        imgs = driver.find_elements(By.XPATH, "//div[contains(@class, 'practice__insurance-logos')]//img")
        vals = []
        for img in imgs:
            alt = (img.get_attribute('alt') or '').strip()
            if alt and alt not in vals:
                vals.append(alt)
        return ', '.join(vals) if vals else '-'
    except Exception:
        return '-'


def scrape_prices_myhealth():
    try:
        items = driver.find_elements(By.XPATH, "//div[contains(@class, 'practice__pricing-text--item')]")
        vals = []
        for item in items:
            try:
                name = item.find_element(By.XPATH, ".//p[contains(@class, 'dummy--reason__name')]").text.strip()
                price = item.find_element(By.XPATH, ".//p[contains(@class, 'dummy--reason__price')]").text.strip()
                vals.append(f'{name}: {price}')
            except Exception:
                pass
        return ' | '.join(vals) if vals else '-'
    except Exception:
        return '-'


def get_coordinates_from_map_link(context=None):
    try:
        ctx = context or driver
        links = ctx.find_elements(By.XPATH, ".//a[contains(@href, 'google.com/maps') and contains(@href, 'daddr')]")
        for link in links:
            href = link.get_attribute('href') or ''
            m = re.search(r'(?:[?&])daddr=([+-]?\d+(?:\.\d+)?),([+-]?\d+(?:\.\d+)?)', href)
            if m:
                return f'{m.group(1)}, {m.group(2)}'
        return '-'
    except Exception:
        return '-'


def get_full_biography():
    try:
        hidden = driver.find_elements(By.ID, 'hidden-profile-resume')
        if hidden:
            text = driver.execute_script("return arguments[0].textContent || '';", hidden[0]).strip()
            if text:
                return text
        try:
            btn = driver.find_element(By.CSS_SELECTOR, "button[data-hidden-text-id='profile-resume']")
            if btn.is_displayed():
                driver.execute_script('arguments[0].click();', btn)
                time.sleep(0.5)
        except Exception:
            pass
        return driver.find_element(By.ID, 'profile-resume').text.strip()
    except Exception:
        return '-'


def scrape_practices_detailed():
    results = []
    dates_map = {}
    try:
        try:
            box = driver.find_element(By.CLASS_NAME, 'dummy--detailed-profile-card__practices')
            titles = box.find_elements(By.CLASS_NAME, 'dummy--detailed-profile-card__practices-title')
            dates = box.find_elements(By.CLASS_NAME, 'dummy--detailed-profile-card__practices-fa')
            if len(titles) == len(dates):
                for title, date_el in zip(titles, dates):
                    key = re.sub(r'\s+', '', title.text.strip().lower())
                    raw = date_el.get_attribute('data-date')
                    value = raw.replace('T', ' ').split('+')[0] if raw else date_el.text.strip()
                    if key:
                        dates_map[key] = value
        except Exception:
            pass

        workplaces = driver.find_elements(By.CLASS_NAME, 'doctor-details__workplace-item')
        for wp in workplaces:
            try:
                name = wp.find_element(By.CLASS_NAME, 'doctor-details__workplace-item-title').text.strip()
                address = wp.find_element(By.CLASS_NAME, 'doctor-details__workplace-item-address').text.strip()
                full = re.sub(r'\s+', '', (name + address).lower())
                addr = re.sub(r'\s+', '', address.lower())
                first = 'Няма свободни часове'
                for key, value in dates_map.items():
                    if key and (key in full or full in key):
                        first = value
                        break
                    if addr and len(addr) > 5 and addr in key:
                        first = value
                        break
                results.append({'Hospital': name, 'Address': address, 'First_Date': first, 'Coords': get_coordinates_from_map_link(wp)})
            except Exception:
                pass
    except Exception:
        pass
    return results


def get_all_first_available_dates_summary():
    found = []
    try:
        for el in driver.find_elements(By.CLASS_NAME, 'dummy--detailed-profile-card__practices-fa'):
            raw = el.get_attribute('data-date')
            val = raw.replace('T', ' ').split('+')[0] if raw else el.text.strip()
            if val:
                found.append(val)
    except Exception:
        pass
    if not found:
        try:
            for el in driver.find_elements(By.CLASS_NAME, 'dummy--booking-component__first_available'):
                raw = el.get_attribute('data-dummy-first-available')
                if raw:
                    found.append(raw.replace('T', ' ').split('+')[0])
        except Exception:
            pass
    return ' | '.join(dict.fromkeys(found)) if found else 'Няма свободни часове'


def extract_doctor_urls():
    urls = []
    try:
        for link in driver.find_elements(By.TAG_NAME, 'a'):
            href = link.get_attribute('href')
            if not href:
                continue
            u = canonicalize_url(href)
            p = urlparse(u)
            if p.netloc.lower() != 'myhealth.bg':
                continue
            path = p.path.lower()
            if ('/lekar/' in path or '/practices/lekar/' in path) and '/search' not in path:
                urls.append(u)
    except Exception:
        pass
    return list(dict.fromkeys(urls))


def wait_for_doctor_urls():
    end = time.time() + PAGE_WAIT_SECONDS
    while time.time() < end:
        if blocked_page():
            raise RuntimeError('Blocked / rate-limited page detected')
        urls = extract_doctor_urls()
        if urls:
            return urls
        time.sleep(1)
    return []


def load_search_page(page_number):
    url = f'{BASE_SEARCH_URL}{page_number}'
    for attempt in range(1, MAX_PAGE_RETRIES + 1):
        if time_limit_reached():
            return None
        print(f'\n[PAGE {page_number}] Attempt {attempt}/{MAX_PAGE_RETRIES}: {url}')
        try:
            driver.get(url)
            urls = wait_for_doctor_urls()
            if urls:
                print(f'[INFO] Page {page_number}: found {len(urls)} doctor profile URLs.')
                return urls
            print('[WARN] Page loaded but no doctor URLs were found.')
        except Exception as e:
            print(f'[WARN] Search page attempt failed: {e}')
        if attempt < MAX_PAGE_RETRIES:
            restart_driver()
            time.sleep(2 * attempt)
    return None


def scrape_doctor_profile_myhealth(url):
    for attempt in range(1, MAX_PROFILE_RETRIES + 1):
        if time_limit_reached():
            return None
        try:
            print(f'    [PROFILE {attempt}/{MAX_PROFILE_RETRIES}] {url}')
            
            driver.get(url)
            
            WebDriverWait(driver, PROFILE_WAIT_SECONDS).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'doctor-header'))
            )
            time.sleep(1.0)
            
            if blocked_page():
                raise RuntimeError('Blocked / rate-limited profile detected')
            
            doc_name = get_text_safe("//div[contains(@class, 'doctor-header')]//h2/a")
            # Корекция: Не прекратяваме изпълнението, ако липсва име (уеднаквено с PC скрипта)
            if not doc_name or doc_name == '-':
                print(f'    [WARN] Doctor name not found for {url}.')
                doc_name = '-'
                
            nzok = 'Да' if driver.find_elements(By.XPATH, "//span[contains(@class, 'ww-nzok')]") else 'Не'
            phones = []
            for link in driver.find_elements(By.XPATH, "//a[contains(@href, 'tel:')]"):
                href = link.get_attribute('href') or ''
                if href.startswith('tel:') and href[4:] not in phones:
                    phones.append(href[4:])
                    
            practices = scrape_practices_detailed() or [{'Hospital': '-', 'Address': '-', 'First_Date': '-', 'Coords': '-'}]
            
            row = {
                'Име': doc_name,
                'Специалност': get_text_safe("//div[contains(@class, 'doctor-speciality')]"),
                'Рейтинг_Инфо': get_text_safe("//span[contains(@class, 'doctor-rating-score_count')]"),
                'Първи свободен час (Общо)': get_all_first_available_dates_summary(),
                'Телефони': ', '.join(phones) if phones else '-',
                'НЗОК': nzok,
                'Биография': get_full_biography()[:1000],
                'URL': canonicalize_url(url),
                'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'Цени': scrape_prices_myhealth(),
                'Застрахователи': scrape_insurances_myhealth(),
            }
            
            # Корекция: Индексиране до 5 практики
            for i in range(1, 6):
                p = practices[i - 1] if i <= len(practices) else None
                row[f'Hospital_{i}'] = p['Hospital'] if p else '-'
                row[f'Address_{i}'] = p['Address'] if p else '-'
                row[f'First_Free_{i}'] = p['First_Date'] if p else '-'
                row[f'Coords_{i}'] = p['Coords'] if p else '-'
                
            return row
        except Exception as e:
            print(f'    [WARN] Profile attempt failed: {e}')
            if attempt < MAX_PROFILE_RETRIES:
                restart_driver()
    return None


def flag_for_continuation():
    with open(CONTINUE_FLAG_FILE, 'w', encoding='utf-8') as f:
        f.write('CONTINUE\n')


def clear_continuation_flag():
    try:
        if os.path.exists(CONTINUE_FLAG_FILE):
            os.remove(CONTINUE_FLAG_FILE)
    except Exception:
        pass


def main():
    global BASE_SEARCH_URL
    
    # Корекция: Интелигентно прочитане на входните данни
    # 1. Приоритетно проверяваме environment променливите (за YAML интеграцията)
    env_url = os.getenv('BASE_SEARCH_URL', '').strip()
    if env_url:
        BASE_SEARCH_URL = env_url
    else:
        # 2. Ако няма променлива, проверяваме дали скриптът се изпълнява интерактивно (локално)
        if sys.stdin.isatty():
            BASE_SEARCH_URL = input("Моля, въведете базовия URL адрес за търсене (напр. https://myhealth.bg/search/?page=): ").strip()
            while not BASE_SEARCH_URL:
                BASE_SEARCH_URL = input("URL адресът не може да бъде празен. Моля, въведете отново: ").strip()
        else:
            # 3. В случай на пайпнати данни (echo "url" | python script.py) четем директно от stdin
            piped_input = sys.stdin.read().strip()
            BASE_SEARCH_URL = piped_input if piped_input else "https://myhealth.bg/search/?page="

    clear_continuation_flag()
    init_csv()
    load_memory()
    load_state()
    init_driver()
    
    try:
        while True:
            if time_limit_reached():
                save_state(); flag_for_continuation(); break
            page = int(state['page'])
            urls = load_search_page(page)
            
            if urls is None:
                state['consecutive_fails'] = int(state.get('consecutive_fails', 0)) + 1
                save_state(); flag_for_continuation()
                print(f'[ERROR] Could not load page {page}. Keeping state on same page.')
                break
            
            state['consecutive_fails'] = 0
            for n, url in enumerate(urls, 1):
                if time_limit_reached():
                    save_state(); flag_for_continuation(); return
                if is_parsed(url):
                    print(f'  [{n}/{len(urls)}] [SKIP] Already parsed: {url}')
                    continue
                
                row = scrape_doctor_profile_myhealth(url)
                if row:
                    append_csv(row)
                    mark_as_parsed(url)
                    save_state()
                    print(f"  [{n}/{len(urls)}] [+] Saved: {row['Име']}")
                else:
                    print(f'  [{n}/{len(urls)}] [FAIL] {url}')
                    
            state['page'] = page + 1
            save_state()
            print(f'[INFO] Finished page {page}. Next page: {state["page"]}')
    finally:
        close_driver()
    print('[INFO] Scraper session finished.')


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        close_driver()
        print('[INFO] Interrupted by user.')
