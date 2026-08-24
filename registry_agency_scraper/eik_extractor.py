import os
import time
import json
import sys
import urllib.parse
from datetime import datetime
from playwright.sync_api import sync_playwright

# Гарантиране на UTF-8 енкодинг за конзолата (полезно за GitHub Actions)
if sys.stdout.encoding.lower() != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        pass

START_TIME = time.time()
TIME_LIMIT_SECONDS = 5.4 * 60 * 60  # Лимит за GitHub Actions (5.4 часа)

# ------------------------------------------
# ДИНАМИЧНИ ПЪТИЩА (СЪЩИТЕ КАТО В СКРИПТ 1)
# ------------------------------------------
try:
    base_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    base_dir = os.getcwd()

output_dir = os.path.join(base_dir, "registry_agency_outputs")
os.makedirs(output_dir, exist_ok=True)

# Файловете ще бъдат в същата папка като първия код, за да работят в синергия
OUTPUT_FILE = os.path.join(output_dir, "100_percent_valid_uics.txt")
QUERIES_MEMORY_FILE = os.path.join(output_dir, "processed_queries.txt")
STATE_FILE = os.path.join(output_dir, "savegame_uic_finder.json")
CONTINUE_FLAG_FILE = os.path.join(output_dir, "CONTINUE_FLAG_UIC_FINDER")

def log_msg(msg):
    """Помощна функция за красиво принтиране с точен час."""
    current_time = datetime.now().strftime('%H:%M:%S')
    print(f"[{current_time}] {msg}", flush=True)

def time_limit_reached():
    return (time.time() - START_TIME) >= TIME_LIMIT_SECONDS

def flag_for_continuation():
    try:
        with open(CONTINUE_FLAG_FILE, 'w') as f:
            f.write("CONTINUE")
    except Exception:
        pass

def clear_continuation_flag():
    if os.path.exists(CONTINUE_FLAG_FILE):
        try:
            os.remove(CONTINUE_FLAG_FILE)
        except:
            pass

# Глобален state, за да знаем до коя комбинация сме стигнали
state = {
    "query_idx": 0
}

# Зареждане на предишното състояние
if os.path.exists(STATE_FILE):
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            loaded_state = json.load(f)
            state["query_idx"] = loaded_state.get("query_idx", 0)
    except Exception:
        pass

def save_state():
    """Запазва текущия индекс сигурно, за да не загубим прогреса при краш."""
    payload = {
        "query_idx": state["query_idx"],
        "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    temp_file = STATE_FILE + ".tmp"
    try:
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(temp_file, STATE_FILE)
    except Exception:
        pass

def main():
    clear_continuation_flag()
    
    # 1. Зареждаме изтеглените ЕИК номера (Памет 1)
    extracted_uics = set()
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            for line in f:
                val = line.strip()
                if val:
                    extracted_uics.add(val)
    log_msg(f"[INFO] Заредени от базата до момента: {len(extracted_uics)} ЕИК номера.")

    # 2. Зареждаме вече претърсените комбинации (Памет 2)
    processed_queries = set()
    if os.path.exists(QUERIES_MEMORY_FILE):
        with open(QUERIES_MEMORY_FILE, "r", encoding="utf-8") as f:
            for line in f:
                val = line.strip()
                if val:
                    processed_queries.add(val)
    log_msg(f"[INFO] Напълно завършени търсения (комбинации): {len(processed_queries)}.")

    # 3. Генериране на пълната матрица
    bg_alphabet = [chr(i) for i in range(1040, 1072)] # А-Я
    en_alphabet = [chr(i) for i in range(65, 91)]     # A-Z
    digits = [str(i) for i in range(10)]              # 0-9
    
    all_chars = bg_alphabet + en_alphabet + digits
    single_chars = [a for a in all_chars]
    double_chars = [a + b for a in all_chars for b in all_chars]
    
    search_queries = single_chars + double_chars
    log_msg(f"[INFO] Започваме UI сканиране по {len(search_queries)} комбинации...")

    with sync_playwright() as p:
        # ЗАДЪЛЖИТЕЛНИ аргументи за сървърно изпълнение (GitHub Actions)
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
        )
        context = browser.new_context(
            locale='bg-BG',
            viewport={'width': 1280, 'height': 720}
        )
        page = context.new_page()

        while state["query_idx"] < len(search_queries):
            # Проверка за времевия лимит преди всяка нова комбинация
            if time_limit_reached():
                log_msg(f"[ВРЕМЕТО ИЗТЕЧЕ] Спираме изпълнението. Индекс: {state['query_idx']}")
                save_state()
                flag_for_continuation()
                break

            query = search_queries[state["query_idx"]]

            if query in processed_queries:
                state["query_idx"] += 1
                save_state()
                continue

            encoded_query = urllib.parse.quote(query)
            url = f"https://portal.registryagency.bg/CR/Reports/VerificationPersonOrg?name={encoded_query}&selectedSearchFilter=1"
            
            try:
                # Пауза за сигурност преди зареждане
                time.sleep(2)
                page.goto(url, wait_until='networkidle', timeout=30000)
                time.sleep(2)
                
                if "Достигнат е максимално допустимият брой заявки" in page.content():
                    raise Exception("RATE_LIMIT")
                
                try:
                    page.wait_for_selector('table.table-collapsible tbody tr', timeout=5000)
                except:
                    if "Достигнат е максимално допустимият брой заявки" in page.content():
                        raise Exception("RATE_LIMIT")
                        
                    log_msg(f"[{query}] Няма намерени резултати. Отбелязваме като завършено.")
                    with open(QUERIES_MEMORY_FILE, "a", encoding="utf-8") as f:
                        f.write(f"{query}\n")
                    processed_queries.add(query)
                    state["query_idx"] += 1
                    save_state()
                    continue
                
                page_num = 1
                
                while True:
                    # Проверка за времевия лимит и при прелистване на дълги резултати
                    if time_limit_reached():
                        log_msg("[ВРЕМЕТО ИЗТЕЧЕ] Спираме изпълнението по време на странициране.")
                        save_state()
                        flag_for_continuation()
                        try:
                            browser.close()
                        except:
                            pass
                        return

                    if "Достигнат е максимално допустимият брой заявки" in page.content():
                        raise Exception("RATE_LIMIT")

                    rows = page.locator('table.table-collapsible tbody tr:not(.collapsible-row)').all()
                    new_uics = 0
                    
                    for row in rows:
                        cols = row.locator('td').all()
                        if len(cols) >= 3:
                            uic_text = cols[2].locator('p.field-text').inner_text().strip()
                            uic_clean = "".join(filter(str.isdigit, uic_text))
                            
                            if len(uic_clean) >= 9 and uic_clean not in extracted_uics:
                                with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                                    f.write(f"{uic_clean}\n")
                                extracted_uics.add(uic_clean)
                                new_uics += 1
                                
                    log_msg(f"[{query} - Стр {page_num}] Извлечени. Нови: {new_uics}. Общо в базата: {len(extracted_uics)}")
                    
                    next_btn = page.locator('li.page-item.next:not(.disabled) a').first
                    
                    if next_btn.count() > 0 and next_btn.is_visible():
                        next_btn.click()
                        page_num += 1
                        
                        page.wait_for_timeout(1000) 
                        try:
                            page.wait_for_selector('table.table-collapsible tbody tr', state='attached', timeout=5000)
                        except:
                            pass
                    else:
                        log_msg(f"[УСПЕХ] Комбинацията '{query}' е напълно източена.")
                        with open(QUERIES_MEMORY_FILE, "a", encoding="utf-8") as f:
                            f.write(f"{query}\n")
                        processed_queries.add(query)
                        state["query_idx"] += 1
                        save_state()
                        break
                        
            except Exception as e:
                if "RATE_LIMIT" in str(e) or "Достигнат е максимално допустимият брой заявки" in page.content():
                    log_msg("[БЛОКАЖ] Сървърът ни ограничи! Затваряме браузъра и заспиваме за 10 секунди...")
                    try:
                        browser.close()
                    except:
                        pass
                    
                    time.sleep(10)
                    log_msg(f"[INFO] Събуждане! Рестартираме браузъра и опитваме отново комбинация '{query}'...")
                    
                    browser = p.chromium.launch(
                        headless=True,
                        args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
                    )
                    context = browser.new_context(
                        locale='bg-BG',
                        viewport={'width': 1280, 'height': 720}
                    )
                    page = context.new_page()
                else:
                    log_msg(f"[ГРЕШКА] при '{query}': {e}. Преминаваме към следващата комбинация след 2 секунди.")
                    time.sleep(2)
                    state["query_idx"] += 1
                    save_state()

        try:
            browser.close()
        except:
            pass
        log_msg("[КРАЙ] Всички комбинации са напълно сканирани!")

if __name__ == "__main__":
    main()
