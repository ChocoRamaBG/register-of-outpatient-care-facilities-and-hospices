import os
import csv
import json
import time
import sys
from datetime import datetime
from playwright.sync_api import sync_playwright

if sys.stdout.encoding.lower() != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        pass

START_TIME = time.time()
TIME_LIMIT_SECONDS = 5.4 * 60 * 60

try:
    base_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    base_dir = os.getcwd()

output_dir = os.path.join(base_dir, "registry_agency_outputs")
os.makedirs(output_dir, exist_ok=True)

csv_file_path = os.path.join(output_dir, 'registry_agency_data_mega.csv')
memory_file_path = os.path.join(output_dir, 'processed_uics_registry.txt')
state_file = os.path.join(output_dir, "savegame_registry_agency.json")
CONTINUE_FLAG_FILE = os.path.join(output_dir, "CONTINUE_FLAG_REGISTRY_AGENCY")
PRIORITY_LIST_FILE = os.path.join(output_dir, "100_percent_valid_uics.txt")

def log_msg(msg):
    """Помощна функция за красиво принтиране с точен час."""
    current_time = datetime.now().strftime('%H:%M:%S')
    print(f"[{current_time}] {msg}", flush=True)

# ==========================================
# МОДУЛ 11 ЦЕДКА ЗА ВАЛИДЕН ЕИК (БУЛСТАТ)
# ==========================================
def is_valid_eik(eik: str) -> bool:
    if len(eik) != 9 or not eik.isdigit():
        return False
    weights1 = [1, 2, 3, 4, 5, 6, 7, 8]
    sum1 = sum(int(eik[i]) * weights1[i] for i in range(8))
    rem1 = sum1 % 11
    if rem1 != 10:
        return rem1 == int(eik[8])
    weights2 = [3, 4, 5, 6, 7, 8, 9, 10]
    sum2 = sum(int(eik[i]) * weights2[i] for i in range(8))
    rem2 = sum2 % 11
    if rem2 != 10:
        return rem2 == int(eik[8])
    return int(eik[8]) == 0
# ==========================================

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

state = {
    "priority_index": 0,
    "current_index": 0
}

if os.path.exists(state_file):
    try:
        with open(state_file, "r", encoding="utf-8") as f:
            loaded_state = json.load(f)
            state["priority_index"] = loaded_state.get("priority_index", 0)
            state["current_index"] = loaded_state.get("current_index", 0)
    except Exception:
        pass

def save_state():
    payload = {
        "priority_index": state["priority_index"],
        "current_index": state["current_index"],
        "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    temp_file = state_file + ".tmp"
    try:
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(temp_file, state_file)
    except Exception:
        pass

def load_memory():
    processed = set()
    if os.path.exists(memory_file_path):
        with open(memory_file_path, "r", encoding="utf-8") as f:
            for line in f:
                processed.add(line.strip())
    return processed

def save_to_memory(uic_str):
    with open(memory_file_path, 'a', encoding='utf-8') as f:
        f.write(f"{uic_str}\n")

# ==========================================
# ИЗНЕСЕНА ЛОГИКА ЗА СКРЕЙПВАНЕ
# ==========================================
def scrape_company(uic_str, page, base_url, processed_uics, csv_writer_args):
    """Скрейпва даден ЕИК. Връща (статус, име_на_фирма_или_грешка)."""
    if uic_str in processed_uics:
        return "SKIPPED", ""

    target_url = f"{base_url}{uic_str}"
    fieldnames, label_map = csv_writer_args

    try:
        page.goto(target_url, wait_until='domcontentloaded', timeout=15000)
        
        try:
            page.wait_for_selector('.page-heading', timeout=1500)
        except Exception:
            pass

        field_containers = page.locator('.field-container')
        heading_title_loc = page.locator('.page-heading-title')
        heading_subtitle_loc = page.locator('.page-heading-sub-title')

        # Ако страницата е празна (няма такава фирма)
        if heading_title_loc.count() == 0 and field_containers.count() == 0:
            save_to_memory(uic_str)
            processed_uics.add(uic_str)
            return "EMPTY", ""

        row_data = {"UIC_Query": uic_str, "URL": target_url}
        other_data = {}

        if heading_title_loc.count() > 0:
            row_data["Заглавие (Статус)"] = heading_title_loc.first.inner_text().strip()

        if heading_subtitle_loc.count() > 0:
            subtitle_text = heading_subtitle_loc.first.inner_text().strip()
            if "състояние към дата:" in subtitle_text:
                row_data["Състояние към дата"] = subtitle_text.split("състояние към дата:")[-1].strip()
            else:
                row_data["Състояние към дата"] = subtitle_text

        count = field_containers.count()
        for j in range(count):
            container = field_containers.nth(j)
            title_loc = container.locator('.field-title')
            text_loc = container.locator('.field-text')

            if title_loc.count() > 0 and text_loc.count() > 0:
                raw_title = title_loc.first.inner_text().strip()
                
                text_elements = text_loc.all()
                raw_text = "\n".join([el.inner_text().strip() for el in text_elements if el.inner_text().strip()])
                
                mapped_title = label_map.get(raw_title, raw_title)
                
                if mapped_title == "1. ЕИК/ПИК":
                    lines = [line.strip() for line in raw_text.split('\n') if line.strip()]
                    if lines:
                        row_data["ЕИК/ПИК"] = lines[0]
                    if len(lines) > 1 and "Фирмено дело:" in lines[1]:
                        row_data["Фирмено дело"] = lines[1].replace("Фирмено дело:", "").strip()
                elif mapped_title == "2. Фирма/Наименование":
                    row_data["Фирма/Наименование"] = raw_text
                elif mapped_title == "3. Правна форма":
                    row_data["Правна форма"] = raw_text
                elif mapped_title == "5. Седалище и адрес на управление":
                    lines = [line.strip() for line in raw_text.split('\n') if line.strip()]
                    address_parts = []
                    for line in lines:
                        if line.startswith("Държава:"):
                            row_data["Държава"] = line.replace("Държава:", "").strip()
                        elif line.startswith("Област:"):
                            row_data["Област и Община"] = line.strip()
                        elif line.startswith("Населено място:"):
                            row_data["Населено място"] = line.replace("Населено място:", "").strip()
                        else:
                            address_parts.append(line.strip())
                    if address_parts:
                        row_data["Адрес"] = ", ".join(address_parts)
                elif mapped_title == "6. Предмет на дейност":
                    row_data["Предмет на дейност"] = raw_text
                elif mapped_title == "18. Физическо лице - търговец":
                    parts = raw_text.split(', Държава:')
                    row_data["Физическо лице"] = parts[0].strip()
                else:
                    other_data[raw_title] = raw_text

        if other_data:
            row_data["Other_Data"] = json.dumps(other_data, ensure_ascii=False)

        with open(csv_file_path, mode='a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writerow(row_data)

        save_to_memory(uic_str)
        processed_uics.add(uic_str)
        
        # Опитваме се да извадим името за лога
        company_name = row_data.get("Заглавие (Статус)", row_data.get("Фирма/Наименование", "Неизвестно име"))
        return "SUCCESS", company_name

    except Exception as e:
        return "ERROR", str(e)


def main():
    clear_continuation_flag()
    base_url = "https://portal.registryagency.bg/CR/Reports/ActiveConditionTabResult?uic="
    
    processed_uics = load_memory()
    log_msg(f"[СТАРТ] Възстановяване на сесията... Кеширани записи до момента: {len(processed_uics)}")

    session_extracted_count = 0  # Брояч за текущата сесия (колко НОВИ сме източили сега)

    fieldnames = [
        "UIC_Query", "URL", "Заглавие (Статус)", "Състояние към дата",
        "ЕИК/ПИК", "Фирмено дело", "Фирма/Наименование", "Правна форма",
        "Държава", "Област и Община", "Населено място", "Адрес",
        "Предмет на дейност", "Физическо лице", "Other_Data" 
    ]

    label_map = {
        "1. UIC/PIC": "1. ЕИК/ПИК", "1. ЕИК/ПИК": "1. ЕИК/ПИК",
        "2. Company/Name": "2. Фирма/Наименование", "2. Фирма/Наименование": "2. Фирма/Наименование",
        "3. Legal form": "3. Правна форма", "3. Правна форма": "3. Правна форма",
        "5. Head office and registered office": "5. Седалище и адрес на управление", "5. Седалище и адрес на управление": "5. Седалище и адрес на управление",
        "6. Scope of business activity": "6. Предмет на дейност", "6. Предмет на дейност": "6. Предмет на дейност",
        "18. Natural person - trader": "18. Физическо лице - търговец", "18. Физическо лице - търговец": "18. Физическо лице - търговец"
    }

    if not os.path.exists(csv_file_path):
        with open(csv_file_path, mode='w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()

    csv_args = (fieldnames, label_map)

    # Зареждаме приоритетния списък (ако го има)
    priority_uics = []
    if os.path.exists(PRIORITY_LIST_FILE):
        with open(PRIORITY_LIST_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                val = line.strip()
                if val:
                    priority_uics.append(val)
        log_msg(f"[ИНФО] Зареден списък с {len(priority_uics)} гарантирани ЕИК номера.")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
        )
        context = browser.new_context(
            locale='bg-BG',
            extra_http_headers={'Accept-Language': 'bg-BG,bg;q=0.9'},
            viewport={'width': 1920, 'height': 1080}
        )
        page = context.new_page()

        # ========================================================
        # ФАЗА 1: Приоритетен списък + Съседни ЕИК номера (Квартал)
        # ========================================================
        if priority_uics and state["priority_index"] < len(priority_uics):
            log_msg(f"[ФАЗА 1] Старт на Квартално сканиране (започваме от индекс {state['priority_index']} / {len(priority_uics)})...")
            
            for idx in range(state["priority_index"], len(priority_uics)):
                base_uic = priority_uics[idx]
                
                if time_limit_reached():
                    log_msg(f"[ВРЕМЕТО ИЗТЕЧЕ] Спираме Фаза 1. Общо източени нови фирми тази сесия: {session_extracted_count}")
                    state["priority_index"] = idx
                    save_state()
                    flag_for_continuation()
                    browser.close()
                    return

                # Генерираме "Квартала": базовия номер + 30 надолу и 30 нагоре
                base_num = int(base_uic)
                neighborhood = []
                for n in range(max(0, base_num - 30), base_num + 31):
                    n_str = f"{n:09d}"
                    if is_valid_eik(n_str):
                        neighborhood.append(n_str)
                
                scraped_count = 0
                empty_count = 0
                skipped_count = 0

                log_msg(f"[КВАРТАЛ] Базов ЕИК: {base_uic} | Валидни съседи за проверка: {len(neighborhood)}")
                
                for neighbor_uic in neighborhood:
                    if time_limit_reached():
                        log_msg(f"[ВРЕМЕТО ИЗТЕЧЕ] Спираме Фаза 1. Общо източени нови фирми тази сесия: {session_extracted_count}")
                        state["priority_index"] = idx
                        save_state()
                        flag_for_continuation()
                        browser.close()
                        return
                        
                    status, name_or_err = scrape_company(neighbor_uic, page, base_url, processed_uics, csv_args)
                    
                    if status == "SUCCESS":
                        scraped_count += 1
                        session_extracted_count += 1
                        log_msg(f"  -> [УСПЕХ] {neighbor_uic} : {name_or_err}")
                    elif status == "EMPTY":
                        empty_count += 1
                    elif status == "SKIPPED":
                        skipped_count += 1
                    elif status == "ERROR":
                        log_msg(f"  -> [ГРЕШКА] {neighbor_uic} : {name_or_err}")

                log_msg(f"[РЕЗЮМЕ КВАРТАЛ] {base_uic} завършен. Нови: {scraped_count} | Празни: {empty_count} | Прескочени: {skipped_count}\n")
                    
                # Запазваме прогреса на всеки изчистен базов номер
                state["priority_index"] = idx + 1
                save_state()

        # Когато приключим изцяло с Фаза 1, маркираме я като приключена
        if priority_uics:
            state["priority_index"] = len(priority_uics)
            save_state()

        # ========================================================
        # ФАЗА 2: Класически последователен скенер (Брутфорс)
        # ========================================================
        log_msg(f"[ФАЗА 2] Старт на последователно сканиране от ЕИК {state['current_index']:09d} нагоре...")
        
        for i in range(state['current_index'], 10000000000):
            if time_limit_reached():
                log_msg(f"[ВРЕМЕТО ИЗТЕЧЕ] Спираме Фаза 2. Общо източени нови фирми тази сесия: {session_extracted_count}")
                state["current_index"] = i
                save_state()
                flag_for_continuation()
                break

            uic_str = f"{i:09d}"
            
            if not is_valid_eik(uic_str):
                if i % 1000 == 0:
                    log_msg(f"[ТЪРСЕНЕ] Стигнахме до номер: {uic_str}...")
                continue
            
            status, name_or_err = scrape_company(uic_str, page, base_url, processed_uics, csv_args)
            
            if status == "SUCCESS":
                session_extracted_count += 1
                log_msg(f"[УСПЕХ ФАЗА 2] {uic_str} -> {name_or_err}")
            elif status == "ERROR":
                log_msg(f"[ГРЕШКА ФАЗА 2] {uic_str} -> {name_or_err}")

            state["current_index"] = i + 1
            save_state()
        
        browser.close()
        log_msg(f"[КРАЙ] Скриптът приключи успешно. Общо източени нови фирми тази сесия: {session_extracted_count}")

if __name__ == "__main__":
    main()
