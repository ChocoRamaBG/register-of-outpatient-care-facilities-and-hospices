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

def time_limit_reached():
    return (time.time() - START_TIME) >= TIME_LIMIT_SECONDS

def flag_for_continuation():
    try:
        with open(CONTINUE_FLAG_FILE, 'w') as f:
            f.write("CONTINUE")
    except Exception as e:
        print(f"[ERROR] Could not write continue flag: {e}")

def clear_continuation_flag():
    if os.path.exists(CONTINUE_FLAG_FILE):
        try:
            os.remove(CONTINUE_FLAG_FILE)
        except:
            pass

state = {"current_index": 0}
if os.path.exists(state_file):
    try:
        with open(state_file, "r", encoding="utf-8") as f:
            loaded_state = json.load(f)
            state["current_index"] = loaded_state.get("current_index", 0)
    except Exception:
        pass

def save_state(index_val):
    payload = {
        "current_index": index_val,
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

def main():
    clear_continuation_flag()
    base_url = "https://portal.registryagency.bg/CR/Reports/ActiveConditionTabResult?uic="
    
    processed_uics = load_memory()
    print(f"[INFO] Resuming from UIC index {state['current_index']}. Cached: {len(processed_uics)}")

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

        for i in range(state['current_index'], 10000000000):
            if time_limit_reached():
                print("[INFO] Time limit reached. Triggering continue flag.")
                flag_for_continuation()
                break

            uic_str = f"{i:09d}"
            state['current_index'] = i
            
            if uic_str in processed_uics:
                save_state(i + 1)
                continue

            target_url = f"{base_url}{uic_str}"

            try:
                page.goto(target_url, wait_until='domcontentloaded', timeout=15000)
                
                try:
                    page.wait_for_selector('.page-heading', timeout=1500)
                except Exception:
                    pass

                field_containers = page.locator('.field-container')
                heading_title_loc = page.locator('.page-heading-title')
                heading_subtitle_loc = page.locator('.page-heading-sub-title')

                if heading_title_loc.count() == 0 and field_containers.count() == 0:
                    save_to_memory(uic_str)
                    processed_uics.add(uic_str)
                    save_state(i + 1)
                    continue

                row_data = {"UIC_Query": uic_str, "URL": target_url}
                other_data = {}

                if heading_title_loc.count() > 0:
                    row_data["Заглавие (Статус)"] = heading_title_loc.inner_text().strip()

                if heading_subtitle_loc.count() > 0:
                    subtitle_text = heading_subtitle_loc.inner_text().strip()
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
                        raw_title = title_loc.inner_text().strip()
                        raw_text = text_loc.inner_text().strip()
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
                print(f"[{uic_str}] Data extracted successfully.")

            except Exception as e:
                print(f"[{uic_str}] Timeout or processing error: {e}")

            save_state(i + 1)
        
        browser.close()

if __name__ == "__main__":
    main()
