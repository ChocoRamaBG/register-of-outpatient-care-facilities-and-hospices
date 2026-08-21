import os
import csv
import json
import time
import sys
import requests
from datetime import datetime

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
    
    processed_uics = load_memory()
    print(f"[INFO] Resuming from UIC index {state['current_index']}. Cached: {len(processed_uics)}", flush=True)

    fieldnames = [
        "UIC_Query", "URL", "Заглавие (Статус)", "Състояние към дата",
        "ЕИК/ПИК", "Фирмено дело", "Фирма/Наименование", "Правна форма",
        "Държава", "Област и Община", "Населено място", "Адрес",
        "Предмет на дейност", "Физическо лице", "Raw_API_JSON" 
    ]

    if not os.path.exists(csv_file_path):
        with open(csv_file_path, mode='w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()

    session = requests.Session()
    session.headers.update({
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'bg',
        'content-type': 'application/json; charset=utf-8',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
        'referer': 'https://portal.registryagency.bg/'
    })

    current_date = datetime.now().strftime("%Y-%m-%dT23:59:59.999Z")
    valid_counter = 0

    for i in range(state['current_index'], 10000000000):
        uic_str = f"{i:09d}"
        
        if not is_valid_eik(uic_str):
            continue
            
        if time_limit_reached():
            print("[INFO] Time limit reached. Triggering continue flag.", flush=True)
            save_state(i)
            flag_for_continuation()
            break
            
        state['current_index'] = i
        valid_counter += 1
        
        if valid_counter % 500 == 0:
            print(f"[PROGRESS] Checking valid API UIC: {uic_str}...", flush=True)
        
        if uic_str in processed_uics:
            save_state(i + 1)
            continue

        api_url = f"https://portal.registryagency.bg/CR/api/Deeds/{uic_str}?entryDate={current_date}&loadFieldsFromAllLegalForms=false"
        ui_url = f"https://portal.registryagency.bg/CR/Reports/ActiveConditionTabResult?uic={uic_str}"

        # Железен цикъл за retry
        success = False
        retry_wait = 15
        response = None
        
        while not success:
            try:
                response = session.get(api_url, timeout=10)
                
                # Ако ударим лимита, чакаме и повтаряме СЪЩИЯ номер, не го прескачаме
                if response.status_code == 429:
                    print(f"[WARNING] Rate limited (429) at {uic_str}. Sleeping for {retry_wait} seconds...", flush=True)
                    time.sleep(retry_wait)
                    retry_wait += 5  # Увеличаваме наказанието, ако продължава да ни реже
                    continue
                
                success = True
            except Exception as e:
                print(f"[{uic_str}] Network error: {e}. Retrying in 5 seconds...", flush=True)
                time.sleep(5)

        # Ако фирмата не съществува
        if response.status_code in [204, 404] or not response.text:
            save_to_memory(uic_str)
            processed_uics.add(uic_str)
            save_state(i + 1)
            continue

        try:
            data = response.json()
            
            if not data:
                save_to_memory(uic_str)
                processed_uics.add(uic_str)
                save_state(i + 1)
                continue

            row_data = {
                "UIC_Query": uic_str,
                "URL": ui_url,
                "Raw_API_JSON": json.dumps(data, ensure_ascii=False)
            }

            with open(csv_file_path, mode='a', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writerow(row_data)

            save_to_memory(uic_str)
            processed_uics.add(uic_str)
            print(f"[{uic_str}] API Data extracted successfully.", flush=True)
            
        except Exception as e:
            print(f"[{uic_str}] JSON parsing error: {e}", flush=True)

        # Малко по-голяма пауза, за да не ни режат толкова агресивно
        time.sleep(0.1)
        save_state(i + 1)

if __name__ == "__main__":
    main()
