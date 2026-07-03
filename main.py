import requests
import pandas as pd
import os
import time
import random
import sys
import shutil
import re
from datetime import datetime

# --- CONFIGURATION & PATH SETUP ---
try:
    output_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    output_dir = os.getcwd()

INPUT_FILENAME = "BG_Medical_Registry_Remaining.xlsx" 
INPUT_FILE_PATH = os.path.join(output_dir, INPUT_FILENAME)
PROCESSED_LOG_FILE = os.path.join(output_dir, "processed_ids.txt")
CONTINUE_FLAG_FILE = os.path.join(output_dir, "CONTINUE_FLAG")

# Safety margin: GitHub Actions timeout is typically 6h. Stopping at 5h 40m.
MAX_RUNTIME_SECONDS = 20400 
START_TIME = time.time()

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_FILE = os.path.join(output_dir, f'FINAL_DOCTORS_BATCH_{TIMESTAMP}.xlsx')

headers = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9,bg;q=0.8',
    'origin': 'https://opendata.his.bg',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36'
}

# --- DATA CLEANING FUNCTIONS ---

def clean_person_name(raw_name):
    """Removes professional titles and special characters from names."""
    if not isinstance(raw_name, str) or not raw_name.strip():
        return ""
    
    # Remove common professional and academic titles
    titles_pattern = r'(?i)\b(д-р|доц\.|проф\.|акад\.|д\.м\.н\.|д\.м\.|дър|д-р\.)\b'
    cleaned = re.sub(titles_pattern, '', raw_name)
    
    # Remove any special characters, keeping only letters, spaces, and hyphens
    cleaned = re.sub(r'[^а-яА-Яa-zA-Z\s\-]', ' ', cleaned)
    
    # Normalize multiple spaces into a single space
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned

def clean_bg_address(raw_addr):
    """Standardizes and cleans Bulgarian address strings."""
    if not isinstance(raw_addr, str) or not raw_addr:
        return ""
    
    # 0. Metadata / Status verification
    invalid_indicators = ["ЗАЛИЧЕН", "ЗАКРИТ", "НЕ СЪЩЕСТВУВА", "НЯМА ДАННИ", "ПРИЗЕМЕН", "СУТЕРЕН", "ПОЛИКЛИНИКА", "ЗДРАВНА СЛУЖБА", "СЗС"]
    if len(raw_addr) < 25 and any(x in raw_addr.upper() for x in invalid_indicators):
        return "INVALID_ADDRESS_METADATA"

    # 1. Standardize Symbols
    clean = raw_addr.replace('№', ' ').replace(' N ', ' ').replace(' No ', ' ').replace('номер', ' ')
    clean = clean.replace('"', '').replace('„', '').replace('“', '').replace("'", "").replace("`", "")
    clean = re.sub(r'\s+', ' ', clean)
    
    # 2. Remove Administrative Prefixes
    clean = re.sub(r'Обл\.\s*[^,;]+[,;]?', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'област\s*[^,;]+[,;]?', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'Общ\.\s*[^,;]+[,;]?', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'община\s*[^,;]+[,;]?', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'^\s*\d+[\.,]\s*', '', clean)

    # 3. Stop Words (Truncation Triggers)
    stop_words = [
        # Buildings & Locations
        r'ет\.', r'етаж', r'ет\s', r'е\.', r'ниво', r'Е-', 
        r'ап\.', r'апартамент', r'ап\s', r'ап\d', r'ателие', r'ат\.', r'АП\.',
        r'каб\.', r'кабинет', r'к-т', r'к\.\s*\d', r'к\d+', r'К-',
        r'амб\.', r'амбулатория', r'амб\s', r'стая', r'ст\.', r'ст\d', r'офис', r'оф\.', 
        r'помещение', r'зала', r'хале', r'салон', r'склад', 'мазе', r'маг\.', r'магазин', r'обект', 
        r'пав\.', r'павилион', r'барака', r'бунгало', 'фургон', 'контейнер', 'каравана',
        r'партер', r'сутерен', r'приземен', r'кота', 'полуетаж', 'подблоково',
        r'вх\.', r'вход', r'вх\s', r'В-', r'крило', r'сектор', r'тяло', r'корпус', r'база', r'Б:', 
        r'блок\s+[А-Яа-я]', r'бл\.', r'бл\s', r'б\.', r'щанд', r'гараж', r'трафопост',
        
        # Medical Institutions
        r'ДКЦ', r'МБАЛ', r'УМБАЛ', r'СБАЛ', r'МЦ\s', r'МЦ-', r'МДЦ', r'АИПП', r'СМДЛ', 'МСЦ', 'ДМСГД',
        r'Поликлиника', r'п-ка', r'Здравна служба', r'Здравен дом', r'Здравен участък', r'Здраве',
        r'СЗС', r'СЗУ', r'ФЗП', r'ФСМП', r'ЦСМП', 'АПЗЗ', 'СБР', 'ДП', 'ОБ', 'РБ', 'ВМБ',
        r'Болница', r'Диспансер', r'Лаборатория', 'Микробиология', 'Рентген', 'Хематология', 'Хистология',
        r'Филиал', r'Ф\.', r'Ф:', r'ЦПЗ', r'КОЦ', r'ФДМ', r'ДЦ', r'ТЕЛК', r'РЗИ', r'ХЕИ', r'ОСП', 'ТДКЦ', 'ОДПФЗС',
        r'ВМА', 'МБАБ', 'СБАЛО', 'СБАЛАГ', 'УПМБАЛ', 'СБДПЛР', 'ЦКВБ', 'ЦКВЗ',
        r'Медицински център', r'Дентален център', r'Болнична', r'Спешна помощ',
        r'манипулационна', r'манип\.', r'приемно', r'регистратура', r'център за', r'звено',
        r'отделение', r'клиника', r'катедра', r'\bЗС\b', 
        r'СХБАЛ', 'СБАЛББ', 'МДЛ', 'СМЛ', 'ЛЗУ', 'ДДМУИ', 'ПФДПО', 'ОДОЗС',
        r'РСП', 'ДПО', 'МТЛ', 'ЦНИКА', 'СБХЛ', 'ОМЦ', 'САГБАЛ', 'УСБАЛЕ', 'ГПСМП', 'АМЦСМП', 'ГППМП', 'АИСМП', 'ИПСМП', 'КЦА',
        r'ЛК', r'РК',
        
        # Education, Admin & Business
        r'кметство', r'община\s', r'съвет', r'читалище', r'поща', r'съдебна палата',
        r'училище', r'ОУ\s', r'СУ\s', r'ЕГ\s', r'ПГ\s', r'СОУ\s', r'СПТУ', 'ПТУ', 'НУ\s', 'ДГ',
        r'гимназия', 'колеж', 'университет', 'факултет', 'институт', 'академия', 'ПФК', 'НСА', 'БАН',
        r'детска градина', r'ОДЗ', r'ясла', r'дом за', r'пансион', r'общежитие',
        r'стадион', r'автогара', r'жп гара', r'гара', r'летище', 'терминал',
        r'завод', 'цех', 'фабрика', 'предприятие', 'комбинат', 'миби', 'рудник',
        r'АД\s', r'ЕООД', r'ООД', r'ЕАД', r'ЕТ\s', 'КД', 'СД',
        r'ООС', r'ДСК', 'МВР', 'БДЖ', 'ВиК', 'БТК', 'ТПК', 'ДЗИ', 'ДАП', 'АПК', 'ТКЗС', 'ПК',
        r'Търговски център', r'ТЦ\s', r'Т\.Ц\.', r'Мол\s', r'Mall', r'Бизнес център', r'БЦ\s',
        r'Ритейл', r'Аптека', r'Оптика', r'Дрогерия', r'супермаркет',
        r'ТЕЦ', r'ВЕЦ', r'АЕЦ', r'Електроцентрала', r'ЗПЗ', r'СПЗ', r'НПЗ', r'ЮПЗ', r'ПЗ\s',
        
        # Tourism
        r'хотел', r'х-л', r'комплекс', r'резорт', r'resort', 'вила', 'вили',
        r'ваканционно', r'къмпинг', r'хижа', r'санаториум', r'балнео', 'СПА', 'SPA',
        r'к\.к\.', r'к\.к', r'курортен комплекс', 'ваканционен',
        r'ж\.г\.', r'жилищна група', r'в\.з\.', r'вилна зона', 
        r'местност', r'м-ст', r'стопански двор', r'к-с',
        
        # Connectors & Misc
        r'в сградата', r'сграда', r'бивш', r'бивша', r'бивше', 'бившо', 'старата',
        r'срещу', r'до бл\.', r'до вх\.', r'зад ', r'на територията', 
        r'продължение', r'разширение', r'до ', r'между', r'под ', r'на ъгъла', r'на гърба',
        r'УПИ', r'ПИ\s', r'идентификатор', r'АОС', 'имот', 'кв\.', 'квартал \d', 'парцел', 'П-Л',
        r'адрес 2', r'2-ри', r'3-ти', r'р-н', r'Р\.П\.', r'УЧ-ЩЕ'
    ]
    
    # 1. Clean using Stop Words
    pattern_str = r'([,\s\(\.\/-]+)(' + '|'.join(stop_words) + r').*$'
    clean = re.sub(pattern_str, '', clean, flags=re.IGNORECASE)

    # 2. Parentheses cleanup
    clean = re.sub(r'\(.*?\)', '', clean)
    clean = re.sub(r'/.*?/', '', clean)   
    
    # 3. Specific Edge Cases
    clean = re.sub(r'\bномер\b', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'\bс\.\s*$', '', clean) 
    clean = re.sub(r'\bул\.\s*$', '', clean)
    
    # 4. Final Formatting
    clean = re.sub(r'\s+', ' ', clean)      
    clean = re.sub(r'\s,', ',', clean)      
    clean = re.sub(r',+', ',', clean)       
    clean = clean.strip(' ,.-/\\')
    
    # 5. Sanity Check
    if len(clean) < 3:
        if "гр." in raw_addr or "с." in raw_addr:
             city_match = re.search(r'(гр\.|с\.)\s*([А-Яа-я\s\-]+)', raw_addr)
             if city_match:
                 return city_match.group(0)
        return "INVALID_ADDRESS_TOO_SHORT"

    return clean

# --- STATE MANAGEMENT ---

def get_processed_ids():
    """Reads the set of already processed record IDs."""
    if not os.path.exists(PROCESSED_LOG_FILE):
        return set()
    with open(PROCESSED_LOG_FILE, 'r', encoding='utf-8') as f:
        return set(line.strip() for line in f if line.strip())

def save_processed_id(id_val):
    """Appends a successfully processed ID to the tracking log."""
    with open(PROCESSED_LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{id_val}\n")

def load_ids_from_col_b():
    """Extracts target IDs from column B of the specified Excel file."""
    print(f"[INFO] Targeting input file: {INPUT_FILE_PATH}")
    if not os.path.exists(INPUT_FILE_PATH):
        print("[ERROR] Input file not found. Ensure it exists in the script directory.")
        sys.exit(1)
    
    temp_file = os.path.join(output_dir, "temp_processing_copy.xlsx")
    try:
        shutil.copy2(INPUT_FILE_PATH, temp_file)
        df = pd.read_excel(temp_file, dtype=str)
        
        if df.shape[1] < 2:
            print("[ERROR] The provided Excel file lacks a second column (Column B).")
            os.remove(temp_file)
            sys.exit(1)

        print("[INFO] Extracting IDs from Column B...")
        raw_list = df.iloc[:, 1].tolist()
        
        clean_list = []
        for x in raw_list:
            try:
                s_val = str(x).strip()
                if s_val.lower() == 'nan' or s_val == "": 
                    continue
                if s_val.endswith('.0'): 
                    s_val = s_val[:-2]
                clean_list.append(s_val)
            except Exception:
                continue
        
        print(f"[INFO] Loaded {len(clean_list)} total IDs.")
        del df 
        try: os.remove(temp_file)
        except OSError: pass
        return clean_list
    except Exception as e:
        print(f"[ERROR] Failed to read file: {e}")
        sys.exit(1)

# --- API & DATA PARSING ---

def fetch_details(id_number):
    """Fetches details from the HIS open data API."""
    url = f'https://registries.his.bg/api/V1/outpatientcare/getOutpatientCareByNumberForApiV1?number={id_number}'
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            return None
        else:
            print(f"    [WARN] Server returned status {response.status_code} for ID {id_number}.")
            return None
    except Exception as e:
        print(f"    [ERROR] Network exception for ID {id_number}: {e}")
        return None

def parse_data(records, all_hospitals, all_addresses, all_doctors):
    """Extracts and structures data into Hospitals, Addresses, and Doctors dictionaries."""
    if not isinstance(records, list):
        records = [records]

    for rec in records:
        h_id = rec.get('number')
        if not h_id: continue

        # --- 1. HOSPITALS ---
        owners_list = rec.get('owners', [])
        
        base_hospital_data = {
            'Hospital_ID': h_id,
            'Old_Number': rec.get('oldNumber'),
            'Name': rec.get('name'),
            'Status': rec.get('statuslabel'),
            'Reg_Date': rec.get('registrationDate'),
            'Vid_LZ': rec.get('vid', {}).get('label') if isinstance(rec.get('vid'), dict) else rec.get('vid')
        }

        if owners_list and isinstance(owners_list, list):
            for o in owners_list:
                fn = o.get('firstname', '')
                mn = o.get('middlename', '')
                ln = o.get('lastname', '')
                full_n = f"{fn} {mn} {ln}".strip()
                
                entry = base_hospital_data.copy()
                entry['Managers'] = clean_person_name(full_n)
                all_hospitals.append(entry)
        else:
            entry = base_hospital_data.copy()
            entry['Managers'] = "N/A"
            all_hospitals.append(entry)

        # --- 2. ADDRESSES ---
        addrs = rec.get('address', [])
        if addrs and isinstance(addrs, list):
            for ad in addrs:
                raw_full_addr = ad.get('fulladdress', '')
                clean_addr = clean_bg_address(raw_full_addr)
                
                addr_specs = ad.get('specialities', [])
                addr_spec_str = ", ".join([s.get('label', '') for s in addr_specs]) if addr_specs else ""
                
                addr_acts = ad.get('activities', [])
                addr_act_str = ", ".join([a.get('label', '') for a in addr_acts]) if addr_acts else ""

                addr_entry = {
                    'Hospital_ID': h_id,
                    'Type': ad.get('typeaddresslabel'),
                    'City': ad.get('ekatte'),
                    'Full_Address': raw_full_addr,
                    'Full_Address_Clean': clean_addr,
                    'Address_Specialties': addr_spec_str,
                    'Address_Activities': addr_act_str,
                    'Region': ad.get('district'),
                    'Municipality': ad.get('munincipaliti')
                }
                all_addresses.append(addr_entry)
        else:
            all_addresses.append({
                'Hospital_ID': h_id, 
                'Full_Address': 'N/A', 
                'Full_Address_Clean': 'N/A'
            })

        # --- 3. DOCTORS ---
        staff = rec.get('medicalStaff', [])
        if staff and isinstance(staff, list):
            for doc in staff:
                fname = doc.get('firstname', '')
                mname = doc.get('middlename', '')
                lname = doc.get('lastname', '')
                fullname = f"{fname} {mname} {lname}".strip()

                specs = doc.get('specialities', [])
                spec_str = ", ".join([s.get('label', '') for s in specs]) if specs else ""

                doc_entry = {
                    'Hospital_ID': h_id,
                    'Doctor_Name': clean_person_name(fullname),
                    'Type': doc.get('typelabel'),
                    'Specialty': spec_str
                }
                all_doctors.append(doc_entry)
        else:
            all_doctors.append({'Hospital_ID': h_id, 'Doctor_Name': 'N/A'})

def save_multisheet_excel(hospitals, addresses, doctors):
    """Exports structured data to an Excel file with multiple sheets."""
    try:
        df_h = pd.DataFrame(hospitals)
        df_a = pd.DataFrame(addresses)
        df_d = pd.DataFrame(doctors)

        with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
            df_h.to_excel(writer, sheet_name='Hospitals', index=False)
            df_a.to_excel(writer, sheet_name='Addresses', index=False)
            df_d.to_excel(writer, sheet_name='Doctors', index=False)
        print(f"[INFO] Successfully saved batch data to: {OUTPUT_FILE}")
    except Exception as e:
        print(f"[CRITICAL ERROR] Failed to save Excel output: {e}")

# --- MAIN EXECUTION ---

def main_loop():
    all_ids = load_ids_from_col_b()
    processed_ids = get_processed_ids()
    print(f"[INFO] History loaded. Total previously processed records: {len(processed_ids)}")

    pending_ids = [x for x in all_ids if x not in processed_ids]
    total_pending = len(pending_ids)
    
    if total_pending == 0:
        print("[INFO] Processing complete. No pending records found.")
        return

    print(f"[INFO] --- STARTING BATCH PROCESSING (Targets Pending: {total_pending}) ---")
    
    all_hospitals = []
    all_addresses = []
    all_doctors = []
    
    for i, id_number in enumerate(pending_ids):
        elapsed = time.time() - START_TIME
        if elapsed > MAX_RUNTIME_SECONDS:
            print("\n[WARN] Execution time limit reached. Initiating graceful shutdown sequence.")
            with open(CONTINUE_FLAG_FILE, 'w') as f:
                f.write("CONTINUE_REQUIRED")
            break 

        percent_done = ((i + 1) / total_pending) * 100
        print(f"[{i+1}/{total_pending}] >> {percent_done:.2f}% << Processing ID: {id_number}...")
        
        data = fetch_details(id_number)
        
        if data:
            parse_data(data, all_hospitals, all_addresses, all_doctors)
            save_processed_id(id_number)
            print(f"    [+] Successfully processed.")
        else:
            # Marking as processed to prevent infinite retry loops on dead links
            save_processed_id(id_number)
            print(f"    [-] Skipped or unavailable.")
        
        time.sleep(random.uniform(0.5, 1.2))

    if all_hospitals:
        print("[INFO] Finalizing and exporting extracted records to Excel...")
        save_multisheet_excel(all_hospitals, all_addresses, all_doctors)
    else:
        print("[INFO] No valid data extracted during this run.")

if __name__ == "__main__":
    main_loop()
