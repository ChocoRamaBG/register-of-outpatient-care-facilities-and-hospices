import requests
import pandas as pd
import os
import time
import random
import sys
import shutil
import re
from datetime import datetime

# --- CONFIGURATION ---
try:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    SCRIPT_DIR = os.getcwd()

# Input file name (must be in the same directory as the script)
INPUT_FILENAME = "BG_Medical_Registry_FULL.xlsx"
INPUT_FILE_PATH = os.path.join(SCRIPT_DIR, INPUT_FILENAME)

# Log file for processed IDs (enables pausing and resuming the script)
PROCESSED_LOG_FILE = os.path.join(SCRIPT_DIR, "processed_ids.txt")

# Flag file to trigger GitHub Actions loop
CONTINUE_FLAG_FILE = os.path.join(SCRIPT_DIR, "CONTINUE_FLAG")

# Output file name (will be continuously updated and tracked by Git)
OUTPUT_FILE = os.path.join(SCRIPT_DIR, 'FINAL_DOCTORS_DATA.xlsx')

MAX_RUNTIME_SECONDS = 20400  # 5 hours and 40 minutes limit
START_TIME = time.time()

headers = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9,bg;q=0.8',
    'origin': 'https://opendata.his.bg',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0'
}

# --- DATA CLEANING FUNCTIONS ---
def clean_bg_address(raw_addr):
    if not isinstance(raw_addr, str) or not raw_addr:
        return ""
    
    # 0. Filter invalid records (Metadata indicators)
    invalid_indicators = ["ЗАЛИЧЕН", "ЗАКРИТ", "НЕ СЪЩЕСТВУВА", "НЯМА ДАННИ", "ПРИЗЕМЕН", "СУТЕРЕН", "ПОЛИКЛИНИКА", "ЗДРАВНА СЛУЖБА", "СЗС", "СЗУ", "ФЗП", "АПЗЗ"]
    if len(raw_addr) < 20 and any(x in raw_addr.upper() for x in invalid_indicators):
        return ""

    # 1. Standardize symbols (Remove quotation marks and clean formatting)
    clean = raw_addr
    quotes = ['"', "'", '„', '“', '”', '’', '`']
    for q in quotes:
        clean = clean.replace(q, '')
        
    clean = clean.replace('№', ' ').replace(' N ', ' ').replace(' No ', ' ').replace('номер', ' ')
    
    # Correct Roman numerals and excessive spacing
    clean = re.sub(r'\s+', ' ', clean)
    
    # 2. Remove administrative prefixes
    clean = re.sub(r'Обл\.\s*[^,;]+[,;]?', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'област\s*[^,;]+[,;]?', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'Общ\.\s*[^,;]+[,;]?', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'община\s*[^,;]+[,;]?', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'Район\s*[^,;]+[,;]?', '', clean, flags=re.IGNORECASE) 
    clean = re.sub(r'р-н\s*[^,;]+[,;]?', '', clean, flags=re.IGNORECASE)
    
    # Remove leading numbering (e.g., "1. София...")
    clean = re.sub(r'^\s*\d+[\.,]\s*', '', clean)
    
    # Remove Cadastral identifiers without truncating the string
    clean = re.sub(r'(УПИ|ПИ|идентификатор|парцел|кв\.|квартал)\s*[IVX0-9\-\.]+', '', clean, flags=re.IGNORECASE)

    # 3. Structural cutoff words
    stop_words = [
        r'ет\.', r'етаж', r'ет\s', r'е\.', r'ниво', r'Е-', 
        r'ап\.', r'апартамент', r'ап\s', r'ап\d', r'ателие', r'ат\.', r'АП\.', r'А-',
        r'каб\.', r'кабинет', r'к-т', r'к\.\s*\d', r'к\d+', r'К-', 
        r'амб\.', r'амбулатория', r'амб\s',
        r'стая', r'ст\s*\d', r'ст\.\s*\d', 
        r'офис', r'оф\.', 
        r'помещение', r'зала', r'хале', r'салон', 'мазе',
        r'маг\.', r'магазин', r'обект', 
        r'пав\.', r'павилион', r'барака', r'бунгало', 'фургон', 'контейнер', 'каравана',
        r'партер', r'сутерен', r'приземен', r'кота', 'полуетаж', 'подблоково',
        r'вх\.', r'вход', r'вх\s', r'В-',
        r'крило', r'сектор', r'тяло', r'корпус', r'база', 
        r'щанд', r'гараж', r'трафопост', 'трафо', 'абонатна', 'котелно', 'парно',
        r'манипулационна', r'манип\.', r'ман\.', r'приемно', r'регистратура', 
        r'отделение', r'клиника', r'катедра',
        r'ЛК', r'РК', 
        r'ЗП', r'З\.П\.', 'ЛАБ\.', 'ДЕТ\.', 'КОНС\.'
    ]
    
    pattern_str = r'([,\s\(\.\/-]+)(' + '|'.join(stop_words) + r').*$'
    
    clean = re.sub(pattern_str, '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'\(.*?\)', '', clean)
    
    noise_words = [
        r'МБАЛ', r'УМБАЛ', r'СБАЛ', r'ДКЦ', r'МЦ\s', r'МЦ-', r'МДЦ', r'АИПП', r'СМДЛ', 'МСЦ', 'ДМСГД',
        r'ЕООД', r'ООД', r'ЕАД', r'АД\s', 'ЕТ\s', 'ЕГ\s',
        r'ТПК', 'ЗС', 'СЗС', 'СЗУ', 'ФЗП', 'ФСМП', 'ЦСМП', 'АПЗЗ', 'СБР', 'РБ', 'ВМБ', 'ФСП', 'ФЗУ',
        r'ЦПЗ', r'КОЦ', r'ТЕЛК', r'РЗИ', r'ХЕИ', r'ОСП', 'ТДКЦ', 'ОДПФЗС', 'НМТБ', 'ДУБ', 'ДОЗ', 'ОАПС',
        r'БАН', 'НСА', 'МВР', 'МО', 'БДЖ', 'ВиК', 'БТК', 'ТПК', 'ДЗИ', 'ДАП', 'АПК', 'ТКЗС',
        r'Търговски център', r'ТЦ\s', r'Т\.Ц\.', r'Мол\s', r'Mall', r'Бизнес център', r'БЦ\s',
        r'Ритейл', r'Аптека', r'Оптика', r'Дрогерия', r'супермаркет',
        r'ТЕЦ', r'ВЕЦ', r'АЕЦ', r'Електроцентрала', r'ЗПЗ', r'СПЗ', r'НПЗ', r'ЮПЗ', r'ПЗ\s',
        r'МК\s', 
    ]
    
    clean = re.sub(r'\b(' + '|'.join(noise_words) + r')\b', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'\s+', ' ', clean)      
    clean = re.sub(r'\s,', ',', clean)      
    clean = re.sub(r',+', ',', clean)       
    
    clean = clean.strip(' ,.-/\\')
    
    if len(clean) < 3:
        if "гр." in raw_addr or "с." in raw_addr:
             city_match = re.search(r'(гр\.|с\.)\s*([А-Яа-я\s\-]+)', raw_addr)
             if city_match:
                 return city_match.group(0)
        return ""

    return clean

def get_processed_ids():
    if not os.path.exists(PROCESSED_LOG_FILE):
        return set()
    with open(PROCESSED_LOG_FILE, 'r', encoding='utf-8') as f:
        return set(line.strip() for line in f if line.strip())

def save_processed_id(id_val):
    with open(PROCESSED_LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{id_val}\n")

def load_ids_from_col_b():
    print(f"Targeting input file: {INPUT_FILE_PATH}")
    if not os.path.exists(INPUT_FILE_PATH):
        print("Error: Input file not found in the specified directory.")
        sys.exit(1)
    
    temp_file = os.path.join(SCRIPT_DIR, "temp_processing_copy.xlsx")
    try:
        shutil.copy2(INPUT_FILE_PATH, temp_file)
        df = pd.read_excel(temp_file, dtype=str)
        
        if df.shape[1] < 2:
            print("Error: The file does not contain a second column (Column B).")
            os.remove(temp_file)
            sys.exit(1)

        print(">>> Extracting IDs from Column B...")
        raw_list = df.iloc[:, 1].tolist()
        
        clean_list = []
        for x in raw_list:
            try:
                s_val = str(x).strip()
                if s_val.lower() == 'nan' or s_val == "": continue
                if s_val.endswith('.0'): s_val = s_val[:-2]
                clean_list.append(s_val)
            except: continue
        
        print(f"Loaded {len(clean_list)} total IDs.")
        del df 
        try: os.remove(temp_file)
        except: pass
        return clean_list
    except Exception as e:
        print(f"Failed to read file: {e}")
        sys.exit(1)

def fetch_details(id_number):
    url = f'https://registries.his.bg/api/V1/outpatientcare/getOutpatientCareByNumberForApiV1?number={id_number}'
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            return None
        else:
            print(f"    [!] Received HTTP {response.status_code} for ID {id_number}.")
            return None
    except Exception as e:
        print(f"    [!] Network error encountered for ID {id_number}: {e}")
        return None

def parse_data(records, all_hospitals, all_addresses, all_doctors):
    if not isinstance(records, list):
        records = [records]

    for rec in records:
        h_id = rec.get('number')
        if not h_id: continue

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
                entry['Managers'] = full_n
                all_hospitals.append(entry)
        else:
            entry = base_hospital_data.copy()
            entry['Managers'] = "N/A"
            all_hospitals.append(entry)

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
                    'Doctor_Name': fullname,
                    'Type': doc.get('typelabel'),
                    'Specialty': spec_str
                }
                all_doctors.append(doc_entry)
        else:
            all_doctors.append({'Hospital_ID': h_id, 'Doctor_Name': 'N/A'})

def save_multisheet_excel(hospitals, addresses, doctors):
    try:
        df_h = pd.DataFrame(hospitals)
        df_a = pd.DataFrame(addresses)
        df_d = pd.DataFrame(doctors)

        with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
            df_h.to_excel(writer, sheet_name='Hospitals', index=False)
            df_a.to_excel(writer, sheet_name='Addresses', index=False)
            df_d.to_excel(writer, sheet_name='Doctors', index=False)
    except Exception as e:
        print(f"!!! CRITICAL: Failed to save Excel file: {e}")

def main_loop():
    # Remove old flag if present
    if os.path.exists(CONTINUE_FLAG_FILE):
        os.remove(CONTINUE_FLAG_FILE)

    # 1. Load targets
    all_ids = load_ids_from_col_b()
    
    # 2. Load previously processed IDs
    processed_ids = get_processed_ids()
    print(f"Found {len(processed_ids)} previously processed IDs.")

    # 3. Filter pending list
    pending_ids = [x for x in all_ids if x not in processed_ids]
    total_pending = len(pending_ids)
    
    if total_pending == 0:
        print("All IDs have been processed successfully. Exiting.")
        return

    print(f"--- STARTING PROCESSING BATCH (Remaining targets: {total_pending}) ---")
    
    all_hospitals = []
    all_addresses = []
    all_doctors = []
    
    for i, id_number in enumerate(pending_ids):
        # Enforce execution time limit to allow graceful workflow re-trigger
        elapsed = time.time() - START_TIME
        if elapsed > MAX_RUNTIME_SECONDS:
            print("\n[WARN] Execution time limit reached. Initiating graceful shutdown sequence.")
            with open(CONTINUE_FLAG_FILE, 'w') as f:
                f.write("CONTINUE_REQUIRED")
            
            if all_hospitals:
                save_multisheet_excel(all_hospitals, all_addresses, all_doctors)
            
            sys.exit(0)

        percent_done = ((i + 1) / total_pending) * 100
        print(f"[{i+1}/{total_pending}] >> {percent_done:.2f}% << Processing ID: {id_number}...")
        
        data = fetch_details(id_number)
        
        if data:
            parse_data(data, all_hospitals, all_addresses, all_doctors)
            save_processed_id(id_number)
            print(f"    [+] Data acquired successfully.")
            
            # Save optimization: Save periodically rather than on every single loop
            if (i + 1) % 50 == 0:
                save_multisheet_excel(all_hospitals, all_addresses, all_doctors)
        else:
            save_processed_id(id_number)
            print(f"    [-] Record skipped.")
        
        sleep_time = random.uniform(0.3, 0.8)
        time.sleep(sleep_time)

    print("--- EXECUTION COMPLETE ---")
    if all_hospitals:
        save_multisheet_excel(all_hospitals, all_addresses, all_doctors)
    print(f"Data processing finished successfully. Output file saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main_loop()
