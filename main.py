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
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILENAME = "BG_Medical_Registry_Remaining.xlsx" 
INPUT_FILE_PATH = os.path.join(SCRIPT_DIR, INPUT_FILENAME)
PROCESSED_LOG_FILE = os.path.join(SCRIPT_DIR, "processed_ids.txt") # Тук ще пазим ID-тата на готовите пациентчовци
CONTINUE_FLAG_FILE = "CONTINUE_FLAG" # Флагче за рестарт

# Safety margin: GitHub kills at 6h. We stop at 5h 40m just to be safe.
# 5 hours * 3600 + 40 mins * 60 = 18000 + 2400 = 20400 seconds.
MAX_RUNTIME_SECONDS = 20400 
START_TIME = time.time()

# Output file (Dynamic naming to avoid overwriting)
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_FILE = os.path.join(SCRIPT_DIR, f'FINAL_DOCTORS_BATCH_{TIMESTAMP}.xlsx')

headers = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9,bg;q=0.8',
    'origin': 'https://opendata.his.bg',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0'
}

# --- THE SINGULARITY CLEANER V16 (INTEGRATED & ENRICHED) ---
def clean_bg_address(raw_addr):
    if not isinstance(raw_addr, str) or not raw_addr:
        return ""
    
    # 0. INSTANT KILL (Metadata brainrot)
    # Ако адресът съдържа тези думи и е твърде къс, значи е просто статус, а не локация.
    brainrot_indicators = ["ЗАЛИЧЕН", "ЗАКРИТ", "НЕ СЪЩЕСТВУВА", "НЯМА ДАННИ", "ПРИЗЕМЕН", "СУТЕРЕН", "ПОЛИКЛИНИКА", "ЗДРАВНА СЛУЖБА", "СЗС"]
    # Ако целият стринг е само "Здравна служба" или подобно, нямаме адрес.
    if len(raw_addr) < 25 and any(x in raw_addr.upper() for x in brainrot_indicators):
        return "INVALID_ADDRESS_METADATA"

    # 1. STANDARDIZE SYMBOLS (Sigma Cleanup)
    # Махаме №, думи като "номер", кавички и всякакви странни скоби
    clean = raw_addr.replace('№', ' ').replace(' N ', ' ').replace(' No ', ' ').replace('номер', ' ')
    clean = clean.replace('"', '').replace('„', '').replace('“', '').replace("'", "").replace("`", "")
    
    # Оправяне на римски цифри и интервали (напр. "VI - ТИ" -> "6", "ет. 2" -> "ет.2")
    clean = re.sub(r'\s+', ' ', clean)
    
    # 2. REMOVE ADMINISTRATIVE PREFIXES (Admin junk)
    # Махаме "Обл.", "община" - те само бъркат Google Maps.
    clean = re.sub(r'Обл\.\s*[^,;]+[,;]?', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'област\s*[^,;]+[,;]?', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'Общ\.\s*[^,;]+[,;]?', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'община\s*[^,;]+[,;]?', '', clean, flags=re.IGNORECASE)
    
    # Remove leading numbering (e.g. "1. София...")
    clean = re.sub(r'^\s*\d+[\.,]\s*', '', clean)

    # 3. THE KILL LIST V16 (Updated with industrial, short-form horrors, and enrichments)
    # Това е списъкът на Страшния съд. Срещне ли дума от тук след разделител - реже всичко след нея.
    stop_words = [
        # --- СГРАДЕН ФОНД & ЛОКАЦИЯ ---
        r'ет\.', r'етаж', r'ет\s', r'е\.', r'ниво', r'Е-', # Е-1 style
        r'ап\.', r'апартамент', r'ап\s', r'ап\d', r'ателие', r'ат\.', r'АП\.',
        r'каб\.', r'кабинет', r'к-т', r'к\.\s*\d', r'к\d+', r'К-', # К-1 style
        r'амб\.', r'амбулатория', r'амб\s',
        r'стая', r'ст\.', r'ст\d',
        r'офис', r'оф\.', 
        r'помещение', r'зала', r'хале', r'салон', r'склад', 'мазе',
        r'маг\.', r'магазин', r'обект', 
        r'пав\.', r'павилион', r'барака', r'бунгало', 'фургон', 'контейнер', 'каравана',
        r'партер', r'сутерен', r'приземен', r'кота', 'полуетаж', 'подблоково',
        r'вх\.', r'вход', r'вх\s', r'В-', # В-А style
        r'крило', r'сектор', r'тяло', r'корпус', r'база', r'Б:', # База: ...
        r'блок\s+[А-Яа-я]', r'бл\.', r'бл\s', r'б\.', # Block variants
        r'щанд', r'гараж', r'трафопост',
        
        # --- МЕДИЦИНСКИ ИНСТИТУЦИИ (Hell Level abbreviations) ---
        r'ДКЦ', r'МБАЛ', r'УМБАЛ', r'СБАЛ', r'МЦ\s', r'МЦ-', r'МДЦ', r'АИПП', r'СМДЛ', 'МСЦ', 'ДМСГД',
        r'Поликлиника', r'п-ка', r'Здравна служба', r'Здравен дом', r'Здравен участък', r'Здраве',
        r'СЗС', r'СЗУ', r'ФЗП', r'ФСМП', r'ЦСМП', 'АПЗЗ', 'СБР', 'ДП', 'ОБ', 'РБ', 'ВМБ',
        r'Болница', r'Диспансер', r'Лаборатория', 'Микробиология', 'Рентген', 'Хематология', 'Хистология',
        r'Филиал', r'Ф\.', r'Ф:', # Филиал:
        r'ЦПЗ', r'КОЦ', r'ФДМ', r'ДЦ', r'ТЕЛК', r'РЗИ', r'ХЕИ', r'ОСП', 'ТДКЦ', 'ОДПФЗС',
        r'ВМА', 'МБАБ', 'СБАЛО', 'СБАЛАГ', 'ОДПФЗС', 'УПМБАЛ', 'СБДПЛР', 'ЦКВБ', 'ЦКВЗ',
        r'Медицински център', r'Дентален център', r'Болнична', r'Спешна помощ',
        r'манипулационна', r'манип\.', r'приемно', r'регистратура', r'център за', r'звено',
        r'отделение', r'клиника', r'катедра', r'\bЗС\b', 
        r'СХБАЛ', 'СБАЛББ', 'МДЛ', 'СМЛ', 'ЛЗУ', 'ДДМУИ', 'ПФДПО', 'ОДОЗС',
        r'РСП', 'ДПО', 'МТЛ', 'ЦНИКА', 'СБХЛ', 'ОМЦ', 'САГБАЛ', 'УСБАЛЕ', 'ГПСМП', 'АМЦСМП', 'ГППМП', 'АИСМП', 'ИПСМП', 'КЦА',
        r'ЛК', r'РК', # Лекарски кабинет, Рентгенов кабинет
        
        # --- ОБРАЗОВАНИЕ, АДМИНИСТРАЦИЯ И БИЗНЕС ---
        r'кметство', r'община\s', r'съвет', r'читалище', r'поща', r'съдебна палата',
        r'училище', r'ОУ\s', r'СУ\s', r'ЕГ\s', r'ПГ\s', r'СОУ\s', r'СПТУ', 'ПТУ', 'НУ\s', 'ДГ',
        r'гимназия', 'колеж', 'университет', 'факултет', 'институт', 'академия', 'ПФК', 'НСА', 'БАН',
        r'детска градина', r'ОДЗ', r'ясла', r'дом за', r'пансион', r'общежитие',
        r'стадион', r'автогара', r'жп гара', r'гара', r'летище', 'терминал',
        r'завод', 'цех', 'фабрика', 'предприятие', 'комбинат', 'миби', 'рудник',
        r'АД\s', r'ЕООД', r'ООД', r'ЕАД', r'ЕТ\s', 'КД', 'СД', # Фирми
        r'ООС', r'ДСК', 'МВР', 'БДЖ', 'ВиК', 'БТК', 'ТПК', 'ДЗИ', 'ДАП', 'АПК', 'ТКЗС', 'ПК',
        r'Търговски център', r'ТЦ\s', r'Т\.Ц\.', r'Мол\s', r'Mall', r'Бизнес център', r'БЦ\s',
        r'Ритейл', r'Аптека', r'Оптика', r'Дрогерия', r'супермаркет',
        r'ТЕЦ', r'ВЕЦ', r'АЕЦ', r'Електроцентрала', r'ЗПЗ', r'СПЗ', r'НПЗ', r'ЮПЗ', r'ПЗ\s',
        
        # --- ТУРИЗЪМ ---
        r'хотел', r'х-л', r'комплекс', r'резорт', r'resort', 'вила', 'вили',
        r'ваканционно', r'къмпинг', r'хижа', r'санаториум', r'балнео', 'СПА', 'SPA',
        r'к\.к\.', r'к\.к', r'курортен комплекс', 'ваканционен',
        r'ж\.г\.', r'жилищна група', r'в\.з\.', r'вилна зона', 
        r'местност', r'м-ст',
        r'стопански двор', r'к-с',
        
        # --- CONNECTORS & BRAINROT ---
        r'в сградата', r'сграда', r'бивш', r'бивша', r'бивше', 'бившо', 'старата',
        r'срещу', r'до бл\.', r'до вх\.', r'зад ', r'на територията', 
        r'продължение', r'разширение', r'до ', r'между', r'под ', r'на ъгъла', r'на гърба',
        r'УПИ', r'ПИ\s', r'идентификатор', r'АОС', 'имот', 'кв\.', 'квартал \d', 'парцел', 'П-Л',
        r'адрес 2', r'2-ри', r'3-ти', r'р-н',
        r'Р\.П\.', r'УЧ-ЩЕ'
    ]
    
    # Regex Magic: (разделител) + (стоп дума) + (всичко до края)
    pattern_str = r'([,\s\(\.\/-]+)(' + '|'.join(stop_words) + r').*$'
    
    # 1. Clean using Stop Words
    clean = re.sub(pattern_str, '', clean, flags=re.IGNORECASE)

    # 2. Additional cleanup for things inside parentheses if they survived
    clean = re.sub(r'\(.*?\)', '', clean)
    clean = re.sub(r'/.*?/', '', clean)   
    
    # 3. Specific Brainrot Fixes
    clean = re.sub(r'\bномер\b', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'\bс\.\s*$', '', clean) # Ако завършва на "с." без име
    clean = re.sub(r'\bул\.\s*$', '', clean) # Ако завършва на "ул." без име
    
    # 4. Final Polish
    clean = re.sub(r'\s+', ' ', clean)      # Двойни интервали -> единичен
    clean = re.sub(r'\s,', ',', clean)      # Интервал преди запетая
    clean = re.sub(r',+', ',', clean)       # Двойни запетаи
    
    # Махаме точки, запетаи и тирета от края и началото
    clean = clean.strip(' ,.-/\\')
    
    # 5. Sanity Check (Da ne se izlojim pred chujdencite)
    # Ако сме изтрили всичко (напр. адресът е бил само "АПЗЗ"), връщаме оригиналния или грешка
    if len(clean) < 3:
        # Check if original had city info
        if "гр." in raw_addr or "с." in raw_addr:
             # Try to extract just the city/village name as a last resort
             city_match = re.search(r'(гр\.|с\.)\s*([А-Яа-я\s\-]+)', raw_addr)
             if city_match:
                 return city_match.group(0)
        return "INVALID_ADDRESS_TOO_SHORT"

    return clean

def get_processed_ids():
    """Reads the list of ID-chovtsi we already destroyed."""
    if not os.path.exists(PROCESSED_LOG_FILE):
        return set()
    with open(PROCESSED_LOG_FILE, 'r', encoding='utf-8') as f:
        # Reading lines like a Sigma reader
        return set(line.strip() for line in f if line.strip())

def save_processed_id(id_val):
    """Appends a completed ID to the log file immediately."""
    with open(PROCESSED_LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{id_val}\n")

def load_ids_from_col_b():
    print(f"Yo shefe, targeting: {INPUT_FILE_PATH}")
    if not os.path.exists(INPUT_FILE_PATH):
        print("Faila go nyama. Slagay go pri skripta, lyolyo.")
        sys.exit(1)
    
    # Reading excel... hope your RAM has enough rizz
    temp_file = os.path.join(SCRIPT_DIR, "temp_brainrot_copy.xlsx")
    try:
        shutil.copy2(INPUT_FILE_PATH, temp_file)
        df = pd.read_excel(temp_file, dtype=str)
        
        if df.shape[1] < 2:
            print("!!! GRESHKA: Tozi fail nyama Kolona B. Negative IQ moment.")
            os.remove(temp_file)
            sys.exit(1)

        print(">>> Grabbing IDs from COLUMN B...")
        raw_list = df.iloc[:, 1].tolist()
        
        clean_list = []
        for x in raw_list:
            try:
                s_val = str(x).strip()
                if s_val.lower() == 'nan' or s_val == "": continue
                if s_val.endswith('.0'): s_val = s_val[:-2]
                clean_list.append(s_val)
            except: continue
        
        print(f"Loaded {len(clean_list)} total ID-chovtsi.")
        del df 
        try: os.remove(temp_file)
        except: pass
        return clean_list
    except Exception as e:
        print(f"Failed to read file: {e}")
        sys.exit(1)

def fetch_details(id_number):
    # API endpoint goes brrr
    url = f'https://registries.his.bg/api/V1/outpatientcare/getOutpatientCareByNumberForApiV1?number={id_number}'
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            return None
        else:
            print(f"    [!] Error {response.status_code} for ID {id_number}.")
            return None
    except Exception as e:
        print(f"    [!] Network died (Skill Issue) on {id_number}: {e}")
        return None

def parse_data(records, all_hospitals, all_addresses, all_doctors):
    if not isinstance(records, list):
        records = [records]

    for rec in records:
        h_id = rec.get('number')
        if not h_id: continue

        # --- 1. HOSPITALS (MODIFIED LOGIC: One row per manager) ---
        # Mamka mu choveche, here we split the managers into multiple rows
        owners_list = rec.get('owners', [])
        
        # Prepare the base data that repeats for every manager row
        base_hospital_data = {
            'Hospital_ID': h_id,
            'Old_Number': rec.get('oldNumber'),
            'Name': rec.get('name'),
            # Manager comes later
            'Status': rec.get('statuslabel'),
            'Reg_Date': rec.get('registrationDate'),
            'Vid_LZ': rec.get('vid', {}).get('label') if isinstance(rec.get('vid'), dict) else rec.get('vid')
        }

        if owners_list and isinstance(owners_list, list):
            # If we have multiple boss-chovtsi, we make multiple rows
            for o in owners_list:
                fn = o.get('firstname', '')
                mn = o.get('middlename', '')
                ln = o.get('lastname', '')
                full_n = f"{fn} {mn} {ln}".strip()
                
                # Clone the dict so we don't mess up references (no Ohio bugs allowed)
                entry = base_hospital_data.copy()
                entry['Managers'] = full_n
                all_hospitals.append(entry)
        else:
            # No managers? Still add the hospital but with empty manager field
            entry = base_hospital_data.copy()
            entry['Managers'] = "N/A"
            all_hospitals.append(entry)

        # --- 2. ADDRESSES ---
        addrs = rec.get('address', [])
        if addrs and isinstance(addrs, list):
            for ad in addrs:
                raw_full_addr = ad.get('fulladdress', '')
                # Apply the V16 Singularity Cleaner here!
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
        print(f"SAVED BATCH: {OUTPUT_FILE}")
    except Exception as e:
        print(f"!!! CRITICAL: Failed to save Excel: {e}")

def main_loop():
    # 1. Load targets
    all_ids = load_ids_from_col_b()
    
    # 2. Load already done IDs
    processed_ids = get_processed_ids()
    print(f"History check: We have already roasted {len(processed_ids)} ID-chovtsi.")

    # 3. Filter list
    pending_ids = [x for x in all_ids if x not in processed_ids]
    total_pending = len(pending_ids)
    
    if total_pending == 0:
        print("Nothing left to do. Ez clap. GG WP.")
        return

    print(f"--- STARTING BATCH (Targets Left: {total_pending}) ---")
    
    all_hospitals = []
    all_addresses = []
    all_doctors = []
    
    batch_counter = 0
    save_interval = 100 # Optional: intermediate in-memory flush if needed, but we rely on huge batch at end or timeout
    
    for i, id_number in enumerate(pending_ids):
        # --- TIME CHECK ---
        elapsed = time.time() - START_TIME
        if elapsed > MAX_RUNTIME_SECONDS:
            print("\n!!! TIME LIMIT REACHED !!!")
            print("Initiating emergency save protocol. Skibidi bop mm dada.")
            
            # Create a flag file to tell GitHub to restart
            with open(CONTINUE_FLAG_FILE, 'w') as f:
                f.write("MORE_BLOOD")
            
            break # Break the loop to save and exit

        # --- LOGIC ---
        percent_done = ((i + 1) / total_pending) * 100
        print(f"[{i+1}/{total_pending}] >> {percent_done:.2f}% << Processing: {id_number}...")
        
        data = fetch_details(id_number)
        
        if data:
            parse_data(data, all_hospitals, all_addresses, all_doctors)
            # Log as done only after parsing
            save_processed_id(id_number)
            batch_counter += 1
            print(f"    [+] Data Acquired.")
        else:
            # Even if 404 or Error, mark as processed so we don't retry forever
            # Or maybe you want to retry? Assuming we skip bad ones:
            save_processed_id(id_number)
            print(f"    [-] Skipped.")
        
        # Sleep to avoid WAF ban-chovtsi
        sleep_time = random.uniform(0.5, 1.2)
        time.sleep(sleep_time)

    # --- FINAL SAVE FOR THIS RUN ---
    if all_hospitals:
        print("Saving harvested soul-chovtsi to Excel...")
        save_multisheet_excel(all_hospitals, all_addresses, all_doctors)
    else:
        print("No valid data found in this batch. L.")

if __name__ == "__main__":
    main_loop()
