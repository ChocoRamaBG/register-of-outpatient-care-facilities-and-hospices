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
INPUT_FILENAME = "BG_Medical_Registry_FULL.xlsx" 
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

# --- THE CHAINSAW CLEANER V6 (Dotless Edition) ---
def clean_bg_address(raw_addr):
    if not raw_addr or not isinstance(raw_addr, str):
        return ""
    
    # Brainrot sanitization protocols active
    clean = raw_addr.replace('№', ' ').replace('"', '').replace('„', '').replace('“', '').replace("'", "").replace("`", "")
    clean = re.sub(r'\(.*?\)', '', clean)
    clean = re.sub(r'/.*?/', '', clean)

    clean = re.sub(r'Обл\.\s*[^,]+,?','', clean, flags=re.IGNORECASE)
    clean = re.sub(r'област\s*[^,]+,?','', clean, flags=re.IGNORECASE)
    clean = re.sub(r'общ\.\s*[^,]+,?','', clean, flags=re.IGNORECASE)
    clean = re.sub(r'община\s*[^,]+,?','', clean, flags=re.IGNORECASE)

    cutoff_pattern = r'[,\s]+(ет\.|етаж|ап\.|апартамент|каб\.|кабинет|стая|офис|помещение|маг\.|магазин|обект|партер|сутерен|поликлиника|здравна служба|болница).*$'
    clean = re.sub(cutoff_pattern, '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'УПИ\s*[0-9XIV-]+', '', clean, flags=re.IGNORECASE)
    
    clean = re.sub(r'\s+,', ',', clean)
    clean = re.sub(r',+', ',', clean)
    clean = re.sub(r'\s+', ' ', clean)
    
    return clean.strip(', .')

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

        # --- 1. HOSPITALS ---
        owners_list = rec.get('owners', [])
        managers_str = ""
        if owners_list and isinstance(owners_list, list):
            mgr_names = []
            for o in owners_list:
                fn = o.get('firstname', '')
                mn = o.get('middlename', '')
                ln = o.get('lastname', '')
                full_n = f"{fn} {mn} {ln}".strip()
                if full_n: mgr_names.append(full_n)
            managers_str = "; ".join(mgr_names)

        hospital_entry = {
            'Hospital_ID': h_id,
            'Old_Number': rec.get('oldNumber'),
            'Name': rec.get('name'),
            'Managers': managers_str,
            'Status': rec.get('statuslabel'),
            'Reg_Date': rec.get('registrationDate'),
            'Vid_LZ': rec.get('vid', {}).get('label') if isinstance(rec.get('vid'), dict) else rec.get('vid')
        }
        all_hospitals.append(hospital_entry)

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
