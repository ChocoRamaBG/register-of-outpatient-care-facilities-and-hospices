import requests
import pandas as pd
import os
import time
import random
import sys
import re
import json
import hashlib

# --- CONFIGURATION ---
try:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    SCRIPT_DIR = os.getcwd()

OUTPUT_DIR = os.path.join(SCRIPT_DIR, "hisbg_outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Log file for processed IDs and their state hashes (format: ID|HASH)
PROCESSED_LOG_FILE = os.path.join(OUTPUT_DIR, "hisbg_processed_ids.txt")

# Flag file to trigger GitHub Actions continuation loop if time limit is hit
CONTINUE_FLAG_FILE = os.path.join(OUTPUT_DIR, "CONTINUE_FLAG_HISBG")

# Output Excel file
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "HISBG_FINAL_DOCTORS_DATA.xlsx")

MAX_RUNTIME_SECONDS = 20400  # 5 hours and 40 minutes limit
START_TIME = time.time()

headers = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9,bg;q=0.8",
    "origin": "https://opendata.his.bg",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0"
}


# --- DATA CLEANING FUNCTIONS ---
def clean_bg_address(raw_addr):
    if not isinstance(raw_addr, str) or not raw_addr:
        return ""

    invalid_indicators = [
        "ЗАЛИЧЕН", "ЗАКРИТ", "НЕ СЪЩЕСТВУВА", "НЯМА ДАННИ",
        "ПРИЗЕМЕН", "СУТЕРЕН", "ПОЛИКЛИНИКА", "ЗДРАВНА СЛУЖБА",
        "СЗС", "СЗУ", "ФЗП", "АПЗЗ"
    ]
    if len(raw_addr) < 20 and any(x in raw_addr.upper() for x in invalid_indicators):
        return ""

    clean = raw_addr
    quotes = ['"', "'", '„', '“', '”', '’', '`']
    for q in quotes:
        clean = clean.replace(q, "")

    clean = clean.replace("№", " ").replace(" N ", " ").replace(" No ", " ").replace("номер", " ")
    clean = re.sub(r"\s+", " ", clean)

    clean = re.sub(r"Обл\.\s*[^,;]+[,;]?", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"област\s*[^,;]+[,;]?", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"Общ\.\s*[^,;]+[,;]?", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"община\s*[^,;]+[,;]?", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"Район\s*[^,;]+[,;]?", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"р-н\s*[^,;]+[,;]?", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"^\s*\d+[\.,]\s*", "", clean)
    clean = re.sub(r"(УПИ|ПИ|идентификатор|парцел|кв\.|квартал)\s*[IVX0-9\-\.]+", "", clean, flags=re.IGNORECASE)

    stop_words = [
        r"ет\.", r"етаж", r"ет\s", r"е\.", r"ниво", r"Е-",
        r"ап\.", r"апартамент", r"ап\s", r"ап\d", r"ателие", r"ат\.", r"АП\.", r"А-",
        r"каб\.", r"кабинет", r"к-т", r"к\.\s*\d", r"к\d+", r"К-",
        r"амб\.", r"амбулатория", r"амб\s",
        r"стая", r"ст\s*\d", r"ст\.\s*\d",
        r"офис", r"оф\.",
        r"помещение", r"зала", r"хале", r"салон", "мазе",
        r"маг\.", r"магазин", r"обект",
        r"пав\.", r"павилион", r"барака", r"бунгало", "фургон", "контейнер", "каравана",
        r"партер", r"сутерен", r"приземен", r"кота", "полуетаж", "подблоково",
        r"вх\.", r"вход", r"вх\s", r"В-",
        r"крило", r"сектор", r"тяло", r"корпус", r"база",
        r"щанд", r"гараж", r"трафопост", "трафо", "абонатна", "котелно", "парно",
        r"манипулационна", r"манип\.", r"ман\.", r"приемно", r"регистратура",
        r"отделение", r"клиника", r"катедра",
        r"ЛК", r"РК",
        r"ЗП", r"З\.П\.", r"ЛАБ\.", r"ДЕТ\.", r"КОНС\."
    ]

    pattern_str = r"([,\s\(\.\/-]+)(" + "|".join(stop_words) + r").*$"
    clean = re.sub(pattern_str, "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\(.*?\)", "", clean)

    noise_words = [
        r"МБАЛ", r"УМБАЛ", r"СБАЛ", r"ДКЦ", r"МЦ\s", r"МЦ-", r"МДЦ", r"АИПП", r"СМДЛ", "МСЦ", "ДМСГД",
        r"ЕООД", r"ООД", r"ЕАД", r"АД\s", r"ЕТ\s", r"ЕГ\s",
        r"ТПК", "ЗС", "СЗС", "СЗУ", "ФЗП", "ФСМП", "ЦСМП", "АПЗЗ", "СБР", "РБ", "ВМБ", "ФСП", "ФЗУ",
        r"ЦПЗ", r"КОЦ", r"ТЕЛК", r"РЗИ", r"ХЕИ", r"ОСП", "ТДКЦ", "ОДПФЗС", "НМТБ", "ДУБ", "ДОЗ", "ОАПС",
        r"БАН", "НСА", "МВР", "МО", "БДЖ", "ВиК", "БТК", "ДЗИ", "ДАП", "АПК", "ТКЗС",
        r"Търговски център", r"ТЦ\s", r"Т\.Ц\.", r"Мол\s", r"Mall", r"Бизнес център", r"БЦ\s",
        r"Ритейл", r"Аптека", r"Оптика", r"Дрогерия", r"супермаркет",
        r"ТЕЦ", r"ВЕЦ", r"АЕЦ", r"Електроцентрала", r"ЗПЗ", r"СПЗ", r"НПЗ", r"ЮПЗ", r"ПЗ\s",
        r"МК\s",
    ]

    clean = re.sub(r"\b(" + "|".join(noise_words) + r")\b", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\s+", " ", clean)
    clean = re.sub(r"\s,", ",", clean)
    clean = re.sub(r",+", ",", clean)
    clean = clean.strip(" ,.-/\\")

    if len(clean) < 3:
        if "гр." in raw_addr or "с." in raw_addr:
            city_match = re.search(r"(гр\.|с\.)\s*([А-Яа-я\s\-]+)", raw_addr)
            if city_match:
                return city_match.group(0)
        return ""

    return clean


# --- STATE & HASH MANAGEMENT ---
def compute_record_hash(record):
    """Generates a deterministic MD5 hash for a base registry record."""
    serialized = json.dumps(record, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(serialized.encode("utf-8")).hexdigest()


def load_processed_state():
    """
    Loads processed IDs and their hashes.
    Supports both legacy format ('ID') and new format ('ID|HASH').
    """
    state = {}
    if not os.path.exists(PROCESSED_LOG_FILE):
        return state

    with open(PROCESSED_LOG_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if "|" in line:
                rec_id, rec_hash = line.split("|", 1)
                state[rec_id.strip()] = rec_hash.strip()
            else:
                state[line] = None  # Legacy entry without hash
    return state


def save_processed_state(state):
    """Writes the full ID|HASH state map to disk."""
    with open(PROCESSED_LOG_FILE, "w", encoding="utf-8") as f:
        for rec_id, rec_hash in state.items():
            if rec_hash:
                f.write(f"{rec_id}|{rec_hash}\n")
            else:
                f.write(f"{rec_id}\n")


# --- 100% AUTONOMOUS BASE REGISTRY FETCHING ---
def fetch_base_registry_records():
    url = "https://registries.his.bg/api/V1/outpatientcare/getOutpatientCareWithFilterForApiV1"
    start = 0
    chunk_size = 3000
    base_records = {}

    print("[INFO] Fetching base registry index from API...")

    while True:
        params = {
            "searchVidOutpatientCare": "",
            "searchStatus": "",
            "searchCity": "",
            "searchText": "",
            "searchMedicalStaff": "",
            "start": start,
            "length": chunk_size
        }
        try:
            print(f"    -> Fetching base index chunk: {start} to {start + chunk_size}")
            response = requests.get(url, headers=headers, params=params, timeout=30)

            if response.status_code != 200:
                print(f"[CRITICAL] Base API returned status {response.status_code}. Aborting extraction.")
                break

            data = response.json()
            batch = data.get("registry", [])

            if not batch:
                break

            for record in batch:
                rec_number = record.get("number")
                if rec_number:
                    clean_id = str(rec_number).strip()
                    base_records[clean_id] = record

            if len(batch) < chunk_size:
                break

            start += chunk_size
            time.sleep(0.5)

        except Exception as e:
            print(f"[ERROR] Failed to fetch base registry: {e}")
            break

    print(f"[INFO] Retrieved {len(base_records)} unique records from the base registry.")
    return base_records


# --- API & DATA PARSING ---
def fetch_details(id_number, retries=3):
    url = f"https://registries.his.bg/api/V1/outpatientcare/getOutpatientCareByNumberForApiV1?number={id_number}"
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                return None
        except Exception:
            pass
        time.sleep(1.0 * (attempt + 1))
    return None


def parse_data_for_id(records, hospitals_by_id, addresses_by_id, doctors_by_id):
    if not isinstance(records, list):
        records = [records]

    for rec in records:
        h_id = str(rec.get("number", "")).strip()
        if not h_id:
            continue

        # Replace existing records for this Hospital_ID so updates never duplicate rows
        hospitals_by_id[h_id] = []
        addresses_by_id[h_id] = []
        doctors_by_id[h_id] = []

        owners_list = rec.get("owners", [])
        base_hospital_data = {
            "Hospital_ID": h_id,
            "Old_Number": rec.get("oldNumber"),
            "Name": rec.get("name"),
            "Status": rec.get("statuslabel"),
            "Reg_Date": rec.get("registrationDate"),
            "Vid_LZ": rec.get("vid", {}).get("label") if isinstance(rec.get("vid"), dict) else rec.get("vid")
        }

        if owners_list and isinstance(owners_list, list):
            for o in owners_list:
                fn = o.get("firstname", "")
                mn = o.get("middlename", "")
                ln = o.get("lastname", "")
                full_n = f"{fn} {mn} {ln}".strip()

                entry = base_hospital_data.copy()
                entry["Managers"] = full_n
                hospitals_by_id[h_id].append(entry)
        else:
            entry = base_hospital_data.copy()
            entry["Managers"] = "N/A"
            hospitals_by_id[h_id].append(entry)

        addrs = rec.get("address", [])
        if addrs and isinstance(addrs, list):
            for ad in addrs:
                raw_full_addr = ad.get("fulladdress", "")
                clean_addr = clean_bg_address(raw_full_addr)

                addr_specs = ad.get("specialities", [])
                addr_spec_str = ", ".join([s.get("label", "") for s in addr_specs]) if addr_specs else ""

                addr_acts = ad.get("activities", [])
                addr_act_str = ", ".join([a.get("label", "") for a in addr_acts]) if addr_acts else ""

                addr_entry = {
                    "Hospital_ID": h_id,
                    "Type": ad.get("typeaddresslabel"),
                    "City": ad.get("ekatte"),
                    "Full_Address": raw_full_addr,
                    "Full_Address_Clean": clean_addr,
                    "Address_Specialties": addr_spec_str,
                    "Address_Activities": addr_act_str,
                    "Region": ad.get("district"),
                    "Municipality": ad.get("munincipaliti")
                }
                addresses_by_id[h_id].append(addr_entry)
        else:
            addresses_by_id[h_id].append({
                "Hospital_ID": h_id,
                "Full_Address": "N/A",
                "Full_Address_Clean": "N/A"
            })

        staff = rec.get("medicalStaff", [])
        if staff and isinstance(staff, list):
            for doc in staff:
                fname = doc.get("firstname", "")
                mname = doc.get("middlename", "")
                lname = doc.get("lastname", "")
                fullname = f"{fname} {mname} {lname}".strip()

                specs = doc.get("specialities", [])
                spec_str = ", ".join([s.get("label", "") for s in specs]) if specs else ""

                doc_entry = {
                    "Hospital_ID": h_id,
                    "Doctor_Name": fullname,
                    "Type": doc.get("typelabel"),
                    "Specialty": spec_str
                }
                doctors_by_id[h_id].append(doc_entry)
        else:
            doctors_by_id[h_id].append({"Hospital_ID": h_id, "Doctor_Name": "N/A"})


def save_multisheet_excel(hospitals_by_id, addresses_by_id, doctors_by_id):
    try:
        flat_hospitals = [row for rows in hospitals_by_id.values() for row in rows]
        flat_addresses = [row for rows in addresses_by_id.values() for row in rows]
        flat_doctors = [row for rows in doctors_by_id.values() for row in rows]

        df_h = pd.DataFrame(flat_hospitals)
        df_a = pd.DataFrame(flat_addresses)
        df_d = pd.DataFrame(flat_doctors)

        with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
            df_h.to_excel(writer, sheet_name="Hospitals", index=False)
            df_a.to_excel(writer, sheet_name="Addresses", index=False)
            df_d.to_excel(writer, sheet_name="Doctors", index=False)
    except Exception as e:
        print(f"!!! CRITICAL: Failed to save Excel file: {e}")


def load_existing_excel(hospitals_by_id, addresses_by_id, doctors_by_id):
    if not os.path.exists(OUTPUT_FILE):
        return

    print("[INFO] Existing Excel file found. Loading data into memory...")
    try:
        df_h = pd.read_excel(OUTPUT_FILE, sheet_name="Hospitals", dtype=str).fillna("")
        for row in df_h.to_dict("records"):
            h_id = str(row.get("Hospital_ID", "")).strip()
            if h_id:
                hospitals_by_id.setdefault(h_id, []).append(row)

        df_a = pd.read_excel(OUTPUT_FILE, sheet_name="Addresses", dtype=str).fillna("")
        for row in df_a.to_dict("records"):
            h_id = str(row.get("Hospital_ID", "")).strip()
            if h_id:
                addresses_by_id.setdefault(h_id, []).append(row)

        df_d = pd.read_excel(OUTPUT_FILE, sheet_name="Doctors", dtype=str).fillna("")
        for row in df_d.to_dict("records"):
            h_id = str(row.get("Hospital_ID", "")).strip()
            if h_id:
                doctors_by_id.setdefault(h_id, []).append(row)

        print(f"[INFO] Loaded {len(hospitals_by_id)} unique hospitals from previous dataset.")
    except Exception as e:
        print(f"[WARN] Failed to read existing Excel file. Starting clean: {e}")


def is_legacy_record_unchanged(h_id, base_rec, hospitals_by_id):
    """
    For legacy IDs in hisbg_processed_ids.txt that do not have a saved hash yet,
    compares key fields against the loaded Excel data so we don't re-scrape unchanged records.
    """
    if h_id not in hospitals_by_id or not hospitals_by_id[h_id]:
        return False

    existing = hospitals_by_id[h_id][0]
    api_name = str(base_rec.get("name") or "").strip()
    api_status = str(base_rec.get("statuslabel") or "").strip()

    saved_name = str(existing.get("Name") or "").strip()
    saved_status = str(existing.get("Status") or "").strip()

    return api_name == saved_name and api_status == saved_status


def main_loop():
    if os.path.exists(CONTINUE_FLAG_FILE):
        os.remove(CONTINUE_FLAG_FILE)

    hospitals_by_id = {}
    addresses_by_id = {}
    doctors_by_id = {}

    load_existing_excel(hospitals_by_id, addresses_by_id, doctors_by_id)
    processed_state = load_processed_state()
    base_records = fetch_base_registry_records()

    if not base_records:
        print("[CRITICAL] No records returned from base registry. Exiting.")
        return

    pending_ids = []
    state_hashes_migrated = False

    for h_id, base_rec in base_records.items():
        current_hash = compute_record_hash(base_rec)

        if h_id not in processed_state or h_id not in hospitals_by_id:
            # Brand new ID or missing from Excel
            pending_ids.append((h_id, current_hash))
        elif processed_state[h_id] is None:
            # Legacy ID without a stored hash: verify against Excel data
            if is_legacy_record_unchanged(h_id, base_rec, hospitals_by_id):
                processed_state[h_id] = current_hash
                state_hashes_migrated = True
            else:
                pending_ids.append((h_id, current_hash))
        elif processed_state[h_id] != current_hash:
            # Hash mismatch: record was modified in the registry
            pending_ids.append((h_id, current_hash))

    if state_hashes_migrated:
        save_processed_state(processed_state)

    total_pending = len(pending_ids)
    print(f"[INFO] Delta check complete: {total_pending} new or modified records require updating.")

    if total_pending == 0:
        print("[INFO] All records are up to date. Exiting.")
        return

    print(f"--- STARTING INCREMENTAL UPDATE (Targets: {total_pending}) ---")

    for i, (id_number, new_hash) in enumerate(pending_ids):
        elapsed = time.time() - START_TIME
        if elapsed > MAX_RUNTIME_SECONDS:
            print("\n[WARN] Execution time limit reached. Saving state and initiating continuation run...")
            with open(CONTINUE_FLAG_FILE, "w", encoding="utf-8") as f:
                f.write("CONTINUE_REQUIRED")

            save_processed_state(processed_state)
            if hospitals_by_id:
                save_multisheet_excel(hospitals_by_id, addresses_by_id, doctors_by_id)
            sys.exit(0)

        percent_done = ((i + 1) / total_pending) * 100
        print(f"[{i+1}/{total_pending}] >> {percent_done:.2f}% << Updating ID: {id_number}...")

        data = fetch_details(id_number)

        if data:
            parse_data_for_id(data, hospitals_by_id, addresses_by_id, doctors_by_id)
            processed_state[id_number] = new_hash

            if (i + 1) % 200 == 0:
                save_processed_state(processed_state)
                save_multisheet_excel(hospitals_by_id, addresses_by_id, doctors_by_id)

        sleep_time = random.uniform(0.3, 0.8)
        time.sleep(sleep_time)

    print("--- EXECUTION COMPLETE ---")
    save_processed_state(processed_state)
    if hospitals_by_id:
        save_multisheet_excel(hospitals_by_id, addresses_by_id, doctors_by_id)
    print(f"Incremental update finished successfully. Output saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main_loop()
