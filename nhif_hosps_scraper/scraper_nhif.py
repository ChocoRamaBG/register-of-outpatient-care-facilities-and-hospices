import os
import re
import glob
import time
import json
from urllib.parse import unquote
from datetime import datetime

import pandas as pd
from playwright.sync_api import (
    sync_playwright,
    TimeoutError as PlaywrightTimeoutError,
)

# ============================================================
# КОНФИГУРАЦИЯ
# ============================================================
START_TIME = time.time()
TIME_LIMIT_SECONDS = 5.4 * 60 * 60  # ~5 часа и 24 минути (лимит за GitHub Actions)

BASE_URL = "https://www.nhif.bg"
START_URL = f"{BASE_URL}/bg/outpatientcare-report"
TARGET_SECTION_TEXT = (
    "Брой отчетени консултативни прегледи от ОПЛ и лекари специалисти"
)

MAX_PAGE_RETRIES = 3
RETRY_DELAY_SECONDS = 3
CLEANUP_RAW_AFTER_PARSE = True

SHEETS_TO_READ = ["ПИМП", "СИМП"]
DROP_COL_NORM = "Име на МКБ 10"

GROUP_1_COLS = ["РЗИ на лекар", "Месец на дейност"]
GROUP_2_COLS = ["РЗИ на лекар", "Месец на дейност", "Код специалност"]
GROUP_3_COLS = ["РЗИ на лекар", "Месец на дейност", "МКБ 10"]

ALL_REQUIRED_GROUP_COLS = [
    "РЗИ на лекар",
    "Месец на дейност",
    "Код специалност",
    "МКБ 10",
]

SUM_COLS_NORM = [
    "Бр. ИМП",
    "Бр. първични амбулаторни прегледи",
    "Бр. вторични амбулаторни прегледи",
]

OUTPUT_COL_NAMES = {
    "Бр. вторични амбулаторни прегледи": "Бр.  вторични амбулаторни прегледи"
}

MAX_EXCEL_DATA_ROWS = 1_048_575

MASTER_1_NAME = "NHIF_Master_Consultations"
MASTER_2_NAME = "NHIF_Master_Consultations_By_Specialty"
MASTER_3_NAME = "NHIF_Master_Consultations_By_MKB10"

# ============================================================
# ПЪТИЩА И ДИРЕКТОРИИ
# ============================================================
try:
    base_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    base_dir = os.getcwd()

output_dir = os.path.join(base_dir, "nhif_hosps_output")
os.makedirs(output_dir, exist_ok=True)

raw_downloads_dir = os.path.join(output_dir, "raw_temp_downloads")
os.makedirs(raw_downloads_dir, exist_ok=True)

memory_file = os.path.join(output_dir, "parsed_urls_nhif.txt")
failed_files_file = os.path.join(output_dir, "failed_files_nhif.json")
CONTINUE_FLAG_FILE = os.path.join(output_dir, "CONTINUE_FLAG_NHIF")

# ============================================================
# УПРАВЛЕНИЕ НА ВРЕМЕТО И ФЛАГОВЕ
# ============================================================
def time_limit_reached() -> bool:
    return (time.time() - START_TIME) >= TIME_LIMIT_SECONDS

def flag_for_continuation():
    with open(CONTINUE_FLAG_FILE, "w", encoding="utf-8") as f:
        f.write("CONTINUE")

def clear_continuation_flag():
    if os.path.exists(CONTINUE_FLAG_FILE):
        os.remove(CONTINUE_FLAG_FILE)

# ============================================================
# ПАМЕТ ЗА ОБРАБОТЕНИ URL АДРЕСИ
# ============================================================
parsed_urls = set()

if os.path.exists(memory_file):
    with open(memory_file, "r", encoding="utf-8") as f:
        for line in f:
            url = line.strip()
            if url:
                parsed_urls.add(unquote(url))
                parsed_urls.add(url)
print(f"[INFO] Заредени {len(parsed_urls) // 2 if parsed_urls else 0} вече обработени файла.")

def is_url_parsed(url: str) -> bool:
    return unquote(url) in parsed_urls or url in parsed_urls

def mark_as_parsed(url: str):
    decoded = unquote(url)
    if decoded not in parsed_urls:
        parsed_urls.add(decoded)
        parsed_urls.add(url)
        with open(memory_file, "a", encoding="utf-8") as f:
            f.write(decoded + "\n")

# ============================================================
# PLAYWRIGHT ИНСТАНЦИЯ
# ============================================================
_pw_instance = None
_browser = None
_context = None
_page = None

def create_driver():
    global _pw_instance, _browser, _context, _page
    if _pw_instance is None:
        _pw_instance = sync_playwright().start()

    _browser = _pw_instance.chromium.launch(
        headless=True,
        args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
    )
    _context = _browser.new_context(
        accept_downloads=True,
        viewport={"width": 1920, "height": 1080},
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
    )
    _context.route(
        "**/*.{png,jpg,jpeg,webp,svg,css,woff,woff2}",
        lambda route: route.abort(),
    )

    _page = _context.new_page()
    _page.set_default_navigation_timeout(60000)
    _page.set_default_timeout(60000)
    return _page

def restart_driver():
    global _page, _context, _browser
    print("[INFO] Рестартиране на браузъра...")
    try:
        if _page: _page.close()
        if _context: _context.close()
        if _browser: _browser.close()
    except: pass
    time.sleep(RETRY_DELAY_SECONDS)
    return create_driver()

def close_driver():
    try:
        if _page: _page.close()
        if _context: _context.close()
        if _browser: _browser.close()
        if _pw_instance: _pw_instance.stop()
    except: pass

driver_page = create_driver()

# ============================================================
# ПОМОЩНИ ФУНКЦИИ И ПАРСВАНЕ НА EXCEL
# ============================================================
def decline_cookies():
    try:
        reject_btn = driver_page.locator("#cookies button.btn-reject").first
        if reject_btn.is_visible(timeout=3000):
            reject_btn.click(timeout=3000)
    except: pass

def normalize_col_name(col: object) -> str:
    return re.sub(r"\s+", " ", str(col)).strip()

def find_existing_local_file(raw_dir: str, year_str: str, idx: int, file_href: str) -> str | None:
    if not os.path.exists(raw_dir): return None
    prefix = f"{year_str}_{idx:02d}_"
    url_filename = unquote(file_href.split("/")[-1]).strip()
    for fname in os.listdir(raw_dir):
        if not fname.lower().endswith(".xlsx"): continue
        if fname.startswith("~$") or fname.endswith(".crdownload") or fname.endswith(".tmp"): continue
        full_path = os.path.join(raw_dir, fname)
        if not os.path.isfile(full_path) or os.path.getsize(full_path) == 0: continue
        if fname.startswith(prefix) or fname.endswith(url_filename):
            return full_path
    return None

def process_excel_file_all_groups(filepath: str):
    g1_frames, g2_frames, g3_frames = [], [], []
    try:
        xls = pd.ExcelFile(filepath, engine="openpyxl")
    except Exception as e:
        print(f"  [ERROR] Неуспешно отваряне на Excel файл {os.path.basename(filepath)}: {e}")
        return None

    available_sheets = {normalize_col_name(s).upper(): s for s in xls.sheet_names}

    for target_sheet in SHEETS_TO_READ:
        actual_sheet = available_sheets.get(target_sheet.upper())
        if not actual_sheet: continue

        try:
            df = pd.read_excel(xls, sheet_name=actual_sheet)
        except Exception as e:
            print(f"  [ERROR] Грешка при четене на лист '{actual_sheet}': {e}")
            return None

        if df.empty: continue
        df.columns = [normalize_col_name(c) for c in df.columns]
        if DROP_COL_NORM in df.columns:
            df.drop(columns=[DROP_COL_NORM], inplace=True)

        missing_cols = [c for c in (ALL_REQUIRED_GROUP_COLS + SUM_COLS_NORM) if c not in df.columns]
        if missing_cols: continue

        df = df[ALL_REQUIRED_GROUP_COLS + SUM_COLS_NORM].copy()
        df.dropna(subset=["РЗИ на лекар", "Месец на дейност"], how="any", inplace=True)

        for col in ALL_REQUIRED_GROUP_COLS:
            df[col] = df[col].fillna("").astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
        for s_col in SUM_COLS_NORM:
            df[s_col] = pd.to_numeric(df[s_col], errors="coerce").fillna(0)

        g1_frames.append(df.groupby(GROUP_1_COLS, as_index=False, dropna=False)[SUM_COLS_NORM].sum())
        g2_frames.append(df.groupby(GROUP_2_COLS, as_index=False, dropna=False)[SUM_COLS_NORM].sum())
        g3_frames.append(df.groupby(GROUP_3_COLS, as_index=False, dropna=False)[SUM_COLS_NORM].sum())

    def combine_and_sum(frames, group_cols):
        if not frames: return pd.DataFrame()
        combined = pd.concat(frames, ignore_index=True)
        return combined.groupby(group_cols, as_index=False, dropna=False)[SUM_COLS_NORM].sum()

    res_g1 = combine_and_sum(g1_frames, GROUP_1_COLS)
    res_g2 = combine_and_sum(g2_frames, GROUP_2_COLS)
    res_g3 = combine_and_sum(g3_frames, GROUP_3_COLS)

    if res_g1.empty: return None
    return res_g1, res_g2, res_g3

def consolidate_chunks(chunks: list[pd.DataFrame], group_cols: list[str]) -> list[pd.DataFrame]:
    if not chunks: return []
    merged = pd.concat(chunks, ignore_index=True)
    merged = merged.groupby(group_cols, as_index=False, dropna=False)[SUM_COLS_NORM].sum()
    return [merged]

def load_existing_master_chunks(base_filename: str, group_cols: list[str]) -> list[pd.DataFrame]:
    single_file = os.path.join(output_dir, f"{base_filename}.xlsx")
    part_files = sorted(glob.glob(os.path.join(output_dir, f"{base_filename}_part_*.xlsx")))
    files_to_load = [single_file] if os.path.exists(single_file) else part_files
    loaded_frames = []

    for fpath in files_to_load:
        try:
            df = pd.read_excel(fpath, engine="openpyxl")
            if df.empty: continue
            df.columns = [normalize_col_name(c) for c in df.columns]
            for col in group_cols:
                if col in df.columns:
                    df[col] = df[col].fillna("").astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
            for s_col in SUM_COLS_NORM:
                if s_col in df.columns:
                    df[s_col] = pd.to_numeric(df[s_col], errors="coerce").fillna(0)
            loaded_frames.append(df[group_cols + SUM_COLS_NORM])
        except: pass
    return consolidate_chunks(loaded_frames, group_cols)

def save_master_excel(df: pd.DataFrame, sort_cols: list[str], base_filename: str):
    if df.empty: return
    df.sort_values(by=sort_cols, inplace=True)
    df_out = df.rename(columns=OUTPUT_COL_NAMES)
    total_rows = len(df_out)
    print(f"[INFO] Записване на {base_filename} (общо редове: {total_rows:,})...")

    old_files = glob.glob(os.path.join(output_dir, f"{base_filename}*.xlsx"))

    if total_rows <= MAX_EXCEL_DATA_ROWS:
        out_path = os.path.join(output_dir, f"{base_filename}.xlsx")
        temp_path = out_path + ".tmp.xlsx"
        df_out.to_excel(temp_path, index=False, sheet_name="Master")
        for old_f in old_files:
            if old_f != out_path and os.path.exists(old_f): os.remove(old_f)
        os.replace(temp_path, out_path)
    else:
        num_parts = (total_rows // MAX_EXCEL_DATA_ROWS) + (1 if total_rows % MAX_EXCEL_DATA_ROWS != 0 else 0)
        new_paths = set()
        for part_idx in range(num_parts):
            start_row = part_idx * MAX_EXCEL_DATA_ROWS
            end_row = min(start_row + MAX_EXCEL_DATA_ROWS, total_rows)
            chunk_df = df_out.iloc[start_row:end_row]
            out_path = os.path.join(output_dir, f"{base_filename}_part_{part_idx + 1}.xlsx")
            temp_path = out_path + ".tmp.xlsx"
            chunk_df.to_excel(temp_path, index=False, sheet_name="Master")
            os.replace(temp_path, out_path)
            new_paths.add(out_path)
        for old_f in old_files:
            if old_f not in new_paths and os.path.exists(old_f): os.remove(old_f)

def flush_all_masters(g1_chunks, g2_chunks, g3_chunks, pending_parsed_urls):
    if not pending_parsed_urls: return
    if g1_chunks: save_master_excel(consolidate_chunks(g1_chunks, GROUP_1_COLS)[0], ["Месец на дейност", "РЗИ на лекар"], MASTER_1_NAME)
    if g2_chunks: save_master_excel(consolidate_chunks(g2_chunks, GROUP_2_COLS)[0], ["Месец на дейност", "РЗИ на лекар", "Код специалност"], MASTER_2_NAME)
    if g3_chunks: save_master_excel(consolidate_chunks(g3_chunks, GROUP_3_COLS)[0], ["Месец на дейност", "РЗИ на лекар", "МКБ 10"], MASTER_3_NAME)
    for url in pending_parsed_urls: mark_as_parsed(url)
    pending_parsed_urls.clear()

# ============================================================
# ЛОГОВЕ ЗА ГРЕШКИ И ПОВТОРНИ ОПИТИ
# ============================================================
def add_failed_file(file_href: str, year_str: str, title_text: str, error_msg: str = ""):
    failed_list = []
    if os.path.exists(failed_files_file):
        try:
            with open(failed_files_file, "r", encoding="utf-8") as f: failed_list = json.load(f)
        except: pass
    decoded_href = unquote(file_href)
    failed_list = [item for item in failed_list if item.get("File_Href") != decoded_href]
    failed_list.append({
        "File_Href": decoded_href, "Raw_Href": file_href, "Year": year_str,
        "Title": title_text, "Error": str(error_msg),
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })
    with open(failed_files_file, "w", encoding="utf-8") as f:
        json.dump(failed_list, f, ensure_ascii=False, indent=2)

def download_and_parse_single_file(year_str, idx, title_text, file_href):
    global driver_page
    existing_local = find_existing_local_file(raw_downloads_dir, year_str, idx, file_href)
    if existing_local:
        local_filepath = existing_local
    else:
        year_page_url = f"{BASE_URL}/bg/outpatientcare-report/consultation/{year_str}"
        if driver_page.url != year_page_url:
            driver_page.goto(year_page_url, wait_until="domcontentloaded")
            decline_cookies()
        driver_page.wait_for_selector("#files_container", timeout=30000)
        driver_page.evaluate("() => { document.querySelectorAll('#files_container li').forEach(el => { el.style.display = 'block'; }); }")
        link_locator = driver_page.locator(f'#files_container li a[href="{file_href}"]').first
        if link_locator.count() == 0:
            link_locator = driver_page.locator(f'#files_container li a[href*="{file_href.split("/")[-1]}"]').first
        link_locator.scroll_into_view_if_needed()
        with driver_page.expect_download(timeout=180000) as download_info:
            link_locator.click()
        download = download_info.value
        safe_filename = f"{year_str}_{idx:02d}_{download.suggested_filename}"
        local_filepath = os.path.join(raw_downloads_dir, safe_filename)
        download.save_as(local_filepath)

    parsed_result = process_excel_file_all_groups(local_filepath)
    if parsed_result is not None and CLEANUP_RAW_AFTER_PARSE and os.path.exists(local_filepath):
        try: os.remove(local_filepath)
        except: pass
    return parsed_result

def retry_failed_files(g1_chunks, g2_chunks, g3_chunks, pending_parsed_urls) -> bool:
    global driver_page
    if not os.path.exists(failed_files_file): return False
    try:
        with open(failed_files_file, "r", encoding="utf-8") as f: failed_list = json.load(f)
    except: return False
    if not failed_list: return False

    still_failed = []
    time_limit_hit = False

    for idx, item in enumerate(failed_list, start=1):
        if time_limit_hit or time_limit_reached():
            if not time_limit_hit:
                print("[INFO] Лимитът на времето е достигнат. Флагът за продължение е активиран.")
                flag_for_continuation()
            time_limit_hit = True
            still_failed.append(item)
            continue
        raw_href = item.get("Raw_Href") or item.get("File_Href")
        if is_url_parsed(raw_href): continue
        try:
            res = download_and_parse_single_file(item.get("Year", "2024"), idx, item.get("Title", raw_href), raw_href)
            if res is not None:
                df_g1, df_g2, df_g3 = res
                g1_chunks.append(df_g1)
                g2_chunks.append(df_g2)
                g3_chunks.append(df_g3)
                pending_parsed_urls.append(raw_href)
            else:
                still_failed.append(item)
        except Exception as e:
            item["Error"] = str(e)
            still_failed.append(item)
            driver_page = restart_driver()

    with open(failed_files_file, "w", encoding="utf-8") as f:
        json.dump(still_failed, f, ensure_ascii=False, indent=2)
    if pending_parsed_urls:
        flush_all_masters(g1_chunks, g2_chunks, g3_chunks, pending_parsed_urls)
    return time_limit_hit

def scrape_available_years() -> list[dict]:
    driver_page.goto(START_URL, wait_until="domcontentloaded")
    decline_cookies()
    section_locator = driver_page.locator("div.mb-4", has=driver_page.locator("h2.h5", has_text=TARGET_SECTION_TEXT))
    section_locator.wait_for(state="visible", timeout=30000)
    year_links = section_locator.locator("a.u-btn").all()
    years_found = []
    for link in year_links:
        text = link.inner_text().strip()
        href = link.get_attribute("href")
        if text.isdigit() and href:
            years_found.append({"year": int(text), "year_str": text, "href": href})
    years_found.sort(key=lambda x: x["year"])
    return years_found

# ============================================================
# ОСНОВНА ЛОГИКА
# ============================================================
def main():
    global driver_page
    clear_continuation_flag()

    # 1. Първо събираме всички години и проверяваме дали ИМА НОВИ ФАЙЛОВЕ
    try:
        years_list = scrape_available_years()
    except Exception as e:
        print(f"[CRITICAL] Неуспешно извличане на налични години: {e}")
        close_driver()
        return

    new_file_tasks = []
    for year_obj in years_list:
        year_str = year_obj["year_str"]
        year_url = year_obj["href"] if year_obj["href"].startswith("http") else f"{BASE_URL}{year_obj['href']}"
        
        try:
            driver_page.goto(year_url, wait_until="domcontentloaded")
            decline_cookies()
            driver_page.wait_for_selector("#files_container", timeout=30000)
            driver_page.evaluate("() => { document.querySelectorAll('#files_container li').forEach(el => { el.style.display = 'block'; }); }")
        except Exception as e:
            print(f"[WARN] Грешка при проверка на {year_str}: {e}")
            continue

        file_anchors = driver_page.locator("#files_container li a").all()
        file_items = []
        for anchor in file_anchors:
            href = anchor.get_attribute("href")
            title_text = re.sub(r"\s+", " ", anchor.inner_text()).strip()
            if href and ".xlsx" in href.lower():
                file_items.append((title_text, href))
        file_items.reverse()

        for idx, (title_text, file_href) in enumerate(file_items, start=1):
            if not is_url_parsed(file_href):
                new_file_tasks.append({
                    "year_str": year_str,
                    "idx": idx,
                    "title_text": title_text,
                    "file_href": file_href
                })

    # 2. Проверяваме и дали имаме стари fail-нати файлове за retry
    has_failed_files = False
    if os.path.exists(failed_files_file):
        try:
            with open(failed_files_file, "r", encoding="utf-8") as f:
                fails = json.load(f)
                if fails: has_failed_files = True
        except: pass

    # 3. АКО НЯМА НОВИ И НЯМА ФЕЙЛНАТИ - БИЕМ КЕПЕНЦИТЕ ВЕДНАГА!
    if not new_file_tasks and not has_failed_files:
        print("\n[INFO] Няма нови файлове. Пропускаме зареждането на 18 милиона реда в RAM! Всичко е актуално.")
        close_driver()
        return

    # 4. АКО ИМА НОВИ, ЧАК ТОГАВА ТОВАРИМ ПАМЕТТА И ЧАКАМЕ ДА ПРОЧЕТЕ EXCEL-ИТЕ
    print(f"\n[INFO] Открити {len(new_file_tasks)} нови и {has_failed_files} failed файла! Започва тежко зареждане на Master базата...")
    g1_chunks = load_existing_master_chunks(MASTER_1_NAME, GROUP_1_COLS)
    g2_chunks = load_existing_master_chunks(MASTER_2_NAME, GROUP_2_COLS)
    g3_chunks = load_existing_master_chunks(MASTER_3_NAME, GROUP_3_COLS)
    pending_parsed_urls = []

    if retry_failed_files(g1_chunks, g2_chunks, g3_chunks, pending_parsed_urls):
        close_driver()
        return

    new_files_count = 0
    for task in new_file_tasks:
        if time_limit_reached():
            print("[INFO] Лимитът на времето е достигнат. Флагът за продължение е активиран.")
            flush_all_masters(g1_chunks, g2_chunks, g3_chunks, pending_parsed_urls)
            flag_for_continuation()
            break

        print(f"\n  [{task['year_str']} - {task['idx']}] Обработка на НОВ файл: {task['title_text']}")
        try:
            res = download_and_parse_single_file(task['year_str'], task['idx'], task['title_text'], task['file_href'])
            if res is not None:
                df_g1, df_g2, df_g3 = res
                g1_chunks.append(df_g1)
                g2_chunks.append(df_g2)
                g3_chunks.append(df_g3)
                pending_parsed_urls.append(task['file_href'])
                new_files_count += 1
                g1_chunks = consolidate_chunks(g1_chunks, GROUP_1_COLS)
                g2_chunks = consolidate_chunks(g2_chunks, GROUP_2_COLS)
                g3_chunks = consolidate_chunks(g3_chunks, GROUP_3_COLS)
            else:
                add_failed_file(task['file_href'], task['year_str'], task['title_text'], "Празен или невалиден Excel файл")
        except Exception as e:
            add_failed_file(task['file_href'], task['year_str'], task['title_text'], str(e))
            driver_page = restart_driver()

        if len(pending_parsed_urls) >= 3:
            flush_all_masters(g1_chunks, g2_chunks, g3_chunks, pending_parsed_urls)

    if pending_parsed_urls:
        flush_all_masters(g1_chunks, g2_chunks, g3_chunks, pending_parsed_urls)

    close_driver()
    if new_files_count == 0:
        print("\n[INFO] Реално нямаше нови валидни файлове за добавяне.")
    else:
        print(f"\n[INFO] Успешно обработени и добавени {new_files_count} нови файла!")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        close_driver()
        print("\n[INFO] Прекъснато от потребител.")
