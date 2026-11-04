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

# Ако е True, изтрива суровия 25MB .xlsx файл след успешно агрегиране,
# за да не препълва Git хранилището в GitHub Actions.
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

state_file = os.path.join(output_dir, "savegame_nhif.json")
memory_file = os.path.join(output_dir, "parsed_urls_nhif.txt")
failed_files_file = os.path.join(output_dir, "failed_files_nhif.json")
CONTINUE_FLAG_FILE = os.path.join(output_dir, "CONTINUE_FLAG_NHIF")

# ============================================================
# УПРАВЛЕНИЕ НА ВРЕМЕТО И ФЛАГОВЕ ЗА ПРОДЪЛЖЕНИЕ
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
# УПРАВЛЕНИЕ НА СЪСТОЯНИЕТО (STATE)
# ============================================================
state = {
    "year_index": 0,
    "years": [],
    "in_progress": False,
    "consecutive_fails": 0,
    "last_check_timestamp": None,
}

if os.path.exists(state_file):
    try:
        with open(state_file, "r", encoding="utf-8") as f:
            loaded_state = json.load(f)
            state.update(loaded_state)
        print(
            f"[INFO] Заредено състояние: Година индекс {state['year_index']}, "
            f"Активна сесия: {state['in_progress']}"
        )
    except Exception as e:
        print(f"[WARN] Грешка при зареждане на състоянието: {e}")


def save_state():
    temp_file = state_file + ".tmp"
    try:
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        os.replace(temp_file, state_file)
    except Exception as e:
        print(f"[ERROR] Неуспешен запис на state файл: {e}")


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
    # Блокиране на изображения, шрифтове и стилове за по-бързо зареждане
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
        if _page:
            _page.close()
        if _context:
            _context.close()
        if _browser:
            _browser.close()
    except Exception:
        pass
    time.sleep(RETRY_DELAY_SECONDS)
    return create_driver()


def close_driver():
    try:
        if _page:
            _page.close()
        if _context:
            _context.close()
        if _browser:
            _browser.close()
        if _pw_instance:
            _pw_instance.stop()
    except Exception:
        pass


driver_page = create_driver()


# ============================================================
# ПОМОЩНИ ФУНКЦИИ И ПАРСВАНЕ НА EXCEL
# ============================================================
def decline_cookies():
    try:
        reject_btn = driver_page.locator("#cookies button.btn-reject").first
        if reject_btn.is_visible(timeout=3000):
            reject_btn.click(timeout=3000)
    except PlaywrightTimeoutError:
        pass
    except Exception:
        pass


def normalize_col_name(col: object) -> str:
    return re.sub(r"\s+", " ", str(col)).strip()


def find_existing_local_file(raw_dir: str, year_str: str, idx: int, file_href: str) -> str | None:
    """Проверява дали файлът вече съществува локално в raw_dir."""
    if not os.path.exists(raw_dir):
        return None

    prefix = f"{year_str}_{idx:02d}_"
    url_filename = unquote(file_href.split("/")[-1]).strip()

    for fname in os.listdir(raw_dir):
        if not fname.lower().endswith(".xlsx"):
            continue
        if fname.startswith("~$") or fname.endswith(".crdownload") or fname.endswith(".tmp"):
            continue

        full_path = os.path.join(raw_dir, fname)
        if not os.path.isfile(full_path) or os.path.getsize(full_path) == 0:
            continue

        if fname.startswith(prefix) or fname.endswith(url_filename):
            return full_path

    return None


def process_excel_file_all_groups(
    filepath: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame] | None:
    """
    Чете листове ПИМП и СИМП еднократно и връща 3 предварително агрегирани DataFrame обекта:
      1. Групиране по (РЗИ на лекар, Месец на дейност)
      2. Групиране по (РЗИ на лекар, Месец на дейност, Код специалност)
      3. Групиране по (РЗИ на лекар, Месец на дейност, МКБ 10)
    """
    g1_frames, g2_frames, g3_frames = [], [], []

    try:
        xls = pd.ExcelFile(filepath, engine="openpyxl")
    except Exception as e:
        print(f"  [ERROR] Неуспешно отваряне на Excel файл {os.path.basename(filepath)}: {e}")
        return None

    available_sheets = {normalize_col_name(s).upper(): s for s in xls.sheet_names}

    for target_sheet in SHEETS_TO_READ:
        actual_sheet = available_sheets.get(target_sheet.upper())
        if not actual_sheet:
            print(f"  [WARN] Лист '{target_sheet}' липсва във {os.path.basename(filepath)}")
            continue

        try:
            df = pd.read_excel(xls, sheet_name=actual_sheet)
        except Exception as e:
            print(f"  [ERROR] Грешка при четене на лист '{actual_sheet}': {e}")
            return None

        if df.empty:
            continue

        df.columns = [normalize_col_name(c) for c in df.columns]

        if DROP_COL_NORM in df.columns:
            df.drop(columns=[DROP_COL_NORM], inplace=True)

        missing_cols = [
            c for c in (ALL_REQUIRED_GROUP_COLS + SUM_COLS_NORM) if c not in df.columns
        ]
        if missing_cols:
            print(
                f"  [WARN] Липсващи колони {missing_cols} в лист '{actual_sheet}'. "
                f"Налични: {list(df.columns)}"
            )
            continue

        df = df[ALL_REQUIRED_GROUP_COLS + SUM_COLS_NORM].copy()
        df.dropna(subset=["РЗИ на лекар", "Месец на дейност"], how="any", inplace=True)

        for col in ALL_REQUIRED_GROUP_COLS:
            df[col] = (
                df[col]
                .fillna("")
                .astype(str)
                .str.replace(r"\.0$", "", regex=True)
                .str.strip()
            )

        for s_col in SUM_COLS_NORM:
            df[s_col] = pd.to_numeric(df[s_col], errors="coerce").fillna(0)

        g1_frames.append(
            df.groupby(GROUP_1_COLS, as_index=False, dropna=False)[SUM_COLS_NORM].sum()
        )
        g2_frames.append(
            df.groupby(GROUP_2_COLS, as_index=False, dropna=False)[SUM_COLS_NORM].sum()
        )
        g3_frames.append(
            df.groupby(GROUP_3_COLS, as_index=False, dropna=False)[SUM_COLS_NORM].sum()
        )

    def combine_and_sum(frames: list[pd.DataFrame], group_cols: list[str]) -> pd.DataFrame:
        if not frames:
            return pd.DataFrame()
        combined = pd.concat(frames, ignore_index=True)
        return combined.groupby(group_cols, as_index=False, dropna=False)[SUM_COLS_NORM].sum()

    res_g1 = combine_and_sum(g1_frames, GROUP_1_COLS)
    res_g2 = combine_and_sum(g2_frames, GROUP_2_COLS)
    res_g3 = combine_and_sum(g3_frames, GROUP_3_COLS)

    if res_g1.empty:
        return None

    return res_g1, res_g2, res_g3


def consolidate_chunks(chunks: list[pd.DataFrame], group_cols: list[str]) -> list[pd.DataFrame]:
    if not chunks:
        return []
    merged = pd.concat(chunks, ignore_index=True)
    merged = merged.groupby(group_cols, as_index=False, dropna=False)[SUM_COLS_NORM].sum()
    return [merged]


def load_existing_master_chunks(base_filename: str, group_cols: list[str]) -> list[pd.DataFrame]:
    """
    Зарежда вече съществуващите Master Excel файлове (вкл. _part_1, _part_2),
    за да се добавят новите месечни данни към досегашните.
    """
    single_file = os.path.join(output_dir, f"{base_filename}.xlsx")
    part_files = sorted(glob.glob(os.path.join(output_dir, f"{base_filename}_part_*.xlsx")))

    files_to_load = [single_file] if os.path.exists(single_file) else part_files
    loaded_frames = []

    for fpath in files_to_load:
        try:
            df = pd.read_excel(fpath, engine="openpyxl")
            if df.empty:
                continue
            df.columns = [normalize_col_name(c) for c in df.columns]
            for col in group_cols:
                if col in df.columns:
                    df[col] = (
                        df[col]
                        .fillna("")
                        .astype(str)
                        .str.replace(r"\.0$", "", regex=True)
                        .str.strip()
                    )
            for s_col in SUM_COLS_NORM:
                if s_col in df.columns:
                    df[s_col] = pd.to_numeric(df[s_col], errors="coerce").fillna(0)
            loaded_frames.append(df[group_cols + SUM_COLS_NORM])
        except Exception as e:
            print(f"[WARN] Неуспешно зареждане на съществуващ Master файл {fpath}: {e}")

    return consolidate_chunks(loaded_frames, group_cols)


def save_master_excel(df: pd.DataFrame, sort_cols: list[str], base_filename: str):
    """Записва агрегирания DataFrame в Excel, като разделя на части при над 1,048,575 реда."""
    if df.empty:
        return

    df.sort_values(by=sort_cols, inplace=True)
    df_out = df.rename(columns=OUTPUT_COL_NAMES)
    total_rows = len(df_out)
    print(f"[INFO] Записване на {base_filename} (общо редове: {total_rows:,})...")

    # Изчистване на стари части преди запис на обновените файлове
    old_files = glob.glob(os.path.join(output_dir, f"{base_filename}*.xlsx"))

    if total_rows <= MAX_EXCEL_DATA_ROWS:
        out_path = os.path.join(output_dir, f"{base_filename}.xlsx")
        temp_path = out_path + ".tmp.xlsx"
        df_out.to_excel(temp_path, index=False, sheet_name="Master")
        for old_f in old_files:
            if old_f != out_path and os.path.exists(old_f):
                os.remove(old_f)
        os.replace(temp_path, out_path)
    else:
        num_parts = (total_rows // MAX_EXCEL_DATA_ROWS) + (
            1 if total_rows % MAX_EXCEL_DATA_ROWS != 0 else 0
        )
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
            if old_f not in new_paths and os.path.exists(old_f):
                os.remove(old_f)


def flush_all_masters(
    g1_chunks: list[pd.DataFrame],
    g2_chunks: list[pd.DataFrame],
    g3_chunks: list[pd.DataFrame],
    pending_parsed_urls: list[str],
):
    """Записва трите Master таблици на диска и маркира URL адресите като обработени."""
    if not pending_parsed_urls:
        return

    if g1_chunks:
        save_master_excel(
            consolidate_chunks(g1_chunks, GROUP_1_COLS)[0],
            sort_cols=["Месец на дейност", "РЗИ на лекар"],
            base_filename=MASTER_1_NAME,
        )
    if g2_chunks:
        save_master_excel(
            consolidate_chunks(g2_chunks, GROUP_2_COLS)[0],
            sort_cols=["Месец на дейност", "РЗИ на лекар", "Код специалност"],
            base_filename=MASTER_2_NAME,
        )
    if g3_chunks:
        save_master_excel(
            consolidate_chunks(g3_chunks, GROUP_3_COLS)[0],
            sort_cols=["Месец на дейност", "РЗИ на лекар", "МКБ 10"],
            base_filename=MASTER_3_NAME,
        )

    for url in pending_parsed_urls:
        mark_as_parsed(url)
    pending_parsed_urls.clear()


# ============================================================
# ЛОГОВЕ ЗА ГРЕШКИ И ПОВТОРНИ ОПИТИ
# ============================================================
def add_failed_file(file_href: str, year_str: str, title_text: str, error_msg: str = ""):
    failed_list = []
    if os.path.exists(failed_files_file):
        try:
            with open(failed_files_file, "r", encoding="utf-8") as f:
                failed_list = json.load(f)
        except json.JSONDecodeError:
            pass

    decoded_href = unquote(file_href)
    # Избягване на дублирани записи за един и същ файл
    failed_list = [item for item in failed_list if item.get("File_Href") != decoded_href]
    failed_list.append(
        {
            "File_Href": decoded_href,
            "Raw_Href": file_href,
            "Year": year_str,
            "Title": title_text,
            "Error": str(error_msg),
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    )
    with open(failed_files_file, "w", encoding="utf-8") as f:
        json.dump(failed_list, f, ensure_ascii=False, indent=2)


def download_and_parse_single_file(
    year_str: str, idx: int, title_text: str, file_href: str
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame] | None:
    """Изтегля (или намира локално) един месечен Excel файл и връща трите агрегации."""
    global driver_page

    existing_local = find_existing_local_file(raw_downloads_dir, year_str, idx, file_href)
    if existing_local:
        local_filepath = existing_local
        print(f"  [LOCAL] Намерен локален файл: {os.path.basename(local_filepath)}")
    else:
        year_page_url = f"{BASE_URL}/bg/outpatientcare-report/consultation/{year_str}"
        if driver_page.url != year_page_url:
            driver_page.goto(year_page_url, wait_until="domcontentloaded")
            decline_cookies()

        driver_page.wait_for_selector("#files_container", timeout=30000)
        driver_page.evaluate(
            """() => {
                document.querySelectorAll('#files_container li').forEach(el => {
                    el.style.display = 'block';
                });
            }"""
        )

        link_locator = driver_page.locator(f'#files_container li a[href="{file_href}"]').first
        if link_locator.count() == 0:
            # Резервно търсене по декодиран href
            link_locator = driver_page.locator(
                f'#files_container li a[href*="{file_href.split("/")[-1]}"]'
            ).first

        link_locator.scroll_into_view_if_needed()

        with driver_page.expect_download(timeout=180000) as download_info:
            link_locator.click()

        download = download_info.value
        safe_filename = f"{year_str}_{idx:02d}_{download.suggested_filename}"
        local_filepath = os.path.join(raw_downloads_dir, safe_filename)
        download.save_as(local_filepath)
        print(f"  [DOWNLOADED] Изтеглен файл: {os.path.basename(local_filepath)}")

    parsed_result = process_excel_file_all_groups(local_filepath)

    if parsed_result is not None and CLEANUP_RAW_AFTER_PARSE and os.path.exists(local_filepath):
        try:
            os.remove(local_filepath)
        except Exception:
            pass

    return parsed_result


def retry_failed_files(
    g1_chunks: list[pd.DataFrame],
    g2_chunks: list[pd.DataFrame],
    g3_chunks: list[pd.DataFrame],
    pending_parsed_urls: list[str],
) -> bool:
    global driver_page
    if not os.path.exists(failed_files_file):
        return False

    try:
        with open(failed_files_file, "r", encoding="utf-8") as f:
            failed_list = json.load(f)
    except Exception:
        return False

    if not failed_list:
        return False

    print(f"\n[INFO] Опит за възстановяване на {len(failed_list)} неуспешни файла...")
    still_failed = []
    time_limit_hit = False

    for idx, item in enumerate(failed_list, start=1):
        if time_limit_hit or time_limit_reached():
            if not time_limit_hit:
                print("[INFO] Лимитът на времето е достигнат по време на възстановяване.")
                flag_for_continuation()
                time_limit_hit = True
            still_failed.append(item)
            continue

        raw_href = item.get("Raw_Href") or item.get("File_Href")
        year_str = item.get("Year", "2024")
        title_text = item.get("Title", raw_href)

        if is_url_parsed(raw_href):
            continue

        try:
            res = download_and_parse_single_file(year_str, idx, title_text, raw_href)
            if res is not None:
                df_g1, df_g2, df_g3 = res
                g1_chunks.append(df_g1)
                g2_chunks.append(df_g2)
                g3_chunks.append(df_g3)
                pending_parsed_urls.append(raw_href)
                print(f"  [+] Възстановен и обработен: {title_text}")
            else:
                item["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                still_failed.append(item)
        except Exception as e:
            print(f"  [WARN] Повторна грешка за {title_text}: {e}")
            item["Error"] = str(e)
            item["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            still_failed.append(item)
            driver_page = restart_driver()

    with open(failed_files_file, "w", encoding="utf-8") as f:
        json.dump(still_failed, f, ensure_ascii=False, indent=2)

    if pending_parsed_urls:
        flush_all_masters(g1_chunks, g2_chunks, g3_chunks, pending_parsed_urls)

    return time_limit_hit


# ============================================================
# ОТКРИВАНЕ НА ГОДИНИ НА СТРАНИЦАТА НА НЗОК
# ============================================================
def scrape_available_years() -> list[dict]:
    print(f"[INFO] Проверка за налични години на {START_URL} ...")
    driver_page.goto(START_URL, wait_until="domcontentloaded")
    decline_cookies()

    section_locator = driver_page.locator(
        "div.mb-4",
        has=driver_page.locator("h2.h5", has_text=TARGET_SECTION_TEXT),
    )
    section_locator.wait_for(state="visible", timeout=30000)

    year_links = section_locator.locator("a.u-btn").all()
    years_found = []
    for link in year_links:
        text = link.inner_text().strip()
        href = link.get_attribute("href")
        if text.isdigit() and href:
            years_found.append({"year": int(text), "year_str": text, "href": href})

    if not years_found:
        raise ValueError("Не са открити години в целевата секция на НЗОК.")

    # Сортиране от най-старата към най-новата година (2024 -> 2025 -> 2026 -> ...)
    years_found.sort(key=lambda x: x["year"])
    print(f"[INFO] Налични години: {[y['year_str'] for y in years_found]}")
    return years_found


# ============================================================
# ОСНОВНА ЛОГИКА
# ============================================================
def main():
    global driver_page
    clear_continuation_flag()

    # Зареждане на досегашните Master файлове в паметта
    g1_chunks = load_existing_master_chunks(MASTER_1_NAME, GROUP_1_COLS)
    g2_chunks = load_existing_master_chunks(MASTER_2_NAME, GROUP_2_COLS)
    g3_chunks = load_existing_master_chunks(MASTER_3_NAME, GROUP_3_COLS)
    pending_parsed_urls = []

    if retry_failed_files(g1_chunks, g2_chunks, g3_chunks, pending_parsed_urls):
        close_driver()
        return

    # При ново месечно стартиране (когато предишният цикъл е завършил),
    # проверяваме отново всички години на сайта за нови месечни файлове или нова година.
    years_list = scrape_available_years()
    state["years"] = years_list

    if not state.get("in_progress", False):
        state["year_index"] = 0
        state["in_progress"] = True
        save_state()

    total_years = len(state["years"])
    new_files_count = 0

    while state["year_index"] < total_years:
        if time_limit_reached():
            print("\n[INFO] Лимитът на времето е достигнат. Флагът за продължение е активиран.")
            flush_all_masters(g1_chunks, g2_chunks, g3_chunks, pending_parsed_urls)
            flag_for_continuation()
            save_state()
            break

        year_obj = state["years"][state["year_index"]]
        year_str = year_obj["year_str"]
        year_url = (
            year_obj["href"]
            if year_obj["href"].startswith("http")
            else f"{BASE_URL}{year_obj['href']}"
        )

        print(
            f"\n--- Проверка на година ({state['year_index'] + 1}/{total_years}): {year_str} ---"
        )

        try:
            driver_page.goto(year_url, wait_until="domcontentloaded")
            decline_cookies()
            driver_page.wait_for_selector("#files_container", timeout=30000)
            driver_page.evaluate(
                """() => {
                    document.querySelectorAll('#files_container li').forEach(el => {
                        el.style.display = 'block';
                    });
                }"""
            )
        except Exception as e:
            print(f"[WARN] Грешка при зареждане на година {year_str}: {e}")
            state["consecutive_fails"] += 1
            if state["consecutive_fails"] >= MAX_PAGE_RETRIES:
                print(f"[ERROR] Достигнат лимит за опити на година {year_str}. Преминаване нататък.")
                state["year_index"] += 1
                state["consecutive_fails"] = 0
            save_state()
            driver_page = restart_driver()
            continue

        state["consecutive_fails"] = 0

        file_anchors = driver_page.locator("#files_container li a").all()
        file_items = []
        for anchor in file_anchors:
            href = anchor.get_attribute("href")
            title_text = re.sub(r"\s+", " ", anchor.inner_text()).strip()
            if href and ".xlsx" in href.lower():
                file_items.append((title_text, href))

        # Обръщане на списъка (от януари към декември)
        file_items.reverse()
        print(f"[INFO] Открити {len(file_items)} Excel файла за {year_str} г.")

        time_limit_hit_in_files = False

        for idx, (title_text, file_href) in enumerate(file_items, start=1):
            if time_limit_reached():
                print("[INFO] Лимитът на времето е достигнат по време на обработка на файлове.")
                flush_all_masters(g1_chunks, g2_chunks, g3_chunks, pending_parsed_urls)
                flag_for_continuation()
                time_limit_hit_in_files = True
                break

            if is_url_parsed(file_href):
                print(f"  [SKIP] Вече обработен: {title_text}")
                continue

            print(f"\n  [{year_str} - {idx}/{len(file_items)}] Обработка на НОВ файл: {title_text}")
            try:
                res = download_and_parse_single_file(year_str, idx, title_text, file_href)
                if res is not None:
                    df_g1, df_g2, df_g3 = res
                    g1_chunks.append(df_g1)
                    g2_chunks.append(df_g2)
                    g3_chunks.append(df_g3)
                    pending_parsed_urls.append(file_href)
                    new_files_count += 1

                    g1_chunks = consolidate_chunks(g1_chunks, GROUP_1_COLS)
                    g2_chunks = consolidate_chunks(g2_chunks, GROUP_2_COLS)
                    g3_chunks = consolidate_chunks(g3_chunks, GROUP_3_COLS)

                    print(
                        f"  [+] Успешно агрегиран: {title_text} | "
                        f"Нови редове (База: {len(df_g1):,}, Спец: {len(df_g2):,}, МКБ: {len(df_g3):,})"
                    )
                else:
                    add_failed_file(file_href, year_str, title_text, "Празен или невалиден Excel файл")
            except Exception as e:
                print(f"  [ERROR] Грешка при обработка на {title_text}: {e}")
                add_failed_file(file_href, year_str, title_text, str(e))
                driver_page = restart_driver()

            # Периодичен запис на всеки 3 нови файла за защита при прекъсване
            if len(pending_parsed_urls) >= 3:
                flush_all_masters(g1_chunks, g2_chunks, g3_chunks, pending_parsed_urls)

        # Запис в края на всяка година
        if pending_parsed_urls:
            flush_all_masters(g1_chunks, g2_chunks, g3_chunks, pending_parsed_urls)

        if time_limit_hit_in_files:
            save_state()
            break

        state["year_index"] += 1
        save_state()

    close_driver()

    if state["year_index"] >= total_years:
        state["in_progress"] = False
        state["last_check_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        save_state()
        if new_files_count == 0:
            print("\n[INFO] Няма нови файлове за обработка. Всички данни са актуални!")
        else:
            print(f"\n[INFO] Успешно обработени и добавени {new_files_count} нови файла!")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        close_driver()
        print("\n[INFO] Прекъснато от потребител.")
