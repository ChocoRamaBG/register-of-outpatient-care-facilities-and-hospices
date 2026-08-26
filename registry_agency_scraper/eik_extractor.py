import os
import time
import json
import sys
import urllib.parse
import traceback
import signal
import threading
import atexit
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


# ============================================================
# CONFIG
# ============================================================

START_TIME = time.time()

# Оставяме сериозен buffer преди GitHub Actions timeout-а.
TIME_LIMIT_SECONDS = 5 * 60 * 60 + 20 * 60  # 5h 20m

HEARTBEAT_INTERVAL = 30
HEARTBEAT_LOG_INTERVAL = 300

PAGE_TIMEOUT = 30_000
SELECTOR_TIMEOUT = 8_000

MAX_QUERY_RETRIES = 3
RATE_LIMIT_BACKOFFS = [30, 60, 120, 180]

STATE_SAVE_EVERY_PAGE = True


# ============================================================
# UTF-8
# ============================================================

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except AttributeError:
        pass


# ============================================================
# PATHS
# ============================================================

try:
    base_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    base_dir = os.getcwd()

output_dir = os.path.join(base_dir, "registry_agency_outputs")
diagnostics_dir = os.path.join(output_dir, "diagnostics")

os.makedirs(output_dir, exist_ok=True)
os.makedirs(diagnostics_dir, exist_ok=True)


OUTPUT_FILE = os.path.join(
    output_dir,
    "100_percent_valid_uics.txt"
)

QUERIES_MEMORY_FILE = os.path.join(
    output_dir,
    "processed_queries.txt"
)

STATE_FILE = os.path.join(
    output_dir,
    "savegame_uic_finder.json"
)

CONTINUE_FLAG_FILE = os.path.join(
    output_dir,
    "CONTINUE_FLAG_UIC_FINDER"
)

FAILED_QUERIES_FILE = os.path.join(
    output_dir,
    "failed_queries.txt"
)

HEARTBEAT_FILE = os.path.join(
    diagnostics_dir,
    "heartbeat.json"
)

LAST_ERROR_FILE = os.path.join(
    diagnostics_dir,
    "last_error.txt"
)


# ============================================================
# GLOBAL RUNTIME STATE
# ============================================================

state = {
    "query_idx": 0,
    "query": None,
    "page_num": 0,
    "status": "starting",
    "started_at": datetime.now().isoformat(),
    "last_activity": datetime.now().isoformat(),
    "last_url": None,
    "total_uics": 0,
    "last_new_uics": 0,
    "elapsed_seconds": 0,
    "error": None,
}

_shutdown_requested = False
_heartbeat_stop = threading.Event()
_last_heartbeat_log = 0


# ============================================================
# LOGGING
# ============================================================

def log_msg(msg):
    current_time = datetime.now().strftime("%H:%M:%S")

    elapsed = time.time() - START_TIME
    elapsed_h = int(elapsed // 3600)
    elapsed_m = int((elapsed % 3600) // 60)
    elapsed_s = int(elapsed % 60)

    print(
        f"[{current_time}] "
        f"[+{elapsed_h:02d}:{elapsed_m:02d}:{elapsed_s:02d}] "
        f"{msg}",
        flush=True
    )


# ============================================================
# STATE
# ============================================================

def touch_state():
    state["last_activity"] = datetime.now().isoformat()
    state["elapsed_seconds"] = int(time.time() - START_TIME)


def save_state(reason="unknown"):
    touch_state()

    payload = {
        **state,
        "saved_at": datetime.now().isoformat(),
        "save_reason": reason,
    }

    temp_file = STATE_FILE + ".tmp"

    try:
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(
                payload,
                f,
                ensure_ascii=False,
                indent=2
            )

        os.replace(temp_file, STATE_FILE)

    except Exception as e:
        log_msg(f"[STATE ERROR] Не успях да запазя state: {repr(e)}")


def load_state():
    if not os.path.exists(STATE_FILE):
        return

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            loaded = json.load(f)

        for key in state:
            if key in loaded:
                state[key] = loaded[key]

        log_msg(
            f"[STATE] Възстановен query_idx={state['query_idx']}, "
            f"query={state['query']}, "
            f"page={state['page_num']}"
        )

    except Exception as e:
        log_msg(
            f"[STATE] Не успях да заредя state: {repr(e)}"
        )


# ============================================================
# TIME LIMIT
# ============================================================

def time_limit_reached():
    return (time.time() - START_TIME) >= TIME_LIMIT_SECONDS


# ============================================================
# CONTINUATION FLAG
# ============================================================

def flag_for_continuation():
    try:
        with open(CONTINUE_FLAG_FILE, "w", encoding="utf-8") as f:
            f.write("CONTINUE\n")

        log_msg("[CONTINUE] Флагът за продължаване е записан.")

    except Exception as e:
        log_msg(
            f"[CONTINUE ERROR] {repr(e)}"
        )


def clear_continuation_flag():
    if os.path.exists(CONTINUE_FLAG_FILE):
        try:
            os.remove(CONTINUE_FLAG_FILE)
        except Exception:
            pass


# ============================================================
# HEARTBEAT
# ============================================================

def heartbeat_worker():
    global _last_heartbeat_log

    while not _heartbeat_stop.wait(HEARTBEAT_INTERVAL):

        try:
            touch_state()

            heartbeat = {
                "timestamp": datetime.now().isoformat(),
                "elapsed_seconds": int(time.time() - START_TIME),
                "query_idx": state["query_idx"],
                "query": state["query"],
                "page_num": state["page_num"],
                "status": state["status"],
                "last_url": state["last_url"],
                "total_uics": state["total_uics"],
                "last_new_uics": state["last_new_uics"],
            }

            with open(
                HEARTBEAT_FILE,
                "w",
                encoding="utf-8"
            ) as f:
                json.dump(
                    heartbeat,
                    f,
                    ensure_ascii=False,
                    indent=2
                )

            now = time.time()

            if now - _last_heartbeat_log >= HEARTBEAT_LOG_INTERVAL:
                _last_heartbeat_log = now

                log_msg(
                    "[HEARTBEAT] "
                    f"query_idx={state['query_idx']} | "
                    f"query={state['query']} | "
                    f"page={state['page_num']} | "
                    f"total_uics={state['total_uics']} | "
                    f"status={state['status']}"
                )

        except Exception as e:
            log_msg(
                f"[HEARTBEAT ERROR] {repr(e)}"
            )


# ============================================================
# SIGNAL HANDLING
# ============================================================

def handle_shutdown(signum, frame):
    global _shutdown_requested

    _shutdown_requested = True

    log_msg(
        f"[SHUTDOWN] Получен signal {signum}. "
        f"Опит за безопасно спиране..."
    )

    state["status"] = f"shutdown_signal_{signum}"

    try:
        save_state(
            reason=f"signal_{signum}"
        )
    except Exception:
        pass

    try:
        flag_for_continuation()
    except Exception:
        pass

    raise SystemExit(143)


signal.signal(
    signal.SIGTERM,
    handle_shutdown
)

signal.signal(
    signal.SIGINT,
    handle_shutdown
)


# ============================================================
# ATEXIT
# ============================================================

def emergency_save():
    try:
        state["status"] = "atexit"
        save_state(reason="atexit")
    except Exception:
        pass


atexit.register(emergency_save)


# ============================================================
# DEBUGGING
# ============================================================

def save_debug_artifacts(page, label):
    timestamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    safe_label = "".join(
        c if c.isalnum() or c in "-_" else "_"
        for c in str(label)
    )

    prefix = os.path.join(
        diagnostics_dir,
        f"{timestamp}_{safe_label}"
    )

    try:
        if page:
            try:
                page.screenshot(
                    path=f"{prefix}.png",
                    full_page=True
                )
            except Exception as e:
                log_msg(
                    f"[DEBUG] Screenshot failed: {repr(e)}"
                )

            try:
                html = page.content()

                with open(
                    f"{prefix}.html",
                    "w",
                    encoding="utf-8"
                ) as f:
                    f.write(html)

            except Exception as e:
                log_msg(
                    f"[DEBUG] HTML dump failed: {repr(e)}"
                )

    except Exception as e:
        log_msg(
            f"[DEBUG] Debug artifact error: {repr(e)}"
        )


def save_exception(label, exc):
    try:
        text = (
            f"Timestamp: {datetime.now().isoformat()}\n"
            f"Label: {label}\n"
            f"Exception: {repr(exc)}\n\n"
            f"STATE:\n"
            f"{json.dumps(state, ensure_ascii=False, indent=2)}\n\n"
            f"TRACEBACK:\n"
            f"{traceback.format_exc()}\n"
        )

        with open(
            LAST_ERROR_FILE,
            "w",
            encoding="utf-8"
        ) as f:
            f.write(text)

    except Exception:
        pass


# ============================================================
# SAFE PAGE HELPERS
# ============================================================

def safe_page_content(page):
    if page is None:
        return ""

    try:
        return page.content()

    except Exception as e:
        log_msg(
            f"[PAGE CONTENT ERROR] {repr(e)}"
        )
        return ""


def is_rate_limited(page, exception=None):
    if exception and "RATE_LIMIT" in str(exception):
        return True

    content = safe_page_content(page)

    return (
        "Достигнат е максимално допустимият брой заявки"
        in content
    )


# ============================================================
# UIC LOADING
# ============================================================

def load_uics():
    extracted_uics = set()

    if os.path.exists(OUTPUT_FILE):

        with open(
            OUTPUT_FILE,
            "r",
            encoding="utf-8"
        ) as f:

            for line in f:

                value = line.strip()

                if value:
                    extracted_uics.add(value)

    return extracted_uics


# ============================================================
# PROCESSED QUERIES
# ============================================================

def load_processed_queries():
    processed_queries = set()

    if os.path.exists(QUERIES_MEMORY_FILE):

        with open(
            QUERIES_MEMORY_FILE,
            "r",
            encoding="utf-8"
        ) as f:

            for line in f:

                value = line.strip()

                if value:
                    processed_queries.add(value)

    return processed_queries


def mark_query_processed(query):
    with open(
        QUERIES_MEMORY_FILE,
        "a",
        encoding="utf-8"
    ) as f:

        f.write(
            f"{query}\n"
        )


def mark_query_failed(query, reason):
    timestamp = datetime.now().isoformat()

    with open(
        FAILED_QUERIES_FILE,
        "a",
        encoding="utf-8"
    ) as f:

        f.write(
            f"{timestamp}\t{query}\t{reason}\n"
        )


# ============================================================
# BROWSER FACTORY
# ============================================================

def create_browser(p):

    browser = p.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
        ]
    )

    context = browser.new_context(
        locale="bg-BG",
        viewport={
            "width": 1280,
            "height": 720
        }
    )

    page = context.new_page()

    page.set_default_timeout(
        SELECTOR_TIMEOUT
    )

    page.set_default_navigation_timeout(
        PAGE_TIMEOUT
    )

    def on_page_error(error):
        log_msg(
            f"[PAGE ERROR] {error}"
        )

    def on_page_crash(_):
        log_msg(
            "[PAGE CRASH] Browser page reported a crash."
        )

    def on_request_failed(request):
        try:
            resource_type = request.resource_type

            if resource_type in (
                "document",
                "xhr",
                "fetch"
            ):

                log_msg(
                    "[REQUEST FAILED] "
                    f"{resource_type} | "
                    f"{request.url} | "
                    f"{request.failure}"
                )

        except Exception:
            pass

    def on_browser_disconnected():
        log_msg(
            "[BROWSER] Browser disconnected!"
        )

    page.on(
        "pageerror",
        on_page_error
    )

    page.on(
        "crash",
        on_page_crash
    )

    page.on(
        "requestfailed",
        on_request_failed
    )

    browser.on(
        "disconnected",
        on_browser_disconnected
    )

    log_msg(
        "[BROWSER] Нов Chromium процес стартиран."
    )

    return browser, context, page


# ============================================================
# MAIN
# ============================================================

def main():

    clear_continuation_flag()
    load_state()

    heartbeat_thread = threading.Thread(
        target=heartbeat_worker,
        daemon=True
    )

    heartbeat_thread.start()

    extracted_uics = load_uics()

    state["total_uics"] = len(
        extracted_uics
    )

    log_msg(
        f"[INFO] Заредени от базата: "
        f"{len(extracted_uics)} ЕИК."
    )

    processed_queries = load_processed_queries()

    log_msg(
        f"[INFO] Завършени комбинации: "
        f"{len(processed_queries)}."
    )

    # --------------------------------------------------------
    # QUERY MATRIX
    # --------------------------------------------------------

    bg_alphabet = [
        chr(i)
        for i in range(1040, 1072)
    ]

    en_alphabet = [
        chr(i)
        for i in range(65, 91)
    ]

    digits = [
        str(i)
        for i in range(10)
    ]

    all_chars = (
        bg_alphabet +
        en_alphabet +
        digits
    )

    single_chars = all_chars

    double_chars = [
        a + b
        for a in all_chars
        for b in all_chars
    ]

    bg_triples = [
        a + b + c
        for a in bg_alphabet
        for b in bg_alphabet
        for c in bg_alphabet
    ]

    bg_double_digit = [
        a + b + c
        for a in bg_alphabet
        for b in bg_alphabet
        for c in digits
    ]

    search_queries = (
        single_chars +
        double_chars +
        bg_triples +
        bg_double_digit
    )

    log_msg(
        f"[INFO] Общо комбинации: "
        f"{len(search_queries)}"
    )

    browser = None
    context = None
    page = None

    try:

        with sync_playwright() as p:

            browser, context, page = create_browser(p)

            while (
                state["query_idx"]
                < len(search_queries)
            ):

                # ------------------------------------------------
                # TIME LIMIT
                # ------------------------------------------------

                if time_limit_reached():

                    log_msg(
                        "[TIME LIMIT] "
                        "Стигнахме вътрешния лимит."
                    )

                    state["status"] = "time_limit"

                    save_state(
                        reason="time_limit"
                    )

                    flag_for_continuation()

                    break

                # ------------------------------------------------
                # QUERY
                # ------------------------------------------------

                query = search_queries[
                    state["query_idx"]
                ]

                state["query"] = query
                state["page_num"] = 0
                state["status"] = "starting_query"

                save_state(
                    reason="query_start"
                )

                if query in processed_queries:

                    log_msg(
                        f"[SKIP] '{query}' вече е обработен."
                    )

                    state["query_idx"] += 1

                    continue

                encoded_query = (
                    urllib.parse.quote(query)
                )

                url = (
                    "https://portal.registryagency.bg/"
                    "CR/Reports/VerificationPersonOrg"
                    f"?name={encoded_query}"
                    "&selectedSearchFilter=1"
                )

                state["last_url"] = url
                state["status"] = "loading_query"

                retry_count = 0

                query_finished = False

                while (
                    retry_count <
                    MAX_QUERY_RETRIES
                    and not query_finished
                ):

                    try:

                        retry_count += 1

                        log_msg(
                            f"[QUERY] "
                            f"'{query}' "
                            f"(опит {retry_count}/"
                            f"{MAX_QUERY_RETRIES})"
                        )

                        # ----------------------------------------
                        # NEW PAGE
                        # ----------------------------------------

                        if page is None:

                            browser, context, page = \
                                create_browser(p)

                        state["status"] = \
                            "goto"

                        state["last_url"] = url

                        page.goto(
                            url,
                            wait_until="domcontentloaded",
                            timeout=PAGE_TIMEOUT
                        )

                        touch_state()

                        # Give SPA time to render.
                        page.wait_for_timeout(
                            1500
                        )

                        if is_rate_limited(page):

                            raise RuntimeError(
                                "RATE_LIMIT"
                            )

                        # ----------------------------------------
                        # WAIT FOR RESULTS
                        # ----------------------------------------

                        try:

                            page.wait_for_selector(
                                "table.table-collapsible "
                                "tbody tr",
                                timeout=SELECTOR_TIMEOUT
                            )

                        except PlaywrightTimeoutError:

                            if is_rate_limited(page):

                                raise RuntimeError(
                                    "RATE_LIMIT"
                                )

                            # No table may simply mean no results.
                            log_msg(
                                f"[{query}] "
                                "Няма резултати."
                            )

                            mark_query_processed(
                                query
                            )

                            processed_queries.add(
                                query
                            )

                            state["status"] = \
                                "query_empty"

                            state["query_idx"] += 1

                            save_state(
                                reason="empty_query"
                            )

                            query_finished = True

                            break

                        # ----------------------------------------
                        # PAGINATION
                        # ----------------------------------------

                        page_num = 1

                        while True:

                            if time_limit_reached():

                                log_msg(
                                    "[TIME LIMIT] "
                                    f"Спиране при "
                                    f"query='{query}', "
                                    f"page={page_num}"
                                )

                                state["page_num"] = \
                                    page_num

                                state["status"] = \
                                    "time_limit_pagination"

                                save_state(
                                    reason="time_limit_pagination"
                                )

                                flag_for_continuation()

                                try:
                                    browser.close()
                                except Exception:
                                    pass

                                return

                            # ------------------------------------
                            # UPDATE STATE
                            # ------------------------------------

                            state["query"] = query
                            state["page_num"] = page_num
                            state["status"] = \
                                "extracting"

                            state["last_url"] = \
                                page.url

                            touch_state()

                            # ------------------------------------
                            # RATE LIMIT
                            # ------------------------------------

                            if is_rate_limited(page):

                                raise RuntimeError(
                                    "RATE_LIMIT"
                                )

                            # ------------------------------------
                            # EXTRACT
                            # ------------------------------------

                            rows = page.locator(
                                "table.table-collapsible "
                                "tbody tr"
                                ":not(.collapsible-row)"
                            ).all()

                            new_uics = 0

                            for row in rows:

                                try:

                                    cols = row.locator(
                                        "td"
                                    ).all()

                                    if len(cols) < 3:
                                        continue

                                    uic_text = (
                                        cols[2]
                                        .locator(
                                            "p.field-text"
                                        )
                                        .inner_text()
                                        .strip()
                                    )

                                    uic_clean = "".join(
                                        filter(
                                            str.isdigit,
                                            uic_text
                                        )
                                    )

                                    if (
                                        len(uic_clean) >= 9
                                        and
                                        uic_clean
                                        not in extracted_uics
                                    ):

                                        with open(
                                            OUTPUT_FILE,
                                            "a",
                                            encoding="utf-8"
                                        ) as f:

                                            f.write(
                                                f"{uic_clean}\n"
                                            )

                                        extracted_uics.add(
                                            uic_clean
                                        )

                                        new_uics += 1

                                except Exception as row_error:

                                    log_msg(
                                        "[ROW ERROR] "
                                        f"{repr(row_error)}"
                                    )

                            state["total_uics"] = \
                                len(extracted_uics)

                            state["last_new_uics"] = \
                                new_uics

                            state["status"] = \
                                "page_complete"

                            touch_state()

                            save_state(
                                reason="page_complete"
                            )

                            log_msg(
                                f"[{query} - Стр {page_num}] "
                                f"Извлечени. "
                                f"Нови: {new_uics}. "
                                f"Общо: "
                                f"{len(extracted_uics)}"
                            )

                            # ------------------------------------
                            # NEXT PAGE
                            # ------------------------------------

                            next_btn = page.locator(
                                "li.page-item.next"
                                ":not(.disabled) a"
                            ).first

                            next_exists = (
                                next_btn.count() > 0
                            )

                            if (
                                next_exists
                                and
                                next_btn.is_visible(
                                    timeout=2000
                                )
                            ):

                                state["status"] = \
                                    "clicking_next"

                                next_btn.click(
                                    timeout=5000
                                )

                                page_num += 1

                                state["page_num"] = \
                                    page_num

                                touch_state()

                                # Small wait for SPA update.
                                page.wait_for_timeout(
                                    1000
                                )

                                try:

                                    page.wait_for_selector(
                                        "table.table-collapsible "
                                        "tbody tr",
                                        state="attached",
                                        timeout=SELECTOR_TIMEOUT
                                    )

                                except PlaywrightTimeoutError:

                                    log_msg(
                                        "[PAGINATION] "
                                        "Table did not appear "
                                        "after next click."
                                    )

                                continue

                            # ------------------------------------
                            # QUERY FINISHED
                            # ------------------------------------

                            log_msg(
                                f"[SUCCESS] "
                                f"Комбинацията '{query}' "
                                f"е напълно източена."
                            )

                            mark_query_processed(
                                query
                            )

                            processed_queries.add(
                                query
                            )

                            state["status"] = \
                                "query_complete"

                            state["query_idx"] += 1
                            state["page_num"] = page_num

                            save_state(
                                reason="query_complete"
                            )

                            query_finished = True

                            break

                    except Exception as e:

                        save_exception(
                            f"query={query}, "
                            f"retry={retry_count}",
                            e
                        )

                        save_debug_artifacts(
                            page,
                            f"{query}_retry{retry_count}"
                        )

                        log_msg(
                            f"[EXCEPTION] "
                            f"query='{query}' | "
                            f"page={state['page_num']} | "
                            f"retry={retry_count} | "
                            f"{repr(e)}"
                        )

                        log_msg(
                            "[TRACEBACK]\n"
                            + traceback.format_exc()
                        )

                        # ----------------------------------------
                        # RATE LIMIT
                        # ----------------------------------------

                        if is_rate_limited(
                            page,
                            e
                        ):

                            state["status"] = \
                                "rate_limited"

                            save_state(
                                reason="rate_limit"
                            )

                            backoff_index = min(
                                retry_count - 1,
                                len(RATE_LIMIT_BACKOFFS) - 1
                            )

                            sleep_seconds = \
                                RATE_LIMIT_BACKOFFS[
                                    backoff_index
                                ]

                            log_msg(
                                "[RATE LIMIT] "
                                f"Чакаме "
                                f"{sleep_seconds}s..."
                            )

                            try:
                                browser.close()
                            except Exception:
                                pass

                            browser = None
                            context = None
                            page = None

                            time.sleep(
                                sleep_seconds
                            )

                            log_msg(
                                "[RATE LIMIT] "
                                "Стартираме чист browser."
                            )

                            browser, context, page = \
                                create_browser(p)

                            continue

                        # ----------------------------------------
                        # OTHER ERROR
                        # ----------------------------------------

                        state["status"] = \
                            "generic_error"

                        save_state(
                            reason="generic_error"
                        )

                        try:
                            browser.close()
                        except Exception:
                            pass

                        browser = None
                        context = None
                        page = None

                        if retry_count < MAX_QUERY_RETRIES:

                            sleep_seconds = \
                                retry_count * 10

                            log_msg(
                                "[RETRY] "
                                f"Чакаме "
                                f"{sleep_seconds}s "
                                "и опитваме същия query."
                            )

                            time.sleep(
                                sleep_seconds
                            )

                            browser, context, page = \
                                create_browser(p)

                            continue

                        # ----------------------------------------
                        # GIVE UP AFTER RETRIES
                        # ----------------------------------------

                        log_msg(
                            f"[FAILED] "
                            f"'{query}' "
                            f"се провали след "
                            f"{MAX_QUERY_RETRIES} опита."
                        )

                        mark_query_failed(
                            query,
                            repr(e)
                        )

                        state["query_idx"] += 1

                        state["status"] = \
                            "query_failed"

                        save_state(
                            reason="query_failed"
                        )

                        query_finished = True

                # END RETRY LOOP

            # END QUERY LOOP

            if (
                state["query_idx"]
                >= len(search_queries)
            ):

                state["status"] = \
                    "all_queries_complete"

                save_state(
                    reason="all_queries_complete"
                )

                clear_continuation_flag()

                log_msg(
                    "[КРАЙ] "
                    "Всички комбинации са обработени."
                )

    except SystemExit:
        raise

    except Exception as e:

        log_msg(
            f"[FATAL] "
            f"{repr(e)}"
        )

        log_msg(
            "[FATAL TRACEBACK]\n"
            + traceback.format_exc()
        )

        save_exception(
            "FATAL",
            e
        )

        save_debug_artifacts(
            page,
            "fatal"
        )

        state["status"] = \
            "fatal_error"

        save_state(
            reason="fatal_error"
        )

        flag_for_continuation()

        raise

    finally:

        _heartbeat_stop.set()

        try:

            if browser:
                browser.close()

        except Exception:
            pass

        save_state(
            reason="finally"
        )


if __name__ == "__main__":
    main()
