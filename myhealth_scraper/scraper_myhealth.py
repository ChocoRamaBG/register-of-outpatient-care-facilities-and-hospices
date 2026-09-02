import time
import os
import re
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Гарантирано запазване в правилната директория
try:
    output_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    output_dir = os.getcwd()

os.makedirs(output_dir, exist_ok=True)
output_filename = os.path.join(output_dir, "myhealth_headless.xlsx")

# Предварително създаване на файла, за да не гърми GitHub Actions Artifact Upload
# в случай, че страницата няма данни за скрапване.
if not os.path.exists(output_filename):
    empty_df = pd.DataFrame(columns=[
        "Име", "Специалност", "Рейтинг_Инфо", "Първи свободен час (Общо)", 
        "Телефони", "НЗОК", "Биография", "URL", "Timestamp", "Цени", "Застрахователи"
    ])
    empty_df.to_excel(output_filename, index=False)

options = webdriver.ChromeOptions()
options.add_argument('--headless=new')
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument('--log-level=3')
options.add_argument('--window-size=1920,1080')
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')

driver = webdriver.Chrome(options=options)

def get_text_safe(xpath, search_context=None, default="-"):
    try:
        ctx = search_context if search_context else driver
        element = ctx.find_element(By.XPATH, xpath)
        return element.text.strip().replace('\n', ' ')
    except:
        return default

def scrape_insurances_myhealth():
    try:
        logos = driver.find_elements(By.XPATH, "//div[contains(@class, 'practice__insurance-logos')]//img")
        insurances = [img.get_attribute("alt").strip() for img in logos if img.get_attribute("alt")]
        return ", ".join(insurances) if insurances else "-"
    except:
        return "-"

def scrape_prices_myhealth():
    try:
        price_items = driver.find_elements(By.XPATH, "//div[contains(@class, 'practice__pricing-text--item')]")
        found_prices = []
        for item in price_items:
            try:
                name = item.find_element(By.XPATH, ".//p[contains(@class, 'dummy--reason__name')]").text.strip()
                val = item.find_element(By.XPATH, ".//p[contains(@class, 'dummy--reason__price')]").text.strip()
                found_prices.append(f"{name}: {val}")
            except:
                continue
        return " | ".join(found_prices) if found_prices else "-"
    except:
        return "-"

def get_coordinates_from_map_link(context=None):
    try:
        ctx = context if context else driver
        map_link = ctx.find_element(By.XPATH, ".//a[contains(@href, 'google.com/maps') and contains(@href, 'daddr')]")
        href = map_link.get_attribute("href")
        match = re.search(r"daddr=([\d\.]+),([\d\.]+)", href)
        if match:
            return f"{match.group(1)}, {match.group(2)}"
        return "-"
    except:
        return "-"

def get_full_biography():
    try:
        hidden_bio_el = driver.find_elements(By.ID, "hidden-profile-resume")
        if hidden_bio_el:
            text = driver.execute_script("return arguments[0].textContent;", hidden_bio_el[0]).strip()
            if text:
                return text

        try:
            read_more_btn = driver.find_element(By.CSS_SELECTOR, "button[data-hidden-text-id='profile-resume']")
            if read_more_btn.is_displayed():
                driver.execute_script("arguments[0].click();", read_more_btn)
                time.sleep(0.5) 
        except:
            pass 

        bio_el = driver.find_element(By.ID, "profile-resume")
        return bio_el.text.strip()
    except:
        return "-"

def scrape_practices_detailed():
    practices_data = []
    try:
        free_dates_map = {}
        try:
            dates_container = driver.find_element(By.CLASS_NAME, "dummy--detailed-profile-card__practices")
            titles = dates_container.find_elements(By.CLASS_NAME, "dummy--detailed-profile-card__practices-title")
            dates = dates_container.find_elements(By.CLASS_NAME, "dummy--detailed-profile-card__practices-fa")
            
            if len(titles) == len(dates):
                for i in range(len(titles)):
                    t_text = titles[i].text.strip()
                    d_raw = dates[i].get_attribute("data-date")
                    d_text = dates[i].text.strip()
                    
                    final_date = d_raw.replace("T", " ").split("+")[0] if d_raw else d_text
                    key = re.sub(r'\s+', '', t_text.lower())[:50]
                    free_dates_map[key] = final_date
        except:
            pass

        workplaces = driver.find_elements(By.CLASS_NAME, "doctor-details__workplace-item")
        
        for wp in workplaces:
            try:
                h_name = wp.find_element(By.CLASS_NAME, "doctor-details__workplace-item-title").text.strip()
                h_addr = wp.find_element(By.CLASS_NAME, "doctor-details__workplace-item-address").text.strip()
                h_coords = get_coordinates_from_map_link(wp)
                h_date = "Няма свободни часове"
                
                check_str_full = re.sub(r'\s+', '', (h_name + h_addr).lower())
                check_str_addr = re.sub(r'\s+', '', h_addr.lower())
                
                for k, v in free_dates_map.items():
                    if k in check_str_full or check_str_full in k:
                        h_date = v
                        break
                    if check_str_addr and len(check_str_addr) > 5 and check_str_addr in k:
                        h_date = v
                        break
                
                practices_data.append({
                    "Hospital": h_name,
                    "Address": h_addr,
                    "First_Date": h_date,
                    "Coords": h_coords
                })
            except Exception:
                continue

    except Exception as e:
        print(f"Practice scrape error: {e}")
        
    return practices_data

def get_all_first_available_dates_summary():
    dates_found = []
    try:
        date_elements = driver.find_elements(By.CLASS_NAME, "dummy--detailed-profile-card__practices-fa")
        for date_el in date_elements:
            raw_date = date_el.get_attribute("data-date")
            if raw_date:
                clean_date = raw_date.replace("T", " ").split("+")[0]
                dates_found.append(clean_date)
            else:
                txt = date_el.text.strip()
                if txt: dates_found.append(txt)
    except: pass

    if not dates_found:
        try:
            btns = driver.find_elements(By.CLASS_NAME, "dummy--booking-component__first_available")
            for btn in btns:
                raw_date = btn.get_attribute("data-dummy-first-available")
                if raw_date:
                    clean_date = raw_date.replace("T", " ").split("+")[0]
                    dates_found.append(clean_date)
        except: pass
        
    return " | ".join(dates_found) if dates_found else "Няма свободни часове"

def scrape_doctor_profile_myhealth(url):
    driver.get(url)
    try:
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CLASS_NAME, "doctor-header")))
        time.sleep(1.0)
        
        doc_name = get_text_safe("//div[contains(@class, 'doctor-header')]//h2/a")
        specialty = get_text_safe("//div[contains(@class, 'doctor-speciality')]")
        rating_text = get_text_safe("//span[contains(@class, 'doctor-rating-score_count')]")
        bio = get_full_biography()

        nzok = "Не"
        try:
            if driver.find_elements(By.XPATH, "//span[contains(@class, 'ww-nzok')]"):
                nzok = "Да"
        except: pass

        phones = []
        try:
            phone_links = driver.find_elements(By.XPATH, "//a[contains(@href, 'tel:')]")
            phones = [lnk.get_attribute("href").replace("tel:", "") for lnk in phone_links]
        except: pass
        phone_str = ", ".join(list(set(phones))) if phones else "-"

        doc_info = {
            "Име": doc_name,
            "Специалност": specialty,
            "Рейтинг_Инфо": rating_text,
            "Телефони": phone_str,
            "НЗОК": nzok,
            "Биография": bio[:1000],
            "URL": url,
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Цени": scrape_prices_myhealth(),
            "Застрахователи": scrape_insurances_myhealth(),
            "Първи свободен час (Общо)": get_all_first_available_dates_summary()
        }
        
        practices = scrape_practices_detailed()
        
        if not practices:
             practices = [{"Hospital": "-", "Address": "-", "First_Date": "-", "Coords": "-"}]

        for i, p in enumerate(practices):
            idx = i + 1
            if idx > 5: break
            doc_info[f"Hospital_{idx}"] = p["Hospital"]
            doc_info[f"Address_{idx}"] = p["Address"]
            doc_info[f"First_Free_{idx}"] = p["First_Date"]
            doc_info[f"Coords_{idx}"] = p["Coords"]

        for i in range(len(practices) + 1, 4):
             doc_info[f"Hospital_{i}"] = "-"
             doc_info[f"Address_{i}"] = "-"
             doc_info[f"First_Free_{i}"] = "-"
             doc_info[f"Coords_{i}"] = "-"

        if doc_name == "-" or not doc_name:
            print(f"WARNING: Doctor name not found for {url}")

        return doc_info

    except Exception as e:
        print(f"Profile scrape error for {url}: {e}")
        return None

def save_to_excel(data):
    if not data: return
    try:
        df = pd.DataFrame(data)
        
        cols = ["Име", "Специалност", "Рейтинг_Инфо", "Първи свободен час (Общо)", "Телефони", "НЗОК", "Биография", "URL", "Timestamp", "Цени", "Застрахователи"]
        remaining_cols = [c for c in df.columns if c not in cols]
        remaining_cols.sort()
        final_cols = cols + remaining_cols
        
        df = df.reindex(columns=final_cols)

        if os.path.exists(output_filename):
            try:
                existing_df = pd.read_excel(output_filename)
                df = pd.concat([existing_df, df], ignore_index=True)
            except: pass
            
        df.to_excel(output_filename, index=False)
        print(f"Saved {len(data)} record(s) to {output_filename}")
    except Exception as e:
        print(f"Excel write error: {e}")


page = 164

try:
    while True:
        target_url = f"https://myhealth.bg/search/?page={page}"
        print(f"\n🚀 Отиваме на страница {page}: {target_url}")
        driver.get(target_url)
        
        try:
            # Изчакваме специфично да се зареди поне един лекарски профил (или практиките).
            # Ако след 10 секунди не се появи такъв линк, значи страницата е празна.
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//a[contains(@href, '/lekar/') and not(contains(@href, 'search'))]"))
            )
            all_links = driver.find_elements(By.TAG_NAME, "a")
        except Exception:
            # Ако изтече времето, отиваме директно на проверка и спираме изпълнението.
            all_links = []
            
        doctor_urls = []
        for link in all_links:
            href = link.get_attribute("href")
            if href and ("/lekar/" in href or "/practices/lekar/" in href) and "search" not in href:
                doctor_urls.append(href)
        
        doctor_urls = list(set(doctor_urls))
        
        if not doctor_urls:
            print("No doctor profile links found. Stopping.")
            break
        
        print(f"Found {len(doctor_urls)} doctor profile(s).")
        
        for u in doctor_urls:
            print(f"Scraping: {u}")
            res = scrape_doctor_profile_myhealth(u)
            if res:
                save_to_excel([res])
        
        page += 1
        
except Exception as e:
    print(f"Page loop error: {e}")

finally:
    try:
        driver.quit()
        print("Chrome WebDriver stopped.")
    except:
        pass
    print(f"Finished. Output file: {output_filename}")
