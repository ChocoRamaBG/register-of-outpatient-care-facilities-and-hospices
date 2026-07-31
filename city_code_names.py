import requests
import pandas as pd
import os
import sys

try:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    SCRIPT_DIR = os.getcwd()

OUTPUT_DIR = os.path.join(SCRIPT_DIR, "hisbg_outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)

output_file = os.path.join(OUTPUT_DIR, 'hisbg_city_dump.xlsx')

headers = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'access-control-allow-origin': '*',
    'origin': 'https://opendata.his.bg',
    'priority': 'u=1, i',
    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36'
}

url = 'https://registries.his.bg/api/V1/outpatientcare/getNomenclatureOutpatientCareForApiV1?typeNomenclature=city'

def fetch_and_excelify():
    print(f"Downloading the map (Cities) from: {url} ...")
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list):
                print(f"Got {len(data)} items. Converting to spreadsheet...")
                df = pd.DataFrame(data)
                df.to_excel(output_file, index=False)
                print(f"--- SUCCESS ---")
                print(f"Excel file spawned at: {output_file}")
            else:
                print("Data format is weird (not a list).")
        else:
            print(f"Server rejected us (Status {response.status_code}).")
    except Exception as e:
        print(f"Fatal error: {e}")

if __name__ == "__main__":
    fetch_and_excelify()
