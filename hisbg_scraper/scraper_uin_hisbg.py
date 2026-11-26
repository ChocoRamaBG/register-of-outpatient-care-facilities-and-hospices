#!/usr/bin/env python3
"""BLS UIN -> HIS outpatient registry. Independent, resumable cloud scraper.

Run from any directory. The BLS workbook must already exist in the checkout.
State is a compressed SQLite snapshot; a UIN is committed only after its whole
search and all required facility detail requests succeed. No Windows paths.
"""
import argparse
import csv
import gzip
import hashlib
import json
import os
import random
import re
import shutil
import sqlite3
import sys
import tempfile
import time
import unicodedata
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import requests
from openpyxl import Workbook, load_workbook
from openpyxl.cell import WriteOnlyCell

ROOT = Path(__file__).resolve().parent.parent
BLS_FILE = ROOT / 'blsbg_scraper/blsbg_outputs/bg_medics_dynamic_2029.xlsx'
OUTPUT = ROOT / 'hisbg_scraper/hisbg_uin_outputs'
SEARCH_URL = 'https://registries.his.bg/api/V1/outpatientcare/getOutpatientCareWithFilterForApiV1'
DETAIL_URL = 'https://registries.his.bg/api/V1/outpatientcare/getOutpatientCareByNumberForApiV1'
PAGE_SIZE = 100
SEP = '|||SEP|||'
SCHEMA = 1
STATE = 'hisbg_uin_state.sqlite3.gz'
MANIFEST = 'manifest.json'
MAX_GIT_FILE = 95 * 1024 * 1024


class BudgetReached(Exception):
    pass


class APIError(Exception):
    pass


class AccessStopped(APIError):
    pass


def now():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def text(value):
    if value is None:
        return ''
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def normalize_uin(value):
    s = text(value)
    if re.fullmatch(r'[0-9]+\.0', s):
        s = s[:-2]
    return s.zfill(10) if re.fullmatch(r'[0-9]{1,10}', s) and int(s) else ''


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def digest(value):
    return hashlib.sha256(canonical(value).encode('utf-8')).hexdigest()


def file_hash(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def norm(value):
    return ' '.join(unicodedata.normalize('NFKC', text(value)).casefold().split())


def row_id(prefix, *values):
    return prefix + '_' + digest([norm(v) for v in values])[:24]


def full_name(person):
    return ' '.join(filter(None, (text(person.get(k)) for k in ('firstname', 'middlename', 'lastname'))))


def label(value):
    if isinstance(value, dict):
        return text(value.get('label') or value.get('name') or value.get('specialityLabel'))
    return text(value)


def spec_code(value):
    if isinstance(value, dict):
        return next((text(value[k]) for k in ('code', 'id', 'specialityCode', 'specialityId', 'value', 'key') if text(value.get(k))), '')
    return ''


def list_field(obj, key, required=False):
    if required and key not in obj:
        raise APIError(f'Detail response missing required field {key!r}')
    value = obj.get(key)
    if value is None:
        return []
    if not isinstance(value, list) or any(not isinstance(v, dict) for v in value):
        raise APIError(f'Unexpected structure for {key!r}')
    return value


def dedup(rows, keys):
    seen = set()
    out = []
    for row in rows:
        key = tuple(row.get(k, '') for k in keys)
        if key not in seen:
            seen.add(key)
            out.append(row)
    return out


def read_bls(path):
    if not path.is_file():
        raise ValueError(f'BLS workbook missing from checkout: {path}')
    book = load_workbook(path, read_only=True, data_only=True)
    try:
        sheet = book['Sheet1'] if 'Sheet1' in book.sheetnames else book.worksheets[0]
        rows = sheet.iter_rows(values_only=True)
        headers = [text(v).lstrip('\ufeff') for v in next(rows, ())]
        if headers.count('UIN') != 1:
            raise ValueError('Expected exactly one UIN column in the BLS workbook')
        ui = headers.index('UIN')
        ni = headers.index('Name') if 'Name' in headers else None
        codes = {}
        invalid = 0
        for row in rows:
            if not any(v is not None and text(v) for v in row):
                continue
            code = normalize_uin(row[ui] if ui < len(row) else None)
            if not code:
                invalid += 1
                continue
            name = text(row[ni]) if ni is not None and ni < len(row) else ''
            if code not in codes or not codes[code]:
                codes[code] = name
        if invalid:
            raise ValueError(f'BLS workbook contains {invalid} nonempty rows with invalid UINs; refusing an incomplete scan')
        if not codes:
            raise ValueError('BLS workbook contains no valid UINs')
        return codes
    finally:
        book.close()


class Registry:
    """Sequential requests. Never bypass access restrictions."""
    def __init__(self, deadline, search_delay=1.0, detail_delay=0.5):
        self.deadline = deadline
        self.search_delay = max(0.8, search_delay)
        self.detail_delay = max(0.25, detail_delay)
        self.session = requests.Session()
        self.session.headers.update({'Accept': 'application/json, */*',
            'Accept-Language': 'bg-BG,bg;q=0.9,en;q=0.8',
            'Origin': 'https://opendata.his.bg', 'Referer': 'https://opendata.his.bg/',
            'User-Agent': 'Mozilla/5.0 (compatible; HISBG-UIN-Registry-Client/1.0)'})

    def close(self):
        self.session.close()

    def remaining(self):
        return self.deadline - time.monotonic()

    def wait(self, delay):
        if self.remaining() < delay + 45:
            raise BudgetReached('Not enough time left for another HTTP request')
        time.sleep(delay)

    def get(self, url, params, delay):
        self.wait(delay + random.uniform(0, 0.3))
        last_error = 'unknown request error'
        for attempt in range(5):
            if self.remaining() < 45:
                raise BudgetReached('HTTP budget reached')
            response = None
            try:
                response = self.session.get(url, params=params, timeout=(10, 25), allow_redirects=False)
                status = response.status_code
                if status in (401, 403):
                    raise AccessStopped(f'HIS refused access (HTTP {status}); no bypass attempted')
                if status == 200:
                    try:
                        return response.json()
                    except ValueError:
                        raise APIError('HIS returned invalid JSON')
                if status == 429 or status in (500, 502, 503, 504):
                    last_error = f'HIS HTTP {status}'
                else:
                    raise APIError(f'HIS HTTP {status}')
            except requests.RequestException as exc:
                last_error = f'HIS {type(exc).__name__}'
            if attempt == 4:
                if response is not None and response.status_code == 429:
                    raise AccessStopped('HIS rate limit persisted after bounded retries; stop this run')
                raise APIError(last_error)
            backoff = 5 * (2 ** attempt) + random.uniform(0, 2)
            if response is not None and response.status_code == 429:
                retry_after = response.headers.get('Retry-After', '')
                try:
                    seconds = float(retry_after)
                except ValueError:
                    try:
                        seconds = (parsedate_to_datetime(retry_after) - datetime.now(timezone.utc)).total_seconds()
                    except (ValueError, TypeError, OverflowError):
                        seconds = 60
                backoff = max(backoff, seconds, 30)
                if backoff + 45 > self.remaining() or backoff > 600:
                    raise AccessStopped('HIS requested a longer rate-limit wait; defer to a later scheduled run')
            print(f'[RETRY] {last_error}; waiting {backoff:.1f}s', flush=True)
            self.wait(backoff)
        raise APIError(last_error)

    def search(self, uin):
        found = {}
        start = 0
        while True:
            payload = self.get(SEARCH_URL, {
                'searchVidOutpatientCare': '', 'searchStatus': '', 'searchCity': '',
                'searchText': '', 'searchMedicalStaff': uin,
                'start': str(start), 'length': str(PAGE_SIZE),
            }, self.search_delay)
            if not isinstance(payload, dict) or not isinstance(payload.get('registry'), list):
                raise APIError('Missing or invalid registry result; not a confirmed empty search')
            page = payload['registry']
            if len(page) > PAGE_SIZE:
                raise APIError('Unexpected search page size')
            if not page:
                break
            for item in page:
                if not isinstance(item, dict) or not text(item.get('number')):
                    raise APIError('Search result has no facility number')
                number = text(item['number'])
                if number in found and found[number] != item:
                    raise APIError('Conflicting duplicate facility in search response')
                if number in found and start:
                    raise APIError('Overlapping search pages; results cannot be trusted')
                found[number] = item
            if len(page) < PAGE_SIZE:
                break
            start += PAGE_SIZE
            if start >= 10000:
                raise APIError('Unexpectedly large UIN search; manual investigation required')
        return found

    def detail(self, number):
        payload = self.get(DETAIL_URL, {'number': number}, self.detail_delay)
        records = payload if isinstance(payload, list) else [payload]
        matches = [r for r in records if isinstance(r, dict) and text(r.get('number')) == number]
        if len(matches) != 1:
            raise APIError(f'Expected one detail record for facility {number}; got {len(matches)}')
        return matches[0]


def normalize_facility(number, detail):
    """Only store fields needed by the exports; do not store raw API payloads."""
    if text(detail.get('number')) != number:
        raise APIError('Detail record number disagrees with search')
    # Required arrays may explicitly be null, but missing keys must not silently
    # turn a partial/changed response into a successfully empty roster/address.
    staff_source = list_field(detail, 'medicalStaff', required=True)
    address_source = list_field(detail, 'address', required=True)
    facility = {
        'facility_id': number, 'facility_number': number,
        'new_facility_number': text(detail.get('oldNumber')), # legacy column alias
        'name': text(detail.get('name')), 'identifier': text(detail.get('identifier')),
        'registration_date': text(detail.get('registrationDate')).split('T', 1)[0],
        'facility_type': label(detail.get('vid')), 'status': label(detail.get('statuslabel')),
    }
    if not facility['name']:
        raise APIError('Facility detail has no name')
    owners = []
    for owner in list_field(detail, 'owners'):
        name = full_name(owner)
        if name:
            owners.append({'owner_id': row_id('owner', number, name), 'facility_id': number, 'owner_name': name})
    addresses, address_specs = [], []
    for address_index, ad in enumerate(address_source):
        raw = text(ad.get('fulladdress'))
        kind = label(ad.get('typeaddresslabel'))
        city = label(ad.get('ekatte'))
        aid = row_id('address', number, kind, raw, city, address_index)
        activities = ', '.join(filter(None, (label(a) for a in list_field(ad, 'activities'))))
        addresses.append({'address_id': aid, 'facility_id': number, 'address_type': kind,
            'full_address': raw, 'full_address_clean': ' '.join(raw.split()), 'city': city, 'region': label(ad.get('district')),
            'municipality': label(ad.get('munincipaliti') or ad.get('municipality')),
            'activities': activities})
        for s in list_field(ad, 'specialities'):
            if label(s):
                address_specs.append({'address_id': aid, 'facility_id': number, 'speciality': label(s), 'speciality_code': spec_code(s)})
    staff, staff_specs = [], []
    identified_names = {}
    for index, person in enumerate(staff_source):
        name = full_name(person)
        uin = normalize_uin(person.get('uin')) # Only an explicit roster UIN, never the searched UIN.
        source_id = next((text(person.get(k)) for k in ('id', 'personId', 'medicalStaffId') if text(person.get(k))), '')
        if not name and not uin and not source_id:
            continue
        if uin:
            did = row_id('doctor', 'uin', uin)
            identity = 'explicit_roster_uin'
        elif source_id:
            did = row_id('doctor', number, 'api_id', source_id)
            identity = 'facility_scoped_api_id'
        else:
            did = row_id('doctor', number, name, index)
            identity = 'facility_scoped_roster_row'
        if did in identified_names and norm(name) and identified_names[did] and norm(name) != identified_names[did]:
            raise APIError('Conflicting names for the same roster identity')
        identified_names[did] = norm(name) or identified_names.get(did, '')
        staff.append({'doctor_id': did, 'facility_id': number, 'medical_code': uin,
            'firstname': text(person.get('firstname')), 'middlename': text(person.get('middlename')),
            'lastname': text(person.get('lastname')), 'doctor_name': name,
            'staff_type': label(person.get('typelabel')), 'identity_basis': identity})
        for s in list_field(person, 'specialities'):
            if label(s):
                staff_specs.append({'doctor_id': did, 'facility_id': number, 'speciality': label(s), 'speciality_code': spec_code(s)})
    return {'facility': facility,
        'owners': dedup(owners, ('owner_id',)), 'addresses': dedup(addresses, ('address_id',)),
        'address_specialities': dedup(address_specs, ('address_id', 'speciality', 'speciality_code')),
        'staff': dedup(staff, ('doctor_id', 'facility_id')),
        'staff_specialities': dedup(staff_specs, ('doctor_id', 'facility_id', 'speciality', 'speciality_code'))}


def open_state(db_path, output):
    snapshot = output / STATE
    if snapshot.exists():
        with gzip.open(snapshot, 'rb') as src, db_path.open('wb') as dst:
            shutil.copyfileobj(src, dst)
    elif output.exists() and any(output.iterdir()):
        raise ValueError('Output directory is nonempty but the state snapshot is missing; refusing to overwrite previous data')
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA foreign_keys=ON')
    if snapshot.exists():
        if db.execute('PRAGMA quick_check').fetchone()[0] != 'ok' or db.execute('PRAGMA user_version').fetchone()[0] != SCHEMA:
            db.close()
            raise ValueError('Invalid or unsupported scraper state database')
    else:
        db.executescript('''
          CREATE TABLE targets (uin TEXT PRIMARY KEY, bls_name TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1, success_cycle TEXT NOT NULL DEFAULT '',
            last_checked TEXT NOT NULL DEFAULT '', attempt_day TEXT NOT NULL DEFAULT '',
            last_error TEXT NOT NULL DEFAULT '', failures INTEGER NOT NULL DEFAULT 0);
          CREATE TABLE facilities (number TEXT PRIMARY KEY, base_hash TEXT NOT NULL,
            detail_hash TEXT NOT NULL, cycle TEXT NOT NULL, data TEXT NOT NULL);
          CREATE TABLE links (uin TEXT NOT NULL REFERENCES targets(uin),
            number TEXT NOT NULL REFERENCES facilities(number),
            PRIMARY KEY(uin,number));
          PRAGMA user_version=1;
        ''')
        db.commit()
    return db


def synchronize_targets(db, codes):
    existing = {r['uin']: r for r in db.execute('SELECT uin,bls_name,active FROM targets')}
    changed = False
    with db:
        for uin, name in codes.items():
            old = existing.get(uin)
            if old is None:
                db.execute('INSERT INTO targets(uin,bls_name) VALUES (?,?)', (uin, name))
                changed = True
            elif old['bls_name'] != name or not old['active']:
                # Reappearing codes must be checked again, even in the same month.
                db.execute('UPDATE targets SET bls_name=?,active=1,success_cycle=CASE WHEN active=0 THEN \'\' ELSE success_cycle END WHERE uin=?', (name, uin))
                changed = True
        # The supplied BLS scraper retains historical UINs. Refuse a shrinking
        # workbook instead of treating a partial checkout as doctor removals.
        missing = set(existing) - set(codes)
        if missing:
            raise ValueError(f'BLS workbook is missing {len(missing)} previously tracked UINs. Restore the full source workbook before continuing.')
    return changed


def pending(db, cycle, day, retry_deferred=False):
    query = "SELECT uin FROM targets WHERE active=1 AND success_cycle<>?"
    args = [cycle]
    if not retry_deferred:
        query += " AND NOT(last_error<>'' AND attempt_day=?)"
        args.append(day)
    query += ' ORDER BY last_checked, uin'
    return [r['uin'] for r in db.execute(query, args)]


def process_uin(db, registry, uin, cycle, day):
    hits = registry.search(uin)
    # All network operations finish before the link set is replaced. Details
    # cached once per cycle, unless the search summary hash changes.
    fresh = []
    for number, base in hits.items():
        bh = digest(base)
        cached = db.execute('SELECT base_hash,cycle FROM facilities WHERE number=?', (number,)).fetchone()
        if cached and cached['cycle'] == cycle and cached['base_hash'] == bh:
            continue
        normalized = normalize_facility(number, registry.detail(number))
        fresh.append((number, bh, digest(normalized), cycle, canonical(normalized)))
    with db:
        db.executemany('''INSERT INTO facilities VALUES (?,?,?,?,?) ON CONFLICT(number) DO UPDATE SET
            base_hash=excluded.base_hash, detail_hash=excluded.detail_hash, cycle=excluded.cycle, data=excluded.data''', fresh)
        db.execute('DELETE FROM links WHERE uin=?', (uin,))
        db.executemany('INSERT INTO links VALUES (?,?)', [(uin, number) for number in hits])
        db.execute("UPDATE targets SET success_cycle=?,last_checked=?,attempt_day=?,last_error='',failures=0 WHERE uin=?", (cycle, now(), day, uin))
    return len(hits), len(fresh)


FACILITY = ['facility_id','searched_medical_code','facility_number','new_facility_number','name','identifier','registration_date','facility_type','status','detail_cycle']
STAFF = ['doctor_id','facility_id','medical_code','firstname','middlename','lastname','doctor_name','staff_type','identity_basis','detail_cycle']
STAFF_SPEC = ['doctor_id','facility_id','speciality','speciality_code']
ADDRESS = ['address_id','facility_id','address_type','full_address','full_address_clean','city','region','municipality','activities','detail_cycle']
ADDRESS_SPEC = ['address_id','facility_id','speciality','speciality_code']
OWNER = ['owner_id','facility_id','owner_name']
FLAT = ['facility_id','Търсен лекарски код','Номер на ЛЗ','Нов номер на ЛЗ','Наименование на ЛЗ','ЕИК','Дата на вписване','Вид на ЛЗ','Статус','Управители','Адреси','Медицински състав','detail_cycle']
LINK = ['searched_medical_code','bls_name','facility_id','last_checked','search_cycle','current_cycle','match_evidence','roster_doctor_name','roster_specialities']
REVIEW = ['searched_medical_code','reason','facilities_found_count','last_checked','search_cycle']
STATUS = ['uin','bls_name','search_cycle','last_checked','last_error','failures','facilities_found_count','current_cycle']
PQ = ['Match Status','Търсен УИН','Лекар в ЛЗ','Специалност','Номер на ЛЗ','Нов номер на ЛЗ','Наименование на ЛЗ','Дата на вписване','Вид на ЛЗ','Статус','Match Evidence','Search Cycle']
TABLES = {
    'facilities.csv': FACILITY, 'medical_staff.csv': STAFF, 'staff_specialities.csv': STAFF_SPEC,
    'addresses.csv': ADDRESS, 'address_specialities.csv': ADDRESS_SPEC, 'owners.csv': OWNER,
    'facilities_flat.csv': FLAT, 'uin_facility_links.csv': LINK, 'manual_review.csv': REVIEW,
    'search_status.csv': STATUS,
}


def spec_text(rows):
    return ', '.join(dict.fromkeys(r['speciality'] + (f" ({r['speciality_code']})" if r['speciality_code'] else '') for r in rows))


def name_key(value):
    # Only remove recognizable title prefixes, not substrings inside names.
    value = norm(value)
    return re.sub(r'^(?:(?:д-р|доктор|проф\.?|доц\.?|професор|доцент)\s+)+', '', value)


def link_evidence(data, uin, bls_name):
    matching = [r for r in data['staff'] if r['medical_code'] == uin]
    if len(matching) == 1:
        evidence, row = 'exact_roster_uin', matching[0]
    else:
        key = name_key(bls_name)
        candidates = [r for r in data['staff'] if key and name_key(r['doctor_name']) == key]
        if len(candidates) != 1:
            return 'his_search_result_only', '', ''
        row = candidates[0]
        evidence = 'conflicting_roster_uin' if row['medical_code'] else 'unique_exact_bls_name_review'
    specs = [s for s in data['staff_specialities'] if s['doctor_id'] == row['doctor_id']]
    return evidence, row['doctor_name'], spec_text(specs)


def export_rows(db, cycle):
    tables = {k: [] for k in TABLES}
    pq_rows = []
    # Current latest successful searches, including stale rows clearly labelled.
    facility_records = {r['number']: (json.loads(r['data']), r['cycle']) for r in db.execute('SELECT * FROM facilities WHERE number IN (SELECT number FROM links) ORDER BY number')}
    grouped = {}
    for link in db.execute('SELECT l.uin,l.number,t.bls_name,t.success_cycle,t.last_checked FROM links l JOIN targets t ON t.uin=l.uin WHERE t.active=1 ORDER BY l.uin,l.number'):
        uin, number = link['uin'], link['number']
        data, dc = facility_records[number]
        evidence, name, specs = link_evidence(data, uin, link['bls_name'])
        current = link['success_cycle'] == cycle and dc == cycle
        row = {'searched_medical_code': uin, 'bls_name': link['bls_name'], 'facility_id': number,
            'last_checked': link['last_checked'], 'search_cycle': link['success_cycle'], 'current_cycle': int(current),
            'match_evidence': evidence, 'roster_doctor_name': name, 'roster_specialities': specs}
        tables['uin_facility_links.csv'].append(row)
        grouped.setdefault(number, []).append(uin)
        f = data['facility']
        tables['facilities.csv'].append({**f, 'searched_medical_code': uin, 'detail_cycle': dc})
        pq_rows.append({'Match Status': 'MATCH' if current and evidence == 'exact_roster_uin' else 'REVIEW',
            'Търсен УИН': uin, 'Лекар в ЛЗ': name, 'Специалност': specs,
            'Номер на ЛЗ': f['facility_number'], 'Нов номер на ЛЗ': f['new_facility_number'],
            'Наименование на ЛЗ': f['name'], 'Дата на вписване': f['registration_date'],
            'Вид на ЛЗ': f['facility_type'], 'Статус': f['status'], 'Match Evidence': evidence,
            'Search Cycle': link['success_cycle']})
    for number, uins in sorted(grouped.items()):
        data, dc = facility_records[number]
        f = data['facility']
        for target, key in [('medical_staff.csv','staff'), ('staff_specialities.csv','staff_specialities'),
                            ('addresses.csv','addresses'), ('address_specialities.csv','address_specialities'), ('owners.csv','owners')]:
            tables[target].extend({**r, 'detail_cycle': dc} for r in data[key])
        address_strings = []
        for a in data['addresses']:
            specs = spec_text([s for s in data['address_specialities'] if s['address_id'] == a['address_id']])
            address_strings.append(f"{a['address_type']}: {a['full_address']}" + (f' [{specs}]' if specs else ''))
        staff_strings = []
        for d in data['staff']:
            specs = spec_text([s for s in data['staff_specialities'] if s['doctor_id'] == d['doctor_id']])
            staff_strings.append(f"{d['doctor_name']} ({specs or 'Без специалност'})")
        tables['facilities_flat.csv'].append({'facility_id': number, 'Търсен лекарски код': SEP.join(uins),
            'Номер на ЛЗ': f['facility_number'], 'Нов номер на ЛЗ': f['new_facility_number'],
            'Наименование на ЛЗ': f['name'], 'ЕИК': f['identifier'], 'Дата на вписване': f['registration_date'],
            'Вид на ЛЗ': f['facility_type'], 'Статус': f['status'],
            'Управители': SEP.join(o['owner_name'] for o in data['owners']), 'Адреси': SEP.join(address_strings),
            'Медицински състав': SEP.join(staff_strings), 'detail_cycle': dc})
    for r in db.execute('''SELECT t.*, (SELECT count(*) FROM links l WHERE l.uin=t.uin) AS n
                          FROM targets t WHERE active=1 ORDER BY uin'''):
        current = r['success_cycle'] == cycle
        tables['search_status.csv'].append({'uin': r['uin'], 'bls_name': r['bls_name'], 'search_cycle': r['success_cycle'],
            'last_checked': r['last_checked'], 'last_error': r['last_error'], 'failures': r['failures'],
            'facilities_found_count': r['n'] if r['success_cycle'] else '', 'current_cycle': int(current)})
        if r['success_cycle'] and r['n'] != 1:
            tables['manual_review.csv'].append({'searched_medical_code': r['uin'], 'reason': 'No Result' if r['n'] == 0 else 'Multiple Facilities (Unsure Match)',
                'facilities_found_count': r['n'], 'last_checked': r['last_checked'], 'search_cycle': r['success_cycle']})
    return tables, pq_rows


def write_csv(path, columns, rows):
    with path.open('w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore', lineterminator='\n')
        writer.writeheader()
        writer.writerows(rows)


def add_sheet(book, title, columns, rows):
    # Do not truncate rows or cells to fit Excel. CSV remains authoritative.
    if len(rows) > 1048575:
        raise ValueError(f'Excel sheet {title} exceeds the row limit; use CSV exports')
    sheet = book.create_sheet(title)
    sheet.append(columns)
    for row in rows:
        cells = []
        for col in columns:
            value = text(row.get(col))
            if len(value) > 32767:
                raise ValueError(f'Excel field {col} exceeds the cell length limit; use CSV exports')
            cell = WriteOnlyCell(sheet, value=value)
            cell.data_type = 's'  # UINs remain strings; no formula evaluation.
            cells.append(cell)
        sheet.append(cells)


def publish(db, output, cycle, day, reason, allow_continue, source_hash):
    tables, pq = export_rows(db, cycle)
    counts = {name: len(rows) for name, rows in tables.items()}
    total = db.execute('SELECT count(*) FROM targets WHERE active=1').fetchone()[0]
    done = db.execute('SELECT count(*) FROM targets WHERE active=1 AND success_cycle=?', (cycle,)).fetchone()[0]
    eligible = len(pending(db, cycle, day))
    status = {'cycle': cycle, 'updated_at': now(), 'source_workbook_sha256': source_hash,
        'total_uins': total, 'completed_this_cycle': done, 'pending_uins': total-done,
        'eligible_now': eligible, 'deferred_uins': total-done-eligible, 'stop_reason': reason,
        'continue_required': bool(allow_continue and eligible), 'rows': counts,
        'confirmed_current_links': sum(r['Match Status']=='MATCH' for r in pq)}
    with tempfile.TemporaryDirectory(prefix='.uin-publish-', dir=output.parent) as tmp:
        stage = Path(tmp)
        # Use SQLite's backup API so the compressed snapshot is consistent.
        backup = stage / 'snapshot.sqlite3'
        dest = sqlite3.connect(backup)
        try:
            db.backup(dest)
        finally:
            dest.close()
        with backup.open('rb') as src, (stage / STATE).open('wb') as raw:
            with gzip.GzipFile(fileobj=raw, filename='', mode='wb', mtime=0) as dst:
                shutil.copyfileobj(src, dst)
        backup.unlink()
        for filename, columns in TABLES.items():
            write_csv(stage / filename, columns, tables[filename])
        # Separate match workbook, first sheet compatible with the pasted PQ.
        # Normalized CSVs retain full data; optional Excel omitted if too big.
        workbook = Workbook(write_only=True)
        excel_name = 'uin_facility_doctor_match.xlsx'
        sheet_defs = [('Matches', PQ, pq), ('Hospitals', FACILITY, tables['facilities.csv']),
            ('Addresses', ADDRESS, tables['addresses.csv']), ('Doctors', STAFF, tables['medical_staff.csv']),
            ('Search_Links', LINK, tables['uin_facility_links.csv']), ('Manual_Review', REVIEW, tables['manual_review.csv']),
            ('Search_Status', STATUS, tables['search_status.csv']), ('Owners', OWNER, tables['owners.csv']),
            ('Staff_Specialities', STAFF_SPEC, tables['staff_specialities.csv']), ('Address_Specialities', ADDRESS_SPEC, tables['address_specialities.csv'])]
        # Excel cannot store XML control characters; leave those values intact in CSV.
        illegal_excel = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f]')
        oversized = any(len(rows) > 1048575 or any(len(text(row.get(col))) > 32767 or illegal_excel.search(text(row.get(col))) for row in rows for col in cols) for _,cols,rows in sheet_defs)
        if not oversized:
            for title, cols, rows in sheet_defs:
                add_sheet(workbook, title, cols, rows)
            workbook.save(stage / excel_name)
            if (stage / excel_name).stat().st_size >= MAX_GIT_FILE:
                (stage / excel_name).unlink()
                oversized = True
        if oversized:
            status['excel_omitted_reason'] = 'Workbook exceeds Excel/Git limits or has unsupported control characters; normalized CSVs remain authoritative.'
            # Preserve the actual match table even if XLSX is unavailable.
        write_csv(stage / 'uin_facility_doctor_match.csv', PQ, pq)
        processed = db.execute('SELECT uin FROM targets WHERE active=1 AND success_cycle=? ORDER BY uin', (cycle,))
        (stage / 'processed_codes.txt').write_text(''.join(r['uin']+'\n' for r in processed), encoding='utf-8')
        failed = db.execute("SELECT uin FROM targets WHERE active=1 AND success_cycle<>? AND last_error<>'' ORDER BY uin", (cycle,))
        (stage / 'failed_codes.txt').write_text(''.join(r['uin']+'\n' for r in failed), encoding='utf-8')
        (stage / 'status.json').write_text(json.dumps(status, indent=2, ensure_ascii=False)+'\n', encoding='utf-8')
        for p in stage.iterdir():
            if p.stat().st_size >= MAX_GIT_FILE:
                raise ValueError(f'{p.name} exceeds the 95 MiB safety threshold. No new snapshot published; use sharded data or external storage.')
        manifest = {'schema': SCHEMA, 'files': {p.name: file_hash(p) for p in sorted(stage.iterdir())}}
        (stage / MANIFEST).write_text(json.dumps(manifest, indent=2)+'\n', encoding='utf-8')
        output.mkdir(parents=True, exist_ok=True)
        # Manifest last: workflow verifies every file before committing the batch.
        for p in sorted(stage.iterdir()):
            if p.name != MANIFEST:
                os.replace(p, output / p.name)
        if oversized:
            (output / excel_name).unlink(missing_ok=True)
        os.replace(stage / MANIFEST, output / MANIFEST)
    print(json.dumps(status, ensure_ascii=False, indent=2), flush=True)
    return status


def verify_output(output):
    manifest = json.loads((output / MANIFEST).read_text(encoding='utf-8'))
    if manifest.get('schema') != SCHEMA:
        raise ValueError('Unsupported output manifest schema')
    files = manifest.get('files', {})
    required = set(TABLES) | {STATE, 'status.json', 'uin_facility_doctor_match.csv', 'processed_codes.txt', 'failed_codes.txt'}
    if not required <= set(files):
        raise ValueError('Output manifest lacks required files')
    actual = {p.name for p in output.iterdir()}
    if actual != set(files) | {MANIFEST}:
        raise ValueError('Unexpected or missing file in managed output directory')
    for name, expected in files.items():
        if Path(name).name != name or not (output/name).is_file() or file_hash(output/name) != expected:
            raise ValueError(f'Output integrity check failed for {name}')
    print('[VERIFY] All checkpoint and output file hashes match.', flush=True)


def run(args, registry_factory=Registry, cycle=None, day=None):
    clock = time.monotonic()
    cycle = cycle or datetime.now(timezone.utc).strftime('%Y-%m')
    day = day or datetime.now(timezone.utc).date().isoformat()
    codes = read_bls(args.bls)
    source_hash = file_hash(args.bls)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.output.exists() and any(args.output.iterdir()):
        if not (args.output / MANIFEST).is_file():
            raise ValueError('Existing output directory has no integrity manifest; restore the last verified Git checkpoint.')
        verify_output(args.output)
    print(f'[BLS] {len(codes):,} unique UINs; refresh cycle {cycle}', flush=True)
    with tempfile.TemporaryDirectory(prefix='hisbg-uin-') as tmp:
        db_path = Path(tmp) / 'state.sqlite3'
        db = open_state(db_path, args.output)
        registry = None
        try:
            changed = synchronize_targets(db, codes)
            queue = pending(db, cycle, day, args.retry_deferred)
            prev_status = {}
            if (args.output / 'status.json').exists():
                prev_status = json.loads((args.output / 'status.json').read_text(encoding='utf-8'))
            if not queue and not changed and prev_status.get('cycle') == cycle and prev_status.get('source_workbook_sha256') == source_hash:
                print('[INFO] No eligible UINs. Completed searches are not repeated this month.', flush=True)
                return 0
            # Export and git steps have a separate time allowance in the YAML.
            deadline = clock + args.max_seconds
            registry = registry_factory(deadline)
            reason = 'complete_or_deferred'
            allow_continue = False
            consecutive_errors = 0
            exit_code = 0
            attempted = 0
            completed_in_batch = 0
            for uin in queue:
                if attempted >= args.max_uins or time.monotonic() + 45 >= deadline:
                    reason = 'batch_or_runtime_limit'
                    allow_continue = True
                    break
                attempted += 1
                try:
                    count, fetched = process_uin(db, registry, uin, cycle, day)
                    consecutive_errors = 0
                    completed_in_batch += 1
                    print(f'[{attempted}/{len(queue)}] UIN {uin}: {count} facilities, {fetched} detail fetches', flush=True)
                except BudgetReached:
                    reason = 'runtime_limit'
                    allow_continue = True
                    break
                except APIError as exc:
                    error = str(exc)[:500]
                    with db:
                        db.execute('UPDATE targets SET attempt_day=?,last_error=?,failures=failures+1 WHERE uin=?', (day,error,uin))
                    print(f'[DEFERRED] UIN {uin}: {error}', flush=True)
                    consecutive_errors += 1
                    if isinstance(exc, AccessStopped) or consecutive_errors >= 3:
                        reason = 'access_or_repeated_api_error'
                        exit_code = 1
                        break
                except KeyboardInterrupt:
                    reason = 'interrupted'
                    exit_code = 130
                    break
            # If some failed but other work remains only because of a batch limit,
            # failed UINs are excluded from automatic same-day continuations.
            # Avoid unbounded self-dispatch if a run cannot finish even one UIN.
            if allow_continue and completed_in_batch == 0:
                allow_continue = False
                reason = 'runtime_or_batch_limit_without_progress'
            status = publish(db, args.output, cycle, day, reason, allow_continue, source_hash)
            verify_output(args.output)
            if status['pending_uins'] and not status['continue_required'] and exit_code == 0:
                print('[INFO] Some UINs remain deferred; the next daily run retries them.', flush=True)
            return exit_code
        finally:
            if registry is not None:
                registry.close()
            db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bls', type=Path, default=BLS_FILE)
    parser.add_argument('--output', type=Path, default=OUTPUT)
    parser.add_argument('--max-seconds', type=int, default=18000)
    parser.add_argument('--max-uins', type=int, default=5000)
    parser.add_argument('--retry-deferred', action='store_true', help='Retry UINs already deferred today; use manually, not for continuation')
    parser.add_argument('--verify-output', action='store_true')
    args = parser.parse_args()
    try:
        if args.verify_output:
            verify_output(args.output)
            return 0
        if args.max_seconds < 60 or args.max_seconds > 19000 or args.max_uins < 1:
            raise ValueError('Invalid batch limits (max-seconds 60..19000; max-uins >=1)')
        return run(args)
    except (ValueError, OSError, sqlite3.Error, APIError, KeyError) as exc:
        print(f'[FATAL] {exc}', file=sys.stderr, flush=True)
        return 2


if __name__ == '__main__':
    sys.exit(main())
