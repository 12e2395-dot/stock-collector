# collector_dart.py — 속도 최적화

import os, time, tempfile, zipfile, io
import requests
import gspread
from datetime import datetime
import xml.etree.ElementTree as ET

DART_API_KEY = "3639678c518e2b0da39794089538e1613dd00003"
FIN_SHEET = "fin_statement"
MAX_DAILY_CALLS = 30000

SERVICE_ACCOUNT_JSON = os.environ.get("SERVICE_ACCOUNT_JSON")
SHEET_ID = os.environ.get("SHEET_ID")

print("="*60)
print("collector_dart.py START")
print("="*60)

def open_sheet():
    if not SERVICE_ACCOUNT_JSON or not SHEET_ID:
        raise RuntimeError("ENV missing")
    
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as fp:
        fp.write(SERVICE_ACCOUNT_JSON)
        sa_path = fp.name
    
    gc = gspread.service_account(filename=sa_path)
    return gc.open_by_key(SHEET_ID)

def get_corp_code_map():
    url = "https://opendart.fss.or.kr/api/corpCode.xml"
    params = {"crtfc_key": DART_API_KEY}
    
    try:
        print("[1/4] Downloading corpCode ZIP...")
        response = requests.get(url, params=params, timeout=15)
        
        if response.status_code != 200:
            print(f"[ERROR] HTTP {response.status_code}")
            return {}
        
        print(f"[2/4] Extracting ZIP ({len(response.content)/1024/1024:.1f}MB)...")
        
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            xml_filename = z.namelist()[0]
            with z.open(xml_filename) as f:
                xml_data = f.read()
        
        print(f"[3/4] Parsing XML...")
        root = ET.fromstring(xml_data)
        mapping = {}
        
        for item in root.findall('list'):
            corp_code = item.findtext('corp_code', '').strip()
            stock_code = item.findtext('stock_code', '').strip()
            
            if stock_code and len(stock_code) == 6:
                mapping[stock_code] = corp_code
        
        print(f"[4/4] Loaded {len(mapping)} corp_codes")
        return mapping
        
    except Exception as e:
        print(f"[ERROR] {e}")
        return {}

def fetch_financials(corp_code, year, quarter):
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
    params = {
        "crtfc_key": DART_API_KEY,
        "corp_code": corp_code,
        "bsns_year": year,
        "reprt_code": quarter,
        "fs_div": "CFS"
    }
    
    try:
        response = requests.get(url, params=params, timeout=5)  # 15초→5초
        if response.status_code != 200:
            return None
        
        data = response.json()
        if data.get("status") != "000":
            return None
        
        items = data.get("list", [])
        if not items:
            return None
        
        result = {}
        for item in items:
            account = item.get("account_nm", "")
            value = item.get("thstrm_amount", "0").replace(",", "")
            
            try:
                value = int(value) if value else 0
            except:
                value = 0
            
            if "매출액" in account and "매출액" not in result:
                result["매출액"] = value
            elif "영업이익" in account and "영업이익" not in result:
                result["영업이익"] = value
            elif "당기순이익" in account and "당기순이익" not in result:
                result["당기순이익"] = value
            elif "자본총계" in account or "자기자본" in account:
                result["자기자본"] = value
            elif "부채총계" in account:
                result["부채총계"] = value
            elif "자산총계" in account:
                result["자산총계"] = value
        
        return result
    except:
        return None

def collect_financials():
    print("[STEP 1/5] Opening Sheets...")
    sheet = open_sheet()
    
    try:
        ws = sheet.worksheet(FIN_SHEET)
    except:
        ws = sheet.add_worksheet(title=FIN_SHEET, rows=10, cols=15)
    
    header = ["ticker", "corp_code", "year", "quarter", "date",
              "매출액", "영업이익", "당기순이익", 
              "자기자본", "부채총계", "자산총계"]
    
    try:
        first = ws.row_values(1)
    except:
        first = []
    
    if not first or first[0] != "ticker":
        ws.insert_row(header, 1)
    
    print("[STEP 2/5] Loading existing...")
    all_vals = ws.get_all_values()
    existing = {(r[0], r[2], r[3]) for r in all_vals[1:]} if len(all_vals) > 1 else set()
    print(f"  → {len(existing)} existing records")
    
    print("[STEP 3/5] Getting corp_codes...")
    corp_map = get_corp_code_map()
    if not corp_map:
        print("[ABORT] No corp_codes")
        return
    
    current_year = datetime.now().year
    current_month = datetime.now().month
    
    required_years = {str(current_year - 1), str(current_year)}
    existing_years = {r[2] for r in all_vals[1:]} if len(all_vals) > 1 else set()
    is_initial = not required_years.issubset(existing_years) or len(existing) < 15000
    
    if is_initial:
        print(f"[MODE] INITIAL ({len(existing)} records)")
        years = [str(current_year - 1), str(current_year)]
        quarters = [("11011", "Q1"), ("11012", "Q2"), ("11013", "Q3"), ("11014", "Q4")]
    else:
        print("[MODE] INCREMENTAL")
        years = [str(current_year)]
        if current_month <= 3:
            quarters = [("11011", "Q1")]
        elif current_month <= 6:
            quarters = [("11012", "Q2")]
        elif current_month <= 9:
            quarters = [("11013", "Q3")]
        else:
            quarters = [("11014", "Q4")]
    
    print(f"[STEP 4/5] Collection: {len(corp_map)} tickers")
    
    batch = []
    api_calls = 0
    new_records = 0
    last_print = time.time()
    
    for ticker, corp_code in corp_map.items():
        for year in years:
            for q_code, q_name in quarters:
                if api_calls >= MAX_DAILY_CALLS:
                    if batch:
                        ws.append_rows(batch, value_input_option="RAW")
                    print(f"\n[LIMIT] {api_calls} calls, {new_records} new")
                    return
                
                key = (ticker, year, q_name)
                if key in existing:
                    continue
                
                api_calls += 1
                fin = fetch_financials(corp_code, year, q_code)
                
                if fin:
                    row = [
                        ticker, corp_code, year, q_name, f"{year}-{q_name}",
                        fin.get("매출액", 0), fin.get("영업이익", 0), fin.get("당기순이익", 0),
                        fin.get("자기자본", 0), fin.get("부채총계", 0), fin.get("자산총계", 0)
                    ]
                    batch.append(row)
                    existing.add(key)
                    new_records += 1
                
                if len(batch) >= 200:  # 100→200으로 증가
                    ws.append_rows(batch, value_input_option="RAW")
                    batch.clear()
                    
                    # 5초마다 진행 상황 출력
                    if time.time() - last_print > 5:
                        print(f"  → API: {api_calls}/{MAX_DAILY_CALLS} | New: {new_records}")
                        last_print = time.time()
                
                time.sleep(0.03)  # 0.05→0.03으로 단축 (초당 33회)
    
    if batch:
        ws.append_rows(batch, value_input_option="RAW")
    
    print(f"\n[STEP 5/5] DONE: {api_calls} calls, {new_records} new records")

if __name__ == "__main__":
    try:
        collect_financials()
    except Exception as e:
        print(f"[FATAL] {e}")
        import traceback
        traceback.print_exc()
