# collector_dart.py — OpenDart API로 재무제표 수집 → fin_statement 시트
# 실행: python collector_dart.py
# 필요: pip install requests pandas gspread google-auth

import os, time, tempfile, zipfile, io
import requests
import pandas as pd
import gspread
from datetime import datetime
import xml.etree.ElementTree as ET

print("=" * 60)
print("collector_dart.py START")
print("=" * 60)

# ====== 설정 ======
DART_API_KEY = "3639678c518e2b0da39794089538e1613dd00003"
FIN_SHEET = "fin_statement"
MAX_DAILY_CALLS = 9500

SERVICE_ACCOUNT_JSON = os.environ.get("SERVICE_ACCOUNT_JSON")
SHEET_ID = os.environ.get("SHEET_ID")

# ====== Google Sheets 열기 ======
def open_sheet():
    if not SERVICE_ACCOUNT_JSON or not SHEET_ID:
        raise RuntimeError("SERVICE_ACCOUNT_JSON or SHEET_ID not found")
    
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as fp:
        fp.write(SERVICE_ACCOUNT_JSON)
        sa_path = fp.name
    
    gc = gspread.service_account(filename=sa_path)
    return gc.open_by_key(SHEET_ID)

# ====== OpenDart: 종목 코드 → 기업 고유번호 매핑 ======
def get_corp_code_map():
    """전체 상장사 코드 맵 다운로드 (ZIP 파일)"""
    url = "https://opendart.fss.or.kr/api/corpCode.xml"
    params = {"crtfc_key": DART_API_KEY}
    
    try:
        print("[INFO] Downloading corp_code ZIP file...")
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code != 200:
            print(f"[ERROR] corpCode API HTTP {response.status_code}")
            return {}
        
        # ZIP 파일 압축 해제
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            xml_filename = z.namelist()[0]  # CORPCODE.xml
            with z.open(xml_filename) as f:
                xml_data = f.read()
        
        # XML 파싱
        root = ET.fromstring(xml_data)
        mapping = {}
        
        for item in root.findall('list'):
            corp_code = item.findtext('corp_code', '').strip()
            stock_code = item.findtext('stock_code', '').strip()
            
            # 상장사만 (stock_code가 6자리)
            if stock_code and len(stock_code) == 6:
                mapping[stock_code] = corp_code
        
        print(f"[OK] Loaded {len(mapping)} corp_codes")
        return mapping
        
    except Exception as e:
        print(f"[ERROR] get_corp_code_map: {e}")
        import traceback
        traceback.print_exc()
        return {}

# ====== OpenDart: 단일 회사 재무제표 조회 ======
def fetch_financials(corp_code, year, quarter):
    """
    year: "2024"
    quarter: "11011" (1분기), "11012" (반기), "11013" (3분기), "11014" (연간)
    """
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
    params = {
        "crtfc_key": DART_API_KEY,
        "corp_code": corp_code,
        "bsns_year": year,
        "reprt_code": quarter,
        "fs_div": "CFS"
    }
    
    try:
        response = requests.get(url, params=params, timeout=15)
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
        
    except Exception as e:
        return None

# ====== 메인: 전체 종목 재무제표 수집 ======
def collect_financials():
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
    
    all_vals = ws.get_all_values()
    existing = {(r[0], r[2], r[3]) for r in all_vals[1:]} if len(all_vals) > 1 else set()
    
    corp_map = get_corp_code_map()
    if not corp_map:
        print("[ERROR] Failed to load corp_code mapping")
        return
    
    current_year = datetime.now().year
    current_month = datetime.now().month
    
    required_years = {str(current_year - 1), str(current_year)}
    existing_years = {r[2] for r in all_vals[1:]} if len(all_vals) > 1 else set()
    is_initial = not required_years.issubset(existing_years) or len(existing) < 15000
    
    if is_initial:
        print(f"[INITIAL MODE] Collecting 2 years of data")
        print(f"[INFO] Current progress: {len(existing)} records")
        print(f"[INFO] Will stop at {MAX_DAILY_CALLS} API calls")
        years = [str(current_year - 1), str(current_year)]
        quarters = [
            ("11011", "Q1"),
            ("11012", "Q2"),
            ("11013", "Q3"),
            ("11014", "Q4")
        ]
    else:
        print("[INCREMENTAL MODE] Collecting current quarter only")
        years = [str(current_year)]
        if current_month <= 3:
            quarters = [("11011", "Q1")]
        elif current_month <= 6:
            quarters = [("11012", "Q2")]
        elif current_month <= 9:
            quarters = [("11013", "Q3")]
        else:
            quarters = [("11014", "Q4")]
    
    batch = []
    api_call_count = 0
    new_records = 0
    
    for ticker, corp_code in corp_map.items():
        for year in years:
            for q_code, q_name in quarters:
                if api_call_count >= MAX_DAILY_CALLS:
                    print(f"\n[LIMIT REACHED] {MAX_DAILY_CALLS} API calls used")
                    print(f"[INFO] Added {new_records} new records today")
                    if batch:
                        ws.append_rows(batch, value_input_option="RAW")
                        print(f"[OK] Final batch uploaded: {len(batch)} rows")
                    print("[INFO] Progress saved. Will resume tomorrow.")
                    return
                
                key = (ticker, year, q_name)
                if key in existing:
                    continue
                
                api_call_count += 1
                fin = fetch_financials(corp_code, year, q_code)
                
                if not fin:
                    time.sleep(0.05)
                    continue
                
                date_str = f"{year}-{q_name}"
                row = [
                    ticker, corp_code, year, q_name, date_str,
                    fin.get("매출액", 0),
                    fin.get("영업이익", 0),
                    fin.get("당기순이익", 0),
                    fin.get("자기자본", 0),
                    fin.get("부채총계", 0),
                    fin.get("자산총계", 0)
                ]
                
                batch.append(row)
                existing.add(key)
                new_records += 1
                
                if len(batch) >= 100:
                    ws.append_rows(batch, value_input_option="RAW")
                    print(f"[PROGRESS] API: {api_call_count}/{MAX_DAILY_CALLS} | New: {new_records} | Total: {len(existing)}")
                    batch.clear()
                
                time.sleep(0.05)
    
    if batch:
        ws.append_rows(batch, value_input_option="RAW")
        print(f"[OK] Final batch: {len(batch)} rows")
    
    print(f"\n{'='*60}")
    print(f"[COMPLETE] Collection finished")
    print(f"  - API calls used: {api_call_count}")
    print(f"  - New records added: {new_records}")
    print(f"  - Total records: {len(existing)}")
    
    if is_initial and len(existing) < 15000:
        print(f"  - Status: Initial collection incomplete")
        print(f"  - Next run will continue")
    else:
        print(f"  - Status: All data collected")
        print(f"  - Next run will use incremental mode")
    print(f"{'='*60}")

if __name__ == "__main__":
    print("[START] Beginning collection...")
    collect_financials()
    print("DONE")
