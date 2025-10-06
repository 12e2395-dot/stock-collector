# collector_dart.py — OpenDart API로 재무제표 수집 → fin_statement 시트
# 실행: python collector_dart.py
# 필요: pip install requests pandas gspread google-auth

import os, time, tempfile
import requests
import pandas as pd
import gspread
from datetime import datetime

# ====== 설정 ======
DART_API_KEY = "3639678c518e2b0da39794089538e1613dd00003"
FIN_SHEET = "fin_statement"
MAX_DAILY_CALLS = 9500  # 안전 마진 (실제 제한 10,000)

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
    """전체 상장사 코드 맵 다운로드"""
    url = "https://opendart.fss.or.kr/api/corpCode.xml"
    params = {"crtfc_key": DART_API_KEY}
    
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code != 200:
            print(f"[ERROR] corpCode API failed: {response.status_code}")
            return {}
        
        import re
        pattern = r'<corp_code>(.*?)</corp_code>.*?<stock_code>(.*?)</stock_code>'
        matches = re.findall(pattern, response.text, re.DOTALL)
        
        mapping = {}
        for corp, stock in matches:
            stock = stock.strip()
            if stock and len(stock) == 6:
                mapping[stock] = corp.strip()
        
        print(f"[OK] Loaded {len(mapping)} corp_codes")
        return mapping
        
    except Exception as e:
        print(f"[ERROR] get_corp_code_map: {e}")
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
        print(f"[ERROR] fetch_financials {corp_code}: {e}")
        return None

# ====== 메인: 전체 종목 재무제표 수집 ======
def collect_financials():
    sheet = open_sheet()
    
    # fin_statement 시트 생성 또는 열기
    try:
        ws = sheet.worksheet(FIN_SHEET)
    except:
        ws = sheet.add_worksheet(title=FIN_SHEET, rows=10, cols=15)
    
    # 헤더 설정
    header = ["ticker", "corp_code", "year", "quarter", "date",
              "매출액", "영업이익", "당기순이익", 
              "자기자본", "부채총계", "자산총계"]
    
    try:
        first = ws.row_values(1)
    except:
        first = []
    
    if not first or first[0] != "ticker":
        ws.insert_row(header, 1)
    
    # 기존 데이터 조회
    all_vals = ws.get_all_values()
    existing = {(r[0], r[2], r[3]) for r in all_vals[1:]} if len(all_vals) > 1 else set()
    
    # 종목 코드 맵 다운로드
    corp_map = get_corp_code_map()
    if not corp_map:
        print("[ERROR] Failed to load corp_code mapping")
        return
    
    # 수집 모드 결정
    is_initial = len(existing) < 1000
    current_year = datetime.now().year
    current_month = datetime.now().month
    
    if is_initial:
        print(f"[INITIAL MODE] Collecting 2 years, will stop at {MAX_DAILY_CALLS} API calls")
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
    
    for ticker, corp_code in corp_map.items():
        for year in years:
            for q_code, q_name in quarters:
                # API 제한 체크
                if api_call_count >= MAX_DAILY_CALLS:
                    print(f"\n[LIMIT] Reached daily API limit ({MAX_DAILY_CALLS})")
                    print(f"[INFO] Progress saved. Run again tomorrow to continue.")
                    if batch:
                        ws.append_rows(batch, value_input_option="RAW")
                        print(f"[OK] Final batch uploaded: {len(batch)} rows")
                    return
                
                key = (ticker, year, q_name)
                if key in existing:
                    continue
                
                api_call_count += 1
                fin = fetch_financials(corp_code, year, q_code)
                
                if not fin:
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
                existing.add(key)  # 메모리에도 추가 (중복 방지)
                
                # 100개씩 배치 업로드
                if len(batch) >= 100:
                    ws.append_rows(batch, value_input_option="RAW")
                    print(f"[OK] Uploaded batch (API calls: {api_call_count}/{MAX_DAILY_CALLS})")
                    batch.clear()
                
                time.sleep(0.05)  # API 제한 (초당 20회)
        
        # 진행 상황 출력 (100개 종목마다)
        if api_call_count % 400 == 0:
            print(f"[PROGRESS] {api_call_count}/{MAX_DAILY_CALLS} API calls used")
    
    # 남은 데이터 업로드
    if batch:
        ws.append_rows(batch, value_input_option="RAW")
        print(f"[OK] Final upload: {len(batch)} rows")
    
    print(f"\n[COMPLETE] Total API calls: {api_call_count}")
    if is_initial and api_call_count >= MAX_DAILY_CALLS:
        print("[INFO] Initial collection incomplete. Will resume tomorrow.")
    else:
        print("[INFO] Collection finished successfully.")

if __name__ == "__main__":
    collect_financials()
    print("DONE")
