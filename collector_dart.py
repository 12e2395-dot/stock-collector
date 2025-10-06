# collector_dart.py — 하이브리드 최종판 (병렬+세션+증분업데이트)

import os, sys, time, tempfile, zipfile, io, threading
import requests
import gspread
from datetime import datetime
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

# 출력 버퍼링 해제
os.environ['PYTHONUNBUFFERED'] = '1'

DART_API_KEY = "3639678c518e2b0da39794089538e1613dd00003"
FIN_SHEET = "fin_statement"
MAX_DAILY_CALLS = 20000
MAX_WORKERS = 6
DART_RPS = 4.0  # 초당 최대 요청
BATCH_SIZE = 200
TIMEOUT = 8

SERVICE_ACCOUNT_JSON = os.environ.get("SERVICE_ACCOUNT_JSON")
SHEET_ID = os.environ.get("SHEET_ID")

print("="*60, flush=True)
print("collector_dart.py HYBRID START", flush=True)
print(f"MAX_WORKERS={MAX_WORKERS}, RPS={DART_RPS}, BATCH={BATCH_SIZE}", flush=True)
print("="*60, flush=True)

# --------- 세션 + 레이트 리미터 ---------
_session = requests.Session()
_session.headers.update({
    "Connection": "keep-alive",
    "User-Agent": "dart-collector/2.0"
})

_token_lock = threading.Lock()
_tokens = DART_RPS
_last_refill = time.time()

def _acquire_token():
    """토큰 버킷: 초당 DART_RPS 요청 보장"""
    global _tokens, _last_refill
    while True:
        with _token_lock:
            now = time.time()
            elapsed = now - _last_refill
            _tokens = min(DART_RPS, _tokens + elapsed * DART_RPS)
            _last_refill = now
            
            if _tokens >= 1.0:
                _tokens -= 1.0
                return
        time.sleep(0.02)

def _get_with_retry(url, params, max_retry=3):
    """재시도 + 지수 백오프"""
    backoff = 0.5
    for attempt in range(max_retry):
        _acquire_token()
        try:
            resp = _session.get(url, params=params, timeout=TIMEOUT)
            if resp.status_code == 200:
                return resp
            if resp.status_code in (429, 500, 502, 503):
                time.sleep(backoff)
                backoff *= 2
                continue
            return None
        except:
            time.sleep(backoff)
            backoff *= 2
    return None

# --------- Google Sheets ---------
def open_sheet():
    if not SERVICE_ACCOUNT_JSON or not SHEET_ID:
        raise RuntimeError("ENV missing")
    
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as fp:
        fp.write(SERVICE_ACCOUNT_JSON)
        sa_path = fp.name
    
    gc = gspread.service_account(filename=sa_path)
    return gc.open_by_key(SHEET_ID)

_write_lock = threading.Lock()

def append_rows_safe(ws, rows, tag=""):
    """무손실 배치 업로드"""
    if not rows:
        return
    
    for attempt in range(3):
        try:
            with _write_lock:
                ws.append_rows(rows, value_input_option="RAW")
            print(f"[SAVE] {len(rows)} rows ({tag})", flush=True)
            return
        except Exception as e:
            time.sleep(1.5 ** attempt)
    
    # 분할 재시도
    if len(rows) > 1:
        mid = len(rows) // 2
        append_rows_safe(ws, rows[:mid], f"{tag}-A")
        append_rows_safe(ws, rows[mid:], f"{tag}-B")

# --------- corpCode 다운로드 ---------
def get_corp_code_map():
    url = "https://opendart.fss.or.kr/api/corpCode.xml"
    params = {"crtfc_key": DART_API_KEY}
    
    try:
        print("[1/4] Downloading corpCode...", flush=True)
        resp = _session.get(url, params=params, timeout=15)
        if resp.status_code != 200:
            print(f"[ERROR] HTTP {resp.status_code}", flush=True)
            return {}
        
        print(f"[2/4] Extracting ZIP ({len(resp.content)/1024/1024:.1f}MB)...", flush=True)
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            xml_file = z.namelist()[0]
            with z.open(xml_file) as f:
                xml_data = f.read()
        
        print("[3/4] Parsing XML...", flush=True)
        root = ET.fromstring(xml_data)
        mapping = {}
        
        for item in root.findall('list'):
            corp_code = item.findtext('corp_code', '').strip()
            stock_code = item.findtext('stock_code', '').strip()
            if stock_code and len(stock_code) == 6:
                mapping[stock_code] = corp_code
        
        print(f"[4/4] Loaded {len(mapping)} corp_codes", flush=True)
        return mapping
    except Exception as e:
        print(f"[ERROR] {e}", flush=True)
        return {}

# --------- 재무 데이터 조회 ---------
def fetch_financials(corp_code, year, quarter):
    """CFS 시도 → 실패 시 OFS 폴백"""
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
    
    for fs_div in ["CFS", "OFS"]:
        params = {
            "crtfc_key": DART_API_KEY,
            "corp_code": corp_code,
            "bsns_year": year,
            "reprt_code": quarter,
            "fs_div": fs_div
        }
        
        resp = _get_with_retry(url, params)
        if not resp:
            continue
        
        try:
            data = resp.json()
        except:
            continue
        
        if data.get("status") != "000":
            continue
        
        items = data.get("list", [])
        if not items:
            continue
        
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
        
        if result:
            return result
    
    return None

# --------- 메인 수집 ---------
def collect_financials():
    print("[STEP 1/6] Opening Sheets...", flush=True)
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
    
    print("[STEP 2/6] Loading existing...", flush=True)
    all_vals = ws.get_all_values()
    existing = {(r[0], r[2], r[3]) for r in all_vals[1:]} if len(all_vals) > 1 else set()
    print(f"  → {len(existing)} existing records", flush=True)
    
    print("[STEP 3/6] Getting corp_codes...", flush=True)
    corp_map = get_corp_code_map()
    if not corp_map:
        print("[ABORT]", flush=True)
        return
    
    current_year = datetime.now().year
    
    # 초기/증분 모드 판단
    required_years = {str(current_year - 1), str(current_year)}
    existing_years = {r[2] for r in all_vals[1:]} if len(all_vals) > 1 else set()
    is_initial = not required_years.issubset(existing_years) or len(existing) < 15000
    
    if is_initial:
        print(f"[MODE] INITIAL", flush=True)
        years = [str(current_year - 1), str(current_year)]
        quarters = [("11011", "Q1"), ("11012", "Q2"), ("11013", "Q3"), ("11014", "Q4")]
    else:
        print("[MODE] INCREMENTAL", flush=True)
        # 증분: 현재 연도 전체 분기 시도 (공시된 것만 저장됨)
        years = [str(current_year)]
        quarters = [("11011", "Q1"), ("11012", "Q2"), ("11013", "Q3"), ("11014", "Q4")]
    
    print("[STEP 4/6] Building tasks...", flush=True)
    tasks = []
    for ticker, corp_code in corp_map.items():
        for year in years:
            for q_code, q_name in quarters:
                key = (ticker, year, q_name)
                if key in existing:
                    continue
                tasks.append((ticker, corp_code, year, q_code, q_name))
    
    total = len(tasks)
    print(f"  → {total} tasks pending", flush=True)
    
    if total == 0:
        print("[DONE] No new data to collect", flush=True)
        return
    
    print(f"[STEP 5/6] Fetching (workers={MAX_WORKERS})...", flush=True)
    
    api_calls = 0
    new_records = 0
    batch = []
    batch_lock = threading.Lock()
    call_lock = threading.Lock()
    
    def worker(task):
        nonlocal api_calls, new_records
        ticker, corp_code, year, q_code, q_name = task
        
        # 일일 호출 상한
        with call_lock:
            if api_calls >= MAX_DAILY_CALLS:
                return None
        
        fin = fetch_financials(corp_code, year, q_code)
        
        with call_lock:
            api_calls += 1
        
        if not fin:
            return None
        
        row = [
            ticker, corp_code, year, q_name, f"{year}-{q_name}",
            fin.get("매출액", 0), fin.get("영업이익", 0), fin.get("당기순이익", 0),
            fin.get("자기자본", 0), fin.get("부채총계", 0), fin.get("자산총계", 0)
        ]
        
        with batch_lock:
            new_records += 1
        
        return row
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(worker, t) for t in tasks]
        completed = 0
        
        for future in as_completed(futures):
            completed += 1
            row = future.result()
            
            if row:
                with batch_lock:
                    batch.append(row)
                    
                    if len(batch) >= BATCH_SIZE:
                        append_rows_safe(ws, batch, f"batch-{completed}")
                        batch.clear()
            
            if completed % 1000 == 0:
                print(f"[PROGRESS] {completed}/{total} | API: {api_calls} | New: {new_records}", flush=True)
            
            # 상한 체크
            with call_lock:
                if api_calls >= MAX_DAILY_CALLS:
                    print(f"[LIMIT] Hit {MAX_DAILY_CALLS}", flush=True)
                    break
    
    if batch:
        append_rows_safe(ws, batch, "final")
    
    print(f"[STEP 6/6] DONE: {api_calls} calls, {new_records} new", flush=True)

if __name__ == "__main__":
    try:
        collect_financials()
    except Exception as e:
        print(f"[FATAL] {e}", flush=True)
        import traceback
        traceback.print_exc()
