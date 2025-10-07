# collector_dart.py — RAW 수집(기본, 증분/정정 반영) + 요약 모드(옵션)

import os, sys, time, tempfile, zipfile, io, threading
import requests
import gspread
from datetime import datetime
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

os.environ['PYTHONUNBUFFERED'] = '1'

DART_API_KEY = os.environ.get("DART_API_KEY", "2577b83cd4832faf40689fb65ad1c51e34a1bfb3")
FIN_SHEET = "fin_statement"     # (구) 요약본
RAW_SHEET = "fin_raw_all"       # (신) 원천행

MAX_DAILY_CALLS = 28000
MAX_WORKERS = 6
DART_RPS = 4.0
BATCH_SIZE = 200
TIMEOUT = 8

SERVICE_ACCOUNT_JSON = os.environ.get("SERVICE_ACCOUNT_JSON")
SHEET_ID = os.environ.get("SHEET_ID")

print("="*60, flush=True)
print("collector_dart.py (RAW default, incremental + restatement-aware)", flush=True)
print(f"MAX_WORKERS={MAX_WORKERS}, RPS={DART_RPS}, BATCH={BATCH_SIZE}", flush=True)
print("="*60, flush=True)

# --------- 공통 상수 ---------
REPRT = {"Q1":"11013","H1":"11012","Q3":"11014","ANNUAL":"11011"}
REPRT_LABEL = {v:k for k,v in REPRT.items()}

# --------- 세션 + 레이트 리미터 ---------
_session = requests.Session()
_session.headers.update({
    "Connection": "keep-alive",
    "User-Agent": "dart-collector/3.2"
})

_token_lock = threading.Lock()
_tokens = DART_RPS
_last_refill = time.time()

def _acquire_token():
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
    backoff = 0.5
    for _ in range(max_retry):
        _acquire_token()
        try:
            resp = _session.get(url, params=params, timeout=TIMEOUT)
            if resp.status_code == 200:
                return resp
            if resp.status_code in (429,500,502,503):
                time.sleep(backoff); backoff *= 2; continue
            return None
        except:
            time.sleep(backoff); backoff *= 2
    return None

# --------- Google Sheets ---------
def open_sheet():
    if not SERVICE_ACCOUNT_JSON or not SHEET_ID:
        raise RuntimeError("ENV missing: SERVICE_ACCOUNT_JSON/SHEET_ID")
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as fp:
        fp.write(SERVICE_ACCOUNT_JSON)
        sa_path = fp.name
    gc = gspread.service_account(filename=sa_path)
    return gc.open_by_key(SHEET_ID)

_write_lock = threading.Lock()
def append_rows_safe(ws, rows, tag=""):
    if not rows: return
    for attempt in range(3):
        try:
            with _write_lock:
                ws.append_rows(rows, value_input_option="RAW")
            if tag: print(f"[SAVE] {len(rows)} rows ({tag})", flush=True)
            return
        except Exception:
            time.sleep(1.5 ** attempt)
    if len(rows) > 1:
        mid = len(rows)//2
        append_rows_safe(ws, rows[:mid], f"{tag}-A")
        append_rows_safe(ws, rows[mid:], f"{tag}-B")

# --------- 상장 종목 필터링 ---------
def get_listed_tickers():
    try:
        from pykrx import stock
        kospi = stock.get_market_ticker_list(market="KOSPI")
        kosdaq = stock.get_market_ticker_list(market="KOSDAQ")
        return set(kospi + kosdaq)
    except Exception as e:
        print(f"[WARN] Failed to get listed tickers: {e}", flush=True)
        return set()

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
            corp_code = item.findtext('corp_code','').strip()
            stock_code = item.findtext('stock_code','').strip()
            if stock_code and len(stock_code) == 6:
                mapping[stock_code] = corp_code
        print(f"[4/4] Loaded {len(mapping)} corp_codes", flush=True)
        return mapping
    except Exception as e:
        print(f"[ERROR] {e}", flush=True)
        return {}

# --------- 헬퍼 ---------
def _to_int_or_blank(s):
    if s is None: return ""
    s = str(s).replace(",","").strip()
    if s == "": return ""
    try: return int(s)
    except: return ""

# --------- RAW: 원천행 가져오기 ---------
def fetch_financials_rows(corp_code, year, reprt_code):
    """회사/연도/보고서코드에 대해 CFS/OFS의 list 전체를 원천행으로 반환"""
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
    out_rows = []
    for fs_div in ("CFS","OFS"):
        params = {
            "crtfc_key": DART_API_KEY,
            "corp_code": corp_code,
            "bsns_year": year,
            "reprt_code": reprt_code,
            "fs_div": fs_div,
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
        for it in (data.get("list") or []):
            out_rows.append({
                "fs_div": fs_div,
                "rcept_no": it.get("rcept_no",""),
                "reprt_code": it.get("reprt_code",""),
                "bsns_year": it.get("bsns_year",""),
                "sj_div": it.get("sj_div",""),
                "sj_nm": it.get("sj_nm",""),
                "account_id": (it.get("account_id") or it.get("account_cd") or "").strip(),
                "account_nm": (it.get("account_nm") or "").strip(),
                "account_detail": (it.get("account_detail") or "").strip(),
                "thstrm_nm": it.get("thstrm_nm",""),
                "thstrm_amount": _to_int_or_blank(it.get("thstrm_amount")),
                "thstrm_add_amount": _to_int_or_blank(it.get("thstrm_add_amount")),
                "frmtrm_nm": it.get("frmtrm_nm",""),
                "frmtrm_amount": _to_int_or_blank(it.get("frmtrm_amount")),
                "frmtrm_q_nm": it.get("frmtrm_q_nm",""),
                "frmtrm_q_amount": _to_int_or_blank(it.get("frmtrm_q_amount")),
                "frmtrm_add_amount": _to_int_or_blank(it.get("frmtrm_add_amount")),
                "bfefrmtrm_nm": it.get("bfefrmtrm_nm",""),
                "bfefrmtrm_amount": _to_int_or_blank(it.get("bfefrmtrm_amount")),
                "ord": it.get("ord",""),
                "currency": it.get("currency",""),
            })
    return out_rows

# --------- RAW: 시트 적재(증분/정정 반영) ---------
def collect_raw_all():
    print("[RAW] Opening Sheets...", flush=True)
    sheet = open_sheet()
    try:
        ws = sheet.worksheet(RAW_SHEET)
    except:
        ws = sheet.add_worksheet(title=RAW_SHEET, rows=10, cols=30)

    header = [
        "ticker","corp_code","year","reprt_code","reprt_label",
        "fs_div","rcept_no","bsns_year","sj_div","sj_nm",
        "account_id","account_nm","account_detail",
        "thstrm_nm","thstrm_amount","thstrm_add_amount",
        "frmtrm_nm","frmtrm_amount","frmtrm_q_nm","frmtrm_q_amount","frmtrm_add_amount",
        "bfefrmtrm_nm","bfefrmtrm_amount",
        "ord","currency"
    ]
    try:
        first = ws.row_values(1)
    except:
        first = []
    if not first or first[0] != "ticker":
        ws.insert_row(header, 1)

    print("[RAW] Loading corp map...", flush=True)
    corp_map = get_corp_code_map()
    if not corp_map:
        print("[ABORT] no corp map", flush=True); return

    listed = get_listed_tickers()
    if listed:
        corp_map = {t:c for t,c in corp_map.items() if t in listed}
        print(f"[RAW] Listed only: {len(corp_map)}", flush=True)

    # 증분 범위 결정 (행 수/연도 기반)
    all_vals = ws.get_all_values()
    H = {h:i for i,h in enumerate(all_vals[0])}
    existing_rows = len(all_vals) - 1
    existing_years = set()
    if existing_rows > 0:
        for r in all_vals[1:]:
            if len(r) > H.get("year", 2):
                existing_years.add(str(r[H["year"]]).strip())

    cy = datetime.now().year
    need_years = {str(cy-1), str(cy)}
    is_initial = (existing_rows < 10000) or (not need_years.issubset(existing_years))
    if is_initial:
        print("[MODE] INITIAL (RAW)", flush=True)
        years = [str(cy-1), str(cy)]
    else:
        print("[MODE] INCREMENTAL (RAW)", flush=True)
        years = [str(cy)]
    reprts = ["11013","11012","11014","11011"]  # Q1/H1/Q3/ANNUAL

    # 중복/정정 관리: rcept_no까지 포함해서 저장 (같은 항목도 접수번호 다르면 새 행)
    seen = set()
    if existing_rows > 0:
        for r in all_vals[1:]:
            try:
                k = (
                    r[H["ticker"]], r[H["year"]], r[H["reprt_code"]],
                    r[H["fs_div"]], r[H["sj_div"]], r[H["ord"]],
                    r[H["account_id"]], r[H["account_nm"]], r[H["rcept_no"]],
                )
                seen.add(k)
            except:
                continue
    print(f"[RAW] Seen keys: {len(seen)}", flush=True)

    BATCH, uploaded = [], 0
    def flush(tag=""):
        nonlocal BATCH, uploaded
        if not BATCH: return
        append_rows_safe(ws, BATCH, tag)
        uploaded += len(BATCH)
        BATCH.clear()

    total = len(corp_map)*len(years)*len(reprts)
    done = 0
    for ticker, corp_code in corp_map.items():
        for y in years:
            for rc in reprts:
                rows = fetch_financials_rows(corp_code, y, rc)
                if rows:
                    for it in rows:
                        k = (ticker, y, rc, it["fs_div"], it["sj_div"], str(it["ord"]), it["account_id"], it["account_nm"], it["rcept_no"])
                        if k in seen: 
                            continue
                        seen.add(k)
                        BATCH.append([
                            ticker, corp_code, y, rc, REPRT_LABEL.get(rc,""),
                            it["fs_div"], it["rcept_no"], it["bsns_year"], it["sj_div"], it["sj_nm"],
                            it["account_id"], it["account_nm"], it["account_detail"],
                            it["thstrm_nm"], it["thstrm_amount"], it["thstrm_add_amount"],
                            it["frmtrm_nm"], it["frmtrm_amount"], it["frmtrm_q_nm"], it["frmtrm_q_amount"], it["frmtrm_add_amount"],
                            it["bfefrmtrm_nm"], it["bfefrmtrm_amount"],
                            it["ord"], it["currency"]
                        ])
                        if len(BATCH) >= BATCH_SIZE:
                            flush(f"{done}/{total}")
                done += 1
                if done % 500 == 0:
                    flush(f"{done}/{total}")
    flush("final")
    print(f"[RAW] DONE: uploaded rows={uploaded}", flush=True)

# --------- (옵션) 요약 수집 유지 ---------
def fetch_financials(corp_code, year, reprt_code):
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
    ACCOUNT_MAP = {
        "ifrs-full_Revenue":"매출액","ifrs_Revenue":"매출액","dart_Revenue":"매출액","dart_SalesRevenue":"매출액","ifrs-full_Sales":"매출액",
        "dart_OperatingIncomeLoss":"영업이익","ifrs-full_OperatingIncomeLoss":"영업이익","ifrs_OperatingIncomeLoss":"영업이익",
        "ifrs-full_ProfitLoss":"당기순이익","ifrs_ProfitLoss":"당기순이익","dart_NetIncomeLoss":"당기순이익","ifrs-full_ProfitLossAttributableToOwnersOfParent":"당기순이익",
        "ifrs-full_Assets":"자산총계","ifrs-full_Liabilities":"부채총계","ifrs-full_Equity":"자기자본","ifrs-full_EquityAttributableToOwnersOfParent":"자기자본"
    }
    NAME_MAP = {
        "매출액":"매출액","매출":"매출액",
        "영업이익":"영업이익","영업이익(손실)":"영업이익",
        "당기순이익":"당기순이익","분기순이익":"당기순이익","반기순이익":"당기순이익",
        "자산총계":"자산총계","자산":"자산총계","부채총계":"부채총계","부채":"부채총계","자기자본":"자기자본","자본총계":"자기자본",
    }
    for fs_div in ("CFS","OFS"):
        params = {"crtfc_key": DART_API_KEY,"corp_code": corp_code,"bsns_year": year,"reprt_code": reprt_code,"fs_div": fs_div}
        resp = _get_with_retry(url, params)
        if not resp: continue
        data = resp.json()
        if data.get("status") != "000": continue
        out = {k:"" for k in ["매출액","영업이익","당기순이익","자산총계","부채총계","자기자본"]}
        for it in (data.get("list") or []):
            aid = it.get("account_id") or it.get("account_cd") or ""
            anm = (it.get("account_nm") or "").strip()
            v = it.get("thstrm_amount")
            try: v = int(str(v).replace(",","").strip())
            except: continue
            key = ACCOUNT_MAP.get(aid) or NAME_MAP.get(anm)
            if key and out[key] == "": out[key] = v
        if any(out.values()):
            out["_fs_div"] = fs_div
            return out
    return {k:"" for k in ["매출액","영업이익","당기순이익","자산총계","부채총계","자기자본"]}

def collect_financials():
    # (요약 모드 유지 — 필요시 사용)
    print("[STEP 1/7] Opening Sheets...", flush=True)
    sheet = open_sheet()
    try: ws = sheet.worksheet(FIN_SHEET)
    except: ws = sheet.add_worksheet(title=FIN_SHEET, rows=10, cols=15)

    header = ["ticker","corp_code","year","quarter","date","매출액","영업이익","당기순이익","자기자본","부채총계","자산총계"]
    try: first = ws.row_values(1)
    except: first = []
    if not first or first[0] != "ticker": ws.insert_row(header, 1)

    all_vals = ws.get_all_values()
    existing = {}
    if len(all_vals) > 1:
        for r in all_vals[1:]:
            if len(r) >= 8:
                existing[(r[0], r[2], r[3])] = (r[7] if len(r)>7 else "0")

    corp_map = get_corp_code_map()
    if not corp_map: print("[ABORT]", flush=True); return
    listed = get_listed_tickers()
    if listed: corp_map = {k:v for k,v in corp_map.items() if k in listed}

    cy = datetime.now().year
    required = {str(cy-1), str(cy)}
    existing_years = {r[2] for r in all_vals[1:]} if len(all_vals)>1 else set()
    is_initial = (len(existing) < 10000) or (not required.issubset(existing_years))
    years = [str(cy-1), str(cy)] if is_initial else [str(cy)]
    quarters = [("11013","Q1"),("11012","H1"),("11014","Q3"),("11011","ANNUAL")]

    tasks = []
    for t, c in corp_map.items():
        for y in years:
            for q_code, q_name in quarters:
                key = (t, y, q_name)
                val = (existing.get(key) or "").strip()
                if key not in existing or val in ("","0"):
                    tasks.append((t,c,y,q_code,q_name))

    api_calls = 0; new_records = 0; batch = []
    lock = threading.Lock(); call_lock = threading.Lock()
    def worker(task):
        nonlocal api_calls, new_records
        t,c,y,q_code,q_name = task
        with call_lock:
            if api_calls >= MAX_DAILY_CALLS: return None
        fin = fetch_financials(c,y,q_code)
        with call_lock: api_calls += 1
        if not fin: return None
        row = [t,c,y,q_name,f"{y}-{q_name}", fin.get("매출액",""),fin.get("영업이익",""),fin.get("당기순이익",""),
               fin.get("자기자본",""),fin.get("부채총계",""),fin.get("자산총계","")]
        with lock: new_records += 1
        return row

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(worker, t) for t in tasks]
        completed = 0
        for fut in as_completed(futures):
            completed += 1
            row = fut.result()
            if row:
                batch.append(row)
                if len(batch) >= BATCH_SIZE:
                    append_rows_safe(ws, batch, f"batch-{completed}")
                    batch.clear()
            if completed % 1000 == 0:
                print(f"[PROGRESS] {completed}/{len(tasks)} | API:{api_calls} | New:{new_records}", flush=True)
            with call_lock:
                if api_calls >= MAX_DAILY_CALLS: print(f"[LIMIT] Hit {MAX_DAILY_CALLS}", flush=True); break
    if batch: append_rows_safe(ws, batch, "final")
    print(f"[DONE] API:{api_calls} New:{new_records}", flush=True)

# --------- main ---------
if __name__ == "__main__":
    try:
        mode = (sys.argv[1] if len(sys.argv)>1 else "raw").lower()
        if mode in ("raw","all","raw_all"):
            collect_raw_all()
        elif mode in ("fin","statement","summary"):
            collect_financials()
        else:
            collect_raw_all()
    except Exception as e:
        print(f"[FATAL] {e}", flush=True)
        import traceback; traceback.print_exc()
