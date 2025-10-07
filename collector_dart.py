import os, sys, time, tempfile, zipfile, io, threading, json, re
import requests, gspread
from datetime import datetime
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===============================================
# CONFIG
# ===============================================
DART_API_KEY = "2577b83cd4832faf40689fb65ad1c51e34a1bfb3"   # 🔸 반드시 본인 키로 교체
FIN_SHEET = "fin_statement_quarter"
MAX_DAILY_CALLS = 30000
MAX_WORKERS = 6
DART_RPS = 4.0
TIMEOUT = 8
CHECKPOINT_FILE = "dart_checkpoint.json"
TREAT_OPERATING_REVENUE_AS_SALES = True  # 금융업 대응

# GitHub Actions 환경 변수 반영
MAX_DAILY_CALLS = int(os.getenv("MAX_DAILY_CALLS", MAX_DAILY_CALLS))
SAMPLE_TICKERS = int(os.getenv("SAMPLE_TICKERS", "0"))  # 0이면 전체 종목
FS_DIV_ONLY_CFS = os.getenv("FS_DIV_ONLY_CFS", "0") == "1"

SERVICE_ACCOUNT_JSON = os.environ.get("SERVICE_ACCOUNT_JSON")
SHEET_ID = os.environ.get("SHEET_ID")

# ===============================================
# HTTP + RateLimiter
# ===============================================
_session = requests.Session()
_session.headers.update({"User-Agent": "dart-collector/3.0"})
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
        except:
            pass
        time.sleep(backoff)
        backoff *= 2
    return None

# ===============================================
# Google Sheet
# ===============================================
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
    if not rows: return
    with _write_lock:
        ws.append_rows(rows, value_input_option="RAW")
    print(f"[SAVE] {len(rows)} rows ({tag})", flush=True)

# ===============================================
# Checkpoint 기능
# ===============================================
def save_checkpoint(done_list):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(list(done_list), f)

def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return set()
    with open(CHECKPOINT_FILE, encoding="utf-8") as f:
        return set(json.load(f))

# ===============================================
# corpCode Map
# ===============================================
def get_corp_code_map():
    url = "https://opendart.fss.or.kr/api/corpCode.xml"
    resp = _session.get(url, params={"crtfc_key": DART_API_KEY}, timeout=20)
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        xml_data = z.read(z.namelist()[0])
    root = ET.fromstring(xml_data)
    mapping = {}
    for item in root.findall("list"):
        corp_code = item.findtext("corp_code", "").strip()
        stock_code = item.findtext("stock_code", "").strip()
        if stock_code and len(stock_code) == 6:
            mapping[stock_code] = corp_code
    print(f"[corpCode] Loaded {len(mapping)} companies")
    return mapping

# ===============================================
# 재무 데이터 매핑
# ===============================================
REVENUE_IDS = {
    "ifrs-full_Revenue","ifrs_Revenue","dart_Revenue","dart_SalesRevenue",
    "ifrs-full_RevenueFromContractsWithCustomers","ifrs_RevenueFromContractsWithCustomers"
}
OPERATING_INCOME_IDS = {"ifrs-full_OperatingIncomeLoss","dart_OperatingIncomeLoss"}
NET_INCOME_IDS = {
    "ifrs-full_ProfitLoss","dart_NetIncomeLoss","ifrs-full_ProfitLossAttributableToOwnersOfParent"
}
ASSETS_IDS = {"ifrs-full_Assets"}
LIAB_IDS = {"ifrs-full_Liabilities"}
EQUITY_IDS = {"ifrs-full_Equity","ifrs-full_EquityAttributableToOwnersOfParent"}

REV_NAME_RE = re.compile(r"(매\s*출|매출액|영업수익|상품매출|제품매출)", re.I)
OP_NAME_RE  = re.compile(r"(영업\s*이익|영업이익\(손실\)|영업손실)", re.I)
NI_NAME_RE  = re.compile(r"(당기\s*순\s*이익|분기\s*순\s*이익|당기순손익)", re.I)
AS_NAME_RE  = re.compile(r"(자산\s*총계|자산$)", re.I)
LI_NAME_RE  = re.compile(r"(부채\s*총계|부채$)", re.I)
EQ_NAME_RE  = re.compile(r"(자기\s*자본|자본\s*총계|지배기업의소유주지분)", re.I)

def _parse_amount(v):
    if v is None: return None
    s = str(v).replace(",", "").strip()
    if s in {"", "-", "–"}: return None
    try: return int(float(s))
    except: return None

def _pick_by_id_or_name(items, id_set, name_re, used):
    for it in items:
        aid = (it.get("account_id") or it.get("account_cd") or "").strip()
        if aid and aid in id_set and f"id:{aid}" not in used:
            val = _parse_amount(it.get("thstrm_amount"))
            if val is not None: return val, f"id:{aid}"
    for it in items:
        nm = (it.get("account_nm") or "").strip()
        if name_re.search(nm) and f"nm:{nm}" not in used:
            val = _parse_amount(it.get("thstrm_amount"))
            if val is not None: return val, f"nm:{nm}"
    return None, None

def fetch_financials(corp_code, year, reprt_code):
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
    for fs_div in ("CFS", "OFS"):
        params = {
            "crtfc_key": DART_API_KEY,
            "corp_code": corp_code,
            "bsns_year": year,
            "reprt_code": reprt_code,
            "fs_div": fs_div,
        }
        resp = _get_with_retry(url, params)
        if not resp: continue
        data = resp.json()
        if data.get("status") != "000": continue
        items = data.get("list") or []
        if not items: continue

        used = set()
        revenue, rk = _pick_by_id_or_name(items, REVENUE_IDS, REV_NAME_RE, used)
        if revenue is None and TREAT_OPERATING_REVENUE_AS_SALES:
            revenue, rk = _pick_by_id_or_name(items, set(), re.compile(r"(영업수익|이자수익|보험료수익)", re.I), used)
        if rk: used.add(rk)
        op, ok = _pick_by_id_or_name(items, OPERATING_INCOME_IDS, OP_NAME_RE, used);  used.add(ok or "")
        ni, nk = _pick_by_id_or_name(items, NET_INCOME_IDS, NI_NAME_RE, used);         used.add(nk or "")
        at, ak = _pick_by_id_or_name(items, ASSETS_IDS, AS_NAME_RE, used);             used.add(ak or "")
        li, lk = _pick_by_id_or_name(items, LIAB_IDS, LI_NAME_RE, used);               used.add(lk or "")
        eq, ek = _pick_by_id_or_name(items, EQUITY_IDS, EQ_NAME_RE, used);             used.add(ek or "")

        return {
            "매출액": revenue or 0, "영업이익": op or 0, "당기순이익": ni or 0,
            "자산총계": at or 0, "부채총계": li or 0, "자기자본": eq or 0
        }
    return {"매출액":0,"영업이익":0,"당기순이익":0,"자산총계":0,"부채총계":0,"자기자본":0}

# ===============================================
# 분기 단품 계산
# ===============================================
def _v(d, key): return int(d.get(key, 0) or 0)
def _sub(a,b): return max(0,a-b)

def make_quarter_single(series):
    Q1, H1, Q3, AN = [series.get(x,{}) for x in ("Q1","H1","Q3","ANNUAL")]
    return {
        "Q1": {"매출액":_v(Q1,"매출액"), "영업이익":_v(Q1,"영업이익"), "당기순이익":_v(Q1,"당기순이익"),
               "자기자본":_v(Q1,"자기자본"), "부채총계":_v(Q1,"부채총계"), "자산총계":_v(Q1,"자산총계")},
        "Q2": {"매출액":_sub(_v(H1,"매출액"),_v(Q1,"매출액")), "영업이익":_sub(_v(H1,"영업이익"),_v(Q1,"영업이익")),
               "당기순이익":_sub(_v(H1,"당기순이익"),_v(Q1,"당기순이익")),
               "자기자본":_v(H1,"자기자본"), "부채총계":_v(H1,"부채총계"), "자산총계":_v(H1,"자산총계")},
        "Q3": {"매출액":_sub(_v(Q3,"매출액"),_v(H1,"매출액")), "영업이익":_sub(_v(Q3,"영업이익"),_v(H1,"영업이익")),
               "당기순이익":_sub(_v(Q3,"당기순이익"),_v(H1,"당기순이익")),
               "자기자본":_v(Q3,"자기자본"), "부채총계":_v(Q3,"부채총계"), "자산총계":_v(Q3,"자산총계")},
        "Q4": {"매출액":_sub(_v(AN,"매출액"),_v(Q3,"매출액")), "영업이익":_sub(_v(AN,"영업이익"),_v(Q3,"영업이익")),
               "당기순이익":_sub(_v(AN,"당기순이익"),_v(Q3,"당기순이익")),
               "자기자본":_v(AN,"자기자본"), "부채총계":_v(AN,"부채총계"), "자산총계":_v(AN,"자산총계")},
    }

# ===============================================
# 메인 수집 (자동 재개 포함)
# ===============================================
def collect_financials():
    sheet = open_sheet()
    try:
        ws = sheet.worksheet(FIN_SHEET)
    except:
        ws = sheet.add_worksheet(title=FIN_SHEET, rows=10, cols=15)
    if ws.row_values(1)[:1] != ["ticker"]:
        ws.insert_row(["ticker","corp_code","year","quarter","date","매출액","영업이익","당기순이익","자기자본","부채총계","자산총계"], 1)

    corp_map = get_corp_code_map()
    current_year = datetime.now().year
    years = [str(current_year-1), str(current_year)]
    quarters = [("11013","Q1"),("11012","H1"),("11014","Q3"),("11011","ANNUAL")]
    tasks = [(t,c,y,rc,qn) for t,c in corp_map.items() for y in years for rc,qn in quarters]
    print(f"[TASKS] {len(tasks)}")

    done = load_checkpoint()
    acc = {}
    api_calls = 0
    done_today = set()

    def worker(task):
        nonlocal api_calls
        ticker, corp_code, year, reprt_code, q_name = task
        key = f"{ticker}-{year}-{q_name}"
        if key in done or api_calls >= MAX_DAILY_CALLS:
            return None
        fin = fetch_financials(corp_code, year, reprt_code)
        api_calls += 1
        done_today.add(key)
        return (ticker, corp_code, year, q_name, fin)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for res in ex.map(worker, tasks):
            if api_calls >= MAX_DAILY_CALLS:
                print(f"[LIMIT] Hit {MAX_DAILY_CALLS}, saving checkpoint and exiting.")
                break
            if res:
                ticker, corp_code, year, q_name, fin = res
                acc.setdefault((ticker,year),{"corp_code":corp_code}).update({q_name:fin})

    save_checkpoint(done.union(done_today))

    # Q1~Q4 계산 및 시트 저장
    print("[STEP] Calculating quarter singles...")
    rows = []
    for (ticker,year), s in acc.items():
        corp_code = s.get("corp_code","")
        singles = make_quarter_single(s)
        for q in ["Q1","Q2","Q3","Q4"]:
            fin = singles[q]
            rows.append([ticker,corp_code,year,q,f"{year}-{q}",
                         fin["매출액"],fin["영업이익"],fin["당기순이익"],
                         fin["자기자본"],fin["부채총계"],fin["자산총계"]])
    if rows:
        append_rows_safe(ws, rows, "quarter-singles")
    print(f"[DONE] {len(rows)} rows saved | API calls today: {api_calls}")

if __name__ == "__main__":
    collect_financials()
