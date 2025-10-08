# collector_dart.py  —  FINAL (2025-10-08)
import os, sys, time, tempfile, zipfile, io, threading, json, re
import requests, gspread
from datetime import datetime
import xml.etree.ElementTree as ET
from zipfile import BadZipFile
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG (env로 오버라이드 가능)
# =========================
DART_API_KEY = os.getenv("DART_API_KEY", "")
FIN_SHEET = os.getenv("FIN_SHEET", "fin_statement_quarter")
MAX_DAILY_CALLS = int(os.getenv("MAX_DAILY_CALLS", "30000"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "6"))
DART_RPS = float(os.getenv("DART_RPS", "4.0"))
TIMEOUT = int(os.getenv("TIMEOUT", "8"))
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "dart_checkpoint.json")
TREAT_OPERATING_REVENUE_AS_SALES = os.getenv("TREAT_OPERATING_REVENUE_AS_SALES", "1") == "1"
FS_DIV_ONLY_CFS = os.getenv("FS_DIV_ONLY_CFS", "1") == "1"
YEARS_ENV = os.getenv("YEARS", "")
RUN_REPAIR_ZERO = os.getenv("RUN_REPAIR_ZERO", "1") == "1"
DEBUG_TICKER = os.getenv("DEBUG_TICKER", "")
SAMPLE_TICKERS = int(os.getenv("SAMPLE_TICKERS", "0"))
SKIP_IF_EXISTS = os.getenv("SKIP_IF_EXISTS", "1") == "1"

SERVICE_ACCOUNT_JSON = os.environ.get("SERVICE_ACCOUNT_JSON")
SHEET_ID = os.environ.get("SHEET_ID")

# =========================
# HTTP + RateLimiter
# =========================
_session = requests.Session()
_session.headers.update({
    "User-Agent": "dart-collector/4.2",
    "Connection": "keep-alive",
    "Accept": "*/*"
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
    for attempt in range(max_retry):
        _acquire_token()
        try:
            resp = _session.get(url, params=params, timeout=TIMEOUT)
            if resp.status_code == 200:
                return resp
            if resp.status_code in (429, 500, 502, 503):
                time.sleep(backoff); backoff *= 2; continue
        except requests.Timeout:
            time.sleep(backoff); backoff *= 2; continue
        except Exception:
            time.sleep(backoff); backoff *= 2; continue
    return None

# =========================
# Google Sheet IO
# =========================
def open_sheet():
    if not SERVICE_ACCOUNT_JSON or not SHEET_ID:
        raise RuntimeError("ENV missing: SERVICE_ACCOUNT_JSON / SHEET_ID")
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

def update_row_safe(ws, row_idx, row_values):
    rng = f"A{row_idx}:K{row_idx}"
    with _write_lock:
        ws.update(rng, [row_values], value_input_option="RAW")

# =========================
# Checkpoint
# =========================
def save_checkpoint(done_list):
    try:
        with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
            json.dump(list(done_list), f)
    except Exception as e:
        print(f"[WARN] save_checkpoint failed: {e}", flush=True)

def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return set()
    try:
        with open(CHECKPOINT_FILE, encoding="utf-8") as f:
            return set(json.load(f))
    except Exception as e:
        print(f"[WARN] load_checkpoint failed: {e}, starting fresh", flush=True)
        return set()

# =========================
# 시트에서 corp_map 폴백 생성
# =========================
def corp_map_from_sheet(ws):
    vals = ws.get_all_values()
    if not vals or len(vals) < 2:
        return {}
    header = vals[0]
    try:
        it = header.index("ticker")
        ic = header.index("corp_code")
    except ValueError:
        return {}
    out = {}
    for row in vals[1:]:
        if len(row) <= max(it, ic): continue
        t = row[it].strip(); c = row[ic].strip()
        if len(t) == 6 and c:
            out[t] = c
    if out:
        print(f"[FALLBACK] corp_map from sheet: {len(out)}", flush=True)
    return out

# =========================
# corpCode Map (+ 상장필터, 강건화)
# =========================
def get_corp_code_map(ws):
    if not DART_API_KEY:
        m = corp_map_from_sheet(ws)
        if m:
            print("[WARN] DART_API_KEY missing — using sheet mapping.", flush=True)
            return m
        raise RuntimeError("DART_API_KEY is not set and sheet has no corp_code mapping.")

    url = "https://opendart.fss.or.kr/api/corpCode.xml"

    # 몇 번은 그냥 재시도 (ZIP 직접 파싱)
    last_err = None
    for attempt in range(4):
        try:
            resp = _session.get(url, params={"crtfc_key": DART_API_KEY}, timeout=20)
            if not resp or resp.status_code != 200:
                last_err = f"HTTP {getattr(resp,'status_code',None)}"
                time.sleep(0.5 * (2 ** attempt)); continue

            # 바로 ZIP 시도
            try:
                with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                    xml_data = z.read(z.namelist()[0])
                root = ET.fromstring(xml_data)
                mapping = {}
                for item in root.findall("list"):
                    corp_code = (item.findtext("corp_code") or "").strip()
                    stock_code = (item.findtext("stock_code") or "").strip()
                    if stock_code and len(stock_code) == 6:
                        mapping[stock_code] = corp_code
                print(f"[corpCode] Loaded {len(mapping)} companies", flush=True)
                # PyKRX 상장사 필터
                try:
                    from pykrx import stock
                    listed = set(stock.get_market_ticker_list(market="KOSPI") +
                                 stock.get_market_ticker_list(market="KOSDAQ"))
                    before = len(mapping)
                    mapping = {k: v for k, v in mapping.items() if k in listed}
                    print(f"[FILTER] Listed only: {len(mapping)} / {before}", flush=True)
                except Exception as e:
                    print(f"[WARN] PyKRX filter failed: {e}", flush=True)
                return mapping
            except BadZipFile as e:
                # 진짜 ZIP이 아니면 JSON 에러 메시지 시도
                try:
                    j = resp.json()
                    last_err = f"DART says: {j}"
                except Exception:
                    ct = resp.headers.get("Content-Type")
                    preview = resp.content[:200]
                    last_err = f"Non-zip & non-json. CT={ct} Preview={preview!r}"
                time.sleep(0.5 * (2 ** attempt)); continue
        except Exception as e:
            last_err = str(e)
            time.sleep(0.5 * (2 ** attempt)); continue

    # 여기까지 왔으면 API쪽 문제 — 시트 폴백 사용
    m = corp_map_from_sheet(ws)
    if m:
        print(f"[WARN] corpCode API failed after retries — using sheet mapping. ({last_err})", flush=True)
        return m
    raise RuntimeError(f"corpCode failed: {last_err}")

# =========================
# 매핑 규칙 (ID/이름)
# =========================
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

REV_NAME_RE = re.compile(
    r"(매\s*출|매출액|영업수익|상품매출|제품매출|수수료수익|이자수익|보험료수익|수익\(영업\))",
    re.I
)
OP_NAME_RE  = re.compile(r"(영업\s*이익|영업이익\(손실\)|영업손실)", re.I)
NI_NAME_RE  = re.compile(r"(당기\s*순\s*이익|분기\s*순\s*이익|당기순손익|지배주주지분순이익)", re.I)
AS_NAME_RE  = re.compile(r"(자산\s*총계|자산$)", re.I)
LI_NAME_RE  = re.compile(r"(부채\s*총계|부채$)", re.I)
EQ_NAME_RE  = re.compile(r"(자기\s*자본|자본\s*총계|지배기업의소유주지분)", re.I)

def _parse_amount(v):
    if v is None: return None
    s = str(v).replace(",", "").strip()
    if s in {"", "-", "–"}: return None
    try:
        return int(float(s))
    except:
        return None

def _pick_by_id_or_name(items, id_set, name_re, used):
    # id 우선
    for it in items:
        aid = (it.get("account_id") or it.get("account_cd") or "").strip()
        if aid and (aid in id_set) and (f"id:{aid}" not in used):
            val = _parse_amount(it.get("thstrm_amount"))
            if val is not None:
                return val, f"id:{aid}"
    # 이름 보조
    for it in items:
        nm = (it.get("account_nm") or "").strip()
        if name_re.search(nm) and (f"nm:{nm}" not in used):
            val = _parse_amount(it.get("thstrm_amount"))
            if val is not None:
                return val, f"nm:{nm}"
    return None, None

# =========================
# DART 조회 (호출 수 반환)
# =========================
def fetch_financials(corp_code, year, reprt_code, ticker=None):
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
    calls_made = 0

    def _one(fs_div):
        nonlocal calls_made
        params = {
            "crtfc_key": DART_API_KEY,
            "corp_code": corp_code,
            "bsns_year": year,
            "reprt_code": reprt_code,
            "fs_div": fs_div,
        }
        resp = _get_with_retry(url, params)
        calls_made += 1
        if not resp:
            return None
        try:
            data = resp.json()
        except:
            return None
        if data.get("status") != "000":
            return None
        items = data.get("list") or []
        if not items:
            return None

        used = set()
        revenue, rk = _pick_by_id_or_name(items, REVENUE_IDS, REV_NAME_RE, used)
        if revenue is None and TREAT_OPERATING_REVENUE_AS_SALES:
            revenue, rk = _pick_by_id_or_name(
                items, set(), re.compile(r"(영업수익|이자수익|보험료수익|수수료수익)", re.I), used
            )
        if rk: used.add(rk)

        op, ok = _pick_by_id_or_name(items, OPERATING_INCOME_IDS, OP_NAME_RE, used);  used.add(ok or "")
        ni, nk = _pick_by_id_or_name(items, NET_INCOME_IDS, NI_NAME_RE, used);         used.add(nk or "")
        at, ak = _pick_by_id_or_name(items, ASSETS_IDS, AS_NAME_RE, used);             used.add(ak or "")
        li, lk = _pick_by_id_or_name(items, LIAB_IDS, LI_NAME_RE, used);               used.add(lk or "")
        eq, ek = _pick_by_id_or_name(items, EQUITY_IDS, EQ_NAME_RE, used);             used.add(ek or "")

        if ticker and ticker == DEBUG_TICKER:
            print(f"[DBG] {ticker} y={year} rc={reprt_code} fs={fs_div} "
                  f"rev={revenue} op={op} ni={ni} at={at} li={li} eq={eq}", flush=True)

        return {
            "매출액": revenue, "영업이익": op, "당기순이익": ni,
            "자산총계": at, "부채총계": li, "자기자본": eq
        }

    out = _one("CFS")
    core_missing = (not out) or all(out.get(k) is None for k in ("매출액","영업이익","당기순이익"))

    if core_missing and (not FS_DIV_ONLY_CFS or (ticker and ticker.startswith("900"))):
        alt = _one("OFS")
        if alt:
            out = alt

    result = out or {"매출액":None,"영업이익":None,"당기순이익":None,"자산총계":None,"부채총계":None,"자기자본":None}
    return result, calls_made

# =========================
# 분기 단품 계산 (음수 허용, 결측은 None 유지)
# =========================
def _v(d, key):
    if not isinstance(d, dict): return None
    v = d.get(key, None)
    return v if (isinstance(v, (int,float)) or v is None) else None

def _sub(a, b):
    if a is None or b is None: return None
    return a - b

def make_quarter_single(series):
    Q1, H1, Q3, AN = [series.get(x, {}) for x in ("Q1","H1","Q3","ANNUAL")]
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

# =========================
# 작업 빌드
# =========================
def build_tasks(corp_map, years, quarters):
    items = list(corp_map.items())
    if SAMPLE_TICKERS > 0:
        items = items[:SAMPLE_TICKERS]
        print(f"[SAMPLE] limiting to {len(items)} tickers", flush=True)
    return [(t, c, y, rc, qn) for t, c in items for y in years for rc, qn in quarters]

# =========================
# 기존 데이터 로드 (중복/스킵)
# =========================
def load_existing_data(ws):
    try:
        all_vals = ws.get_all_values()
        if not all_vals or len(all_vals) < 2:
            return set(), {}
        header = all_vals[0]
        try:
            idx_ticker = header.index("ticker")
            idx_year = header.index("year")
            idx_quarter = header.index("quarter")
        except ValueError:
            return set(), {}
        existing = set()
        row_map = {}
        for rno, row in enumerate(all_vals[1:], start=2):
            if len(row) <= max(idx_ticker, idx_year, idx_quarter): continue
            tic = (row[idx_ticker] or "").strip()
            yr  = (row[idx_year] or "").strip()
            q   = (row[idx_quarter] or "").strip()
            if tic and yr and q:
                key = (tic, yr, q)
                existing.add(key)
                row_map[key] = rno
        print(f"[EXISTING] Loaded {len(existing)} existing records", flush=True)
        return existing, row_map
    except Exception as e:
        print(f"[WARN] Failed to load existing data: {e}", flush=True)
        return set(), {}

# =========================
# 기존 시트 0/빈칸 자동 수정
# =========================
def repair_zero_rows(ws, corp_map, years, api_calls_left):
    print("[REPAIR] start scanning sheet zeros", flush=True)
    all_vals = ws.get_all_values()
    if not all_vals or len(all_vals) < 2:
        print("[REPAIR] no data", flush=True)
        return 0, api_calls_left

    header = all_vals[0]
    idx = {name:i for i,name in enumerate(header)}
    need_cols = ["ticker","corp_code","year","quarter","매출액","영업이익","당기순이익","자기자본","부채총계","자산총계"]
    if any(col not in idx for col in need_cols):
        print("[REPAIR] header mismatch; skip", flush=True)
        return 0, api_calls_left

    pos = {}
    for rno, row in enumerate(all_vals[1:], start=2):
        try:
            tic = row[idx["ticker"]].strip()
            yr  = row[idx["year"]].strip()
            q   = row[idx["quarter"]].strip()
        except:
            continue
        if not tic or not yr or q not in ("Q1","Q2","Q3","Q4"):
            continue
        pos.setdefault((tic, yr), {})[q] = rno

    targets = set()
    for (tic, yr), qrows in pos.items():
        for q in ("Q1","Q2","Q3","Q4"):
            rno = qrows.get(q)
            if not rno:
                targets.add((tic, yr)); break
            row = all_vals[rno-1]
            mv = row[idx["매출액"]]; ov = row[idx["영업이익"]]; nv = row[idx["당기순이익"]]
            def _zeroish(x):
                sx = str(x).strip()
                return sx == "" or sx == "0" or sx == "0.0"
            if _zeroish(mv) or _zeroish(ov) or _zeroish(nv):
                targets.add((tic, yr)); break

    if not targets:
        print("[REPAIR] nothing to fix", flush=True)
        return 0, api_calls_left

    print(f"[REPAIR] candidates: {len(targets)}", flush=True)
    reprts = [("11013","Q1"),("11012","H1"),("11014","Q3"),("11011","ANNUAL")]
    fixed = 0
    total_calls_used = 0

    for (tic, yr) in targets:
        if api_calls_left - total_calls_used <= 0:
            print("[REPAIR] hit call limit during repair", flush=True)
            break

        corp_code = None
        any_q = pos[(tic,yr)].get("Q1") or pos[(tic,yr)].get("Q2") or pos[(tic,yr)].get("Q3") or pos[(tic,yr)].get("Q4")
        if any_q:
            row = all_vals[any_q-1]
            corp_code = row[idx.get("corp_code", -1)].strip() if idx.get("corp_code") is not None else None
        if not corp_code:
            corp_code = corp_map.get(tic)
        if not corp_code:
            continue

        series = {}
        for rc, qn in reprts:
            if api_calls_left - total_calls_used <= 0: break
            out, calls = fetch_financials(corp_code, yr, rc, ticker=tic)
            total_calls_used += calls
            series[qn] = out

        singles = make_quarter_single(series)
        for q in ("Q1","Q2","Q3","Q4"):
            rno = pos[(tic,yr)].get(q)
            if not rno: continue
            fin = singles.get(q, {})
            def _cell(v): return "" if v is None else v
            new_row = [tic, corp_code, yr, q, f"{yr}-{q}",
                       _cell(fin.get("매출액")), _cell(fin.get("영업이익")), _cell(fin.get("당기순이익")),
                       _cell(fin.get("자기자본")), _cell(fin.get("부채총계")), _cell(fin.get("자산총계"))]
            update_row_safe(ws, rno, new_row)
            fixed += 1

    api_calls_left -= total_calls_used
    print(f"[REPAIR] updated rows: {fixed}, calls_used: {total_calls_used}", flush=True)
    return fixed, api_calls_left

# =========================
# 메인 수집
# =========================
def collect_financials():
    sheet = open_sheet()
    try:
        ws = sheet.worksheet(FIN_SHEET)
    except:
        ws = sheet.add_worksheet(title=FIN_SHEET, rows=10, cols=15)

    if ws.row_values(1)[:1] != ["ticker"]:
        ws.insert_row(
            ["ticker","corp_code","year","quarter","date",
             "매출액","영업이익","당기순이익","자기자본","부채총계","자산총계"], 1)

    existing_data, row_map = load_existing_data(ws)

    corp_map = get_corp_code_map(ws)

    current_year = datetime.now().year
    if YEARS_ENV.strip():
        years = [y.strip() for y in YEARS_ENV.split(",") if y.strip()]
    else:
        years = [str(current_year-1), str(current_year)]

    quarters = [("11013","Q1"),("11012","H1"),("11014","Q3"),("11011","ANNUAL")]
    tasks = build_tasks(corp_map, years, quarters)
    print(f"[TASKS] {len(tasks)}", flush=True)

    api_calls_left = MAX_DAILY_CALLS
    if RUN_REPAIR_ZERO:
        fixed, api_calls_left = repair_zero_rows(ws, corp_map, set(years), api_calls_left)
        print(f"[INFO] repair done: fixed={fixed}, calls_left≈{api_calls_left}", flush=True)

    done = load_checkpoint()
    acc = {}
    done_today = set()
    api_calls_lock = threading.Lock()

    def take_calls(n):
        nonlocal api_calls_left
        with api_calls_lock:
            if api_calls_left < n:
                return False
            api_calls_left -= n
            return True

    def worker(task):
        ticker, corp_code, year, reprt_code, q_name = task
        key = f"{ticker}-{year}-{q_name}"

        if key in done:
            return None
        if SKIP_IF_EXISTS and (ticker, year, q_name) in existing_data:
            # 이미 존재하면 스킵 (호출 절약)
            return None

        # 최대 2회 (CFS + OFS) 여유 예약
        if not take_calls(2):
            return "LIMIT"

        fin, calls_made = fetch_financials(corp_code, year, reprt_code, ticker=ticker)
        unused = 2 - calls_made
        if unused > 0:
            with api_calls_lock:
                api_calls_left += unused

        done_today.add(key)
        return (ticker, corp_code, year, q_name, fin)

    completed = 0
    total = len(tasks)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(worker, t) for t in tasks]
        for fut in as_completed(futures):
            res = fut.result()
            completed += 1
            if res == "LIMIT":
                print("[LIMIT] Hit MAX_DAILY_CALLS (approx). Saving checkpoint.", flush=True)
                break
            if isinstance(res, tuple):
                ticker, corp_code, year, q_name, fin = res
                acc.setdefault((ticker,year), {"corp_code":corp_code})[q_name] = fin

            if completed % 1000 == 0:
                print(f"[PROGRESS] {completed}/{total} | calls_left≈{api_calls_left}", flush=True)

            if api_calls_left <= 0:
                print("[LIMIT] Hit MAX_DAILY_CALLS (approx). Saving checkpoint.", flush=True)
                break

    save_checkpoint(done.union(done_today))

    print("[STEP] Calculating quarter singles...", flush=True)
    rows = []
    def _cell(v): return "" if v is None else v

    for (ticker,year), s in acc.items():
        corp_code = s.get("corp_code","")
        singles = make_quarter_single(s)
        for q in ("Q1","Q2","Q3","Q4"):
            if (ticker, year, q) in existing_data:
                if DEBUG_TICKER and ticker == DEBUG_TICKER:
                    print(f"[SKIP] Duplicate: {ticker}-{year}-{q}", flush=True)
                continue
            fin = singles.get(q, {})
            core = [fin.get("매출액"), fin.get("영업이익"), fin.get("당기순이익")]
            if all(v is None for v in core):
                continue
            rows.append([
                ticker, corp_code, year, q, f"{year}-{q}",
                _cell(fin.get("매출액")), _cell(fin.get("영업이익")), _cell(fin.get("당기순이익")),
                _cell(fin.get("자기자본")), _cell(fin.get("부채총계")), _cell(fin.get("자산총계"))
            ])

    if rows:
        append_rows_safe(ws, rows, "quarter-singles")

    print(f"[DONE] saved_rows={len(rows)} | calls_left≈{api_calls_left}", flush=True)

if __name__ == "__main__":
    try:
        collect_financials()
    except Exception as e:
        print(f"[FATAL] {e}", flush=True)
        import traceback; traceback.print_exc()
        sys.exit(1)
