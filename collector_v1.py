# collector_v1.py — 한국 시간대 + 최신 영업일 자동 탐색

import os, time, math, tempfile, random
import pandas as pd
from pykrx import stock
import gspread
from gspread.exceptions import APIError

SLEEP_SEC = float(os.environ.get("SLEEP_SEC", "0.4"))
SERVICE_ACCOUNT_JSON = os.environ.get("SERVICE_ACCOUNT_JSON")
SHEET_ID = os.environ.get("SHEET_ID")

def open_sheet():
    if not SERVICE_ACCOUNT_JSON or not SHEET_ID:
        raise RuntimeError("ENV not found")
    
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as fp:
        fp.write(SERVICE_ACCOUNT_JSON)
        sa_path = fp.name
    
    gc = gspread.service_account(filename=sa_path)
    return gc.open_by_key(SHEET_ID)

def target_kr_date():
    """가장 최근 한국 주식시장 영업일 찾기"""
    now = pd.Timestamp.now(tz="Asia/Seoul")
    
    # 오후 10시 이전이면 어제부터 시작
    if now.hour < 22:
        target = now - pd.Timedelta(days=1)
    else:
        target = now
    
    # 최대 10일 전까지 영업일 탐색
    for _ in range(10):
        # 주말 건너뛰기
        if target.weekday() >= 5:
            target = target - pd.Timedelta(days=1)
            continue
        
        # 실제 데이터가 있는지 확인 (삼성전자로 테스트)
        date_str = target.strftime("%Y%m%d")
        try:
            test = stock.get_market_ohlcv_by_date(date_str, date_str, "005930")
            if not test.empty:
                return date_str
        except:
            pass
        
        target = target - pd.Timedelta(days=1)
    
    # 찾지 못하면 최근 평일
    target = now - pd.Timedelta(days=1)
    while target.weekday() >= 5:
        target = target - pd.Timedelta(days=1)
    
    return target.strftime("%Y%m%d")

def get_quarterly_sheet_name():
    now = pd.Timestamp.now(tz="Asia/Seoul")
    year = now.year
    quarter = (now.month - 1) // 3 + 1
    return f"raw_daily_{year}Q{quarter}"

def get_all_tickers():
    kospi = stock.get_market_ticker_list(market="KOSPI")
    kosdaq = stock.get_market_ticker_list(market="KOSDAQ")
    return kospi + kosdaq, set(kospi), set(kosdaq)

def fetch_one(date, t, kospi_set, kosdaq_set):
    name = stock.get_market_ticker_name(t)
    ohlcv = stock.get_market_ohlcv_by_date(date, date, t)
    if ohlcv.empty:
        return None
    
    row = ohlcv.iloc[0]
    
    def g(row, key):
        try:
            return row[key] if key in row.index else None
        except:
            return None
    
    def to_int(x):
        try:
            if x is None or (isinstance(x, float) and math.isnan(x)):
                return None
            return int(x)
        except:
            return None
    
    close  = to_int(g(row, "종가"))
    open_  = to_int(g(row, "시가"))
    high   = to_int(g(row, "고가"))
    low    = to_int(g(row, "저가"))
    volume = to_int(g(row, "거래량"))
    
    EPS = BPS = PER = PBR = DIV = DPS = None
    f = stock.get_market_fundamental_by_date(date, date, t)
    if not f.empty:
        fr = f.iloc[0]
        def nz(x):
            try:
                return None if (x is None or (isinstance(x, float) and math.isnan(x))) else float(x)
            except:
                return None
        EPS = nz(fr.get("EPS"))
        BPS = nz(fr.get("BPS"))
        PER = nz(fr.get("PER"))
        PBR = nz(fr.get("PBR"))
        DIV = nz(fr.get("DIV"))
        DPS = nz(fr.get("DPS"))
    
    mktcap = shares_out = None
    cap = stock.get_market_cap_by_date(date, date, t)
    if not cap.empty:
        cr = cap.iloc[0]
        mktcap     = to_int(cr.get("시가총액"))
        shares_out = to_int(cr.get("상장주식수"))
    
    market = "KOSPI" if t in kospi_set else ("KOSDAQ" if t in kosdaq_set else "")
    
    return [date, t, name, market, close, open_, high, low, volume,
            mktcap, shares_out, EPS, BPS, PER, PBR, DIV, DPS]

def collect_and_upload():
    sheet = open_sheet()
    RAW_SHEET = get_quarterly_sheet_name()
    date = target_kr_date()
    
    print(f"[INFO] Current time (KST): {pd.Timestamp.now(tz='Asia/Seoul')}")
    print(f"[INFO] Target sheet: {RAW_SHEET}")
    print(f"[INFO] Target date: {date}")
    
    try:
        ws = sheet.worksheet(RAW_SHEET)
        print(f"[OK] Sheet found")
    except:
        ws = sheet.add_worksheet(title=RAW_SHEET, rows=10, cols=20)
        print(f"[OK] Sheet created")
    
    tickers, kospi_set, kosdaq_set = get_all_tickers()
    print(f"[INFO] Total tickers: {len(tickers)}")
    
    header = ["date","ticker","name","market","close","open","high","low","volume",
              "mktcap","shares_out","EPS","BPS","PER","PBR","DIV","DPS"]
    N_COLS = len(header)
    
    try:
        first = ws.row_values(1)
    except:
        first = []
    
    if not first or first[0] != "date":
        ws.insert_row(header, 1)
        print("[OK] Header inserted")
    
    all_vals = ws.get_all_values()
    print(f"[INFO] Sheet has {len(all_vals)} rows (including header)")
    
    existing = {(r[0], r[1]) for r in all_vals[1:]} if len(all_vals) > 1 else set()
    print(f"[INFO] Existing records: {len(existing)}")
    
    def normalize(rec):
        if rec is None:
            return None
        if len(rec) != N_COLS:
            return None
        return [("" if x is None else x) for x in rec]
    
    def append_rows_safe(rows, tag=""):
        if not rows:
            return
        max_retries = 3
        delay = 1.0
        for attempt in range(max_retries):
            try:
                ws.append_rows(rows, value_input_option="RAW", table_range="A1")
                if tag:
                    print(f"[OK] Uploaded {len(rows)} rows ({tag})")
                return
            except APIError as e:
                print(f"[RETRY] Attempt {attempt+1}/{max_retries}")
                time.sleep(delay)
                delay *= 2
        
        if len(rows) > 1:
            mid = len(rows) // 2
            append_rows_safe(rows[:mid], f"{tag}-1")
            append_rows_safe(rows[mid:], f"{tag}-2")
    
    batch, uploaded, skipped = [], 0, 0
    BATCH_TARGET = 100
    
    for i, t in enumerate(tickers, 1):
        key = (date, t)
        if key in existing:
            skipped += 1
            continue
        
        try:
            rec = normalize(fetch_one(date, t, kospi_set, kosdaq_set))
            if rec:
                batch.append(rec)
        except Exception as e:
            pass
        finally:
            time.sleep(SLEEP_SEC)
        
        if len(batch) >= BATCH_TARGET:
            append_rows_safe(batch, f"batch-{i}")
            uploaded += len(batch)
            batch.clear()
            print(f"[PROGRESS] Uploaded: {uploaded} | Skipped: {skipped}")
    
    if batch:
        append_rows_safe(batch, "final")
        uploaded += len(batch)
    
    print(f"[DONE] New records: {uploaded} | Skipped: {skipped}")

if __name__ == "__main__":
    collect_and_upload()
