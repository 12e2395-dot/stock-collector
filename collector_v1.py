# collector_v1.py — PyKRX → Google Sheets 업서트 자동화 (최종 안정판)
# 실행:  python collector_v1.py
# 필요: pip install pykrx pandas gspread google-auth google-auth-oauthlib google-auth-httplib2

import os, time, math, tempfile, random
import pandas as pd
from pykrx import stock
import gspread
from gspread.exceptions import APIError

# ====== 설정 ======
RAW_SHEET  = "raw_daily"                         # 원천 데이터 탭
SLEEP_SEC  = float(os.environ.get("SLEEP_SEC", "0.4"))   # 호출 속도(과다호출 방지)

# GitHub Actions / 로컬 환경변수에서 읽기
SERVICE_ACCOUNT_JSON = os.environ.get("SERVICE_ACCOUNT_JSON")
SHEET_ID             = os.environ.get("SHEET_ID")

# ====== Google Sheets 핸들러 ======
def open_sheet():
    if not SERVICE_ACCOUNT_JSON:
        raise RuntimeError("SERVICE_ACCOUNT_JSON env not found")
    if not SHEET_ID:
        raise RuntimeError("SHEET_ID env not found")

    # Secrets의 JSON 문자열을 임시 파일로 저장해 인증
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as fp:
        fp.write(SERVICE_ACCOUNT_JSON)
        sa_path = fp.name

    gc = gspread.service_account(filename=sa_path)
    return gc.open_by_key(SHEET_ID)

# ====== 수집 날짜(장 마감 후 22시 기준) ======
def target_kr_date():
    now = pd.Timestamp.now(tz="Asia/Tokyo")
    if now.hour >= 22:
        return now.strftime("%Y%m%d")
    return (now - pd.Timedelta(days=1)).strftime("%Y%m%d")

# ====== 전 종목 목록 ======
def get_all_tickers():
    kospi = stock.get_market_ticker_list(market="KOSPI")
    kosdaq = stock.get_market_ticker_list(market="KOSDAQ")
    return kospi + kosdaq, set(kospi), set(kosdaq)

# ====== 단일 종목 수집 (OHLCV + Fundamental + Cap) ======
def fetch_one(date, t, kospi_set, kosdaq_set):
    name = stock.get_market_ticker_name(t)

    # OHLCV (일자 1건)
    ohlcv = stock.get_market_ohlcv_by_date(date, date, t)
    if ohlcv.empty:
        return None
    row = ohlcv.iloc[0]

    def g(row, key):
        try:
            return row[key] if key in row.index else None
        except Exception:
            return None

    def to_int(x):
        try:
            if x is None or (isinstance(x, float) and math.isnan(x)):
                return None
            return int(x)
        except Exception:
            return None

    close  = to_int(g(row, "종가"))
    open_  = to_int(g(row, "시가"))
    high   = to_int(g(row, "고가"))
    low    = to_int(g(row, "저가"))
    volume = to_int(g(row, "거래량"))
    value  = to_int(g(row, "거래대금"))  # 없으면 None 가능

    # Fundamental (EPS/BPS/PER/PBR/DIV/DPS)
    EPS = BPS = PER = PBR = DIV = DPS = None
    f = stock.get_market_fundamental_by_date(date, date, t)
    if not f.empty:
        fr = f.iloc[0]
        def nz(x):
            try:
                return None if (x is None or (isinstance(x, float) and math.isnan(x))) else float(x)
            except Exception:
                return None
        EPS = nz(fr.get("EPS"))
        BPS = nz(fr.get("BPS"))
        PER = nz(fr.get("PER"))
        PBR = nz(fr.get("PBR"))
        DIV = nz(fr.get("DIV"))
        DPS = nz(fr.get("DPS"))

    # 시총/상장주식수
    mktcap = shares_out = None
    cap = stock.get_market_cap_by_date(date, date, t)
    if not cap.empty:
        cr = cap.iloc[0]
        mktcap     = to_int(cr.get("시가총액"))
        shares_out = to_int(cr.get("상장주식수"))

    # 시장 구분
    market = "KOSPI" if t in kospi_set else ("KOSDAQ" if t in kosdaq_set else "")

    # 정확히 18개 컬럼 반환
    return [date, t, name, market, close, open_, high, low, volume, value,
            mktcap, shares_out, EPS, BPS, PER, PBR, DIV, DPS]

# ====== 메인 수집 → 시트 업서트 ======
def collect_and_upload():
    sheet = open_sheet()
    try:
        ws = sheet.worksheet(RAW_SHEET)
    except gspread.exceptions.WorksheetNotFound:
        ws = sheet.add_worksheet(title=RAW_SHEET, rows=10, cols=20)

    date = target_kr_date()
    tickers, kospi_set, kosdaq_set = get_all_tickers()

    header = ["date","ticker","name","market","close","open","high","low","volume","value",
              "mktcap","shares_out","EPS","BPS","PER","PBR","DIV","DPS"]
    N_COLS = len(header)

    # --- 1) 헤더 강제 보정 (A1에 header 없으면 삽입) ---
    try:
        first = ws.row_values(1)
    except Exception:
        first = []
    if not first or (len(first) == 0) or (first[0] != "date"):
        ws.insert_row(header, 1)  # 기존 데이터는 2행으로 밀림(안전)
    # 최신 전체값 재조회
    all_vals = ws.get_all_values()

    # --- 2) 기존 키 세트(중복 방지) ---
    existing = {(r[0], r[1]) for r in all_vals[1:]} if len(all_vals) > 1 else set()

    # --- 3) 업로드 안전화 유틸 ---
    def normalize(rec):
        """길이/타입 안전화: None→'' 치환 + 정확히 18열만 허용"""
        if rec is None:
            return None
        if len(rec) != N_COLS:
            print(f"[SKIP] bad length {len(rec)} for {rec[:3]}...")
            return None
        return [("" if x is None else x) for x in rec]

    def append_rows_safe(rows, tag=""):
        """
        rows: List[List[Any]]
        table_range="A1"로 A열 앵커 고정. 500/503 등 일시 에러는 지수 백오프로 재시도.
        반복 실패 시 반으로 쪼개 재귀 업로드.
        """
        if not rows:
            return
        max_retries = 5
        delay = 1.0
        for attempt in range(max_retries):
            try:
                ws.append_rows(rows, value_input_option="RAW", table_range="A1")
                if tag:
                    print(f"[OK] appended {len(rows)} rows ({tag})")
                return
            except APIError as e:
                print(f"[WARN] append_rows failed (attempt {attempt+1}/{max_retries}, size={len(rows)}): {e}")
                time.sleep(delay + random.uniform(0, 0.5))
                delay = min(delay * 2, 8.0)
        if len(rows) == 1:
            print(f"[ERROR] permanently failed for a single row: {rows[0][:3]}")
            return
        mid = len(rows) // 2
        append_rows_safe(rows[:mid], tag=f"{tag}-split1")
        append_rows_safe(rows[mid:], tag=f"{tag}-split2")

    # --- 4) 루프 & 배치 업로드 ---
    batch, uploaded = [], 0
    BATCH_TARGET = 100  # 안정성 위해 100행 단위로 업로드

    for i, t in enumerate(tickers, 1):
        key = (date, t)
        if key in existing:
            continue
        try:
            rec = normalize(fetch_one(date, t, kospi_set, kosdaq_set))
            if rec:
                batch.append(rec)
        except Exception as e:
            print("ERR", t, e)
        finally:
            time.sleep(SLEEP_SEC)

        if len(batch) >= BATCH_TARGET:
            append_rows_safe(batch, tag=f"upto#{i}")
            uploaded += len(batch)
            batch.clear()
            print(f"Uploaded up to #{i} (total uploaded so far: {uploaded})")

    if batch:
        append_rows_safe(batch, tag="final")
        uploaded += len(batch)
        print(f"Uploaded final batch ({len(batch)} rows). Total uploaded: {uploaded}")

if __name__ == "__main__":
    collect_and_upload()
    print("DONE.")
