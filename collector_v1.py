# collector_v1.py  — 최소 완성본 (PyKRX → Google Sheets 업서트)
# 실행:  python collector_v1.py
# 필요: pip install pykrx pandas gspread google-auth google-auth-oauthlib google-auth-httplib2

import os, time, math
import pandas as pd
from pykrx import stock
import gspread
from google.oauth2.service_account import Credentials

# ====== 설정 ======
SHEET_NAME = "KR_MARKET"          # 구글 스프레드시트 이름
RAW_SHEET  = "raw_daily"          # 원천 데이터 탭
SA_PATH    = "./stock-scanner-key.json"   # 서비스계정 JSON 키 경로
SCOPES     = ["https://www.googleapis.com/auth/spreadsheets""https://www.googleapis.com/auth/drive"]
SLEEP_SEC  = 0.4                  # 호출 속도(과다호출 방지)

# ====== Google Sheets 핸들러 ======
def open_sheet():
    import gspread
    # gspread의 helper: service_account() 사용
    gc = gspread.service_account(filename=SA_PATH)  # SA_PATH는 '/.../stock-scanner-key.json' 절대경로 가능
    return gc.open(SHEET_NAME)  # 제목으로 열기 (또는 .open_by_key(SHEET_ID))
# ====== 수집 날짜(장 마감 후 22시 기준) ======
def target_kr_date():
    now = pd.Timestamp.now(tz="Asia/Tokyo")
    # 22시 이후면 '오늘', 그 전엔 '어제' (휴장일 보정은 v1.1에서)
    if now.hour >= 22:
        return now.strftime("%Y%m%d")
    return (now - pd.Timedelta(days=1)).strftime("%Y%m%d")

# ====== 전 종목 목록 ======
def get_all_tickers():
    kospi = stock.get_market_ticker_list(market="KOSPI")
    kosdaq = stock.get_market_ticker_list(market="KOSDAQ")
    return kospi + kosdaq, set(kospi), set(kosdaq)

# ====== 단일 종목 수집 (OHLCV + Fundamental + Cap) ======
# ====== 단일 종목 수집 (OHLCV + Fundamental + Cap) ======
def fetch_one(date, t, kospi_set, kosdaq_set):
    name = stock.get_market_ticker_name(t)

    # OHLCV (일자 1건)
    ohlcv = stock.get_market_ohlcv_by_date(date, date, t)
    if ohlcv.empty:
        return None
    row = ohlcv.iloc[0]

    # 안전 접근 헬퍼
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
    value  = to_int(g(row, "거래대금"))   # 일부 종목에서 컬럼이 없을 수 있음 → None 허용

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
        mktcap    = to_int(cr.get("시가총액"))
        shares_out = to_int(cr.get("상장주식수"))  # 없으면 None

    # 시장 구분
    market = "KOSPI" if t in kospi_set else ("KOSDAQ" if t in kosdaq_set else "")

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

    # 헤더 없으면 생성
    all_vals = ws.get_all_values()
    if len(all_vals) == 0:
        ws.append_row(header)
        all_vals = [header]

    # 기존 키 세트(중복 방지): (date,ticker)
    if len(all_vals) > 1:
        existing = {(r[0], r[1]) for r in all_vals[1:]}
    else:
        existing = set()

    batch = []
    for i, t in enumerate(tickers, 1):
        key = (date, t)
        if key in existing:
            continue
        try:
            rec = fetch_one(date, t, kospi_set, kosdaq_set)
            if rec:
                batch.append(rec)
        except Exception as e:
            print("ERR", t, e)
        finally:
            time.sleep(SLEEP_SEC)

        # 배치 업로드(200건 단위)
        if len(batch) >= 200:
            ws.append_rows(batch, value_input_option="RAW")
            batch.clear()
            print(f"Uploaded up to #{i}")

    if batch:
        ws.append_rows(batch, value_input_option="RAW")
        print(f"Uploaded final batch ({len(batch)} rows)")

if __name__ == "__main__":
    collect_and_upload()
    print("DONE.")
