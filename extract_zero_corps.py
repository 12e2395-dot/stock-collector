# extract_zero_corps_v2.py - 재무제표의 0 또는 공란 기업 전체 자동 분석

import os
import sys
import tempfile
import gspread
import requests
import time
from collections import defaultdict

# 환경변수
SERVICE_ACCOUNT_JSON = os.environ.get("SERVICE_ACCOUNT_JSON")
SHEET_ID = os.environ.get("SHEET_ID")
DART_API_KEY = os.environ.get("DART_API_KEY") or "2577b83cd4832faf40689fb65ad1c51e34a1bfb3"
FIN_SHEET = "fin_statement"

def open_sheet():
    """구글 시트 열기"""
    if not SERVICE_ACCOUNT_JSON or not SHEET_ID:
        raise RuntimeError("환경변수 없음: SERVICE_ACCOUNT_JSON, SHEET_ID")
    
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as fp:
        fp.write(SERVICE_ACCOUNT_JSON)
        sa_path = fp.name
    
    gc = gspread.service_account(filename=sa_path)
    return gc.open_by_key(SHEET_ID)

def safe_int(v):
    """공란이나 문자를 0으로 변환"""
    try:
        return int(v.replace(",", "").strip()) if v.strip() else 0
    except Exception:
        return 0

def extract_zero_corps():
    """스프레드시트에서 재무 항목 중 하나라도 0 또는 공란인 기업 전체 추출"""
    print("📊 스프레드시트 열기...", flush=True)
    sheet = open_sheet()
    ws = sheet.worksheet(FIN_SHEET)
    
    print("📥 데이터 로딩...", flush=True)
    all_vals = ws.get_all_values()
    
    if len(all_vals) < 2:
        print("❌ 데이터가 없습니다")
        return []
    
    header = all_vals[0]
    required_cols = ["corp_code", "year", "quarter", "매출액", "영업이익", "당기순이익", "자기자본", "부채총계", "자산총계"]
    
    missing_cols = [c for c in required_cols if c not in header]
    if missing_cols:
        raise ValueError(f"❌ 누락된 컬럼: {missing_cols}")
    
    idx = {col: header.index(col) for col in required_cols}
    
    zero_corps = []
    seen = set()
    
    print("🔍 0 또는 공란 데이터 찾는 중...", flush=True)
    
    for row in all_vals[1:]:
        if len(row) <= max(idx.values()):
            continue
        
        corp_code = row[idx["corp_code"]].strip()
        year = row[idx["year"]].strip()
        quarter = row[idx["quarter"]].strip()
        
        # 재무 항목 추출
        revenue = safe_int(row[idx["매출액"]])
        operating = safe_int(row[idx["영업이익"]])
        net_income = safe_int(row[idx["당기순이익"]])
        equity = safe_int(row[idx["자기자본"]])
        liability = safe_int(row[idx["부채총계"]])
        asset = safe_int(row[idx["자산총계"]])
        
        # 하나라도 0이면 대상
        if any(v == 0 for v in [revenue, operating, net_income, equity, liability, asset]):
            key = (corp_code, year, quarter)
            if key not in seen:
                zero_corps.append({
                    "corp_code": corp_code,
                    "year": year,
                    "quarter": quarter,
                    "매출액": revenue,
                    "영업이익": operating,
                    "당기순이익": net_income,
                    "자기자본": equity,
                    "부채총계": liability,
                    "자산총계": asset
                })
                seen.add(key)
    
    print(f"✅ {len(zero_corps)}개 기업의 0 또는 공란 데이터 발견")
    return zero_corps

def main():
    print(f"\n{'='*80}")
    print("🔍 재무제표에서 0 또는 공란 기업 전체 분석")
    print(f"{'='*80}\n")
    
    zero_corps = extract_zero_corps()
    
    if not zero_corps:
        print("❌ 0 또는 공란 데이터가 없습니다")
        return
    
    print(f"\n📊 분석 대상: {len(zero_corps)}개 기업")
    for item in zero_corps[:10]:
        print(f"  - {item['corp_code']} ({item['year']} {item['quarter']})")
    if len(zero_corps) > 10:
        print(f"  ... 외 {len(zero_corps)-10}개 기업")

    print(f"\n{'='*80}")
    print("✅ 데이터 추출 완료")
    print(f"{'='*80}\n")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
