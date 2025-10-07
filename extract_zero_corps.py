# extract_zero_corps.py - 스프레드시트에서 0인 기업 자동 추출 및 분석

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
DART_API_KEY = "2577b83cd4832faf40689fb65ad1c51e34a1bfb3"
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

def extract_zero_corps(max_samples=30):
    """스프레드시트에서 0인 corp_code 추출"""
    print("📊 스프레드시트 열기...", flush=True)
    sheet = open_sheet()
    ws = sheet.worksheet(FIN_SHEET)
    
    print("📥 데이터 로딩...", flush=True)
    all_vals = ws.get_all_values()
    
    if len(all_vals) < 2:
        print("❌ 데이터가 없습니다")
        return []
    
    header = all_vals[0]
    
    # 컬럼 인덱스 찾기
    try:
        corp_idx = header.index("corp_code")
        year_idx = header.index("year")
        quarter_idx = header.index("quarter")
        revenue_idx = header.index("매출액")
        operating_idx = header.index("영업이익")
        net_income_idx = header.index("당기순이익")
    except ValueError as e:
        print(f"❌ 컬럼을 찾을 수 없습니다: {e}")
        return []
    
    # 0인 데이터 찾기
    zero_corps = []
    seen = set()
    
    print("🔍 0인 데이터 찾는 중...", flush=True)
    
    for row in all_vals[1:]:
        if len(row) <= max(corp_idx, revenue_idx, operating_idx, net_income_idx):
            continue
        
        corp_code = row[corp_idx].strip()
        year = row[year_idx].strip()
        quarter = row[quarter_idx].strip()
        
        try:
            revenue = int(row[revenue_idx]) if row[revenue_idx] else 0
            operating = int(row[operating_idx]) if row[operating_idx] else 0
            net_income = int(row[net_income_idx]) if row[net_income_idx] else 0
        except:
            continue
        
        # 하나라도 0이면
        if revenue == 0 or operating == 0 or net_income == 0:
            key = (corp_code, year, quarter)
            if key not in seen:
                zero_corps.append({
                    "corp_code": corp_code,
                    "year": year,
                    "quarter": quarter,
                    "revenue": revenue,
                    "operating": operating,
                    "net_income": net_income
                })
                seen.add(key)
                
                if len(zero_corps) >= max_samples:
                    break
    
    print(f"✅ {len(zero_corps)}개의 0인 데이터 발견")
    return zero_corps

def analyze_company(corp_code, year, quarter_code):
    """단일 기업 분석"""
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
    
    result = {
        "corp_code": corp_code,
        "revenue_accounts": [],
        "operating_accounts": [],
        "net_income_accounts": [],
        "error": None
    }
    
    for fs_div in ["CFS", "OFS"]:
        params = {
            "crtfc_key": DART_API_KEY,
            "corp_code": corp_code,
            "bsns_year": year,
            "reprt_code": quarter_code,
            "fs_div": fs_div
        }
        
        try:
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                continue
            
            data = resp.json()
            if data.get("status") != "000":
                result["error"] = data.get("message", "")
                continue
            
            items = data.get("list", [])
            cis_items = [i for i in items if i.get("sj_div") == "CIS"]
            
            if not cis_items:
                continue
            
            for item in cis_items:
                account = item.get("account_nm", "")
                value = item.get("thstrm_amount", "")
                
                # 매출 관련 (좁은 범위)
                if account in ["매출", "매출액", "영업수익", "수익"]:
                    result["revenue_accounts"].append(account)
                
                # 영업이익 관련 (정확한 매칭)
                if "영업" in account and any(kw in account for kw in ["이익", "손익", "손실"]):
                    if "주당" not in account and "기본" not in account:
                        result["operating_accounts"].append(account)
                
                # 순이익 관련 (정확한 매칭)
                if ("순이익" in account or "순손실" in account):
                    if "비지배" not in account and "주당" not in account and "기본" not in account:
                        if "법인세" not in account:  # 세전이익 제외
                            result["net_income_accounts"].append(account)
            
            if cis_items:
                result["found_fs_div"] = fs_div
                result["error"] = None
                break
        
        except Exception as e:
            result["error"] = str(e)
    
    return result

def quarter_name_to_code(quarter):
    """분기명을 분기코드로 변환"""
    mapping = {
        "Q1": "11013",
        "H1": "11012",
        "Q3": "11014",
        "ANNUAL": "11011"
    }
    return mapping.get(quarter, "11013")

def main():
    print(f"\n{'='*80}")
    print("🔍 스프레드시트에서 0인 기업 자동 분석")
    print(f"{'='*80}\n")
    
    # 1. 스프레드시트에서 0인 corp_code 추출
    zero_corps = extract_zero_corps(max_samples=30)
    
    if not zero_corps:
        print("❌ 0인 데이터가 없습니다")
        return
    
    print(f"\n📊 분석 대상: {len(zero_corps)}개")
    for item in zero_corps[:5]:
        print(f"  - {item['corp_code']} ({item['year']} {item['quarter']}): "
              f"매출={item['revenue']}, 영업이익={item['operating']}, 순이익={item['net_income']}")
    if len(zero_corps) > 5:
        print(f"  ... 외 {len(zero_corps)-5}개")
    
    # 2. DART API로 실제 계정과목명 조회
    print(f"\n{'='*80}")
    print("🔍 DART API 조회 중...")
    print(f"{'='*80}\n")
    
    all_revenue = defaultdict(int)
    all_operating = defaultdict(int)
    all_net_income = defaultdict(int)
    
    success = 0
    fail = 0
    
    for idx, item in enumerate(zero_corps, 1):
        corp_code = item['corp_code']
        year = item['year']
        quarter_code = quarter_name_to_code(item['quarter'])
        
        print(f"[{idx}/{len(zero_corps)}] {corp_code} ({year} {item['quarter']})...", 
              end=" ", flush=True)
        
        result = analyze_company(corp_code, year, quarter_code)
        
        if result.get("found_fs_div"):
            success += 1
            print(f"✓")
            
            for acc in result["revenue_accounts"]:
                all_revenue[acc] += 1
            for acc in result["operating_accounts"]:
                all_operating[acc] += 1
            for acc in result["net_income_accounts"]:
                all_net_income[acc] += 1
        else:
            fail += 1
            print(f"✗ ({result.get('error', 'No data')})")
        
        time.sleep(0.25)
    
    # 3. 결과 출력
    print(f"\n{'='*80}")
    print(f"📊 분석 결과: 성공 {success}개 / 실패 {fail}개")
    print(f"{'='*80}\n")
    
    if success == 0:
        print("❌ 분석 가능한 데이터가 없습니다")
        return
    
    print("🔍 발견된 매출 관련 계정과목명:")
    if all_revenue:
        for account, count in sorted(all_revenue.items(), key=lambda x: -x[1]):
            print(f"  [{count:3d}개] {account}")
    else:
        print("  (없음)")
    
    print("\n🔍 발견된 영업이익 관련 계정과목명:")
    if all_operating:
        for account, count in sorted(all_operating.items(), key=lambda x: -x[1]):
            print(f"  [{count:3d}개] {account}")
    else:
        print("  (없음)")
    
    print("\n🔍 발견된 순이익 관련 계정과목명:")
    if all_net_income:
        for account, count in sorted(all_net_income.items(), key=lambda x: -x[1]):
            print(f"  [{count:3d}개] {account}")
    else:
        print("  (없음)")
    
    # 4. 현재 코드와 비교
    print(f"\n{'='*80}")
    print("⚠️  현재 코드에서 누락된 계정과목명")
    print(f"{'='*80}\n")
    
    current_revenue = ["매출", "매출액", "영업수익"]
    current_operating = ["영업이익(손실)", "영업이익", "영업손익", "영업손실"]
    current_net_income = [
        "당기순이익(손실)", "당기순이익", "당기순손실",
        "분기순이익(손실)", "분기순이익", "분기순손실",
        "반기순이익(손실)", "반기순이익", "반기순손실"
    ]
    
    missing_revenue = [acc for acc in all_revenue if acc not in current_revenue]
    missing_operating = [acc for acc in all_operating if acc not in current_operating]
    missing_net = [acc for acc in all_net_income 
                   if "지배기업" not in acc and acc not in current_net_income]
    
    if missing_revenue:
        print("🚨 추가 필요한 매출 계정과목명:")
        for acc in missing_revenue:
            print(f'  - "{acc}" (사용: {all_revenue[acc]}개 기업)')
    else:
        print("✅ 매출: 모든 계정과목명 인식 가능")
    
    if missing_operating:
        print("\n🚨 추가 필요한 영업이익 계정과목명:")
        for acc in missing_operating:
            print(f'  - "{acc}" (사용: {all_operating[acc]}개 기업)')
    else:
        print("\n✅ 영업이익: 모든 계정과목명 인식 가능")
    
    if missing_net:
        print("\n🚨 추가 필요한 순이익 계정과목명:")
        for acc in missing_net:
            print(f'  - "{acc}" (사용: {all_net_income[acc]}개 기업)')
    else:
        print("\n✅ 순이익: 모든 계정과목명 인식 가능")
    
    print(f"\n{'='*80}")
    print("✅ 분석 완료!")
    print(f"{'='*80}\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  사용자가 중단했습니다.")
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()