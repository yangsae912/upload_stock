"""
incoming_inventory_sku 적재 스크립트
- input: 구매팀 입고일정 CSV 파일
- 입고 = X 항목만 적재 (미입고 = 입고 예정)
- handle 단위 수량을 고정 도수 비율로 SKU별 배분
- 실행 주기: 구매팀 입고일정 파일 업데이트 시 수동 실행
"""

import psycopg2
import csv
from datetime import datetime

DB_CONFIG = {
    "host":     "",
    "database": "",
    "user":     "",
    "password": "",
    "port":     
}

FILE_PATH = r'C:\Users\User\Desktop\오프라인 운영\vscode\upload_pgadmin\stock\inventory_weekly\구매팀 _ 입고일정_렌즈파트_표.csv'

# 고정 도수 비율
POWER_RATIO = {
    '000': 0.239,
    '050': 0.029,
    '100': 0.036,
    '125': 0.032,
    '150': 0.035,
    '175': 0.035,
    '200': 0.035,
    '225': 0.031,
    '250': 0.038,
    '275': 0.035,
    '300': 0.037,
    '325': 0.032,
    '350': 0.037,
    '375': 0.038,
    '400': 0.035,
    '425': 0.028,
    '450': 0.034,
    '475': 0.029,
    '500': 0.036,
    '550': 0.039,
    '600': 0.031,
    '650': 0.025,
    '700': 0.019,
    '750': 0.016,
    '800': 0.018,
}

def parse_date(date_str: str):
    """'2026. 3. 19.' 형식 → date"""
    date_str = date_str.strip().rstrip('.')
    return datetime.strptime(date_str, '%Y. %m. %d').date()

def to_month_first(d):
    """날짜 → 해당 월 1일"""
    return d.replace(day=1)

def load_incoming():
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()

    # ── CSV 파싱 (입고 = X만) ────────────────────────────────────
    handle_records = []
    with open(FILE_PATH, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['입고'].strip() != 'X':
                continue
            handle = row['대표코드'].strip()
            qty    = int(str(row['수량(Pack)']).replace(',', '').strip() or 0)
            try:
                incoming_month = to_month_first(parse_date(row['입고예정일']))
            except:
                print(f"날짜 파싱 실패: {row['입고예정일']}")
                continue
            if not handle or qty <= 0:
                continue
            handle_records.append({
                'handle':         handle,
                'incoming_month': incoming_month,
                'handle_qty':     qty,
            })

    print(f"입고 예정 건수: {len(handle_records)}개")

    # ── SKU별 입고 예정 수량 계산 ────────────────────────────────
    sku_records = []
    for r in handle_records:
        for power_code, ratio in POWER_RATIO.items():
            hcode   = f"{r['handle']}S{power_code}C"
            sku_qty = round(r['handle_qty'] * ratio)
            if sku_qty <= 0:
                continue
            sku_records.append((
                hcode,
                r['handle'],
                r['incoming_month'],
                r['handle_qty'],
                ratio,
                sku_qty,
            ))

    print(f"SKU별 적재 건수: {len(sku_records)}개")

    # ── DB 적재 ─────────────────────────────────────────────────
    cur.execute("TRUNCATE TABLE public.incoming_inventory_sku")

    cur.executemany("""
        INSERT INTO public.incoming_inventory_sku (
            hcode, handle, incoming_month,
            handle_qty, power_ratio, sku_qty
        ) VALUES (%s, %s, %s, %s, %s, %s)
    """, sku_records)

    conn.commit()
    cur.close()
    conn.close()
    print(f"적재 완료: {len(sku_records)}개 레코드")

if __name__ == "__main__":
    load_incoming()
