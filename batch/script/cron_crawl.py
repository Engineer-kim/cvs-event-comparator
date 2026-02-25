"""
다음달 1일 00:30에 실행될 배치 스크립트입니다. (KST 기준)
- 편의점 행사 상품 크롤링 (7-Eleven, CU, GS25, emart24)
- 데이터 클리닝 및 통합
- 카테고리 분류
- 실패시 대응을 위해, 로그 기록
ex) 26년 3월이면 crawl_batch_log 아래 YY_MM 형식으로 디렉토리 만들고 로그 저장
 Write By: 김한진(2026.02.25)
 Update By: 김한진(2026.02.25) - 최초 작성 및 수정
"""
import os
import sys
import argparse
from datetime import datetime
import pytz
import importlib

#최상위 폴더에서 탐색하도록 설정 혹시나 파일위치 혹은 파일 못찫아서 익셉션 터지면 안되니까
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

def get_kst_now():
    """KST(Korea Standard Time) 현재 시간 반환"""
    kst = pytz.timezone('Asia/Seoul')
    return datetime.now(kst).replace(tzinfo=None)

def prepare_env():
    os.makedirs(os.path.join(PROJECT_ROOT, 'data'), exist_ok=True)
    os.makedirs(os.path.join(PROJECT_ROOT, 'crawl_batch_log'), exist_ok=True)

def get_log_dir_for_time(t: datetime):
    yy = t.year % 100
    m = t.month
    dirname = f"{yy}_{m}"
    dirpath = os.path.join(PROJECT_ROOT, 'crawl_batch_log', dirname)
    os.makedirs(dirpath, exist_ok=True)
    return dirpath


def make_datetime(fixed_dt: datetime):
    class DateTime:
        @staticmethod
        def now():
            return fixed_dt
    return DateTime


def write_log(msg: str, run_time: datetime):
    log_dir = get_log_dir_for_time(run_time)
    fname = run_time.strftime('batch_%Y%m%d_%H%M%S.log')
    path = os.path.join(log_dir, fname)
    timestamp = run_time.strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{timestamp} KST] {msg}\n"
    with open(path, 'a', encoding='utf-8') as f:
        f.write(line)
    print(line, end='')


def get_next_month_data_batch(year: int, month: int, dry_run: bool = False, run_time: datetime = None):
    prepare_env()

    write_log('=== BATCH START ===', run_time)
    write_log(f'Running batch for target month: {year}-{month}', run_time)

    # import scraper modules
    mods = [
        'scraper.seven_eleven_scraper',
        'scraper.cu_scraper',
        'scraper.gs25_scraper',
        'scraper.emart24_scraper'
    ]
    dummy = make_datetime(run_time)
    imported = []
    for m in mods:
        try:
            mod = importlib.import_module(m)
            setattr(mod, 'datetime', dummy)
            imported.append(mod)
            write_log(f'Module patched: {m}', run_time)
        except Exception as e:
            write_log(f'Failed to import/patch {m}: {e}', run_time)
    if dry_run:
        write_log('Dry run enabled: Skipping actual crawler execution.(배포 직전에는 플래그 False로 바꿔야함)', run_time)
    else:
        try:
            # 7-Eleven
            write_log('Starting crawl: 7-Eleven', run_time)
            from scraper.seven_eleven_scraper import crawl_7eleven
            crawl_7eleven()
            write_log('Finished crawl: 7-Eleven', run_time)
        except Exception as e:
            write_log(f'7-Eleven crawl failed: {e}', run_time)

        try:
            write_log('Starting crawl: CU', run_time)
            from scraper.cu_scraper import CUCrawler
            CUCrawler().run()
            write_log('Finished crawl: CU', run_time)
        except Exception as e:
            write_log(f'CU crawl failed: {e}', run_time)

        try:
            write_log('Starting crawl: GS25', run_time)
            from scraper.gs25_scraper import scrape_gs25_event_goods
            scrape_gs25_event_goods()
            write_log('Finished crawl: GS25', run_time)
        except Exception as e:
            write_log(f'GS25 crawl failed: {e}', run_time)

        try:
            write_log('Starting crawl: emart24', run_time)
            from scraper.emart24_scraper import Emart24Scraper
            Emart24Scraper().run()
            write_log('Finished crawl: emart24', run_time)
        except Exception as e:
            write_log(f'emart24 crawl failed: {e}', run_time)

    try:
        write_log('Starting data_cleaner.clean_and_merge', run_time)
        from utils.data_cleaner import clean_and_merge
        clean_and_merge()
        write_log('Finished data_cleaner.clean_and_merge', run_time)
    except Exception as e:
        write_log(f'data_cleaner failed: {e}', run_time)

    try:
        write_log('Starting data_categorize.run_categorization', run_time)
        from utils.data_categorize import run_categorization
        run_categorization()
        write_log('Finished data_categorize.run_categorization', run_time)
    except Exception as e:
        write_log(f'data_categorize failed: {e}', run_time)

    write_log('=== BATCH COMPLETE ===', run_time)
    return True


def cli():
    p = argparse.ArgumentParser()
    p.add_argument('--year', type=int, help='Target year e.g. 2026', required=False)
    p.add_argument('--month', type=int, help='Target month 1-12', required=False)
    p.add_argument('--dry-run', action='store_true', help='Do not execute network crawls')
    args = p.parse_args()

    if args.year and args.month:
        y = args.year
        m = args.month
    else:
        # KST 기준 현재 시간 사용
        now = get_kst_now()
        y = now.year
        m = now.month

    get_next_month_data_batch(y, m, dry_run=args.dry_run)


if __name__ == '__main__':
    cli()