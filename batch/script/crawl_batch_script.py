"""
배치 스케줄러에 의해 호출되는 스크립트입니다.
지정된 연/월 정보를 바탕으로 편의점 4사 크롤링 및 데이터 통합을 수행합니다.
"""
import os
import sys
import importlib
from datetime import datetime

# 최상위 폴더에서 탐색하도록 설정
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)


def prepare_env():
    """필요한 디렉토리 자동 생성"""
    os.makedirs(os.path.join(PROJECT_ROOT, 'data'), exist_ok=True)
    os.makedirs(os.path.join(PROJECT_ROOT, 'crawl_batch_log'), exist_ok=True)


def get_log_dir_for_time(t: datetime):
    """YY_MM 형식의 로그 디렉토리 경로 반환"""
    yy = t.year % 100
    m = t.month
    dirname = f"{yy}_{m}"
    dirpath = os.path.join(PROJECT_ROOT, 'crawl_batch_log', dirname)
    os.makedirs(dirpath, exist_ok=True)
    return dirpath


def write_log(msg: str, run_time: datetime):
    """지정된 실행 시점에 맞춰 파일 로그 기록"""
    log_dir = get_log_dir_for_time(run_time)
    fname = run_time.strftime('batch_%Y%m%d_%H%M%S.log')
    path = os.path.join(log_dir, fname)
    timestamp = run_time.strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{timestamp} KST] {msg}\n"
    with open(path, 'a', encoding='utf-8') as f:
        f.write(line)
    print(line, end='')


def make_datetime(fixed_dt: datetime):
    """스크래퍼 모듈 내의 datetime.now()를 배치 실행 시점으로 패칭하기 위한 클래스"""

    class DateTime:
        @staticmethod
        def now():
            return fixed_dt

    return DateTime


def get_next_month_data_batch(year: int, month: int, dry_run: bool = False, run_time: datetime = None):
    """
    스케줄러로부터 인자를 받아 실행되는 메인 배치 함수
    """
    prepare_env()
    if run_time is None:
        run_time = datetime.now()

    write_log('=== BATCH START ===', run_time)
    write_log(f'Target: {year}-{month} | Execute At: {run_time}', run_time)

    # 스크래퍼 모듈
    mods = [
        'scraper.seven_eleven_scraper',
        'scraper.cu_scraper',
        'scraper.gs25_scraper',
        'scraper.emart24_scraper'
    ]
    time_patch = make_datetime(run_time)

    for m in mods:
        try:
            mod = importlib.import_module(m)
            setattr(mod, 'datetime', time_patch)
            write_log(f'Module patched: {m}', run_time)
        except Exception as e:
            write_log(f'Failed to patch {m}: {e}', run_time)

    # 크롤링 실행
    if dry_run:
        write_log('Dry run enabled: Skipping actual crawler execution.', run_time)
    else:
        # 7-Eleven
        try:
            from scraper.seven_eleven_scraper import crawl_7eleven
            crawl_7eleven()
            write_log('Finished: 7-Eleven', run_time)
        except Exception as e:
            write_log(f'7-Eleven failed: {e}', run_time)

        # CU
        try:
            from scraper.cu_scraper import CUCrawler
            CUCrawler().run()
            write_log('Finished: CU', run_time)
        except Exception as e:
            write_log(f'CU failed: {e}', run_time)

        # GS25
        try:
            from scraper.gs25_scraper import scrape_gs25_event_goods
            scrape_gs25_event_goods()
            write_log('Finished: GS25', run_time)
        except Exception as e:
            write_log(f'GS25 failed: {e}', run_time)

        # emart24
        try:
            from scraper.emart24_scraper import Emart24Scraper
            Emart24Scraper().run()
            write_log('Finished: emart24', run_time)
        except Exception as e:
            write_log(f'emart24 failed: {e}', run_time)

    #데이터 후처리 (Cleaning & Categorizing)
    try:
        from utils.data_cleaner import clean_and_merge
        clean_and_merge()
        write_log('Finished: data_cleaner', run_time)

        from utils.data_categorize import run_categorization
        run_categorization()
        write_log('Finished: data_categorize', run_time)
    except Exception as e:
        write_log(f'Post-processing failed: {e}', run_time)

    write_log('=== BATCH COMPLETE ===', run_time)
    return True