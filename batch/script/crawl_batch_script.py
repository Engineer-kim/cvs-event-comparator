import os
import sys
import importlib
from datetime import datetime

# 최상위 폴더 설정
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)

# 로그 저장 기본 경로 (루트/batch/batch_script)
LOG_BASE_DIR = os.path.join(PROJECT_ROOT, 'batch', 'batch_script_log')


def get_log_path(run_time: datetime):
    """실행 시점(run_time)을 기준으로 단 하나의 로그 파일 경로를 생성"""

    yy = run_time.year % 100
    m = run_time.month
    dirname = f"{yy}_{m}"
    dirpath = os.path.join(LOG_BASE_DIR, dirname)

    # 폴더는 여기서 딱 한 번 생성
    os.makedirs(dirpath, exist_ok=True)

    fname = run_time.strftime('batch_script_%Y%m%d_%H%M%S.log')
    return os.path.join(dirpath, fname)


def write_log(msg: str, run_time: datetime):
    """전달받은 run_time 기준의 로그 파일에 메시지 추가"""
    path = get_log_path(run_time)

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    line = f"[{timestamp} KST] {msg}\n"
    with open(path, 'a', encoding='utf-8') as f:
        f.write(line)
    print(line, end='')


def make_datetime(fixed_dt: datetime):
    """스크래퍼 내부의 datetime.now()를 배치 시점으로 고정"""
    if fixed_dt is None:
        fixed_dt = datetime.now()

    class DateTime:
        @staticmethod
        def now():
            return fixed_dt

    return DateTime


def get_next_month_data_batch(year: int, month: int, run_time: datetime, dry_run: bool = False) -> bool:
    """
    메인 배치 함수
    """
    # 현재 작업 디렉토리를 프로젝트 루트로 변경
    os.chdir(PROJECT_ROOT)

    data_dir = os.path.join(PROJECT_ROOT, 'data')

    os.makedirs(data_dir, exist_ok=True)

    # 환경변수로 data 폴더 경로 전달
    os.environ['DATA_DIR'] = data_dir

    # 2. 시작 로그 기록
    write_log('=== BATCH START ===', run_time)
    write_log(f'Target Month: {year}-{month} | Batch ID Time: {run_time.strftime("%H:%M:%S")}', run_time)
    write_log(f'Data directory: {data_dir}', run_time)

    # 3. 스크래퍼 패칭
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

    # 4. 크롤링 실행
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

    # 5. 후처리
    try:
        from utils.data_cleaner_batch import clean_and_merge_batch
        clean_and_merge_batch()
        write_log('Finished: data_cleaner', run_time)

        from utils.data_categorize import run_categorization
        run_categorization()
        write_log('Finished: data_categorize', run_time)
    except Exception as e:
        write_log(f'Post-processing failed: {e}', run_time)

    write_log('=== BATCH COMPLETE ===', run_time)
    return True