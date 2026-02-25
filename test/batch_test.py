"""
테스트베드 환경으로 2026년 3월을 대상으로 배치 크롤링을 실행해봅니다.
데이터는 2월꺼로 가정
"""
import sys, os
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from batch.script.cron_crawl import get_next_month_data_batch

TARGET_YEAR = 2026
TARGET_MONTH = 2

print(f"Running batch test for {TARGET_YEAR}-{TARGET_MONTH} (dry-run)")
get_next_month_data_batch(TARGET_YEAR, TARGET_MONTH, dry_run=False)

log_dir = os.path.join(PROJECT_ROOT, 'crawl_batch_log', f"{str(TARGET_YEAR)[-2:]}_{TARGET_MONTH}")
print('Expected log dir:', log_dir)
print('Exists:', os.path.exists(log_dir))
if os.path.exists(log_dir):
    print('Files:', os.listdir(log_dir))
else:
    print('No logs created')
