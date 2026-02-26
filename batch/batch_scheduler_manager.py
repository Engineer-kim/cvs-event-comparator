import streamlit as st
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from loguru import logger
from datetime import datetime
import pytz
import time
import os

# 경로 설정
CURRENT_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_FILE_DIR)
SCHEDULER_LOG_DIR = os.path.join(PROJECT_ROOT, "batch", "batch_scheduler_log")


def get_kst_now():
    kst = pytz.timezone('Asia/Seoul')
    return datetime.now(kst).replace(tzinfo=None)


# 배치 작업: 지정된 연/월의 데이터를 가져옴 (재시도 최대 3회)
def run_monthly_batch_task(year: int, month: int, batch_name: str = None, max_retry: int = 3, dry_run: bool = False):
    """
    지정된 연/월의 배치를 실행합니다.

    Args:
        year: 실행 대상 연도 (e.g., 2026)
        month: 실행 대상 월 (1-12)
        batch_name: 배치 식별자 (로그용)
        max_retry: 최대 재시도 횟수
        dry_run: True이면 크롤링 건너뜀
    """
    batch_name = batch_name or f"{year}년 {month}월"
    run_time = datetime(year, month, 1, 0, 30, 0)
    logger.info(f"🚀 [{batch_name}] 스케줄러에 의해 배치 루틴 호출됨")

    attempt = 0
    success = False
    while attempt <= max_retry and not success:
        try:
            from batch.script.crawl_batch_script import get_next_month_data_batch
            get_next_month_data_batch(year=year, month=month, dry_run=dry_run, run_time=run_time)
            logger.success(f"✅ [{batch_name}] 배치 완료 - {get_kst_now().strftime('%H:%M:%S')}")
            success = True
        except Exception as e:
            attempt += 1
            logger.error(f"❌ [{batch_name}] 배치 오류: {e}")
            if attempt <= max_retry:
                logger.info(f"🔁 재시도 {attempt}/{max_retry}회 진행 중...")
                time.sleep(5)
            else:
                logger.error(f"❌ [{batch_name}] 모든 재시도 실패")


class SchedulerManager:
    """여러 개의 배치 작업을 관리하는 스케줄러"""

    def __init__(self):
        os.makedirs(SCHEDULER_LOG_DIR, exist_ok=True)

        log_file_path = os.path.join(SCHEDULER_LOG_DIR, "scheduler_{time:YYYY-MM-DD}.log")

        # 기본 핸들러 제거 후 스케줄러 전용 로그 설정 적용
        logger.remove()
        logger.add(
            log_file_path,
            rotation="00:00",
            retention="30 days",
            level="INFO",
            encoding="utf-8",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
            enqueue=True  # 멀티쓰레드 안전성 확보
        )

        print(f"DEBUG: Scheduler Log will be saved at -> {log_file_path}")

        self.scheduler = BackgroundScheduler(
            jobstores={'default': MemoryJobStore()},
            timezone='Asia/Seoul'
        )
        self.job_configs = {}  # job_id별 설정 저장

    def add_job(self, day: int, hour: int, minute: int,year: int = None, month: int = None,
                batch_name: str = None, job_id: str = None, dry_run: bool = False):
        """
        새로운 배치 작업을 등록 부(커스텀 해서 사용가능)

        Args:
            day: 실행 날짜 (1-31, * 사용 가능)
            hour: 실행 시간 (0-23)
            minute: 실행 분 (0-59)
            batch_name: 배치 이름 (기본값: "연도월")
            job_id: job 식별자 (기본값: "batch_{year}_{month}_{day}_{hour}_{minute}")
            dry_run: True이면 크롤링 건너뜀
        """
        now = datetime.now()
        target_year = year or now.year
        target_month = month or now.month
        batch_name = batch_name or f"{target_year}년 {target_month}월"
        job_id = job_id or f"batch_{target_year}_{target_month}_{day}_{hour}_{minute}"

        # 기존 job이 있으면 제거
        if self.scheduler.get_job(job_id):
            self.scheduler.remove_job(job_id)
            logger.info(f"⚠️  기존 job 제거: {job_id}")

        # job 설정 저장
        self.job_configs[job_id] = {
            'day': day,
            'hour': hour,
            'minute': minute,
            'year': year,
            'month': month,
            'batch_name': batch_name,
            'dry_run': dry_run
        }

        # job 추가
        self.scheduler.add_job(
            run_monthly_batch_task,
            'cron',
            day=day,
            hour=hour,
            minute=minute,
            id=job_id,
            replace_existing=True,
            kwargs={
                'year': year,
                'month': month,
                'batch_name': batch_name,
                'dry_run': dry_run
            }
        )
        logger.info(f"✅ 배치 등록 완료: {job_id}")
        logger.info(f"   매월 {day}일 {hour:02d}:{minute:02d} - [{batch_name}] (dry_run={dry_run})")

    def remove_job(self, job_id: str):
        """등록된 배치 작업을 제거합니다."""
        if self.scheduler.get_job(job_id):
            self.scheduler.remove_job(job_id)
            if job_id in self.job_configs:
                del self.job_configs[job_id]
            logger.info(f"🗑️  job 제거: {job_id}")
        else:
            logger.warning(f"⚠️  job을 찾을 수 없습니다: {job_id}")

    def start(self):
        """스케줄러를 시작합니다."""
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("🟢 스케줄러 시작됨")
        else:
            logger.info("⚠️  스케줄러가 이미 실행 중입니다")

    def stop(self):
        """스케줄러를 중지"""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("🛑 스케줄러 중지됨")

    def get_jobs(self):
        """Batch Job 리턴."""
        jobs = self.scheduler.get_jobs()
        job_details = []
        for job in jobs:
            job_details.append({
                "id": job.id,
                "trigger": str(job.trigger),
                "next_run": job.next_run_time.strftime('%Y-%m-%d %H:%M:%S') if job.next_run_time else "N/A",
                "config": self.job_configs.get(job.id, {})
            })
        return {
            "is_running": self.scheduler.running,
            "total_jobs": len(job_details),
            "jobs": job_details
        }

    def get_job_info(self, job_id: str):
        """특정 job의 상세 정보 리턴."""
        job = self.scheduler.get_job(job_id)
        if job:
            return {
                "id": job.id,
                "trigger": str(job.trigger),
                "next_run": job.next_run_time.strftime('%Y-%m-%d %H:%M:%S') if job.next_run_time else "N/A",
                "config": self.job_configs.get(job_id, {})
            }
        else:
            return None

    # 라이브에서 호출 절대 금지
    def trigger_now(self, job_id: str):
        """등록된 job을 스케줄 상관없이 지금 즉시 한 번 실행(테스트용)"""
        job = self.scheduler.get_job(job_id)
        if job:
            logger.info(f"⚡ [테스트] Job {job_id} 즉시 실행 시작")
            # 스케줄러 스레드와 별개로 현재 스레드에서 즉시 실행
            job.func(**job.kwargs)
            return True
        else:
            logger.error(f"❌ 해당 ID의 Job이 없습니다: {job_id}")
            return False


@st.cache_resource
def get_scheduler_manager():
    """Streamlit 캐시를 사용해 전역 스케줄러 인스턴스를 반환"""
    manager = SchedulerManager()
    manager.start()
    return manager