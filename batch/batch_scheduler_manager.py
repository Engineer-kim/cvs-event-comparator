import streamlit as st
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from loguru import logger
from datetime import datetime
import time

# 다음달 데이터 상품 가져오는 배치, 실패시  최대 3번까지 재시도
def run_monthly_batch_task(run_time=None, max_retry=3):
    run_time = run_time or datetime.now()
    logger.info(f"🚀 [다음달 데이터 상품 가져오는 배치 시작] {run_time.strftime('%Y-%m-%d %H:%M:%S')} - 실행")

    next_month = (run_time.month % 12) + 1
    year = run_time.year + (1 if next_month == 1 else 0)

    attempt = 0
    success = False
    while attempt <= max_retry and not success:
        try:
            from batch.script.cron_crawl import get_next_month_data_batch
            get_next_month_data_batch(year=year, month=next_month, dry_run=False, run_time=run_time)
            logger.success(f"✅ [다음달 상품 데이터 가져오는 배치 완료] {datetime.now().strftime('%H:%M:%S')} - 성공")
            success = True
        except Exception as e:
            attempt += 1
            logger.error(f"❌ [다음달 상품 데이터 가져오는 배치 오류] 실행 중 예외 발생: {e}")
            if attempt <= max_retry:
                logger.info(f"🔁 재시도 {attempt}/{max_retry} 진행 중...")
                time.sleep(5)
            else:
                logger.error(f"❌ [다음달 상품 데이터 가져오는 배치] 모든 재시도 실패")

class SchedulerManager:
    def __init__(self):
        self.scheduler = BackgroundScheduler(
            jobstores={'default': MemoryJobStore()},
            timezone='Asia/Seoul'
        )

    def add_job(self, day, hour, minute, batch_id):
        job_config = {
            'day': day,
            'hour': hour,
            'minute': minute,
            'id': batch_id
        }
        self.scheduler.add_job(
            run_monthly_batch_task,
            'cron',
            day=job_config['day'],
            hour=job_config['hour'],
            minute=job_config['minute'],
            id=job_config['id'],
            replace_existing=True,
            kwargs=job_config
        )
        logger.info(f"📅 월간 배치 등록 완료: {job_config['id']} (매월 {job_config['day']}일 {job_config['hour']}:{job_config['minute']})")

    def start(self):
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("🟢 Scheduler Manager: 백그라운드 스케줄러 활성화.")

    def get_info(self):
        jobs = self.scheduler.get_jobs()
        job_details = []
        for job in jobs:
            job_details.append({
                "id": job.id,
                "next_run": job.next_run_time.strftime('%Y-%m-%d %H:%M:%S') if job.next_run_time else "N/A"
            })
        return {
            "is_running": self.scheduler.running,
            "jobs": job_details
        }

@st.cache_resource
def get_scheduler_manager():
    manager = SchedulerManager()
    manager.start()
    return manager