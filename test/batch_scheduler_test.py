import sys
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

try:
    from batch.batch_scheduler_manager import SchedulerManager
except ImportError:
    print("SchedulerManager 모듈을 찾을 수 없습니다. 경로를 확인해주세요.")

def test():
    manager = SchedulerManager()
    manager.start()
    test_id = "test_job_123"
    manager.add_job(
        day=1, hour=0, minute=0,
        year=2026, month=2,
        batch_name="테스트배치",
        job_id=test_id,
        dry_run=True
    )

    print("\n--- [START] 스케줄러 즉시 실행 테스트 ---")
    manager.trigger_now(test_id)
    print("--- [END] 테스트 완료 ---\n")

    # 4. 등록된 리스트 확인
    jobs = manager.get_jobs()
    print(f"현재 등록된 작업 개수: {jobs['total_jobs']}")

    manager.stop()


if __name__ == "__main__":
    test()