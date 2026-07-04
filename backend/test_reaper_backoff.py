import asyncio
import urllib.request
import json
import sys
import time
import datetime
import subprocess
import os
import uuid
from sqlalchemy import select, update, text

# Add backend to sys.path so we can query DB and import worker/reaper components
sys.path.append("backend")
from app.database import AsyncSessionLocal
from app.models.job_execution import JobExecution
from app.models.job import Job, JobStatus
from app.models.worker import Worker, WorkerStatus
from app.models.worker_heartbeat import WorkerHeartbeat
from app.models.dead_letter_queue import DeadLetterQueue
from worker.reaper import reap_once

BASE = "http://localhost:8000/api/v1"

def api(method, path, body=None, token=None):
    url = f"{BASE}{path}"
    data = json.dumps(body).encode() if body else None
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        resp = urllib.request.urlopen(req)
        return resp.status, json.loads(resp.read().decode()) if resp.status != 204 else None
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())

async def run_reaper_backoff_test():
    print("=" * 70)
    print("Reaper Backoff & DLQ Verification Test (Subprocess Crash Simulation)")
    print("=" * 70)

    # 1. Register/Login
    print("\n1. Registering test user...")
    status, data = api("POST", "/auth/register", {
        "email": f"reaper-test-{int(time.time())}@example.com",
        "password": "securepass123",
        "org_name": f"Reaper Org {int(time.time())}"
    })
    assert status == 201
    token = data["access_token"]

    # 2. Get/Create Project
    status, data = api("POST", "/projects", {"name": "Reaper Project"}, token=token)
    assert status == 201
    project_id = data["id"]

    # 3. Create Queue with Linear Retry Policy (max 3 retries, base delay 2s)
    # The policy ID 'a0000000-0000-0000-0000-000000000002' corresponds to the seeded linear policy
    print("\n2. Creating queue with linear retry policy (max 5 retries, base delay 2s)...")
    status, data = api("POST", f"/projects/{project_id}/queues", {
        "name": "reaper-queue",
        "priority": 80,
        "concurrency_limit": 1,
        "retry_policy_id": "a0000000-0000-0000-0000-000000000002"
    }, token=token)
    assert status == 201
    queue_id = data["id"]

    # 4. Submit immediate slow job
    print("\n3. Submitting slow job...")
    status, data = api("POST", f"/queues/{queue_id}/jobs", {
        "job_type": "immediate",
        "payload": {"duration": 15, "task": "slow_job"}
    }, token=token)
    assert status == 201
    job_id = data["id"]

    # 5. Start a worker subprocess
    print("\n4. Starting worker subprocess...")
    env = os.environ.copy()
    env["DATABASE_URL"] = "postgresql+asyncpg://scheduler:scheduler@localhost:5432/job_scheduler"
    env["POLL_INTERVAL_SECONDS"] = "1"
    env["HEARTBEAT_INTERVAL_SECONDS"] = "2"
    
    proc = subprocess.Popen(
        [sys.executable, "-m", "worker.engine"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    # 6. Wait for worker to claim the job and set it to running
    print("   Waiting for worker to claim the job...")
    worker_id = None
    for _ in range(15):
        time.sleep(1)
        async with AsyncSessionLocal() as session:
            job_res = await session.execute(select(Job).where(Job.id == job_id))
            job_obj = job_res.scalar_one()
            
            exec_res = await session.execute(
                select(JobExecution).where(JobExecution.job_id == job_id, JobExecution.status == "running")
            )
            exec_obj = exec_res.scalar_one_or_none()

            if job_obj.status == JobStatus.RUNNING and exec_obj:
                worker_id = exec_obj.worker_id
                print(f"   Job is now RUNNING! Claimed by worker: {worker_id}")
                break
    
    assert worker_id is not None, "Worker did not claim the job within timeout."

    # 7. SIGKILL/Hard crash the worker process mid-job
    print("\n5. Killing worker process (SIGKILL simulation)...")
    proc.kill()
    proc.wait()
    print("   Worker process hard terminated.")

    # 8. Directly update worker's heartbeat in DB to be stale
    print("\n6. Updating heartbeat record in database to simulate stale status (120s ago)...")
    async with AsyncSessionLocal() as session:
        async with session.begin():
            # Set worker heartbeat time to 120 seconds ago
            stale_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=120)
            await session.execute(
                update(WorkerHeartbeat)
                .where(WorkerHeartbeat.worker_id == worker_id)
                .values(last_seen=stale_time)
            )
            print("   Heartbeat updated to stale.")

    # 9. Run the reaper once
    print("\n7. Executing reaper run pass...")
    # Temporarily set STALE_THRESHOLD_SECONDS to 60 for consistency
    os.environ["STALE_THRESHOLD_SECONDS"] = "60"
    await reap_once()

    # 10. Verify (a) retry_count incremented, (b) scheduled state, (c) run_at matches backoff delay
    print("\n8. Verifying job state post-reclamation...")
    async with AsyncSessionLocal() as session:
        job_res = await session.execute(select(Job).where(Job.id == job_id))
        job_obj = job_res.scalar_one()
        
        exec_res = await session.execute(
            select(JobExecution).where(JobExecution.job_id == job_id).order_by(JobExecution.started_at.desc())
        )
        executions = exec_res.scalars().all()

    print(f"   Job Status: {job_obj.status} (Expected: scheduled)")
    print(f"   Job Retry Count: {job_obj.retry_count} (Expected: 1)")
    print(f"   Job Scheduled run_at: {job_obj.run_at}")
    
    # Assert status is scheduled
    assert job_obj.status == JobStatus.SCHEDULED, f"Expected scheduled status, got {job_obj.status}"
    # Assert retry count is 1
    assert job_obj.retry_count == 1, f"Expected retry count to be 1, got {job_obj.retry_count}"
    # Verify execution was marked failed
    assert executions[0].status == "failed", f"Expected execution status 'failed', got {executions[0].status}"
    
    # Verify backoff calculation
    # Linear policy formula for attempt 1: 2s * 1 = 2s delay.
    # Allowing minor query offset, expected run_at should be ~2s in the future
    time_diff = (job_obj.run_at - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
    print(f"   Calculated delay remaining: {time_diff:.2f}s (Expected: ~2.0s)")
    assert 1.0 <= time_diff <= 5.0, f"Expected delay around 2s, calculated delay was {time_diff}s"
    print("   [OK] First reclamation verification successful!")

    # 11. Simulate maximum retries exhaustion via consecutive crashes
    print("\n9. Simulating retry exhaustion (forcing retry_count to max_retries = 5)...")
    async with AsyncSessionLocal() as session:
        async with session.begin():
            # Artificially set retry_count to 5 (equal to linear policy max_retries)
            # and status back to running with a new mock execution to simulate subsequent crashes
            await session.execute(
                update(Job)
                .where(Job.id == job_id)
                .values(status=JobStatus.RUNNING, retry_count=5)
            )
            # Create a running execution row for the mock crash
            new_exec = JobExecution(
                id=uuid.uuid4(),
                job_id=job_id,
                worker_id=worker_id,
                status="running",
                started_at=datetime.datetime.now(datetime.timezone.utc),
                attempt_number=6
            )
            session.add(new_exec)
            
            # Keep worker heartbeat stale
            stale_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=120)
            await session.execute(
                update(WorkerHeartbeat)
                .where(WorkerHeartbeat.worker_id == worker_id)
                .values(last_seen=stale_time)
            )
            # Ensure the worker itself is online for the reaper to pick it up
            await session.execute(
                update(Worker)
                .where(Worker.id == worker_id)
                .values(status=WorkerStatus.ONLINE)
            )

    print("   Running reaper pass again to trigger DLQ routing...")
    await reap_once()

    # 12. Verify (d) job in dead_letter and DLQ entry exists
    print("\n10. Verifying job status and DLQ routing post-exhaustion...")
    async with AsyncSessionLocal() as session:
        job_res = await session.execute(select(Job).where(Job.id == job_id))
        job_obj = job_res.scalar_one()

        dlq_res = await session.execute(select(DeadLetterQueue).where(DeadLetterQueue.job_id == job_id))
        dlq_obj = dlq_res.scalar_one_or_none()

    print(f"    Job Status: {job_obj.status} (Expected: dead_letter)")
    print(f"    Job Retry Count: {job_obj.retry_count} (Expected: 6)")
    print(f"    DLQ Entry Found: {dlq_obj is not None}")
    if dlq_obj:
        print(f"    DLQ Failure Reason: {dlq_obj.failure_reason}")

    assert job_obj.status == JobStatus.DEAD_LETTER, f"Expected dead_letter status, got {job_obj.status}"
    assert job_obj.retry_count == 6, f"Expected retry count to be 6, got {job_obj.retry_count}"
    assert dlq_obj is not None, "Expected DeadLetterQueue entry to be created."
    assert "Worker stalled or crashed" in dlq_obj.failure_reason
    print("   [OK] Dead Letter Queue routing verification successful!")

    # ==========================================
    # PHASE 4 REST API VERIFICATIONS
    # ==========================================
    print("\n11. Verification of GET /retry-policies endpoint...")
    status, policies = api("GET", "/retry-policies", token=token)
    assert status == 200
    print(f"    Available retry policies count: {len(policies)}")
    assert len(policies) >= 3, f"Expected at least 3 seeded policies, got {len(policies)}"
    policy_names = [p["name"] for p in policies]
    assert "Default Fixed (3 retries, 1s)" in policy_names
    print("    [OK] Seeded policies listed successfully.")

    print("\n12. Verification of GET /queues/{queue_id}/dlq (Listing)...")
    status, dlq_list = api("GET", f"/queues/{queue_id}/dlq?page=1&page_size=10", token=token)
    assert status == 200
    print(f"    DLQ list items: {len(dlq_list['items'])}, total: {dlq_list['total']}")
    assert dlq_list["total"] == 1
    assert dlq_list["items"][0]["job_id"] == str(job_id)
    print("    [OK] DLQ jobs listed successfully.")

    # 13. Test cross-org access control boundaries
    print("\n13. Testing cross-org security boundary (403 Forbidden checks)...")
    # Register another user under a completely separate org
    status, data2 = api("POST", "/auth/register", {
        "email": f"other-user-{int(time.time())}@example.com",
        "password": "securepass123",
        "org_name": f"Other Org {int(time.time())}"
    })
    assert status == 201
    other_token = data2["access_token"]

    # Try listing the first org's queue's DLQ with other_token
    status, other_res = api("GET", f"/queues/{queue_id}/dlq", token=other_token)
    print(f"    GET /queues/{{id}}/dlq cross-org status: {status} (Expected: 403)")
    assert status == 403

    # Try requeuing the first org's job with other_token
    status, other_res = api("POST", f"/dlq/{job_id}/requeue", token=other_token)
    print(f"    POST /dlq/{{id}}/requeue cross-org status: {status} (Expected: 403)")
    assert status == 403

    # Try discarding the first org's job with other_token
    status, other_res = api("DELETE", f"/dlq/{job_id}", token=other_token)
    print(f"    DELETE /dlq/{{id}} cross-org status: {status} (Expected: 403)")
    assert status == 403
    print("    [OK] Cross-org access control verified successfully.")

    # Fetch execution count before requeuing to ensure preservation
    async with AsyncSessionLocal() as session:
        initial_exec_res = await session.execute(select(JobExecution).where(JobExecution.job_id == job_id))
        initial_exec_count = len(initial_exec_res.scalars().all())

    # 14. Test manual requeue via REST API
    print("\n14. Requeuing job manually via POST /dlq/{job_id}/requeue...")
    status, requeued_job = api("POST", f"/dlq/{job_id}/requeue", token=token)
    assert status == 200
    print(f"    Requeued Job Status: {requeued_job['status']} (Expected: queued)")
    print(f"    Requeued Job Retry Count: {requeued_job['retry_count']} (Expected: 0)")
    assert requeued_job["status"] == "queued"
    assert requeued_job["retry_count"] == 0

    # Ensure DLQ record is deleted
    async with AsyncSessionLocal() as session:
        dlq_check = await session.execute(select(DeadLetterQueue).where(DeadLetterQueue.job_id == job_id))
        dlq_row = dlq_check.scalar_one_or_none()
        
        # Verify executions and logs are fully preserved untouched
        exec_check = await session.execute(select(JobExecution).where(JobExecution.job_id == job_id))
        exec_rows = exec_check.scalars().all()
        
    print(f"    DLQ entry deleted: {dlq_row is None} (Expected: True)")
    print(f"    Job executions preserved count: {len(exec_rows)} (Expected: >= {initial_exec_count})")
    assert dlq_row is None
    assert len(exec_rows) >= initial_exec_count
    print("    [OK] Manual requeue verified (counters reset, history preserved).")

    # 15. Test manual soft-discard via REST API
    # Re-dead-letter the job artificially to verify soft-discard
    print("\n15. Discarding job manually (soft-discard) via DELETE /dlq/{job_id}...")
    async with AsyncSessionLocal() as session:
        async with session.begin():
            await session.execute(
                update(Job)
                .where(Job.id == job_id)
                .values(status=JobStatus.DEAD_LETTER)
            )
            mock_dlq = DeadLetterQueue(
                id=uuid.uuid4(),
                job_id=job_id,
                failure_reason="Forced deletion test",
                moved_at=datetime.datetime.now(datetime.timezone.utc)
            )
            session.add(mock_dlq)

    # Record initial executions count
    async with AsyncSessionLocal() as session:
        exec_count_check = await session.execute(select(JobExecution).where(JobExecution.job_id == job_id))
        pre_discard_exec_count = len(exec_count_check.scalars().all())

    status, _ = api("DELETE", f"/dlq/{job_id}", token=token)
    assert status == 204
    
    # Assert job is NOT deleted but transitioned to failed
    status, job_details_post = api("GET", f"/jobs/{job_id}", token=token)
    print(f"    GET /jobs/{{id}} status post-discard: {status} (Expected: 200)")
    print(f"    Job status in payload: {job_details_post['status']} (Expected: failed)")
    assert status == 200
    assert job_details_post["status"] == "failed"

    # Assert DLQ row is deleted, and JobExecutions are preserved
    async with AsyncSessionLocal() as session:
        dlq_check_deleted = await session.execute(select(DeadLetterQueue).where(DeadLetterQueue.job_id == job_id))
        dlq_row_deleted = dlq_check_deleted.scalar_one_or_none()

        exec_check_preserved = await session.execute(select(JobExecution).where(JobExecution.job_id == job_id))
        exec_rows_preserved = exec_check_preserved.scalars().all()

    print(f"    DLQ entry post-discard: {'PRESENT' if dlq_row_deleted else 'DELETED'} (Expected: DELETED)")
    print(f"    Job executions post-discard: {len(exec_rows_preserved)} (Expected: >= {pre_discard_exec_count})")
    assert dlq_row_deleted is None
    assert len(exec_rows_preserved) >= pre_discard_exec_count
    print("    [OK] Manual soft-discard verified (job status failed, history preserved).")

    # Clean up project
    print("\nCleaning up project...")
    api("DELETE", f"/projects/{project_id}", token=token)

    print("\n" + "=" * 70)
    print("ALL REAPER BACKOFF, DLQ & PHASE 4 REST API CHECKS PASSED!")
    print("=" * 70)

if __name__ == "__main__":
    asyncio.run(run_reaper_backoff_test())
