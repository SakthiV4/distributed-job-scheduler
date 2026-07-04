import asyncio
import urllib.request
import json
import uuid
import sys
import datetime
from sqlalchemy import select
from app.database import AsyncSessionLocal
from app.models.job import Job, JobStatus
from app.models.dead_letter_queue import DeadLetterQueue
from app.models.job_execution import JobExecution
from app.models.worker import Worker

BASE = "http://localhost:8000/api/v1"

def api(method, path, body=None, token=None):
    url = f"{BASE}{path}" if path.startswith("/") else path
    data = json.dumps(body).encode() if body else None
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        resp = urllib.request.urlopen(req)
        return resp.status, json.loads(resp.read().decode()) if resp.status != 204 else None
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except Exception:
            return e.code, e.read().decode()

async def run_dlq_soft_discard_test():
    print("=" * 70)
    print("Phase 6 E2E Verification: Soft-Discard & System Router")
    print("=" * 70)

    # 1. Login with seeded credentials
    print("\n1. Logging in with seeded admin@scheduler.xyz credentials...")
    status, auth_data = api("POST", "/auth/login", {
        "email": "admin@scheduler.xyz",
        "password": "AdminPassword123!"
    })
    assert status == 200, f"Failed to login: {status}"
    token = auth_data["access_token"]
    print("   [OK] Seeded demo admin login successful.")

    # 2. Get the seeded Project & Queue
    print("\n2. Fetching seeded projects and queues...")
    status, projects_data = api("GET", "/projects", token=token)
    assert status == 200
    project_id = projects_data["items"][0]["id"]
    
    status, queues_data = api("GET", f"/projects/{project_id}/queues", token=token)
    assert status == 200
    queue_id = queues_data["items"][0]["id"]
    print(f"   [OK] Found project_id: {project_id}, queue_id: {queue_id}")

    # 3. Test system/summary endpoint
    print("\n3. Testing GET /system/summary endpoint...")
    status, summary = api("GET", f"/system/summary?project_id={project_id}", token=token)
    assert status == 200
    assert "job_counts" in summary
    assert "active_workers_count" in summary
    print(f"   [OK] Summary response: {summary}")

    # 4. Test system/workers endpoint
    print("\n4. Testing GET /system/workers endpoint...")
    status, workers = api("GET", "/system/workers", token=token)
    assert status == 200
    print(f"   [OK] Fleet list retrieved successfully, workers count: {len(workers)}")

    # 5. Create a job, execution, and DLQ row directly in DB to run soft-discard check
    print("\n5. Inserting a mock job directly into the DLQ database tables...")
    job_id = uuid.uuid4()
    async with AsyncSessionLocal() as session:
        async with session.begin():
            # Get a valid worker_id to avoid ForeignKeyViolationError
            worker_res = await session.execute(select(Worker.id))
            worker_id = worker_res.scalars().first()
            if not worker_id:
                worker_id = uuid.uuid4()
                mock_worker = Worker(
                    id=worker_id,
                    hostname="mock-verification-worker",
                    status="online"
                )
                session.add(mock_worker)
                await session.flush()

            mock_job = Job(
                id=job_id,
                queue_id=uuid.UUID(queue_id),
                status=JobStatus.DEAD_LETTER,
                job_type="immediate",
                retry_count=5
            )
            session.add(mock_job)
            await session.flush()

            # Add mock execution history
            mock_execution = JobExecution(
                id=uuid.uuid4(),
                job_id=job_id,
                worker_id=worker_id,
                status="failed",
                started_at=datetime.datetime.now(datetime.timezone.utc),
                attempt_number=1
            )
            session.add(mock_execution)

            # Add DLQ entry
            mock_dlq = DeadLetterQueue(
                id=uuid.uuid4(),
                job_id=job_id,
                failure_reason="Simulated terminal crash",
                moved_at=datetime.datetime.now(datetime.timezone.utc)
            )
            session.add(mock_dlq)
    print("   [OK] Mock DLQ job inserted.")

    # 6. Call DELETE /dlq/{job_id} to test soft-discard
    print("\n6. Invoking DELETE /dlq/{job_id} REST API...")
    status, discard_res = api("DELETE", f"/dlq/{job_id}", token=token)
    assert status == 204, f"Failed to discard: {status}"
    print("   [OK] REST API returned 204 No Content.")

    # 7. Verify the DB directly for soft-discard (DLQ deleted, Job status failed, Executions preserved)
    print("\n7. Direct Database Query Auditing for Soft-Discard Verification...")
    async with AsyncSessionLocal() as session:
        # Check Job row status
        job_res = await session.execute(select(Job).where(Job.id == job_id))
        job = job_res.scalar_one_or_none()
        
        # Check DLQ entry
        dlq_res = await session.execute(select(DeadLetterQueue).where(DeadLetterQueue.job_id == job_id))
        dlq = dlq_res.scalar_one_or_none()

        # Check execution rows
        exec_res = await session.execute(select(JobExecution).where(JobExecution.job_id == job_id))
        executions = exec_res.scalars().all()

    print(f"    Job row status in DB: {job.status if job else 'DELETED'} (Expected: failed)")
    print(f"    DLQ row in DB: {'PRESENT' if dlq else 'DELETED'} (Expected: DELETED)")
    print(f"    Execution history rows in DB: {len(executions)} (Expected: 1)")

    assert job is not None
    assert job.status == JobStatus.FAILED, f"Expected failed, got {job.status}"
    assert dlq is None, "DLQ entry was not deleted!"
    assert len(executions) == 1, "Execution history was incorrectly deleted!"
    print("   [OK] Soft-discard verified successfully! History is preserved.")

    print("\n" + "=" * 70)
    print("ALL PHASE 6 E2E VERIFICATIONS COMPLETED & PASSED!")
    print("=" * 70)

if __name__ == "__main__":
    asyncio.run(run_dlq_soft_discard_test())
