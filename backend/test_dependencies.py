import asyncio
import urllib.request
import json
import sys
import time
import uuid
from datetime import datetime, timezone

# Add backend to sys.path
sys.path.append("backend")
from app.database import AsyncSessionLocal
from app.models.job import Job, JobStatus
from app.models.queue import Queue
from app.models.project import Project
from worker.engine import WorkerEngine

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
        try:
            return e.code, json.loads(e.read().decode())
        except Exception:
            return e.code, e.read().decode()

async def run_dependency_tests():
    print("=" * 70)
    print("PHASE 9: JOB DEPENDENCY DAG INTEGRATION TESTS")
    print("=" * 70)

    # 1. Auth Setup
    print("\nSetting up authentication...")
    status, data = api("POST", "/auth/register", {
        "email": "dep-test@example.com",
        "password": "securepass123",
        "org_name": f"Dependency Testing Org"
    })
    if status == 409:
        status, data = api("POST", "/auth/login", {
            "email": "dep-test@example.com",
            "password": "securepass123"
        })
    assert status in (200, 201), f"Auth failed: {data}"
    token = data["access_token"]

    # 2. Project & Queue Setup
    print("Creating test projects and queues...")
    # Project A
    proj_a_name = f"Project A {int(time.time())}"
    status, data = api("POST", "/projects", {"name": proj_a_name}, token=token)
    assert status == 201, f"Failed to create Project A: {status} -> {data}"
    proj_a_id = data["id"]
    
    # Queue A (Project A)
    status, data = api("POST", f"/projects/{proj_a_id}/queues", {
        "name": "queue-a", "priority": 10, "concurrency_limit": 5
    }, token=token)
    assert status == 201, f"Failed to create Queue A: {status} -> {data}"
    queue_a_id = data["id"]

    # Project B (separate tenant)
    proj_b_name = f"Project B {int(time.time())}"
    status, data = api("POST", "/projects", {"name": proj_b_name}, token=token)
    assert status == 201, f"Failed to create Project B: {status} -> {data}"
    proj_b_id = data["id"]
    
    # Queue B (Project B)
    status, data = api("POST", f"/projects/{proj_b_id}/queues", {
        "name": "queue-b", "priority": 10, "concurrency_limit": 5
    }, token=token)
    assert status == 201, f"Failed to create Queue B: {status} -> {data}"
    queue_b_id = data["id"]

    # Initialize a test worker engine instance
    worker = WorkerEngine()
    await worker.register_worker()
    # Load queue-a database object
    async with AsyncSessionLocal() as session:
        res = await session.execute(
            select(Queue).where(Queue.id == uuid.UUID(queue_a_id))
        )
        db_queue_a = res.scalar_one()

    # -------------------------------------------------------------------------
    # CASE 1: Submission Validation
    # -------------------------------------------------------------------------
    print("\n--- CASE 1: Submission Validation ---")
    
    # Submit parent job in Project A
    status, parent_job = api("POST", f"/queues/{queue_a_id}/jobs", {
        "job_type": "immediate",
        "payload": {"task": "parent_task"}
    }, token=token)
    assert status == 201
    parent_job_id = parent_job["id"]
    print(f"Parent Job submitted successfully in Project A: {parent_job_id}")

    # Submit parent job in Project B
    status, parent_job_b = api("POST", f"/queues/{queue_b_id}/jobs", {
        "job_type": "immediate",
        "payload": {"task": "parent_task_b"}
    }, token=token)
    assert status == 201
    parent_job_b_id = parent_job_b["id"]
    print(f"Parent Job B submitted successfully in Project B: {parent_job_b_id}")

    # A: Reject dependency belonging to a different project
    print("Testing cross-project dependency rejection...")
    status, err_data = api("POST", f"/queues/{queue_a_id}/jobs", {
        "job_type": "immediate",
        "payload": {"task": "child_task"},
        "depends_on": [parent_job_b_id]
    }, token=token)
    print(f"Response status: {status}, detail: {err_data.get('detail')}")
    assert status == 400
    assert "belongs to a different project" in err_data["detail"]
    print("=> Cross-project dependency successfully rejected!")

    # B: Reject non-existent dependency ID
    print("Testing non-existent dependency ID rejection...")
    fake_id = str(uuid.uuid4())
    status, err_data = api("POST", f"/queues/{queue_a_id}/jobs", {
        "job_type": "immediate",
        "payload": {"task": "child_task"},
        "depends_on": [fake_id]
    }, token=token)
    print(f"Response status: {status}, detail: {err_data.get('detail')}")
    assert status == 400
    assert "does not exist" in err_data["detail"]
    print("=> Non-existent dependency ID successfully rejected!")

    # -------------------------------------------------------------------------
    # CASE 2: Dependency Gating
    # -------------------------------------------------------------------------
    print("\n--- CASE 2: Dependency Gating ---")

    # Reset parent job back to queued so we can control its status
    async with AsyncSessionLocal() as session:
        async with session.begin():
            p_job = await session.get(Job, uuid.UUID(parent_job_id))
            p_job.status = JobStatus.QUEUED
            p_job.retry_count = 0

    # Submit Child Job B depending on Parent Job A
    status, child_job = api("POST", f"/queues/{queue_a_id}/jobs", {
        "job_type": "immediate",
        "payload": {"task": "child_task"},
        "depends_on": [parent_job_id]
    }, token=token)
    assert status == 201
    child_job_id = child_job["id"]
    print(f"Child Job B depends on Parent Job A: {child_job_id}")

    # A: Parent Job is QUEUED
    print("Checking child claimability when parent is 'queued'...")
    # Attempting to claim from queue-a
    # Since parent is queued and child is queued, the engine should claim the parent first, not the child
    claim1 = await worker.claim_job(db_queue_a)
    assert claim1 is not None
    claimed_job_id, _ = claim1
    assert str(claimed_job_id) == parent_job_id, f"Expected parent job {parent_job_id} to be claimed, got {claimed_job_id}"
    print("   Parent job claimed successfully.")

    # Try claiming again; since parent is now running, child should be skipped
    claim2 = await worker.claim_job(db_queue_a)
    assert claim2 is None, f"Child job was claimed early while parent is running!"
    print("=> Child job remained gated (queued) successfully while parent is 'running'!")

    # B: Parent Job is FAILED
    # NOTE: We manually set the parent's status in the database to isolate the 
    # dependency-gating query logic rather than running the full retry workflow.
    print("Checking child claimability when parent is 'failed'...")
    async with AsyncSessionLocal() as session:
        async with session.begin():
            p_job = await session.get(Job, uuid.UUID(parent_job_id))
            p_job.status = JobStatus.FAILED
    claim3 = await worker.claim_job(db_queue_a)
    assert claim3 is None, f"Child job was claimed while parent is failed!"
    print("=> Child job remained gated (queued) successfully while parent is 'failed'!")

    # C: Parent Job is DEAD_LETTER
    # NOTE: We manually set the parent's status in the database here to isolate
    # the dead-letter gating check without waiting for the full DLQ routing flow.
    print("Checking child claimability when parent is 'dead_letter'...")
    async with AsyncSessionLocal() as session:
        async with session.begin():
            p_job = await session.get(Job, uuid.UUID(parent_job_id))
            p_job.status = JobStatus.DEAD_LETTER
    claim4 = await worker.claim_job(db_queue_a)
    assert claim4 is None, f"Child job was claimed while parent is dead_letter!"
    print("=> Child job remained gated (queued) successfully while parent is 'dead_letter'!")

    # -------------------------------------------------------------------------
    # CASE 3: Completion Unblocking
    # -------------------------------------------------------------------------
    print("\n--- CASE 3: Completion Unblocking ---")
    
    # Transition parent to completed
    print("Setting parent status to 'completed'...")
    async with AsyncSessionLocal() as session:
        async with session.begin():
            p_job = await session.get(Job, uuid.UUID(parent_job_id))
            p_job.status = JobStatus.COMPLETED
            
    # Try claiming now; child should be claimable immediately!
    print("Attempting to claim child job B...")
    claim5 = await worker.claim_job(db_queue_a)
    assert claim5 is not None, "Child job was not claimed after parent completed!"
    claimed_child_id, _ = claim5
    assert str(claimed_child_id) == child_job_id, f"Expected child job {child_job_id} to be claimed, got {claimed_child_id}"
    print("=> Child job unblocked and claimed successfully after parent hit 'completed'!")

    # -------------------------------------------------------------------------
    # CASE 4: Concurrent Worker Safety
    # -------------------------------------------------------------------------
    print("\n--- CASE 4: Concurrent Worker Safety ---")
    
    # Submit Job C
    status, parent_job_c = api("POST", f"/queues/{queue_a_id}/jobs", {
        "job_type": "immediate",
        "payload": {"task": "parent_c"}
    }, token=token)
    assert status == 201
    parent_c_id = parent_job_c["id"]
    
    # Submit Job D depending on Job C
    status, child_job_d = api("POST", f"/queues/{queue_a_id}/jobs", {
        "job_type": "immediate",
        "payload": {"task": "child_d"},
        "depends_on": [parent_c_id]
    }, token=token)
    assert status == 201
    child_d_id = child_job_d["id"]
    
    # Set parent C status to 'running'
    async with AsyncSessionLocal() as session:
        async with session.begin():
            p_c = await session.get(Job, uuid.UUID(parent_c_id))
            p_c.status = JobStatus.RUNNING

    # Spawn 10 concurrent claim tasks simulating workers polling concurrently
    print("Workers polling concurrently while dependencies are unsatisfied...")
    workers_claims = []
    
    async def worker_poll(w_id):
        test_worker = WorkerEngine()
        test_worker.worker_id = w_id
        await test_worker.register_worker()
        return await test_worker.claim_job(db_queue_a)

    poll_tasks = [worker_poll(uuid.uuid4()) for _ in range(10)]
    poll_results = await asyncio.gather(*poll_tasks)
    
    # Verify no worker claimed the child job D while parent is running
    claimed_ids = [str(r[0]) for r in poll_results if r is not None]
    assert len(claimed_ids) == 0, f"Expected 0 claims, but saw jobs claimed: {claimed_ids}"
    print("   Verified 0 concurrent workers claimed the child job prematurely.")

    # Now, complete parent Job C
    print("Setting parent Job C status to 'completed'...")
    async with AsyncSessionLocal() as session:
        async with session.begin():
            p_c = await session.get(Job, uuid.UUID(parent_c_id))
            p_c.status = JobStatus.COMPLETED

    # Concurrently poll again now that dependencies are satisfied
    print("Workers polling concurrently immediately after dependencies completed...")
    poll_tasks2 = [worker_poll(uuid.uuid4()) for _ in range(10)]
    poll_results2 = await asyncio.gather(*poll_tasks2)

    # Analyze results
    success_claims = [r for r in poll_results2 if r is not None]
    print(f"   Total concurrent successful claims: {len(success_claims)}")
    
    # Verify exactly one worker got the child job D (concurrency limit = 5, but we only have 1 eligible job)
    assert len(success_claims) == 1, f"Expected exactly 1 claim, got {len(success_claims)}"
    claimed_job_id, _ = success_claims[0]
    assert str(claimed_job_id) == child_d_id, f"Expected claimed job to be child job D {child_d_id}, got {claimed_job_id}"
    print("=> Concurrent worker safety verified successfully! Exactly one worker claimed the child job.")

    # Clean up project
    print("\nCleaning up test project A...")
    api("DELETE", f"/projects/{proj_a_id}", token=token)
    print("Cleaning up test project B...")
    api("DELETE", f"/projects/{proj_b_id}", token=token)

    print("\n" + "=" * 70)
    print("ALL JOB DEPENDENCY DAG TESTS PASSED SUCCESSFULLY!")
    print("=" * 70)

if __name__ == "__main__":
    from sqlalchemy import select
    asyncio.run(run_dependency_tests())
