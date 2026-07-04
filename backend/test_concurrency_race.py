import asyncio
import urllib.request
import json
import sys
import time
import datetime
from sqlalchemy import select, func

# Since we want to query the DB directly, we add backend/ to sys.path
sys.path.append("backend")
from app.database import AsyncSessionLocal
from app.models.job_execution import JobExecution
from app.models.job import Job, JobStatus

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

async def run_race_test():
    print("=" * 60)
    print("Concurreny Race & Double-Claim Test (3 Worker Replicas)")
    print("=" * 60)

    # 1. Register/Login
    print("\n1. Registering test user...")
    status, data = api("POST", "/auth/register", {
        "email": "race-test@example.com",
        "password": "securepass123",
        "org_name": f"Race Org {int(time.time())}"
    })
    if status == 409:
        status, data = api("POST", "/auth/login", {
            "email": "race-test@example.com",
            "password": "securepass123"
        })
    assert status in (200, 201)
    token = data["access_token"]

    # 2. Get/Create Project
    status, data = api("POST", "/projects", {"name": f"Race Project {int(time.time())}"}, token=token)
    assert status == 201
    project_id = data["id"]

    # 3. Create Queue with high concurrency limit (e.g., 20)
    status, data = api("POST", f"/projects/{project_id}/queues", {
        "name": f"race-queue-{int(time.time())}",
        "priority": 100,
        "concurrency_limit": 20
    }, token=token)
    assert status == 201
    queue_id = data["id"]

    # 4. Submit 60 immediate jobs (0.5s duration)
    print("\n2. Submitting 60 immediate jobs...")
    jobs_payload = {
        "jobs": [
            {"job_type": "immediate", "payload": {"duration": 0.5, "task": f"race_job_{i}"}}
            for i in range(60)
        ]
    }
    status, data = api("POST", f"/queues/{queue_id}/batches", jobs_payload, token=token)
    assert status == 201
    job_ids = [j["id"] for j in data]
    print(f"   Submitted {len(job_ids)} jobs.")

    # 5. Wait for all 60 jobs to be completed
    print("\n3. Waiting for jobs to complete...")
    all_completed = False
    for attempt in range(35):
        time.sleep(2)
        completed_count = 0
        for jid in job_ids:
            status, job_data = api("GET", f"/jobs/{jid}", token=token)
            if job_data["status"] == "completed":
                completed_count += 1
        print(f"   Completed jobs: {completed_count}/60")
        if completed_count == 60:
            all_completed = True
            break

    assert all_completed, "Jobs did not finish in time"
    print("   All 60 jobs successfully completed.")

    # 6. Query DB directly to check job execution records
    print("\n4. Directly querying database to inspect execution records...")
    async with AsyncSessionLocal() as session:
        # Get execution count grouped by job_id for the submitted jobs
        result = await session.execute(
            select(
                JobExecution.job_id,
                func.count(JobExecution.id).label("exec_count"),
                func.array_agg(JobExecution.worker_id).label("worker_ids")
            )
            .where(JobExecution.job_id.in_(job_ids))
            .group_by(JobExecution.job_id)
        )
        rows = result.all()

    # Analyze execution distribution
    total_execs = 0
    worker_set = set()
    multi_claims = []
    
    for row in rows:
        job_id, exec_count, worker_ids = row
        total_execs += exec_count
        for wid in worker_ids:
            if wid:
                worker_set.add(wid)
        if exec_count > 1:
            multi_claims.append((job_id, exec_count, worker_ids))

    print(f"   Total executed jobs: {len(rows)}")
    print(f"   Total execution records found: {total_execs}")
    print(f"   Distinct workers that claimed jobs: {len(worker_set)} {list(worker_set)}")
    
    # Assertions for zero double-claims and multiple worker cooperation
    assert len(multi_claims) == 0, f"Error! Found double-claimed jobs: {multi_claims}"
    assert total_execs == 60, f"Error! Expected 60 execution rows, found {total_execs}"
    assert len(worker_set) > 1, f"Error! Expected multiple worker processes to claim jobs, but only saw {len(worker_set)} worker(s)."
    
    print("\n" + "=" * 60)
    print("CONCURRENCY RACES TEST PASSED SUCCESSFULLY!")
    print("Exactly-once claiming verified across multiple separate worker replicas!")
    print("=" * 60)

    # Clean up
    api("DELETE", f"/projects/{project_id}", token=token)

if __name__ == "__main__":
    asyncio.run(run_race_test())
