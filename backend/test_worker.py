import urllib.request
import json
import sys
import time
import datetime

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

def main():
    print("=" * 60)
    print("Phase 3 Worker Engine E2E Tests")
    print("=" * 60)

    # 1. Register & Auth
    print("\n1. Register and login test user...")
    status, data = api("POST", "/auth/register", {
        "email": "worker-test@example.com",
        "password": "securepass123",
        "org_name": "Worker Test Org"
    })
    if status == 409:
        status, data = api("POST", "/auth/login", {
            "email": "worker-test@example.com",
            "password": "securepass123"
        })
    assert status in (200, 201), f"Auth failed with status {status}: {data}"
    token = data["access_token"]

    # 2. Get/Create Project
    print("\n2. Get/Create project...")
    status, data = api("GET", "/projects", token=token)
    if status == 200 and len(data["items"]) > 0:
        project_id = data["items"][0]["id"]
    else:
        status, data = api("POST", "/projects", {"name": "Worker Test Project"}, token=token)
        assert status == 201
        project_id = data["id"]

    # 3. Create a queue with Concurrency Limit = 2
    print("\n3. Creating queue with concurrency limit 2...")
    queue_name = f"concurrency-queue-{int(time.time())}"
    status, data = api("POST", f"/projects/{project_id}/queues", {
        "name": queue_name,
        "priority": 50,
        "concurrency_limit": 2
    }, token=token)
    assert status == 201, f"Failed to create queue: {data}"
    queue_id = data["id"]

    # 4. Submit 5 slow jobs (duration = 2 seconds)
    print("\n4. Submitting 5 slow jobs (concurrency limit is 2)...")
    job_ids = []
    for i in range(5):
        status, job_data = api("POST", f"/queues/{queue_id}/jobs", {
            "job_type": "immediate",
            "payload": {"task": "slow_job", "duration": 2, "index": i}
        }, token=token)
        assert status == 201
        job_ids.append(job_data["id"])

    print(f"Submitted jobs: {job_ids}")
    
    # Wait 0.5s to let worker poll and claim jobs
    time.sleep(1.0)
    
    # Fetch job statuses
    running_count = 0
    queued_count = 0
    for jid in job_ids:
        status, job_data = api("GET", f"/jobs/{jid}", token=token)
        assert status == 200
        print(f"Job {jid}: {job_data['status']}")
        if job_data["status"] == "running":
            running_count += 1
        elif job_data["status"] == "queued":
            queued_count += 1

    print(f"Active running jobs: {running_count}, queued jobs: {queued_count}")
    # Verify that concurrency limit of 2 is enforced
    assert running_count <= 2, f"Concurrency limit exceeded! Found {running_count} running jobs (limit: 2)"
    
    # Wait for all 5 jobs to complete
    print("\nWaiting for all 5 jobs to complete...")
    all_completed = False
    for attempt in range(10):
        time.sleep(2)
        completed_count = 0
        for jid in job_ids:
            status, job_data = api("GET", f"/jobs/{jid}", token=token)
            if job_data["status"] == "completed":
                completed_count += 1
        print(f"Completed jobs: {completed_count}/5")
        if completed_count == 5:
            all_completed = True
            break
            
    assert all_completed, "Not all jobs completed in time!"
    print("Concurrency limit verification successful!")

    # 5. Submit failing job with retry policy
    print("\n5. Submitting failing job (should retry up to 2 times, then fail to DLQ)...")
    status, job_data = api("POST", f"/queues/{queue_id}/jobs", {
        "job_type": "immediate",
        "max_retries": 2,
        "payload": {"task": "fail", "error_message": "Bad error"}
    }, token=token)
    assert status == 201
    fail_job_id = job_data["id"]
    
    # Wait for execution and retries (there's no custom retry policy on queue, so defaults to 1s fixed delay)
    print("Waiting for retries to complete...")
    for _ in range(8):
        time.sleep(1.5)
        status, job_data = api("GET", f"/jobs/{fail_job_id}", token=token)
        print(f"Failing Job status: {job_data['status']}, retries: {job_data['retry_count']}")
        if job_data["status"] == "dead_letter":
            break
            
    assert job_data["status"] == "dead_letter", f"Expected dead_letter status, got {job_data['status']}"
    assert job_data["retry_count"] == 3, f"Expected 3 retry attempts (original + 2 retries), got {job_data['retry_count']}"
    print("Retries and Dead Letter Queue verification successful!")

    # 6. Verify Recurring Job re-scheduling
    print("\n6. Submitting a recurring job (run every 1 min)...")
    status, job_data = api("POST", f"/queues/{queue_id}/jobs", {
        "job_type": "recurring",
        "cron_expr": "*/1 * * * *",
        "payload": {"task": "cron_job"}
    }, token=token)
    assert status == 201
    cron_job_id = job_data["id"]
    
    # Manually check next run time is set
    assert job_data["status"] == "scheduled"
    assert job_data["run_at"] is not None
    
    # Let's clean up
    print("\nCleaning up project...")
    status, _ = api("DELETE", f"/projects/{project_id}", token=token)
    assert status == 204
    
    print("\n" + "=" * 60)
    print("ALL WORKER E2E CHECKS PASSED (PHASE 3 VERIFIED)")
    print("=" * 60)

if __name__ == "__main__":
    main()
