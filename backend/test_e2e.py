"""End-to-end smoke test for Phase 1 API."""
import urllib.request
import json
import sys

BASE = "http://localhost:8000/api/v1"

import asyncio

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

def api_with_headers(method, path, body=None, token=None):
    url = f"{BASE}{path}"
    data = json.dumps(body).encode() if body else None
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        resp = urllib.request.urlopen(req)
        return resp.status, json.loads(resp.read().decode()) if resp.status != 204 else None, resp.headers
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode()), e.headers

import time

print("=" * 60)
print("Phase 1 End-to-End Smoke Test")
print("=" * 60)

test_email = f"test-{int(time.time())}@example.com"
test_org = f"Test Org {int(time.time())}"

# 1. Register
print("\n1. Register user...")
status, data = api("POST", "/auth/register", {
    "email": test_email,
    "password": "securepass123",
    "org_name": test_org
})
print(f"   Status: {status}")
assert status == 201, f"Expected 201, got {status}: {data}"
access_token = data["access_token"]
refresh_token = data["refresh_token"]
print(f"   Got access_token: {access_token[:20]}...")
print(f"   Got refresh_token: {refresh_token[:20]}...")

# 2. Duplicate registration
print("\n2. Duplicate registration (should fail)...")
status, data = api("POST", "/auth/register", {
    "email": test_email,
    "password": "securepass123",
    "org_name": f"{test_org} 2"
})
print(f"   Status: {status} (expected 409)")
assert status == 409

# 3. Login
print("\n3. Login...")
status, data = api("POST", "/auth/login", {
    "email": test_email,
    "password": "securepass123"
})
print(f"   Status: {status}")
assert status == 200
access_token = data["access_token"]
print(f"   Fresh access_token: {access_token[:20]}...")

# 4. Token refresh
print("\n4. Token refresh...")
status, data = api("POST", "/auth/refresh", {"refresh_token": refresh_token})
print(f"   Status: {status}")
assert status == 200
access_token = data["access_token"]

# 5. List orgs
print("\n5. List organizations...")
status, data = api("GET", "/organizations", token=access_token)
print(f"   Status: {status}, count: {len(data)}")
assert status == 200
matched_orgs = [org for org in data if org["name"] == test_org]
assert len(matched_orgs) == 1, f"Expected to find exactly one organization named '{test_org}'"
org_id = matched_orgs[0]["id"]
print(f"   Org: {matched_orgs[0]['name']} (id: {org_id})")

# 6. Create project
print("\n6. Create project...")
status, data = api("POST", "/projects", {"name": f"My First Project {int(time.time())}"}, token=access_token)
print(f"   Status: {status}")
assert status == 201
project_id = data["id"]
print(f"   Project: {data['name']} (id: {project_id})")
assert data["org_id"] == org_id, f"Assertion Failed: project org_id {data['org_id']} does not match org_id {org_id}"
print(f"   FK org_id: {data['org_id']} == {org_id} [VERIFIED]")

# 7. List projects
print("\n7. List projects (paginated)...")
status, data = api("GET", "/projects?page=1&page_size=10", token=access_token)
print(f"   Status: {status}, total: {data['total']}, items: {len(data['items'])}")
assert status == 200

# 8. Create queue
print("\n8. Create queue...")
status, data = api("POST", f"/projects/{project_id}/queues", {
    "name": "email-queue",
    "priority": 10,
    "concurrency_limit": 3
}, token=access_token)
print(f"   Status: {status}")
assert status == 201
queue_id = data["id"]
print(f"   Queue: {data['name']} (priority={data['priority']}, concurrency={data['concurrency_limit']})")
print(f"   FK project_id: {data['project_id']} == {project_id}: {data['project_id'] == project_id}")

# 9. List queues
print("\n9. List queues (paginated)...")
status, data = api("GET", f"/projects/{project_id}/queues?page=1&page_size=10", token=access_token)
print(f"   Status: {status}, total: {data['total']}, items: {len(data['items'])}")
assert status == 200

# 10. Get queue by ID
print("\n10. Get queue by ID...")
status, data = api("GET", f"/queues/{queue_id}", token=access_token)
print(f"   Status: {status}, name: {data['name']}")
assert status == 200

# 11. Update queue (pause it)
print("\n11. Update queue (pause)...")
status, data = api("PUT", f"/queues/{queue_id}", {"paused": True}, token=access_token)
print(f"   Status: {status}, paused: {data['paused']}")
assert status == 200 and data["paused"] == True

# --- Phase 2: Job Submission & Explorer Verification ---

# 12. Submit Immediate Job
print("\n12. Submit immediate job...")
status, data = api("POST", f"/queues/{queue_id}/jobs", {
    "job_type": "immediate",
    "payload": {"task": "send_welcome_email", "to": "user@example.com"},
    "idempotency_key": "idemp-immediate-1"
}, token=access_token)
print(f"   Status: {status}, job_status: {data['status']}, job_type: {data['job_type']}")
assert status == 201
assert data["status"] == "queued"
assert data["job_type"] == "immediate"
immediate_job_id = data["id"]

# 13. Submit Delayed Job
print("\n13. Submit delayed job (60s delay)...")
status, data = api("POST", f"/queues/{queue_id}/jobs", {
    "job_type": "delayed",
    "delay_seconds": 60,
    "payload": {"task": "delayed_cleanup"}
}, token=access_token)
print(f"   Status: {status}, job_status: {data['status']}, run_at: {data['run_at']}")
assert status == 201
assert data["status"] == "scheduled"
assert data["run_at"] is not None

# 14. Submit Scheduled Job (Future timestamp)
import datetime
future_time = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2)).isoformat()
print(f"\n14. Submit scheduled job for future time: {future_time}...")
status, data = api("POST", f"/queues/{queue_id}/jobs", {
    "job_type": "scheduled",
    "run_at": future_time,
    "payload": {"task": "future_report"}
}, token=access_token)
print(f"   Status: {status}, job_status: {data['status']}, run_at: {data['run_at']}")
assert status == 201
assert data["status"] == "scheduled"

# 15. Submit Recurring (Cron) Job
print("\n15. Submit recurring cron job (every 5 minutes)...")
status, data = api("POST", f"/queues/{queue_id}/jobs", {
    "job_type": "recurring",
    "cron_expr": "*/5 * * * *",
    "payload": {"task": "sync_analytics"}
}, token=access_token)
print(f"   Status: {status}, job_status: {data['status']}, cron_expr: {data['scheduled_job']['cron_expr']}, next_run_at: {data['scheduled_job']['next_run_at']}")
assert status == 201
assert data["status"] == "scheduled"
assert data["scheduled_job"]["cron_expr"] == "*/5 * * * *"
assert data["scheduled_job"]["next_run_at"] is not None

# 16. Verify Idempotency Key logic (Duplicate submission)
print("\n16. Re-submit immediate job with duplicate idempotency key...")
status, data_replay = api("POST", f"/queues/{queue_id}/jobs", {
    "job_type": "immediate",
    "payload": {"task": "send_welcome_email", "to": "user@example.com"},
    "idempotency_key": "idemp-immediate-1"
}, token=access_token)
# We can't inspect response headers directly via api() helper unless we catch it,
# but we can verify the status code is 200 (indicating duplicate reuse rather than 201)
# and the returned job ID matches the first one.
print(f"   Status: {status} (expected 200), job_id: {data_replay['id']} == {immediate_job_id}")
assert status == 200
assert data_replay["id"] == immediate_job_id

# 16b. Concurrent Race Verification
print("\n16b. Fire two concurrent requests with same idempotency key simultaneously...")
async def run_concurrent_requests():
    async def make_req():
        return await asyncio.to_thread(
            api_with_headers,
            "POST",
            f"/queues/{queue_id}/jobs",
            {
                "job_type": "immediate",
                "payload": {"task": "concurrent_test"},
                "idempotency_key": "idemp-concurrent-race-1"
            },
            access_token
        )
    return await asyncio.gather(make_req(), make_req())

results = asyncio.run(run_concurrent_requests())
statuses = [r[0] for r in results]
replays = [r[2].get("X-Idempotent-Replay") if r[2] else None for r in results]

print(f"   Statuses: {statuses} (expected one 201 and one 200)")
print(f"   X-Idempotent-Replay headers: {replays}")

# Exactly one response must be 201 (Created), and the other must be 200 (Replay)
assert sorted(statuses) == [200, 201], f"Expected one 201 and one 200, got: {statuses}"
# The replay response (status 200) must have the header 'true'
replay_index = statuses.index(200)
assert replays[replay_index] == "true", f"Expected X-Idempotent-Replay: true on the 200 response, got: {replays[replay_index]}"
print("   Concurrent idempotency race test passed successfully!")


# 17. Submit Batch of Jobs
print("\n17. Submit batch of 3 jobs...")
status, data = api("POST", f"/queues/{queue_id}/batches", {
    "jobs": [
        {"job_type": "immediate", "payload": {"item": 1}},
        {"job_type": "immediate", "payload": {"item": 2}},
        {"job_type": "delayed", "delay_seconds": 30, "payload": {"item": 3}}
    ]
}, token=access_token)
print(f"   Status: {status}, created jobs count: {len(data)}")
assert status == 201
assert len(data) == 3
batch_job_1_id = data[0]["id"]
assert data[0]["job_type"] == "batch"  # Enforced db type
assert data[0]["payload"]["_batch_id"] is not None

# 18. List Jobs (Paginated + Filtered)
print("\n18. List jobs in queue (filter by job_type=immediate)...")
status, data = api("GET", f"/queues/{queue_id}/jobs?job_type=immediate&page=1&page_size=10", token=access_token)
print(f"   Status: {status}, total immediate jobs: {data['total']}, count returned: {len(data['items'])}")
assert status == 200
# Should contain our immediate_job_id
job_ids = [j["id"] for j in data["items"]]
assert immediate_job_id in job_ids

print("\n19. List jobs in queue (filter by status=scheduled)...")
status, data = api("GET", f"/queues/{queue_id}/jobs?status=scheduled&page=1&page_size=10", token=access_token)
print(f"   Status: {status}, total scheduled jobs: {data['total']}, count returned: {len(data['items'])}")
assert status == 200

# 20. Get Job by ID
print("\n20. Get job details by ID...")
status, data = api("GET", f"/jobs/{immediate_job_id}", token=access_token)
print(f"   Status: {status}, job_id: {data['id']}, status: {data['status']}")
assert status == 200
assert data["id"] == immediate_job_id

# 21. Unauthenticated request
print("\n21. Unauthenticated request (should fail)...")
status, data = api("GET", "/projects")
print(f"   Status: {status} (expected 401)")
assert status == 401

# 22. Cascade delete project
print("\n22. Delete project (should trigger cascade delete on queues and jobs)...")
status, data = api("DELETE", f"/projects/{project_id}", token=access_token)
print(f"   Status: {status} (expected 204)")
assert status == 204

# 23. Verify queue is deleted automatically via cascade
print("\n23. Verify queue is deleted automatically (should return 404)...")
status, data = api("GET", f"/queues/{queue_id}", token=access_token)
print(f"   Status: {status} (expected 404)")
assert status == 404

print("\n" + "=" * 60)
print("ALL 23 CHECKS PASSED (PHASE 2 E2E COMPLETE)")
print("=" * 60)

# Teardown / Cleanup test data
async def cleanup_db_data():
    print("\nExecuting database teardown to clean up user and organization test data...")
    try:
        from app.database import AsyncSessionLocal
        from app.models.user import User
        from app.models.organization import Organization
        from sqlalchemy import delete
        
        async with AsyncSessionLocal() as session:
            async with session.begin():
                await session.execute(delete(User).where(User.email == test_email))
                await session.execute(delete(Organization).where(Organization.name == test_org))
        print("   [OK] Organization and user records cleaned successfully.")
    except Exception as e:
        print(f"   [Error] Teardown database cleanup failed: {e}")

import asyncio
asyncio.run(cleanup_db_data())
