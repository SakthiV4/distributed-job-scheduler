import asyncio
import urllib.request
import json
import sys
import time
import datetime
import threading

BASE = "http://localhost:8000/api/v1"
OPENAPI_URL = "http://localhost:8000/openapi.json"

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

def run_concurrent_requests(func, count=2):
    results = [None] * count
    threads = []
    
    def worker(index):
        results[index] = func()
        
    for i in range(count):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        
    for t in threads:
        t.start()
    for t in threads:
        t.join()
        
    return results

def main():
    print("=" * 60)
    print("Phase 5 API Consistency & OpenAPI E2E Validation")
    print("=" * 60)

    # 1. OpenAPI validation
    print("\n1. Fetching and validating OpenAPI specs...")
    status, schema = api("GET", OPENAPI_URL)
    assert status == 200, f"Failed to fetch OpenAPI specs: {status}"
    
    schemas = schema["components"]["schemas"]
    required_paginated_schemas = [
        "PaginatedProjectsResponse",
        "PaginatedQueuesResponse",
        "PaginatedJobsResponse",
        "PaginatedDLQResponse"
    ]
    
    for ps in required_paginated_schemas:
        assert ps in schemas, f"OpenAPI components/schemas is missing definition for: {ps}"
        print(f"    [OK] Found schema definition: {ps}")
    print("   [OK] OpenAPI specs validated successfully.")

    # 2. Concurrent Registration Race (IntegrityError catching)
    print("\n2. Testing concurrent registration race condition...")
    shared_email = f"race-user-{int(time.time())}@example.com"
    shared_org = f"Race Org {int(time.time())}"
    
    def register_call():
        return api("POST", "/auth/register", {
            "email": shared_email,
            "password": "securepass123",
            "org_name": shared_org
        })
        
    results = run_concurrent_requests(register_call, 2)
    statuses = [r[0] for r in results]
    print(f"    Registration statuses returned: {statuses}")
    assert 201 in statuses, "Expected at least one registration to succeed"
    assert 409 in statuses, "Expected collision to return 409 conflict"
    
    # Verify the error payload shape matches the standard FastAPI {"detail": "..."} shape
    collision_payload = [r[1] for r in results if r[0] == 409][0]
    print(f"    Collision Payload: {collision_payload}")
    assert "detail" in collision_payload
    assert collision_payload["detail"] in ("Email already registered", "Organization name already taken")
    print("   [OK] Registration race IntegrityError handler verified.")

    # Find the successful token for subsequent steps
    token = [r[1]["access_token"] for r in results if r[0] == 201][0]

    # 3. Concurrent Project Name Race (uq_project_org_name catching)
    print("\n3. Testing concurrent project creation race condition...")
    shared_project_name = f"Race Project {int(time.time())}"
    
    def create_project_call():
        return api("POST", "/projects", {
            "name": shared_project_name
        }, token=token)
        
    results = run_concurrent_requests(create_project_call, 2)
    statuses = [r[0] for r in results]
    print(f"    Project creation statuses: {statuses}")
    assert 201 in statuses, "Expected at least one project to be created successfully"
    assert 409 in statuses, "Expected collision to return 409 conflict"
    
    collision_payload = [r[1] for r in results if r[0] == 409][0]
    print(f"    Project Collision Payload: {collision_payload}")
    assert "detail" in collision_payload
    assert "already exists in your organization" in collision_payload["detail"]
    print("   [OK] Project creation race IntegrityError handler verified.")

    # Find the successful project ID
    created_project = [r[1] for r in results if r[0] == 201][0]
    project_id = created_project["id"]

    # 4. Testing invalid foreign key constraint handling on queue creation (queues.py)
    print("\n4. Testing invalid foreign key constraint handling on queue creation...")
    invalid_policy_uuid = "e0000000-0000-0000-0000-000000000000"
    status, queue_res = api("POST", f"/projects/{project_id}/queues", {
        "name": "invalid-fk-queue",
        "priority": 10,
        "concurrency_limit": 5,
        "retry_policy_id": invalid_policy_uuid
    }, token=token)
    print(f"    Queue creation with invalid policy ID status: {status}")
    print(f"    Queue error payload: {queue_res}")
    assert status == 409
    assert "detail" in queue_res
    assert "Database integrity violation or invalid retry policy" in queue_res["detail"]
    print("   [OK] Queue creation invalid FK IntegrityError handler verified.")

    print("\n" + "=" * 60)
    print("ALL PHASE 5 API CONSISTENCY & OPENAPI TESTS PASSED!")
    print("=" * 60)

if __name__ == "__main__":
    main()
