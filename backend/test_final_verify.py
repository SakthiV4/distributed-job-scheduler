"""
Final System Verification Script
Checks: health, OpenAPI docs, seeded data, system summary, worker fleet, DLQ entries.
"""
import urllib.request
import urllib.error
import json
import sys

BASE = "http://localhost:8000"

def api(method, path, body=None, token=None):
    url = f"{BASE}{path}"
    data = json.dumps(body).encode() if body else None
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        resp = urllib.request.urlopen(req)
        return resp.status, json.loads(resp.read().decode()) if resp.status != 204 else {}
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())

PASS = "[PASS]"
FAIL = "[FAIL]"
results = []

def check(label, condition, detail=""):
    status = PASS if condition else FAIL
    msg = f"  {status} {label}"
    if detail:
        msg += f" — {detail}"
    print(msg)
    results.append((label, condition))

print("=" * 70)
print("FINAL SYSTEM VERIFICATION")
print("=" * 70)

# ── STEP 1: /health ──────────────────────────────────────────────────────────
print("\n[Step 1] Health endpoint")
s, d = api("GET", "/health")
check("/health returns 200", s == 200)
check("/health status=healthy", d.get("status") == "healthy", f"got {d}")

# ── STEP 2: OpenAPI / Swagger ─────────────────────────────────────────────────
print("\n[Step 2] OpenAPI spec (/openapi.json)")
s, d = api("GET", "/openapi.json")
check("/openapi.json returns 200", s == 200)
paths = sorted(d.get("paths", {}).keys())
expected_prefixes = ["/api/v1/auth", "/api/v1/projects", "/api/v1/queues",
                     "/api/v1/jobs", "/api/v1/dlq", "/api/v1/system"]
for prefix in expected_prefixes:
    has = any(p.startswith(prefix) for p in paths)
    check(f"Route group '{prefix}' present", has)
total = len(paths)
check(f"Total routes documented ({total})", total >= 15, f"found {total}")
print(f"  All routes: {paths}")

# ── STEP 3: Login as demo admin ───────────────────────────────────────────────
print("\n[Step 3] Demo admin login")
s, d = api("POST", "/api/v1/auth/login", {
    "email": "admin@scheduler.xyz",
    "password": "AdminPassword123!"
})
check("Login returns 200", s == 200, f"got {s}")
token = d.get("access_token", "")
check("Got access token", bool(token))

# ── STEP 4: Seeded project exists ─────────────────────────────────────────────
print("\n[Step 4] Seeded project 'Video Processing Cluster'")
s, d = api("GET", "/api/v1/projects", token=token)
check("GET /projects returns 200", s == 200)
items = d.get("items", [])
vp = next((p for p in items if p["name"] == "Video Processing Cluster"), None)
check("'Video Processing Cluster' project seeded", vp is not None)
if vp:
    project_id = vp["id"]

    # ── STEP 5: Seeded queues ─────────────────────────────────────────────────
    print("\n[Step 5] Seeded queues")
    s, d = api("GET", f"/api/v1/projects/{project_id}/queues", token=token)
    check("GET /queues returns 200", s == 200)
    qnames = [q["name"] for q in d.get("items", [])]
    check("'default-queue' seeded", "default-queue" in qnames, f"found: {qnames}")
    check("'critical-queue' seeded", "critical-queue" in qnames, f"found: {qnames}")

    # ── STEP 6: System summary ────────────────────────────────────────────────
    print("\n[Step 6] System summary")
    s, d = api("GET", f"/api/v1/system/summary?project_id={project_id}", token=token)
    check("GET /system/summary returns 200", s == 200, f"got {s}")
    jc = d.get("job_counts", {})
    aw = d.get("active_workers_count", -1)
    check("job_counts key present", "queued" in jc or "completed" in jc, f"counts: {jc}")
    check("active_workers_count >= 1", aw >= 1, f"got {aw}")
    print(f"  Job counts: {jc}")
    print(f"  Active workers: {aw}")

    # ── STEP 7: DLQ entries ───────────────────────────────────────────────────
    print("\n[Step 7] DLQ entries in seeded default-queue")
    default_q = next((q for q in d.get("job_counts", {}) and [] or []), None)
    # Re-fetch queues for queue_id
    s2, d2 = api("GET", f"/api/v1/projects/{project_id}/queues", token=token)
    dq = next((q for q in d2.get("items", []) if q["name"] == "default-queue"), None)
    if dq:
        queue_id = dq["id"]
        s3, d3 = api("GET", f"/api/v1/queues/{queue_id}/dlq", token=token)
        check("GET /queues/{id}/dlq returns 200", s3 == 200, f"got {s3}")
        dlq_items = d3.get("items", [])
        check("At least 1 DLQ entry seeded", len(dlq_items) >= 1, f"found {len(dlq_items)}")
        if dlq_items:
            fr = dlq_items[0].get("failure_reason", "")
            check("DLQ entry has failure_reason text", bool(fr), f"reason: '{fr[:80]}'")

# ── STEP 8: Worker fleet ──────────────────────────────────────────────────────
print("\n[Step 8] Worker fleet monitor")
s, d = api("GET", "/api/v1/system/workers", token=token)
check("GET /system/workers returns 200", s == 200, f"got {s}")
if s == 200:
    check("At least 1 active worker", len(d) >= 1, f"fleet size: {len(d)}")
    if d:
        w = d[0]
        check("Worker has hostname", bool(w.get("hostname")))
        check("Worker status=online", w.get("status") == "online", f"got {w.get('status')}")
        check("Worker has last_seen timestamp", bool(w.get("last_seen")))
        print(f"  Worker: {w}")

# ── STEP 9: Swagger UI HTML ───────────────────────────────────────────────────
print("\n[Step 9] Swagger UI page (/docs)")
try:
    resp = urllib.request.urlopen(f"{BASE}/docs")
    html = resp.read().decode()
    check("/docs returns 200", resp.status == 200)
    check("/docs contains swagger-ui", "swagger" in html.lower() or "openapi" in html.lower())
except Exception as e:
    check("/docs accessible", False, str(e))

# ── SUMMARY ───────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
total_checks = len(results)
passed = sum(1 for _, ok in results if ok)
failed = total_checks - passed
print(f"FINAL RESULT: {passed}/{total_checks} checks passed, {failed} failed")
if failed:
    print("\nFailed checks:")
    for label, ok in results:
        if not ok:
            print(f"  {FAIL} {label}")
print("=" * 70)
sys.exit(0 if failed == 0 else 1)
