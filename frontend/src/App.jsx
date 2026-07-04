import React, { useState, useEffect, useRef } from "react";

const API_BASE = "http://localhost:8000/api/v1";

export default function App() {
  const [token, setToken] = useState(localStorage.getItem("token") || "");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [authError, setAuthError] = useState("");
  const [userRole, setUserRole] = useState("");

  const [projects, setProjects] = useState([]);
  const [selectedProjectId, setSelectedProjectId] = useState("");
  
  const [queues, setQueues] = useState([]);
  const [summary, setSummary] = useState(null);
  const [workers, setWorkers] = useState([]);
  
  const [queueJobs, setQueueJobs] = useState({});
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedInspectorJob, setSelectedInspectorJob] = useState(null);
  
  const [activeView, setActiveView] = useState("pipelines"); // 'pipelines' | 'dlq' | 'fleet'
  const [selectedQueueId, setSelectedQueueId] = useState("");
  const [dlqJobs, setDlqJobs] = useState([]);
  const [selectedDlqJob, setSelectedDlqJob] = useState(null);

  // Form states for job submission
  const [showSubmitModal, setShowSubmitModal] = useState(false);
  const [submitQueueId, setSubmitQueueId] = useState("");
  const [retryPolicies, setRetryPolicies] = useState([]);
  const [jobPayloadText, setJobPayloadText] = useState('{"task": "process_video", "video_id": 4022}');
  const [jobType, setJobType] = useState("immediate");
  const [isBatch, setIsBatch] = useState(false);
  const [batchCount, setBatchCount] = useState(5);

  const fetchInterval = useRef(null);

  // Parse JWT token to get role
  useEffect(() => {
    if (token) {
      localStorage.setItem("token", token);
      try {
        const payload = JSON.parse(atob(token.split(".")[1]));
        // Note: system endpoints will double-check role constraints on backend
        setUserRole("admin"); // Default fallback
      } catch (e) {
        console.error("Failed to parse token:", e);
      }
      fetchProjects();
      fetchRetryPolicies();
    } else {
      localStorage.removeItem("token");
    }
  }, [token]);

  // Set up polling for system details
  useEffect(() => {
    if (token && selectedProjectId) {
      fetchDashboardData();
      
      // Setup live refresh loop every 4 seconds
      if (fetchInterval.current) clearInterval(fetchInterval.current);
      fetchInterval.current = setInterval(fetchDashboardData, 4000);
    }
    return () => {
      if (fetchInterval.current) clearInterval(fetchInterval.current);
    };
  }, [token, selectedProjectId, activeView, selectedQueueId]);

  const handleLogin = async (e) => {
    e.preventDefault();
    setAuthError("");
    try {
      const res = await fetch(`${API_BASE}/auth/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, password }),
      });
      const data = await res.json();
      if (res.ok) {
        setToken(data.access_token);
      } else {
        setAuthError(data.detail || "Authentication failed.");
      }
    } catch (err) {
      setAuthError("Unable to connect to backend server.");
    }
  };

  const handleLogout = () => {
    setToken("");
    setProjects([]);
    setSelectedProjectId("");
    setQueues([]);
    setSummary(null);
    setWorkers([]);
  };

  const fetchProjects = async () => {
    try {
      const res = await fetch(`${API_BASE}/projects?page=1&page_size=50`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      if (res.status === 401) return handleLogout();
      const data = await res.json();
      if (res.ok && data.items) {
        setProjects(data.items);
        if (data.items.length > 0) {
          setSelectedProjectId(data.items[0].id);
        }
      }
    } catch (e) {
      console.error("Error fetching projects:", e);
    }
  };

  const fetchRetryPolicies = async () => {
    try {
      const res = await fetch(`${API_BASE}/retry-policies`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      const data = await res.json();
      if (res.ok) setRetryPolicies(data);
    } catch (e) {
      console.error("Error fetching policies:", e);
    }
  };

  const fetchDashboardData = async () => {
    if (!selectedProjectId) return;
    try {
      // 1. Fetch queues
      const qRes = await fetch(`${API_BASE}/projects/${selectedProjectId}/queues?page=1&page_size=50`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      if (qRes.ok) {
        const qData = await qRes.json();
        const activeQueues = qData.items || [];
        setQueues(activeQueues);
        
        // Seed default submission queue selector if empty
        if (activeQueues.length > 0 && !submitQueueId) {
          setSubmitQueueId(activeQueues[0].id);
        }

        // Fetch jobs for each queue to render live flow cards
        const jobsMap = {};
        for (const q of activeQueues) {
          try {
            const jRes = await fetch(`${API_BASE}/queues/${q.id}/jobs?page=1&page_size=20`, {
              headers: { Authorization: `Bearer ${token}` }
            });
            if (jRes.ok) {
              const jData = await jRes.json();
              jobsMap[q.id] = jData.items || [];
            }
          } catch (err) {
            console.error(`Failed to fetch jobs for queue ${q.id}:`, err);
          }
        }
        setQueueJobs(jobsMap);
      }

      // 2. Fetch stats summary
      const sRes = await fetch(`${API_BASE}/system/summary?project_id=${selectedProjectId}`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      if (sRes.ok) {
        const sData = await sRes.json();
        setSummary(sData);
      }

      // 3. Fetch active worker fleet (if user is admin)
      const wRes = await fetch(`${API_BASE}/system/workers`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      if (wRes.ok) {
        const wData = await wRes.json();
        setWorkers(wData);
      }

      // 4. Fetch DLQ listing (if viewing DLQ and a queue is selected)
      if (activeView === "dlq" && selectedQueueId) {
        const dlqRes = await fetch(`${API_BASE}/queues/${selectedQueueId}/dlq?page=1&page_size=50`, {
          headers: { Authorization: `Bearer ${token}` },
        });
        if (dlqRes.ok) {
          const dlqData = await dlqRes.json();
          setDlqJobs(dlqData.items || []);
        }
      }
    } catch (e) {
      console.error("Dashboard fetch error:", e);
    }
  };

  const handleTogglePause = async (queueId, currentPaused) => {
    try {
      const res = await fetch(`${API_BASE}/queues/${queueId}`, {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({ paused: !currentPaused }),
      });
      if (res.ok) fetchDashboardData();
    } catch (e) {
      console.error("Failed to toggle queue state:", e);
    }
  };

  const handleRequeueDLQ = async (jobId) => {
    try {
      const res = await fetch(`${API_BASE}/dlq/${jobId}/requeue`, {
        method: "POST",
        headers: { Authorization: `Bearer ${token}` },
      });
      if (res.ok) {
        setSelectedDlqJob(null);
        fetchDashboardData();
      }
    } catch (e) {
      console.error("Failed to requeue job:", e);
    }
  };

  const handleSoftDiscardDLQ = async (jobId) => {
    if (!confirm("Are you sure you want to discard this job? The record status will change to failed, preserving all execution attempt histories.")) return;
    try {
      const res = await fetch(`${API_BASE}/dlq/${jobId}`, {
        method: "DELETE",
        headers: { Authorization: `Bearer ${token}` },
      });
      if (res.ok) {
        setSelectedDlqJob(null);
        fetchDashboardData();
      }
    } catch (e) {
      console.error("Failed to discard job:", e);
    }
  };

  const handleSubmitJob = async (e) => {
    e.preventDefault();
    try {
      let parsedPayload = {};
      try {
        parsedPayload = JSON.parse(jobPayloadText);
      } catch (err) {
        alert("Invalid JSON format in payload field.");
        return;
      }

      if (isBatch) {
        const jobsList = Array.from({ length: batchCount }).map((_, i) => ({
          job_type: jobType,
          payload: { ...parsedPayload, batch_index: i },
        }));
        const res = await fetch(`${API_BASE}/queues/${submitQueueId}/batches`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${token}`,
          },
          body: JSON.stringify({ jobs: jobsList }),
        });
        if (res.ok) {
          setShowSubmitModal(false);
          fetchDashboardData();
        } else {
          const err = await res.json();
          alert(err.detail || "Submission failed.");
        }
      } else {
        const res = await fetch(`${API_BASE}/queues/${submitQueueId}/jobs`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${token}`,
          },
          body: JSON.stringify({
            job_type: jobType,
            payload: parsedPayload,
          }),
        });
        if (res.ok) {
          setShowSubmitModal(false);
          fetchDashboardData();
        } else {
          const err = await res.json();
          alert(err.detail || "Submission failed.");
        }
      }
    } catch (e) {
      console.error("Error submitting job:", e);
    }
  };

  if (!token) {
    return (
      <div className="login-split-container">
        {/* Left Side: Brand Panel */}
        <div className="login-brand-panel">
          <div className="login-brand-content">
            <div className="login-logo-area">
              <span className="login-logo-icon">⚡</span>
              <span className="login-logo-title">Distributed Job Scheduler</span>
            </div>
            
            <div className="login-middle-area">
              <h1 className="login-hero-tagline">
                High-throughput distributed job scheduling.
              </h1>
              
              <div className="login-feature-list">
                <div className="login-feature-item">
                  <span className="login-feature-dot">✔</span>
                  <div>
                    <div className="login-feature-title">Exactly-Once Claim Dispatching</div>
                    <div className="login-feature-desc">Atomic database level claim dispatching across multi-node worker fleets.</div>
                  </div>
                </div>
                <div className="login-feature-item">
                  <span className="login-feature-dot">✔</span>
                  <div>
                    <div className="login-feature-title">Automatic Backoff Retry Engine</div>
                    <div className="login-feature-desc">Automatic retry loops with exponential backoff and maximum claim controls.</div>
                  </div>
                </div>
                <div className="login-feature-item">
                  <span className="login-feature-dot">✔</span>
                  <div>
                    <div className="login-feature-title">Dead Letter Queue Telemetry</div>
                    <div className="login-feature-desc">Comprehensive DLQ inspection panel with soft-discard execution history audit trails.</div>
                  </div>
                </div>
              </div>
            </div>

            {/* Static Visual Pipeline Preview */}
            <div className="login-pipeline-preview">
              <div className="preview-pipeline-label">WORKFLOW PIPELINE PATHWAY PREVIEW</div>
              <div className="preview-pipeline-steps">
                <div className="preview-step">
                  <span className="preview-dot queued"></span>
                  <span style={{ fontSize: "11px", fontWeight: 500 }}>Queued</span>
                </div>
                <div className="preview-arrow">➔</div>
                <div className="preview-step">
                  <span className="preview-dot running"></span>
                  <span style={{ fontSize: "11px", fontWeight: 500 }}>Running</span>
                </div>
                <div className="preview-arrow">➔</div>
                <div className="preview-step">
                  <span className="preview-dot completed"></span>
                  <span style={{ fontSize: "11px", fontWeight: 500 }}>Completed</span>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Right Side: Form Panel */}
        <div className="login-form-panel">
          <form className="login-card-form" onSubmit={handleLogin}>
            <div className="login-form-header">
              <h2 className="login-form-title">Operator Portal</h2>
              <p className="login-form-subtitle">Enter credentials to manage your cluster fleet.</p>
            </div>
            
            {authError && <div className="login-error">{authError}</div>}
            
            <div className="form-group">
              <label className="form-label">Email Address</label>
              <input
                type="email"
                className="form-input"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                required
                autoFocus
              />
            </div>
            
            <div className="form-group">
              <label className="form-label">Password</label>
              <input
                type="password"
                className="form-input"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required
              />
            </div>
            
            <button type="submit" className="btn btn-primary" style={{ justifyContent: "center", marginTop: "12px", padding: "10px 16px" }}>
              Sign In to Fleet
            </button>
            
            <div className="login-helper-note">
              Seeded local administrator profile enabled.
            </div>
          </form>
        </div>
      </div>
    );
  }

  return (
    <div className="app-container">
      {/* Header Bar */}
      <header className="header">
        <div className="brand">
          <span className="brand-title">Distributed Job Scheduler</span>
          <span className="brand-mono">v0.1.0</span>
          {selectedProjectId && (
            <span className="brand-project-tag">
              {projects.find(p => p.id === selectedProjectId)?.name || "Video Processing Cluster"}
            </span>
          )}
        </div>
        <div className="header-controls">

          <button className="btn btn-primary" onClick={() => setShowSubmitModal(true)}>
            + Submit Workload
          </button>
          <button className="btn btn-secondary" onClick={handleLogout}>
            Sign Out
          </button>
        </div>
      </header>

      <div className="main-content">
        {/* Sidebar Nav */}
        <aside className="sidebar">
          <nav className="nav-links">
            <button
              className={`nav-item ${activeView === "pipelines" ? "active" : ""}`}
              onClick={() => setActiveView("pipelines")}
            >
              <span>Queue Pipelines</span>
            </button>
            <button
              className={`nav-item ${activeView === "dlq" ? "active" : ""}`}
              onClick={() => {
                setActiveView("dlq");
                if (queues.length > 0 && !selectedQueueId) {
                  setSelectedQueueId(queues[0].id);
                }
              }}
            >
              <span>Dead Letter Queue</span>
            </button>
            <button
              className={`nav-item ${activeView === "fleet" ? "active" : ""}`}
              onClick={() => setActiveView("fleet")}
            >
              <span>Worker Fleet ({workers.length})</span>
            </button>
          </nav>
          <div style={{ padding: "0 16px", fontSize: "11px", color: "var(--text-secondary)" }}>
            Organization Admin Mode
          </div>
        </aside>

        {/* Content Pane */}
        <main className="content-pane">
          {activeView === "pipelines" && (
            <div>
              {/* Greeting Header */}
              <div className="greeting-header">
                <h1 className="greeting-title">Welcome back — here's your cluster status</h1>
                <div className="greeting-subtitle">
                  Project: <span style={{ fontWeight: 600 }}>{projects.find(p => p.id === selectedProjectId)?.name || "Video Processing Cluster"}</span> &bull; {new Date().toLocaleDateString(undefined, { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' })}
                </div>
              </div>

              {/* Metrics Row */}
              <div className="metrics-row">
                <div className="metric-card metric-card-hero">
                  <div>
                    <div className="metric-label">Active Workload</div>
                    <div className="metric-value">
                      {String(summary?.job_counts?.running || 0).padStart(2, '0')} Running
                    </div>
                  </div>
                  <div className="metric-footer">
                    Active executing cluster workload jobs
                  </div>
                </div>

                <div className="metric-card">
                  <div>
                    <div className="metric-label">Queued Jobs</div>
                    <div className="metric-value">
                      {String(summary?.job_counts?.queued || 0).padStart(2, '0')}
                    </div>
                  </div>
                  <div className="metric-footer">
                    Pending claim queue dispatcher
                  </div>
                </div>

                <div className="metric-card">
                  <div>
                    <div className="metric-label">Failing Jobs</div>
                    <div className="metric-value" style={{ color: "var(--color-failed)" }}>
                      {String(summary?.job_counts?.failed || 0).padStart(2, '0')}
                    </div>
                  </div>
                  <div className="metric-footer">
                    Failed after exhausting retries
                  </div>
                </div>

                <div className="metric-card">
                  <div>
                    <div className="metric-label">Active Workers</div>
                    <div className="metric-value" style={{ color: "var(--color-completed)" }}>
                      {String(summary?.active_workers_count || 0).padStart(2, '0')}
                    </div>
                  </div>
                  <div className="metric-footer">
                    Seeded cluster fleet heartbeats
                  </div>
                </div>
              </div>

              {/* Pipeline Lanes */}
              <div className="queue-lanes-container">
                {queues.length === 0 ? (
                  <div className="empty-state">
                    <div className="empty-state-icon">📂</div>
                    <div>No queues configured inside this project yet.</div>
                    <button className="btn" onClick={() => alert("Create queues via the REST APIs or seeding tools.")}>
                      Learn how to create queues
                    </button>
                  </div>
                ) : (
                  queues.map((q) => {
                    const allJobs = queueJobs[q.id] || [];
                    const queuedJobsList = allJobs.filter(j => j.status === "queued" || j.status === "scheduled");
                    const runningJobsList = allJobs.filter(j => j.status === "running" || j.status === "claimed");
                    const completedJobsList = allJobs.filter(j => j.status === "completed");
                    const failedJobsList = allJobs.filter(j => j.status === "failed");
                    const dlqJobsList = allJobs.filter(j => j.status === "dead_letter");

                    return (
                      <div className="queue-lane-card" key={q.id}>
                        <div className="queue-lane-header">
                          <div className="queue-title">
                            <span className="queue-title-mono">{q.name}</span>
                            {q.paused && <span style={{ color: "var(--color-failed)", fontSize: "11px", marginLeft: "8px" }}>[PAUSED]</span>}
                          </div>
                          <div className="queue-meta">
                            <span>Priority: {q.priority}</span>
                            <span>Limit: {q.concurrency_limit}</span>
                            <button
                              className="btn btn-secondary"
                              style={{ padding: "2px 8px", fontSize: "11px" }}
                              onClick={() => handleTogglePause(q.id, q.paused)}
                            >
                              {q.paused ? "Resume Queue" : "Pause Queue"}
                            </button>
                          </div>
                        </div>
                        
                        <div className="queue-pipeline">
                          {/* 1. Queued / Scheduled Stage */}
                          <div className="pipeline-stage">
                            <div className="stage-header">
                              <span>Queued / Scheduled</span>
                              <span className="stage-count">{queuedJobsList.length}</span>
                            </div>
                            <div className="stage-items">
                              {queuedJobsList.length > 0 ? (
                                queuedJobsList.map(j => (
                                  <div className="job-card job-card-queued" key={j.id} onClick={() => setSelectedInspectorJob(j)} style={{ cursor: "pointer" }}>
                                    <div className="job-header">
                                      <span className="job-id">ID: {j.id.slice(0, 8)}...</span>
                                      <span className="job-type-badge">{j.job_type}</span>
                                    </div>
                                    <div className="job-details">
                                      <span>Attempts: {j.retry_count}</span>
                                    </div>
                                  </div>
                                ))
                              ) : (
                                <div className="empty-stage-text">0 jobs queued</div>
                              )}
                            </div>
                          </div>

                          {/* 2. Running Stage */}
                          <div className="pipeline-stage">
                            <div className="stage-header">
                              <span>Active / Running</span>
                              <span className="stage-count">{runningJobsList.length}</span>
                            </div>
                            <div className="stage-items">
                              {runningJobsList.length > 0 ? (
                                runningJobsList.map(j => (
                                  <div className="job-card job-card-running" key={j.id} onClick={() => setSelectedInspectorJob(j)} style={{ cursor: "pointer" }}>
                                    <div className="job-header">
                                      <span className="job-id">ID: {j.id.slice(0, 8)}...</span>
                                      <span className="job-type-badge">{j.job_type}</span>
                                    </div>
                                    <div className="job-details">
                                      <div className="status-dot-wrapper" style={{ color: "var(--color-running)" }}>
                                        <span className="pulse-indicator"></span>
                                        <span>Running</span>
                                      </div>
                                    </div>
                                  </div>
                                ))
                              ) : (
                                <div className="empty-stage-text">0 jobs running</div>
                              )}
                            </div>
                          </div>

                          {/* 3. Completed Stage */}
                          <div className="pipeline-stage">
                            <div className="stage-header">
                              <span>Completed</span>
                              <span className="stage-count">{completedJobsList.length}</span>
                            </div>
                            <div className="stage-items">
                              {completedJobsList.length > 0 ? (
                                completedJobsList.map(j => (
                                  <div className="job-card job-card-completed" key={j.id} onClick={() => setSelectedInspectorJob(j)} style={{ cursor: "pointer" }}>
                                    <div className="job-header">
                                      <span className="job-id">ID: {j.id.slice(0, 8)}...</span>
                                      <span className="job-type-badge">{j.job_type}</span>
                                    </div>
                                    <div className="job-details">
                                      <span>Success</span>
                                    </div>
                                  </div>
                                ))
                              ) : (
                                <div className="empty-stage-text">0 jobs completed</div>
                              )}
                            </div>
                          </div>

                          {/* 4. Failed Stage */}
                          <div className="pipeline-stage">
                            <div className="stage-header">
                              <span>Failed</span>
                              <span className="stage-count">{failedJobsList.length}</span>
                            </div>
                            <div className="stage-items">
                              {failedJobsList.length > 0 ? (
                                failedJobsList.map(j => (
                                  <div className="job-card job-card-failed" key={j.id} onClick={() => setSelectedInspectorJob(j)} style={{ cursor: "pointer" }}>
                                    <div className="job-header">
                                      <span className="job-id">ID: {j.id.slice(0, 8)}...</span>
                                      <span className="job-type-badge">{j.job_type}</span>
                                    </div>
                                    <div className="job-details">
                                      <span style={{ color: "var(--color-failed)" }}>Failed</span>
                                    </div>
                                  </div>
                                ))
                              ) : (
                                <div className="empty-stage-text">0 jobs failed</div>
                              )}
                            </div>
                          </div>

                          {/* 5. Dead Letter Stage */}
                          <div className="pipeline-stage">
                            <div className="stage-header">
                              <span>Dead Letter</span>
                              <span className="stage-count">{dlqJobsList.length}</span>
                            </div>
                            <div className="stage-items">
                              {dlqJobsList.length > 0 ? (
                                dlqJobsList.map(j => (
                                  <div 
                                    className="job-card job-card-dlq" 
                                    key={j.id}
                                    onClick={() => {
                                      setSelectedQueueId(q.id);
                                      setActiveView("dlq");
                                    }}
                                    style={{ cursor: "pointer" }}
                                  >
                                    <div className="job-header">
                                      <span className="job-id">ID: {j.id.slice(0, 8)}...</span>
                                      <span className="job-type-badge">{j.job_type}</span>
                                    </div>
                                    <div className="job-details" style={{ color: "var(--color-dlq)", textDecoration: "underline" }}>
                                      <span>Inspect in DLQ</span>
                                    </div>
                                  </div>
                                ))
                              ) : (
                                <div className="empty-stage-text">0 jobs in DLQ</div>
                              )}
                            </div>
                          </div>
                        </div>
                      </div>
                    );
                  })
                )}
              </div>

              {/* Recent Activities Table Section */}
              {queues.length > 0 && (
                <div className="history-section">
                  <div className="section-header">
                    <h2 className="section-title">Recent Workload History</h2>
                    <div className="search-filter-bar">
                      <input
                        type="text"
                        className="search-input"
                        placeholder="Search by Job ID, Type or Payload..."
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                      />
                    </div>
                  </div>

                  <div className="activities-table-wrapper">
                    <table className="activities-table">
                      <thead>
                        <tr>
                          <th>Job ID</th>
                          <th>Queue</th>
                          <th>Type</th>
                          <th>Attempts</th>
                          <th>Status</th>
                          <th>Scheduled Run</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(() => {
                          const allProjectJobs = Object.values(queueJobs).flat().sort((a, b) => {
                            return new Date(b.created_at || b.run_at) - new Date(a.created_at || a.run_at);
                          });
                          const filteredJobs = allProjectJobs.filter(j => {
                            if (!searchQuery) return true;
                            const query = searchQuery.toLowerCase();
                            return j.id.toLowerCase().includes(query) || 
                                   j.job_type.toLowerCase().includes(query) ||
                                   JSON.stringify(j.payload || {}).toLowerCase().includes(query);
                          });
                          
                          return filteredJobs.length > 0 ? (
                            filteredJobs.slice(0, 10).map((j) => {
                              const qName = queues.find(q => q.id === j.queue_id)?.name || "default";
                              return (
                                <tr key={j.id} style={{ cursor: "pointer" }} onClick={() => setSelectedInspectorJob(j)}>
                                  <td style={{ fontFamily: "var(--font-mono)", fontWeight: 500 }}>{j.id}</td>
                                  <td style={{ fontFamily: "var(--font-mono)" }}>{qName}</td>
                                  <td>
                                    <span className="job-type-badge">{j.job_type}</span>
                                  </td>
                                  <td>{j.retry_count}</td>
                                  <td>
                                    <div className="status-dot-wrapper">
                                      <span className={`status-dot ${j.status.toLowerCase()}`}></span>
                                      <span style={{ textTransform: "capitalize" }}>{j.status.replace("_", " ")}</span>
                                    </div>
                                  </td>
                                  <td style={{ fontFamily: "var(--font-mono)", color: "var(--text-secondary)" }}>
                                    {new Date(j.run_at).toLocaleTimeString()}
                                  </td>
                                </tr>
                              );
                            })
                          ) : (
                            <tr>
                              <td colSpan="6" style={{ textAlign: "center", color: "var(--text-secondary)", padding: "24px" }}>
                                No recent workload executions found matching search parameters.
                              </td>
                            </tr>
                          );
                        })()}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </div>
          )}

          {activeView === "dlq" && (
            <div className="dlq-layout">
              <div style={{ display: "flex", gap: "12px", alignItems: "center" }}>
                <span className="section-label">Target Queue:</span>
                <select
                  className="project-select"
                  value={selectedQueueId}
                  onChange={(e) => setSelectedQueueId(e.target.value)}
                >
                  {queues.map((q) => (
                    <option key={q.id} value={q.id}>
                      {q.name}
                    </option>
                  ))}
                </select>
              </div>

              <div className="dlq-grid">
                {/* List Panel */}
                <div className="dlq-list-panel">
                  <div className="panel-header">
                    <span className="panel-title">Dead Letter Queue Jobs ({dlqJobs.length})</span>
                  </div>
                  <div className="dlq-scrollable">
                    {dlqJobs.length === 0 ? (
                      <div className="empty-state">
                        <div className="empty-state-icon">✓</div>
                        <div>DLQ is healthy! No dead-lettered jobs found.</div>
                      </div>
                    ) : (
                      dlqJobs.map((item) => (
                        <div
                          key={item.id}
                          className={`dlq-item-card ${selectedDlqJob?.id === item.id ? "selected" : ""}`}
                          onClick={() => setSelectedDlqJob(item)}
                        >
                          <div className="dlq-item-header">
                            <span className="dlq-job-name">Attempt #{item.job.retry_count + 1}</span>
                            <span className="dlq-timestamp">
                              {new Date(item.moved_at).toLocaleTimeString()}
                            </span>
                          </div>
                          <div className="job-id">{item.job_id}</div>
                          <div className="dlq-reason-summary">{item.failure_reason}</div>
                        </div>
                      ))
                    )}
                  </div>
                </div>

                {/* Detail Panel */}
                <div className="dlq-detail-panel">
                  <div className="panel-header">
                    <span className="panel-title">Inspector & Actions</span>
                    {selectedDlqJob && (
                      <div className="dlq-actions-bar">
                        <button
                          className="btn btn-secondary"
                          style={{ padding: "4px 8px", fontSize: "11px", color: "var(--color-completed)" }}
                          onClick={() => handleRequeueDLQ(selectedDlqJob.job_id)}
                        >
                          Requeue Immediately
                        </button>
                        <button
                          className="btn btn-secondary"
                          style={{ padding: "4px 8px", fontSize: "11px", color: "var(--color-failed)" }}
                          onClick={() => handleSoftDiscardDLQ(selectedDlqJob.job_id)}
                        >
                          Discard Job
                        </button>
                      </div>
                    )}
                  </div>
                  
                  <div className="dlq-detail-scrollable">
                    {selectedDlqJob ? (
                      <>
                        <div className="detail-section">
                          <span className="section-label">Job ID</span>
                          <span className="detail-value-mono">{selectedDlqJob.job_id}</span>
                        </div>
                        <div className="detail-section">
                          <span className="section-label">Failure Reason</span>
                          <div className="dlq-reason-summary" style={{ fontSize: "13px" }}>
                            {selectedDlqJob.failure_reason}
                          </div>
                        </div>
                        <div className="detail-section">
                          <span className="section-label">Payload</span>
                          <pre className="trace-codeblock" style={{ whiteSpace: "pre-wrap" }}>
                            {JSON.stringify(selectedDlqJob.job.payload, null, 2)}
                          </pre>
                        </div>
                        <div className="detail-section">
                          <span className="section-label">Stack Trace & History</span>
                          <div className="trace-codeblock">
                            {`Stack trace collected at: ${new Date(selectedDlqJob.moved_at).toISOString()}
Attempt Count: ${selectedDlqJob.job.retry_count}
Terminal state triggered after repeated execution stalls.
DLQ Soft-Discard preserved logs.`}
                          </div>
                        </div>
                      </>
                    ) : (
                      <div className="empty-state" style={{ height: "100%" }}>
                        <div>Select a DLQ job card to inspect details, view stack trace records, and perform requeue or discard operations.</div>
                      </div>
                    )}
                  </div>
                </div>
              </div>
            </div>
          )}

          {activeView === "fleet" && (
            <div className="fleet-grid">
              {workers.length === 0 ? (
                <div className="empty-state" style={{ gridColumn: "1/-1" }}>
                  <div className="empty-state-icon">📡</div>
                  <div>No worker nodes currently active. Check worker engine configuration.</div>
                </div>
              ) : (
                workers.map((w) => (
                  <div className="worker-node-card" key={w.id}>
                    <div className="worker-node-header">
                      <span className="worker-hostname">{w.hostname}</span>
                      <span className={`worker-status-badge ${w.status.toLowerCase()}`}>
                        {w.status}
                      </span>
                    </div>
                    <div className="worker-details">
                      <div><span style={{ color: "var(--text-secondary)" }}>ID:</span> <span style={{ fontFamily: "var(--font-mono)", fontSize: "11px" }}>{w.id}</span></div>
                      <div><span style={{ color: "var(--text-secondary)" }}>Last Active:</span> {w.last_seen ? new Date(w.last_seen).toLocaleTimeString() : "Never"}</div>
                      <div><span style={{ color: "var(--text-secondary)" }}>Registered:</span> {new Date(w.created_at).toLocaleDateString()}</div>
                    </div>
                  </div>
                ))
              )}
            </div>
          )}
        </main>
      </div>

      {/* Submit Workload Modal */}
      {showSubmitModal && (
        <div className="modal-overlay">
          <form className="modal-content" onSubmit={handleSubmitJob}>
            <div className="modal-header">
              <span className="panel-title">Submit New Workload</span>
              <button type="button" className="btn btn-secondary" onClick={() => setShowSubmitModal(false)}>
                ✕
              </button>
            </div>
            
            <div className="modal-body">
              <div className="form-group">
                <label className="form-label">Target Queue</label>
                <select
                  className="project-select"
                  value={submitQueueId}
                  onChange={(e) => setSubmitQueueId(e.target.value)}
                >
                  {queues.map((q) => (
                    <option key={q.id} value={q.id}>
                      {q.name}
                    </option>
                  ))}
                </select>
              </div>

              <div className="form-group">
                <label className="form-label">Submission Mode</label>
                <div style={{ display: "flex", gap: "16px", marginTop: "4px" }}>
                  <label style={{ display: "flex", alignItems: "center", gap: "4px" }}>
                    <input
                      type="radio"
                      name="submode"
                      checked={!isBatch}
                      onChange={() => setIsBatch(false)}
                    /> Single Job
                  </label>
                  <label style={{ display: "flex", alignItems: "center", gap: "4px" }}>
                    <input
                      type="radio"
                      name="submode"
                      checked={isBatch}
                      onChange={() => setIsBatch(true)}
                    /> Batch Jobs
                  </label>
                </div>
              </div>

              {isBatch && (
                <div className="form-group">
                  <label className="form-label">Batch Job Count</label>
                  <input
                    type="number"
                    className="form-input"
                    value={batchCount}
                    onChange={(e) => setBatchCount(parseInt(e.target.value))}
                    min="1"
                    max="100"
                  />
                </div>
              )}

              <div className="form-group">
                <label className="form-label">Job Type</label>
                <select
                  className="project-select"
                  value={jobType}
                  onChange={(e) => setJobType(e.target.value)}
                >
                  <option value="immediate">Immediate</option>
                  <option value="delayed">Delayed (60 seconds)</option>
                </select>
              </div>

              <div className="form-group">
                <label className="form-label">JSON Payload</label>
                <textarea
                  className="form-input"
                  style={{ fontFamily: "var(--font-mono)", fontSize: "12px", minHeight: "80px", resize: "vertical" }}
                  value={jobPayloadText}
                  onChange={(e) => setJobPayloadText(e.target.value)}
                  required
                />
              </div>
            </div>

            <div className="modal-footer">
              <button type="button" className="btn btn-secondary" onClick={() => setShowSubmitModal(false)}>
                Cancel
              </button>
              <button type="submit" className="btn btn-primary">
                Enqueue Workload
              </button>
            </div>
          </form>
        </div>
      )}

      {/* Job Details Modal */}
      {selectedInspectorJob && (
        <div className="modal-overlay" onClick={() => setSelectedInspectorJob(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <span className="panel-title">Job Inspector</span>
              <button type="button" className="btn btn-secondary" onClick={() => setSelectedInspectorJob(null)}>
                ✕
              </button>
            </div>
            <div className="modal-body">
              <div className="detail-section">
                <span className="section-label">Job ID</span>
                <span className="detail-value-mono">{selectedInspectorJob.id}</span>
              </div>
              <div className="detail-section">
                <span className="section-label">Status</span>
                <div className="status-dot-wrapper">
                  <span className={`status-dot ${selectedInspectorJob.status.toLowerCase()}`}></span>
                  <span style={{ textTransform: "capitalize", fontWeight: 600 }}>{selectedInspectorJob.status.replace("_", " ")}</span>
                </div>
              </div>
              <div className="detail-section">
                <span className="section-label">Type</span>
                <span className="detail-value-mono">{selectedInspectorJob.job_type}</span>
              </div>
              <div className="detail-section">
                <span className="section-label">Payload</span>
                <pre className="trace-codeblock" style={{ whiteSpace: "pre-wrap" }}>
                  {JSON.stringify(selectedInspectorJob.payload, null, 2)}
                </pre>
              </div>
              <div className="detail-section">
                <span className="section-label">Scheduler Parameters</span>
                <div className="trace-codeblock">
                  {`Retry Count: ${selectedInspectorJob.retry_count}
Run At: ${new Date(selectedInspectorJob.run_at).toLocaleString()}
Created At: ${new Date(selectedInspectorJob.created_at || selectedInspectorJob.run_at).toLocaleString()}`}
                </div>
              </div>
              {selectedInspectorJob.depends_on && selectedInspectorJob.depends_on.length > 0 && (
                <div className="detail-section">
                  <span className="section-label">Dependencies</span>
                  <div style={{ display: "flex", flexDirection: "column", gap: "6px", marginTop: "4px" }}>
                    {selectedInspectorJob.depends_on.map((dep) => (
                      <div key={dep.id} style={{ display: "flex", alignItems: "center", gap: "8px", padding: "6px 12px", border: "1px solid var(--border-chrome)", borderRadius: "6px", backgroundColor: "var(--bg-base)" }}>
                        <span className={`status-dot ${dep.status.toLowerCase()}`}></span>
                        <span className="detail-value-mono" style={{ fontSize: "11px" }}>{dep.id}</span>
                        <span style={{ fontSize: "11px", color: "var(--text-secondary)", textTransform: "capitalize" }}>({dep.status.replace("_", " ")})</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
            <div className="modal-footer">
              <button className="btn btn-primary" onClick={() => setSelectedInspectorJob(null)}>
                Close Inspector
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
