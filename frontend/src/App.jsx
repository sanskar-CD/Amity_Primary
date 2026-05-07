import { useCallback, useEffect, useState } from "react";

const apiBase = import.meta.env.VITE_API_URL || "";

async function api(path, options = {}) {
  const url = `${apiBase}${path}`;
  const res = await fetch(url, {
    ...options,
    headers: { "Content-Type": "application/json", ...options.headers },
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || res.statusText);
  }
  return res.json();
}

export default function App() {
  const [selectedDate, setSelectedDate] = useState(() => new Date().toISOString().slice(0, 10));
  const [job, setJob] = useState(null);
  const [jobPoll, setJobPoll] = useState(null);
  const [leads, setLeads] = useState(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [autoSync, setAutoSync] = useState(false);
  const [syncSettingsLoading, setSyncSettingsLoading] = useState(true);

  const loadSyncSettings = useCallback(async () => {
    try {
      const s = await api("/settings/sync");
      setAutoSync(!!s.auto_sync_enabled);
    } catch (e) {
      setError(String(e.message || e));
    } finally {
      setSyncSettingsLoading(false);
    }
  }, []);

  useEffect(() => {
    loadSyncSettings();
  }, [loadSyncSettings]);

  const toggleAutoSync = async () => {
    setError("");
    try {
      const s = await api("/settings/sync", {
        method: "PATCH",
        body: JSON.stringify({ auto_sync_enabled: !autoSync }),
      });
      setAutoSync(!!s.auto_sync_enabled);
    } catch (e) {
      setError(String(e.message || e));
    }
  };

  const fetchLeads = async () => {
    setError("");
    setLoading(true);
    setJob(null);
    setLeads(null);
    try {
      const queued = await api("/fetch-leads", {
        method: "POST",
        body: JSON.stringify({ selected_date: selectedDate }),
      });
      setJob(queued);
    } catch (e) {
      setError(String(e.message || e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (!job?.job_id) return;
    let cancelled = false;
    const id = setInterval(async () => {
      try {
        const j = await api(`/jobs/${job.job_id}`);
        if (cancelled) return;
        setJobPoll(j);
        if (j.status === "completed" || j.status === "failed") {
          clearInterval(id);
          const L = await api(`/leads?date=${encodeURIComponent(selectedDate)}`);
          if (!cancelled) setLeads(L);
        }
      } catch {
        /* ignore transient errors while polling */
      }
    }, 2000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [job, selectedDate]);

  const loadLeadsOnly = async () => {
    setError("");
    setLeads(null);
    try {
      const L = await api(`/leads?date=${encodeURIComponent(selectedDate)}`);
      setLeads(L);
    } catch (e) {
      setError(String(e.message || e));
    }
  };

  const displayJob = jobPoll || job;

  return (
    <div style={{ maxWidth: 960, margin: "0 auto", padding: "2rem 1.5rem" }}>
      <h1 style={{ marginTop: 0 }}>Lead sync</h1>
      <p style={{ color: "#64748b", marginBottom: "1.5rem" }}>
        Use this UI at <strong>http://localhost:5173</strong> with the API on <strong>http://localhost:8000</strong> (Vite
        proxy). Portal credentials stay on the API server only.
      </p>

      <section
        style={{
          background: "#fff",
          borderRadius: 12,
          padding: "1.25rem",
          marginBottom: "1rem",
          boxShadow: "0 1px 3px rgb(0 0 0 / 0.08)",
        }}
      >
        <h2 style={{ marginTop: 0, fontSize: "1.1rem" }}>Auto sync</h2>
        {syncSettingsLoading ? (
          <p>Loading settings…</p>
        ) : (
          <label style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer" }}>
            <input type="checkbox" checked={autoSync} onChange={toggleAutoSync} />
            <span>Daily automatic fetch when enabled (see backend cron hour in .env)</span>
          </label>
        )}
      </section>

      <section
        style={{
          background: "#fff",
          borderRadius: 12,
          padding: "1.25rem",
          marginBottom: "1rem",
          boxShadow: "0 1px 3px rgb(0 0 0 / 0.08)",
        }}
      >
        <h2 style={{ marginTop: 0, fontSize: "1.1rem" }}>Manual fetch</h2>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 12, alignItems: "center" }}>
          <label>
            Date{" "}
            <input type="date" value={selectedDate} onChange={(e) => setSelectedDate(e.target.value)} />
          </label>
          <button type="button" disabled={loading} onClick={fetchLeads}>
            {loading ? "Starting…" : "POST /fetch-leads"}
          </button>
          <button type="button" className="ghost" onClick={loadLeadsOnly}>
            GET /leads for date
          </button>
        </div>
      </section>

      {error ? (
        <div style={{ color: "#b91c1c", marginBottom: "1rem", whiteSpace: "pre-wrap" }}>{error}</div>
      ) : null}

      {displayJob ? (
        <section
          style={{
            background: "#fff",
            borderRadius: 12,
            padding: "1.25rem",
            marginBottom: "1rem",
            boxShadow: "0 1px 3px rgb(0 0 0 / 0.08)",
          }}
        >
          <h2 style={{ marginTop: 0, fontSize: "1.1rem" }}>Job</h2>
          <pre style={{ margin: 0, fontSize: 13, overflow: "auto" }}>{JSON.stringify(displayJob, null, 2)}</pre>
        </section>
      ) : null}

      {leads ? (
        <section
          style={{
            background: "#fff",
            borderRadius: 12,
            padding: "1.25rem",
            boxShadow: "0 1px 3px rgb(0 0 0 / 0.08)",
          }}
        >
          <h2 style={{ marginTop: 0, fontSize: "1.1rem" }}>
            Leads ({leads.count} for {leads.date})
          </h2>
          <pre style={{ margin: 0, fontSize: 12, overflow: "auto", maxHeight: 420 }}>{JSON.stringify(leads.leads, null, 2)}</pre>
        </section>
      ) : null}
    </div>
  );
}
