"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { useOrg } from "@/lib/org-context";
import { api, QueueSummary } from "@/lib/api";
import { Empty, StatCard, TableSkeleton, age } from "@/lib/ui";

export default function Queues() {
  const { org } = useOrg();
  const [queues, setQueues] = useState<QueueSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState("");

  useEffect(() => {
    let alive = true;
    setLoading(true);
    const load = async () => {
      try {
        const q = await api.listQueues(org.token);
        if (!alive) return;
        setQueues(q);
        setErr("");
      } catch (e: any) {
        if (alive) setErr(e.message);
      } finally {
        if (alive) setLoading(false);
      }
    };
    load();
    const t = setInterval(load, 2000);
    return () => {
      alive = false;
      clearInterval(t);
    };
  }, [org.token]);

  const ready = queues.reduce((n, q) => n + q.messages, 0);
  const inFlight = queues.reduce((n, q) => n + q.inFlight, 0);
  const oldest = queues.reduce((n, q) => Math.max(n, q.oldestMessageAgeSeconds), 0);

  return (
    <>
      <div className="page-head">
        <div>
          <h1>Queues</h1>
          <p className="sub">{org.label}</p>
        </div>
        <Link href="/queues/new">
          <button className="primary">New queue</button>
        </Link>
      </div>

      {err && <p className="err">{err}</p>}

      <div className="grid cols-4" style={{ marginBottom: 24 }}>
        <StatCard label="Queues" value={queues.length} loading={loading} />
        <StatCard label="Ready" value={ready.toLocaleString()} loading={loading} />
        <StatCard label="In flight" value={inFlight.toLocaleString()} loading={loading} />
        <StatCard label="Oldest" value={age(oldest)} loading={loading} />
      </div>

      <div className="card flush">
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Placement</th>
              <th className="num">Ready</th>
              <th className="num">In flight</th>
              <th className="num">Oldest</th>
              <th>Owner node</th>
              <th>State</th>
            </tr>
          </thead>
          {loading ? (
            <TableSkeleton rows={3} cols={7} />
          ) : (
            <tbody>
              {queues.length === 0 && (
                <tr>
                  <td colSpan={7}>
                    <Empty title="No queues yet">
                      Create one to start sending messages.
                    </Empty>
                  </td>
                </tr>
              )}
              {queues.map((q) => (
                <tr key={q.name}>
                  <td>
                    <Link href={`/queue?name=${encodeURIComponent(q.name)}`}>{q.name}</Link>
                  </td>
                  <td>
                    {q.distributed ? (
                      <span className="tag dist" title="Machines this queue may use">
                        distributed{q.placementWidth ? ` · ${q.placementWidth}` : ""}
                      </span>
                    ) : (
                      <span className="tag plain">single node</span>
                    )}
                  </td>
                  <td className="num">{q.messages.toLocaleString()}</td>
                  <td className="num">{q.inFlight.toLocaleString()}</td>
                  <td className="num muted">{age(q.oldestMessageAgeSeconds)}</td>
                  <td className="mono muted">{q.ownerNode ?? "—"}</td>
                  <td>
                    <span className={"tag " + (q.state === "active" ? "ok" : "medium")}>
                      {q.state}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          )}
        </table>
      </div>
    </>
  );
}
