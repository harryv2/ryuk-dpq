"use client";

import { Suspense, useCallback, useEffect, useState } from "react";
import { useSearchParams } from "next/navigation";
import Link from "next/link";
import { useOrg } from "@/lib/org-context";
import { api, NodeDetail } from "@/lib/api";
import { Empty, Skeleton, StatCard, age } from "@/lib/ui";
import { CopyButton } from "@/lib/copy";

function NodeDetailInner() {
  const { org } = useOrg();
  const id = useSearchParams().get("id") ?? "";

  const [node, setNode] = useState<NodeDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState("");

  const refresh = useCallback(async () => {
    try {
      setNode(await api.node(org.token, id));
      setErr("");
    } catch (e: any) {
      setErr(e.message);
    } finally {
      setLoading(false);
    }
  }, [org.token, id]);

  useEffect(() => {
    setLoading(true);
    refresh();
    const t = setInterval(refresh, 2000);
    return () => clearInterval(t);
  }, [refresh]);

  const container = node?.addr.split(":")[0] ?? "";
  const queues = node?.queues ?? [];

  return (
    <>
      <div className="page-head">
        <div>
          <h1 className="mono">{id}</h1>
          <p className="sub">
            {node?.live === false ? (
              <span className="tag high">not answering</span>
            ) : (
              <>
                <span className="live-dot" /> {node?.addr}
              </>
            )}
          </p>
        </div>
        <div className="row">
          <Link href="/cluster"><button>Back to cluster</button></Link>
          {container && (
            <CopyButton
              value={`docker stop ${container}`}
              label="docker stop"
              title={`Copy: docker stop ${container}`}
            />
          )}
        </div>
      </div>

      {err && <p className="err">{err}</p>}

      <div className="grid cols-4" style={{ marginBottom: 28 }}>
        <StatCard
          label="Ready here" value={(node?.ready ?? 0).toLocaleString()} loading={loading}
          hint="waiting in this node's slots"
        />
        <StatCard label="In flight" value={(node?.inFlight ?? 0).toLocaleString()} loading={loading}
          hint="taken, not yet acked" />
        <StatCard label="Delayed" value={(node?.delayed ?? 0).toLocaleString()} loading={loading}
          hint="not due yet" />
        <StatCard label="Slots held" value={node?.slots ?? 0} loading={loading}
          hint={`across ${queues.length} queue${queues.length === 1 ? "" : "s"}`} />
      </div>

      <h2 style={{ marginBottom: 4 }}>Queues on this node</h2>
      <p className="muted" style={{ margin: "0 0 14px", maxWidth: 720, fontSize: 13 }}>
        Counts are this node&rsquo;s share only. A distributed queue holds the rest of
        its messages on the other machines its slots are placed on.
      </p>

      <div className="card flush">
        <table>
          <thead>
            <tr>
              <th>Queue</th>
              <th className="num">Slots</th>
              <th className="num">Ready</th>
              <th className="num">High</th>
              <th className="num">Medium</th>
              <th className="num">Low</th>
              <th className="num">In flight</th>
              <th className="num">Delayed</th>
              <th className="num">Oldest</th>
            </tr>
          </thead>
          <tbody>
            {loading && (
              <tr><td colSpan={9}><Skeleton h={14} /></td></tr>
            )}
            {!loading && queues.length === 0 && (
              <tr>
                <td colSpan={9}>
                  <Empty title="This node holds nothing">
                    Slots land here as queues are created or the cluster rebalances.
                  </Empty>
                </td>
              </tr>
            )}
            {queues.map((q) => (
              <tr key={q.queue}>
                <td>
                  <Link href={`/queue?name=${encodeURIComponent(q.queue)}`}>{q.queue}</Link>
                  {q.distributed && <span className="tag dist" style={{ marginLeft: 8 }}>distributed</span>}
                </td>
                <td className="num muted">{q.slots} / {q.totalSlots}</td>
                <td className="num">{q.ready.toLocaleString()}</td>
                <td className="num muted">{(q.byPriority?.high ?? 0).toLocaleString()}</td>
                <td className="num muted">{(q.byPriority?.medium ?? 0).toLocaleString()}</td>
                <td className="num muted">{(q.byPriority?.low ?? 0).toLocaleString()}</td>
                <td className="num">{q.inFlight.toLocaleString()}</td>
                <td className="num muted">{q.delayed.toLocaleString()}</td>
                <td className="num muted">{age(q.oldestMessageAgeSeconds)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <h2 style={{ marginTop: 32, marginBottom: 4 }}>Since this node started</h2>
      <p className="muted" style={{ margin: "0 0 14px", maxWidth: 720, fontSize: 13 }}>
        Running totals, held in memory. A restart replays the log and rebuilds the
        messages, not these counters.
      </p>

      <div className="card flush">
        <table>
          <thead>
            <tr>
              <th>Queue</th>
              <th className="num">Acked</th>
              <th className="num">Redelivered</th>
              <th className="num">Expired</th>
              <th className="num">Dead-lettered</th>
              <th className="num">Starvation escapes</th>
            </tr>
          </thead>
          <tbody>
            {loading && (
              <tr><td colSpan={6}><Skeleton h={14} /></td></tr>
            )}
            {!loading && queues.length === 0 && (
              <tr><td colSpan={6}><Empty title="Nothing to report yet" /></td></tr>
            )}
            {queues.map((q) => (
              <tr key={q.queue}>
                <td className="mono">{q.queue}</td>
                <td className="num">{q.acked.toLocaleString()}</td>
                <td className="num muted">{q.redelivered.toLocaleString()}</td>
                <td className="num muted">{q.expired.toLocaleString()}</td>
                <td className="num muted">{q.deadLettered.toLocaleString()}</td>
                <td className="num muted">{q.starvationEscapes.toLocaleString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}

export default function NodePage() {
  return (
    <Suspense fallback={<p className="muted">Loading…</p>}>
      <NodeDetailInner />
    </Suspense>
  );
}
