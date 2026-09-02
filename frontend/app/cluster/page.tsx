"use client";

import { useEffect, useState } from "react";
import { useOrg } from "@/lib/org-context";
import { api, ClusterNode, ClusterPlacement } from "@/lib/api";
import { Empty, Skeleton, StatCard } from "@/lib/ui";

type QueueView = {
  queue: string;
  org: string;
  totalSlots: number;
  placed: { node: string; addr: string; slots: number }[];
};

// One row per queue rather than one per node, because a distributed queue lives
// on several nodes and counting it once per node is what made the old totals
// read like there were more queues than there are.
function byQueue(nodes: ClusterNode[], want: boolean): QueueView[] {
  const out = new Map<string, QueueView>();
  for (const n of nodes) {
    for (const p of n.queues) {
      if (p.distributed !== want) continue;
      const k = `${p.org}/${p.queue}`;
      if (!out.has(k)) {
        out.set(k, { queue: p.queue, org: p.org, totalSlots: p.totalSlots, placed: [] });
      }
      out.get(k)!.placed.push({ node: n.id, addr: n.addr, slots: p.slots });
    }
  }
  for (const v of out.values()) v.placed.sort((a, b) => b.slots - a.slots);
  return [...out.values()].sort((a, b) => a.queue.localeCompare(b.queue));
}

export default function Cluster() {
  const { org } = useOrg();
  const [nodes, setNodes] = useState<ClusterNode[]>([]);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState("");

  useEffect(() => {
    let alive = true;
    const load = async () => {
      try {
        const n = await api.cluster(org.token);
        if (!alive) return;
        setNodes(n);
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

  const distributed = byQueue(nodes, true);
  const single = byQueue(nodes, false);
  const idle = nodes.filter((n) => n.queues.length === 0);

  return (
    <>
      <div className="page-head">
        <div>
          <h1>Cluster</h1>
          <p className="sub">
            {loading ? (
              "Loading…"
            ) : (
              <>
                <span className="live-dot" /> Nodes register themselves under a lease.
                Add more with <span className="mono">docker compose up --scale node=6</span>.
              </>
            )}
          </p>
        </div>
      </div>

      {err && <p className="err">{err}</p>}

      <div className="grid cols-4" style={{ marginBottom: 28 }}>
        <StatCard label="Live nodes" value={nodes.length} loading={loading} />
        <StatCard
          label="Distributed queues" value={distributed.length} loading={loading}
          hint="each spread over several nodes"
        />
        <StatCard
          label="Single-node queues" value={single.length} loading={loading}
          hint="each placed whole on one node"
        />
        <StatCard
          label="Idle nodes" value={idle.length} loading={loading}
          hint="registered, holding nothing"
        />
      </div>

      <Section
        title="Distributed queues"
        note="Each slot is placed on its own, so one queue occupies several machines. Losing a node costs only its slots."
        loading={loading}
        empty="No distributed queues"
        emptyHint="Create one with the distributed flag to see slots spread across nodes."
      >
        {distributed.map((q) => (
          <div className="card" key={q.org + q.queue}>
            <div className="card-head">
              <h2 className="mono">{q.queue}</h2>
              <span className="tag dist">
                {q.placed.length} node{q.placed.length > 1 ? "s" : ""}
              </span>
            </div>
            <SlotBar placed={q.placed} total={q.totalSlots} />
            <table style={{ marginTop: 14 }}>
              <tbody>
                {q.placed.map((p) => (
                  <tr key={p.node}>
                    <td className="mono" style={{ padding: "8px 0" }}>{p.node}</td>
                    <td className="num muted" style={{ padding: "8px 0" }}>
                      {p.slots} / {q.totalSlots} slots
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ))}
      </Section>

      <Section
        title="Single-node queues"
        note="Placed whole, so priority order and counts are exact. If its node goes down the queue waits for it to return."
        loading={loading}
        empty="No single-node queues"
        emptyHint="The default for a new queue."
      >
        {single.map((q) => (
          <div className="card" key={q.org + q.queue}>
            <div className="card-head">
              <h2 className="mono">{q.queue}</h2>
              <span className="tag plain">{q.totalSlots} slots</span>
            </div>
            <p className="muted mono" style={{ margin: 0 }}>{q.placed[0]?.node}</p>
            <p className="muted mono" style={{ margin: "4px 0 0", fontSize: 12 }}>
              {q.placed[0]?.addr}
            </p>
          </div>
        ))}
      </Section>

      <h2 style={{ marginTop: 32 }}>Nodes</h2>
      <div className="card flush">
        <table>
          <thead>
            <tr>
              <th>Node</th>
              <th>Address</th>
              <th className="num">Queues</th>
              <th>State</th>
            </tr>
          </thead>
          <tbody>
            {loading && (
              <tr>
                <td colSpan={4}><Skeleton h={14} /></td>
              </tr>
            )}
            {!loading && nodes.length === 0 && (
              <tr>
                <td colSpan={4}>
                  <Empty title="No nodes registered">
                    Start one with <span className="mono">docker compose up -d node</span>.
                  </Empty>
                </td>
              </tr>
            )}
            {nodes.map((n) => (
              <tr key={n.id}>
                <td className="mono">{n.id}</td>
                <td className="mono muted">{n.addr}</td>
                <td className="num">{n.queues.length}</td>
                <td><span className="tag ok">live</span></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}

function Section({
  title, note, loading, empty, emptyHint, children,
}: {
  title: string; note: string; loading: boolean;
  empty: string; emptyHint: string; children: React.ReactNode[];
}) {
  return (
    <div style={{ marginBottom: 28 }}>
      <h2 style={{ marginBottom: 4 }}>{title}</h2>
      <p className="muted" style={{ margin: "0 0 14px", maxWidth: 700, fontSize: 13 }}>{note}</p>
      {loading ? (
        <div className="grid cols-3">
          {[0, 1].map((i) => (
            <div className="card" key={i}>
              <Skeleton w={120} h={16} />
              <Skeleton h={10} style={{ marginTop: 16 }} />
              <Skeleton w="60%" h={14} style={{ marginTop: 14 }} />
            </div>
          ))}
        </div>
      ) : children.length === 0 ? (
        <div className="card">
          <Empty title={empty}>{emptyHint}</Empty>
        </div>
      ) : (
        <div className="grid cols-3">{children}</div>
      )}
    </div>
  );
}

// How much of the queue each node holds, at a glance.
const SHADES = ["var(--accent)", "var(--low)", "var(--ok)", "var(--medium)", "var(--high)"];

function SlotBar({ placed, total }: { placed: { node: string; slots: number }[]; total: number }) {
  const held = placed.reduce((n, p) => n + p.slots, 0);
  return (
    <>
      <div style={{ display: "flex", height: 10, borderRadius: 5, overflow: "hidden", gap: 2 }}>
        {placed.map((p, i) => (
          <div
            key={p.node}
            title={`${p.node}: ${p.slots} slots`}
            style={{
              width: `${(p.slots / total) * 100}%`,
              background: SHADES[i % SHADES.length],
              opacity: 0.85,
            }}
          />
        ))}
        {held < total && (
          <div style={{ flex: 1, background: "var(--border)" }} title={`${total - held} slots not yet used`} />
        )}
      </div>
      <p className="muted" style={{ margin: "8px 0 0", fontSize: 12 }}>
        {held} of {total} slots placed
      </p>
    </>
  );
}
