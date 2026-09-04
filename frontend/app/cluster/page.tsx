"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { useOrg } from "@/lib/org-context";
import { api, ClusterNode, ClusterPlacement, Registry } from "@/lib/api";
import { Empty, Skeleton, StatCard } from "@/lib/ui";
import { CopyButton } from "@/lib/copy";

// A node runs in a container whose hostname is its short id, and that hostname
// is what the node advertises. Pulling it out of the address gives the exact
// argument for `docker stop`, so a failure scenario can be triggered by hand.
function containerOf(addr: string): string {
  return addr.split(":")[0];
}

type QueueView = {
  queue: string;
  org: string;
  totalSlots: number;
  placed: { node: string; addr: string; slots: number }[];
  lost: number;
};

// One row per queue, not one per node: a distributed queue lives on several,
// and counting it once per node is what made the old totals read too high.
// Slots whose owner is gone are folded in here too, so a queue that lost its
// machines still has a row instead of silently disappearing.
function byQueue(
  nodes: ClusterNode[], unavailable: ClusterPlacement[], want: boolean,
): QueueView[] {
  const out = new Map<string, QueueView>();
  const view = (p: ClusterPlacement) => {
    const k = `${p.org}/${p.queue}`;
    if (!out.has(k)) {
      out.set(k, { queue: p.queue, org: p.org, totalSlots: p.totalSlots, placed: [], lost: 0 });
    }
    return out.get(k)!;
  };

  for (const n of nodes) {
    for (const p of n.queues) {
      if (p.distributed !== want) continue;
      view(p).placed.push({ node: n.id, addr: n.addr, slots: p.slots });
    }
  }
  for (const p of unavailable) {
    if (p.distributed !== want) continue;
    view(p).lost += p.slots;
  }

  for (const v of out.values()) v.placed.sort((a, b) => b.slots - a.slots);
  return [...out.values()].sort((a, b) => a.queue.localeCompare(b.queue));
}

export default function Cluster() {
  const { org } = useOrg();
  const [nodes, setNodes] = useState<ClusterNode[]>([]);
  const [unavailable, setUnavailable] = useState<ClusterPlacement[]>([]);
  const [registry, setRegistry] = useState<Registry | null>(null);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState("");

  useEffect(() => {
    let alive = true;
    const load = async () => {
      try {
        const c = await api.cluster(org.token);
        if (!alive) return;
        setNodes(c.nodes);
        setUnavailable(c.unavailable);
        setRegistry(await api.registry(org.token));
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

  const distributed = byQueue(nodes, unavailable, true);
  const single = byQueue(nodes, unavailable, false);
  const lostSlots = unavailable.reduce((n, p) => n + p.slots, 0);
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
        {lostSlots > 0 ? (
          <StatCard
            label="Slots unavailable" value={lostSlots} loading={loading}
            hint="owner is no longer registered"
          />
        ) : (
          <StatCard
            label="Idle nodes" value={idle.length} loading={loading}
            hint="registered, holding nothing"
          />
        )}
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
              <span className={q.lost > 0 ? "tag high" : "tag dist"}>
                {q.placed.length} node{q.placed.length === 1 ? "" : "s"}
              </span>
            </div>
            <SlotBar placed={q.placed} total={q.totalSlots} lost={q.lost} />
            <table style={{ marginTop: 14 }}>
              <tbody>
                {q.lost > 0 && (
                  <tr>
                    <td style={{ padding: "8px 0" }}>
                      <span className="tag high">machines gone</span>
                    </td>
                    <td className="num muted" style={{ padding: "8px 0" }}>
                      {q.lost} / {q.totalSlots} slots
                    </td>
                  </tr>
                )}
                {q.placed.map((p) => (
                  <tr key={p.node}>
                    <td className="mono" style={{ padding: "8px 0" }}>
                      <Link href={`/cluster/node?id=${encodeURIComponent(p.node)}`}>{p.node}</Link>
                      <span className="muted" style={{ marginLeft: 8, fontSize: 11 }}>
                        {containerOf(p.addr)}
                      </span>
                    </td>
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
            {q.placed[0] ? (
              <>
                <p className="muted mono" style={{ margin: 0 }}>{q.placed[0].node}</p>
                <p className="muted mono" style={{ margin: "4px 0 0", fontSize: 12 }}>
                  {q.placed[0].addr}
                </p>
              </>
            ) : (
              <p className="muted" style={{ margin: 0, fontSize: 13 }}>
                Its owner is not registered, so the queue is unavailable until that
                machine comes back.
              </p>
            )}
          </div>
        ))}
      </Section>

      <h2 style={{ marginTop: 32, marginBottom: 4 }}>Nodes</h2>
      <p className="muted" style={{ margin: "0 0 14px", maxWidth: 720, fontSize: 13 }}>
        The node id survives a restart because it lives with the node&rsquo;s data; the
        container id does not. Stop a container to watch what its queues do &mdash; a
        single-node queue waits for it, a distributed one keeps serving the rest.
      </p>
      <div className="card flush">
        <table>
          <thead>
            <tr>
              <th>Node</th>
              <th>Container</th>
              <th className="num">Queues</th>
              <th>State</th>
              <th>Take it down</th>
            </tr>
          </thead>
          <tbody>
            {loading && (
              <tr>
                <td colSpan={5}><Skeleton h={14} /></td>
              </tr>
            )}
            {!loading && nodes.length === 0 && (
              <tr>
                <td colSpan={5}>
                  <Empty title="No nodes registered">
                    Start one with <span className="mono">docker compose up -d node</span>.
                  </Empty>
                </td>
              </tr>
            )}
            {nodes.map((n) => {
              const container = containerOf(n.addr);
              return (
                <tr key={n.id}>
                  <td className="mono">
                    <Link href={`/cluster/node?id=${encodeURIComponent(n.id)}`}>{n.id}</Link>
                  </td>
                  <td className="mono muted">{container}</td>
                  <td className="num">{n.queues.length}</td>
                  <td><span className="tag ok">live</span></td>
                  <td>
                    <CopyButton
                      value={`docker stop ${container}`}
                      label="docker stop"
                      title={`Copy: docker stop ${container}`}
                    />
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      <RegistryView data={registry} />
    </>
  );
}

// etcd holds one key per node under a lease. Showing it raw is worth a screen
// of its own: the member list above is what the gateway believes, and this is
// what is actually stored, so the two disagreeing is visible rather than
// something to guess at.
function RegistryView({ data }: { data: Registry | null }) {
  const [open, setOpen] = useState(false);
  const entries = data?.entries ?? [];
  const stale = data ? data.watching !== entries.length : false;

  return (
    <>
      <div className="row" style={{ marginTop: 32, alignItems: "baseline", gap: 10 }}>
        <h2 style={{ margin: 0 }}>Registry</h2>
        <button className="ghost" onClick={() => setOpen(!open)}>
          {open ? "hide" : `show ${entries.length} key${entries.length === 1 ? "" : "s"}`}
        </button>
        {stale && (
          <span className="tag high" title="The gateway's member list does not match what etcd holds">
            gateway sees {data!.watching}
          </span>
        )}
      </div>
      <p className="muted" style={{ margin: "4px 0 14px", maxWidth: 720, fontSize: 13 }}>
        What etcd actually holds, under <span className="mono">/ryuk/</span>. Each key
        is kept alive by a lease; when a node stops renewing, the key expires and
        the node leaves the cluster on its own.
      </p>

      {open && (
        <div className="card flush">
          <table>
            <thead>
              <tr>
                <th>Key</th>
                <th>Value</th>
                <th className="num">Lease</th>
                <th className="num">Expires in</th>
              </tr>
            </thead>
            <tbody>
              {entries.length === 0 && (
                <tr>
                  <td colSpan={4}>
                    <Empty title="Nothing registered">
                      A node writes its key here when it starts.
                    </Empty>
                  </td>
                </tr>
              )}
              {entries.map((e) => (
                <tr key={e.key}>
                  <td className="mono">{e.key}</td>
                  <td className="mono muted" style={{ fontSize: 12 }}>{e.value}</td>
                  <td className="num mono muted">{e.lease || "—"}</td>
                  <td className="num muted">
                    {e.ttlSeconds ? `${e.ttlSeconds}s` : "no lease"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
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

const SHADES = ["var(--accent)", "var(--low)", "var(--ok)", "var(--medium)", "var(--high)"];

function SlotBar({
  placed, total, lost = 0,
}: {
  placed: { node: string; slots: number }[]; total: number; lost?: number;
}) {
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
        {lost > 0 && (
          <div
            style={{ width: `${(lost / total) * 100}%`, background: "var(--high)", opacity: 0.35 }}
            title={`${lost} slots on machines that are gone`}
          />
        )}
        {held + lost < total && (
          <div style={{ flex: 1, background: "var(--border)" }} title={`${total - held - lost} slots not yet used`} />
        )}
      </div>
      <p className="muted" style={{ margin: "8px 0 0", fontSize: 12 }}>
        {held} of {total} slots placed
        {lost > 0 && ` · ${lost} on machines that are gone`}
      </p>
    </>
  );
}
