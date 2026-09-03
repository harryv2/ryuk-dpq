"use client";

import { Suspense, useCallback, useEffect, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import Link from "next/link";
import { useOrg } from "@/lib/org-context";
import { api, decodePayload, priorityLabel, Message, QueueStats } from "@/lib/api";
import { Busy, Empty, StatCard, age } from "@/lib/ui";

type Tab = "send" | "poll";

type Sent = { id: string; payload: string; priority: number; groupId: string; at: number };

function QueueDetailInner() {
  const { org } = useOrg();
  const router = useRouter();
  const name = useSearchParams().get("name") ?? "";

  const [tab, setTab] = useState<Tab>("send");
  const [stats, setStats] = useState<QueueStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [held, setHeld] = useState<Message[]>([]);
  const [err, setErr] = useState("");

  const refresh = useCallback(async () => {
    try {
      setStats(await api.stats(org.token, name));
      setErr("");
    } catch (e: any) {
      setErr(e.message);
    } finally {
      setLoading(false);
    }
  }, [org.token, name]);

  useEffect(() => {
    setLoading(true);
    refresh();
    const t = setInterval(refresh, 2000);
    return () => clearInterval(t);
  }, [refresh]);

  const remove = async () => {
    if (!confirm(`Delete ${name}? Its messages are discarded.`)) return;
    try {
      await api.deleteQueue(org.token, name);
      router.push("/");
    } catch (e: any) {
      setErr(e.message);
    }
  };

  return (
    <>
      <div className="page-head">
        <div>
          <h1>{name}</h1>
          <p className="sub">
            {stats?.distributed ? (
              <span className="tag dist">distributed</span>
            ) : (
              <span className="tag plain">single node</span>
            )}
            {stats?.ownerNode && (
              <>
                {" "}owner node <span className="mono">{stats.ownerNode}</span>
              </>
            )}
            {stats && !stats.exact && <> · counts are a point-in-time sum</>}
          </p>
        </div>
        <div className="row">
          <Link href={`/queue/metrics?name=${encodeURIComponent(name)}`}>
            <button>Metrics</button>
          </Link>
          <button className="danger" onClick={remove}>Delete</button>
        </div>
      </div>

      {err && <p className="err">{err}</p>}

      <div className="grid cols-4" style={{ marginBottom: 24 }}>
        <StatCard label="Ready" value={(stats?.messages ?? 0).toLocaleString()} loading={loading} />
        <StatCard label="In flight" value={(stats?.inFlight ?? 0).toLocaleString()} loading={loading} />
        <StatCard label="Delayed" value={(stats?.delayed ?? 0).toLocaleString()} loading={loading} />
        <StatCard label="Dead-lettered" value={(stats?.deadLettered ?? 0).toLocaleString()} loading={loading} />
      </div>

      <div className="tabs">
        <button data-active={tab === "send"} onClick={() => setTab("send")}>Send</button>
        <button data-active={tab === "poll"} onClick={() => setTab("poll")}>
          Poll
          {held.length > 0 && <span className="count">{held.length}</span>}
        </button>
      </div>

      <div className="fade-in" key={tab}>
        {tab === "send" ? (
          <SendPanel queue={name} token={org.token} onSent={refresh} onError={setErr} />
        ) : (
          <PollPanel
            queue={name} token={org.token} held={held} setHeld={setHeld}
            onChange={refresh} onError={setErr}
          />
        )}
      </div>
    </>
  );
}

function SendPanel({
  queue, token, onSent, onError,
}: {
  queue: string; token: string; onSent: () => void; onError: (s: string) => void;
}) {
  const [f, setF] = useState({
    payload: "", priority: "HIGH", custom: 90, groupId: "", count: 1, ttl: "", deliverAfter: "",
  });

  // The API takes 0-100 as well as the three names; HIGH, MEDIUM and LOW are
  // just 75, 50 and 25. "Custom" exposes the scale the names are shorthand for.
  const priorityValue = () => (f.priority === "CUSTOM" ? f.custom : f.priority);

  // What the names stand for, so the feed can colour and sort a sent message
  // whichever way it was chosen.
  const NAMED: Record<string, number> = { HIGH: 75, MEDIUM: 50, LOW: 25 };
  const priorityNumber = () => (f.priority === "CUSTOM" ? f.custom : NAMED[f.priority]);
  const [busy, setBusy] = useState(false);
  const [recent, setRecent] = useState<Sent[]>([]);

  const set = (k: string, v: unknown) => setF({ ...f, [k]: v });

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setBusy(true);
    onError("");
    try {
      const just: Sent[] = [];
      for (let i = 0; i < f.count; i++) {
        const payload = f.count > 1 ? `${f.payload} #${i + 1}` : f.payload;
        const res = await api.enqueue(token, queue, {
          payload,
          priority: priorityValue(),
          groupId: f.groupId || undefined,
          ttl: f.ttl || undefined,
          deliverAfter: f.deliverAfter || undefined,
        });
        just.push({
          id: res?.messageId ?? "",
          payload,
          priority: priorityNumber(),
          groupId: f.groupId,
          at: Date.now(),
        });
      }
      setRecent((r) => [...just.reverse(), ...r].slice(0, 12));
      setF({ ...f, payload: "" });
      onSent();
    } catch (e: any) {
      onError(e.message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="split">
      <form className="card" onSubmit={submit}>
        <label>
          <span>Payload</span>
          <textarea
            required rows={3} value={f.payload}
            onChange={(e) => set("payload", e.target.value)}
            placeholder="ship order 9981"
          />
        </label>

        <div className="fields">
          <label>
            <span>Priority</span>
            <select value={f.priority} onChange={(e) => set("priority", e.target.value)}>
              <option value="HIGH">HIGH &middot; 75</option>
              <option value="MEDIUM">MEDIUM &middot; 50</option>
              <option value="LOW">LOW &middot; 25</option>
              <option value="CUSTOM">Custom&hellip;</option>
            </select>
          </label>
          {f.priority === "CUSTOM" && (
            <label>
              <span>Value (0&ndash;100)</span>
              <input
                type="number" min={0} max={100} value={f.custom}
                onChange={(e) =>
                  set("custom", Math.min(100, Math.max(0, Number(e.target.value) || 0)))
                }
              />
            </label>
          )}
          <label>
            <span>Group</span>
            <input
              value={f.groupId}
              onChange={(e) => set("groupId", e.target.value)}
              placeholder="user-123"
            />
          </label>
          <label>
            <span>Copies</span>
            <input
              type="number" min={1} max={100} value={f.count}
              onChange={(e) => set("count", Math.max(1, Number(e.target.value) || 1))}
            />
          </label>
          <label>
            <span>Deliver after</span>
            <input
              value={f.deliverAfter}
              onChange={(e) => set("deliverAfter", e.target.value)}
              placeholder="e.g. 30s"
            />
          </label>
        </div>

        <p className="field-hint" style={{ marginTop: -4 }}>
          Messages sharing a group are delivered one at a time, in order. Leave it
          empty and the queue is free to hand out messages in parallel. A message
          with a delivery time is held back and counted as delayed until it
          arrives. Priority is a number from 0 to 100 and is ordered exactly:
          91 is served before 90. The three names are shorthand, and metrics
          group the scale into three bands.
        </p>

        <div className="row" style={{ marginTop: 16 }}>
          <button className="primary" disabled={busy}>
            {busy ? <Busy label="Sending…" /> : `Send${f.count > 1 ? ` ${f.count}` : ""}`}
          </button>
        </div>
      </form>

      <div className="card flush">
        <div className="card-head" style={{ padding: "16px 16px 0", marginBottom: 12 }}>
          <h2>Just sent</h2>
          {recent.length > 0 && (
            <button className="ghost sm" onClick={() => setRecent([])}>Clear</button>
          )}
        </div>
        {recent.length === 0 ? (
          <Empty title="Nothing sent yet">
            What you queue from here shows up in this list, newest first.
          </Empty>
        ) : (
          <ul className="feed">
            {recent.map((m) => (
              <li key={m.id + m.at} className="fade-in">
                <span className={"tag " + priorityLabel(m.priority).toLowerCase()}>
                  {priorityLabel(m.priority)}
                </span>
                <span className="muted mono" style={{ fontSize: 11 }}>{m.priority}</span>
                <span className="mono body">{m.payload}</span>
                {m.groupId && <span className="mono muted group">{m.groupId}</span>}
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}

function PollPanel({
  queue, token, held, setHeld, onChange, onError,
}: {
  queue: string; token: string;
  held: Message[]; setHeld: (f: (h: Message[]) => Message[]) => void;
  onChange: () => void; onError: (s: string) => void;
}) {
  const [count, setCount] = useState(5);
  const [busy, setBusy] = useState(false);
  const [last, setLast] = useState<number | null>(null);
  const [working, setWorking] = useState<string>("");

  const poll = async () => {
    setBusy(true);
    onError("");
    try {
      const msgs = await api.dequeue(token, queue, count);
      setHeld((h) => [...h, ...msgs]);
      setLast(msgs.length);
      onChange();
    } catch (e: any) {
      onError(e.message);
    } finally {
      setBusy(false);
    }
  };

  const finish = async (m: Message, kind: "ack" | "nack") => {
    setWorking(m.receipt);
    try {
      await (kind === "ack" ? api.ack : api.nack)(token, queue, m.receipt);
      setHeld((h) => h.filter((x) => x.receipt !== m.receipt));
      onChange();
    } catch (e: any) {
      onError(e.message);
    } finally {
      setWorking("");
    }
  };

  const finishAll = async (kind: "ack" | "nack") => {
    setBusy(true);
    for (const m of held) {
      try {
        await (kind === "ack" ? api.ack : api.nack)(token, queue, m.receipt);
      } catch (e: any) {
        onError(e.message);
      }
    }
    setHeld(() => []);
    onChange();
    setBusy(false);
  };

  return (
    <div className="card flush">
      <div style={{ padding: 18, borderBottom: held.length ? "1px solid var(--border)" : "none" }}>
        <div className="row" style={{ justifyContent: "space-between" }}>
          <div className="row">
            <label style={{ margin: 0 }}>
              <span>Messages to take</span>
              <input
                type="number" min={1} max={10} value={count} style={{ width: 90 }}
                onChange={(e) => setCount(Math.min(10, Math.max(1, Number(e.target.value) || 1)))}
              />
            </label>
            <button className="primary" onClick={poll} disabled={busy} style={{ marginTop: 20 }}>
              {busy ? <Busy label="Polling…" /> : "Poll"}
            </button>
            {last !== null && !busy && (
              <span className="muted fade-in" key={String(last) + held.length} style={{ marginTop: 20 }}>
                took {last}
              </span>
            )}
          </div>
          {held.length > 0 && (
            <div className="row" style={{ marginTop: 20 }}>
              <button className="sm" onClick={() => finishAll("nack")} disabled={busy}>Nack all</button>
              <button className="sm" onClick={() => finishAll("ack")} disabled={busy}>Ack all</button>
            </div>
          )}
        </div>
        <p className="field-hint" style={{ marginTop: 10 }}>
          The server caps a single poll at 10. What you take stays invisible to other
          consumers until you acknowledge it or the visibility timeout expires.
        </p>
      </div>

      {held.length === 0 ? (
        <Empty title="Nothing held yet">
          Poll to take messages. They appear here until you ack or nack them.
        </Empty>
      ) : (
        <table>
          <thead>
            <tr>
              <th>Payload</th>
              <th>Priority</th>
              <th>Group</th>
              <th className="num">Try</th>
              <th className="num">Waited</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {held.map((m) => (
              <tr key={m.receipt} className="fade-in">
                <td className="mono">{decodePayload(m.payload).slice(0, 48)}</td>
                <td>
                  <span className={"tag " + priorityLabel(m.priority).toLowerCase()}>
                    {priorityLabel(m.priority)}
                  </span>
                  <span className="muted mono" style={{ marginLeft: 8, fontSize: 11 }}>
                    {m.priority}
                  </span>
                </td>
                <td className="mono muted">{m.groupId || "—"}</td>
                <td className="num">{m.attempts}</td>
                <td className="num muted">
                  {age((Date.now() - new Date(m.enqueuedAt).getTime()) / 1000)}
                </td>
                <td>
                  <div className="row" style={{ justifyContent: "flex-end", flexWrap: "nowrap" }}>
                    <button className="sm" disabled={working === m.receipt} onClick={() => finish(m, "nack")}>
                      Nack
                    </button>
                    <button className="sm primary" disabled={working === m.receipt} onClick={() => finish(m, "ack")}>
                      {working === m.receipt ? <span className="spinner" /> : "Ack"}
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

export default function QueueDetail() {
  return (
    <Suspense fallback={<p className="muted">Loading…</p>}>
      <QueueDetailInner />
    </Suspense>
  );
}
