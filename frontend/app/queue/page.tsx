"use client";

import { Suspense, useCallback, useEffect, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import Link from "next/link";
import { useOrg } from "@/lib/org-context";
import { api, decodePayload, priorityLabel, Message, QueueStats } from "@/lib/api";
import { Busy, Empty, StatCard, age } from "@/lib/ui";

type Tab = "send" | "poll";

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
                {" "}owner <span className="mono">{stats.ownerNode}</span>
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
  const [f, setF] = useState({ payload: "", priority: "HIGH", groupId: "", count: 1, ttl: "" });
  const [busy, setBusy] = useState(false);
  const [sent, setSent] = useState(0);

  const set = (k: string, v: unknown) => setF({ ...f, [k]: v });

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setBusy(true);
    onError("");
    try {
      for (let i = 0; i < f.count; i++) {
        await api.enqueue(token, queue, {
          payload: f.count > 1 ? `${f.payload} #${i + 1}` : f.payload,
          priority: f.priority,
          groupId: f.groupId || undefined,
          ttl: f.ttl || undefined,
        });
      }
      setSent(f.count);
      setF({ ...f, payload: "" });
      onSent();
    } catch (e: any) {
      onError(e.message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="card" style={{ maxWidth: 640 }}>
      <form onSubmit={submit}>
        <label>
          <span>Payload</span>
          <textarea
            required rows={3} value={f.payload}
            onChange={(e) => set("payload", e.target.value)}
            placeholder="ship order 9981"
          />
        </label>

        <div className="grid cols-2">
          <label>
            <span>Priority</span>
            <select value={f.priority} onChange={(e) => set("priority", e.target.value)}>
              <option>HIGH</option>
              <option>MEDIUM</option>
              <option>LOW</option>
            </select>
          </label>
          <label>
            <span>Copies to send</span>
            <input
              type="number" min={1} max={100} value={f.count}
              onChange={(e) => set("count", Math.max(1, Number(e.target.value) || 1))}
            />
          </label>
        </div>

        <label>
          <span>Group</span>
          <input
            value={f.groupId}
            onChange={(e) => set("groupId", e.target.value)}
            placeholder="user-123"
          />
          <p className="field-hint">
            Messages sharing a group are delivered one at a time, in order. Leave it
            empty and the queue is free to hand out messages in parallel.
          </p>
        </label>

        <div className="row">
          <button className="primary" disabled={busy}>
            {busy ? <Busy label="Sending…" /> : `Send${f.count > 1 ? ` ${f.count}` : ""}`}
          </button>
          {sent > 0 && !busy && (
            <span className="muted fade-in" key={sent}>
              {sent} message{sent > 1 ? "s" : ""} queued
            </span>
          )}
        </div>
      </form>
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
