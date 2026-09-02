"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { useOrg } from "@/lib/org-context";
import { api, QueueSummary } from "@/lib/api";
import { Busy } from "@/lib/ui";

export default function NewQueue() {
  const { org } = useOrg();
  const router = useRouter();
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");
  const [f, setF] = useState({
    name: "",
    visibilityTimeout: "30s",
    maxRetries: 3,
    defaultTtl: "1h",
    starvationThreshold: "5m",
    starvationReserve: 0.2,
    deadLetterQueue: "",
    distributed: false,
  });

  // The dead-letter queue has to already exist, so it is picked rather than typed.
  const [existing, setExisting] = useState<QueueSummary[]>([]);
  useEffect(() => {
    api.listQueues(org.token).then(setExisting).catch(() => setExisting([]));
  }, [org.token]);

  const set = (k: string, v: unknown) => setF({ ...f, [k]: v });

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setBusy(true);
    setErr("");
    try {
      await api.createQueue(org.token, { ...f, maxRetries: Number(f.maxRetries) });
      router.push(`/queue?name=${encodeURIComponent(f.name)}`);
    } catch (e: any) {
      setErr(e.message);
      setBusy(false);
    }
  };

  return (
    <>
      <div className="page-head">
        <div>
          <h1>New queue</h1>
          <p className="sub">{org.label}</p>
        </div>
      </div>

      {err && <p className="err">{err}</p>}

      <form onSubmit={submit} className="card" style={{ maxWidth: 660 }}>
        <label>
          <span>Name</span>
          <input
            required value={f.name} placeholder="orders"
            onChange={(e) => set("name", e.target.value)}
          />
          <p className="field-hint">Letters, digits, hyphen and underscore.</p>
        </label>

        <label>
          <span>Placement</span>
          <select
            value={String(f.distributed)}
            onChange={(e) => set("distributed", e.target.value === "true")}
          >
            <option value="false">Single node — exact priority, FIFO and counts</option>
            <option value="true">Distributed — spreads over up to 64 machines</option>
          </select>
          <p className="field-hint">
            {f.distributed
              ? "Slots are placed independently, so losing a node costs only its slots. Priority order and counts become approximate across machines."
              : "All 16 slots live on one machine, which handles a few hundred thousand messages a second. If that machine goes down the queue waits for it."}
          </p>
        </label>

        <div className="grid cols-2">
          <label>
            <span>Visibility timeout</span>
            <input value={f.visibilityTimeout} onChange={(e) => set("visibilityTimeout", e.target.value)} />
          </label>
          <label>
            <span>Max retries before dead-letter</span>
            <input
              type="number" min={0} value={f.maxRetries}
              onChange={(e) => set("maxRetries", e.target.value)}
            />
          </label>
          <label>
            <span>Default TTL</span>
            <input value={f.defaultTtl} onChange={(e) => set("defaultTtl", e.target.value)} />
          </label>
          <label>
            <span>Starvation threshold</span>
            <input value={f.starvationThreshold} onChange={(e) => set("starvationThreshold", e.target.value)} />
          </label>
          <label>
            <span>Starvation reserve</span>
            <input
              type="number" step="0.05" min={0} max={0.95} value={f.starvationReserve}
              onChange={(e) => set("starvationReserve", Number(e.target.value))}
            />
          </label>
          <label>
            <span>Dead-letter queue</span>
            <select value={f.deadLetterQueue} onChange={(e) => set("deadLetterQueue", e.target.value)}>
              <option value="">None</option>
              {existing
                .filter((q) => q.name !== f.name)
                .map((q) => (
                  <option key={q.name} value={q.name}>{q.name}</option>
                ))}
            </select>
          </label>
        </div>

        <p className="note">
          A message that has waited longer than the starvation threshold can use a
          reserved share of deliveries even when higher priorities are queued, so
          low-priority work still moves. The threshold has to stay below the TTL, or
          messages expire while waiting for their turn.
        </p>

        <div className="row" style={{ marginTop: 16 }}>
          <button className="primary" disabled={busy}>
            {busy ? <Busy label="Creating…" /> : "Create queue"}
          </button>
          <button type="button" className="ghost" onClick={() => router.push("/")}>
            Cancel
          </button>
        </div>
      </form>
    </>
  );
}
