const BASE =
  process.env.NEXT_PUBLIC_RYUK_API ??
  (typeof window !== "undefined" ? window.location.origin : "http://localhost:8090");

export type QueueSummary = {
  name: string;
  distributed: boolean;
  placementWidth?: number;
  state: string;
  ownerNode?: string;
  messages: number;
  inFlight: number;
  oldestMessageAgeSeconds: number;
  settings: QueueSettings;
};

// Durations arrive as Go nanoseconds.
export type QueueSettings = {
  visibilityTimeout: number;
  maxRetries: number;
  defaultTtl: number;
  starvationThreshold: number;
  starvationReserve: number;
  maxDepth: number;
  deadLetterQueue?: string;
  placementWidth?: number;
};

export type QueueStats = {
  queue: string;
  messages: number;
  inFlight: number;
  delayed: number;
  byPriority: { low: number; medium: number; high: number };
  oldestMessageAgeSeconds: number;
  deadLettered: number;
  enqueued: number;
  acked: number;
  expired: number;
  redelivered: number;
  starvationEscapes: number;
  enqueueRate: number;
  ackRate: number;
  ownerNode?: string;
  distributed: boolean;
  exact: boolean;
  unavailableSlots?: number;
  asOf: string;
};

export type SeriesPoint = { at: string; value: number };
export type Series = { name: string; points: SeriesPoint[] };

export type Timeseries = {
  // false when no monitoring system is configured; the page then samples the
  // live endpoint itself, which only covers the time it has been open.
  available: boolean;
  range: string;
  ready: Series[];
  inFlight: Series[];
  oldestAge: Series[];
  rates: Series[];
};

export type ClusterPlacement = {
  queue: string;
  org: string;
  distributed: boolean;
  slots: number;
  totalSlots: number;
};

export type ClusterNode = { id: string; addr: string; queues: ClusterPlacement[] };

export type Cluster = { nodes: ClusterNode[]; unavailable: ClusterPlacement[] };

export type RegistryEntry = {
  key: string;
  value: string;
  lease?: string;
  ttlSeconds?: number;
  version: number;
};

export type Registry = { entries: RegistryEntry[]; watching: number };

export type NodeQueueDetail = {
  queue: string;
  distributed: boolean;
  slots: number;
  totalSlots: number;
  ready: number;
  byPriority?: { low: number; medium: number; high: number };
  inFlight: number;
  delayed: number;
  oldestMessageAgeSeconds: number;
  enqueued: number;
  acked: number;
  expired: number;
  redelivered: number;
  deadLettered: number;
  starvationEscapes: number;
};

export type NodeDetail = {
  id: string;
  addr: string;
  live: boolean;
  slots: number;
  ready: number;
  inFlight: number;
  delayed: number;
  oldestMessageAgeSeconds: number;
  queues: NodeQueueDetail[];
};

export type Message = {
  messageId: string;
  payload: string;
  priority: number;
  groupId?: string;
  attempts: number;
  enqueuedAt: string;
  receipt: string;
};

async function call<T>(token: string, path: string, init?: RequestInit): Promise<T | null> {
  const res = await fetch(BASE + path, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
      ...(init?.headers ?? {}),
    },
    cache: "no-store",
  });
  if (res.status === 204) return null;
  const body = await res.text();
  if (!res.ok) {
    let msg = body;
    try {
      msg = JSON.parse(body).error ?? body;
    } catch {}
    throw new Error(msg || `HTTP ${res.status}`);
  }
  return body ? (JSON.parse(body) as T) : null;
}

export const api = {
  listQueues: (t: string) =>
    call<{ queues: QueueSummary[] }>(t, "/v1/queues").then((r) => r?.queues ?? []),

  createQueue: (t: string, body: Record<string, unknown>) =>
    call<{ name: string; created: boolean; ownerNode?: string }>(t, "/v1/queues", {
      method: "POST",
      body: JSON.stringify(body),
    }),

  updateQueue: (t: string, name: string, body: Record<string, unknown>) =>
    call<QueueSummary>(t, `/v1/queues/${encodeURIComponent(name)}`, {
      method: "PATCH",
      body: JSON.stringify(body),
    }),

  deleteQueue: (t: string, name: string) =>
    call<unknown>(t, `/v1/queues/${encodeURIComponent(name)}`, { method: "DELETE" }),

  stats: (t: string, name: string) =>
    call<QueueStats>(t, `/v1/queues/${encodeURIComponent(name)}/stats`),

  metrics: (t: string) =>
    call<{ queues: QueueStats[] }>(t, "/v1/metrics").then((r) => r?.queues ?? []),

  timeseries: (t: string, name: string, window: string) =>
    call<Timeseries>(t, `/v1/queues/${encodeURIComponent(name)}/timeseries?window=${window}`),

  cluster: (t: string) =>
    call<Cluster>(t, "/v1/cluster").then((r) => ({
      nodes: r?.nodes ?? [],
      unavailable: r?.unavailable ?? [],
    })),

  registry: (t: string) =>
    call<Registry>(t, "/v1/cluster/registry").then((r) => ({
      entries: r?.entries ?? [],
      watching: r?.watching ?? 0,
    })),

  node: (t: string, id: string) =>
    call<NodeDetail>(t, `/v1/cluster/nodes/${encodeURIComponent(id)}`),

  enqueue: (t: string, name: string, body: Record<string, unknown>) =>
    call<{ messageId: string }>(t, `/v1/queues/${encodeURIComponent(name)}/messages`, {
      method: "POST",
      body: JSON.stringify(body),
    }),

  dequeue: (t: string, name: string, max = 1) =>
    call<{ messages: Message[] }>(t, `/v1/queues/${encodeURIComponent(name)}/messages/dequeue`, {
      method: "POST",
      body: JSON.stringify({ maxMessages: max }),
    }).then((r) => r?.messages ?? []),

  ack: (t: string, name: string, receipt: string) =>
    call<unknown>(t, `/v1/queues/${encodeURIComponent(name)}/messages/ack`, {
      method: "POST",
      body: JSON.stringify({ receipt }),
    }),

  nack: (t: string, name: string, receipt: string) =>
    call<unknown>(t, `/v1/queues/${encodeURIComponent(name)}/messages/nack`, {
      method: "POST",
      body: JSON.stringify({ receipt }),
    }),
};

export function decodePayload(b64: string): string {
  try {
    return atob(b64);
  } catch {
    return b64;
  }
}

// The band a value falls in, matching the engine's bucketOf: metrics report
// three, but the value itself is ordered exactly.
export const PRIORITY_BANDS = [
  { key: "high", label: "high", from: 67, to: 100 },
  { key: "medium", label: "medium", from: 34, to: 66 },
  { key: "low", label: "low", from: 0, to: 33 },
] as const;

export function priorityLabel(p: number): string {
  if (p <= 33) return "LOW";
  if (p <= 66) return "MEDIUM";
  return "HIGH";
}
