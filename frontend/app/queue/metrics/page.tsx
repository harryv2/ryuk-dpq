"use client";

import { Suspense, useEffect, useRef, useState } from "react";
import { useSearchParams } from "next/navigation";
import Link from "next/link";
import { useOrg } from "@/lib/org-context";
import { api, QueueStats } from "@/lib/api";
import { StatCard, age } from "@/lib/ui";

type Point = {
  t: number;
  low: number; medium: number; high: number;
  inFlight: number; ageSeconds: number;
};

const SAMPLE_MS = 2000;
const WINDOW = 60;

function MetricsInner() {
  const { org } = useOrg();
  const name = useSearchParams().get("name") ?? "";
  const [stats, setStats] = useState<QueueStats | null>(null);
  const [loading, setLoading] = useState(true);
  const history = useRef<Point[]>([]);
  const [, tick] = useState(0);

  useEffect(() => {
    history.current = [];
    let alive = true;
    const load = async () => {
      const s = await api.stats(org.token, name).catch(() => null);
      if (!s || !alive) return;
      setStats(s);
      setLoading(false);
      history.current = [
        ...history.current,
        {
          t: Date.now(),
          low: s.byPriority.low, medium: s.byPriority.medium, high: s.byPriority.high,
          inFlight: s.inFlight, ageSeconds: s.oldestMessageAgeSeconds,
        },
      ].slice(-WINDOW); // a rolling window, kept here rather than in a database
      tick((n) => n + 1);
    };
    load();
    const t = setInterval(load, SAMPLE_MS);
    return () => {
      alive = false;
      clearInterval(t);
    };
  }, [org.token, name]);

  const h = history.current;
  const span = h.length > 1 ? ((h[h.length - 1].t - h[0].t) / 1000).toFixed(0) : "0";

  return (
    <>
      <div className="page-head">
        <div>
          <h1>{name}</h1>
          <p className="sub">
            <span className="live-dot" /> live · {h.length} samples over {span}s
            {stats && !stats.exact && <> · summed across machines</>}
          </p>
        </div>
        <Link href={`/queue?name=${encodeURIComponent(name)}`}>
          <button>Back to queue</button>
        </Link>
      </div>

      <div className="grid cols-4" style={{ marginBottom: 16 }}>
        <StatCard label="Enqueued" value={(stats?.enqueued ?? 0).toLocaleString()} loading={loading} />
        <StatCard label="Acknowledged" value={(stats?.acked ?? 0).toLocaleString()} loading={loading} />
        <StatCard label="Redelivered" value={(stats?.redelivered ?? 0).toLocaleString()} loading={loading} />
        <StatCard label="Expired" value={(stats?.expired ?? 0).toLocaleString()} loading={loading} />
      </div>

      <div className="card" style={{ marginBottom: 16 }}>
        <div className="card-head">
          <h2>Ready messages by priority</h2>
          <span className="muted" style={{ fontSize: 12 }}>
            {((stats?.byPriority.high ?? 0) + (stats?.byPriority.medium ?? 0) + (stats?.byPriority.low ?? 0)).toLocaleString()} ready now
          </span>
        </div>
        <StackedArea data={h} />
        <div className="legend">
          <Legend color="var(--high)" label="high" value={stats?.byPriority.high} />
          <Legend color="var(--medium)" label="medium" value={stats?.byPriority.medium} />
          <Legend color="var(--low)" label="low" value={stats?.byPriority.low} />
        </div>
      </div>

      <div className="grid cols-2" style={{ marginBottom: 16 }}>
        <div className="card">
          <div className="card-head">
            <h2>Oldest message age</h2>
            <span className="muted" style={{ fontSize: 12 }}>{age(stats?.oldestMessageAgeSeconds ?? 0)}</span>
          </div>
          <LineArea
            data={h.map((p) => p.ageSeconds)}
            color="var(--medium)"
            format={(v) => age(v)}
          />
        </div>
        <div className="card">
          <div className="card-head">
            <h2>In flight</h2>
            <span className="muted" style={{ fontSize: 12 }}>{stats?.inFlight ?? 0} held</span>
          </div>
          <LineArea data={h.map((p) => p.inFlight)} color="var(--accent)" />
        </div>
      </div>

      <div className="card">
        <div className="card-head">
          <h2>Starvation escapes</h2>
        </div>
        <div className="value" style={{ fontSize: 28, fontWeight: 650 }}>
          {(stats?.starvationEscapes ?? 0).toLocaleString()}
        </div>
        <p className="muted" style={{ marginBottom: 0, maxWidth: 720 }}>
          Deliveries that used the share reserved for work that had waited too long. If
          this is climbing, consumers cannot keep up with urgent work and low-priority
          work is only moving because of the safety net.
        </p>
      </div>
    </>
  );
}

function Legend({ color, label, value }: { color: string; label: string; value?: number }) {
  return (
    <span className="item">
      <span className="swatch" style={{ background: color }} />
      {label}
      {value !== undefined && <strong style={{ fontVariantNumeric: "tabular-nums" }}>{value}</strong>}
    </span>
  );
}

/* ---------- charts ----------
   One coordinate system for both. The plot area is inset so axis labels have
   somewhere to live, and the aspect ratio is left alone -- stretching it is what
   turned the old charts into a flat bar. */

const W = 720, H = 190;
const PAD = { top: 12, right: 12, bottom: 22, left: 42 };
const PW = W - PAD.left - PAD.right;
const PH = H - PAD.top - PAD.bottom;

// A round number at or above the peak, so the line has headroom and the ticks
// read as 0 / 25 / 50 rather than 0 / 23.5 / 47.
function niceMax(peak: number): number {
  if (peak <= 0) return 4;
  const pow = Math.pow(10, Math.floor(Math.log10(peak)));
  for (const step of [1, 2, 2.5, 5, 10]) {
    const cap = step * pow;
    if (cap >= peak) return cap;
  }
  return 10 * pow;
}

function axes(max: number, format: (v: number) => string) {
  return [0, 0.25, 0.5, 0.75, 1].map((f) => {
    const y = PAD.top + PH - f * PH;
    return (
      <g key={f}>
        <line className="grid-line" x1={PAD.left} x2={PAD.left + PW} y1={y} y2={y} />
        <text className="axis-text" x={PAD.left - 8} y={y + 3.5} textAnchor="end">
          {format(max * f)}
        </text>
      </g>
    );
  });
}

function Collecting() {
  return (
    <div style={{ height: H, display: "flex", alignItems: "center", justifyContent: "center", gap: 8 }}>
      <span className="spinner muted" />
      <span className="muted">Collecting samples…</span>
    </div>
  );
}

function StackedArea({ data }: { data: Point[] }) {
  if (data.length < 2) return <Collecting />;

  const max = niceMax(Math.max(...data.map((p) => p.low + p.medium + p.high)));
  const x = (i: number) => PAD.left + (i / (data.length - 1)) * PW;
  const y = (v: number) => PAD.top + PH - (v / max) * PH;

  // Drawn from the top down so each band sits on the one below it.
  const layers = [
    { key: "high", color: "var(--high)", pick: (p: Point) => p.high, base: (p: Point) => p.low + p.medium },
    { key: "medium", color: "var(--medium)", pick: (p: Point) => p.medium, base: (p: Point) => p.low },
    { key: "low", color: "var(--low)", pick: (p: Point) => p.low, base: () => 0 },
  ];

  return (
    <svg className="chart" viewBox={`0 0 ${W} ${H}`} height={H} role="img">
      {axes(max, (v) => String(Math.round(v)))}
      {layers.map((l) => {
        const top = data.map((p, i) => `${x(i)},${y(l.base(p) + l.pick(p))}`).join(" ");
        const bottom = data.map((p, i) => `${x(i)},${y(l.base(p))}`).reverse().join(" ");
        return (
          <g key={l.key}>
            <polygon points={`${top} ${bottom}`} fill={l.color} opacity={0.55} />
            <polyline points={top} fill="none" stroke={l.color} strokeWidth={1.75} strokeLinejoin="round" />
          </g>
        );
      })}
      <XAxis count={data.length} />
    </svg>
  );
}

function LineArea({
  data, color, format = (v: number) => String(Math.round(v)),
}: {
  data: number[]; color: string; format?: (v: number) => string;
}) {
  if (data.length < 2) return <Collecting />;

  const max = niceMax(Math.max(...data));
  const x = (i: number) => PAD.left + (i / (data.length - 1)) * PW;
  const y = (v: number) => PAD.top + PH - (v / max) * PH;

  const line = data.map((v, i) => `${x(i)},${y(v)}`).join(" ");
  const fill = `${PAD.left},${PAD.top + PH} ${line} ${PAD.left + PW},${PAD.top + PH}`;
  const lastX = x(data.length - 1);
  const lastY = y(data[data.length - 1]);
  const id = `fill-${color.replace(/[^a-z]/gi, "")}`;

  return (
    <svg className="chart" viewBox={`0 0 ${W} ${H}`} height={H} role="img">
      <defs>
        <linearGradient id={id} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={color} stopOpacity="0.32" />
          <stop offset="100%" stopColor={color} stopOpacity="0.02" />
        </linearGradient>
      </defs>
      {axes(max, format)}
      <polygon points={fill} fill={`url(#${id})`} />
      <polyline points={line} fill="none" stroke={color} strokeWidth={2} strokeLinejoin="round" strokeLinecap="round" />
      <circle cx={lastX} cy={lastY} r={3.5} fill={color} />
      <XAxis count={data.length} />
    </svg>
  );
}

// Samples are a fixed interval apart, so the axis can be labelled from the count
// without carrying timestamps into the chart.
function XAxis({ count }: { count: number }) {
  const oldest = ((count - 1) * SAMPLE_MS) / 1000;
  const y = PAD.top + PH + 14;
  return (
    <>
      <line className="grid-line" x1={PAD.left} x2={PAD.left + PW} y1={PAD.top + PH} y2={PAD.top + PH} />
      <text className="axis-text" x={PAD.left} y={y}>-{Math.round(oldest)}s</text>
      <text className="axis-text" x={PAD.left + PW} y={y} textAnchor="end">now</text>
    </>
  );
}

export default function Metrics() {
  return (
    <Suspense fallback={<p className="muted">Loading…</p>}>
      <MetricsInner />
    </Suspense>
  );
}
