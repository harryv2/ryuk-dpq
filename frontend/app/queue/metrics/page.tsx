"use client";

import { Suspense, useEffect, useRef, useState } from "react";
import { useSearchParams } from "next/navigation";
import Link from "next/link";
import { useOrg } from "@/lib/org-context";
import { api, QueueStats, Series, Timeseries } from "@/lib/api";
import { StatCard, age } from "@/lib/ui";
import { useWidth } from "@/lib/measure";

type Band = "low" | "medium" | "high";

type Point = {
  t: number;
  low: number; medium: number; high: number;
  inFlight: number; ageSeconds: number;
};

// Rates come from the gateway, which derives them from two collections; a node
// only reports totals.
function rate(v?: number): string {
  if (!v) return "0/s";
  return (v < 10 ? v.toFixed(1) : Math.round(v).toString()) + "/s";
}

const SAMPLE_MS = 2000;
const WINDOW = 300; // samples the fallback keeps when there is no history store

// Prometheus scrapes the gateway and keeps the history, so a window is a real
// span rather than however long this page has been open. It has to match the
// set the gateway accepts.
const RANGES = ["5m", "1h", "6h", "24h"];

function MetricsInner() {
  const { org } = useOrg();
  const name = useSearchParams().get("name") ?? "";
  const [stats, setStats] = useState<QueueStats | null>(null);
  const [loading, setLoading] = useState(true);
  const samples = useRef<Point[]>([]);
  const [, tick] = useState(0);
  const [range, setRange] = useState(RANGES[0]);
  const [history, setHistory] = useState<Timeseries | null>(null);
  const [show, setShow] = useState<Record<Band, boolean>>({ high: true, medium: true, low: true });
  const allShown = show.high && show.medium && show.low;

  useEffect(() => {
    samples.current = [];
    let alive = true;
    const load = async () => {
      const s = await api.stats(org.token, name).catch(() => null);
      if (!s || !alive) return;
      setStats(s);
      setLoading(false);
      samples.current = [
        ...samples.current,
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

  // History comes from the monitoring system, so it survives a reload and
  // reaches back further than this page has been open.
  useEffect(() => {
    let alive = true;
    const pull = () =>
      api
        .timeseries(org.token, name, range)
        .then((h) => alive && setHistory(h))
        .catch(() => alive && setHistory(null));
    pull();
    const t = setInterval(pull, 10000);
    return () => {
      alive = false;
      clearInterval(t);
    };
  }, [org.token, name, range]);

  const stored = history?.available ?? false;
  const local = samples.current;

  // Prometheus keeps history per priority as its own series; the fallback has
  // one Point carrying all three. Both collapse to the same shape here.
  const bands: Record<Band, SeriesPoint[]> = stored
    ? {
        high: pointsFor(history!.ready, "high"),
        medium: pointsFor(history!.ready, "medium"),
        low: pointsFor(history!.ready, "low"),
      }
    : {
        high: local.map((p) => ({ t: p.t, v: p.high })),
        medium: local.map((p) => ({ t: p.t, v: p.medium })),
        low: local.map((p) => ({ t: p.t, v: p.low })),
      };

  const inFlightPoints = stored
    ? pointsFor(history!.inFlight, "")
    : local.map((p) => ({ t: p.t, v: p.inFlight }));
  const agePoints = stored
    ? pointsFor(history!.oldestAge, "")
    : local.map((p) => ({ t: p.t, v: p.ageSeconds }));

  const covered = Math.max(bands.high.length, bands.medium.length, bands.low.length);
  const span = covered > 1
    ? ((bands.high[covered - 1]?.t ?? 0) - (bands.high[0]?.t ?? 0)) / 1000
    : 0;
  const toggle = (b: Band) => setShow({ ...show, [b]: !show[b] });
  const showAll = () => setShow({ high: true, medium: true, low: true });

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
        <StatCard
          label="Enqueue rate" value={rate(stats?.enqueueRate)} loading={loading}
          hint="per second, last interval"
        />
        <StatCard
          label="Ack rate" value={rate(stats?.ackRate)} loading={loading}
          hint="per second, last interval"
        />
        <StatCard label="Enqueued" value={(stats?.enqueued ?? 0).toLocaleString()} loading={loading} hint="since the owner started" />
        <StatCard label="Acknowledged" value={(stats?.acked ?? 0).toLocaleString()} loading={loading} hint="since the owner started" />
        <StatCard label="Redelivered" value={(stats?.redelivered ?? 0).toLocaleString()} loading={loading} hint="nacked or timed out" />
        <StatCard label="Expired" value={(stats?.expired ?? 0).toLocaleString()} loading={loading} hint="outlived their TTL" />
      </div>

      <div className="card" style={{ marginBottom: 16 }}>
        <div className="card-head">
          <h2>Ready messages by priority</h2>
          <div className="row">
            <div className="chart-filters">
              {BANDS.map((b) => (
                <button
                  key={b.key}
                  data-on={show[b.key]}
                  onClick={() => toggle(b.key)}
                  title={show[b.key] ? `Hide ${b.label}` : `Show ${b.label}`}
                >
                  <span className="swatch" style={{ background: b.color }} />
                  {b.label}
                  <span className="n">{stats?.byPriority[b.key] ?? 0}</span>
                </button>
              ))}
              <button
                onClick={showAll}
                disabled={allShown}
                title={allShown ? "Every priority is already shown" : "Show every priority"}
              >
                all
              </button>
            </div>
            <div className="window-pick">
              {RANGES.map((r) => {
                const reached = collected >= r.samples || r.samples >= WINDOW;
                return (
                  <button
                    key={r.label}
                    data-active={r.label === range.label}
                    disabled={!reached}
                    onClick={() => setRange(r)}
                    title={
                      reached
                        ? `Last ${r.label}`
                        : `Needs ${r.samples} samples, ${collected} collected`
                    }
                  >
                    {r.label}
                  </button>
                );
              })}
            </div>
          </div>
        </div>
        <PriorityLines data={h} show={show} />
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


/* ---------- charts ----------
   The chart is drawn at the container's measured width, so one SVG unit is one
   pixel. Drawing into a fixed viewBox instead letterboxes the chart inside a
   wider card: the aspect ratio is kept and the spare width becomes margin. */

const H = 200;
const PAD = { top: 14, right: 16, bottom: 24, left: 46 };

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

type Geom = { w: number; pw: number; ph: number; x: (i: number, n: number) => number; y: (v: number) => number };

function geometry(width: number, max: number): Geom {
  const pw = Math.max(40, width - PAD.left - PAD.right);
  const ph = H - PAD.top - PAD.bottom;
  return {
    w: width,
    pw,
    ph,
    x: (i, n) => PAD.left + (n < 2 ? pw : (i / (n - 1)) * pw),
    y: (v) => PAD.top + ph - (v / max) * ph,
  };
}

function Axes({ g, max, format }: { g: Geom; max: number; format: (v: number) => string }) {
  return (
    <>
      {[0, 0.25, 0.5, 0.75, 1].map((f) => {
        const y = PAD.top + g.ph - f * g.ph;
        return (
          <g key={f}>
            <line className="grid-line" x1={PAD.left} x2={PAD.left + g.pw} y1={y} y2={y} />
            <text className="axis-text" x={PAD.left - 8} y={y + 3.5} textAnchor="end">
              {format(max * f)}
            </text>
          </g>
        );
      })}
    </>
  );
}

// Samples are a fixed interval apart, so the axis is labelled from the count
// without carrying timestamps into the chart.
function XAxis({ g, count }: { g: Geom; count: number }) {
  const oldest = ((count - 1) * SAMPLE_MS) / 1000;
  const y = PAD.top + g.ph + 16;
  return (
    <>
      <line className="grid-line" x1={PAD.left} x2={PAD.left + g.pw} y1={PAD.top + g.ph} y2={PAD.top + g.ph} />
      <text className="axis-text" x={PAD.left} y={y}>-{Math.round(oldest)}s</text>
      <text className="axis-text" x={PAD.left + g.pw} y={y} textAnchor="end">now</text>
    </>
  );
}

function Collecting() {
  return (
    <div style={{ height: H, display: "flex", alignItems: "center", justifyContent: "center", gap: 8 }}>
      <span className="spinner muted" />
      <span className="muted">Collecting samples…</span>
    </div>
  );
}

const BANDS = [
  { key: "high" as const, color: "var(--high)", label: "high" },
  { key: "medium" as const, color: "var(--medium)", label: "medium" },
  { key: "low" as const, color: "var(--low)", label: "low" },
];

// One line per priority rather than a stacked area. Stacking made a band's own
// value unreadable -- you had to subtract the layers beneath it -- and a band
// sitting at zero was an invisible sliver rather than a flat line on the floor.
function PriorityLines({ data, show }: { data: Point[]; show: Record<Band, boolean> }) {
  const { ref, width } = useWidth<HTMLDivElement>();
  const visible = BANDS.filter((b) => show[b.key]);

  // Scaled to the largest single series, not their sum, so each line uses the
  // full height it can.
  const peak = Math.max(0, ...data.flatMap((p) => visible.map((b) => p[b.key])));
  const max = niceMax(peak);
  const g = geometry(width, max);

  if (data.length < 2) {
    return <div ref={ref}><Collecting /></div>;
  }
  if (visible.length === 0) {
    return (
      <div ref={ref} style={{ height: H, display: "flex", alignItems: "center", justifyContent: "center" }}>
        <span className="muted">Every priority is hidden. Turn one back on.</span>
      </div>
    );
  }

  const n = data.length;

  return (
    <div ref={ref}>
      <svg className="chart" viewBox={`0 0 ${g.w} ${H}`} width={g.w} height={H} role="img">
        <Axes g={g} max={max} format={(v) => String(Math.round(v))} />
        {visible.map((b) => {
          const pts = data.map((p, i) => `${g.x(i, n)},${g.y(p[b.key])}`).join(" ");
          const last = data[n - 1][b.key];
          return (
            <g key={b.key}>
              <polyline
                points={pts} fill="none" stroke={b.color} strokeWidth={2.25}
                strokeLinejoin="round" strokeLinecap="round"
              />
              <circle cx={g.x(n - 1, n)} cy={g.y(last)} r={3.5} fill={b.color} />
            </g>
          );
        })}
        <XAxis g={g} count={n} />
      </svg>
    </div>
  );
}

function LineArea({
  data, color, format = (v: number) => String(Math.round(v)),
}: {
  data: number[]; color: string; format?: (v: number) => string;
}) {
  const { ref, width } = useWidth<HTMLDivElement>();
  if (data.length < 2) {
    return <div ref={ref}><Collecting /></div>;
  }

  const max = niceMax(Math.max(...data));
  const g = geometry(width, max);
  const n = data.length;

  const line = data.map((v, i) => `${g.x(i, n)},${g.y(v)}`).join(" ");
  const floor = PAD.top + g.ph;
  const fill = `${PAD.left},${floor} ${line} ${PAD.left + g.pw},${floor}`;
  const id = `fill-${color.replace(/[^a-z]/gi, "")}`;

  return (
    <div ref={ref}>
      <svg className="chart" viewBox={`0 0 ${g.w} ${H}`} width={g.w} height={H} role="img">
        <defs>
          <linearGradient id={id} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={color} stopOpacity="0.3" />
            <stop offset="100%" stopColor={color} stopOpacity="0.02" />
          </linearGradient>
        </defs>
        <Axes g={g} max={max} format={format} />
        <polygon points={fill} fill={`url(#${id})`} />
        <polyline points={line} fill="none" stroke={color} strokeWidth={2} strokeLinejoin="round" strokeLinecap="round" />
        <circle cx={g.x(n - 1, n)} cy={g.y(data[n - 1])} r={3.5} fill={color} />
        <XAxis g={g} count={n} />
      </svg>
    </div>
  );
}

export default function Metrics() {
  return (
    <Suspense fallback={<p className="muted">Loading…</p>}>
      <MetricsInner />
    </Suspense>
  );
}
