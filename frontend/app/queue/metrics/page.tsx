"use client";

import { Suspense, useEffect, useRef, useState } from "react";
import { useSearchParams } from "next/navigation";
import Link from "next/link";
import { useOrg } from "@/lib/org-context";
import { api, QueueStats, Series, Timeseries } from "@/lib/api";
import { StatCard, age } from "@/lib/ui";
import { useWidth } from "@/lib/measure";

type Band = "low" | "medium" | "high";
type Line = Band | "total";

// One chart point, whichever source it came from.
type SeriesPoint = { t: number; v: number };

// Prometheus returns a series per label value; the name is empty when a metric
// has only one series.
function pointsFor(series: Series[], name: string): SeriesPoint[] {
  const s = name ? series.find((x) => x.name === name) : series[0];
  if (!s) return [];
  return s.points.map((p) => ({ t: new Date(p.at).getTime(), v: p.value }));
}

type Point = {
  t: number;
  low: number; medium: number; high: number;
  inFlight: number; ageSeconds: number;
};

// Rates come from the gateway, which derives them from two collections; a node
// only reports totals.
// Axis labels on the rate charts, matching the cards above them.
function perSec(v: number): string {
  return v < 10 ? v.toFixed(1) : String(Math.round(v));
}

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
  const [show, setShow] = useState<Record<Line, boolean>>({
    high: true, medium: true, low: true, total: false,
  });


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
  // The total across every priority. Summed here rather than asked for
  // separately: the three series come from one query at one step, so they share
  // timestamps and adding them is exact.
  const totalPoints: SeriesPoint[] = (() => {
    const by = new Map<number, number>();
    for (const b of ["high", "medium", "low"] as Band[]) {
      for (const p of bands[b]) by.set(p.t, (by.get(p.t) ?? 0) + p.v);
    }
    return [...by.entries()].sort((x, y) => x[0] - y[0]).map(([t, v]) => ({ t, v }));
  })();

  const enqueueRatePoints = stored
    ? pointsFor(history!.rates, "enqueue")
    : [];
  const ackRatePoints = stored ? pointsFor(history!.rates, "ack") : [];
  const agePoints = stored
    ? pointsFor(history!.oldestAge, "")
    : local.map((p) => ({ t: p.t, v: p.ageSeconds }));

  const covered = Math.max(bands.high.length, bands.medium.length, bands.low.length);
  const span = covered > 1
    ? ((bands.high[covered - 1]?.t ?? 0) - (bands.high[0]?.t ?? 0)) / 1000
    : 0;
  const toggle = (b: Line) => setShow({ ...show, [b]: !show[b] });

  return (
    <>
      <div className="page-head">
        <div>
          <h1>{name}</h1>
          <p className="sub">
            <span className="live-dot" /> live · {covered} points over {age(span)}
            {stored ? <> · from Prometheus</> : <> · sampled in this tab</>}
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
        <StatCard label="Redelivered" value={(stats?.redelivered ?? 0).toLocaleString()} loading={loading} hint="nacked or timed out" />
        <StatCard label="Dead-lettered" value={(stats?.deadLettered ?? 0).toLocaleString()} loading={loading} hint="ran out of retries" />
        <StatCard label="Expired" value={(stats?.expired ?? 0).toLocaleString()} loading={loading} hint="outlived their TTL" />
        <StatCard label="Starvation escapes" value={(stats?.starvationEscapes ?? 0).toLocaleString()} loading={loading} hint="used the reserved share" small />
      </div>

      <div className="card" style={{ marginBottom: 16 }}>
        <div className="card-head">
          <h2>Ready messages by priority band</h2>
          <div className="row">
            <div className="chart-filters">
              {BANDS.map((b) => (
                <button
                  key={b.key}
                  data-on={show[b.key]}
                  onClick={() => toggle(b.key)}
                  title={
                    (show[b.key] ? `Hide ` : `Show `) +
                    `${b.label} — priority ${b.range}`
                  }
                >
                  <span className="swatch" style={{ background: b.color }} />
                  {b.label}
                  <span className="range">{b.range}</span>
                  <span className="n">{stats?.byPriority[b.key] ?? 0}</span>
                </button>
              ))}
              <button
                data-on={show.total}
                onClick={() => toggle("total")}
                title={show.total ? "Hide the total" : "Show the total across every priority"}
              >
                <span className="swatch" style={{ background: "var(--text-soft)" }} />
                total
                <span className="n">
                  {(stats?.byPriority.high ?? 0) +
                    (stats?.byPriority.medium ?? 0) +
                    (stats?.byPriority.low ?? 0)}
                </span>
              </button>
            </div>
            <div className="window-pick">
              {RANGES.map((r) => (
                <button
                  key={r}
                  data-active={r === range}
                  disabled={!stored && r !== RANGES[0]}
                  onClick={() => setRange(r)}
                  title={
                    stored
                      ? `Last ${r}`
                      : "Needs the monitoring system; this tab only has what it sampled itself"
                  }
                >
                  {r}
                </button>
              ))}
            </div>
          </div>
        </div>
        <PriorityLines bands={bands} total={totalPoints} show={show} />
      </div>

      <div className="grid cols-2" style={{ marginBottom: 16 }}>
        <div className="card">
          <div className="card-head">
            <h2>Write throughput</h2>
            <span className="muted" style={{ fontSize: 12 }}>{rate(stats?.enqueueRate)}</span>
          </div>
          <LineArea points={enqueueRatePoints} color="var(--ok)" format={perSec} />
          <p className="field-hint" style={{ marginTop: 10 }}>
            Messages accepted per second, across every machine holding a slot.
          </p>
        </div>
        <div className="card">
          <div className="card-head">
            <h2>Ack rate</h2>
            <span className="muted" style={{ fontSize: 12 }}>{rate(stats?.ackRate)}</span>
          </div>
          <LineArea points={ackRatePoints} color="var(--accent)" format={perSec} />
          <p className="field-hint" style={{ marginTop: 10 }}>
            Consumers finishing work. Sitting below the write rate for long means
            the queue is growing faster than it drains.
          </p>
        </div>
      </div>

      <div className="grid cols-2" style={{ marginBottom: 16 }}>
        <div className="card">
          <div className="card-head">
            <h2>Oldest message age</h2>
            <span className="muted" style={{ fontSize: 12 }}>{age(stats?.oldestMessageAgeSeconds ?? 0)}</span>
          </div>
          <LineArea points={agePoints} color="var(--medium)" format={(v) => age(v)} />
        </div>
        <div className="card">
          <div className="card-head">
            <h2>In flight</h2>
            <span className="muted" style={{ fontSize: 12 }}>{stats?.inFlight ?? 0} held</span>
          </div>
          <LineArea points={inFlightPoints} color="var(--accent)" />
        </div>
      </div>

      <div className="card" style={{ marginBottom: 16 }}>
        <div className="card-head">
          <h2>Totals since this owner node started</h2>
        </div>
        <div className="grid cols-4">
          <div className="stat">
            <div className="label">Enqueued</div>
            <div className="value sm">{(stats?.enqueued ?? 0).toLocaleString()}</div>
          </div>
          <div className="stat">
            <div className="label">Acknowledged</div>
            <div className="value sm">{(stats?.acked ?? 0).toLocaleString()}</div>
          </div>
        </div>
        <p className="field-hint" style={{ marginTop: 12, maxWidth: 720 }}>
          These live in the owning node&rsquo;s memory, so a restart resets them and a
          distributed queue&rsquo;s totals drop when one of its machines restarts. Read
          the rates above instead; they are derived from the change between two
          collections and are unaffected.
        </p>
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
function XAxis({ g, points }: { g: Geom; points: SeriesPoint[] }) {
  const oldest =
    points.length > 1 ? (points[points.length - 1].t - points[0].t) / 1000 : 0;
  const y = PAD.top + g.ph + 16;
  return (
    <>
      <line className="grid-line" x1={PAD.left} x2={PAD.left + g.pw} y1={PAD.top + g.ph} y2={PAD.top + g.ph} />
      <text className="axis-text" x={PAD.left} y={y}>-{age(oldest)}</text>
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

// Priority is a number from 0 to 100 and is ordered exactly. Metrics report
// three bands rather than 101 series: the engine keeps one counter per band per
// slot, and 101 lines would be neither chartable nor cheap to store. The ranges
// are shown so a custom value lands somewhere obvious.
const BANDS = [
  { key: "high" as const, color: "var(--high)", label: "high", range: "67–100" },
  { key: "medium" as const, color: "var(--medium)", label: "medium", range: "34–66" },
  { key: "low" as const, color: "var(--low)", label: "low", range: "0–33" },
];

// One line per priority rather than a stacked area. Stacking made a band's own
// value unreadable -- you had to subtract the layers beneath it -- and a band
// sitting at zero was an invisible sliver rather than a flat line on the floor.
function PriorityLines({
  bands, total, show,
}: {
  bands: Record<Band, SeriesPoint[]>;
  total: SeriesPoint[];
  show: Record<Line, boolean>;
}) {
  const { ref, width } = useWidth<HTMLDivElement>();

  const drawn = [
    ...BANDS.filter((b) => show[b.key]).map((b) => ({ ...b, points: bands[b.key] })),
    ...(show.total ? [{ key: "total", color: "var(--text-soft)", points: total }] : []),
  ].filter((l) => l.points.length > 1);

  // Scaled to the largest line on screen. The total is the sum of the bands, so
  // turning it on rescales everything -- which is the point: it shows how each
  // band contributes to the depth.
  const peak = Math.max(0, ...drawn.flatMap((l) => l.points.map((p) => p.v)));
  const max = niceMax(peak);
  const g = geometry(width, max);

  if (drawn.length === 0) {
    const anything = BANDS.some((b) => show[b.key]) || show.total;
    return (
      <div ref={ref} style={{ height: H, display: "flex", alignItems: "center", justifyContent: "center" }}>
        {anything ? <Collecting /> : <span className="muted">Nothing selected. Turn a line back on.</span>}
      </div>
    );
  }

  return (
    <div ref={ref}>
      <svg className="chart" viewBox={`0 0 ${g.w} ${H}`} width={g.w} height={H} role="img">
        <Axes g={g} max={max} format={(v) => String(Math.round(v))} />
        {drawn.map((l) => {
          const n = l.points.length;
          const line = l.points.map((p, i) => `${g.x(i, n)},${g.y(p.v)}`).join(" ");
          return (
            <g key={l.key}>
              <polyline
                points={line} fill="none" stroke={l.color}
                strokeWidth={l.key === "total" ? 2.75 : 2.25}
                strokeDasharray={l.key === "total" ? "6 4" : undefined}
                strokeLinejoin="round" strokeLinecap="round"
              />
              <circle cx={g.x(n - 1, n)} cy={g.y(l.points[n - 1].v)} r={3.5} fill={l.color} />
            </g>
          );
        })}
        <XAxis g={g} points={drawn[0].points} />
      </svg>
    </div>
  );
}

function LineArea({
  points, color, format = (v: number) => String(Math.round(v)),
}: {
  points: SeriesPoint[]; color: string; format?: (v: number) => string;
}) {
  const { ref, width } = useWidth<HTMLDivElement>();
  if (points.length < 2) {
    return <div ref={ref}><Collecting /></div>;
  }

  const max = niceMax(Math.max(...points.map((p) => p.v)));
  const g = geometry(width, max);
  const n = points.length;

  const line = points.map((p, i) => `${g.x(i, n)},${g.y(p.v)}`).join(" ");
  const floor = PAD.top + g.ph;
  const fill = `${PAD.left},${floor} ${line} ${PAD.left + g.pw},${floor}`;
  const id = `fill-${color.replace(/[^a-z]/gi, "")}`;

  return (
    <div ref={ref}>
      <svg className="chart" viewBox={`0 0 ${g.w} ${H}`} width={g.w} height={H} role="img">
        <defs>
          <linearGradient id={id} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={color} stopOpacity="0.28" />
            <stop offset="100%" stopColor={color} stopOpacity="0.02" />
          </linearGradient>
        </defs>
        <Axes g={g} max={max} format={format} />
        <polygon points={fill} fill={`url(#${id})`} />
        <polyline points={line} fill="none" stroke={color} strokeWidth={2} strokeLinejoin="round" strokeLinecap="round" />
        <circle cx={g.x(n - 1, n)} cy={g.y(points[n - 1].v)} r={3.5} fill={color} />
        <XAxis g={g} points={points} />
      </svg>
    </div>
  );
}

// Samples are a fixed interval apart, so the axis is labelled from the count
// without carrying timestamps into the chart.
export default function Metrics() {
  return (
    <Suspense fallback={<p className="muted">Loading…</p>}>
      <MetricsInner />
    </Suspense>
  );
}
