"use client";

import { ReactNode } from "react";

export function Skeleton({ w = "100%", h = 16, style }: { w?: string | number; h?: number; style?: React.CSSProperties }) {
  return <div className="skeleton" style={{ width: w, height: h, ...style }} />;
}

export function StatCard({
  label, value, hint, loading, small,
}: {
  label: string; value: ReactNode; hint?: string; loading?: boolean; small?: boolean;
}) {
  return (
    <div className="card stat">
      <div className="label">{label}</div>
      {loading ? (
        <Skeleton w={72} h={30} style={{ marginTop: 6 }} />
      ) : (
        <div className={"value" + (small ? " sm" : "")}>{value}</div>
      )}
      {hint && <div className="hint">{hint}</div>}
    </div>
  );
}

export function TableSkeleton({ rows = 3, cols = 5 }: { rows?: number; cols?: number }) {
  return (
    <tbody>
      {Array.from({ length: rows }).map((_, r) => (
        <tr key={r}>
          {Array.from({ length: cols }).map((_, c) => (
            <td key={c}>
              <Skeleton w={c === 0 ? 120 : 56} h={14} />
            </td>
          ))}
        </tr>
      ))}
    </tbody>
  );
}

export function Empty({ title, children }: { title: string; children?: ReactNode }) {
  return (
    <div className="empty">
      <strong>{title}</strong>
      {children}
    </div>
  );
}

export function Busy({ label }: { label: string }) {
  return (
    <>
      <span className="spinner" /> {label}
    </>
  );
}

// A relative age reads better than a raw second count once it passes a minute.
export function age(seconds: number): string {
  if (!seconds || seconds < 1) return "0s";
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}
