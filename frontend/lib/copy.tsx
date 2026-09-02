"use client";

import { useState } from "react";

export function CopyButton({
  value, label, title,
}: {
  value: string; label: string; title?: string;
}) {
  const [done, setDone] = useState(false);

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(value);
    } catch {
      return;
    }
    setDone(true);
    setTimeout(() => setDone(false), 1400);
  };

  return (
    <button className="sm mono" onClick={copy} title={title ?? value}>
      {done ? "copied" : label}
    </button>
  );
}
