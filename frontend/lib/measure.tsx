"use client";

import { useEffect, useRef, useState } from "react";

// A fixed viewBox letterboxes the chart inside a wider card -- the spare width
// becomes margin. Drawing at the measured width makes one SVG unit one pixel.
export function useWidth<T extends HTMLElement>(fallback = 720) {
  const ref = useRef<T>(null);
  const [width, setWidth] = useState(fallback);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const ro = new ResizeObserver(([entry]) => {
      const w = Math.round(entry.contentRect.width);
      if (w > 0) setWidth(w);
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  return { ref, width };
}
