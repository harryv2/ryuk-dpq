"use client";

import { createContext, useContext, useEffect, useState, ReactNode } from "react";

type Mode = "light" | "dark";

const ThemeContext = createContext<{ mode: Mode; toggle: () => void }>({
  mode: "dark",
  toggle: () => {},
});

export const useTheme = () => useContext(ThemeContext);

// Runs before the first paint so the page never flashes the wrong theme. It is
// inlined into the document head rather than run from React, which is too late.
export const themeScript = `
(function(){
  try {
    var m = localStorage.getItem("ryuk-theme");
    if (m !== "light" && m !== "dark") {
      m = matchMedia("(prefers-color-scheme: light)").matches ? "light" : "dark";
    }
    document.documentElement.setAttribute("data-theme", m);
  } catch (e) {
    document.documentElement.setAttribute("data-theme", "dark");
  }
})();
`;

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [mode, setMode] = useState<Mode>("dark");

  useEffect(() => {
    const m = document.documentElement.getAttribute("data-theme");
    setMode(m === "light" ? "light" : "dark");
  }, []);

  const toggle = () => {
    const next: Mode = mode === "dark" ? "light" : "dark";
    setMode(next);
    document.documentElement.setAttribute("data-theme", next);
    try {
      localStorage.setItem("ryuk-theme", next);
    } catch {}
  };

  return <ThemeContext.Provider value={{ mode, toggle }}>{children}</ThemeContext.Provider>;
}

export function ThemeToggle() {
  const { mode, toggle } = useTheme();
  return (
    <button
      className="ghost icon"
      onClick={toggle}
      title={mode === "dark" ? "Switch to light" : "Switch to dark"}
      aria-label="Toggle theme"
    >
      {mode === "dark" ? <SunIcon /> : <MoonIcon />}
    </button>
  );
}

function SunIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
      <circle cx="12" cy="12" r="4" />
      <path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4" />
    </svg>
  );
}

function MoonIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M21 12.8A9 9 0 1111.2 3a7 7 0 009.8 9.8z" />
    </svg>
  );
}
