"use client";

import { createContext, useContext, useEffect, useState, ReactNode } from "react";
import { ORGS, DEFAULT_ORG, Org } from "./orgs";

const OrgContext = createContext<{ org: Org; setOrg: (o: Org) => void }>({
  org: DEFAULT_ORG,
  setOrg: () => {},
});

export const useOrg = () => useContext(OrgContext);

export function OrgProvider({ children }: { children: ReactNode }) {
  const [org, setOrg] = useState<Org>(DEFAULT_ORG);

  // Remembered so a reload does not silently switch tenants.
  useEffect(() => {
    try {
      const id = localStorage.getItem("ryuk-org");
      const found = ORGS.find((o) => o.id === id);
      if (found) setOrg(found);
    } catch {}
  }, []);

  const pick = (o: Org) => {
    setOrg(o);
    try {
      localStorage.setItem("ryuk-org", o.id);
    } catch {}
  };

  return <OrgContext.Provider value={{ org, setOrg: pick }}>{children}</OrgContext.Provider>;
}

export function OrgSelect() {
  const { org, setOrg } = useOrg();
  return (
    <select
      value={org.id}
      aria-label="Organisation"
      onChange={(e) => setOrg(ORGS.find((o) => o.id === e.target.value) ?? DEFAULT_ORG)}
    >
      {ORGS.map((o) => (
        <option key={o.id} value={o.id}>
          {o.label}
        </option>
      ))}
    </select>
  );
}
