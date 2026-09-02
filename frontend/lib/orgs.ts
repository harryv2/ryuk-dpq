// Hardcoded until there is real authentication. The token is what the gateway
// maps back to an org, so switching orgs here is switching credentials.
export type Org = { id: string; label: string; token: string };

export const ORGS: Org[] = [
  { id: "org_acme", label: "Acme", token: "acme-token" },
  { id: "org_globex", label: "Globex", token: "globex-token" },
  { id: "org_initech", label: "Initech", token: "initech-token" },
];

export const DEFAULT_ORG = ORGS[0];
