"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const LINKS = [
  { href: "/", label: "Queues" },
  { href: "/cluster", label: "Cluster" },
];

export function NavLinks() {
  const path = usePathname();
  return (
    <nav>
      {LINKS.map((l) => (
        <Link
          key={l.href}
          href={l.href}
          data-active={l.href === "/" ? path === "/" : path.startsWith(l.href)}
        >
          {l.label}
        </Link>
      ))}
    </nav>
  );
}
