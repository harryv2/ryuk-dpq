import { ReactNode } from "react";
import { OrgProvider, OrgSelect } from "@/lib/org-context";
import { ThemeProvider, ThemeToggle, themeScript } from "@/lib/theme";
import { NavLinks } from "@/lib/nav";
import "./globals.css";

export const metadata = { title: "Ryuk" };

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" data-theme="dark">
      <head>
        <script dangerouslySetInnerHTML={{ __html: themeScript }} />
      </head>
      <body>
        <ThemeProvider>
          <OrgProvider>
            <header>
              <span className="brand">
                <span className="dot" />
                Ryuk
              </span>
              <NavLinks />
              <span className="spacer" />
              <OrgSelect />
              <ThemeToggle />
            </header>
            <main>{children}</main>
          </OrgProvider>
        </ThemeProvider>
      </body>
    </html>
  );
}
