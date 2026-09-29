import type { Metadata } from "next";
import { preload } from "react-dom";
import "./fonts.css";
import "./globals.css";
import { Header, Footer } from "@/components/Chrome";

// Fonts are served from this site's own origin (fonts.css, public/fonts): a
// visitor's browser opens no connection to a font CDN, which is the same
// promise the rest of the site makes about third parties. They are kept in
// the repository rather than fetched from Google at build time, so a build
// never depends on Google being reachable. The latin files are preloaded, as
// next/font did.
const preloaded = ["ibm-plex-sans-latin", "ibm-plex-mono-latin", "ibm-plex-mono-latin-500", "geist-latin", "geist-mono-latin"];

export const metadata: Metadata = {
  title: "Tensile · Celestia Fibre observer",
  description: "Tensile: independent measurement of whether Celestia validators keep their Fibre serving promise.",
};

// Applies a saved theme before the first paint so a dark-mode reader never
// sees a light flash; with nothing saved, the system preference is the start.
const themeBoot = `try{var t=localStorage.getItem("theme");if(t!=="light"&&t!=="dark")t=window.matchMedia&&window.matchMedia("(prefers-color-scheme: dark)").matches?"dark":"light";document.documentElement.dataset.theme=t}catch(e){}`;

export default function RootLayout({ children }: { children: React.ReactNode }) {
  // React emits each as one <link rel="preload"> in the head.
  for (const f of preloaded) preload(`/fonts/${f}.woff2`, { as: "font", type: "font/woff2", crossOrigin: "" });
  return (
    <html lang="en" suppressHydrationWarning>
      <body>
        <script dangerouslySetInnerHTML={{ __html: themeBoot }} />
        <Header />
        <main className="wrap">
          {children}
          <Footer />
        </main>
      </body>
    </html>
  );
}
