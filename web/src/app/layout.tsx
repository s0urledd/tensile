import type { Metadata } from "next";
import { preload } from "react-dom";
import "./fonts.css";
import "./globals.css";
import { Header, Footer } from "@/components/Chrome";
import { SITE_URL } from "@/lib/site";

// Fonts are served from this site's own origin (fonts.css, public/fonts): a
// visitor's browser opens no connection to a font CDN, which is the same
// promise the rest of the site makes about third parties. They are kept in
// the repository rather than fetched from Google at build time, so a build
// never depends on Google being reachable. The latin files are preloaded, as
// next/font did.
const preloaded = ["ibm-plex-sans-latin", "ibm-plex-mono-latin", "ibm-plex-mono-latin-500", "dm-sans-latin"];

// What a shared link shows (X, Discord, Telegram, Slack): the preview's title, one plain sentence, and
// public/og.png. Pages that set only their own title keep this preview.
const DESCRIPTION = "Real-time data for Fibre on Celestia: blobs, publishers, escrow, Fibre providers, validators and endorsements, with blob availability and an open API.";
const PREVIEW_TITLE = "Tensile · Celestia Fibre explorer";
const PREVIEW_IMAGE = {
  url: "/og.png",
  width: 1200,
  height: 630,
  alt: "Tensile: real-time explorer for blobs and validators on Celestia Fibre, beside a bundle of fibres held between two grips",
};

export const metadata: Metadata = {
  metadataBase: new URL(SITE_URL),
  title: "Tensile · Real-time Celestia Fibre explorer",
  description: DESCRIPTION,
  openGraph: { type: "website", siteName: "Tensile", title: PREVIEW_TITLE, description: DESCRIPTION, images: [PREVIEW_IMAGE] },
  twitter: { card: "summary_large_image", title: PREVIEW_TITLE, description: DESCRIPTION, images: [PREVIEW_IMAGE] },
};

// The site opens dark for everyone; a theme the reader picked with the toggle is applied before the first paint, so
// a light reader never sees a dark flash either. The page itself is drawn dark, so it is dark without script too.
const themeBoot = `try{var t=localStorage.getItem("theme");if(t!=="light"&&t!=="dark")t="dark";document.documentElement.dataset.theme=t}catch(e){}`;

export default function RootLayout({ children }: { children: React.ReactNode }) {
  // React emits each as one <link rel="preload"> in the head.
  for (const f of preloaded) preload(`/fonts/${f}.woff2`, { as: "font", type: "font/woff2", crossOrigin: "" });
  return (
    <html lang="en" data-theme="dark" suppressHydrationWarning>
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
