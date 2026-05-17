/**
 * generate-sitemap.ts — Fetches the FII/FIAGRO ticker list from the API and
 * writes a fresh sitemap.xml into dist/. Runs after `vite build`.
 *
 * One <url> entry per ticker plus the homepage. The <lastmod> is the build
 * date, which is acceptable because the build itself is triggered when fund
 * data refreshes upstream.
 */
import { writeFileSync } from "node:fs";
import { resolve } from "node:path";

// min_dividend_months filters out tickers without recent dividend activity.
// Funds without any dividend record fail to pre-render (their /fundo/dividendos
// returns 404, which the React app surfaces as an error state). Excluding them
// from the sitemap also keeps Google focused on funds with substantive content.
const API_URL = "https://fii-prices.up.railway.app/fiis?min_dividend_months=6";
const SITE_URL = "https://fiiguia.com.br";
const OUTPUT_PATH = resolve(process.cwd(), "dist", "sitemap.xml");

type FiiEntry = {
  ticker: string;
  codigo: string;
  name: string;
  type: "FII" | "FIAGRO";
};

type FiisResponse = {
  count: number;
  tickers: FiiEntry[];
};

async function main(): Promise<void> {
  console.log(`Fetching tickers from ${API_URL}...`);
  const response = await fetch(API_URL);
  if (!response.ok) {
    throw new Error(`API returned ${response.status} ${response.statusText}`);
  }
  const data = (await response.json()) as FiisResponse;
  if (!Array.isArray(data.tickers) || data.tickers.length === 0) {
    throw new Error(`API returned no tickers (count=${data.count})`);
  }
  console.log(`Got ${data.tickers.length} tickers`);

  const today = new Date().toISOString().split("T")[0]; // YYYY-MM-DD

  const urls: string[] = [
    // Homepage — highest priority, changes most often
    `  <url>
    <loc>${SITE_URL}/</loc>
    <lastmod>${today}</lastmod>
    <changefreq>daily</changefreq>
    <priority>1.0</priority>
  </url>`,
    // One entry per fund
    ...data.tickers.map(
      ({ ticker }) => `  <url>
    <loc>${SITE_URL}/fundo/${ticker}</loc>
    <lastmod>${today}</lastmod>
    <changefreq>daily</changefreq>
    <priority>0.8</priority>
  </url>`
    ),
  ];

  const sitemap = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls.join("\n")}
</urlset>
`;

  writeFileSync(OUTPUT_PATH, sitemap, "utf-8");
  console.log(`Wrote ${OUTPUT_PATH} with ${data.tickers.length + 1} URLs`);
}

main().catch((err: unknown) => {
  console.error("Sitemap generation failed:", err);
  process.exit(1);
});
