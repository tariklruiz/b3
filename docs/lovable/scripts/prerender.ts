/**
 * prerender.ts — Pre-renders every /fundo/<TICKER> URL into a real static
 * HTML file under dist/fundo/<TICKER>/index.html. Runs after vite build and
 * after sitemap generation.
 *
 * How it works:
 *   1. Read dist/sitemap.xml to get the list of fund URLs.
 *   2. Spin up a local static HTTP server pointing at dist/.
 *   3. Boot headless Chrome via puppeteer.
 *   4. For each URL, in batches of CONCURRENCY:
 *        - Open the URL in a new page.
 *        - Wait for [data-fund-loaded] to appear (a marker rendered by
 *          FundPage when fund data is ready) — or timeout.
 *        - Snapshot the full document.documentElement.outerHTML.
 *        - Write to dist/fundo/<TICKER>/index.html.
 *   5. Close everything and report.
 *
 * Failures (timeouts, fetch errors, render errors) are logged but do not
 * fail the build by default. We end with a summary; if you want strict mode,
 * set FAIL_ON_ERROR=1 in the environment.
 */
import { createServer } from "node:http";
import { readFileSync, writeFileSync, mkdirSync, existsSync, statSync } from "node:fs";
import { resolve, extname, join } from "node:path";
import puppeteer, { type Browser, type Page } from "puppeteer";

const DIST_DIR = resolve(process.cwd(), "dist");
const SITEMAP_PATH = resolve(DIST_DIR, "sitemap.xml");
const SITE_URL = "https://fiiguia.com.br";
const PORT = 4173;
const LOCAL_BASE = `http://localhost:${PORT}`;
const CONCURRENCY = 5;
const WAIT_TIMEOUT_MS = 20_000;
const FAIL_ON_ERROR = process.env.FAIL_ON_ERROR === "1";

// CLI flag: --limit N processes only the first N URLs. Useful for testing.
const LIMIT_FLAG_INDEX = process.argv.indexOf("--limit");
const LIMIT = LIMIT_FLAG_INDEX !== -1 && process.argv[LIMIT_FLAG_INDEX + 1]
  ? parseInt(process.argv[LIMIT_FLAG_INDEX + 1], 10)
  : 0;
const BUILD_BYPASS_TOKEN = process.env.BUILD_BYPASS_TOKEN || "";

const MIME_TYPES: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js":   "application/javascript; charset=utf-8",
  ".mjs":  "application/javascript; charset=utf-8",
  ".css":  "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg":  "image/svg+xml",
  ".png":  "image/png",
  ".jpg":  "image/jpeg",
  ".jpeg": "image/jpeg",
  ".webp": "image/webp",
  ".ico":  "image/x-icon",
  ".woff": "font/woff",
  ".woff2":"font/woff2",
  ".ttf":  "font/ttf",
  ".xml":  "application/xml; charset=utf-8",
  ".txt":  "text/plain; charset=utf-8",
};

function startStaticServer() {
  return new Promise<() => Promise<void>>((resolveStart, rejectStart) => {
    const server = createServer((req, res) => {
      try {
        // Strip query string, decode, default to /
        const pathname = decodeURIComponent((req.url || "/").split("?")[0]);
        // SPA fallback: if the resolved path doesn't exist as a file, serve index.html
        const candidatePath = join(DIST_DIR, pathname);
        let filePath = candidatePath;
        if (existsSync(filePath) && statSync(filePath).isDirectory()) {
          filePath = join(filePath, "index.html");
        }
        if (!existsSync(filePath)) {
          // Fallback to root index.html for any unknown path so client-side
          // routing can take over (mimics Cloudflare _redirects).
          filePath = join(DIST_DIR, "index.html");
        }
        const ext = extname(filePath).toLowerCase();
        const mime = MIME_TYPES[ext] || "application/octet-stream";
        const body = readFileSync(filePath);
        res.writeHead(200, { "Content-Type": mime, "Content-Length": body.length });
        res.end(body);
      } catch (err) {
        res.writeHead(500);
        res.end(String(err));
      }
    });
    server.on("error", rejectStart);
    server.listen(PORT, () => {
      resolveStart(() => new Promise(r => server.close(() => r())));
    });
  });
}

function readSitemapUrls(): string[] {
  const xml = readFileSync(SITEMAP_PATH, "utf-8");
  const matches = xml.match(/<loc>([^<]+)<\/loc>/g) || [];
  return matches
    .map(m => m.replace(/<\/?loc>/g, ""))
    .filter(url => url.startsWith(`${SITE_URL}/fundo/`));
}

async function prerenderOne(browser: Browser, fullUrl: string): Promise<void> {
  const startedAt = Date.now();
  const localUrl = fullUrl.replace(SITE_URL, LOCAL_BASE);
  const ticker = fullUrl.split("/").pop()!;
  const outDir = join(DIST_DIR, "fundo", ticker);
  const outPath = join(outDir, "index.html");

  let page: Page | null = null;
  try {
    page = await browser.newPage();
    await page.setViewport({ width: 1200, height: 900 });
    // Single interception handler: blocks heavy resources AND injects the
    // build token header only for API requests (not for fonts, images, etc).
    await page.setRequestInterception(true);
    page.on("request", req => {
      const type = req.resourceType();
      if (type === "image" || type === "media" || type === "font") {
        req.abort();
        return;
      }
      const url = req.url();
      if (BUILD_BYPASS_TOKEN && url.includes("fii-prices.up.railway.app")) {
        req.continue({ headers: { ...req.headers(), "x-build-token": BUILD_BYPASS_TOKEN } });
      } else {
        req.continue();
      }
    });

    await page.goto(localUrl, { waitUntil: "domcontentloaded", timeout: WAIT_TIMEOUT_MS });
    // Wait for the marker placed in FundPage.tsx when fund data has loaded
    await page.waitForSelector("[data-fund-loaded]", { timeout: WAIT_TIMEOUT_MS });

    const html = await page.evaluate(() => "<!DOCTYPE html>" + document.documentElement.outerHTML);

    mkdirSync(outDir, { recursive: true });
    writeFileSync(outPath, html, "utf-8");
    const elapsedMs = Date.now() - startedAt;
    console.log(`  ✓ ${ticker} (${(elapsedMs/1000).toFixed(1)}s)`);
  } finally {
    if (page) await page.close().catch(() => undefined);
  }
}

async function runBatch<T>(items: T[], size: number, fn: (item: T) => Promise<void>): Promise<{ ok: number; failed: string[] }> {
  let ok = 0;
  const failed: string[] = [];
  for (let i = 0; i < items.length; i += size) {
    const batch = items.slice(i, i + size);
    const results = await Promise.allSettled(batch.map(it => fn(it)));
    results.forEach((r, idx) => {
      const item = String(batch[idx]);
      if (r.status === "fulfilled") {
        ok++;
      } else {
        failed.push(`${item} :: ${r.reason instanceof Error ? r.reason.message : String(r.reason)}`);
      }
    });
    process.stdout.write(`  Progress: ${Math.min(i + size, items.length)}/${items.length}\r`);
  }
  process.stdout.write("\n");
  return { ok, failed };
}

async function main(): Promise<void> {
  if (!existsSync(SITEMAP_PATH)) {
    throw new Error(`Sitemap not found at ${SITEMAP_PATH}. Run sitemap generator first.`);
  }
  if (!BUILD_BYPASS_TOKEN) {
    console.warn(
      "WARNING: BUILD_BYPASS_TOKEN env var is not set. Pre-rendering will hit\n" +
      "         API rate limits and most pages will likely fail. Set the env var\n" +
      "         to the token configured on Railway and retry."
    );
  }
  let urls = readSitemapUrls();
  if (LIMIT > 0 && LIMIT < urls.length) {
    urls = urls.slice(0, LIMIT);
    console.log(`Pre-rendering first ${urls.length} of ${readSitemapUrls().length} fund pages (--limit ${LIMIT})...`);
  } else {
    console.log(`Pre-rendering ${urls.length} fund pages...`);
  }

  const stopServer = await startStaticServer();
  console.log(`Local server ready at ${LOCAL_BASE}`);

  const browser = await puppeteer.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
  });

  const runStartedAt = Date.now();
  try {
    const { ok, failed } = await runBatch(urls, CONCURRENCY, url => prerenderOne(browser, url));
    const totalSec = ((Date.now() - runStartedAt) / 1000).toFixed(1);
    const avgSec = urls.length > 0 ? (Number(totalSec) / urls.length).toFixed(2) : "0";
    console.log(`\nPre-rendered: ${ok} / ${urls.length} in ${totalSec}s (avg ${avgSec}s/page)`);
    if (failed.length > 0) {
      console.log(`\nFailures (${failed.length}):`);
      failed.forEach(f => console.log(`  - ${f}`));
      if (FAIL_ON_ERROR) {
        throw new Error(`${failed.length} pages failed to pre-render (FAIL_ON_ERROR=1)`);
      }
    }
  } finally {
    await browser.close();
    await stopServer();
  }
}

main().catch((err: unknown) => {
  console.error("Pre-render failed:", err);
  process.exit(1);
});
