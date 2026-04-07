#!/usr/bin/env node
/**
 * Duval County, Florida — Motivated Seller Lead Scraper
 *
 * Flow:
 *  1. Accept disclaimer at /search/Disclaimer
 *  2. GET /search/SearchTypeDocType  (establishes session)
 *  3. POST /search/SearchTypeDocType (submits search, initializes Kendo grid)
 *  4. POST /Search/GridResults       (retrieves paginated JSON data)
 *  5. Score, export JSON + GHL CSV
 */

const { CookieJar } = require("tough-cookie");
const fs   = require("fs");
const path = require("path");

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
const CLERK_BASE    = "https://or.duvalclerk.com";
const LOOKBACK_DAYS = parseInt(process.env.LOOKBACK_DAYS || "30", 10);
const PAGE_SIZE     = 200;   // max per Kendo grid page
const DELAY_MS      = 1200;  // polite delay between requests
const REPO_ROOT     = path.resolve(__dirname, "..");

const DASHBOARD_JSON = path.join(REPO_ROOT, "dashboard", "records.json");
const DATA_JSON      = path.join(REPO_ROOT, "data",      "records.json");
const GHL_CSV        = path.join(REPO_ROOT, "data",      "ghl_export.csv");

// Doc type map: code → [numericId, label, cat, catLabel]
const DOC_TYPES = {
  LP:       [104, "Lis Pendens",               "foreclosure", "Pre-Foreclosure / Lis Pendens"],
  NTD:      [149, "Notice of Tax Deed Sale",   "foreclosure", "Pre-Foreclosure / Lis Pendens"],
  TAXDEED:  [158, "Tax Deed",                  "tax",         "Tax Lien / Tax Deed"],
  TXDC:     [134, "Tax Deed (City Redeemed)",  "tax",         "Tax Lien / Tax Deed"],
  JDG:      [97,  "Judgment",                  "judgment",    "Judgment / Lien"],
  JDGR:     [98,  "Judgment/Restitution",      "judgment",    "Judgment / Lien"],
  CCCJUDG:  [79,  "CC Court Judgment",         "judgment",    "Judgment / Lien"],
  DVJ:      [145, "Domestic Violence Judgment","judgment",    "Judgment / Lien"],
  LN:       [103, "Lien",                      "lien",        "Lien"],
  JVRL:     [102, "Juvenile Restitution Lien", "lien",        "Lien"],
  PROB:     [124, "Probate",                   "probate",     "Probate / Estate"],
  NOC:      [115, "Notice of Commencement",    "noc",         "Notice of Commencement"],
  RELEASE:  [126, "Release",                   "release",     "Release"],
  PTL_REL:  [125, "Partial Release",           "release",     "Release"],
};

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------
function log(msg) {
  console.log(`[${new Date().toISOString().slice(11, 19)}] ${msg}`);
}
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function parseAmount(v) {
  if (v == null) return 0;
  const s = String(v).replace(/[^0-9.]/g, "");
  return parseFloat(s) || 0;
}

/** Convert "20260407" or "2026-04-07T..." to "YYYY-MM-DD" */
function parseDate(v) {
  if (!v) return "";
  const s = String(v);
  // ISO datetime
  const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (iso) return `${iso[1]}-${iso[2]}-${iso[3]}`;
  // "20260407"
  const compact = s.match(/^(\d{4})(\d{2})(\d{2})$/);
  if (compact) return `${compact[1]}-${compact[2]}-${compact[3]}`;
  // MM/DD/YYYY
  const mdy = s.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (mdy) return `${mdy[3]}-${mdy[1].padStart(2,"0")}-${mdy[2].padStart(2,"0")}`;
  return "";
}

function fmtMDY(iso) {
  const m = (iso || "").match(/(\d{4})-(\d{2})-(\d{2})/);
  return m ? `${m[2]}/${m[3]}/${m[1]}` : iso;
}

function dateOffset(days) {
  const d = new Date();
  d.setDate(d.getDate() - days);
  return d.toISOString().slice(0, 10);
}

function splitName(full = "") {
  full = full.trim();
  if (full.includes(",")) {
    const [last, ...rest] = full.split(",");
    return [rest.join(",").trim(), last.trim()];
  }
  const parts = full.split(/\s+/);
  if (parts.length >= 2) return [parts[0], parts.slice(1).join(" ")];
  return [full, ""];
}

// ---------------------------------------------------------------------------
// HTTP Session with cookie jar
// ---------------------------------------------------------------------------
class Session {
  constructor() {
    this.jar = new CookieJar();
    this.ua  = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";
  }
  _cookies(url) { return this.jar.getCookieStringSync(url); }
  _save(url, headers) {
    const list = typeof headers.getSetCookie === "function" ? headers.getSetCookie() : [];
    for (const c of list) { try { this.jar.setCookieSync(c, url); } catch(e) {} }
  }
  async get(url, extra = {}) {
    const res = await fetch(url, {
      redirect: "follow",
      headers: { "User-Agent": this.ua, "Cookie": this._cookies(url), ...extra },
    });
    this._save(url, res.headers);
    return res;
  }
  async post(url, body, extra = {}) {
    const res = await fetch(url, {
      method: "POST", redirect: "follow",
      headers: {
        "User-Agent": this.ua, "Cookie": this._cookies(url),
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "text/html,application/xhtml+xml,*/*",
        "Origin": CLERK_BASE, "Referer": url,
        ...extra,
      },
      body,
    });
    this._save(url, res.headers);
    return res;
  }
  async postJson(url, body, referer) {
    return this.post(url, body, {
      "X-Requested-With": "XMLHttpRequest",
      "Accept": "application/json, text/javascript, */*; q=0.01",
      "Referer": referer || url,
    });
  }
  async postAjax(url, body, referer) {
    return this.post(url, body, {
      "X-Requested-With": "XMLHttpRequest",
      "Accept": "text/html, */*; q=0.01",
      "Referer": referer || url,
    });
  }
}

// ---------------------------------------------------------------------------
// Session init (accept disclaimer)
// ---------------------------------------------------------------------------
async function initSession(session) {
  log("Loading disclaimer page…");
  const rootRes = await session.get(CLERK_BASE + "/");
  const rootHtml = await rootRes.text();

  // Disclaimer form action
  const actionM = rootHtml.match(/action="([^"]+)"/);
  const action  = actionM ? actionM[1] : "/search/Disclaimer";
  const postUrl = action.startsWith("http") ? action : CLERK_BASE + action;

  log(`Submitting disclaimer at ${postUrl}`);
  const params = new URLSearchParams({ btnButton: "I accept the conditions above." });
  const discRes = await session.post(postUrl, params.toString());
  log(`Disclaimer: HTTP ${discRes.status}`);
  return discRes.status < 400;
}

// ---------------------------------------------------------------------------
// Submit search form (must happen before GridResults)
// ---------------------------------------------------------------------------
async function submitSearch(session, numericId, startDate, endDate) {
  const searchUrl = `${CLERK_BASE}/search/SearchTypeDocType`;

  // First GET to establish search page session
  await session.get(searchUrl);
  await sleep(500);

  // POST search form
  const params = new URLSearchParams({
    DocTypes:       String(numericId),
    RecordDateFrom: fmtMDY(startDate),
    RecordDateTo:   fmtMDY(endDate),
    DateRangeList:  " ",
  });
  const res = await session.postAjax(
    `${searchUrl}?Length=6`,
    params.toString(),
    searchUrl,
  );
  const html = await res.text();

  // Check for "too many results" error
  if (/exceeded.*maximum.*limit/i.test(html)) {
    log("  ⚠ Too many results — switching to 7-day window");
    return "LIMIT";
  }
  return "OK";
}

// ---------------------------------------------------------------------------
// Fetch one page of GridResults JSON
// ---------------------------------------------------------------------------
async function fetchGridPage(session, skip) {
  const gridUrl = `${CLERK_BASE}/Search/GridResults`;
  const params  = new URLSearchParams({
    take:     String(PAGE_SIZE),
    skip:     String(skip),
    page:     String(Math.floor(skip / PAGE_SIZE) + 1),
    pageSize: String(PAGE_SIZE),
  });
  const res  = await session.postJson(gridUrl, params.toString(),
                                      `${CLERK_BASE}/search/SearchTypeDocType`);
  const text = await res.text();
  if (!text || text.length < 10) return null;
  try {
    return JSON.parse(text);
  } catch(e) {
    log(`  JSON parse error: ${e.message} — snippet: ${text.slice(0, 100)}`);
    return null;
  }
}

// ---------------------------------------------------------------------------
// Build a record from a Kendo grid data row
// ---------------------------------------------------------------------------
function rowToRecord(row, code, label, cat, catLabel) {
  // IndirectName = Grantor (seller / debtor) — the motivated seller
  // DirectName   = Grantee (lender / plaintiff)
  const owner   = (row.IndirectName   || row.Grantor || "").trim();
  const grantee = (row.DirectName     || row.Grantee || "").trim();
  const docNum  = String(row.InstrumentNumber || row.NumericInstrumentNumber || "").trim();
  const filed   = parseDate(row.RecordDate || row.FiledDate || row.DocDate || "");
  const amount  = parseAmount(row.Consideration || row.Amount || 0);
  const legal   = (row.DocLegalDescription || row.LegalDescription || "").trim().slice(0, 300);

  // Build clerk URL from instrument number
  const clerkUrl = docNum
    ? `${CLERK_BASE}/Search/DocImages/SearchTypeDocType/${docNum}`
    : `${CLERK_BASE}/search/SearchTypeDocType`;

  return {
    doc_num:     docNum,
    doc_type:    code,
    doc_label:   label,
    filed,
    cat,
    cat_label:   catLabel,
    owner,
    grantee,
    amount,
    legal,
    prop_address: "",
    prop_city:    "",
    prop_state:   "FL",
    prop_zip:     "",
    mail_address: "",
    mail_city:    "",
    mail_state:   "",
    mail_zip:     "",
    clerk_url:    clerkUrl,
    flags:        [],
    score:        0,
  };
}

// ---------------------------------------------------------------------------
// Fetch all pages for a doc type
// ---------------------------------------------------------------------------
async function fetchDocType(session, code, numericId, label, cat, catLabel,
                            startDate, endDate) {
  const records = [];

  // Submit the form to load results into session
  const status = await submitSearch(session, numericId, startDate, endDate);
  if (status === "LIMIT") {
    // Retry with 7-day window
    const shortStart = dateOffset(7);
    const retryStatus = await submitSearch(session, numericId, shortStart, endDate);
    if (retryStatus === "LIMIT") {
      // Still too many — try 3 days
      const shortStart2 = dateOffset(3);
      await submitSearch(session, numericId, shortStart2, endDate);
    }
  }
  await sleep(600);

  // Paginate through GridResults
  let skip  = 0;
  let total = null;
  let page  = 0;

  while (true) {
    page++;
    const data = await fetchGridPage(session, skip);
    if (!data) {
      log(`  No data on page ${page}`);
      break;
    }
    if (total === null) {
      total = data.Total || 0;
      log(`  Total records available: ${total}`);
    }
    const rows = data.Data || [];
    if (!rows.length) break;

    for (const row of rows) {
      records.push(rowToRecord(row, code, label, cat, catLabel));
    }

    skip += rows.length;
    if (skip >= total) break;
    await sleep(DELAY_MS);
  }

  return records;
}

// ---------------------------------------------------------------------------
// Score
// ---------------------------------------------------------------------------
function scoreRecord(rec, allRecords) {
  let score = 30;
  const flags = new Set();
  const code  = rec.doc_type;
  const owner = (rec.owner || "").toUpperCase();

  if (["LP","NTD"].includes(code))                       { score += 10; flags.add("Pre-foreclosure"); }
  if (["TAXDEED","TXDC"].includes(code))                 { score += 10; flags.add("Tax lien"); }
  if (["JDG","JDGR","CCCJUDG","DVJ"].includes(code))    { score += 10; flags.add("Judgment lien"); }
  if (["LN","JVRL"].includes(code))                      { score += 10; flags.add("Mechanic lien"); }
  if (code === "PROB")                                    { score += 10; flags.add("Probate / estate"); }

  // Multiple distress signals for same owner
  if (owner) {
    const ownerDocs = allRecords
      .filter(r => (r.owner || "").toUpperCase() === owner)
      .map(r => r.doc_type);
    const hasForeclosure = ownerDocs.includes("LP");
    const hasTax = ownerDocs.some(d => ["TAXDEED","TXDC","NTD"].includes(d));
    const hasLien = ownerDocs.some(d => ["LN","JVRL","JDG","JDGR"].includes(d));
    if ((hasForeclosure && hasTax) || (hasForeclosure && hasLien)) {
      score += 20;
      flags.add("Multiple distress signals");
    }
  }

  if (rec.amount > 100000)     { score += 15; flags.add("High debt (>$100k)"); }
  else if (rec.amount > 50000) { score += 10; flags.add("Significant debt (>$50k)"); }

  if (rec.filed) {
    const daysAgo = (Date.now() - new Date(rec.filed).getTime()) / 86400000;
    if (daysAgo <= 7) { score += 5; flags.add("New this week"); }
  }

  if (rec.prop_address) { score += 5; flags.add("Has address"); }

  const corpWords = ["LLC","INC","CORP","LTD","TRUST","HOLDINGS","PROPERTIES"];
  if (corpWords.some(w => owner.includes(w))) { score += 10; flags.add("LLC / corp owner"); }

  return { score: Math.min(score, 100), flags: [...flags] };
}

// ---------------------------------------------------------------------------
// GHL CSV
// ---------------------------------------------------------------------------
function writeGhlCsv(records, outPath) {
  const cols = [
    "First Name","Last Name",
    "Mailing Address","Mailing City","Mailing State","Mailing Zip",
    "Property Address","Property City","Property State","Property Zip",
    "Lead Type","Document Type","Date Filed","Document Number",
    "Amount/Debt Owed","Seller Score","Motivated Seller Flags",
    "Source","Public Records URL",
  ];
  const esc = v => `"${String(v == null ? "" : v).replace(/"/g,'""')}"`;
  const lines = [cols.map(esc).join(",")];
  for (const r of records) {
    const [first, last] = splitName(r.owner);
    lines.push([
      first, last,
      r.mail_address, r.mail_city, r.mail_state, r.mail_zip,
      r.prop_address, r.prop_city, r.prop_state, r.prop_zip,
      r.cat_label, r.doc_type, r.filed, r.doc_num,
      r.amount, r.score, (r.flags||[]).join("; "),
      "Duval County Clerk of Courts", r.clerk_url,
    ].map(esc).join(","));
  }
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, lines.join("\n"), "utf8");
  log(`GHL CSV → ${outPath} (${records.length} rows)`);
}

// ---------------------------------------------------------------------------
// Push results to GitHub
// ---------------------------------------------------------------------------
async function pushToGitHub(filePath, repoPath, commitMsg) {
  const TOKEN = process.env.GITHUB_PERSONAL_ACCESS_TOKEN;
  if (!TOKEN) { log("No GITHUB_PERSONAL_ACCESS_TOKEN — skipping push"); return; }

  const REPO = "sailakshmi01/duval-county";
  const content = Buffer.from(fs.readFileSync(filePath)).toString("base64");

  // Get current SHA
  const getRes = await fetch(`https://api.github.com/repos/${REPO}/contents/${repoPath}`, {
    headers: {
      "Authorization": `token ${TOKEN}`,
      "Accept": "application/vnd.github.v3+json",
      "User-Agent": "DuvalScraper/2.0",
    },
  });
  const existing = await getRes.json();

  const putRes = await fetch(`https://api.github.com/repos/${REPO}/contents/${repoPath}`, {
    method: "PUT",
    headers: {
      "Authorization": `token ${TOKEN}`,
      "Accept": "application/vnd.github.v3+json",
      "Content-Type": "application/json",
      "User-Agent": "DuvalScraper/2.0",
    },
    body: JSON.stringify({
      message: commitMsg,
      content,
      sha: existing.sha,
    }),
  });
  const putData = await putRes.json();
  if (putRes.status === 200 || putRes.status === 201) {
    log(`✓ Pushed ${repoPath} to GitHub`);
  } else {
    log(`✗ GitHub push failed for ${repoPath}: ${JSON.stringify(putData).slice(0, 200)}`);
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
(async () => {
  const endDate   = new Date().toISOString().slice(0, 10);
  const startDate = dateOffset(LOOKBACK_DAYS);

  log("=== Duval County Motivated Seller Scraper ===");
  log(`Date range: ${startDate} → ${endDate}  (${LOOKBACK_DAYS} days)`);

  const session = new Session();

  // 1) Accept disclaimer
  const ok = await initSession(session);
  if (!ok) { log("Session init failed."); process.exit(1); }
  await sleep(DELAY_MS);

  // 2) Scrape each doc type
  const allRecords = [];
  for (const [code, [numericId, label, cat, catLabel]] of Object.entries(DOC_TYPES)) {
    log(`\n── ${label} (${code}, id=${numericId})`);
    try {
      const recs = await fetchDocType(
        session, code, numericId, label, cat, catLabel, startDate, endDate
      );
      log(`  → ${recs.length} records`);
      allRecords.push(...recs);
    } catch(e) {
      log(`  ERROR ${code}: ${e.message}`);
    }
    await sleep(DELAY_MS);
  }

  log(`\nTotal raw records: ${allRecords.length}`);

  // 3) Score
  for (const rec of allRecords) {
    const { score, flags } = scoreRecord(rec, allRecords);
    rec.score = score;
    rec.flags = flags;
  }
  allRecords.sort((a, b) => b.score - a.score);

  // 4) Save JSON
  const withAddress = allRecords.filter(r => r.prop_address).length;
  const payload = {
    fetched_at: new Date().toISOString(),
    source: "Duval County Clerk of Courts",
    date_range: `${startDate} to ${endDate}`,
    total: allRecords.length,
    with_address: withAddress,
    records: allRecords,
  };

  for (const outPath of [DASHBOARD_JSON, DATA_JSON]) {
    fs.mkdirSync(path.dirname(outPath), { recursive: true });
    fs.writeFileSync(outPath, JSON.stringify(payload, null, 2), "utf8");
    log(`JSON → ${outPath}`);
  }

  writeGhlCsv(allRecords, GHL_CSV);

  // 5) Push to GitHub
  const commitMsg = `data: scrape ${endDate} — ${allRecords.length} records`;
  await pushToGitHub(DASHBOARD_JSON, "dashboard/records.json", commitMsg);
  await pushToGitHub(DATA_JSON,      "data/records.json",      commitMsg);
  await pushToGitHub(GHL_CSV,        "data/ghl_export.csv",    commitMsg);

  log(`\nDone. ${allRecords.length} records.`);
  log(`Top 5 leads:`);
  for (const r of allRecords.slice(0, 5)) {
    log(`  [${r.score}] ${r.owner || "(no name)"} — ${r.doc_type} — ${r.filed}`);
  }
})();
