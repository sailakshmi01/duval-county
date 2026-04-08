#!/usr/bin/env node
/**
 * Florida Motivated Seller Lead Scraper — Multi-County Edition
 *
 * Supports any Florida county running the Acclaim (Harris) OR portal.
 * Flow per county:
 *  1. Accept disclaimer at /search/Disclaimer
 *  2. GET /search/SearchTypeDocType  (establishes session)
 *  3. POST /search/SearchTypeDocType (initialises Kendo grid search)
 *  4. POST /Search/GridResults       (retrieves paginated JSON)
 *  5. Score + enrich, export JSON + GHL CSV
 */

const { CookieJar } = require("tough-cookie");
const fs   = require("fs");
const path = require("path");

// ---------------------------------------------------------------------------
// County Config — add/remove counties here
// active:true  → scraped every run
// active:false → configured but skipped (set true when URL confirmed)
// ---------------------------------------------------------------------------
const COUNTIES = [
  {
    name:   "Duval",
    state:  "FL",
    base:   "https://or.duvalclerk.com",
    active: true,
  },
  // Jacksonville MSA neighbours — activate once URLs are confirmed
  {
    name:   "Clay",
    state:  "FL",
    base:   "https://or.clayclerk.com",
    active: false,
  },
  {
    name:   "Nassau",
    state:  "FL",
    base:   "https://or.nassauclerk.com",
    active: false,
  },
  {
    name:   "St. Johns",
    state:  "FL",
    base:   "https://or.stjohnsclerk.com",
    active: false,
  },
  {
    name:   "Baker",
    state:  "FL",
    base:   "https://or.bakerclerk.com",
    active: false,
  },
  {
    name:   "Flagler",
    state:  "FL",
    base:   "https://or.flaglerclerk.com",
    active: false,
  },
];

// ---------------------------------------------------------------------------
// Doc Types (all 82 Acclaim codes — keyed to the Duval portal numeric IDs)
// code: [numericId, label, category, categoryLabel, motivatedSellerScore]
// motivatedSellerScore: points added to base score for this doc type alone
// ---------------------------------------------------------------------------
const DOC_TYPES = {
  // ── Pre-Foreclosure ────────────────────────────────────────────────────
  LP:       [104, "Lis Pendens",                  "foreclosure", "Pre-Foreclosure",       25],
  NTD:      [149, "Notice of Tax Deed Sale",      "foreclosure", "Pre-Foreclosure",       20],

  // ── Tax Lien / Tax Deed ────────────────────────────────────────────────
  TAXDEED:  [158, "Tax Deed",                     "tax",         "Tax Lien / Tax Deed",   20],
  TXDC:     [134, "Tax Deed (City Redeemed)",     "tax",         "Tax Lien / Tax Deed",   15],

  // ── Post-Foreclosure (Certificate of Title) ───────────────────────────
  CERTDEED: [83,  "Certificate of Title Deed",    "foreclosure", "Post-Foreclosure Deed", 20],

  // ── Judgments ──────────────────────────────────────────────────────────
  JDG:      [97,  "Judgment",                     "judgment",    "Judgment / Lien",       15],
  JDGR:     [98,  "Judgment / Restitution",       "judgment",    "Judgment / Lien",       15],
  JDGS:     [99,  "Judgment / Sentence",          "judgment",    "Judgment / Lien",       15],
  CCCJUDG:  [79,  "CC Court Judgment",            "judgment",    "Judgment / Lien",       15],
  DVJ:      [145, "Domestic Violence Judgment",   "judgment",    "Judgment / Lien",       10],
  DVJMV:    [159, "DV Judgment (Minor Victim)",   "judgment",    "Judgment / Lien",       10],
  FJPD:     [94,  "Final Jdgmt Public Defender",  "judgment",    "Judgment / Lien",       10],
  RPOFJ:    [146, "RPO Final Judgment",           "judgment",    "Judgment / Lien",       10],
  VAFJ:     [147, "VA Final Judgment",            "judgment",    "Judgment / Lien",       10],

  // ── Liens ──────────────────────────────────────────────────────────────
  LN:       [103, "Lien",                         "lien",        "Lien",                  15],
  JVRL:     [102, "Juvenile Restitution Lien",    "lien",        "Lien",                  10],
  NOTCONT:  [118, "Notice Contest of Lien",       "lien",        "Lien",                  10],
  FS:       [95,  "Finance Statement / UCC",      "lien",        "UCC / Finance Lien",    10],

  // ── Probate / Estate ───────────────────────────────────────────────────
  PROB:     [124, "Probate",                      "probate",     "Probate / Estate",      20],
  DTH:      [91,  "Death Certificate",            "probate",     "Probate / Estate",      15],

  // ── Notice of Commencement ─────────────────────────────────────────────
  NOC:      [115, "Notice of Commencement",       "noc",         "Notice of Commencement", 0],

  // ── Releases (negative — resolved) ────────────────────────────────────
  RELEASE:  [126, "Release",                      "release",     "Release",                0],
  PTL_REL:  [125, "Partial Release",              "release",     "Release",                0],
};

// ---------------------------------------------------------------------------
// Runtime config
// ---------------------------------------------------------------------------
const LOOKBACK_DAYS  = parseInt(process.env.LOOKBACK_DAYS  || "30", 10);
const ACTIVE_COUNTIES = (process.env.COUNTIES || "")
  .split(",").map(s => s.trim()).filter(Boolean);

const PAGE_SIZE  = 200;
const DELAY_MS   = 1000;
const REPO_ROOT  = path.resolve(__dirname, "..");

const DASHBOARD_JSON = path.join(REPO_ROOT, "dashboard", "records.json");
const DATA_JSON      = path.join(REPO_ROOT, "data",      "records.json");
const GHL_CSV        = path.join(REPO_ROOT, "data",      "ghl_export.csv");

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------
function log(msg) {
  console.log(`[${new Date().toISOString().slice(11, 19)}] ${msg}`);
}
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function parseAmount(v) {
  if (v == null) return 0;
  return parseFloat(String(v).replace(/[^0-9.]/g, "")) || 0;
}
function parseDate(v) {
  if (!v) return "";
  const s = String(v);
  const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (iso) return `${iso[1]}-${iso[2]}-${iso[3]}`;
  const compact = s.match(/^(\d{4})(\d{2})(\d{2})$/);
  if (compact) return `${compact[1]}-${compact[2]}-${compact[3]}`;
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
// HTTP Session (cookie jar per county)
// ---------------------------------------------------------------------------
class Session {
  constructor(baseUrl) {
    this.base = baseUrl;
    this.jar  = new CookieJar();
    this.ua   = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36";
  }
  _cookies(url) { return this.jar.getCookieStringSync(url); }
  _save(url, headers) {
    const list = typeof headers.getSetCookie === "function" ? headers.getSetCookie() : [];
    for (const c of list) { try { this.jar.setCookieSync(c, url); } catch(e) {} }
  }
  async get(path, extra = {}) {
    const url = path.startsWith("http") ? path : this.base + path;
    const res = await fetch(url, {
      redirect: "follow",
      signal: AbortSignal.timeout(15000),
      headers: { "User-Agent": this.ua, "Cookie": this._cookies(url), ...extra },
    });
    this._save(url, res.headers);
    return res;
  }
  async post(path, body, extra = {}) {
    const url = path.startsWith("http") ? path : this.base + path;
    const res = await fetch(url, {
      method: "POST", redirect: "follow",
      signal: AbortSignal.timeout(20000),
      headers: {
        "User-Agent": this.ua, "Cookie": this._cookies(url),
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "text/html,*/*",
        "Origin": this.base, "Referer": url,
        ...extra,
      },
      body,
    });
    this._save(url, res.headers);
    return res;
  }
  async postJson(path, body, referer) {
    return this.post(path, body, {
      "X-Requested-With": "XMLHttpRequest",
      "Accept": "application/json, text/javascript, */*; q=0.01",
      "Referer": referer || (this.base + "/search/SearchTypeDocType"),
    });
  }
  async postAjax(path, body, referer) {
    return this.post(path, body, {
      "X-Requested-With": "XMLHttpRequest",
      "Accept": "text/html, */*; q=0.01",
      "Referer": referer || (this.base + "/search/SearchTypeDocType"),
    });
  }
}

// ---------------------------------------------------------------------------
// Accept disclaimer
// ---------------------------------------------------------------------------
async function initSession(session) {
  try {
    const rootRes = await session.get("/");
    const html    = await rootRes.text();
    const actionM = html.match(/action="([^"]+)"/);
    const action  = actionM ? actionM[1] : "/search/Disclaimer";
    const postUrl = action.startsWith("http") ? action : session.base + action;
    const params  = new URLSearchParams({ btnButton: "I accept the conditions above." });
    const r       = await session.post(postUrl, params.toString());
    return r.status < 400;
  } catch(e) {
    log(`  Disclaimer error: ${e.message}`);
    return false;
  }
}

// ---------------------------------------------------------------------------
// Submit search form (primes the Kendo grid session)
// ---------------------------------------------------------------------------
async function submitSearch(session, numericId, startDate, endDate) {
  try {
    await session.get("/search/SearchTypeDocType");
    await sleep(400);
    const params = new URLSearchParams({
      DocTypes:       String(numericId),
      RecordDateFrom: fmtMDY(startDate),
      RecordDateTo:   fmtMDY(endDate),
      DateRangeList:  " ",
    });
    const res  = await session.postAjax("/search/SearchTypeDocType?Length=6", params.toString());
    const html = await res.text();
    if (/exceeded.*maximum.*limit/i.test(html)) return "LIMIT";
    return "OK";
  } catch(e) {
    log(`  submitSearch error: ${e.message}`);
    return "ERROR";
  }
}

// ---------------------------------------------------------------------------
// Fetch one page of GridResults
// ---------------------------------------------------------------------------
async function fetchGridPage(session, skip) {
  const params = new URLSearchParams({
    take: String(PAGE_SIZE), skip: String(skip),
    page: String(Math.floor(skip / PAGE_SIZE) + 1),
    pageSize: String(PAGE_SIZE),
  });
  const res  = await session.postJson("/Search/GridResults", params.toString());
  const text = await res.text();
  if (!text || text.length < 10) return null;
  try { return JSON.parse(text); } catch(e) { return null; }
}

// ---------------------------------------------------------------------------
// Map a grid row → lead record
// ---------------------------------------------------------------------------
function rowToRecord(row, code, label, cat, catLabel, county, state, baseUrl) {
  const owner   = (row.IndirectName   || row.Grantor || "").trim();
  const grantee = (row.DirectName     || row.Grantee || "").trim();
  const docNum  = String(row.InstrumentNumber || row.NumericInstrumentNumber || "").trim();
  const filed   = parseDate(row.RecordDate || row.FiledDate || row.DocDate || "");
  const amount  = parseAmount(row.Consideration || row.Amount || 0);
  const legal   = (row.DocLegalDescription || row.LegalDescription || "").trim().slice(0, 300);
  const clerkUrl = docNum
    ? `${baseUrl}/Search/DocImages/SearchTypeDocType/${docNum}`
    : `${baseUrl}/search/SearchTypeDocType`;

  return {
    doc_num: docNum, doc_type: code, doc_label: label,
    filed, cat, cat_label: catLabel,
    county, state,
    owner, grantee, amount, legal,
    prop_address: "", prop_city: "", prop_state: state, prop_zip: "",
    mail_address: "", mail_city: "", mail_state: "",    mail_zip: "",
    clerk_url: clerkUrl,
    flags: [], score: 0,
  };
}

// ---------------------------------------------------------------------------
// Scrape one doc type for one county
// ---------------------------------------------------------------------------
async function fetchDocType(session, county, state, baseUrl,
                             code, numericId, label, cat, catLabel,
                             startDate, endDate) {
  const records = [];

  // Try primary date range, fall back to shorter if too many results
  let status = await submitSearch(session, numericId, startDate, endDate);
  if (status === "LIMIT") {
    log(`  ⚠ Too many results — trying 7-day window`);
    status = await submitSearch(session, numericId, dateOffset(7), endDate);
  }
  if (status === "LIMIT") {
    log(`  ⚠ Still too many — trying 3-day window`);
    status = await submitSearch(session, numericId, dateOffset(3), endDate);
  }
  if (status === "ERROR") return records;
  await sleep(500);

  let skip = 0, total = null, page = 0;
  while (true) {
    page++;
    const data = await fetchGridPage(session, skip);
    if (!data) { log(`  No data on page ${page}`); break; }
    if (total === null) {
      total = data.Total || 0;
      if (total > 0) log(`  ${total} records available`);
    }
    const rows = data.Data || [];
    if (!rows.length) break;
    for (const row of rows) {
      records.push(rowToRecord(row, code, label, cat, catLabel, county, state, baseUrl));
    }
    skip += rows.length;
    if (skip >= total) break;
    await sleep(DELAY_MS);
  }
  return records;
}

// ---------------------------------------------------------------------------
// Scrape one county
// ---------------------------------------------------------------------------
async function scrapeCounty(countyConfig, startDate, endDate) {
  const { name, state, base } = countyConfig;
  log(`\n${"═".repeat(50)}`);
  log(`County: ${name}, ${state}  |  ${base}`);
  log(`${"═".repeat(50)}`);

  const session = new Session(base);
  const ok = await initSession(session);
  if (!ok) {
    log(`  Session init failed — skipping ${name}`);
    return [];
  }
  await sleep(DELAY_MS);

  const allRecords = [];
  for (const [code, [numericId, label, cat, catLabel, _pts]] of Object.entries(DOC_TYPES)) {
    log(`  ── ${label} (${code})`);
    try {
      const recs = await fetchDocType(
        session, name, state, base,
        code, numericId, label, cat, catLabel,
        startDate, endDate,
      );
      if (recs.length) log(`     → ${recs.length} records`);
      allRecords.push(...recs);
    } catch(e) {
      log(`     ERROR: ${e.message}`);
    }
    await sleep(DELAY_MS);
  }
  return allRecords;
}

// ---------------------------------------------------------------------------
// Scoring
// ---------------------------------------------------------------------------
function scoreRecord(rec, allRecords) {
  let score = 20;
  const flags = new Set();
  const code  = rec.doc_type;
  const owner = (rec.owner || "").toUpperCase();

  // Doc type base score
  const [, , , , pts] = DOC_TYPES[code] || [0, "", "", "", 0];
  score += pts;

  // Categorised flags
  if (["LP","NTD","CERTDEED"].includes(code))                         flags.add("Pre/post-foreclosure");
  if (["TAXDEED","TXDC"].includes(code))                              flags.add("Tax lien");
  if (["JDG","JDGR","JDGS","CCCJUDG","DVJ","DVJMV","FJPD","RPOFJ","VAFJ"].includes(code)) flags.add("Judgment lien");
  if (["LN","JVRL","NOTCONT","FS"].includes(code))                    flags.add("Lien / UCC");
  if (["PROB","DTH"].includes(code))                                  flags.add("Probate / estate");

  // Multiple distress signals for same owner
  if (owner) {
    const ownerDocs = allRecords
      .filter(r => (r.owner || "").toUpperCase() === owner && r.county === rec.county)
      .map(r => r.doc_type);
    const n = ownerDocs.length;
    if (n >= 3) { score += 25; flags.add(`${n} distress docs`); }
    else if (n === 2) { score += 15; flags.add("Multiple distress docs"); }

    const hasForeclosure = ownerDocs.some(d => ["LP","NTD","CERTDEED","TAXDEED","TXDC"].includes(d));
    const hasLien        = ownerDocs.some(d => ["LN","JVRL","JDG","JDGR","JDGS","CCCJUDG"].includes(d));
    const hasProb        = ownerDocs.some(d => ["PROB","DTH"].includes(d));
    if (hasForeclosure && hasLien)  { score += 10; flags.add("Foreclosure + lien"); }
    if (hasForeclosure && hasProb)  { score += 10; flags.add("Foreclosure + probate"); }
  }

  // Amount
  if (rec.amount > 200000)    { score += 20; flags.add("High debt (>$200k)"); }
  else if (rec.amount > 100000) { score += 15; flags.add("High debt (>$100k)"); }
  else if (rec.amount > 50000)  { score += 10; flags.add("Debt >$50k"); }

  // Recency
  if (rec.filed) {
    const days = (Date.now() - new Date(rec.filed).getTime()) / 86400000;
    if (days <= 3)  { score += 10; flags.add("Filed last 3 days"); }
    else if (days <= 7)  { score += 5;  flags.add("New this week"); }
  }

  // Property address known
  if (rec.prop_address) { score += 5; flags.add("Has address"); }

  // LLC / corp (often motivated to liquidate)
  const corps = ["LLC","INC","CORP","LTD","TRUST","HOLDINGS","PROPERTIES","GROUP","INVESTMENTS"];
  if (corps.some(w => owner.includes(w))) { score += 10; flags.add("LLC / corp owner"); }

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
    "County","Lead Type","Document Type","Date Filed","Document Number",
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
      r.county, r.cat_label, r.doc_type, r.filed, r.doc_num,
      r.amount, r.score, (r.flags||[]).join("; "),
      `${r.county} County Clerk of Courts — ${r.state}`, r.clerk_url,
    ].map(esc).join(","));
  }
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, lines.join("\n"), "utf8");
  log(`GHL CSV → ${outPath} (${records.length} rows)`);
}

// ---------------------------------------------------------------------------
// Push file to GitHub via API
// ---------------------------------------------------------------------------
async function pushToGitHub(localPath, repoPath, commitMsg) {
  const TOKEN = process.env.GITHUB_PERSONAL_ACCESS_TOKEN;
  if (!TOKEN) return;
  const REPO    = process.env.GITHUB_REPO || "sailakshmi01/duval-county";
  const content = Buffer.from(fs.readFileSync(localPath)).toString("base64");
  const getRes  = await fetch(`https://api.github.com/repos/${REPO}/contents/${repoPath}`, {
    headers: { "Authorization": `token ${TOKEN}`, "Accept": "application/vnd.github.v3+json", "User-Agent": "Scraper/3.0" },
  });
  const existing = await getRes.json();
  const putRes = await fetch(`https://api.github.com/repos/${REPO}/contents/${repoPath}`, {
    method: "PUT",
    headers: {
      "Authorization": `token ${TOKEN}`, "Accept": "application/vnd.github.v3+json",
      "Content-Type": "application/json", "User-Agent": "Scraper/3.0",
    },
    body: JSON.stringify({ message: commitMsg, content, sha: existing.sha }),
  });
  const ok = putRes.status === 200 || putRes.status === 201;
  log(`${ok ? "✓" : "✗"} GitHub push: ${repoPath} (HTTP ${putRes.status})`);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
(async () => {
  const endDate   = new Date().toISOString().slice(0, 10);
  const startDate = dateOffset(LOOKBACK_DAYS);

  log("=== Florida Motivated Seller Lead Scraper v3 ===");
  log(`Date range: ${startDate} → ${endDate}  (${LOOKBACK_DAYS} days)`);
  log(`Doc types: ${Object.keys(DOC_TYPES).length}`);

  // Which counties to run
  const toRun = COUNTIES.filter(c => {
    if (ACTIVE_COUNTIES.length) return ACTIVE_COUNTIES.includes(c.name);
    return c.active;
  });
  log(`Counties: ${toRun.map(c => c.name).join(", ")}`);

  // Scrape each county
  let allRecords = [];
  for (const county of toRun) {
    const recs = await scrapeCounty(county, startDate, endDate);
    allRecords.push(...recs);
    log(`\n${county.name}: ${recs.length} records`);
  }

  log(`\nTotal raw records: ${allRecords.length}`);

  // Score
  for (const rec of allRecords) {
    const { score, flags } = scoreRecord(rec, allRecords);
    rec.score = score;
    rec.flags = flags;
  }
  allRecords.sort((a, b) => b.score - a.score);

  // Deduplicate (same doc_num + county)
  const seen = new Set();
  allRecords = allRecords.filter(r => {
    const key = `${r.county}:${r.doc_num}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
  log(`After dedup: ${allRecords.length} records`);

  // County summary
  const byCounty = {};
  for (const r of allRecords) byCounty[r.county] = (byCounty[r.county] || 0) + 1;
  const byType   = {};
  for (const r of allRecords) byType[r.doc_type]   = (byType[r.doc_type]   || 0) + 1;

  const payload = {
    fetched_at:  new Date().toISOString(),
    source:      "Florida County Clerks of Courts",
    date_range:  `${startDate} to ${endDate}`,
    total:       allRecords.length,
    with_address: allRecords.filter(r => r.prop_address).length,
    by_county:   byCounty,
    by_type:     byType,
    records:     allRecords,
  };

  for (const outPath of [DASHBOARD_JSON, DATA_JSON]) {
    fs.mkdirSync(path.dirname(outPath), { recursive: true });
    fs.writeFileSync(outPath, JSON.stringify(payload, null, 2), "utf8");
    log(`JSON → ${outPath}`);
  }
  writeGhlCsv(allRecords, GHL_CSV);

  // Push to GitHub
  const msg = `data: ${allRecords.length} leads — ${endDate}`;
  await pushToGitHub(DASHBOARD_JSON, "dashboard/records.json", msg);
  await pushToGitHub(DATA_JSON,      "data/records.json",      msg);
  await pushToGitHub(GHL_CSV,        "data/ghl_export.csv",    msg);

  log(`\nDone. ${allRecords.length} records.`);
  log(`By county: ${JSON.stringify(byCounty)}`);
  log(`Top 5 leads:`);
  for (const r of allRecords.slice(0, 5)) {
    log(`  [${r.score}] ${r.owner || "(no name)"} | ${r.county} | ${r.doc_type} | ${r.filed}`);
  }
})();
