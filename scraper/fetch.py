#!/usr/bin/env python3
"""
Duval County, Florida — Motivated Seller Lead Scraper
Scrapes the Duval County Clerk portal for distressed-property documents,
enriches records with parcel data, scores them, and exports JSON + GHL CSV.
"""

import asyncio
import csv
import json
import logging
import os
import re
import sys
import tempfile
import time
import traceback
import zipfile
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("duval_scraper")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CLERK_BASE = "https://or.duvalclerk.com"
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# ---------------------------------------------------------------------------
# Doc type mapping: internal code → (portal numeric ID, label, category)
# IDs sourced directly from the Kendo ComboBox data on the clerk portal.
# ---------------------------------------------------------------------------
DOC_TYPE_CONFIG = {
    # code: (numeric_id, label, category, cat_label)
    "LP":       (104, "Lis Pendens",              "foreclosure", "Pre-Foreclosure / Lis Pendens"),
    "NTD":      (149, "Notice of Tax Deed Sale",  "foreclosure", "Pre-Foreclosure / Lis Pendens"),
    "TAXDEED":  (158, "Tax Deed",                 "tax",         "Tax Lien / Tax Deed"),
    "TXDC":     (134, "Tax Deed (City Redeemed)", "tax",         "Tax Lien / Tax Deed"),
    "JDG":      (97,  "Judgment",                 "judgment",    "Judgment / Lien"),
    "JDGR":     (98,  "Judgment/Restitution",     "judgment",    "Judgment / Lien"),
    "CCCJUDG":  (79,  "CC Court Judgment",        "judgment",    "Judgment / Lien"),
    "DVJ":      (145, "Domestic Violence Judgment","judgment",   "Judgment / Lien"),
    "LN":       (103, "Lien",                     "lien",        "Lien"),
    "JVRL":     (102, "Juvenile Restitution Lien","lien",        "Lien"),
    "PROB":     (124, "Probate",                  "probate",     "Probate / Estate"),
    "NOC":      (115, "Notice of Commencement",   "noc",         "Notice of Commencement"),
    "RELEASE":  (126, "Release",                  "release",     "Release"),
    "PTL_REL":  (125, "Partial Release",          "release",     "Release"),
}

# Output paths
REPO_ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_JSON = REPO_ROOT / "dashboard" / "records.json"
DATA_JSON = REPO_ROOT / "data" / "records.json"
GHL_CSV = REPO_ROOT / "data" / "ghl_export.csv"

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def parse_amount(text: str) -> float:
    if not text:
        return 0.0
    cleaned = re.sub(r"[^\d.]", "", text.replace(",", ""))
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def split_name(full_name: str):
    full_name = (full_name or "").strip()
    if "," in full_name:
        parts = full_name.split(",", 1)
        last = parts[0].strip().title()
        first = parts[1].strip().title()
    else:
        parts = full_name.split()
        if len(parts) >= 2:
            first = parts[0].title()
            last = " ".join(parts[1:]).title()
        else:
            first = full_name.title()
            last = ""
    return first, last

# ---------------------------------------------------------------------------
# Property Appraiser — bulk DBF loader
# ---------------------------------------------------------------------------

class ParcelLookup:
    """
    Downloads the Duval County Property Appraiser bulk parcel DBF and builds
    an owner-name lookup keyed by all name variants.
    """

    # Try multiple known PA data URLs
    PA_PAGES = [
        "https://www.coj.net/departments/property-appraiser/property-data",
        "https://www.coj.net/departments/property-appraiser/property-data.aspx",
        "https://paopropertysearch.coj.net/Basic/Download.aspx",
    ]

    def __init__(self):
        self._by_name: dict = {}

    def _get_dbf_zip_url(self) -> Optional[str]:
        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0 (DuvalLeadScraper/2.0)"})
        for pa_url in self.PA_PAGES:
            try:
                resp = session.get(pa_url, timeout=30)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.text, "lxml")
                    for a in soup.find_all("a", href=True):
                        href = a["href"]
                        if re.search(r"\.(zip|ZIP)$", href):
                            if href.startswith("http"):
                                return href
                            # Try to build absolute URL
                            from urllib.parse import urljoin
                            return urljoin(pa_url, href)
            except Exception as exc:
                log.debug("PA page %s failed: %s", pa_url, exc)
        # Hard-coded fallback if known
        return None

    def _download_zip(self, url: str) -> Optional[bytes]:
        try:
            resp = requests.get(url, timeout=120, stream=True)
            resp.raise_for_status()
            return resp.content
        except Exception as exc:
            log.warning("ZIP download failed: %s", exc)
            return None

    def _extract_dbf(self, zip_bytes: bytes) -> Optional[Path]:
        priority = ["NAL.dbf", "nal.dbf", "parcel.dbf", "PARCEL.dbf"]
        try:
            with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
                names = zf.namelist()
                for target in priority:
                    for name in names:
                        if Path(name).name.lower() == target.lower():
                            tmp = tempfile.NamedTemporaryFile(suffix=".dbf", delete=False)
                            tmp.write(zf.read(name))
                            tmp.close()
                            return Path(tmp.name)
                for name in names:
                    if name.lower().endswith(".dbf"):
                        tmp = tempfile.NamedTemporaryFile(suffix=".dbf", delete=False)
                        tmp.write(zf.read(name))
                        tmp.close()
                        return Path(tmp.name)
        except Exception as exc:
            log.warning("DBF extract failed: %s", exc)
        return None

    def load(self) -> bool:
        try:
            from dbfread import DBF
        except ImportError:
            log.error("dbfread not installed")
            return False

        zip_url = self._get_dbf_zip_url()
        if not zip_url:
            log.warning("PA bulk data URL not found — address enrichment disabled.")
            return False

        log.info("Downloading parcel DBF from %s", zip_url)
        zip_bytes = self._download_zip(zip_url)
        if not zip_bytes:
            return False

        dbf_path = self._extract_dbf(zip_bytes)
        if not dbf_path:
            log.warning("No DBF found in ZIP")
            return False

        log.info("Parsing parcel DBF…")
        try:
            table = DBF(str(dbf_path), encoding="latin-1", ignore_missing_memofile=True)
            count = 0
            for row in table:
                try:
                    self._index_row(dict(row))
                    count += 1
                except Exception:
                    pass
            log.info("Indexed %d parcel records", count)
        except Exception as exc:
            log.error("DBF parse error: %s", exc)
            return False
        finally:
            try:
                dbf_path.unlink()
            except Exception:
                pass
        return True

    def _norm_key(self, name: str) -> str:
        return re.sub(r"\s+", " ", name.strip().upper())

    def _index_row(self, row: dict):
        def g(*keys):
            for k in keys:
                for variant in [k, k.upper(), k.lower()]:
                    v = row.get(variant)
                    if v and str(v).strip():
                        return str(v).strip()
            return ""

        owner_raw = g("OWN1", "OWNER", "OWNER1", "OWN_NAME")
        if not owner_raw:
            return

        parcel = {
            "owner_raw": owner_raw,
            "site_addr": g("SITEADDR", "SITE_ADDR"),
            "site_city": g("SITE_CITY", "SITECITY"),
            "site_state": g("SITE_STATE") or "FL",
            "site_zip": g("SITE_ZIP", "SITEZIP"),
            "mail_addr": g("MAILADR1", "ADDR_1", "MAILADD1"),
            "mail_city": g("MAILCITY", "CITY"),
            "mail_state": g("STATE", "MAIL_STATE") or "FL",
            "mail_zip": g("MAILZIP", "ZIP"),
        }

        raw = owner_raw.upper().strip()
        variants = {raw}
        if "," in raw:
            last_part, first_part = raw.split(",", 1)
            variants.add(first_part.strip() + " " + last_part.strip())
        else:
            parts = raw.split()
            if len(parts) >= 2:
                variants.add(parts[-1] + " " + " ".join(parts[:-1]))
                variants.add(parts[-1] + ", " + " ".join(parts[:-1]))

        for key in variants:
            norm = self._norm_key(key)
            if norm:
                self._by_name.setdefault(norm, []).append(parcel)

    def lookup(self, owner: str) -> Optional[dict]:
        if not owner:
            return None
        key = self._norm_key(owner)
        if key in self._by_name:
            return self._by_name[key][0]
        tokens = key.split()
        if len(tokens) >= 2:
            partial = tokens[0] + " " + tokens[1]
            for stored_key, parcels in self._by_name.items():
                if partial in stored_key:
                    return parcels[0]
        return None

# ---------------------------------------------------------------------------
# Seller Score
# ---------------------------------------------------------------------------

def calculate_score(record: dict, all_records: list) -> tuple:
    flags = []
    score = 30

    doc_type = record.get("doc_type", "")
    cat = record.get("cat", "")
    amount = record.get("amount", 0.0)
    filed = record.get("filed", "")
    owner = record.get("owner", "")
    prop_address = record.get("prop_address", "")
    owner_upper = owner.upper()

    if doc_type in ("LP", "NTD"):
        flags.append("Lis pendens")
        flags.append("Pre-foreclosure")
        score += 10

    if doc_type in ("TAXDEED", "TXDC"):
        flags.append("Tax lien")
        score += 10

    if doc_type in ("JDG", "JDGR", "CCCJUDG", "DVJ"):
        flags.append("Judgment lien")
        score += 10

    if doc_type in ("LN", "JVRL"):
        flags.append("Mechanic lien")
        score += 10

    if doc_type == "PROB":
        flags.append("Probate / estate")
        score += 10

    # LP + related foreclosure combo
    owner_docs = [r.get("doc_type") for r in all_records if r.get("owner", "").upper() == owner_upper]
    has_lp = "LP" in owner_docs
    has_ntd = "NTD" in owner_docs
    if has_lp and (has_ntd or any(d in ("TAXDEED", "TXDC") for d in owner_docs)):
        score += 20

    if amount > 100_000:
        flags.append("High debt (>$100k)")
        score += 15
    elif amount > 50_000:
        flags.append("Significant debt (>$50k)")
        score += 10

    try:
        filed_date = datetime.strptime(filed, "%Y-%m-%d")
        if (datetime.now() - filed_date).days <= 7:
            flags.append("New this week")
            score += 5
    except Exception:
        pass

    if prop_address:
        flags.append("Has address")
        score += 5

    corp_keywords = ["LLC", "INC", "CORP", "LTD", "TRUST", "HOLDINGS", "PROPERTIES"]
    if any(kw in owner_upper for kw in corp_keywords):
        flags.append("LLC / corp owner")
        score += 10

    seen, unique_flags = set(), []
    for f in flags:
        if f not in seen:
            unique_flags.append(f)
            seen.add(f)

    return min(score, 100), unique_flags

# ---------------------------------------------------------------------------
# Clerk Portal Scraper (Playwright)
# ---------------------------------------------------------------------------

class ClerkScraper:
    """
    Uses Playwright to navigate the Duval County Clerk portal.
    - Accepts the disclaimer automatically
    - Uses the Doc Type search page (/search/SearchTypeDocType)
    - Interacts with Kendo UI widgets via JavaScript evaluate()
    - Handles AJAX form submission and paginated results
    """

    SEARCH_URL = f"{CLERK_BASE}/search/SearchTypeDocType"
    TIMEOUT = 45_000  # ms

    def __init__(self, start_date: str, end_date: str):
        self.start_date = start_date  # YYYY-MM-DD
        self.end_date = end_date

    async def fetch_all(self) -> list:
        all_records = []
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                           "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
            page = await context.new_page()
            page.set_default_timeout(self.TIMEOUT)

            # Accept the disclaimer once
            accepted = await self._accept_disclaimer(page)
            if not accepted:
                log.error("Could not accept disclaimer — aborting.")
                await browser.close()
                return []

            for code, (numeric_id, label, cat, cat_label) in DOC_TYPE_CONFIG.items():
                log.info("Fetching: %s (%s, id=%d)", label, code, numeric_id)
                try:
                    records = await self._fetch_doc_type(page, code, numeric_id, label, cat, cat_label)
                    log.info("  → %d records", len(records))
                    all_records.extend(records)
                except Exception:
                    log.error("Error fetching %s:\n%s", code, traceback.format_exc())

            await browser.close()
        return all_records

    # ------------------------------------------------------------------
    # Disclaimer
    # ------------------------------------------------------------------

    async def _accept_disclaimer(self, page) -> bool:
        """Navigate to the portal root and click 'I accept the conditions above.'"""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                await page.goto(CLERK_BASE + "/", timeout=self.TIMEOUT)
                await page.wait_for_load_state("networkidle", timeout=self.TIMEOUT)

                # Click accept button if present
                btn = await page.query_selector("input[value*='accept']")
                if not btn:
                    btn = await page.query_selector("input#btnButton")
                if not btn:
                    btn = await page.query_selector("button:has-text('accept')")

                if btn:
                    await btn.click()
                    await page.wait_for_load_state("networkidle", timeout=self.TIMEOUT)
                    log.info("Disclaimer accepted.")
                else:
                    # Already past the disclaimer
                    log.info("Disclaimer not shown (already accepted).")
                return True
            except Exception as exc:
                log.warning("Disclaimer attempt %d/%d failed: %s", attempt, MAX_RETRIES, exc)
                await asyncio.sleep(RETRY_DELAY)
        return False

    # ------------------------------------------------------------------
    # Fetch one doc type
    # ------------------------------------------------------------------

    async def _fetch_doc_type(self, page, code: str, numeric_id: int,
                               label: str, cat: str, cat_label: str) -> list:
        records = []

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                await page.goto(self.SEARCH_URL, timeout=self.TIMEOUT)
                await page.wait_for_load_state("networkidle", timeout=self.TIMEOUT)
                # Brief pause for Kendo widgets to initialize
                await asyncio.sleep(1.5)
                break
            except Exception as exc:
                log.warning("Page load attempt %d/%d: %s", attempt, MAX_RETRIES, exc)
                if attempt == MAX_RETRIES:
                    return records
                await asyncio.sleep(RETRY_DELAY)

        try:
            await self._set_doc_type(page, numeric_id)
            await self._set_date_range(page)
            await self._submit_form(page)
            await asyncio.sleep(2)  # allow AJAX to settle
        except Exception:
            log.error("Form setup error for %s:\n%s", code, traceback.format_exc())
            return records

        # Collect all pages of results
        page_num = 0
        while True:
            page_num += 1
            try:
                page_records = await self._parse_results(page, code, label, cat, cat_label)
                records.extend(page_records)

                has_next = await self._go_next_page(page)
                if not has_next:
                    break
                await page.wait_for_load_state("networkidle", timeout=self.TIMEOUT)
                await asyncio.sleep(1)
            except Exception:
                log.error("Parse error on page %d for %s:\n%s",
                          page_num, code, traceback.format_exc())
                break

        return records

    # ------------------------------------------------------------------
    # Form interaction
    # ------------------------------------------------------------------

    async def _set_doc_type(self, page, numeric_id: int):
        """Set the Kendo ComboBox value using its JavaScript API."""
        # The DocTypes hidden textarea stores the actual value (numeric ID)
        # DocTypesDisplay is the visible Kendo ComboBox
        result = await page.evaluate(f"""
            () => {{
                try {{
                    var combo = jQuery('#DocTypesDisplay').data('kendoComboBox');
                    if (combo) {{
                        combo.value('{numeric_id}');
                        combo.trigger('change');
                        // Also set the hidden textarea
                        jQuery('#DocTypes').val('{numeric_id}');
                        return 'kendo-ok';
                    }}
                    // Fallback: set the hidden textarea directly
                    jQuery('#DocTypes').val('{numeric_id}');
                    return 'fallback-ok';
                }} catch(e) {{
                    return 'error: ' + e.toString();
                }}
            }}
        """)
        log.debug("Doc type set result: %s", result)

    async def _set_date_range(self, page):
        """Set the date range to 'Last 7 Days' via Kendo DropDownList."""
        start_fmt = datetime.strptime(self.start_date, "%Y-%m-%d").strftime("%m/%d/%Y")
        end_fmt = datetime.strptime(self.end_date, "%Y-%m-%d").strftime("%m/%d/%Y")

        result = await page.evaluate(f"""
            () => {{
                try {{
                    // Try Kendo date range dropdown first
                    var ddl = jQuery('#DateRangeDropDown').data('kendoDropDownList');
                    if (ddl) {{
                        ddl.value('Last7Days');
                        ddl.trigger('change');
                        return 'kendo-date-ok';
                    }}
                    // Fallback: set date fields directly
                    jQuery('#RecordDateFrom').val('{start_fmt}');
                    jQuery('#RecordDateTo').val('{end_fmt}');
                    return 'date-fields-ok';
                }} catch(e) {{
                    return 'error: ' + e.toString();
                }}
            }}
        """)
        log.debug("Date range set result: %s", result)
        await asyncio.sleep(0.5)

        # Also try to fill the date inputs directly as backup
        for sel, val in [("#RecordDateFrom", start_fmt), ("#RecordDateTo", end_fmt)]:
            try:
                inp = await page.query_selector(sel)
                if inp:
                    await inp.fill(val)
            except Exception:
                pass

    async def _submit_form(self, page):
        """Click the search button to submit the form."""
        btn_selectors = [
            "input[type='submit']",
            "button[type='submit']",
            "input.t-button[value*='Search']",
            "button:has-text('Search')",
            "#btnSearch",
        ]
        for sel in btn_selectors:
            try:
                btn = await page.query_selector(sel)
                if btn:
                    await btn.click()
                    await page.wait_for_load_state("networkidle", timeout=self.TIMEOUT)
                    return
            except Exception:
                pass

        # Try form submit via JS
        await page.evaluate("document.getElementById('schfrm') && document.getElementById('schfrm').submit()")
        await page.wait_for_load_state("networkidle", timeout=self.TIMEOUT)

    # ------------------------------------------------------------------
    # Results parsing
    # ------------------------------------------------------------------

    async def _parse_results(self, page, code: str, label: str, cat: str, cat_label: str) -> list:
        records = []
        try:
            content = await page.content()
            current_url = page.url
            soup = BeautifulSoup(content, "lxml")

            # Look for "No records found" indicator
            text = soup.get_text()
            if re.search(r"no records found|0 records|no results", text, re.I):
                return records

            # Find the results table (Kendo grid or standard table)
            table = (
                soup.find("table", class_=re.compile(r"k-grid|result|search", re.I))
                or soup.find("div", class_="k-grid-content")
                or soup.find("table")
            )
            if not table:
                # Try a div-based layout
                return self._parse_div_results(soup, code, label, cat, cat_label, current_url)

            rows = table.find_all("tr")
            if len(rows) < 2:
                return records

            # Headers
            headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]

            def col(cells, *names):
                for name in names:
                    for i, h in enumerate(headers):
                        if name in h and i < len(cells):
                            txt = cells[i].get_text(strip=True)
                            if txt:
                                return txt
                return ""

            for row in rows[1:]:
                cells = row.find_all("td")
                if not cells or len(cells) < 2:
                    continue
                try:
                    # Instrument / doc number — prefer linked text
                    doc_num = ""
                    clerk_url = ""
                    for cell in cells:
                        a = cell.find("a", href=True)
                        if a:
                            href = a["href"]
                            if not href.startswith("http"):
                                href = CLERK_BASE + href
                            clerk_url = href
                            doc_num = a.get_text(strip=True)
                            break
                    if not doc_num:
                        doc_num = col(cells, "instrument", "doc", "number", "book")

                    # Filed date
                    filed_raw = col(cells, "record date", "filed", "date")
                    filed = self._parse_date(filed_raw)

                    # Grantor (owner)
                    owner = col(cells, "grantor", "owner", "party 1", "name")

                    # Grantee
                    grantee = col(cells, "grantee", "buyer", "party 2")

                    # Amount / consideration
                    amount_raw = col(cells, "consideration", "amount", "value", "debt")
                    amount = parse_amount(amount_raw)

                    # Legal description
                    legal = col(cells, "legal", "description", "property")

                    if not doc_num and not owner:
                        continue

                    records.append({
                        "doc_num": doc_num,
                        "doc_type": code,
                        "filed": filed,
                        "cat": cat,
                        "cat_label": cat_label,
                        "owner": owner,
                        "grantee": grantee,
                        "amount": amount,
                        "legal": legal,
                        "prop_address": "",
                        "prop_city": "",
                        "prop_state": "FL",
                        "prop_zip": "",
                        "mail_address": "",
                        "mail_city": "",
                        "mail_state": "",
                        "mail_zip": "",
                        "clerk_url": clerk_url or current_url,
                        "flags": [],
                        "score": 0,
                    })
                except Exception:
                    log.debug("Row error: %s", traceback.format_exc())

        except Exception:
            log.error("Results parse error:\n%s", traceback.format_exc())
        return records

    def _parse_div_results(self, soup, code, label, cat, cat_label, current_url) -> list:
        """Fallback: parse div-based result layouts (e.g. Kendo listview)."""
        records = []
        divs = soup.find_all("div", class_=re.compile(r"result|record|item", re.I))
        for div in divs:
            try:
                text = div.get_text(separator=" | ", strip=True)
                a = div.find("a", href=True)
                clerk_url = ""
                if a:
                    href = a["href"]
                    if not href.startswith("http"):
                        href = CLERK_BASE + href
                    clerk_url = href

                # Try to extract instrument number from the link or text
                doc_num_match = re.search(r"(\d{7,})", text)
                doc_num = doc_num_match.group(1) if doc_num_match else ""
                if not doc_num and not clerk_url:
                    continue

                records.append({
                    "doc_num": doc_num,
                    "doc_type": code,
                    "filed": "",
                    "cat": cat,
                    "cat_label": cat_label,
                    "owner": "",
                    "grantee": "",
                    "amount": 0.0,
                    "legal": "",
                    "prop_address": "",
                    "prop_city": "",
                    "prop_state": "FL",
                    "prop_zip": "",
                    "mail_address": "",
                    "mail_city": "",
                    "mail_state": "",
                    "mail_zip": "",
                    "clerk_url": clerk_url or current_url,
                    "flags": [],
                    "score": 0,
                })
            except Exception:
                pass
        return records

    @staticmethod
    def _parse_date(text: str) -> str:
        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%m/%d/%y"):
            try:
                return datetime.strptime(text.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                pass
        return ""

    # ------------------------------------------------------------------
    # Pagination
    # ------------------------------------------------------------------

    async def _go_next_page(self, page) -> bool:
        """Try to advance to the next results page. Returns True if navigated."""
        # Kendo Grid pager
        next_selectors = [
            ".k-pager-next:not(.k-state-disabled)",
            "a.k-i-arrow-e:not(.k-state-disabled)",
            "a[title='Go to the next page']",
            "a:has-text('Next')",
            ".t-arrow-next:not(.t-state-disabled)",
        ]
        for sel in next_selectors:
            try:
                btn = await page.query_selector(sel)
                if btn:
                    prev_url = page.url
                    await btn.click()
                    await asyncio.sleep(1.5)
                    await page.wait_for_load_state("networkidle", timeout=self.TIMEOUT)
                    return True
            except Exception:
                pass

        # ASP.NET __doPostBack pagination
        try:
            content = await page.content()
            soup = BeautifulSoup(content, "lxml")
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                text = a.get_text(strip=True)
                if "__doPostBack" in href and re.search(r"next|>|»", text, re.I):
                    await page.evaluate(f"eval(unescape('{href}'))")
                    await page.wait_for_load_state("networkidle", timeout=self.TIMEOUT)
                    return True
        except Exception:
            pass

        return False

# ---------------------------------------------------------------------------
# Enrich with parcel data
# ---------------------------------------------------------------------------

def enrich_with_parcel(records: list, parcel: ParcelLookup) -> list:
    for rec in records:
        try:
            match = parcel.lookup(rec.get("owner", ""))
            if match:
                rec["prop_address"] = match.get("site_addr", "")
                rec["prop_city"]    = match.get("site_city", "")
                rec["prop_state"]   = match.get("site_state", "FL")
                rec["prop_zip"]     = match.get("site_zip", "")
                rec["mail_address"] = match.get("mail_addr", "")
                rec["mail_city"]    = match.get("mail_city", "")
                rec["mail_state"]   = match.get("mail_state", "FL")
                rec["mail_zip"]     = match.get("mail_zip", "")
        except Exception:
            pass
    return records

# ---------------------------------------------------------------------------
# GHL CSV Export
# ---------------------------------------------------------------------------

GHL_COLUMNS = [
    "First Name", "Last Name",
    "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
    "Property Address", "Property City", "Property State", "Property Zip",
    "Lead Type", "Document Type", "Date Filed", "Document Number",
    "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
    "Source", "Public Records URL",
]


def write_ghl_csv(records: list, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=GHL_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for rec in records:
            first, last = split_name(rec.get("owner", ""))
            writer.writerow({
                "First Name": first,
                "Last Name": last,
                "Mailing Address": rec.get("mail_address", ""),
                "Mailing City": rec.get("mail_city", ""),
                "Mailing State": rec.get("mail_state", ""),
                "Mailing Zip": rec.get("mail_zip", ""),
                "Property Address": rec.get("prop_address", ""),
                "Property City": rec.get("prop_city", ""),
                "Property State": rec.get("prop_state", "FL"),
                "Property Zip": rec.get("prop_zip", ""),
                "Lead Type": rec.get("cat_label", ""),
                "Document Type": rec.get("doc_type", ""),
                "Date Filed": rec.get("filed", ""),
                "Document Number": rec.get("doc_num", ""),
                "Amount/Debt Owed": rec.get("amount", ""),
                "Seller Score": rec.get("score", 0),
                "Motivated Seller Flags": "; ".join(rec.get("flags", [])),
                "Source": "Duval County Clerk of Courts",
                "Public Records URL": rec.get("clerk_url", ""),
            })
    log.info("GHL CSV saved: %s (%d rows)", path, len(records))

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    end_date = datetime.now()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    log.info("=== Duval County Motivated Seller Scraper v2 ===")
    log.info("Date range: %s → %s", start_str, end_str)
    log.info("Doc types: %s", ", ".join(DOC_TYPE_CONFIG.keys()))

    # Load parcel lookup
    parcel = ParcelLookup()
    parcel_ok = parcel.load()
    if not parcel_ok:
        log.warning("Parcel enrichment disabled.")

    # Scrape clerk portal
    scraper = ClerkScraper(start_str, end_str)
    records = await scraper.fetch_all()
    log.info("Total raw records: %d", len(records))

    # Enrich + score
    if parcel_ok:
        records = enrich_with_parcel(records, parcel)

    for rec in records:
        score, flags = calculate_score(rec, records)
        rec["score"] = score
        rec["flags"] = flags

    records.sort(key=lambda r: r.get("score", 0), reverse=True)

    fetched_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    with_address = sum(1 for r in records if r.get("prop_address"))
    payload = {
        "fetched_at": fetched_at,
        "source": "Duval County Clerk of Courts",
        "date_range": f"{start_str} to {end_str}",
        "total": len(records),
        "with_address": with_address,
        "records": records,
    }

    for out_path in [DASHBOARD_JSON, DATA_JSON]:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False, default=str)
        log.info("JSON saved: %s", out_path)

    write_ghl_csv(records, GHL_CSV)
    log.info("Done. %d records, %d with address.", len(records), with_address)


if __name__ == "__main__":
    asyncio.run(main())
