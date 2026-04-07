#!/usr/bin/env python3
"""
Duval County, Florida — Motivated Seller Lead Scraper
Scrapes the Duval County Clerk portal for foreclosure, lien, judgment, and
other distressed-property documents, enriches each record with parcel data
from the Property Appraiser, and writes output JSON + GHL-ready CSV.
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

# All document-type codes to collect
DOC_TYPES = [
    "LP",
    "NOFC",
    "TAXDEED",
    "JUD",
    "CCJ",
    "DRJUD",
    "LNCORPTX",
    "LNIRS",
    "LNFED",
    "LN",
    "LNMECH",
    "LNHOA",
    "MEDLN",
    "PRO",
    "NOC",
    "RELLP",
]

DOC_TYPE_LABELS = {
    "LP":       "Lis Pendens",
    "NOFC":     "Notice of Foreclosure",
    "TAXDEED":  "Tax Deed",
    "JUD":      "Judgment",
    "CCJ":      "Certified Judgment",
    "DRJUD":    "Domestic Judgment",
    "LNCORPTX": "Corp Tax Lien",
    "LNIRS":    "IRS Lien",
    "LNFED":    "Federal Lien",
    "LN":       "Lien",
    "LNMECH":   "Mechanic Lien",
    "LNHOA":    "HOA Lien",
    "MEDLN":    "Medicaid Lien",
    "PRO":      "Probate",
    "NOC":      "Notice of Commencement",
    "RELLP":    "Release Lis Pendens",
}

# Category groupings
CAT_MAP = {
    "LP":       ("foreclosure", "Pre-Foreclosure / Lis Pendens"),
    "NOFC":     ("foreclosure", "Pre-Foreclosure / Lis Pendens"),
    "TAXDEED":  ("tax",        "Tax Lien / Tax Deed"),
    "JUD":      ("judgment",   "Judgment / Lien"),
    "CCJ":      ("judgment",   "Judgment / Lien"),
    "DRJUD":    ("judgment",   "Judgment / Lien"),
    "LNCORPTX": ("tax",        "Tax Lien / Tax Deed"),
    "LNIRS":    ("tax",        "Tax Lien / Tax Deed"),
    "LNFED":    ("tax",        "Tax Lien / Tax Deed"),
    "LN":       ("lien",       "Lien"),
    "LNMECH":   ("lien",       "Lien"),
    "LNHOA":    ("lien",       "Lien"),
    "MEDLN":    ("lien",       "Lien"),
    "PRO":      ("probate",    "Probate / Estate"),
    "NOC":      ("noc",        "Notice of Commencement"),
    "RELLP":    ("rellp",      "Release Lis Pendens"),
}

# Output paths (relative to repo root; script is in scraper/)
REPO_ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_JSON = REPO_ROOT / "dashboard" / "records.json"
DATA_JSON = REPO_ROOT / "data" / "records.json"
GHL_CSV = REPO_ROOT / "data" / "ghl_export.csv"

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def retry(fn, *args, attempts=MAX_RETRIES, delay=RETRY_DELAY, **kwargs):
    """Call fn(*args, **kwargs) up to `attempts` times, returning the result."""
    for attempt in range(1, attempts + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            log.warning("Attempt %d/%d failed: %s", attempt, attempts, exc)
            if attempt < attempts:
                time.sleep(delay)
    raise RuntimeError(f"All {attempts} attempts failed for {fn.__name__}")


def parse_amount(text: str) -> float:
    """Extract a float dollar amount from a string like '$123,456.78'."""
    if not text:
        return 0.0
    cleaned = re.sub(r"[^\d.]", "", text.replace(",", ""))
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def split_name(full_name: str):
    """
    Return (first, last) by splitting 'LAST, FIRST' or 'FIRST LAST'.
    Very light-weight — suitable for marketing exports.
    """
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

    # Known download endpoints / mirrors (try in order)
    DBF_URLS = [
        "https://www.coj.net/departments/property-appraiser/property-data.aspx",
        # Fallback: direct ZIP if the above page has a direct link
    ]

    # Common DBF file name patterns inside the ZIP
    DBF_NAMES = ["NAL.dbf", "nal.dbf", "parcel.dbf", "PARCEL.dbf", "parcels.dbf"]

    def __init__(self):
        self._by_name: dict[str, list[dict]] = {}  # name_key -> [parcel_dict, ...]

    # ------------------------------------------------------------------
    # Download helpers
    # ------------------------------------------------------------------

    def _get_dbf_zip_url(self) -> Optional[str]:
        """Scrape the COJ property-appraiser page to find the bulk data ZIP."""
        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0 (DuvalLeadScraper/1.0)"})
        try:
            resp = session.get(self.DBF_URLS[0], timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if re.search(r"\.(zip|ZIP)$", href):
                    if re.search(r"(nal|parcel|bulk|data)", href, re.I):
                        if href.startswith("http"):
                            return href
                        return "https://www.coj.net" + href
        except Exception as exc:
            log.warning("Could not scrape PA page for DBF URL: %s", exc)
        return None

    def _download_zip(self, url: str) -> Optional[bytes]:
        """Download a ZIP file from url, return raw bytes."""
        try:
            resp = requests.get(url, timeout=120, stream=True)
            resp.raise_for_status()
            return resp.content
        except Exception as exc:
            log.warning("Failed to download ZIP from %s: %s", url, exc)
            return None

    def _extract_dbf(self, zip_bytes: bytes) -> Optional[Path]:
        """Extract the first recognisable DBF from a ZIP into a temp file."""
        try:
            with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
                names = zf.namelist()
                for target in self.DBF_NAMES:
                    for name in names:
                        if Path(name).name.lower() == target.lower():
                            tmp = tempfile.NamedTemporaryFile(
                                suffix=".dbf", delete=False
                            )
                            tmp.write(zf.read(name))
                            tmp.close()
                            return Path(tmp.name)
                # Last resort: first .dbf file
                for name in names:
                    if name.lower().endswith(".dbf"):
                        tmp = tempfile.NamedTemporaryFile(
                            suffix=".dbf", delete=False
                        )
                        tmp.write(zf.read(name))
                        tmp.close()
                        return Path(tmp.name)
        except Exception as exc:
            log.warning("Failed to extract DBF from ZIP: %s", exc)
        return None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self) -> bool:
        """
        Try to download and parse the bulk parcel DBF.
        Returns True if successful, False otherwise (scraper continues without it).
        """
        try:
            from dbfread import DBF  # noqa: PLC0415
        except ImportError:
            log.error("dbfread not installed — parcel lookup unavailable.")
            return False

        zip_url = self._get_dbf_zip_url()
        if not zip_url:
            log.warning("Could not determine DBF ZIP URL — parcel lookup disabled.")
            return False

        log.info("Downloading parcel DBF from %s …", zip_url)
        zip_bytes = self._download_zip(zip_url)
        if not zip_bytes:
            return False

        dbf_path = self._extract_dbf(zip_bytes)
        if not dbf_path:
            log.warning("No usable DBF found in ZIP — parcel lookup disabled.")
            return False

        log.info("Parsing DBF: %s", dbf_path)
        try:
            table = DBF(str(dbf_path), encoding="latin-1", ignore_missing_memofile=True)
            count = 0
            for row in table:
                try:
                    self._index_row(dict(row))
                    count += 1
                except Exception:
                    pass
            log.info("Indexed %d parcel records.", count)
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
        """
        Index a parcel row under all owner-name variants.
        Column names differ between PA releases; we try several aliases.
        """
        def g(*keys):
            for k in keys:
                v = row.get(k) or row.get(k.upper()) or row.get(k.lower())
                if v and str(v).strip():
                    return str(v).strip()
            return ""

        owner_raw = g("OWN1", "OWNER", "OWNER1", "OWN_NAME")
        if not owner_raw:
            return

        parcel = {
            "owner_raw": owner_raw,
            "site_addr": g("SITEADDR", "SITE_ADDR", "SITE_ADDRESS"),
            "site_city": g("SITE_CITY", "SITECITY"),
            "site_state": g("SITE_STATE", "SITESTATE") or "FL",
            "site_zip": g("SITE_ZIP", "SITEZIP"),
            "mail_addr": g("MAILADR1", "ADDR_1", "MAIL_ADDR1", "MAILADD1"),
            "mail_city": g("MAILCITY", "CITY", "MAIL_CITY"),
            "mail_state": g("STATE", "MAIL_STATE") or "FL",
            "mail_zip": g("MAILZIP", "ZIP", "MAIL_ZIP"),
        }

        # Build 3 variants: "FIRST LAST", "LAST FIRST", "LAST, FIRST"
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
        """Return the first matching parcel dict for owner name, or None."""
        if not owner:
            return None
        key = self._norm_key(owner)
        matches = self._by_name.get(key)
        if matches:
            return matches[0]
        # Fuzzy partial: try first two tokens
        tokens = key.split()
        if len(tokens) >= 2:
            partial = tokens[0] + " " + tokens[1]
            for stored_key, parcels in self._by_name.items():
                if partial in stored_key:
                    return parcels[0]
        return None

# ---------------------------------------------------------------------------
# Seller Score Calculator
# ---------------------------------------------------------------------------

def calculate_score(record: dict, all_records: list[dict]) -> tuple[int, list[str]]:
    """
    Compute a 0–100 motivated-seller score and return (score, flags).
    """
    flags = []
    score = 30  # base

    doc_type = record.get("doc_type", "")
    cat = record.get("cat", "")
    amount = record.get("amount", 0.0)
    filed = record.get("filed", "")
    prop_address = record.get("prop_address", "")
    owner = record.get("owner", "")
    owner_upper = owner.upper()

    # Flag: Lis Pendens
    if doc_type in ("LP", "NOFC"):
        flags.append("Lis pendens")
        flags.append("Pre-foreclosure")
        score += 10

    # Flag: Tax lien
    if doc_type in ("TAXDEED", "LNIRS", "LNFED", "LNCORPTX"):
        flags.append("Tax lien")
        score += 10

    # Flag: Judgment lien
    if doc_type in ("JUD", "CCJ", "DRJUD"):
        flags.append("Judgment lien")
        score += 10

    # Flag: Mechanic lien
    if doc_type in ("LNMECH", "LN", "LNHOA", "MEDLN"):
        flags.append("Mechanic lien")
        score += 10

    # Flag: Probate / estate
    if doc_type == "PRO":
        flags.append("Probate / estate")
        score += 10

    # LP + FC combo: owner has BOTH a Lis Pendens AND a Foreclosure
    owner_docs = [r.get("doc_type") for r in all_records if r.get("owner", "").upper() == owner_upper]
    has_lp = any(d in ("LP",) for d in owner_docs)
    has_fc = any(d in ("NOFC",) for d in owner_docs)
    if has_lp and has_fc:
        score += 20

    # Amount bonus
    if amount > 100_000:
        flags.append("High debt (>$100k)")
        score += 15
    elif amount > 50_000:
        flags.append("Significant debt (>$50k)")
        score += 10

    # New this week (+5)
    try:
        filed_date = datetime.strptime(filed, "%Y-%m-%d")
        if (datetime.now() - filed_date).days <= 7:
            flags.append("New this week")
            score += 5
    except Exception:
        pass

    # Has address (+5)
    if prop_address:
        flags.append("Has address")
        score += 5

    # LLC / Corp owner
    corp_keywords = ["LLC", "INC", "CORP", "LTD", "TRUST", "HOLDINGS", "PROPERTIES"]
    if any(kw in owner_upper for kw in corp_keywords):
        flags.append("LLC / corp owner")
        score += 10

    # Deduplicate flags
    seen = set()
    unique_flags = []
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
    Async Playwright scraper for https://or.duvalclerk.com/
    Navigates Official Records search, filters by doc type and date range,
    and collects all result rows.
    """

    SEARCH_URL = f"{CLERK_BASE}/search/index"
    TIMEOUT = 30_000  # ms

    def __init__(self, start_date: str, end_date: str):
        self.start_date = start_date  # YYYY-MM-DD
        self.end_date = end_date

    async def fetch_all(self) -> list[dict]:
        """Return all records for all configured doc types within date range."""
        all_records: list[dict] = []
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
                           " (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
            page = await context.new_page()

            for doc_type in DOC_TYPES:
                log.info("Fetching doc type: %s", doc_type)
                try:
                    records = await self._fetch_doc_type(page, doc_type)
                    log.info("  → %d records", len(records))
                    all_records.extend(records)
                except Exception:
                    log.error("Error fetching %s:\n%s", doc_type, traceback.format_exc())

            await browser.close()

        return all_records

    async def _fetch_doc_type(self, page, doc_type: str) -> list[dict]:
        """Search for a single doc type and collect all paginated results."""
        records: list[dict] = []

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                await page.goto(self.SEARCH_URL, timeout=self.TIMEOUT)
                await page.wait_for_load_state("networkidle", timeout=self.TIMEOUT)
                break
            except PWTimeout:
                log.warning("Timeout loading search page (attempt %d/%d)", attempt, MAX_RETRIES)
                if attempt == MAX_RETRIES:
                    return records
                await asyncio.sleep(RETRY_DELAY)

        # Fill the search form
        try:
            # Select document type
            await self._select_doc_type(page, doc_type)

            # Date range
            await self._fill_date_range(page)

            # Submit
            await self._submit_search(page)

        except Exception:
            log.error("Form interaction error for %s:\n%s", doc_type, traceback.format_exc())
            return records

        # Collect all pages
        page_num = 0
        while True:
            page_num += 1
            try:
                page_records = await self._parse_results_page(page, doc_type)
                records.extend(page_records)

                # Try to go to next page
                has_next = await self._go_next_page(page)
                if not has_next:
                    break

                await page.wait_for_load_state("networkidle", timeout=self.TIMEOUT)
            except Exception:
                log.error("Error parsing page %d for %s:\n%s",
                          page_num, doc_type, traceback.format_exc())
                break

        return records

    async def _select_doc_type(self, page, doc_type: str):
        """
        Handle doc-type selection.  The Duval clerk portal uses a combo-box /
        drop-down — try several selector strategies.
        """
        selectors = [
            f"select[name*='DocType'] option[value='{doc_type}']",
            f"select[id*='DocType'] option[value='{doc_type}']",
            f"select option[value='{doc_type}']",
        ]
        for sel in selectors:
            try:
                opt = await page.query_selector(sel)
                if opt:
                    parent = await opt.evaluate_handle("el => el.parentElement")
                    await parent.select_option(value=doc_type)
                    return
            except Exception:
                pass

        # Try free-text search field
        text_selectors = [
            "input[placeholder*='Document Type']",
            "input[name*='DocType']",
            "input[id*='DocType']",
        ]
        for sel in text_selectors:
            try:
                inp = await page.query_selector(sel)
                if inp:
                    await inp.fill(doc_type)
                    return
            except Exception:
                pass

        log.warning("Could not select doc type %s — continuing anyway", doc_type)

    async def _fill_date_range(self, page):
        """Fill start and end date fields."""
        start_fmt = datetime.strptime(self.start_date, "%Y-%m-%d").strftime("%m/%d/%Y")
        end_fmt = datetime.strptime(self.end_date, "%Y-%m-%d").strftime("%m/%d/%Y")

        date_field_pairs = [
            (["input[name*='StartDate']", "input[id*='StartDate']", "input[placeholder*='Start']"],
             ["input[name*='EndDate']", "input[id*='EndDate']", "input[placeholder*='End']"]),
            (["input[name*='FromDate']", "input[id*='FromDate']"],
             ["input[name*='ToDate']", "input[id*='ToDate']"]),
            (["input[name*='DateFrom']", "input[id*='DateFrom']"],
             ["input[name*='DateTo']", "input[id*='DateTo']"]),
        ]

        for start_sels, end_sels in date_field_pairs:
            for s_sel in start_sels:
                try:
                    inp = await page.query_selector(s_sel)
                    if inp:
                        await inp.fill(start_fmt)
                        break
                except Exception:
                    pass
            for e_sel in end_sels:
                try:
                    inp = await page.query_selector(e_sel)
                    if inp:
                        await inp.fill(end_fmt)
                        return
                except Exception:
                    pass

    async def _submit_search(self, page):
        """Click the search / submit button."""
        btn_selectors = [
            "button[type='submit']",
            "input[type='submit']",
            "button:has-text('Search')",
            "a:has-text('Search')",
            "#btnSearch",
            "input[value='Search']",
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
        # Try pressing Enter in the form
        await page.keyboard.press("Enter")
        await page.wait_for_load_state("networkidle", timeout=self.TIMEOUT)

    async def _parse_results_page(self, page, doc_type: str) -> list[dict]:
        """Parse the current results page and return a list of record dicts."""
        records = []
        try:
            content = await page.content()
            soup = BeautifulSoup(content, "lxml")
            current_url = page.url

            # Try to find a results table
            table = soup.find("table", class_=re.compile(r"result|search|grid|data", re.I))
            if not table:
                table = soup.find("table")
            if not table:
                return records

            rows = table.find_all("tr")
            if not rows:
                return records

            # Determine headers from first row
            headers = []
            header_row = rows[0]
            for th in header_row.find_all(["th", "td"]):
                headers.append(th.get_text(strip=True).lower())

            def col(row_cells, *names):
                for name in names:
                    for i, h in enumerate(headers):
                        if name in h and i < len(row_cells):
                            return row_cells[i].get_text(strip=True)
                return ""

            for row in rows[1:]:
                cells = row.find_all("td")
                if not cells:
                    continue
                try:
                    # Doc number — look for an <a> link
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
                        doc_num = col(cells, "doc", "instrument", "book", "number")

                    # Filed date
                    filed_raw = col(cells, "date", "filed", "record")
                    filed = ""
                    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
                        try:
                            filed = datetime.strptime(filed_raw, fmt).strftime("%Y-%m-%d")
                            break
                        except ValueError:
                            pass

                    # Grantor / owner
                    owner = col(cells, "grantor", "owner", "party1", "seller")

                    # Grantee
                    grantee = col(cells, "grantee", "buyer", "party2")

                    # Amount
                    amount_raw = col(cells, "amount", "consideration", "value", "debt")
                    amount = parse_amount(amount_raw)

                    # Legal description
                    legal = col(cells, "legal", "description", "property")

                    cat, cat_label = CAT_MAP.get(doc_type, ("other", doc_type))

                    if not doc_num and not owner:
                        continue

                    records.append({
                        "doc_num": doc_num,
                        "doc_type": doc_type,
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
                    log.debug("Row parse error:\n%s", traceback.format_exc())
                    continue
        except Exception:
            log.error("Page parse error:\n%s", traceback.format_exc())
        return records

    async def _go_next_page(self, page) -> bool:
        """
        Attempt to navigate to the next result page.
        Returns True if successful (page changed), False if we're on the last page.
        """
        next_selectors = [
            "a:has-text('Next')",
            "a:has-text('>')",
            "a[rel='next']",
            ".pager .next a",
            "input[value='Next']",
            "button:has-text('Next')",
        ]
        for sel in next_selectors:
            try:
                btn = await page.query_selector(sel)
                if btn:
                    is_disabled = await btn.get_attribute("disabled")
                    cls = await btn.get_attribute("class") or ""
                    if is_disabled or "disabled" in cls.lower():
                        return False
                    prev_url = page.url
                    await btn.click()
                    await page.wait_for_load_state("networkidle", timeout=self.TIMEOUT)
                    return page.url != prev_url
            except Exception:
                pass

        # Try __doPostBack approach for ASP.NET grids
        try:
            content = await page.content()
            soup = BeautifulSoup(content, "lxml")
            links = soup.find_all("a", href=re.compile(r"__doPostBack"))
            for a in links:
                if re.search(r"next|page|>", a.get_text(), re.I):
                    href = a["href"]
                    event_target = re.search(r"__doPostBack\('([^']+)'", href)
                    event_arg = re.search(r"__doPostBack\('[^']+','([^']+)'", href)
                    if event_target:
                        await page.evaluate(
                            f"__doPostBack('{event_target.group(1)}', "
                            f"'{event_arg.group(1) if event_arg else ''}')"
                        )
                        await page.wait_for_load_state("networkidle", timeout=self.TIMEOUT)
                        return True
        except Exception:
            pass

        return False

# ---------------------------------------------------------------------------
# Enrich records with parcel data
# ---------------------------------------------------------------------------

def enrich_with_parcel(records: list[dict], parcel: ParcelLookup) -> list[dict]:
    for rec in records:
        try:
            match = parcel.lookup(rec.get("owner", ""))
            if match:
                rec["prop_address"] = match.get("site_addr", "")
                rec["prop_city"] = match.get("site_city", "")
                rec["prop_state"] = match.get("site_state", "FL")
                rec["prop_zip"] = match.get("site_zip", "")
                rec["mail_address"] = match.get("mail_addr", "")
                rec["mail_city"] = match.get("mail_city", "")
                rec["mail_state"] = match.get("mail_state", "FL")
                rec["mail_zip"] = match.get("mail_zip", "")
        except Exception:
            log.debug("Parcel enrich error: %s", traceback.format_exc())
    return records

# ---------------------------------------------------------------------------
# GHL CSV Export
# ---------------------------------------------------------------------------

GHL_COLUMNS = [
    "First Name",
    "Last Name",
    "Mailing Address",
    "Mailing City",
    "Mailing State",
    "Mailing Zip",
    "Property Address",
    "Property City",
    "Property State",
    "Property Zip",
    "Lead Type",
    "Document Type",
    "Date Filed",
    "Document Number",
    "Amount/Debt Owed",
    "Seller Score",
    "Motivated Seller Flags",
    "Source",
    "Public Records URL",
]


def write_ghl_csv(records: list[dict], path: Path):
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
                "Document Type": DOC_TYPE_LABELS.get(rec.get("doc_type", ""), rec.get("doc_type", "")),
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
# Main entry point
# ---------------------------------------------------------------------------

async def main():
    end_date = datetime.now()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    log.info("=== Duval County Motivated Seller Scraper ===")
    log.info("Date range: %s → %s", start_str, end_str)
    log.info("Doc types: %s", ", ".join(DOC_TYPES))

    # 1. Load parcel lookup
    parcel = ParcelLookup()
    parcel_ok = parcel.load()
    if not parcel_ok:
        log.warning("Parcel lookup unavailable — address fields will be empty.")

    # 2. Scrape clerk portal
    scraper = ClerkScraper(start_str, end_str)
    records = await scraper.fetch_all()
    log.info("Total raw records collected: %d", len(records))

    # 3. Enrich with parcel data
    if parcel_ok:
        records = enrich_with_parcel(records, parcel)

    # 4. Score all records
    for rec in records:
        score, flags = calculate_score(rec, records)
        rec["score"] = score
        rec["flags"] = flags

    # Sort by score descending
    records.sort(key=lambda r: r.get("score", 0), reverse=True)

    # 5. Build output payload
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

    # 6. Write JSON output files
    for out_path in [DASHBOARD_JSON, DATA_JSON]:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False, default=str)
        log.info("JSON saved: %s", out_path)

    # 7. Write GHL CSV
    write_ghl_csv(records, GHL_CSV)

    log.info("Done. %d records, %d with property address.", len(records), with_address)


if __name__ == "__main__":
    asyncio.run(main())
