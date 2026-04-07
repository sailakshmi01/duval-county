# Duval County Motivated Seller Lead Scraper

Automated daily scraper for Duval County, Florida public records. Collects distressed property leads (lis pendens, foreclosures, judgments, liens, probate, etc.), scores them, and exports a dashboard + GHL-ready CSV.

## What it does

- Scrapes the [Duval County Clerk portal](https://or.duvalclerk.com/) for 16 document types
- Enriches records with property/mailing addresses from the Property Appraiser bulk data
- Scores each lead 0–100 based on distress signals
- Publishes a live dashboard to GitHub Pages
- Exports a GoHighLevel-ready CSV

## Document Types Collected

| Code | Type |
|------|------|
| LP | Lis Pendens |
| NOFC | Notice of Foreclosure |
| TAXDEED | Tax Deed |
| JUD / CCJ / DRJUD | Judgments |
| LNCORPTX / LNIRS / LNFED | Tax / Federal Liens |
| LN / LNMECH / LNHOA | Liens |
| MEDLN | Medicaid Lien |
| PRO | Probate |
| NOC | Notice of Commencement |
| RELLP | Release Lis Pendens |

## Seller Score (0–100)

- Base: 30
- +10 per distress flag
- +20 LP + Foreclosure combo
- +15 amount > $100k / +10 amount > $50k
- +5 new this week
- +5 has property address

## Setup

### 1. Enable GitHub Pages

Go to **Settings → Pages** and set source to **GitHub Actions**.

### 2. Run manually

Go to **Actions → Duval County Lead Scraper → Run workflow**.

### 3. Scheduled runs

Runs automatically every day at 7:00 AM UTC (3:00 AM ET).

## Output Files

| File | Description |
|------|-------------|
| `dashboard/records.json` | Latest records (served by GitHub Pages) |
| `data/records.json` | Same data, secondary copy |
| `data/ghl_export.csv` | GoHighLevel import-ready CSV |

## Dashboard

After the first run, your live dashboard will be at:
`https://sailakshmi01.github.io/duval-county/`
