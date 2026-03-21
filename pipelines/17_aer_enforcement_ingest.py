# Databricks notebook source
"""
Pipeline 17: AER Enforcement Register Ingestion

Scrapes the Australian Energy Regulator's public enforcement register.
Extracts real enforcement actions: infringement notices, court proceedings,
enforceable undertakings, and court decisions with company names, penalties,
and breach descriptions.

Sources:
- https://www.aer.gov.au/publications/reports/compliance (HTML listing)
- Individual enforcement action pages (HTML prose)

Target: energy_copilot_catalog.gold.aer_enforcement_actions
Schedule: Weekly (Monday 9am AEST) — enforcement actions published infrequently
"""

import hashlib
import re
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

# COMMAND ----------

try:
    catalog = dbutils.widgets.get("catalog")
except Exception:
    catalog = "energy_copilot_catalog"

SCHEMA = "gold"
TABLE = f"{catalog}.{SCHEMA}.aer_enforcement_actions"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    action_id STRING,
    action_type STRING,
    company_name STRING,
    title STRING,
    description STRING,
    applicable_law STRING,
    penalty_aud DOUBLE,
    action_date DATE,
    url STRING,
    sector STRING,
    status STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

BASE_URL = "https://www.aer.gov.au"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (energy-copilot data pipeline; research use)",
    "Accept": "text/html",
}

# Known enforcement action categories on AER site
ACTION_TYPES = {
    "infringement": "INFRINGEMENT_NOTICE",
    "civil": "CIVIL_PROCEEDINGS",
    "court": "COURT_DECISION",
    "undertaking": "ENFORCEABLE_UNDERTAKING",
}

# COMMAND ----------

def extract_penalty(text: str) -> float:
    """Extract dollar penalty from prose text."""
    # Match patterns like "$542,400", "$1.2 million", "$2,500,000"
    m = re.search(r'\$(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(?:million|m)', text, re.IGNORECASE)
    if m:
        return float(m.group(1).replace(',', '')) * 1_000_000
    m = re.search(r'\$(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)', text)
    if m:
        val = float(m.group(1).replace(',', ''))
        if val > 100:  # Likely a real penalty
            return val
    return 0.0


def extract_company(title: str) -> str:
    """Extract company name from enforcement action title."""
    # Common patterns: "Company fined...", "Company penalised...", "Proceedings against Company"
    patterns = [
        r'^(.+?)\s+(?:fined|penalised|penalty|ordered|infringement)',
        r'(?:against|proceedings\s+against)\s+(.+?)(?:\s+for|\s+–|\s*$)',
        r'^(.+?)\s+–\s+',
        r'^(.+?)\s+(?:pays|receives|enters)',
    ]
    for p in patterns:
        m = re.search(p, title, re.IGNORECASE)
        if m:
            name = m.group(1).strip()
            # Clean up common suffixes
            name = re.sub(r'\s*(Pty Ltd|Limited|Ltd|Inc)\.?$', '', name, flags=re.IGNORECASE)
            if len(name) > 3 and name[0].isupper():
                return name
    # Fallback: first 2-3 words that look like a company name
    words = title.split()
    if len(words) >= 2:
        return ' '.join(words[:3])
    return title


def extract_date_from_text(text: str) -> str:
    """Extract date from text like '15 March 2024' or 'March 2024'."""
    m = re.search(r'(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})', text)
    if m:
        try:
            return datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", "%d %B %Y").strftime("%Y-%m-%d")
        except Exception:
            pass
    m = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})', text)
    if m:
        try:
            return datetime.strptime(f"1 {m.group(1)} {m.group(2)}", "%d %B %Y").strftime("%Y-%m-%d")
        except Exception:
            pass
    m = re.search(r'(\d{4})', text)
    if m:
        return f"{m.group(1)}-01-01"
    return datetime.now().strftime("%Y-%m-%d")


def extract_law(text: str) -> str:
    """Extract applicable law from text."""
    laws = {
        "NERL": r'(?:National Energy Retail Law|NERL)',
        "NERR": r'(?:National Energy Retail Rules|NERR)',
        "NEL": r'(?:National Electricity Law|NEL\b)',
        "NER": r'(?:National Electricity Rules|NER\b)',
        "NGL": r'(?:National Gas Law|NGL\b)',
        "NGR": r'(?:National Gas Rules|NGR\b)',
    }
    found = []
    for code, pattern in laws.items():
        if re.search(pattern, text, re.IGNORECASE):
            found.append(code)
    return ", ".join(found) if found else "NER"


def classify_sector(text: str) -> str:
    """Classify enforcement into sector."""
    if re.search(r'retail|customer|life.?support|disconnection|hardship|billing', text, re.IGNORECASE):
        return "RETAIL"
    if re.search(r'wholesale|generation|bidding|rebid|market', text, re.IGNORECASE):
        return "WHOLESALE"
    if re.search(r'network|distribution|transmission|connection', text, re.IGNORECASE):
        return "NETWORK"
    if re.search(r'gas|pipeline', text, re.IGNORECASE):
        return "GAS"
    return "GENERAL"

# COMMAND ----------

def scrape_aer_enforcement():
    """Scrape AER enforcement actions from publications/reports/compliance."""
    actions = []

    # Scrape the compliance reports listing page
    url = f"{BASE_URL}/publications/reports/compliance"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=60)
        resp.raise_for_status()
    except Exception as e:
        print(f"Failed to fetch AER listing: {e}")
        return actions

    soup = BeautifulSoup(resp.text, "html.parser")

    # Find all links that look like enforcement actions
    links = soup.find_all("a", href=True)
    enforcement_links = []
    for link in links:
        href = link.get("href", "")
        text = link.get_text(strip=True)
        # Filter for enforcement-related links
        if any(kw in text.lower() for kw in ["fined", "penalised", "penalty", "proceedings", "undertaking", "infringement", "court"]):
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            enforcement_links.append((full_url, text))
        elif any(kw in href.lower() for kw in ["infringement", "proceedings", "undertaking", "penalty"]):
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            enforcement_links.append((full_url, text))

    print(f"Found {len(enforcement_links)} potential enforcement links")

    # Deduplicate
    seen = set()
    unique_links = []
    for url, title in enforcement_links:
        if url not in seen:
            seen.add(url)
            unique_links.append((url, title))

    # Fetch each enforcement action page (limit to 100 most recent)
    for page_url, title in unique_links[:100]:
        try:
            resp2 = requests.get(page_url, headers=HEADERS, timeout=30)
            if resp2.status_code != 200:
                continue
            page_soup = BeautifulSoup(resp2.text, "html.parser")

            # Extract body text
            body = page_soup.find("article") or page_soup.find("main") or page_soup.find("body")
            body_text = body.get_text(" ", strip=True) if body else title

            # Determine action type
            action_type = "OTHER"
            title_lower = title.lower()
            for keyword, atype in ACTION_TYPES.items():
                if keyword in title_lower or keyword in body_text.lower()[:500]:
                    action_type = atype
                    break

            action_id = hashlib.md5(page_url.encode()).hexdigest()[:16]

            actions.append({
                "action_id": action_id,
                "action_type": action_type,
                "company_name": extract_company(title),
                "title": title[:500],
                "description": body_text[:2000],
                "applicable_law": extract_law(body_text),
                "penalty_aud": extract_penalty(body_text),
                "action_date": extract_date_from_text(body_text),
                "url": page_url,
                "sector": classify_sector(body_text),
                "status": "FINAL",
                "ingested_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            })
        except Exception as e:
            print(f"Error scraping {page_url}: {e}")
            continue

    return actions

# COMMAND ----------

actions = scrape_aer_enforcement()
print(f"Scraped {len(actions)} enforcement actions")

if actions:
    df = spark.createDataFrame(actions)
    df.createOrReplaceTempView("src_enforcement")

    merge_sql = f"""
    MERGE INTO {TABLE} t
    USING (
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY action_id ORDER BY ingested_at DESC) as _rn
            FROM src_enforcement
        ) WHERE _rn = 1
    ) AS s
    ON t.action_id = s.action_id
    WHEN MATCHED THEN UPDATE SET
        t.action_type = s.action_type,
        t.company_name = s.company_name,
        t.title = s.title,
        t.description = s.description,
        t.applicable_law = s.applicable_law,
        t.penalty_aud = s.penalty_aud,
        t.action_date = s.action_date,
        t.url = s.url,
        t.sector = s.sector,
        t.status = s.status,
        t.ingested_at = s.ingested_at
    WHEN NOT MATCHED THEN INSERT *
    """
    spark.sql(merge_sql)

    count = spark.sql(f"SELECT count(*) as c FROM {TABLE}").collect()[0]["c"]
    print(f"AER enforcement actions table now has {count} rows")
else:
    print("No enforcement actions scraped — site may be unavailable")
