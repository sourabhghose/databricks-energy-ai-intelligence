# Databricks notebook source
"""
Pipeline 18: AER Compliance Calendar & Regulatory Obligations

Seeds and maintains compliance obligations from AER regulatory requirements.
Combines:
1. Known AER reporting deadlines (structured seed data)
2. Scraped AER rule changes and consultations
3. AEMO compliance requirements (market participant obligations)

Target: energy_copilot_catalog.gold.compliance_obligations
Schedule: Weekly (Monday 8am AEST)
"""

import hashlib
from datetime import date, datetime, timezone

import requests
from bs4 import BeautifulSoup

# COMMAND ----------

try:
    catalog = dbutils.widgets.get("catalog")
except Exception:
    catalog = "energy_copilot_catalog"

SCHEMA = "gold"
TABLE = f"{catalog}.{SCHEMA}.compliance_obligations"

# Table already exists from DDL — just verify
count = spark.sql(f"SELECT count(*) as c FROM {TABLE}").collect()[0]["c"]
print(f"compliance_obligations currently has {count} rows")

# COMMAND ----------

HEADERS = {
    "User-Agent": "Mozilla/5.0 (energy-copilot data pipeline; research use)",
    "Accept": "text/html",
}

# =========================================================================
# Part 1: Structured AER/AEMO compliance obligations (known deadlines)
# =========================================================================

KNOWN_OBLIGATIONS = [
    # AER Wholesale Market Monitoring (effective Jan 2025)
    {"obligation_type": "AER_REPORTING", "title": "Quarterly Exchange-Traded Contract Report",
     "description": "Report exchange-traded electricity contracts >1MW to AER under Wholesale Market Monitoring Information Order.",
     "due_date": "2026-03-31", "region": "NEM", "responsible_party": "Market Participants", "priority": "HIGH",
     "source": "AER Wholesale Market Monitoring Guidelines 2024"},
    {"obligation_type": "AER_REPORTING", "title": "Quarterly OTC Contract Report",
     "description": "Report OTC standard and non-standard electricity/gas contracts to AER.",
     "due_date": "2026-03-31", "region": "NEM", "responsible_party": "Market Participants", "priority": "HIGH",
     "source": "AER Wholesale Market Monitoring Guidelines 2024"},
    {"obligation_type": "AER_REPORTING", "title": "Annual PPA Disclosure",
     "description": "Annual disclosure of all Power Purchase Agreements to AER. Due 31 March each year.",
     "due_date": "2026-03-31", "region": "NEM", "responsible_party": "Generators & Retailers", "priority": "HIGH",
     "source": "AER Wholesale Market Monitoring Guidelines 2024"},
    {"obligation_type": "AER_REPORTING", "title": "Historical Contracts Submission (2019-2024)",
     "description": "One-off submission of historical wholesale electricity contracts from 2019-2024 to AER.",
     "due_date": "2025-08-31", "region": "NEM", "responsible_party": "Market Participants", "priority": "CRITICAL",
     "source": "AER Wholesale Market Monitoring Guidelines 2024"},

    # AEMO market participant obligations
    {"obligation_type": "AEMO_REGISTRATION", "title": "Annual Generator Compliance Audit",
     "description": "NER clause 4.15 — annual compliance audit of generator performance standards.",
     "due_date": "2026-06-30", "region": "NEM", "responsible_party": "Registered Generators", "priority": "HIGH",
     "source": "NER Chapter 4"},
    {"obligation_type": "AEMO_REGISTRATION", "title": "Metering Data Compliance Review",
     "description": "NER Chapter 7 — annual review of metering installation compliance.",
     "due_date": "2026-06-30", "region": "NEM", "responsible_party": "Metering Coordinators", "priority": "MEDIUM",
     "source": "NER Chapter 7"},
    {"obligation_type": "AEMO_REGISTRATION", "title": "Demand Side Participation Report",
     "description": "Annual demand side participation information submitted to AEMO under NER 3.7D.",
     "due_date": "2026-04-30", "region": "NEM", "responsible_party": "Large Customers & Aggregators", "priority": "MEDIUM",
     "source": "NER 3.7D"},
    {"obligation_type": "NER_COMPLIANCE", "title": "Rebidding Compliance Declaration",
     "description": "Quarterly declaration of rebidding conduct compliance per NER 3.8.22A.",
     "due_date": "2026-04-15", "region": "NEM", "responsible_party": "Generators", "priority": "HIGH",
     "source": "NER 3.8.22A"},
    {"obligation_type": "NER_COMPLIANCE", "title": "FCAS Enablement Compliance",
     "description": "Quarterly FCAS response verification — generators must demonstrate frequency response capability.",
     "due_date": "2026-03-31", "region": "NEM", "responsible_party": "FCAS Providers", "priority": "HIGH",
     "source": "NER Chapter 4"},

    # AER network compliance
    {"obligation_type": "AER_NETWORK", "title": "Annual Pricing Proposal Submission",
     "description": "DNSPs submit annual pricing proposals for AER approval under revenue determination.",
     "due_date": "2026-05-31", "region": "NEM", "responsible_party": "Distribution NSPs", "priority": "CRITICAL",
     "source": "NER Chapter 6"},
    {"obligation_type": "AER_NETWORK", "title": "Ring-Fencing Annual Compliance Report",
     "description": "Annual report demonstrating compliance with ring-fencing obligations for regulated network services.",
     "due_date": "2026-10-31", "region": "NEM", "responsible_party": "NSPs", "priority": "MEDIUM",
     "source": "AER Ring-fencing Guideline"},
    {"obligation_type": "AER_NETWORK", "title": "Regulatory Information Notice (RIN) Submission",
     "description": "Annual RIN data submission for economic benchmarking of network businesses.",
     "due_date": "2026-11-30", "region": "NEM", "responsible_party": "NSPs", "priority": "HIGH",
     "source": "AER Economic Benchmarking RIN"},

    # Retail obligations
    {"obligation_type": "AER_RETAIL", "title": "Hardship Program Annual Report",
     "description": "Retailers report hardship program statistics per NERL section 44.",
     "due_date": "2026-09-30", "region": "NEM", "responsible_party": "Authorised Retailers", "priority": "HIGH",
     "source": "NERL s44"},
    {"obligation_type": "AER_RETAIL", "title": "Default Market Offer Price Update",
     "description": "Annual DMO price determination — retailers must comply with price cap from 1 July.",
     "due_date": "2026-05-25", "region": "NEM", "responsible_party": "Authorised Retailers", "priority": "CRITICAL",
     "source": "Competition and Consumer (Industry Code—Electricity Retail) Regulations 2019"},
    {"obligation_type": "AER_RETAIL", "title": "Life Support Registration Audit",
     "description": "Quarterly audit of life support customer registrations per NERR 124.",
     "due_date": "2026-03-31", "region": "NEM", "responsible_party": "Retailers & Distributors", "priority": "CRITICAL",
     "source": "NERR Rule 124"},

    # ESB / AEMC reforms
    {"obligation_type": "AEMC_REFORM", "title": "Capacity Investment Scheme Compliance",
     "description": "CIS underwriting scheme — contracted generators must meet availability requirements.",
     "due_date": "2026-06-30", "region": "NEM", "responsible_party": "CIS Contract Holders", "priority": "HIGH",
     "source": "CIS Framework"},
    {"obligation_type": "AEMC_REFORM", "title": "Integrated System Plan Consultation Response",
     "description": "Stakeholder submissions for AEMO's 2026 ISP update.",
     "due_date": "2026-05-15", "region": "NEM", "responsible_party": "All Stakeholders", "priority": "MEDIUM",
     "source": "AEMO ISP"},

    # WEM obligations
    {"obligation_type": "WEM_COMPLIANCE", "title": "WEM Reserve Capacity Certification",
     "description": "Annual capacity certification for WEM Reserve Capacity Mechanism.",
     "due_date": "2026-07-01", "region": "WEM", "responsible_party": "WEM Generators", "priority": "HIGH",
     "source": "WEM Rules Chapter 4"},
    {"obligation_type": "WEM_COMPLIANCE", "title": "WEM Facility Standing Data Update",
     "description": "Annual update of standing data for registered WEM facilities.",
     "due_date": "2026-04-30", "region": "WEM", "responsible_party": "WEM Participants", "priority": "MEDIUM",
     "source": "WEM Rules Chapter 2"},

    # Victorian-specific (ESV)
    {"obligation_type": "ESV_COMPLIANCE", "title": "Electricity Safety Management Scheme Annual Report",
     "description": "Annual ESMS performance report to Energy Safe Victoria per Electricity Safety Act 1998.",
     "due_date": "2026-09-30", "region": "VIC1", "responsible_party": "Victorian Network Operators", "priority": "HIGH",
     "source": "Electricity Safety Act 1998"},
    {"obligation_type": "ESV_COMPLIANCE", "title": "Bushfire Mitigation Plan Submission",
     "description": "Annual bushfire mitigation plan for Victorian electricity infrastructure.",
     "due_date": "2026-05-01", "region": "VIC1", "responsible_party": "Victorian DNSPs", "priority": "CRITICAL",
     "source": "Electricity Safety (Bushfire Mitigation) Regulations 2013"},
]

# COMMAND ----------

def seed_known_obligations():
    """Seed compliance_obligations with known AER/AEMO/ESV deadlines."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    today = date.today()

    rows = []
    for ob in KNOWN_OBLIGATIONS:
        due = datetime.strptime(ob["due_date"], "%Y-%m-%d").date()
        if due < today:
            status = "COMPLETED" if (today - due).days > 30 else "OVERDUE"
        elif (due - today).days <= 30:
            status = "DUE_SOON"
        else:
            status = "PENDING"

        oid = hashlib.md5(f"{ob['title']}_{ob['due_date']}".encode()).hexdigest()[:12]
        rows.append({
            "obligation_id": oid,
            "obligation_type": ob["obligation_type"],
            "title": ob["title"],
            "description": ob.get("description", ""),
            "due_date": ob["due_date"],
            "status": status,
            "region": ob["region"],
            "responsible_party": ob["responsible_party"],
            "priority": ob["priority"],
            "created_at": now,
            "updated_at": now,
        })
    return rows

# COMMAND ----------

# Part 2: Scrape AER consultations and rule changes
def scrape_aer_consultations():
    """Scrape AER current consultations for upcoming regulatory changes."""
    actions = []
    try:
        resp = requests.get("https://www.aer.gov.au/consultations-and-reviews", headers=HEADERS, timeout=60)
        if resp.status_code != 200:
            print(f"AER consultations page returned {resp.status_code}")
            return actions

        soup = BeautifulSoup(resp.text, "html.parser")
        links = soup.find_all("a", href=True)

        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        for link in links:
            text = link.get_text(strip=True)
            href = link.get("href", "")
            if len(text) > 20 and any(kw in text.lower() for kw in [
                "determination", "review", "consultation", "guideline", "rule change",
                "pricing", "revenue", "framework", "draft decision", "final decision"
            ]):
                oid = hashlib.md5(f"consult_{href}".encode()).hexdigest()[:12]
                actions.append({
                    "obligation_id": oid,
                    "obligation_type": "AER_CONSULTATION",
                    "title": text[:300],
                    "description": f"AER consultation/review: {text}. Source: {href}",
                    "due_date": "2026-06-30",  # Default future date
                    "status": "MONITORING",
                    "region": "NEM",
                    "responsible_party": "Compliance Team",
                    "priority": "LOW",
                    "created_at": now,
                    "updated_at": now,
                })
    except Exception as e:
        print(f"Error scraping AER consultations: {e}")

    return actions

# COMMAND ----------

# Seed known obligations
known = seed_known_obligations()
print(f"Prepared {len(known)} known regulatory obligations")

# Scrape AER consultations
consultations = scrape_aer_consultations()
print(f"Scraped {len(consultations)} AER consultations")

all_obligations = known + consultations

if all_obligations:
    df = spark.createDataFrame(all_obligations)
    df.createOrReplaceTempView("src_obligations")

    merge_sql = f"""
    MERGE INTO {TABLE} t
    USING (
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY obligation_id ORDER BY updated_at DESC) as _rn
            FROM src_obligations
        ) WHERE _rn = 1
    ) AS s
    ON t.obligation_id = s.obligation_id
    WHEN MATCHED THEN UPDATE SET
        t.obligation_type = s.obligation_type,
        t.title = s.title,
        t.description = s.description,
        t.due_date = s.due_date,
        t.status = s.status,
        t.region = s.region,
        t.responsible_party = s.responsible_party,
        t.priority = s.priority,
        t.updated_at = s.updated_at
    WHEN NOT MATCHED THEN INSERT (
        obligation_id, obligation_type, title, description, due_date,
        status, region, responsible_party, priority, created_at, updated_at
    ) VALUES (
        s.obligation_id, s.obligation_type, s.title, s.description, s.due_date,
        s.status, s.region, s.responsible_party, s.priority, s.created_at, s.updated_at
    )
    """
    spark.sql(merge_sql)

    count = spark.sql(f"SELECT count(*) as c FROM {TABLE}").collect()[0]["c"]
    print(f"compliance_obligations now has {count} rows")
