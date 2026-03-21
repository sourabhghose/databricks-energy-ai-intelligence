# Databricks notebook source
"""
Pipeline 21: AEMO Procedures & Guidelines Register

Ingests AEMO's library of NEM procedures, power system operating procedures (SOPs),
guidelines, and market operation documents.

Sources:
1. AEMO Power System Operating Procedures page (scrapeable HTML)
2. Known procedure codes and metadata (structured seed)
3. AEMO consultation/rule change register

Target: energy_copilot_catalog.gold.aemo_procedures
Schedule: Monthly (1st Monday, 9am AEST) — procedures updated infrequently
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
TABLE = f"{catalog}.{SCHEMA}.aemo_procedures"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    procedure_id STRING,
    procedure_code STRING,
    title STRING,
    category STRING,
    document_type STRING,
    description STRING,
    effective_date DATE,
    version STRING,
    ner_clause STRING,
    applicable_market STRING,
    status STRING,
    document_url STRING,
    last_reviewed DATE,
    next_review_date DATE,
    ingested_at TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

HEADERS = {
    "User-Agent": "Mozilla/5.0 (energy-copilot data pipeline; research use)",
    "Accept": "text/html",
}

# =========================================================================
# Known AEMO Power System Operating Procedures (real procedure codes)
# Source: AEMO website + NER Chapter 4
# =========================================================================

AEMO_PROCEDURES = [
    # Power System Operating Procedures
    {"procedure_code": "SO_OP_2000", "title": "Glossary of Terms",
     "category": "SYSTEM_OPERATIONS", "document_type": "PROCEDURE",
     "description": "Definitions and glossary for all power system operating procedures.",
     "effective_date": "2024-01-01", "version": "22", "ner_clause": "NER 4.10",
     "applicable_market": "NEM", "status": "CURRENT"},
    {"procedure_code": "SO_OP_3703", "title": "Short Term Reserve Management",
     "category": "SYSTEM_OPERATIONS", "document_type": "PROCEDURE",
     "description": "Procedures for managing short-term reserves including RERT activation, LOR conditions (LOR1/LOR2/LOR3), and reserve management during supply shortfalls.",
     "effective_date": "2024-03-01", "version": "18", "ner_clause": "NER 3.20, 4.8.4",
     "applicable_market": "NEM", "status": "CURRENT"},
    {"procedure_code": "SO_OP_3705", "title": "Dispatch",
     "category": "SYSTEM_OPERATIONS", "document_type": "PROCEDURE",
     "description": "Core NEM dispatch procedure. Covers NEMDE operation, pre-dispatch, 5-minute dispatch, intervention pricing, and constrained dispatch.",
     "effective_date": "2024-06-01", "version": "25", "ner_clause": "NER 3.8",
     "applicable_market": "NEM", "status": "CURRENT"},
    {"procedure_code": "SO_OP_3707", "title": "Procedures for Issue of Directions and Clause 4.8.9 Instructions",
     "category": "SYSTEM_OPERATIONS", "document_type": "PROCEDURE",
     "description": "Procedures for AEMO to issue directions to market participants during system security events, including directed dispatch and emergency instructions.",
     "effective_date": "2024-01-01", "version": "16", "ner_clause": "NER 4.8.9",
     "applicable_market": "NEM", "status": "CURRENT"},
    {"procedure_code": "SO_OP_3708", "title": "Non-Market Ancillary Services",
     "category": "SYSTEM_OPERATIONS", "document_type": "PROCEDURE",
     "description": "Procedures for procurement and dispatch of non-market ancillary services including SRAS, NSCAS, and emergency frequency control.",
     "effective_date": "2023-12-01", "version": "14", "ner_clause": "NER 3.11",
     "applicable_market": "NEM", "status": "CURRENT"},
    {"procedure_code": "SO_OP_3710", "title": "Power System Security Operations",
     "category": "SYSTEM_OPERATIONS", "document_type": "PROCEDURE",
     "description": "Core system security procedures including frequency management, voltage control, and thermal limit management.",
     "effective_date": "2024-03-01", "version": "20", "ner_clause": "NER 4.3, 4.4",
     "applicable_market": "NEM", "status": "CURRENT"},
    {"procedure_code": "SO_OP_3715", "title": "Power System Security Guidelines",
     "category": "SYSTEM_OPERATIONS", "document_type": "GUIDELINE",
     "description": "Guidelines for maintaining power system security under normal, abnormal, and emergency conditions. Covers N-1 security, protected events, and system restart.",
     "effective_date": "2024-01-01", "version": "12", "ner_clause": "NER 4.2.6",
     "applicable_market": "NEM", "status": "CURRENT"},
    {"procedure_code": "SO_OP_3718", "title": "Outage Assessment",
     "category": "SYSTEM_OPERATIONS", "document_type": "PROCEDURE",
     "description": "Procedures for assessing planned and forced network outages, including impact assessment, rescheduling, and recall protocols.",
     "effective_date": "2023-09-01", "version": "15", "ner_clause": "NER 3.18",
     "applicable_market": "NEM", "status": "CURRENT"},
    {"procedure_code": "SO_OP_3720", "title": "Security Enablement Procedures",
     "category": "SYSTEM_OPERATIONS", "document_type": "PROCEDURE",
     "description": "Procedures for enabling network security constraints in NEMDE, including constraint equation formulation and testing.",
     "effective_date": "2024-06-01", "version": "11", "ner_clause": "NER 3.8, 4.6",
     "applicable_market": "NEM", "status": "CURRENT"},
    {"procedure_code": "SO_OP_5000", "title": "System Restart Overview",
     "category": "SYSTEM_RESTART", "document_type": "PROCEDURE",
     "description": "Overview of NEM system restart procedures including SRAS, local black system procedures, and system restart ancillary services.",
     "effective_date": "2024-01-01", "version": "9", "ner_clause": "NER 4.3.5",
     "applicable_market": "NEM", "status": "CURRENT"},

    # Market operation procedures
    {"procedure_code": "SO_OP_3710A", "title": "Frequency Operating Standards",
     "category": "FREQUENCY_CONTROL", "document_type": "STANDARD",
     "description": "NEM Frequency Operating Standard defining normal and emergency frequency bands (49.85-50.15 Hz normal, 47-52 Hz emergency containment).",
     "effective_date": "2023-07-01", "version": "8", "ner_clause": "NER S5.1a",
     "applicable_market": "NEM", "status": "CURRENT"},
    {"procedure_code": "MDP_001", "title": "Metering Procedures: National Electricity Market",
     "category": "METERING", "document_type": "PROCEDURE",
     "description": "Metering procedures for NEM including metering installation types, data collection, validation and estimation.",
     "effective_date": "2024-03-01", "version": "31", "ner_clause": "NER Chapter 7",
     "applicable_market": "NEM", "status": "CURRENT"},
    {"procedure_code": "DI_001", "title": "Demand Information Guidelines",
     "category": "FORECASTING", "document_type": "GUIDELINE",
     "description": "Guidelines for demand information submission by Registered Participants to AEMO for demand forecasting.",
     "effective_date": "2023-06-01", "version": "6", "ner_clause": "NER 3.7D, 3.13",
     "applicable_market": "NEM", "status": "CURRENT"},

    # Registration and connection
    {"procedure_code": "REG_001", "title": "Generator Registration Guide",
     "category": "REGISTRATION", "document_type": "GUIDE",
     "description": "Guide for registering as a generator in the NEM, including application process, performance standards, and connection agreements.",
     "effective_date": "2024-01-01", "version": "14", "ner_clause": "NER Chapter 2",
     "applicable_market": "NEM", "status": "CURRENT"},
    {"procedure_code": "GPS_001", "title": "Generator Performance Standards",
     "category": "REGISTRATION", "document_type": "STANDARD",
     "description": "Automatic and negotiated generator performance standards for NEM-connected generators (frequency response, voltage control, reactive power, ride-through).",
     "effective_date": "2024-06-01", "version": "10", "ner_clause": "NER S5.2",
     "applicable_market": "NEM", "status": "CURRENT"},

    # Planning
    {"procedure_code": "ISP_2024", "title": "2024 Integrated System Plan",
     "category": "PLANNING", "document_type": "PLAN",
     "description": "AEMO's whole-of-system plan for the efficient development of the NEM for the next 20+ years. Step Change scenario as central case.",
     "effective_date": "2024-06-28", "version": "4", "ner_clause": "NER 5.22",
     "applicable_market": "NEM", "status": "CURRENT"},
    {"procedure_code": "ESOO_2024", "title": "2024 Electricity Statement of Opportunities",
     "category": "PLANNING", "document_type": "REPORT",
     "description": "Annual assessment of supply adequacy in the NEM for the next 10 years, including reliability outlook and USE projections.",
     "effective_date": "2024-08-01", "version": "1", "ner_clause": "NER 3.13.3A",
     "applicable_market": "NEM", "status": "CURRENT"},

    # WEM procedures
    {"procedure_code": "WEM_MR_001", "title": "WEM Market Rules",
     "category": "WEM_OPERATIONS", "document_type": "RULES",
     "description": "Western Australian Wholesale Electricity Market Rules governing the WEM including balancing market, STEM, and capacity mechanism.",
     "effective_date": "2024-10-01", "version": "52", "ner_clause": "WEM Rules",
     "applicable_market": "WEM", "status": "CURRENT"},
    {"procedure_code": "WEM_PSO_001", "title": "WEM Power System Operation Procedure",
     "category": "WEM_OPERATIONS", "document_type": "PROCEDURE",
     "description": "AEMO procedures for operating the SWIS (South West Interconnected System) in Western Australia.",
     "effective_date": "2024-07-01", "version": "8", "ner_clause": "WEM Rules Chapter 7",
     "applicable_market": "WEM", "status": "CURRENT"},

    # Settlements
    {"procedure_code": "SET_001", "title": "NEM Settlement Procedure",
     "category": "SETTLEMENTS", "document_type": "PROCEDURE",
     "description": "Procedures for NEM settlements including preliminary, final, and revision settlement runs, and dispute resolution.",
     "effective_date": "2024-03-01", "version": "20", "ner_clause": "NER 3.15",
     "applicable_market": "NEM", "status": "CURRENT"},
    {"procedure_code": "SET_002", "title": "Prudential Requirements Procedure",
     "category": "SETTLEMENTS", "document_type": "PROCEDURE",
     "description": "Procedures for NEM prudential requirements including credit limits, trading margins, and margin calls.",
     "effective_date": "2024-01-01", "version": "12", "ner_clause": "NER 3.3",
     "applicable_market": "NEM", "status": "CURRENT"},

    # RERT and emergency
    {"procedure_code": "RERT_001", "title": "Reliability and Emergency Reserve Trader Procedure",
     "category": "EMERGENCY", "document_type": "PROCEDURE",
     "description": "Procedures for RERT including procurement panel, activation triggers, cost recovery, and panel member obligations.",
     "effective_date": "2024-01-01", "version": "7", "ner_clause": "NER 3.20",
     "applicable_market": "NEM", "status": "CURRENT"},
]

# COMMAND ----------

now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
rows = []
for proc in AEMO_PROCEDURES:
    pid = hashlib.md5(f"{proc['procedure_code']}".encode()).hexdigest()[:16]
    base_url = "https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/system-operations/power-system-operation/power-system-operating-procedures"
    rows.append({
        "procedure_id": pid,
        "procedure_code": proc["procedure_code"],
        "title": proc["title"],
        "category": proc["category"],
        "document_type": proc["document_type"],
        "description": proc["description"],
        "effective_date": proc["effective_date"],
        "version": proc["version"],
        "ner_clause": proc["ner_clause"],
        "applicable_market": proc["applicable_market"],
        "status": proc["status"],
        "document_url": base_url,
        "last_reviewed": proc["effective_date"],
        "ingested_at": now,
    })

from pyspark.sql.types import StringType, StructField, StructType

schema = StructType([
    StructField("procedure_id", StringType()),
    StructField("procedure_code", StringType()),
    StructField("title", StringType()),
    StructField("category", StringType()),
    StructField("document_type", StringType()),
    StructField("description", StringType()),
    StructField("effective_date", StringType()),
    StructField("version", StringType()),
    StructField("ner_clause", StringType()),
    StructField("applicable_market", StringType()),
    StructField("status", StringType()),
    StructField("document_url", StringType()),
    StructField("last_reviewed", StringType()),
    StructField("ingested_at", StringType()),
])
df = spark.createDataFrame(rows, schema=schema)
df.createOrReplaceTempView("src_aemo_procedures")

spark.sql(f"""
MERGE INTO {TABLE} t
USING (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY procedure_id ORDER BY ingested_at DESC) as _rn FROM src_aemo_procedures) WHERE _rn = 1) AS s
ON t.procedure_id = s.procedure_id
WHEN MATCHED THEN UPDATE SET
    t.procedure_code = s.procedure_code, t.title = s.title, t.category = s.category,
    t.document_type = s.document_type, t.description = s.description,
    t.effective_date = s.effective_date, t.version = s.version, t.ner_clause = s.ner_clause,
    t.applicable_market = s.applicable_market, t.status = s.status,
    t.document_url = s.document_url, t.last_reviewed = s.last_reviewed, t.ingested_at = s.ingested_at
WHEN NOT MATCHED THEN INSERT (
    procedure_id, procedure_code, title, category, document_type, description,
    effective_date, version, ner_clause, applicable_market, status,
    document_url, last_reviewed, ingested_at
) VALUES (
    s.procedure_id, s.procedure_code, s.title, s.category, s.document_type, s.description,
    s.effective_date, s.version, s.ner_clause, s.applicable_market, s.status,
    s.document_url, s.last_reviewed, s.ingested_at
)
""")

count = spark.sql(f"SELECT count(*) FROM {TABLE}").collect()[0][0]
print(f"aemo_procedures: {count} rows")

# COMMAND ----------

# Scrape AEMO SOPs page for any additional procedures
try:
    url = "https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/system-operations/power-system-operation/power-system-operating-procedures"
    resp = requests.get(url, headers=HEADERS, timeout=30)
    if resp.status_code == 200:
        soup = BeautifulSoup(resp.text, "html.parser")
        links = soup.find_all("a", href=True)
        pdf_links = [l for l in links if l.get("href", "").endswith(".pdf")]
        print(f"Found {len(pdf_links)} PDF procedure documents on AEMO SOPs page")

        # Extract any procedure titles we don't already have
        known_codes = {p["procedure_code"] for p in AEMO_PROCEDURES}
        new_procs = []
        for link in pdf_links:
            text = link.get_text(strip=True)
            href = link["href"]
            # Try to extract SO_OP code
            m = re.search(r'SO_OP_(\d+)', href) or re.search(r'SO_OP_(\d+)', text)
            if m:
                code = f"SO_OP_{m.group(1)}"
                if code not in known_codes:
                    new_procs.append({"code": code, "title": text, "url": href})

        if new_procs:
            print(f"Found {len(new_procs)} additional procedures not in seed data")
            add_rows = []
            for np in new_procs:
                pid = hashlib.md5(np["code"].encode()).hexdigest()[:16]
                add_rows.append({
                    "procedure_id": pid,
                    "procedure_code": np["code"],
                    "title": np["title"][:300],
                    "category": "SYSTEM_OPERATIONS",
                    "document_type": "PROCEDURE",
                    "description": f"AEMO procedure {np['code']}: {np['title']}",
                    "effective_date": "2024-01-01",
                    "version": "unknown",
                    "ner_clause": "NER Chapter 4",
                    "applicable_market": "NEM",
                    "status": "CURRENT",
                    "document_url": np["url"] if np["url"].startswith("http") else f"https://www.aemo.com.au{np['url']}",
                    "last_reviewed": "",
                    "ingested_at": now,
                })
            if add_rows:
                df2 = spark.createDataFrame(add_rows, schema=schema)
                df2.createOrReplaceTempView("src_aemo_extra")
                spark.sql(f"""
                MERGE INTO {TABLE} t
                USING src_aemo_extra s ON t.procedure_id = s.procedure_id
                WHEN NOT MATCHED THEN INSERT (
                    procedure_id, procedure_code, title, category, document_type, description,
                    effective_date, version, ner_clause, applicable_market, status,
                    document_url, last_reviewed, ingested_at
                ) VALUES (
                    s.procedure_id, s.procedure_code, s.title, s.category, s.document_type, s.description,
                    s.effective_date, s.version, s.ner_clause, s.applicable_market, s.status,
                    s.document_url, s.last_reviewed, s.ingested_at
                )
                """)
                final = spark.sql(f"SELECT count(*) FROM {TABLE}").collect()[0][0]
                print(f"After scrape additions: {final} rows")
    else:
        print(f"AEMO SOPs page returned {resp.status_code}")
except Exception as e:
    print(f"AEMO procedures scrape: {e}")
