from __future__ import annotations
import random
from datetime import datetime, timezone
from typing import Optional
from fastapi import APIRouter, Query

router = APIRouter()

# =========================================================================
# NEM Suspension / Market Events Analysis endpoints
# =========================================================================

@router.get("/api/nem-suspension/dashboard")
async def nem_suspension_dashboard():
    import random
    ts = "2026-02-27T06:00:00+11:00"
    events = [
        {"event_id":"EVT001","event_name":"SA System Black","start_date":"2016-09-28","end_date":"2016-10-11","duration_days":13,"event_type":"SYSTEM_BLACK","regions_affected":["SA1"],"trigger":"Severe storm destroyed transmission towers; cascading failure","avg_spot_price_before_aud_mwh":52.30,"avg_spot_price_during_aud_mwh":14200.00,"max_spot_price_aud_mwh":14200.00,"total_market_cost_m_aud":367.0,"load_shed_mwh":524000,"generators_directed":12,"aemo_market_notices":47},
        {"event_id":"EVT002","event_name":"QLD-NSW Separation Event","start_date":"2021-05-25","end_date":"2021-05-26","duration_days":1,"event_type":"SEPARATION","regions_affected":["QLD1","NSW1"],"trigger":"QNI trip due to lightning; QLD islanded","avg_spot_price_before_aud_mwh":68.40,"avg_spot_price_during_aud_mwh":1540.00,"max_spot_price_aud_mwh":15100.00,"total_market_cost_m_aud":42.0,"load_shed_mwh":0,"generators_directed":4,"aemo_market_notices":15},
        {"event_id":"EVT003","event_name":"Feb 2017 NSW Heatwave","start_date":"2017-02-10","end_date":"2017-02-12","duration_days":2,"event_type":"PRICE_SPIKE","regions_affected":["NSW1","VIC1"],"trigger":"Extreme heat 45°C+; record demand; Liddell unit trip","avg_spot_price_before_aud_mwh":71.20,"avg_spot_price_during_aud_mwh":6800.00,"max_spot_price_aud_mwh":14200.00,"total_market_cost_m_aud":185.0,"load_shed_mwh":35000,"generators_directed":7,"aemo_market_notices":28},
        {"event_id":"EVT004","event_name":"2019 VIC Bushfire Emergency","start_date":"2019-11-21","end_date":"2019-11-24","duration_days":3,"event_type":"EMERGENCY","regions_affected":["VIC1"],"trigger":"Bushfire damage to 500kV Moorabool–Haunted Gully line","avg_spot_price_before_aud_mwh":58.10,"avg_spot_price_during_aud_mwh":2340.00,"max_spot_price_aud_mwh":14700.00,"total_market_cost_m_aud":89.0,"load_shed_mwh":18000,"generators_directed":5,"aemo_market_notices":22},
        {"event_id":"EVT005","event_name":"June 2022 Market Suspension","start_date":"2022-06-15","end_date":"2022-06-24","duration_days":9,"event_type":"MARKET_SUSPENSION","regions_affected":["NSW1","QLD1","SA1","VIC1","TAS1"],"trigger":"Cumulative coal outages + gas shortages; AEMO suspended spot market","avg_spot_price_before_aud_mwh":264.00,"avg_spot_price_during_aud_mwh":0.0,"max_spot_price_aud_mwh":15500.00,"total_market_cost_m_aud":1850.0,"load_shed_mwh":0,"generators_directed":28,"aemo_market_notices":92},
        {"event_id":"EVT006","event_name":"TAS Basslink Cable Failure","start_date":"2015-12-20","end_date":"2016-06-13","duration_days":176,"event_type":"SEPARATION","regions_affected":["TAS1"],"trigger":"Basslink undersea cable failure; TAS islanded for 6 months","avg_spot_price_before_aud_mwh":38.50,"avg_spot_price_during_aud_mwh":95.40,"max_spot_price_aud_mwh":1420.00,"total_market_cost_m_aud":145.0,"load_shed_mwh":0,"generators_directed":3,"aemo_market_notices":180},
        {"event_id":"EVT007","event_name":"Jan 2024 SA Heatwave Spike","start_date":"2024-01-18","end_date":"2024-01-19","duration_days":1,"event_type":"PRICE_SPIKE","regions_affected":["SA1","VIC1"],"trigger":"Heatwave 44°C; wind output dropped to 5% capacity; gas peakers maxed","avg_spot_price_before_aud_mwh":82.00,"avg_spot_price_during_aud_mwh":4200.00,"max_spot_price_aud_mwh":16600.00,"total_market_cost_m_aud":62.0,"load_shed_mwh":8000,"generators_directed":6,"aemo_market_notices":14},
        {"event_id":"EVT008","event_name":"2020 QLD Callide Explosion","start_date":"2021-05-25","end_date":"2021-06-30","duration_days":36,"event_type":"EMERGENCY","regions_affected":["QLD1"],"trigger":"Callide C4 turbine hall explosion; 2 units offline for years","avg_spot_price_before_aud_mwh":55.00,"avg_spot_price_during_aud_mwh":310.00,"max_spot_price_aud_mwh":15100.00,"total_market_cost_m_aud":520.0,"load_shed_mwh":0,"generators_directed":8,"aemo_market_notices":65},
    ]
    interventions = [
        {"intervention_id":"INT001","event_id":"EVT001","intervention_type":"DIRECTION","date":"2016-09-28","region":"SA1","generator_or_party":"Torrens Island B","quantity_mw":480,"duration_hrs":72,"trigger_reason":"System restart after blackout","cost_m_aud":18.5,"outcome":"Successful restart sequence"},
        {"intervention_id":"INT002","event_id":"EVT001","intervention_type":"LOAD_SHEDDING","date":"2016-09-28","region":"SA1","generator_or_party":"SA Power Networks","quantity_mw":1200,"duration_hrs":8,"trigger_reason":"Total system black","cost_m_aud":0.0,"outcome":"Rolling restoration over 8 hours"},
        {"intervention_id":"INT003","event_id":"EVT003","intervention_type":"DIRECTION","date":"2017-02-10","region":"NSW1","generator_or_party":"Vales Point B","quantity_mw":660,"duration_hrs":12,"trigger_reason":"Reliability and Reserve Trader (RERT)","cost_m_aud":8.2,"outcome":"Avoided load shedding"},
        {"intervention_id":"INT004","event_id":"EVT003","intervention_type":"RERT_ACTIVATION","date":"2017-02-11","region":"NSW1","generator_or_party":"EnergyAustralia","quantity_mw":350,"duration_hrs":6,"trigger_reason":"LOR3 condition","cost_m_aud":12.0,"outcome":"350MW emergency reserve activated"},
        {"intervention_id":"INT005","event_id":"EVT005","intervention_type":"MARKET_SUSPENSION","date":"2022-06-15","region":"NSW1","generator_or_party":"AEMO","quantity_mw":0,"duration_hrs":216,"trigger_reason":"Administered pricing cap hit 7 times in 336 intervals","cost_m_aud":0.0,"outcome":"Spot market suspended; AEMO directed all generation"},
        {"intervention_id":"INT006","event_id":"EVT005","intervention_type":"DIRECTION","date":"2022-06-16","region":"QLD1","generator_or_party":"Gladstone PS","quantity_mw":1680,"duration_hrs":192,"trigger_reason":"Coal unit returning from outage; directed to generate","cost_m_aud":45.0,"outcome":"Critical base-load restored"},
        {"intervention_id":"INT007","event_id":"EVT005","intervention_type":"DIRECTION","date":"2022-06-17","region":"VIC1","generator_or_party":"Loy Yang A","quantity_mw":2200,"duration_hrs":168,"trigger_reason":"Unplanned outage recall","cost_m_aud":52.0,"outcome":"VIC supply stabilised"},
        {"intervention_id":"INT008","event_id":"EVT006","intervention_type":"DIRECTION","date":"2016-01-15","region":"TAS1","generator_or_party":"Tamar Valley CCGT","quantity_mw":208,"duration_hrs":2160,"trigger_reason":"Gas generation directed during Basslink outage","cost_m_aud":35.0,"outcome":"TAS supply maintained via hydro + gas"},
        {"intervention_id":"INT009","event_id":"EVT007","intervention_type":"LOAD_SHEDDING","date":"2024-01-18","region":"SA1","generator_or_party":"SA Power Networks","quantity_mw":200,"duration_hrs":2,"trigger_reason":"LOR3 actual; insufficient generation","cost_m_aud":0.0,"outcome":"200MW rotational load shed"},
        {"intervention_id":"INT010","event_id":"EVT008","intervention_type":"DIRECTION","date":"2021-05-26","region":"QLD1","generator_or_party":"Stanwell PS","quantity_mw":1400,"duration_hrs":480,"trigger_reason":"Emergency direction to cover Callide C loss","cost_m_aud":28.0,"outcome":"QLD supply maintained"},
    ]
    timeline = [
        {"record_id":"TL001","event_id":"EVT005","timestamp":"2022-06-12T00:00:00","milestone":"Cumulative Price Threshold (CPT) exceeded","milestone_type":"TRIGGER","region":"QLD1","detail":"QLD CPT hit $1,359,100 — exceeding $1,313,100 threshold"},
        {"record_id":"TL002","event_id":"EVT005","timestamp":"2022-06-13T14:00:00","milestone":"Administered Price Cap activated","milestone_type":"INTERVENTION","region":"NSW1","detail":"APC of $300/MWh applied across NEM"},
        {"record_id":"TL003","event_id":"EVT005","timestamp":"2022-06-15T14:05:00","milestone":"AEMO suspends spot market","milestone_type":"SUSPENSION","region":"ALL","detail":"First NEM-wide market suspension in history"},
        {"record_id":"TL004","event_id":"EVT005","timestamp":"2022-06-22T00:00:00","milestone":"Coal units begin returning","milestone_type":"RECOVERY","region":"QLD1","detail":"Gladstone and Stanwell units restarted"},
        {"record_id":"TL005","event_id":"EVT005","timestamp":"2022-06-24T06:00:00","milestone":"Spot market resumes","milestone_type":"RESOLUTION","region":"ALL","detail":"AEMO lifts suspension; normal dispatch resumes"},
        {"record_id":"TL006","event_id":"EVT001","timestamp":"2016-09-28T16:18:00","milestone":"Tornado destroys transmission towers","milestone_type":"TRIGGER","region":"SA1","detail":"Two tornadoes damage 22 transmission towers on 275kV lines"},
        {"record_id":"TL007","event_id":"EVT001","timestamp":"2016-09-28T16:18:30","milestone":"SA system black","milestone_type":"SUSPENSION","region":"SA1","detail":"Entire SA grid collapses; 1.7 million customers without power"},
        {"record_id":"TL008","event_id":"EVT001","timestamp":"2016-09-28T19:00:00","milestone":"Restoration begins","milestone_type":"RECOVERY","region":"SA1","detail":"Torrens Island B directed to start; first loads restored"},
        {"record_id":"TL009","event_id":"EVT001","timestamp":"2016-10-11T00:00:00","milestone":"Full restoration complete","milestone_type":"RESOLUTION","region":"SA1","detail":"All SA loads restored; investigation begins"},
        {"record_id":"TL010","event_id":"EVT008","timestamp":"2021-05-25T13:44:00","milestone":"Callide C4 turbine explosion","milestone_type":"TRIGGER","region":"QLD1","detail":"Catastrophic turbine failure at Callide C4; 2 workers injured"},
        {"record_id":"TL011","event_id":"EVT008","timestamp":"2021-05-25T14:00:00","milestone":"QLD-NSW separation","milestone_type":"INTERVENTION","region":"QLD1","detail":"QNI trips due to frequency deviation; QLD islanded"},
        {"record_id":"TL012","event_id":"EVT008","timestamp":"2021-06-30T00:00:00","milestone":"QLD supply stabilised","milestone_type":"RESOLUTION","region":"QLD1","detail":"Stanwell and Gladstone compensate for Callide loss"},
        {"record_id":"TL013","event_id":"EVT007","timestamp":"2024-01-18T14:30:00","milestone":"SA LOR3 declared","milestone_type":"TRIGGER","region":"SA1","detail":"Wind drops to 5% capacity; temperature hits 44°C"},
        {"record_id":"TL014","event_id":"EVT007","timestamp":"2024-01-18T15:00:00","milestone":"Load shedding activated","milestone_type":"INTERVENTION","region":"SA1","detail":"200MW rotational load shed across Adelaide metro"},
        {"record_id":"TL015","event_id":"EVT007","timestamp":"2024-01-19T06:00:00","milestone":"Conditions ease","milestone_type":"RESOLUTION","region":"SA1","detail":"Temperature drops; wind output recovers; LOR3 cancelled"},
    ]
    total_cost = sum(e["total_market_cost_m_aud"] for e in events)
    total_shed = sum(e["load_shed_mwh"] for e in events) / 1000  # to GWh
    total_days = sum(e["duration_days"] for e in events)
    return {"timestamp":ts,"total_events_5yr":len(events),"total_suspension_days":total_days,"total_market_cost_m_aud":round(total_cost,1),"total_load_shed_gwh":round(total_shed,1),"events":events,"interventions":interventions,"timeline":timeline}

@router.get("/api/nem-suspension/events")
async def nem_suspension_events():
    d = await nem_suspension_dashboard()
    return d["events"]

@router.get("/api/nem-suspension/interventions")
async def nem_suspension_interventions():
    d = await nem_suspension_dashboard()
    return d["interventions"]

@router.get("/api/nem-suspension/timeline")
async def nem_suspension_timeline():
    d = await nem_suspension_dashboard()
    return d["timeline"]

# =========================================================================
# Price Setter & Marginal Generator Analytics
# =========================================================================

@router.get("/api/price-setter/dashboard")
async def price_setter_dashboard(region: str = "SA1"):
    import random as _r
    _r.seed(hash(region) % 10000)

    fuels = ["Wind", "Solar", "Gas OCGT", "Gas CCGT", "Battery", "Coal", "Hydro"]
    stations = {
        "Wind": [("ARWF1", "Ararat Wind Farm"), ("HDWF1", "Hallet Wind Farm"), ("MLWF1", "Macarthur Wind")],
        "Solar": [("DDSF1", "Darlington Point Solar"), ("BALBG1", "Bald Hills Solar"), ("LRSF1", "Limondale Solar")],
        "Gas OCGT": [("CALL_B_1", "Callide B1"), ("OSPS1", "Osborne PS"), ("PPCCGT", "Pelican Point")],
        "Gas CCGT": [("PPCCGT", "Pelican Point"), ("TORRA1", "Torrens Island A1")],
        "Battery": [("HPRG1", "Hornsdale Power Reserve"), ("VBBL1", "Victorian Big Battery")],
        "Coal": [("BW01", "Bayswater 1"), ("ER01", "Eraring 1"), ("VP5", "Vales Point 5")],
        "Hydro": [("MURRAY1", "Murray 1"), ("TUMUT3", "Tumut 3")],
    }

    # Generate 24 interval records (5-min intervals, last 2 hours)
    records = []
    for i in range(24):
        fuel = _r.choice(fuels)
        duid, sname = _r.choice(stations[fuel])
        price = round(_r.gauss(85 if region == "SA1" else 65, 40), 2)
        strategic = _r.random() < 0.12
        records.append({
            "interval": f"{6 + i // 12}:{(i % 12) * 5:02d}",
            "region": region,
            "duid": duid,
            "station_name": sname,
            "fuel_type": fuel,
            "dispatch_price": price,
            "dispatch_quantity_mw": round(_r.uniform(50, 500), 1),
            "offer_band": f"Band {_r.randint(1, 10)}",
            "offer_price": round(price * _r.uniform(0.7, 1.0), 2),
            "is_strategic": strategic,
            "shadow_price_mw": round(_r.uniform(0.5, 8.0), 1),
        })

    # Fuel type stats
    fuel_stats = []
    remaining = 100.0
    for j, fuel in enumerate(fuels):
        pct = round(_r.uniform(5, 30), 1) if j < len(fuels) - 1 else round(remaining, 1)
        remaining -= pct
        if remaining < 0:
            pct += remaining
            remaining = 0
        fuel_stats.append({
            "fuel_type": fuel,
            "intervals_as_price_setter": _r.randint(10, 80),
            "pct_of_all_intervals": round(max(pct, 1.0), 1),
            "avg_price_aud_mwh": round(_r.uniform(40, 200), 0),
            "max_price_aud_mwh": round(_r.uniform(200, 5000), 0),
            "economic_rent_est_m_aud": round(_r.uniform(0.1, 15.0), 2),
        })

    # Frequency stats
    freq_stats = []
    for fuel in fuels:
        for duid, sname in stations[fuel][:1]:
            freq_stats.append({
                "duid": duid,
                "station_name": sname,
                "fuel_type": fuel,
                "region": region,
                "capacity_mw": _r.randint(100, 800),
                "intervals_as_price_setter": _r.randint(5, 60),
                "pct_intervals": round(_r.uniform(1, 25), 1),
                "avg_price_when_setter": round(_r.uniform(30, 250), 0),
                "max_price_when_setter": round(_r.uniform(200, 8000), 0),
                "estimated_daily_price_power_aud": round(_r.uniform(500, 50000), 0),
                "strategic_bids_pct": round(_r.uniform(0, 35), 1),
            })
    freq_stats.sort(key=lambda x: x["intervals_as_price_setter"], reverse=True)

    dominant = max(freq_stats, key=lambda x: x["intervals_as_price_setter"])
    dominant_fuel = max(fuel_stats, key=lambda x: x["pct_of_all_intervals"])
    strategic_pct = round(sum(1 for r in records if r["is_strategic"]) / max(len(records), 1) * 100, 1)
    avg_price = round(sum(r["dispatch_price"] for r in records) / max(len(records), 1), 0)
    current = records[-1] if records else None

    return {
        "timestamp": "2026-02-27T08:00:00Z",
        "region": region,
        "total_intervals_today": len(records),
        "dominant_price_setter": dominant["station_name"],
        "dominant_fuel_type": dominant_fuel["fuel_type"],
        "strategic_bid_frequency_pct": strategic_pct,
        "avg_price_today": avg_price,
        "current_price_setter": current["station_name"] if current else "Unknown",
        "current_price": current["dispatch_price"] if current else 0,
        "price_setter_records": records,
        "frequency_stats": freq_stats,
        "fuel_type_stats": fuel_stats,
    }

@router.get("/api/price-setter/records")
async def price_setter_records(region: str = "SA1"):
    data = await price_setter_dashboard(region)
    return data["price_setter_records"]

@router.get("/api/price-setter/frequency")
async def price_setter_frequency(region: str = "SA1"):
    data = await price_setter_dashboard(region)
    return data["frequency_stats"]

# =========================================================================
# Spot Price Cap & CPT Analytics
# =========================================================================

def _build_spot_cap_data():
    import random as _r
    _r.seed(42)
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]

    events = []
    for i in range(30):
        reg = _r.choice(regions)
        is_floor = _r.random() < 0.25
        spot = round(_r.uniform(-1100, -900), 2) if is_floor else round(_r.uniform(14000, 15500), 2)
        events.append({
            "event_id": f"CE-2025-{i+1:04d}",
            "region": reg,
            "trading_interval": f"2025-{_r.randint(1,12):02d}-{_r.randint(1,28):02d}T{_r.randint(6,20):02d}:{_r.choice(['00','05','10','15','20','25','30','35','40','45','50','55'])}:00",
            "spot_price": spot,
            "market_price_cap": 15500,
            "below_floor": is_floor,
            "floor_price": -1000,
            "cumulative_price_at_interval": round(_r.uniform(200000, 1350000), 2),
            "dispatch_intervals_capped": _r.randint(0, 12) if not is_floor else 0,
        })

    cpt_records = []
    for reg in regions:
        for q in ["Q1-2025", "Q2-2025", "Q3-2025", "Q4-2025"]:
            base_cum = _r.uniform(400000, 1200000)
            cpt_records.append({
                "region": reg,
                "trading_date": f"2025-{['03','06','09','12'][['Q1-2025','Q2-2025','Q3-2025','Q4-2025'].index(q)]}-28",
                "cumulative_price": round(base_cum, 2),
                "cpt_threshold": 1300000,
                "pct_of_cpt": round(base_cum / 1300000 * 100, 2),
                "daily_avg_price": round(_r.uniform(50, 200), 2),
                "cap_events_today": _r.randint(0, 5),
                "floor_events_today": _r.randint(0, 2),
                "days_until_reset": _r.randint(1, 90),
                "quarter": q,
            })

    regional_summaries = []
    for reg in regions:
        regional_summaries.append({
            "region": reg,
            "year": 2025,
            "total_cap_events": _r.randint(10, 60),
            "total_floor_events": _r.randint(2, 20),
            "avg_price_during_cap_events": round(_r.uniform(12000, 15500), 0),
            "max_cumulative_price": round(_r.uniform(600000, 1350000), 0),
            "cpt_breaches": _r.randint(0, 2),
            "total_cpt_periods": _r.randint(3, 8),
            "revenue_impact_m_aud": round(_r.uniform(5, 80), 1),
        })

    active_cpt = [r for r in regions if _r.random() < 0.4]
    total_cap = sum(s["total_cap_events"] for s in regional_summaries)
    total_floor = sum(s["total_floor_events"] for s in regional_summaries)

    return {
        "timestamp": "2025-12-28T14:30:00+11:00",
        "market_price_cap_aud": 15500,
        "market_floor_price_aud": -1000,
        "cumulative_price_threshold_aud": 1300000,
        "cpt_period_days": 336,
        "national_cap_events_ytd": total_cap,
        "national_floor_events_ytd": total_floor,
        "active_cpt_regions": active_cpt,
        "cap_events": events,
        "cpt_tracker": cpt_records,
        "regional_summaries": regional_summaries,
    }

@router.get("/api/spot-cap/dashboard")
async def spot_cap_dashboard():
    return _build_spot_cap_data()

@router.get("/api/spot-cap/cpt-tracker")
async def spot_cap_cpt_tracker(region: str = None, quarter: str = None):
    data = _build_spot_cap_data()
    records = data["cpt_tracker"]
    if region:
        records = [r for r in records if r["region"] == region]
    if quarter:
        records = [r for r in records if r["quarter"] == quarter]
    return records

@router.get("/api/spot-cap/cap-events")
async def spot_cap_events(region: str = None):
    data = _build_spot_cap_data()
    events = data["cap_events"]
    if region:
        events = [e for e in events if e["region"] == region]
    return events
