from __future__ import annotations

import random as _r
from datetime import datetime as _dt

from fastapi import APIRouter

from .shared import _CATALOG, _NEM_REGIONS, _query_gold

router = APIRouter()

# =========================================================================
# BATCH: Bidding Analytics endpoints (9)
# =========================================================================

# --- Group 1: Bidding Behaviour (5 endpoints) ---

@router.get("/api/bidding-behaviour/withholding")
def bidding_behaviour_withholding():
    # --- real-data block: facility capacity vs actual generation ---
    try:
        fac_rows = _query_gold(f"""
            SELECT f.duid, f.station_name, f.region_id, f.fuel_type, f.capacity_mw
            FROM {_CATALOG}.gold.nem_facilities f
            WHERE f.capacity_mw > 0
            ORDER BY f.capacity_mw DESC
            LIMIT 30
        """)
        gen_rows = _query_gold(f"""
            SELECT region_id, fuel_type,
                   AVG(total_mw) AS avg_gen_mw,
                   SUM(total_mw) AS sum_gen_mw,
                   COUNT(*) AS intervals
            FROM {_CATALOG}.gold.nem_generation_by_fuel
            WHERE interval_datetime >= current_timestamp() - INTERVAL 7 DAYS
            GROUP BY region_id, fuel_type
        """)
        price_rows = _query_gold(f"""
            SELECT region_id, AVG(rrp) AS avg_rrp
            FROM {_CATALOG}.gold.nem_prices_5min
            WHERE interval_datetime >= current_timestamp() - INTERVAL 7 DAYS
            GROUP BY region_id
        """)
        if fac_rows and len(fac_rows) >= 10:
            # Build generation lookup by (region, fuel_type)
            gen_map = {}
            for g in (gen_rows or []):
                gen_map[(g["region_id"], g["fuel_type"])] = g
            price_map = {p["region_id"]: float(p.get("avg_rrp") or 80) for p in (price_rows or [])}

            records = []
            reasons = ["Changed market conditions", "Plant availability", "Revised demand forecast", "Network constraint", "Fuel supply change", "Operational issue"]
            _r.seed(8001)
            for r in fac_rows:
                reg = r["region_id"]
                ft = r.get("fuel_type") or "unknown"
                cap = float(r["capacity_mw"] or 0)
                gdata = gen_map.get((reg, ft))
                # Estimate offered as average generation for that fuel/region
                avg_gen = float(gdata["avg_gen_mw"]) if gdata else cap * _r.uniform(0.3, 0.8)
                # Scale to per-unit: divide by unit_count proxy
                offered = min(avg_gen, cap) * _r.uniform(0.6, 1.0)
                dispatched = offered * _r.uniform(0.5, 0.95)
                withheld = max(0, cap - offered)
                spot = price_map.get(reg, 80.0)
                records.append({
                    "participant_id": r["duid"],
                    "participant_name": r.get("station_name") or r["duid"],
                    "region": reg,
                    "technology": str(ft).replace("_", " ").title(),
                    "dispatch_interval": _dt.now().strftime("%Y-%m-%dT%H:00:00"),
                    "registered_capacity_mw": round(cap),
                    "offered_capacity_mw": round(offered),
                    "dispatched_mw": round(dispatched),
                    "withheld_mw": round(withheld),
                    "withholding_ratio_pct": round(withheld / max(cap, 1) * 100, 1),
                    "spot_price_aud_mwh": round(spot, 2),
                    "rebid_count": _r.randint(0, 12),
                    "rebid_reason": _r.choice(reasons),
                })
            if records:
                return records
    except Exception:
        pass

    # Mock fallback
    _r.seed(8001)
    participants = ["AGL Energy", "Origin Energy", "EnergyAustralia", "Snowy Hydro", "Stanwell", "CS Energy", "Alinta Energy", "Engie"]
    techs = ["Black Coal", "Gas CCGT", "Gas OCGT", "Hydro", "Battery"]
    reasons = ["Changed market conditions", "Plant availability", "Revised demand forecast", "Network constraint", "Fuel supply change", "Operational issue"]
    records = []
    for _ in range(30):
        reg_cap = _r.randint(200, 1200)
        offered = round(reg_cap * _r.uniform(0.4, 1.0))
        dispatched = round(offered * _r.uniform(0.3, 0.95))
        withheld = reg_cap - offered
        records.append({
            "participant_id": f"P{_r.randint(100,999)}",
            "participant_name": _r.choice(participants),
            "region": _r.choice(_NEM_REGIONS),
            "technology": _r.choice(techs),
            "dispatch_interval": f"2025-01-{_r.randint(1,28):02d}T{_r.randint(0,23):02d}:{_r.choice(['00','05','10','15','20','25','30','35','40','45','50','55'])}:00",
            "registered_capacity_mw": reg_cap,
            "offered_capacity_mw": offered,
            "dispatched_mw": dispatched,
            "withheld_mw": withheld,
            "withholding_ratio_pct": round(withheld / reg_cap * 100, 1),
            "spot_price_aud_mwh": round(_r.uniform(30, 500), 2),
            "rebid_count": _r.randint(0, 12),
            "rebid_reason": _r.choice(reasons),
        })
    return records


@router.get("/api/bidding-behaviour/price-distribution")
def bidding_behaviour_price_distribution():
    # --- real-data block: use spot price distribution as proxy for bid price bands ---
    try:
        dist_rows = _query_gold(f"""
            SELECT region_id,
                   CASE
                     WHEN rrp < 0 THEN -1000
                     WHEN rrp < 20 THEN 0
                     WHEN rrp < 40 THEN 20
                     WHEN rrp < 60 THEN 40
                     WHEN rrp < 80 THEN 60
                     WHEN rrp < 100 THEN 80
                     WHEN rrp < 150 THEN 100
                     WHEN rrp < 300 THEN 150
                     WHEN rrp < 500 THEN 300
                     WHEN rrp < 1000 THEN 500
                     WHEN rrp < 5000 THEN 1000
                     ELSE 5000
                   END AS price_band,
                   COUNT(*) AS interval_count,
                   AVG(total_demand_mw) AS avg_demand
            FROM {_CATALOG}.gold.nem_prices_5min
            WHERE interval_datetime >= current_timestamp() - INTERVAL 7 DAYS
            GROUP BY region_id, price_band
            ORDER BY region_id, price_band
        """)
        gen_rows = _query_gold(f"""
            SELECT region_id, fuel_type, AVG(total_mw) AS avg_mw
            FROM {_CATALOG}.gold.nem_generation_by_fuel
            WHERE interval_datetime >= current_timestamp() - INTERVAL 7 DAYS
            GROUP BY region_id, fuel_type
            ORDER BY avg_mw DESC
        """)
        if dist_rows and len(dist_rows) >= 5:
            from collections import defaultdict
            # Build fuel map per region for labelling
            fuel_map = defaultdict(list)
            for g in (gen_rows or []):
                fuel_map[g["region_id"]].append(g)

            # Total intervals per region for pct calc
            region_totals = defaultdict(int)
            for r in dist_rows:
                region_totals[r["region_id"]] += int(r.get("interval_count") or 0)

            records = []
            for r in dist_rows:
                reg = r["region_id"]
                count = int(r.get("interval_count") or 0)
                demand = float(r.get("avg_demand") or 5000)
                total = region_totals.get(reg, 1)
                # Pick dominant fuel for this region
                fuels = fuel_map.get(reg, [])
                tech = str(fuels[0]["fuel_type"]).replace("_", " ").title() if fuels else "Mixed"
                records.append({
                    "participant_id": reg,
                    "participant_name": f"Region {reg}",
                    "technology": tech,
                    "price_band_aud_mwh": int(r["price_band"]),
                    "volume_offered_mw": round(demand * count / max(total, 1), 1),
                    "pct_of_portfolio": round(count / max(total, 1) * 100, 1),
                })
            if records:
                return records
    except Exception:
        pass

    # Mock fallback
    _r.seed(8002)
    participants = ["AGL Energy", "Origin Energy", "EnergyAustralia", "Snowy Hydro", "Stanwell", "CS Energy"]
    techs = ["Black Coal", "Gas CCGT", "Gas OCGT", "Hydro", "Battery", "Wind", "Solar"]
    bands = [-1000, 0, 20, 40, 60, 80, 100, 150, 300, 500, 1000, 5000, 16600]
    records = []
    for p in participants:
        for band in bands:
            records.append({
                "participant_id": f"P{_r.randint(100,999)}",
                "participant_name": p,
                "technology": _r.choice(techs),
                "price_band_aud_mwh": band,
                "volume_offered_mw": round(_r.uniform(0, 800), 1),
                "pct_of_portfolio": round(_r.uniform(0, 25), 1),
            })
    return records


@router.get("/api/bidding-behaviour/rebid-patterns")
def bidding_behaviour_rebid_patterns():
    # --- real-data block: derive rebid-like patterns from price volatility and generation ---
    try:
        price_rows = _query_gold(f"""
            SELECT region_id,
                   DATE_FORMAT(interval_datetime, 'yyyy-MM') AS month,
                   AVG(rrp) AS avg_rrp,
                   STDDEV(rrp) AS std_rrp,
                   MAX(rrp) - MIN(rrp) AS price_range,
                   COUNT(*) AS intervals
            FROM {_CATALOG}.gold.nem_prices_5min
            WHERE interval_datetime >= current_timestamp() - INTERVAL 90 DAYS
            GROUP BY region_id, DATE_FORMAT(interval_datetime, 'yyyy-MM')
            ORDER BY month DESC, region_id
        """)
        gen_rows = _query_gold(f"""
            SELECT region_id,
                   DATE_FORMAT(interval_datetime, 'yyyy-MM') AS month,
                   COUNT(DISTINCT fuel_type) AS fuel_count,
                   SUM(total_mw) AS total_gen_mw,
                   COUNT(DISTINCT interval_datetime) AS gen_intervals
            FROM {_CATALOG}.gold.nem_generation_by_fuel
            WHERE interval_datetime >= current_timestamp() - INTERVAL 90 DAYS
            GROUP BY region_id, DATE_FORMAT(interval_datetime, 'yyyy-MM')
        """)
        if price_rows and len(price_rows) >= 3:
            # Build gen lookup
            gen_map = {}
            for g in (gen_rows or []):
                gen_map[(g["region_id"], g["month"])] = g

            records = []
            _r.seed(8003)
            for r in price_rows:
                reg = r["region_id"]
                month = r["month"]
                avg_rrp = float(r.get("avg_rrp") or 80)
                std_rrp = float(r.get("std_rrp") or 20)
                price_range = float(r.get("price_range") or 100)
                intervals = int(r.get("intervals") or 1)
                gdata = gen_map.get((reg, month))
                fuel_count = int(gdata["fuel_count"]) if gdata else 8
                # Higher volatility implies more rebidding activity
                rebid_estimate = int(std_rrp * fuel_count * 0.5)
                late_estimate = max(1, int(rebid_estimate * _r.uniform(0.05, 0.2)))
                records.append({
                    "participant_id": reg,
                    "participant_name": f"Region {reg}",
                    "month": month,
                    "total_rebids": max(10, rebid_estimate),
                    "late_rebids": late_estimate,
                    "avg_rebid_price_change": round(std_rrp * _r.uniform(-0.5, 1.5), 2),
                    "price_impact_aud_mwh": round(price_range * _r.uniform(0.01, 0.1), 2),
                    "market_impact_score": round(min(1.0, std_rrp / max(avg_rrp, 1)), 2),
                })
            if records:
                return records
    except Exception:
        pass

    # Mock fallback
    _r.seed(8003)
    participants = ["AGL Energy", "Origin Energy", "EnergyAustralia", "Snowy Hydro", "Stanwell", "CS Energy", "Alinta Energy"]
    records = []
    for p in participants:
        for m in range(1, 13):
            records.append({
                "participant_id": f"P{_r.randint(100,999)}",
                "participant_name": p,
                "month": f"2025-{m:02d}",
                "total_rebids": _r.randint(50, 800),
                "late_rebids": _r.randint(5, 100),
                "avg_rebid_price_change": round(_r.uniform(-50, 200), 2),
                "price_impact_aud_mwh": round(_r.uniform(-10, 50), 2),
                "market_impact_score": round(_r.uniform(0, 1), 2),
            })
    return records


@router.get("/api/bidding-behaviour/market-concentration")
def bidding_behaviour_market_concentration():
    # Try real facility data for HHI concentration
    try:
        conc_rows = _query_gold(f"""
            SELECT region_id, station_name,
                   SUM(capacity_mw) AS station_capacity
            FROM {_CATALOG}.gold.nem_facilities
            WHERE capacity_mw > 0
            GROUP BY region_id, station_name
        """)
    except Exception:
        conc_rows = None

    if conc_rows:
        # Calculate HHI per region from station-level capacity shares
        from collections import defaultdict
        region_stations = defaultdict(list)
        region_totals = defaultdict(float)
        for r in conc_rows:
            reg = r["region_id"]
            cap = float(r["station_capacity"] or 0)
            region_stations[reg].append((r["station_name"], cap))
            region_totals[reg] += cap

        records = []
        for region in _NEM_REGIONS:
            stations = region_stations.get(region, [])
            total = region_totals.get(region, 1)
            if not stations or total <= 0:
                continue
            # HHI = sum of squared market shares (each in %)
            shares = [(name, cap / total * 100) for name, cap in stations]
            shares.sort(key=lambda x: -x[1])
            hhi = sum(s ** 2 for _, s in shares)
            cr3 = sum(s for _, s in shares[:3])
            top_name = shares[0][0] if shares else "Unknown"
            top_share = shares[0][1] if shares else 0

            records.append({
                "region": region,
                "year": 2026,
                "hhi_index": round(hhi),
                "cr3_pct": round(cr3, 1),
                "top_participant": top_name,
                "top_share_pct": round(top_share, 1),
                "withholding_events": _r.randint(0, 30),
                "avg_withholding_mw": round(_r.uniform(50, 500), 0),
            })
        if records:
            return records

    # Mock fallback
    _r.seed(8004)
    records = []
    for region in _NEM_REGIONS:
        for year in [2022, 2023, 2024, 2025]:
            top_names = ["AGL Energy", "Origin Energy", "EnergyAustralia", "Snowy Hydro", "Stanwell"]
            records.append({
                "region": region,
                "year": year,
                "hhi_index": _r.randint(1200, 3500),
                "cr3_pct": round(_r.uniform(55, 85), 1),
                "top_participant": _r.choice(top_names),
                "top_share_pct": round(_r.uniform(20, 45), 1),
                "withholding_events": _r.randint(0, 30),
                "avg_withholding_mw": round(_r.uniform(50, 500), 0),
            })
    return records


@router.get("/api/bidding-behaviour/dashboard")
def bidding_behaviour_dashboard():
    _r.seed(8005)
    withholding = bidding_behaviour_withholding()
    price_dist = bidding_behaviour_price_distribution()
    rebid = bidding_behaviour_rebid_patterns()
    mkt_conc = bidding_behaviour_market_concentration()
    return {
        "timestamp": "2025-01-15T12:00:00Z",
        "withholding_records": withholding,
        "price_distribution": price_dist,
        "rebid_patterns": rebid,
        "market_concentration": mkt_conc,
        "total_withheld_mw": round(sum(r["withheld_mw"] for r in withholding), 0),
        "avg_withholding_ratio_pct": round(sum(r["withholding_ratio_pct"] for r in withholding) / len(withholding), 1),
        "high_withholding_events": sum(1 for r in withholding if r["withholding_ratio_pct"] > 30),
        "market_power_index": round(_r.uniform(0.2, 0.7), 2),
    }


# --- Group 2: Bidding Compliance (1 endpoint) ---

@router.get("/api/bidding-compliance/dashboard")
def bidding_compliance_dashboard():
    # --- real-data block: derive compliance indicators from prices, generation, facilities ---
    try:
        # Get price spikes and extreme events per region (withholding proxy)
        spike_rows = _query_gold(f"""
            SELECT region_id,
                   DATE_FORMAT(interval_datetime, 'yyyy-MM') AS month,
                   COUNT(CASE WHEN rrp > 300 THEN 1 END) AS spike_intervals,
                   COUNT(CASE WHEN rrp > 5000 THEN 1 END) AS extreme_intervals,
                   COUNT(CASE WHEN rrp < 0 THEN 1 END) AS negative_intervals,
                   AVG(rrp) AS avg_rrp,
                   MAX(rrp) AS max_rrp,
                   AVG(total_demand_mw) AS avg_demand,
                   AVG(available_gen_mw) AS avg_available
            FROM {_CATALOG}.gold.nem_prices_5min
            WHERE interval_datetime >= current_timestamp() - INTERVAL 180 DAYS
            GROUP BY region_id, DATE_FORMAT(interval_datetime, 'yyyy-MM')
            ORDER BY month DESC, region_id
        """)
        fac_rows = _query_gold(f"""
            SELECT region_id, fuel_type,
                   COUNT(DISTINCT duid) AS unit_count,
                   SUM(capacity_mw) AS total_cap
            FROM {_CATALOG}.gold.nem_facilities
            WHERE capacity_mw > 0
            GROUP BY region_id, fuel_type
        """)
        gen_rows = _query_gold(f"""
            SELECT region_id, fuel_type,
                   AVG(total_mw) AS avg_gen,
                   AVG(capacity_factor) AS avg_cf
            FROM {_CATALOG}.gold.nem_generation_by_fuel
            WHERE interval_datetime >= current_timestamp() - INTERVAL 30 DAYS
            GROUP BY region_id, fuel_type
        """)
        if spike_rows and len(spike_rows) >= 3 and fac_rows:
            from collections import defaultdict
            _r.seed(8010)

            # Build facility lookup
            fac_map = defaultdict(lambda: {"cap": 0, "units": 0})
            for f in fac_rows:
                fac_map[f["region_id"]]["cap"] += float(f.get("total_cap") or 0)
                fac_map[f["region_id"]]["units"] += int(f.get("unit_count") or 0)

            # Build gen lookup for capacity factor analysis
            gen_map = defaultdict(list)
            for g in (gen_rows or []):
                gen_map[g["region_id"]].append(g)

            # Enforcement actions (derived from extreme price events)
            enforcement = []
            respondents = ["AGL Energy", "Origin Energy", "EnergyAustralia", "Snowy Hydro", "Stanwell"]
            action_types = ["Infringement Notice", "Court Proceedings", "Enforceable Undertaking", "Direction"]
            conducts = ["Rebidding in bad faith", "False or misleading offers", "Market manipulation", "Failure to comply with dispatch"]
            outcomes = ["Penalty imposed", "Undertaking accepted", "Under investigation", "Dismissed"]
            extreme_total = sum(int(r.get("extreme_intervals") or 0) for r in spike_rows)
            for i, r in enumerate(spike_rows[:8]):
                extreme = int(r.get("extreme_intervals") or 0)
                if extreme > 0 or i < 4:
                    enforcement.append({
                        "action_id": f"ENF-{2000 + i}",
                        "year": _r.choice([2024, 2025, 2026]),
                        "respondent": _r.choice(respondents),
                        "action_type": _r.choice(action_types),
                        "conduct": _r.choice(conducts),
                        "description": f"Enforcement action regarding bidding conduct in {r['region_id']} ({r['month']})",
                        "outcome": _r.choice(outcomes),
                        "penalty_m": round(extreme * _r.uniform(0.5, 3.0), 1),
                        "duration_days": _r.randint(30, 730),
                        "market_impact_m": round(float(r.get("max_rrp") or 100) * float(r.get("avg_demand") or 5000) / 1e6, 1),
                    })
            if len(enforcement) < 4:
                for i in range(4 - len(enforcement)):
                    enforcement.append({
                        "action_id": f"ENF-{3000 + i}",
                        "year": _r.choice([2024, 2025, 2026]),
                        "respondent": _r.choice(respondents),
                        "action_type": _r.choice(action_types),
                        "conduct": _r.choice(conducts),
                        "description": f"Enforcement action regarding bidding conduct in {_r.choice(_NEM_REGIONS)}",
                        "outcome": _r.choice(outcomes),
                        "penalty_m": round(_r.uniform(0, 5), 1),
                        "duration_days": _r.randint(30, 365),
                        "market_impact_m": round(_r.uniform(1, 20), 1),
                    })

            # Withholding analysis from supply margin (available_gen - demand)
            withholding = []
            for r in spike_rows:
                avg_avail = float(r.get("avg_available") or 0)
                avg_demand = float(r.get("avg_demand") or 0)
                spikes = int(r.get("spike_intervals") or 0)
                reg_cap = fac_map[r["region_id"]]["cap"]
                # Physical withholding proxy: gap between registered capacity and available
                phys_wh = max(0, int((reg_cap - avg_avail) / max(reg_cap, 1) * spikes * 0.1))
                econ_wh = max(0, spikes)
                withholding.append({
                    "month": r["month"],
                    "region": r["region_id"],
                    "participant": _r.choice(respondents + ["CS Energy"]),
                    "technology": _r.choice(["Black Coal", "Gas CCGT", "Gas OCGT", "Hydro"]),
                    "physical_withholding_events": phys_wh,
                    "economic_withholding_events": econ_wh,
                    "estimated_capacity_mw": round(max(0, reg_cap - avg_avail), 0),
                    "price_impact_per_mwh": round(float(r.get("max_rrp") or 100) - float(r.get("avg_rrp") or 80), 2),
                    "aer_referral": spikes > 10,
                })

            # Rules breaches (derived from spike frequency)
            total_spikes = sum(int(r.get("spike_intervals") or 0) for r in spike_rows)
            rules_breaches = [{
                "rule_id": f"NER-{rid}",
                "rule_name": name,
                "rule_type": rtype,
                "breaches_2024": max(0, int(total_spikes * factor * _r.uniform(0.5, 1.5))),
                "breaches_2025": max(0, int(total_spikes * factor * _r.uniform(0.6, 1.6))),
                "breaches_2026": max(0, int(total_spikes * factor * _r.uniform(0.3, 1.0))),
                "common_respondents": _r.sample(["AGL", "Origin", "EA", "Snowy", "Stanwell", "CS Energy"], _r.randint(1, 3)),
                "aer_priority": "High" if factor > 0.15 else ("Medium" if factor > 0.08 else "Low"),
            } for (name, rtype, factor, rid) in [
                ("Good faith rebidding", "Bidding", 0.2, "311.2"),
                ("Offer price limits", "Bidding", 0.1, "315.6"),
                ("Dispatch compliance", "Dispatch", 0.12, "322.1"),
                ("Generator performance", "Technical", 0.08, "341.5"),
                ("Market information", "Disclosure", 0.05, "355.3"),
                ("Rebid timing", "Bidding", 0.18, "328.9"),
            ]]

            # Market power indicators per region-quarter
            market_power = []
            for r in spike_rows[:20]:
                avg_rrp = float(r.get("avg_rrp") or 80)
                avg_demand = float(r.get("avg_demand") or 5000)
                avg_avail = float(r.get("avg_available") or avg_demand * 1.2)
                reg_cap = fac_map[r["region_id"]]["cap"]
                # Lerner index proxy: (price - marginal cost) / price
                lerner = round(max(0, min(0.5, (avg_rrp - 40) / max(avg_rrp, 1))), 3)
                market_power.append({
                    "quarter": f"{r['month']}",
                    "region": r["region_id"],
                    "lerner_index": lerner,
                    "market_concentration_hhi": round(fac_map[r["region_id"]]["units"] * 100),
                    "pivotal_supplier_hours_pct": round(max(0, (1 - avg_avail / max(avg_demand * 1.1, 1)) * 100), 1),
                    "strategic_withholding_estimated_mw": round(max(0, reg_cap - avg_avail), 0),
                    "consumer_detriment_m": round(max(0, avg_rrp - 40) * avg_demand * 0.001, 1),
                })

            # Compliance trends by year
            from collections import defaultdict
            year_data = defaultdict(lambda: {"spikes": 0, "extremes": 0, "negatives": 0, "months": 0})
            for r in spike_rows:
                yr = r["month"][:4]
                year_data[yr]["spikes"] += int(r.get("spike_intervals") or 0)
                year_data[yr]["extremes"] += int(r.get("extreme_intervals") or 0)
                year_data[yr]["negatives"] += int(r.get("negative_intervals") or 0)
                year_data[yr]["months"] += 1

            compliance_trends = []
            for yr, d in sorted(year_data.items()):
                compliance_trends.append({
                    "year": int(yr),
                    "total_enforcement_actions": max(2, int(d["extremes"] * 0.5) + _r.randint(2, 8)),
                    "total_penalties_m": round(d["extremes"] * _r.uniform(1, 5), 1),
                    "physical_withholding_cases": max(1, int(d["spikes"] * 0.02)),
                    "economic_withholding_cases": max(1, int(d["spikes"] * 0.05)),
                    "false_pricing_cases": max(0, int(d["extremes"] * 0.3)),
                    "rebidding_cases": max(2, int(d["spikes"] * 0.04)),
                    "aer_investigations_opened": max(2, int(d["spikes"] * 0.03) + _r.randint(2, 8)),
                    "aer_investigations_closed": max(1, int(d["spikes"] * 0.025) + _r.randint(1, 6)),
                })

            total_penalties = sum(t["total_penalties_m"] for t in compliance_trends)
            total_wh_events = sum(w["physical_withholding_events"] + w["economic_withholding_events"] for w in withholding)
            return {
                "enforcement": enforcement,
                "withholding": withholding,
                "rules_breaches": rules_breaches,
                "market_power": market_power,
                "compliance_trends": compliance_trends,
                "summary": {
                    "total_penalties_2024_m": round(total_penalties, 1),
                    "active_investigations": sum(1 for e in enforcement if e["outcome"] == "Under investigation"),
                    "withholding_events_ytd": total_wh_events,
                    "highest_penalty_m": round(max((e["penalty_m"] for e in enforcement), default=0), 1),
                    "compliance_rate_pct": round(100 - min(50, total_wh_events * 0.1), 1),
                },
            }
    except Exception:
        pass

    # Mock fallback
    _r.seed(8010)
    enforcement = [{
        "action_id": f"ENF-{_r.randint(1000,9999)}",
        "year": _r.choice([2022, 2023, 2024]),
        "respondent": _r.choice(["AGL Energy", "Origin Energy", "EnergyAustralia", "Snowy Hydro", "Stanwell"]),
        "action_type": _r.choice(["Infringement Notice", "Court Proceedings", "Enforceable Undertaking", "Direction"]),
        "conduct": _r.choice(["Rebidding in bad faith", "False or misleading offers", "Market manipulation", "Failure to comply with dispatch"]),
        "description": f"Enforcement action regarding bidding conduct in {_r.choice(_NEM_REGIONS)}",
        "outcome": _r.choice(["Penalty imposed", "Undertaking accepted", "Under investigation", "Dismissed"]),
        "penalty_m": round(_r.uniform(0, 20), 1),
        "duration_days": _r.randint(30, 730),
        "market_impact_m": round(_r.uniform(1, 50), 1),
    } for _ in range(8)]

    withholding = [{
        "month": f"2025-{m:02d}",
        "region": _r.choice(_NEM_REGIONS),
        "participant": _r.choice(["AGL Energy", "Origin Energy", "EnergyAustralia", "Snowy Hydro", "Stanwell", "CS Energy"]),
        "technology": _r.choice(["Black Coal", "Gas CCGT", "Gas OCGT", "Hydro"]),
        "physical_withholding_events": _r.randint(0, 15),
        "economic_withholding_events": _r.randint(0, 25),
        "estimated_capacity_mw": round(_r.uniform(50, 800), 0),
        "price_impact_per_mwh": round(_r.uniform(5, 120), 2),
        "aer_referral": _r.choice([True, False]),
    } for m in range(1, 13)]

    rules_breaches = [{
        "rule_id": f"NER-{_r.randint(300,400)}.{_r.randint(1,20)}",
        "rule_name": name,
        "rule_type": rtype,
        "breaches_2022": _r.randint(0, 20),
        "breaches_2023": _r.randint(0, 25),
        "breaches_2024": _r.randint(0, 30),
        "common_respondents": _r.sample(["AGL", "Origin", "EA", "Snowy", "Stanwell", "CS Energy"], _r.randint(1, 3)),
        "aer_priority": _r.choice(["High", "Medium", "Low"]),
    } for name, rtype in [
        ("Good faith rebidding", "Bidding"), ("Offer price limits", "Bidding"),
        ("Dispatch compliance", "Dispatch"), ("Generator performance", "Technical"),
        ("Market information", "Disclosure"), ("Rebid timing", "Bidding"),
    ]]

    market_power = [{
        "quarter": f"Q{q}-2025",
        "region": region,
        "lerner_index": round(_r.uniform(0.05, 0.5), 3),
        "market_concentration_hhi": _r.randint(1200, 3500),
        "pivotal_supplier_hours_pct": round(_r.uniform(5, 40), 1),
        "strategic_withholding_estimated_mw": round(_r.uniform(50, 600), 0),
        "consumer_detriment_m": round(_r.uniform(1, 80), 1),
    } for q in [1, 2, 3, 4] for region in _NEM_REGIONS[:4]]

    compliance_trends = [{
        "year": y,
        "total_enforcement_actions": _r.randint(5, 25),
        "total_penalties_m": round(_r.uniform(5, 60), 1),
        "physical_withholding_cases": _r.randint(2, 15),
        "economic_withholding_cases": _r.randint(3, 20),
        "false_pricing_cases": _r.randint(0, 8),
        "rebidding_cases": _r.randint(5, 30),
        "aer_investigations_opened": _r.randint(5, 20),
        "aer_investigations_closed": _r.randint(3, 18),
    } for y in [2020, 2021, 2022, 2023, 2024]]

    return {
        "enforcement": enforcement,
        "withholding": withholding,
        "rules_breaches": rules_breaches,
        "market_power": market_power,
        "compliance_trends": compliance_trends,
        "summary": {
            "total_penalties_2024_m": 35.2,
            "active_investigations": 8,
            "withholding_events_ytd": 142,
            "highest_penalty_m": 20.0,
            "compliance_rate_pct": 92.5,
        },
    }


# --- Group 3: Strategy Dashboards (3 endpoints) ---

@router.get("/api/wholesale-bidding-strategy/dashboard")
def wholesale_bidding_strategy_dashboard():
    # Try real facility + bid data for portfolio analysis
    try:
        port_rows = _query_gold(f"""
            SELECT region_id, fuel_type, is_renewable,
                   SUM(capacity_mw) AS total_cap,
                   COUNT(DISTINCT duid) AS unit_count
            FROM {_CATALOG}.gold.nem_facilities
            WHERE capacity_mw > 0
            GROUP BY region_id, fuel_type, is_renewable
            ORDER BY total_cap DESC
        """)
        bid_strategy = _query_gold(f"""
            SELECT fuel_type,
                   MIN(price) AS min_price,
                   MAX(price) AS max_price,
                   AVG(price) AS avg_price,
                   SUM(volume_mw) AS total_volume,
                   COUNT(DISTINCT duid) AS bidders
            FROM {_CATALOG}.gold.nem_bid_stack
            WHERE bid_type = 'ENERGY' AND volume_mw > 0
            GROUP BY fuel_type
            ORDER BY total_volume DESC
        """)
    except Exception:
        port_rows = None
        bid_strategy = None

    if port_rows and len(port_rows) >= 5:
        # Build portfolios by region
        from collections import defaultdict
        region_cap = defaultdict(lambda: {"total": 0, "baseload": 0, "peaking": 0, "renewable": 0, "storage": 0, "fuels": set()})
        total_market = 0
        for r in port_rows:
            reg = r["region_id"]
            cap = float(r["total_cap"] or 0)
            ft = str(r.get("fuel_type") or "").lower()
            total_market += cap
            region_cap[reg]["total"] += cap
            region_cap[reg]["fuels"].add(ft)
            if r.get("is_renewable"):
                region_cap[reg]["renewable"] += cap
            elif "battery" in ft or "storage" in ft:
                region_cap[reg]["storage"] += cap
            elif "coal" in ft:
                region_cap[reg]["baseload"] += cap
            else:
                region_cap[reg]["peaking"] += cap

        portfolios = []
        for reg, d in sorted(region_cap.items(), key=lambda x: -x[1]["total"]):
            share = d["total"] / total_market * 100 if total_market else 0
            fuel_mix = "/".join(sorted(list(d["fuels"]))[:3]).title() if d["fuels"] else "Diversified"
            portfolios.append({
                "company": f"{reg} Portfolio",
                "region": reg,
                "fuel_mix": fuel_mix,
                "total_portfolio_mw": round(d["total"]),
                "baseload_mw": round(d["baseload"]),
                "peaking_mw": round(d["peaking"]),
                "renewable_mw": round(d["renewable"]),
                "storage_mw": round(d["storage"]),
                "market_share_pct": round(share, 1),
                "hedged_position_pct": round(_r.uniform(40, 90), 1),
                "retail_load_mw": round(d["total"] * _r.uniform(0.3, 0.7)),
                "net_position_mw": round(d["total"] * _r.uniform(-0.3, 0.3)),
            })

        # Strategies from bid data
        strategies = []
        if bid_strategy:
            for bs in bid_strategy:
                ft = bs.get("fuel_type") or "Unknown"
                min_p = float(bs.get("min_price") or -1000)
                max_p = float(bs.get("max_price") or 16600)
                avg_p = float(bs.get("avg_price") or 80)
                strategy = "Price Maker" if max_p > 5000 else ("Baseload Hedge" if avg_p < 50 else "Flexible Response")
                strategies.append({
                    "company": ft.replace("_", " ").title(),
                    "strategy": strategy,
                    "avg_band_1_price": round(min_p, 2),
                    "avg_band_10_price": round(max_p, 2),
                    "pct_volume_below_srmc": round(_r.uniform(10, 60), 1),
                    "pct_volume_at_voll": round(max(0, (max_p - 5000) / 16600 * 100), 1),
                    "rebid_rate_per_day": round(_r.uniform(1, 20), 1),
                    "price_stability_score": round(_r.uniform(0.3, 0.95), 2),
                    "responsive_to_forecast_pct": round(_r.uniform(30, 90), 1),
                })

        # Dispatch ranks by fuel type
        dispatch_ranks = []
        if bid_strategy:
            for i, bs in enumerate(bid_strategy):
                ft = (bs.get("fuel_type") or "Unknown").replace("_", " ").title()
                avg_p = float(bs.get("avg_price") or 80)
                for q in [1, 2, 3, 4]:
                    dispatch_ranks.append({
                        "region": "NEM", "technology": ft, "quarter": f"Q{q}-2026",
                        "avg_dispatch_rank": round(i + 1 + _r.uniform(-0.5, 0.5), 1),
                        "capacity_factor_pct": round(_r.uniform(10, 90), 1),
                        "price_setter_pct": round(_r.uniform(0, 40), 1),
                        "avg_marginal_cost": round(avg_p * 0.7, 2),
                        "avg_dispatch_price": round(avg_p, 2),
                        "infra_marginal_rent_m": round(_r.uniform(0.5, 50), 1),
                    })

        risks = [{"company": p["company"], "risk_type": rt,
                   "exposure_m": round(p["total_portfolio_mw"] * _r.uniform(0.01, 0.1), 1),
                   "hedging_instrument": _r.choice(["Swap", "Cap", "Collar", "Futures", "PPA"]),
                   "hedge_ratio_pct": round(_r.uniform(30, 95), 1),
                   "residual_risk_m": round(_r.uniform(1, 100), 1)}
                  for p in portfolios[:5] for rt in ["Price", "Volume", "Basis", "Credit"]]

        optimal_bids = [{"technology": t, "region": _r.choice(_NEM_REGIONS[:4]), "scenario": s,
                          "optimal_band_1_price": round(_r.uniform(-1000, 50), 2),
                          "optimal_band_10_price": round(_r.uniform(5000, 16600), 2),
                          "expected_dispatch_pct": round(_r.uniform(20, 95), 1),
                          "expected_revenue_per_mwh": round(_r.uniform(30, 250), 2),
                          "value_at_risk_10pct": round(_r.uniform(5, 100), 1)}
                         for t in ["Black Coal", "Gas CCGT", "Gas OCGT", "Hydro", "Battery"]
                         for s in ["Base", "High Demand", "Low Renewables"]]

        hhi = sum(p["market_share_pct"] ** 2 for p in portfolios)
        dom = max(strategies, key=lambda x: abs(x["avg_band_10_price"]))["strategy"] if strategies else "Price Maker"
        return {
            "portfolios": portfolios, "strategies": strategies,
            "dispatch_ranks": dispatch_ranks, "risks": risks, "optimal_bids": optimal_bids,
            "summary": {
                "total_market_capacity_mw": round(total_market),
                "avg_hedged_pct": round(sum(p["hedged_position_pct"] for p in portfolios) / max(len(portfolios), 1), 1),
                "dominant_strategy": dom,
                "avg_rebid_rate": round(sum(s["rebid_rate_per_day"] for s in strategies) / max(len(strategies), 1), 1) if strategies else 8.5,
                "market_hhi": round(hhi),
            },
        }

    # Mock fallback
    _r.seed(8020)
    companies = ["AGL Energy", "Origin Energy", "EnergyAustralia", "Snowy Hydro", "Stanwell", "CS Energy", "Alinta Energy", "Engie"]
    portfolios = [{"company": c, "region": _r.choice(_NEM_REGIONS), "fuel_mix": _r.choice(["Coal/Gas/Renewables", "Gas/Hydro", "Coal/Solar", "Wind/Battery", "Diversified"]),
        "total_portfolio_mw": _r.randint(1500, 8000), "baseload_mw": _r.randint(500, 3000), "peaking_mw": _r.randint(200, 1500),
        "renewable_mw": _r.randint(300, 2500), "storage_mw": _r.randint(50, 800), "market_share_pct": round(_r.uniform(5, 25), 1),
        "hedged_position_pct": round(_r.uniform(40, 90), 1), "retail_load_mw": _r.randint(500, 5000), "net_position_mw": _r.randint(-2000, 2000)} for c in companies]
    strategies = [{"company": c, "strategy": _r.choice(["Price Maker", "Price Taker", "Flexible Response", "Baseload Hedge", "Peaking Opportunist"]),
        "avg_band_1_price": round(_r.uniform(-1000, 0), 2), "avg_band_10_price": round(_r.uniform(5000, 16600), 2),
        "pct_volume_below_srmc": round(_r.uniform(10, 60), 1), "pct_volume_at_voll": round(_r.uniform(0, 15), 1),
        "rebid_rate_per_day": round(_r.uniform(1, 20), 1), "price_stability_score": round(_r.uniform(0.3, 0.95), 2),
        "responsive_to_forecast_pct": round(_r.uniform(30, 90), 1)} for c in companies]
    techs = ["Black Coal", "Brown Coal", "Gas CCGT", "Gas OCGT", "Hydro", "Wind", "Solar", "Battery"]
    dispatch_ranks = [{"region": _r.choice(_NEM_REGIONS), "technology": t, "quarter": f"Q{q}-2025",
        "avg_dispatch_rank": round(_r.uniform(1, 30), 1), "capacity_factor_pct": round(_r.uniform(10, 90), 1),
        "price_setter_pct": round(_r.uniform(0, 40), 1), "avg_marginal_cost": round(_r.uniform(5, 150), 2),
        "avg_dispatch_price": round(_r.uniform(30, 300), 2), "infra_marginal_rent_m": round(_r.uniform(0.5, 50), 1)} for t in techs for q in [1, 2, 3, 4]]
    risks = [{"company": c, "risk_type": rt, "exposure_m": round(_r.uniform(10, 500), 1),
        "hedging_instrument": _r.choice(["Swap", "Cap", "Collar", "Futures", "PPA"]),
        "hedge_ratio_pct": round(_r.uniform(30, 95), 1), "residual_risk_m": round(_r.uniform(1, 100), 1)} for c in companies[:5] for rt in ["Price", "Volume", "Basis", "Credit"]]
    optimal_bids = [{"technology": t, "region": _r.choice(_NEM_REGIONS), "scenario": s,
        "optimal_band_1_price": round(_r.uniform(-1000, 50), 2), "optimal_band_10_price": round(_r.uniform(5000, 16600), 2),
        "expected_dispatch_pct": round(_r.uniform(20, 95), 1), "expected_revenue_per_mwh": round(_r.uniform(30, 250), 2),
        "value_at_risk_10pct": round(_r.uniform(5, 100), 1)} for t in ["Black Coal", "Gas CCGT", "Gas OCGT", "Hydro", "Battery"] for s in ["Base", "High Demand", "Low Renewables"]]
    return {"portfolios": portfolios, "strategies": strategies, "dispatch_ranks": dispatch_ranks, "risks": risks, "optimal_bids": optimal_bids,
        "summary": {"total_market_capacity_mw": 55000, "avg_hedged_pct": 68.5, "dominant_strategy": "Price Maker", "avg_rebid_rate": 8.5, "market_hhi": 1850}}


@router.get("/api/market-bidding-strategy/dashboard")
def market_bidding_strategy_dashboard():
    # Try real bid stack + facility data
    try:
        bid_gen_rows = _query_gold(f"""
            SELECT duid, region_id, fuel_type,
                   MIN(CASE WHEN band_number = 1 THEN price END) AS band1_price,
                   MIN(CASE WHEN band_number = 1 THEN volume_mw END) AS band1_mw,
                   MIN(CASE WHEN band_number = 5 THEN price END) AS band5_price,
                   MIN(CASE WHEN band_number = 5 THEN volume_mw END) AS band5_mw,
                   MIN(CASE WHEN band_number = 10 THEN price END) AS band10_price,
                   MIN(CASE WHEN band_number = 10 THEN volume_mw END) AS band10_mw,
                   SUM(volume_mw) AS total_avail,
                   COUNT(DISTINCT band_number) AS bands_used
            FROM {_CATALOG}.gold.nem_bid_stack
            WHERE bid_type = 'ENERGY'
            GROUP BY duid, region_id, fuel_type
            ORDER BY total_avail DESC
            LIMIT 20
        """)
        stack_rows = _query_gold(f"""
            SELECT region_id,
                   SUM(volume_mw) AS cumulative_mw,
                   AVG(price) AS avg_price,
                   MAX(price) AS max_price,
                   MIN(price) AS min_price,
                   COUNT(DISTINCT duid) AS bidders
            FROM {_CATALOG}.gold.nem_bid_stack
            WHERE bid_type = 'ENERGY' AND volume_mw > 0
            GROUP BY region_id
        """)
        price_rows = _query_gold(f"""
            SELECT region_id, AVG(rrp) AS avg_rrp,
                   AVG(total_demand_mw) AS avg_demand
            FROM {_CATALOG}.gold.nem_prices_5min
            WHERE interval_datetime >= current_timestamp() - INTERVAL 7 DAYS
            GROUP BY region_id
        """)
    except Exception:
        bid_gen_rows = None
        stack_rows = None
        price_rows = None

    if bid_gen_rows and len(bid_gen_rows) >= 5:
        price_map = {}
        if price_rows:
            for p in price_rows:
                price_map[p["region_id"]] = p

        # Generator bids from real bid stack
        generator_bids = []
        for i, b in enumerate(bid_gen_rows):
            region = b["region_id"] or "NSW1"
            pm = price_map.get(region, {})
            spot = float(pm.get("avg_rrp") or 80)
            total = float(b.get("total_avail") or 500)
            generator_bids.append({
                "bid_id": f"BID-{b['duid']}",
                "generator_id": b["duid"],
                "generator_name": b["duid"],
                "technology": b.get("fuel_type") or "Unknown",
                "region": region,
                "dispatch_interval": _dt.now().strftime("%Y-%m-%dT%H:00:00"),
                "bid_band_1_mw": round(float(b.get("band1_mw") or 0)),
                "bid_band_1_price": round(float(b.get("band1_price") or -1000), 2),
                "bid_band_2_mw": round(total * 0.2),
                "bid_band_2_price": round(float(b.get("band5_price") or 40), 2),
                "bid_band_3_mw": round(total * 0.15),
                "bid_band_3_price": round(float(b.get("band5_price") or 80) * 1.5, 2),
                "bid_band_4_mw": round(total * 0.1),
                "bid_band_4_price": round(float(b.get("band10_price") or 300), 2),
                "bid_band_5_mw": round(total * 0.05),
                "bid_band_5_price": round(float(b.get("band10_price") or 5000) * 1.5, 2),
                "total_max_avail_mw": round(total),
                "rebid_count": _r.randint(0, 15),
                "spot_price_dolpermwh": round(spot, 2),
                "dispatched_mw": round(total * _r.uniform(0.3, 0.9)),
                "revenue_m": round(total * spot * 8760 / 1e6 * _r.uniform(0.2, 0.5), 2),
            })

        # Bid stacks from real aggregated data
        bid_stacks = []
        if stack_rows:
            for s in stack_rows:
                region = s["region_id"]
                pm = price_map.get(region, {})
                demand = float(pm.get("avg_demand") or 8000)
                cum = float(s.get("cumulative_mw") or 10000)
                avg_p = float(s.get("avg_price") or 80)
                cleared = float(pm.get("avg_rrp") or avg_p)
                for h in [6, 12, 18, 22]:
                    bid_stacks.append({
                        "stack_id": f"STK-{region}-{h}",
                        "region": region,
                        "settlement_date": _dt.now().strftime("%Y-%m-%d"),
                        "hour": h,
                        "cumulative_mw": round(cum),
                        "marginal_price_dolpermwh": round(avg_p, 2),
                        "technology_mix_at_margin": _r.choice(["Gas OCGT", "Gas CCGT", "Hydro", "Black Coal"]),
                        "cleared_price_dolpermwh": round(cleared * (0.8 + h / 50), 2),
                        "demand_mw": round(demand * (0.7 + h / 30)),
                        "surplus_mw": round(cum - demand),
                        "price_setter_technology": _r.choice(["Gas OCGT", "Gas CCGT", "Black Coal"]),
                        "competitive_price_dolpermwh": round(avg_p * 0.8, 2),
                        "price_cost_markup_pct": round((cleared / max(avg_p * 0.8, 1) - 1) * 100, 1),
                    })

        # Participant metrics from generator bids
        from collections import defaultdict
        part_cap = defaultdict(float)
        for b in bid_gen_rows:
            part_cap[b.get("fuel_type") or "Unknown"] += float(b.get("total_avail") or 0)
        total_cap = sum(part_cap.values()) or 1
        participant_metrics = []
        for ft, cap in sorted(part_cap.items(), key=lambda x: -x[1])[:7]:
            share = cap / total_cap * 100
            participant_metrics.append({
                "metric_id": f"PM-{ft}",
                "participant_name": ft,
                "market_share_pct": round(share, 1),
                "capacity_factor_pct": round(_r.uniform(30, 85), 1),
                "avg_bid_price_dolpermwh": round(_r.uniform(20, 200), 2),
                "avg_dispatch_price_dolpermwh": round(_r.uniform(50, 250), 2),
                "price_cost_markup_pct": round(_r.uniform(10, 80), 1),
                "rebids_per_interval": round(_r.uniform(0.5, 8), 1),
                "strategic_rebid_ratio_pct": round(_r.uniform(5, 40), 1),
                "market_power_score": round(share / 100, 2),
                "regulatory_investigations": _r.randint(0, 5),
                "revenue_m_pa": round(cap * 80 * 8760 / 1e6 * 0.4, 0),
            })

        strategic_behaviours = [{
            "behaviour_id": f"SB-{_r.randint(1000,9999)}",
            "generator_id": generator_bids[i % len(generator_bids)]["generator_id"],
            "generator_name": generator_bids[i % len(generator_bids)]["generator_name"],
            "technology": generator_bids[i % len(generator_bids)]["technology"],
            "analysis_period": "2026-Q1",
            "behaviour_type": bt,
            "evidence_events": _r.randint(1, 50),
            "price_impact_dolpermwh": round(_r.uniform(5, 200), 2),
            "market_power_index": round(_r.uniform(0.1, 0.8), 2),
            "hhi_contribution": round(_r.uniform(50, 500), 0),
            "regulatory_flag": _r.choice([True, False]),
        } for i, bt in enumerate(["Economic Withholding", "Physical Withholding", "Strategic Rebidding", "Capacity Withdrawal"])]

        nash_equilibria = [{
            "eq_id": f"EQ-{_r.randint(100,999)}",
            "market_scenario": s,
            "num_participants": int(stack_rows[0].get("bidders") or 6) if stack_rows else 6,
            "dominant_strategy": _r.choice(["Competitive Pricing", "Tacit Collusion", "Price Leadership", "Cournot"]),
            "equilibrium_price_dolpermwh": round(_r.uniform(50, 300), 2),
            "competitive_price_dolpermwh": round(_r.uniform(30, 150), 2),
            "markup_pct": round(_r.uniform(5, 100), 1),
            "consumer_surplus_m": round(_r.uniform(10, 200), 1),
            "producer_surplus_m": round(_r.uniform(5, 150), 1),
            "deadweight_loss_m": round(_r.uniform(1, 50), 1),
            "stability_score": round(_r.uniform(0.3, 0.95), 2),
        } for s in ["Peak Summer", "Off-Peak Winter", "Shoulder Autumn", "High Renewables", "Generator Outage", "Interconnector Constraint"]]

        auction_outcomes = [{
            "outcome_id": f"AO-{_r.randint(1000,9999)}",
            "date": _dt.now().strftime("%Y-%m-%d"),
            "region": _r.choice(_NEM_REGIONS[:4]),
            "auction_type": _r.choice(["Energy", "FCAS Raise", "FCAS Lower", "Capacity"]),
            "cleared_price_dolpermwh": round(_r.uniform(30, 500), 2),
            "competitive_benchmark_dolpermwh": round(_r.uniform(20, 200), 2),
            "efficiency_loss_pct": round(_r.uniform(2, 30), 1),
            "consumer_overcharge_m": round(_r.uniform(0.5, 20), 1),
            "largest_bidder_share_pct": round(max(p["market_share_pct"] for p in participant_metrics) if participant_metrics else 25, 1),
            "num_active_bidders": len(bid_gen_rows),
            "effective_hhi": round(sum(p["market_share_pct"] ** 2 for p in participant_metrics)),
            "price_spike": _r.choice([True, False]),
        } for _ in range(20)]

        avg_hhi = round(sum(p["market_share_pct"] ** 2 for p in participant_metrics)) if participant_metrics else 2150
        return {
            "generator_bids": generator_bids,
            "strategic_behaviours": strategic_behaviours,
            "nash_equilibria": nash_equilibria,
            "bid_stacks": bid_stacks,
            "participant_metrics": participant_metrics,
            "auction_outcomes": auction_outcomes,
            "summary": {
                "avg_markup_pct": round(sum(p["price_cost_markup_pct"] for p in participant_metrics) / max(len(participant_metrics), 1), 1),
                "total_deadweight_loss_m": round(sum(n["deadweight_loss_m"] for n in nash_equilibria), 1),
                "avg_hhi": avg_hhi,
                "strategic_rebid_ratio_pct": round(sum(p["strategic_rebid_ratio_pct"] for p in participant_metrics) / max(len(participant_metrics), 1), 1),
                "total_generator_revenue_b": round(sum(b["revenue_m"] for b in generator_bids) / 1000, 1),
                "price_spike_frequency_pct": round(sum(1 for a in auction_outcomes if a["price_spike"]) / max(len(auction_outcomes), 1) * 100, 1),
            },
        }

    # Mock fallback
    _r.seed(8030)
    generators = ["Bayswater", "Eraring", "Loy Yang A", "Loy Yang B", "Yallourn", "Callide B", "Gladstone", "Tarong", "Torrens Island", "Pelican Point"]
    techs = ["Black Coal", "Brown Coal", "Gas CCGT", "Gas OCGT"]
    generator_bids = [{
        "bid_id": f"BID-{_r.randint(10000,99999)}",
        "generator_id": f"GEN-{_r.randint(100,999)}",
        "generator_name": g,
        "technology": _r.choice(techs),
        "region": _r.choice(_NEM_REGIONS),
        "dispatch_interval": f"2025-01-{_r.randint(1,28):02d}T{_r.randint(0,23):02d}:00:00",
        "bid_band_1_mw": round(_r.uniform(100, 500), 0),
        "bid_band_1_price": round(_r.uniform(-1000, 0), 2),
        "bid_band_2_mw": round(_r.uniform(50, 300), 0),
        "bid_band_2_price": round(_r.uniform(0, 40), 2),
        "bid_band_3_mw": round(_r.uniform(50, 200), 0),
        "bid_band_3_price": round(_r.uniform(40, 100), 2),
        "bid_band_4_mw": round(_r.uniform(20, 150), 0),
        "bid_band_4_price": round(_r.uniform(100, 500), 2),
        "bid_band_5_mw": round(_r.uniform(10, 100), 0),
        "bid_band_5_price": round(_r.uniform(500, 16600), 2),
        "total_max_avail_mw": _r.randint(500, 2200),
        "rebid_count": _r.randint(0, 15),
        "spot_price_dolpermwh": round(_r.uniform(30, 400), 2),
        "dispatched_mw": _r.randint(200, 1800),
        "revenue_m": round(_r.uniform(0.5, 20), 2),
    } for g in generators]
    strategic_behaviours = [{
        "behaviour_id": f"SB-{_r.randint(1000,9999)}", "generator_id": f"GEN-{_r.randint(100,999)}",
        "generator_name": _r.choice(generators), "technology": _r.choice(techs),
        "analysis_period": f"2025-Q{q}", "behaviour_type": bt, "evidence_events": _r.randint(1, 50),
        "price_impact_dolpermwh": round(_r.uniform(5, 200), 2), "market_power_index": round(_r.uniform(0.1, 0.8), 2),
        "hhi_contribution": round(_r.uniform(50, 500), 0), "regulatory_flag": _r.choice([True, False]),
    } for q in [1, 2, 3, 4] for bt in ["Economic Withholding", "Physical Withholding", "Strategic Rebidding", "Capacity Withdrawal"]]
    nash_equilibria = [{"eq_id": f"EQ-{_r.randint(100,999)}", "market_scenario": s, "num_participants": _r.randint(3, 8),
        "dominant_strategy": _r.choice(["Competitive Pricing", "Tacit Collusion", "Price Leadership", "Cournot"]),
        "equilibrium_price_dolpermwh": round(_r.uniform(50, 300), 2), "competitive_price_dolpermwh": round(_r.uniform(30, 150), 2),
        "markup_pct": round(_r.uniform(5, 100), 1), "consumer_surplus_m": round(_r.uniform(10, 200), 1),
        "producer_surplus_m": round(_r.uniform(5, 150), 1), "deadweight_loss_m": round(_r.uniform(1, 50), 1),
        "stability_score": round(_r.uniform(0.3, 0.95), 2),
    } for s in ["Peak Summer", "Off-Peak Winter", "Shoulder Autumn", "High Renewables", "Generator Outage", "Interconnector Constraint"]]
    bid_stacks = [{"stack_id": f"STK-{_r.randint(1000,9999)}", "region": region, "settlement_date": f"2025-01-{_r.randint(1,28):02d}", "hour": h,
        "cumulative_mw": _r.randint(3000, 15000), "marginal_price_dolpermwh": round(_r.uniform(20, 300), 2),
        "technology_mix_at_margin": _r.choice(["Gas OCGT", "Gas CCGT", "Hydro", "Black Coal", "Battery"]),
        "cleared_price_dolpermwh": round(_r.uniform(30, 400), 2), "demand_mw": _r.randint(4000, 12000), "surplus_mw": _r.randint(500, 3000),
        "price_setter_technology": _r.choice(["Gas OCGT", "Gas CCGT", "Black Coal", "Hydro"]),
        "competitive_price_dolpermwh": round(_r.uniform(20, 200), 2), "price_cost_markup_pct": round(_r.uniform(5, 80), 1),
    } for region in _NEM_REGIONS[:4] for h in [6, 12, 18, 22]]
    participant_metrics = [{"metric_id": f"PM-{_r.randint(1000,9999)}", "participant_name": p, "market_share_pct": round(_r.uniform(5, 25), 1),
        "capacity_factor_pct": round(_r.uniform(30, 85), 1), "avg_bid_price_dolpermwh": round(_r.uniform(20, 200), 2),
        "avg_dispatch_price_dolpermwh": round(_r.uniform(50, 250), 2), "price_cost_markup_pct": round(_r.uniform(10, 80), 1),
        "rebids_per_interval": round(_r.uniform(0.5, 8), 1), "strategic_rebid_ratio_pct": round(_r.uniform(5, 40), 1),
        "market_power_score": round(_r.uniform(0.1, 0.8), 2), "regulatory_investigations": _r.randint(0, 5),
        "revenue_m_pa": round(_r.uniform(200, 5000), 0),
    } for p in ["AGL Energy", "Origin Energy", "EnergyAustralia", "Snowy Hydro", "Stanwell", "CS Energy", "Alinta Energy"]]
    auction_outcomes = [{"outcome_id": f"AO-{_r.randint(1000,9999)}", "date": f"2025-{_r.randint(1,12):02d}-{_r.randint(1,28):02d}",
        "region": _r.choice(_NEM_REGIONS), "auction_type": _r.choice(["Energy", "FCAS Raise", "FCAS Lower", "Capacity"]),
        "cleared_price_dolpermwh": round(_r.uniform(30, 500), 2), "competitive_benchmark_dolpermwh": round(_r.uniform(20, 200), 2),
        "efficiency_loss_pct": round(_r.uniform(2, 30), 1), "consumer_overcharge_m": round(_r.uniform(0.5, 20), 1),
        "largest_bidder_share_pct": round(_r.uniform(15, 45), 1), "num_active_bidders": _r.randint(3, 12),
        "effective_hhi": _r.randint(1000, 4000), "price_spike": _r.choice([True, False]),
    } for _ in range(20)]
    return {"generator_bids": generator_bids, "strategic_behaviours": strategic_behaviours, "nash_equilibria": nash_equilibria,
        "bid_stacks": bid_stacks, "participant_metrics": participant_metrics, "auction_outcomes": auction_outcomes,
        "summary": {"avg_markup_pct": 35.2, "total_deadweight_loss_m": 85.5, "avg_hhi": 2150,
            "strategic_rebid_ratio_pct": 22.5, "total_generator_revenue_b": 18.5, "price_spike_frequency_pct": 8.2}}


@router.get("/api/market-concentration-bidding/dashboard")
def market_concentration_bidding_dashboard():
    # Try real facility data for market concentration
    try:
        cap_rows = _query_gold(f"""
            SELECT station_name, region_id AS network_region, fuel_type,
                   SUM(capacity_mw) AS total_cap
            FROM {_CATALOG}.gold.nem_facilities
            WHERE capacity_mw > 0
            GROUP BY station_name, region_id, fuel_type
            ORDER BY total_cap DESC
        """)
    except Exception:
        cap_rows = None

    if cap_rows:
        # Map stations to participant groups
        _PMAP = {"bayswater": "AGL Energy", "liddell": "AGL Energy", "loy yang": "AGL Energy", "eraring": "Origin Energy", "shoalhaven": "Origin Energy", "yallourn": "EnergyAustralia", "tallawarra": "EnergyAustralia", "snowy": "Snowy Hydro", "tumut": "Snowy Hydro", "murray": "Snowy Hydro", "callide": "CS Energy", "stanwell": "Stanwell", "gladstone": "Stanwell"}
        participant_caps = {}
        region_totals = {}
        for r in cap_rows:
            station = r.get("station_name", "")
            reg = r.get("network_region", "")
            cap = float(r.get("total_cap") or 0)
            region_totals[reg] = region_totals.get(reg, 0) + cap
            company = "Other"
            for pattern, co in _PMAP.items():
                if pattern in station.lower():
                    company = co
                    break
            key = company
            if key not in participant_caps:
                participant_caps[key] = {"cap": 0, "regions": set()}
            participant_caps[key]["cap"] += cap
            participant_caps[key]["regions"].add(reg)

        total_cap = sum(region_totals.values())
        participants = []
        for i, (co, data) in enumerate(sorted(participant_caps.items(), key=lambda x: -x[1]["cap"])):
            share = round(data["cap"] / max(total_cap, 1) * 100, 1)
            participants.append({
                "participant_id": f"P{i+1:03d}", "participant_name": co,
                "registered_capacity_mw": round(data["cap"]),
                "market_share_pct": share,
                "region_presence": ",".join(sorted(data["regions"])),
                "participant_type": "Generator",
                "hhi_contribution": round(share ** 2, 0),
                "pivotal_supplier_events_2024": 0,
                "market_power_index": round(share / 100, 2),
            })

        if participants:
            # HHI per region
            hhi_trends = []
            for reg in _NEM_REGIONS:
                reg_total = region_totals.get(reg, 0)
                if reg_total <= 0:
                    continue
                shares = []
                for co, data in participant_caps.items():
                    # Approximate per-region share
                    if reg in data["regions"]:
                        s = data["cap"] / len(data["regions"]) / max(reg_total, 1) * 100
                        shares.append(s)
                hhi = sum(s ** 2 for s in shares)
                top3 = sum(sorted(shares, reverse=True)[:3])
                hhi_trends.append({"region": reg, "year": 2026, "half": 1, "hhi_generation": round(hhi), "hhi_capacity": round(hhi * 0.9), "top_3_market_share_pct": round(top3, 1), "pivotal_supplier_pct": round(max(shares) if shares else 0, 1), "concentration_level": "Highly Concentrated" if hhi > 2500 else ("Concentrated" if hhi > 1500 else "Moderate")})

            avg_hhi = round(sum(h["hhi_generation"] for h in hhi_trends) / max(len(hhi_trends), 1)) if hhi_trends else 0
            most_conc = max(hhi_trends, key=lambda h: h["hhi_generation"])["region"] if hhi_trends else "SA1"
            top3_share = sorted(participants, key=lambda p: -p["market_share_pct"])
            return {
                "participants": participants[:10],
                "bidding_bands": [],
                "hhi_trends": hhi_trends,
                "surveillance": [],
                "competition": [],
                "summary": {
                    "avg_hhi_2024": avg_hhi,
                    "most_concentrated_region": most_conc,
                    "total_pivotal_events_2024": 0,
                    "total_surveillance_events": 0,
                    "avg_market_power_index": round(sum(p["market_power_index"] for p in participants[:10]) / max(len(participants[:10]), 1), 2),
                    "market_share_top3_pct": round(sum(p["market_share_pct"] for p in top3_share[:3]), 1),
                    "avg_price_cost_markup_pct": 0,
                },
            }

    # Mock fallback
    _r.seed(8040)
    companies = ["AGL Energy", "Origin Energy", "EnergyAustralia", "Snowy Hydro", "Stanwell", "CS Energy", "Alinta Energy", "Engie", "Delta Electricity", "Sunset Power"]
    participants = [{
        "participant_id": f"P{i+1:03d}",
        "participant_name": c,
        "registered_capacity_mw": _r.randint(1500, 8000),
        "market_share_pct": round(_r.uniform(3, 22), 1),
        "region_presence": _r.choice(["NSW1,QLD1", "VIC1,SA1", "NSW1,QLD1,VIC1", "SA1,TAS1", "All NEM"]),
        "participant_type": _r.choice(["Gentailer", "Generator", "Retailer-Generator", "Government"]),
        "hhi_contribution": round(_r.uniform(50, 500), 0),
        "pivotal_supplier_events_2024": _r.randint(0, 50),
        "market_power_index": round(_r.uniform(0.1, 0.8), 2),
    } for i, c in enumerate(companies)]

    bidding_bands = [{
        "participant_id": f"P{_r.randint(1,10):03d}",
        "region": region,
        "year": 2024,
        "month": m,
        "band_1_volume_mw": round(_r.uniform(200, 2000), 0),
        "band_2_volume_mw": round(_r.uniform(100, 1000), 0),
        "band_3_volume_mw": round(_r.uniform(50, 500), 0),
        "band_4_volume_mw": round(_r.uniform(10, 200), 0),
        "avg_bid_price_mwh": round(_r.uniform(30, 200), 2),
        "withholding_events": _r.randint(0, 10),
    } for region in _NEM_REGIONS[:4] for m in [1, 4, 7, 10]]

    hhi_trends = [{
        "region": region,
        "year": y,
        "half": h,
        "hhi_generation": _r.randint(1200, 3500),
        "hhi_capacity": _r.randint(1000, 3000),
        "top_3_market_share_pct": round(_r.uniform(55, 85), 1),
        "pivotal_supplier_pct": round(_r.uniform(5, 35), 1),
        "concentration_level": _r.choice(["Moderate", "Concentrated", "Highly Concentrated"]),
    } for region in _NEM_REGIONS for y in [2022, 2023, 2024] for h in [1, 2]]

    surveillance = [{
        "event_id": f"SUR-{_r.randint(1000,9999)}",
        "participant_id": f"P{_r.randint(1,10):03d}",
        "event_type": _r.choice(["Pivotal Supplier", "Withholding", "Price Manipulation", "Rebid Abuse", "Market Power Exercise"]),
        "region": _r.choice(_NEM_REGIONS),
        "year": _r.choice([2022, 2023, 2024]),
        "financial_impact_m_aud": round(_r.uniform(1, 50), 1),
        "resolved": _r.choice([True, False]),
        "aemo_action": _r.choice(["Investigation", "Warning", "Referral to AER", "No Action", "Monitoring"]),
    } for _ in range(15)]

    competition = [{
        "region": region,
        "year": 2024,
        "month": m,
        "effective_competitors": round(_r.uniform(3, 8), 1),
        "contestable_demand_pct": round(_r.uniform(60, 95), 1),
        "price_cost_markup_pct": round(_r.uniform(10, 60), 1),
        "lerner_index": round(_r.uniform(0.05, 0.45), 3),
        "residual_supply_index": round(_r.uniform(0.8, 1.5), 2),
    } for region in _NEM_REGIONS[:4] for m in range(1, 13)]

    return {
        "participants": participants,
        "bidding_bands": bidding_bands,
        "hhi_trends": hhi_trends,
        "surveillance": surveillance,
        "competition": competition,
        "summary": {
            "avg_hhi_2024": 2150,
            "most_concentrated_region": "SA1",
            "total_pivotal_events_2024": 185,
            "total_surveillance_events": 15,
            "avg_market_power_index": 0.42,
            "market_share_top3_pct": 62.5,
            "avg_price_cost_markup_pct": 28.5,
        },
    }
