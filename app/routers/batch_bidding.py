from __future__ import annotations
import random as _r
from datetime import datetime as _dt
from fastapi import APIRouter, Query
from .shared import _NEM_REGIONS

router = APIRouter()

# =========================================================================
# BATCH: Bidding Analytics endpoints (9)
# =========================================================================

# --- Group 1: Bidding Behaviour (5 endpoints) ---

@router.get("/api/bidding-behaviour/withholding")
def bidding_behaviour_withholding():
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
    _r.seed(8020)
    companies = ["AGL Energy", "Origin Energy", "EnergyAustralia", "Snowy Hydro", "Stanwell", "CS Energy", "Alinta Energy", "Engie"]
    portfolios = [{
        "company": c,
        "region": _r.choice(_NEM_REGIONS),
        "fuel_mix": _r.choice(["Coal/Gas/Renewables", "Gas/Hydro", "Coal/Solar", "Wind/Battery", "Diversified"]),
        "total_portfolio_mw": _r.randint(1500, 8000),
        "baseload_mw": _r.randint(500, 3000),
        "peaking_mw": _r.randint(200, 1500),
        "renewable_mw": _r.randint(300, 2500),
        "storage_mw": _r.randint(50, 800),
        "market_share_pct": round(_r.uniform(5, 25), 1),
        "hedged_position_pct": round(_r.uniform(40, 90), 1),
        "retail_load_mw": _r.randint(500, 5000),
        "net_position_mw": _r.randint(-2000, 2000),
    } for c in companies]

    strategies = [{
        "company": c,
        "strategy": _r.choice(["Price Maker", "Price Taker", "Flexible Response", "Baseload Hedge", "Peaking Opportunist"]),
        "avg_band_1_price": round(_r.uniform(-1000, 0), 2),
        "avg_band_10_price": round(_r.uniform(5000, 16600), 2),
        "pct_volume_below_srmc": round(_r.uniform(10, 60), 1),
        "pct_volume_at_voll": round(_r.uniform(0, 15), 1),
        "rebid_rate_per_day": round(_r.uniform(1, 20), 1),
        "price_stability_score": round(_r.uniform(0.3, 0.95), 2),
        "responsive_to_forecast_pct": round(_r.uniform(30, 90), 1),
    } for c in companies]

    techs = ["Black Coal", "Brown Coal", "Gas CCGT", "Gas OCGT", "Hydro", "Wind", "Solar", "Battery"]
    dispatch_ranks = [{
        "region": _r.choice(_NEM_REGIONS),
        "technology": t,
        "quarter": f"Q{q}-2025",
        "avg_dispatch_rank": round(_r.uniform(1, 30), 1),
        "capacity_factor_pct": round(_r.uniform(10, 90), 1),
        "price_setter_pct": round(_r.uniform(0, 40), 1),
        "avg_marginal_cost": round(_r.uniform(5, 150), 2),
        "avg_dispatch_price": round(_r.uniform(30, 300), 2),
        "infra_marginal_rent_m": round(_r.uniform(0.5, 50), 1),
    } for t in techs for q in [1, 2, 3, 4]]

    risks = [{
        "company": c,
        "risk_type": rt,
        "exposure_m": round(_r.uniform(10, 500), 1),
        "hedging_instrument": _r.choice(["Swap", "Cap", "Collar", "Futures", "PPA"]),
        "hedge_ratio_pct": round(_r.uniform(30, 95), 1),
        "residual_risk_m": round(_r.uniform(1, 100), 1),
    } for c in companies[:5] for rt in ["Price", "Volume", "Basis", "Credit"]]

    optimal_bids = [{
        "technology": t,
        "region": _r.choice(_NEM_REGIONS),
        "scenario": s,
        "optimal_band_1_price": round(_r.uniform(-1000, 50), 2),
        "optimal_band_10_price": round(_r.uniform(5000, 16600), 2),
        "expected_dispatch_pct": round(_r.uniform(20, 95), 1),
        "expected_revenue_per_mwh": round(_r.uniform(30, 250), 2),
        "value_at_risk_10pct": round(_r.uniform(5, 100), 1),
    } for t in ["Black Coal", "Gas CCGT", "Gas OCGT", "Hydro", "Battery"] for s in ["Base", "High Demand", "Low Renewables"]]

    return {
        "portfolios": portfolios,
        "strategies": strategies,
        "dispatch_ranks": dispatch_ranks,
        "risks": risks,
        "optimal_bids": optimal_bids,
        "summary": {
            "total_market_capacity_mw": 55000,
            "avg_hedged_pct": 68.5,
            "dominant_strategy": "Price Maker",
            "avg_rebid_rate": 8.5,
            "market_hhi": 1850,
        },
    }


@router.get("/api/market-bidding-strategy/dashboard")
def market_bidding_strategy_dashboard():
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
        "behaviour_id": f"SB-{_r.randint(1000,9999)}",
        "generator_id": f"GEN-{_r.randint(100,999)}",
        "generator_name": _r.choice(generators),
        "technology": _r.choice(techs),
        "analysis_period": f"2025-Q{q}",
        "behaviour_type": bt,
        "evidence_events": _r.randint(1, 50),
        "price_impact_dolpermwh": round(_r.uniform(5, 200), 2),
        "market_power_index": round(_r.uniform(0.1, 0.8), 2),
        "hhi_contribution": round(_r.uniform(50, 500), 0),
        "regulatory_flag": _r.choice([True, False]),
    } for q in [1, 2, 3, 4] for bt in ["Economic Withholding", "Physical Withholding", "Strategic Rebidding", "Capacity Withdrawal"]]

    nash_equilibria = [{
        "eq_id": f"EQ-{_r.randint(100,999)}",
        "market_scenario": s,
        "num_participants": _r.randint(3, 8),
        "dominant_strategy": _r.choice(["Competitive Pricing", "Tacit Collusion", "Price Leadership", "Cournot"]),
        "equilibrium_price_dolpermwh": round(_r.uniform(50, 300), 2),
        "competitive_price_dolpermwh": round(_r.uniform(30, 150), 2),
        "markup_pct": round(_r.uniform(5, 100), 1),
        "consumer_surplus_m": round(_r.uniform(10, 200), 1),
        "producer_surplus_m": round(_r.uniform(5, 150), 1),
        "deadweight_loss_m": round(_r.uniform(1, 50), 1),
        "stability_score": round(_r.uniform(0.3, 0.95), 2),
    } for s in ["Peak Summer", "Off-Peak Winter", "Shoulder Autumn", "High Renewables", "Generator Outage", "Interconnector Constraint"]]

    bid_stacks = [{
        "stack_id": f"STK-{_r.randint(1000,9999)}",
        "region": region,
        "settlement_date": f"2025-01-{_r.randint(1,28):02d}",
        "hour": h,
        "cumulative_mw": _r.randint(3000, 15000),
        "marginal_price_dolpermwh": round(_r.uniform(20, 300), 2),
        "technology_mix_at_margin": _r.choice(["Gas OCGT", "Gas CCGT", "Hydro", "Black Coal", "Battery"]),
        "cleared_price_dolpermwh": round(_r.uniform(30, 400), 2),
        "demand_mw": _r.randint(4000, 12000),
        "surplus_mw": _r.randint(500, 3000),
        "price_setter_technology": _r.choice(["Gas OCGT", "Gas CCGT", "Black Coal", "Hydro"]),
        "competitive_price_dolpermwh": round(_r.uniform(20, 200), 2),
        "price_cost_markup_pct": round(_r.uniform(5, 80), 1),
    } for region in _NEM_REGIONS[:4] for h in [6, 12, 18, 22]]

    participant_metrics = [{
        "metric_id": f"PM-{_r.randint(1000,9999)}",
        "participant_name": p,
        "market_share_pct": round(_r.uniform(5, 25), 1),
        "capacity_factor_pct": round(_r.uniform(30, 85), 1),
        "avg_bid_price_dolpermwh": round(_r.uniform(20, 200), 2),
        "avg_dispatch_price_dolpermwh": round(_r.uniform(50, 250), 2),
        "price_cost_markup_pct": round(_r.uniform(10, 80), 1),
        "rebids_per_interval": round(_r.uniform(0.5, 8), 1),
        "strategic_rebid_ratio_pct": round(_r.uniform(5, 40), 1),
        "market_power_score": round(_r.uniform(0.1, 0.8), 2),
        "regulatory_investigations": _r.randint(0, 5),
        "revenue_m_pa": round(_r.uniform(200, 5000), 0),
    } for p in ["AGL Energy", "Origin Energy", "EnergyAustralia", "Snowy Hydro", "Stanwell", "CS Energy", "Alinta Energy"]]

    auction_outcomes = [{
        "outcome_id": f"AO-{_r.randint(1000,9999)}",
        "date": f"2025-{_r.randint(1,12):02d}-{_r.randint(1,28):02d}",
        "region": _r.choice(_NEM_REGIONS),
        "auction_type": _r.choice(["Energy", "FCAS Raise", "FCAS Lower", "Capacity"]),
        "cleared_price_dolpermwh": round(_r.uniform(30, 500), 2),
        "competitive_benchmark_dolpermwh": round(_r.uniform(20, 200), 2),
        "efficiency_loss_pct": round(_r.uniform(2, 30), 1),
        "consumer_overcharge_m": round(_r.uniform(0.5, 20), 1),
        "largest_bidder_share_pct": round(_r.uniform(15, 45), 1),
        "num_active_bidders": _r.randint(3, 12),
        "effective_hhi": _r.randint(1000, 4000),
        "price_spike": _r.choice([True, False]),
    } for _ in range(20)]

    return {
        "generator_bids": generator_bids,
        "strategic_behaviours": strategic_behaviours,
        "nash_equilibria": nash_equilibria,
        "bid_stacks": bid_stacks,
        "participant_metrics": participant_metrics,
        "auction_outcomes": auction_outcomes,
        "summary": {
            "avg_markup_pct": 35.2,
            "total_deadweight_loss_m": 85.5,
            "avg_hhi": 2150,
            "strategic_rebid_ratio_pct": 22.5,
            "total_generator_revenue_b": 18.5,
            "price_spike_frequency_pct": 8.2,
        },
    }


@router.get("/api/market-concentration-bidding/dashboard")
def market_concentration_bidding_dashboard():
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
