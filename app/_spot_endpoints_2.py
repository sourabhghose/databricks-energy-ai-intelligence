# =============================================================================
# _spot_endpoints_2.py  --  6 mock FastAPI endpoints for NEM energy copilot
# Auto-generated mock data matching exact TypeScript interface shapes.
# Paste into main.py after the existing imports and `app = FastAPI(...)`.
# Requires: import random as _r, from datetime import datetime as _dt
# =============================================================================

import random as _r
from datetime import datetime as _dt

# ---------------------------------------------------------------------------
# 1) /api/electricity-price-index/dashboard  ->  ElectricityPriceIndexDashboard
# ---------------------------------------------------------------------------

@app.get("/api/electricity-price-index/dashboard")
def electricity_price_index_dashboard():
    _r.seed(201)
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    states = ["NSW", "VIC", "QLD", "SA", "WA"]
    quarters = ["2023-Q1", "2023-Q2", "2023-Q3", "2023-Q4", "2024-Q1", "2024-Q2", "2024-Q3", "2024-Q4"]
    drivers = [
        "Wholesale cost increase", "Network investment", "Renewable transition",
        "Gas price spike", "Demand growth", "Carbon policy",
    ]

    cpi_records = []
    for q in quarters:
        for s in states:
            elec_idx = round(_r.uniform(140, 210), 1)
            all_idx = round(_r.uniform(120, 145), 1)
            elec_yoy = round(_r.uniform(-5, 25), 1)
            all_yoy = round(_r.uniform(2, 7), 1)
            cpi_records.append({
                "quarter": q,
                "electricity_cpi_index": elec_idx,
                "electricity_cpi_yoy_pct": elec_yoy,
                "all_cpi_index": all_idx,
                "all_cpi_yoy_pct": all_yoy,
                "electricity_vs_all_cpi_diff_pct": round(elec_yoy - all_yoy, 1),
                "state": s,
                "key_driver": _r.choice(drivers),
            })

    dist_zones = ["Ausgrid", "Endeavour", "Essential", "Energex", "Ergon", "CitiPower", "Powercor", "SA Power", "Western Power"]
    dmo_records = []
    for yr in [2022, 2023, 2024]:
        for s in states[:4]:
            zone = _r.choice(dist_zones)
            usage = _r.choice([3900, 4600, 5500, 6200])
            dmo = round(_r.uniform(1200, 2200), 2)
            market_avg = round(dmo * _r.uniform(0.82, 0.98), 2)
            best = round(market_avg * _r.uniform(0.85, 0.95), 2)
            dmo_records.append({
                "year": yr,
                "state": s,
                "distribution_zone": zone,
                "annual_usage_kwh": usage,
                "dmo_price_aud": dmo,
                "dmo_change_pct": round(_r.uniform(-8, 25), 1),
                "market_offer_avg_aud": market_avg,
                "market_offer_avg_change_pct": round(_r.uniform(-10, 20), 1),
                "best_market_offer_aud": best,
                "potential_saving_aud": round(dmo - best, 2),
            })

    tariff_components = []
    for yr in [2022, 2023, 2024]:
        for s in states[:4]:
            nw = round(_r.uniform(0.08, 0.14), 4)
            wh = round(_r.uniform(0.06, 0.12), 4)
            env = round(_r.uniform(0.01, 0.03), 4)
            ret = round(_r.uniform(0.01, 0.04), 4)
            met = round(_r.uniform(0.005, 0.015), 4)
            tariff_components.append({
                "state": s,
                "year": yr,
                "network_charges_aud_kwh": nw,
                "wholesale_charges_aud_kwh": wh,
                "environmental_charges_aud_kwh": env,
                "retail_margin_aud_kwh": ret,
                "metering_aud_kwh": met,
                "total_tariff_aud_kwh": round(nw + wh + env + ret + met, 4),
            })

    retailer_names = ["AGL", "Origin Energy", "EnergyAustralia", "Alinta", "Red Energy", "Lumo"]
    retailers = []
    for ret_name in retailer_names:
        for s in states[:4]:
            avg_offer = round(_r.uniform(1100, 2000), 2)
            retailers.append({
                "retailer": ret_name,
                "state": s,
                "market_share_pct": round(_r.uniform(3, 32), 1),
                "avg_offer_aud": avg_offer,
                "cheapest_offer_aud": round(avg_offer * _r.uniform(0.82, 0.95), 2),
                "customer_satisfaction_score": round(_r.uniform(55, 85), 1),
                "complaints_per_1000": round(_r.uniform(2, 18), 1),
                "churn_rate_pct": round(_r.uniform(8, 28), 1),
            })

    return {
        "timestamp": _dt.utcnow().isoformat() + "Z",
        "cpi_records": cpi_records,
        "dmo_records": dmo_records,
        "tariff_components": tariff_components,
        "retailers": retailers,
    }


# ---------------------------------------------------------------------------
# 2) /api/market-price-formation-review/dashboard  ->  MPFRDashboard
# ---------------------------------------------------------------------------

@app.get("/api/market-price-formation-review/dashboard")
def market_price_formation_review_dashboard():
    _r.seed(202)
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    states = ["NSW", "VIC", "QLD", "SA", "WA"]
    scarcity_types = [
        "Lack of Reserve 1", "LOR2", "LOR3", "Pre-LOR",
        "Emergency", "VoLL", "RERT", "Voluntary", "Market Suspended",
    ]
    customer_classes = ["Residential", "Small Business", "Commercial", "Industrial", "Agricultural"]
    reform_names = [
        "Capacity Investment Scheme", "Post-2025 Market Design",
        "Transmission Access Reform", "Congestion Management Model",
        "Operating Reserve Demand Curve", "Strategic Reserve Enhancement",
    ]
    consultation_stages = ["Draft Rule", "Final Determination", "Implementation", "Review", "Consultation"]
    outcomes = ["Approved", "Under Review", "Deferred", "Partially Approved"]

    price_caps = []
    for yr in [2022, 2023, 2024]:
        for r in regions:
            price_caps.append({
                "region": r,
                "year": yr,
                "market_price_cap_mwh": 16600.0,
                "administered_price_cap_mwh": 300.0,
                "voll_mwh": round(_r.uniform(33000, 40000), 0),
            })

    scarcity_events = []
    for _ in range(20):
        scarcity_events.append({
            "region": _r.choice(regions),
            "scarcity_type": _r.choice(scarcity_types),
        })

    vcr_values = []
    for cc in customer_classes:
        for s in states:
            vcr_values.append({
                "customer_class": cc,
                "state": s,
                "vcr_aud_per_mwh": round(_r.uniform(12000, 45000), 0),
            })

    reforms = []
    for name in reform_names:
        reforms.append({
            "reform_name": name,
            "consultation_stage": _r.choice(consultation_stages),
            "outcome": _r.choice(outcomes),
            "financial_impact_consumer_m": round(_r.uniform(-200, 500), 1),
        })

    quarters = ["2023-Q1", "2023-Q2", "2023-Q3", "2023-Q4", "2024-Q1", "2024-Q2", "2024-Q3", "2024-Q4"]
    marginal_costs = []
    for r in regions:
        for q in quarters:
            fuel = round(_r.uniform(20, 60), 2)
            carbon = round(_r.uniform(2, 15), 2)
            cap = round(_r.uniform(1, 10), 2)
            scarcity = round(_r.uniform(0, 8), 2)
            actual = round(fuel + carbon + cap + scarcity + _r.uniform(-10, 20), 2)
            marginal_costs.append({
                "region": r,
                "quarter": q,
                "fuel_cost_contribution_mwh": fuel,
                "carbon_cost_contribution_mwh": carbon,
                "capacity_adequacy_premium_mwh": cap,
                "scarcity_premium_mwh": scarcity,
                "actual_spot_price_mwh": actual,
            })

    demand_side = []
    for r in regions:
        for yr in [2022, 2023, 2024]:
            demand_side.append({
                "region": r,
                "year": yr,
                "demand_response_capacity_mw": round(_r.uniform(50, 600), 0),
            })

    return {
        "price_caps": price_caps,
        "scarcity_events": scarcity_events,
        "vcr_values": vcr_values,
        "reforms": reforms,
        "marginal_costs": marginal_costs,
        "demand_side": demand_side,
        "summary": {
            "current_mpc_mwh": 16600.0,
            "total_apc_activations_ytd": _r.randint(2, 12),
            "total_scarcity_cost_m": round(_r.uniform(50, 350), 1),
            "avg_vcr_residential_aud_per_mwh": round(_r.uniform(25000, 38000), 0),
            "active_reforms_count": len(reform_names),
            "demand_response_capacity_mw": round(_r.uniform(800, 2200), 0),
        },
    }


# ---------------------------------------------------------------------------
# 3) /api/electricity-market-price-formation/dashboard  ->  EMPFDashboard
# ---------------------------------------------------------------------------

@app.get("/api/electricity-market-price-formation/dashboard")
def electricity_market_price_formation_dashboard():
    _r.seed(203)
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    months = [f"2024-{m:02d}" for m in range(1, 13)]
    fuel_types = ["Black Coal", "Brown Coal", "Gas CCGT", "Gas OCGT", "Hydro", "Wind", "Solar"]
    event_types = ["Price Spike >$300", "Negative Price", "Administered Price", "VOLL"]
    participants = ["AGL", "Origin", "Snowy Hydro", "EnergyAustralia"]
    technologies = ["New Entrant CCGT", "New Entrant Wind", "New Entrant Solar", "New Entrant BESS"]
    investment_signals = ["Invest", "Marginal", "Do Not Invest", "Under Review"]

    price_drivers = []
    for r in regions:
        for m in months[:6]:
            price_drivers.append({
                "region": r,
                "month": m,
                "fuel_cost_component": round(_r.uniform(15, 55), 2),
                "carbon_cost_component": round(_r.uniform(2, 12), 2),
                "capacity_scarcity_component": round(_r.uniform(0, 15), 2),
                "network_constraint_component": round(_r.uniform(1, 10), 2),
                "renewable_suppression_component": round(_r.uniform(-8, 0), 2),
                "fcas_cost_component": round(_r.uniform(0.5, 5), 2),
            })

    marginal_units = []
    for ft in fuel_types:
        marginal_units.append({
            "fuel_type": ft,
            "pct_intervals_marginal": round(_r.uniform(2, 35), 1),
        })

    price_events = []
    for _ in range(15):
        price_events.append({
            "peak_price": round(_r.uniform(300, 16600), 2),
            "duration_intervals": _r.randint(1, 24),
            "total_cost_m": round(_r.uniform(0.5, 45), 2),
            "event_type": _r.choice(event_types),
            "region": _r.choice(regions),
        })

    quarters = ["2023-Q1", "2023-Q2", "2023-Q3", "2023-Q4", "2024-Q1", "2024-Q2"]
    bidding_behaviour = []
    for q in quarters:
        for p in participants:
            bidding_behaviour.append({
                "quarter": q,
                "participant": p,
                "capacity_bid_at_voll_pct": round(_r.uniform(0, 15), 1),
            })

    long_run_costs = []
    for tech in technologies:
        for r in regions:
            lrmc = round(_r.uniform(45, 120), 2)
            req_cf = round(_r.uniform(15, 60), 1)
            cur_cf = round(req_cf * _r.uniform(0.6, 1.2), 1)
            long_run_costs.append({
                "technology": tech,
                "region": r,
                "lrmc_per_mwh": lrmc,
                "required_capacity_factor_pct": req_cf,
                "current_cf_pct": cur_cf,
                "investment_signal": _r.choice(investment_signals),
            })

    return {
        "price_drivers": price_drivers,
        "marginal_units": marginal_units,
        "price_events": price_events,
        "bidding_behaviour": bidding_behaviour,
        "long_run_costs": long_run_costs,
        "summary": {
            "avg_spot_price_mwh": round(_r.uniform(55, 120), 2),
            "most_frequent_price_setter": _r.choice(fuel_types),
            "price_spike_events_ytd": _r.randint(15, 85),
            "avg_lrmc_new_wind": round(_r.uniform(50, 75), 2),
            "most_concentrated_region": _r.choice(regions),
        },
    }


# ---------------------------------------------------------------------------
# 4) /api/electricity-price-cap-intervention/dashboard  ->  EPCIDashboard
# ---------------------------------------------------------------------------

@app.get("/api/electricity-price-cap-intervention/dashboard")
def electricity_price_cap_intervention_dashboard():
    _r.seed(204)
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    trigger_types = ["MPC Activated", "CPT Breach", "RERT Trigger", "APC Price"]
    action_types = ["RERT Activation", "DR Activation", "Interconnector Dispatch", "Reserve Trader"]
    action_outcomes = ["Prevented Cap", "Reduced Duration", "Ineffective"]
    generators = ["AGL Macquarie", "Origin Eraring", "Stanwell Corp", "Snowy Hydro", "CS Energy", "EnergyAustralia Yallourn"]

    price_cap_events = []
    for _ in range(18):
        price_cap_events.append({
            "trigger_type": _r.choice(trigger_types),
            "spot_price_avg_aud_mwh": round(_r.uniform(300, 16600), 2),
        })

    market_impact = []
    for yr in [2022, 2023, 2024]:
        for q in ["Q1", "Q2", "Q3", "Q4"]:
            for r in regions[:3]:
                market_impact.append({
                    "year": yr,
                    "quarter": q,
                    "region": r,
                    "cap_events_count": _r.randint(0, 8),
                })

    generator_response = []
    for yr in [2022, 2023, 2024]:
        for g in generators:
            generator_response.append({
                "year": yr,
                "generator": g,
                "revenue_impact_m": round(_r.uniform(-50, 30), 1),
            })

    threshold_tracker = []
    for yr in [2023, 2024]:
        for m in range(1, 13):
            for r in regions[:3]:
                threshold_tracker.append({
                    "year": yr,
                    "month": m,
                    "region": r,
                    "cumulative_price_atd": round(_r.uniform(50000, 1200000), 0),
                })

    remedy_actions = []
    for _ in range(15):
        remedy_actions.append({
            "action_type": _r.choice(action_types),
            "outcome": _r.choice(action_outcomes),
            "capacity_mw": round(_r.uniform(50, 800), 0),
        })

    return {
        "price_cap_events": price_cap_events,
        "market_impact": market_impact,
        "generator_response": generator_response,
        "threshold_tracker": threshold_tracker,
        "remedy_actions": remedy_actions,
        "summary": {
            "total_cap_events_fy": _r.randint(10, 45),
            "total_hours_at_cap_fy": round(_r.uniform(5, 80), 1),
            "max_administered_price_aud_mwh": 300.0,
            "total_consumer_savings_m": round(_r.uniform(100, 800), 1),
            "most_affected_region": _r.choice(regions),
        },
    }


# ---------------------------------------------------------------------------
# 5) /api/nem-price-review/dashboard  ->  NEPRdashboard
# ---------------------------------------------------------------------------

@app.get("/api/nem-price-review/dashboard")
def nem_price_review_dashboard():
    _r.seed(205)
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    states = ["NSW", "VIC", "QLD", "SA", "WA", "TAS"]

    spot_prices = []
    for r in regions:
        for yr in [2022, 2023, 2024]:
            for m in range(1, 13):
                spot_prices.append({
                    "region": r,
                    "year": yr,
                    "month": m,
                    "avg_spot_price_mwh": round(_r.uniform(30, 180), 2),
                })

    retail_prices = []
    for s in states:
        for yr in [2022, 2023, 2024]:
            retail_prices.append({
                "state": s,
                "year": yr,
                "avg_residential_c_kwh": round(_r.uniform(22, 42), 2),
                "avg_sme_c_kwh": round(_r.uniform(18, 36), 2),
            })

    quarters = ["Q1", "Q2", "Q3", "Q4"]
    price_drivers = []
    for r in regions:
        for yr in [2023, 2024]:
            for q in quarters:
                wh = round(_r.uniform(25, 45), 1)
                nw = round(_r.uniform(30, 45), 1)
                env = round(_r.uniform(5, 15), 1)
                ret = round(_r.uniform(5, 12), 1)
                oth = round(100 - wh - nw - env - ret, 1)
                price_drivers.append({
                    "region": r,
                    "year": yr,
                    "quarter": q,
                    "wholesale_component_pct": wh,
                    "network_component_pct": nw,
                    "environmental_component_pct": env,
                    "retail_margin_pct": ret,
                    "other_pct": oth,
                })

    affordability = []
    for s in states:
        for yr in [2022, 2023, 2024]:
            affordability.append({
                "state": s,
                "year": yr,
                "avg_household_bill_aud": round(_r.uniform(1200, 2600), 0),
                "energy_poverty_pct": round(_r.uniform(5, 22), 1),
            })

    return {
        "spot_prices": spot_prices,
        "retail_prices": retail_prices,
        "price_drivers": price_drivers,
        "affordability": affordability,
        "summary": {
            "avg_spot_price_2024_mwh": round(_r.uniform(60, 110), 2),
            "avg_retail_price_2024_c_kwh": round(_r.uniform(28, 38), 2),
            "highest_price_region": _r.choice(regions),
            "total_cap_price_events": _r.randint(8, 40),
        },
    }


# ---------------------------------------------------------------------------
# 6) /api/demand-curve-price-anchor/dashboard  ->  DCPAdashboard
# ---------------------------------------------------------------------------

@app.get("/api/demand-curve-price-anchor/dashboard")
def demand_curve_price_anchor_dashboard():
    _r.seed(206)
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    sectors = ["Residential", "Commercial", "Industrial", "Agricultural"]
    quarters = ["Q1", "Q2", "Q3", "Q4"]

    demand = []
    for yr in [2022, 2023, 2024]:
        for m in range(1, 13):
            for r in regions:
                demand.append({
                    "year": yr,
                    "month": m,
                    "region": r,
                    "peak_demand_mw": round(_r.uniform(3000, 14000), 0),
                })

    price_anchors = []
    for yr in [2022, 2023, 2024]:
        for q in quarters:
            for r in regions:
                vwap = round(_r.uniform(50, 150), 2)
                twa = round(_r.uniform(40, 130), 2)
                price_anchors.append({
                    "year": yr,
                    "quarter": q,
                    "region": r,
                    "vwap_mwh": vwap,
                    "twa_price_mwh": twa,
                    "price_anchor_mwh": round((vwap + twa) / 2, 2),
                })

    elasticity = []
    for sec in sectors:
        elasticity.append({
            "sector": sec,
            "price_elasticity": round(_r.uniform(-0.8, -0.05), 3),
        })

    forecasts = []
    for yr in [2024, 2025]:
        for q in quarters:
            for r in regions:
                forecast = round(_r.uniform(4000, 13500), 0)
                actual = round(forecast * _r.uniform(0.9, 1.1), 0)
                forecasts.append({
                    "year": yr,
                    "quarter": q,
                    "region": r,
                    "forecast_demand_mw": forecast,
                    "actual_demand_mw": actual,
                })

    return {
        "demand": demand,
        "price_anchors": price_anchors,
        "elasticity": elasticity,
        "forecasts": forecasts,
        "summary": {
            "avg_peak_demand_mw": round(_r.uniform(7000, 10000), 0),
            "avg_price_anchor_mwh": round(_r.uniform(65, 95), 2),
            "most_elastic_region": _r.choice(regions),
            "total_demand_response_mw": round(_r.uniform(800, 2500), 0),
        },
    }
