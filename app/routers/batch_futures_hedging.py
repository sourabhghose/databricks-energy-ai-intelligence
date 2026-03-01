from __future__ import annotations
import random as _r
from datetime import datetime as _dt
from fastapi import APIRouter, Query

router = APIRouter()


# =========================================================================
# BATCH: Futures endpoints (5)
# =========================================================================

@router.get("/api/futures/dashboard")
def futures_dashboard(region: str = "NSW1"):
    _r.seed(7010)
    qtrs = ["Q1-2026", "Q2-2026", "Q3-2026", "Q4-2026", "Q1-2027", "Q2-2027"]
    contracts = [{"contract_code": f"BL-{region}-{q}", "region": region, "contract_type": _r.choice(["Base", "Peak"]), "year": int(q.split("-")[1]), "quarter": int(q[1]), "settlement_price": round(_r.uniform(50, 180), 2), "peak_price": round(_r.uniform(60, 250), 2), "change_1d": round(_r.uniform(-8, 8), 2), "change_1w": round(_r.uniform(-15, 15), 2), "open_interest": _r.randint(500, 5000), "volume_today": _r.randint(50, 800), "last_trade": "2025-12-20T14:30:00Z"} for q in qtrs]
    fc = [{"date": f"2026-{m:02d}-01", "base_price": round(_r.uniform(55, 170), 2), "peak_price": round(_r.uniform(70, 240), 2), "implied_volatility": round(_r.uniform(15, 55), 1)} for m in range(1, 13)]
    he = [{"hedge_type": ht, "region": region, "contract": f"BL-{region}-Q1-2026", "notional_mwh": _r.randint(10000, 100000), "hedge_price": round(_r.uniform(60, 150), 2), "spot_realised": round(_r.uniform(50, 200), 2), "pnl_aud": round(_r.uniform(-500000, 500000), 0), "effectiveness_pct": round(_r.uniform(60, 99), 1)} for ht in ["Swap", "Cap", "Collar", "Floor"]]
    return {"timestamp": _dt.utcnow().isoformat() + "Z", "region": region, "contracts": contracts, "forward_curve": fc, "hedge_effectiveness": he, "market_summary": {"avg_base_price": 95.4, "avg_peak_price": 142.1, "total_open_interest": 18500, "implied_vol_avg": 32.5}}


@router.get("/api/electricity-futures-options/dashboard")
def efot_dashboard():
    _r.seed(7011)
    regions = ["NSW1", "QLD1", "VIC1", "SA1"]
    contracts = [{"contract_id": f"EFOT-{i+1:04d}", "contract_type": _r.choice(["Base Swap", "Peak Swap", "Cap", "Asian Option"]), "region": reg, "delivery_year": _r.choice([2025, 2026]), "settlement_month": _r.randint(1, 12), "strike_price_mwh": round(_r.uniform(50, 200), 2), "market_price_mwh": round(_r.uniform(45, 220), 2), "open_interest_mwh": round(_r.uniform(10000, 200000), 0), "volume_traded_mwh": round(_r.uniform(1000, 50000), 0), "implied_volatility_pct": round(_r.uniform(15, 60), 1)} for i, reg in enumerate(regions * 3)]
    ohlc = [{"contract_id": f"EFOT-{_r.randint(1,12):04d}", "trade_date_offset": -d, "open_mwh": round(_r.uniform(60, 180), 2), "high_mwh": round(_r.uniform(70, 200), 2), "low_mwh": round(_r.uniform(50, 170), 2), "close_mwh": round(_r.uniform(55, 190), 2), "volume_mwh": round(_r.uniform(5000, 50000), 0), "vwap_mwh": round(_r.uniform(60, 185), 2)} for d in range(20)]
    hp = [{"program_id": f"HP-{i+1:03d}", "participant_name": p, "hedge_type": _r.choice(["Swap", "Cap", "Collar"]), "region": _r.choice(regions), "hedged_volume_twh": round(_r.uniform(1, 15), 1), "hedge_ratio_pct": round(_r.uniform(40, 95), 1), "avg_strike_mwh": round(_r.uniform(55, 160), 2), "portfolio_delta_mwh": round(_r.uniform(-5000, 5000), 0), "mark_to_market_m_aud": round(_r.uniform(-50, 80), 1), "var_95_m_aud": round(_r.uniform(5, 40), 1)} for i, p in enumerate(["AGL", "Origin", "EnergyAustralia", "Snowy Hydro", "Alinta"])]
    md = [{"region": reg, "contract_type": ct, "quarter": f"Q{q}", "year": 2026, "bid_volume_mwh": round(_r.uniform(5000, 50000), 0), "ask_volume_mwh": round(_r.uniform(5000, 50000), 0), "bid_ask_spread_mwh": round(_r.uniform(0.5, 5), 2), "market_maker_count": _r.randint(3, 10), "liquidity_score": round(_r.uniform(0.3, 0.95), 2)} for reg in regions for ct in ["Base", "Peak"] for q in [1, 2]]
    vol = [{"region": reg, "year": 2025, "month": m, "realised_vol_30d": round(_r.uniform(15, 70), 1), "implied_vol": round(_r.uniform(18, 65), 1), "vol_risk_premium_pct": round(_r.uniform(-5, 15), 1), "skew": round(_r.uniform(-0.5, 1.5), 2), "kurtosis": round(_r.uniform(2, 8), 2)} for reg in regions[:2] for m in range(1, 7)]
    return {"contracts": contracts, "daily_ohlc": ohlc, "hedging_programs": hp, "market_depth": md, "volatility": vol, "summary": {"total_open_interest_twh": 45.2, "avg_implied_volatility_pct": 34.8, "most_liquid_region": "NSW1", "total_hedging_volume_twh": 38.5, "avg_hedge_ratio_pct": 72.3, "var_95_portfolio_m_aud": 28.4, "active_contracts": 156}}


@router.get("/api/futures-market-risk/dashboard")
def futures_market_risk_dashboard():
    _r.seed(7012)
    regions = ["NSW1", "QLD1", "VIC1", "SA1"]
    var_recs = [{"date": f"2025-12-{d:02d}", "region": reg, "portfolio_type": pt, "notional_position_m_aud": round(_r.uniform(10, 200), 1), "var_95_m_aud": round(_r.uniform(1, 30), 1), "var_99_m_aud": round(_r.uniform(2, 45), 1), "cvar_95_m_aud": round(_r.uniform(2, 40), 1), "delta_mwh": round(_r.uniform(-50000, 50000), 0), "gamma": round(_r.uniform(-1, 1), 3), "vega": round(_r.uniform(0, 500), 1), "theta_daily_aud": round(_r.uniform(-5000, 0), 0)} for d in [15, 20] for reg in regions[:2] for pt in ["Generator", "Retailer"]]
    he = [{"quarter": f"Q{q}-2025", "region": reg, "participant": p, "hedge_ratio_pct": round(_r.uniform(50, 95), 1), "hedge_instrument": _r.choice(["Swap", "Cap", "Collar"]), "avg_hedge_price": round(_r.uniform(60, 150), 2), "avg_spot_price": round(_r.uniform(50, 180), 2), "hedge_gain_loss_m_aud": round(_r.uniform(-20, 30), 1), "effectiveness_score_pct": round(_r.uniform(60, 98), 1), "basis_risk_m_aud": round(_r.uniform(0.5, 8), 1)} for q in [3, 4] for reg in regions[:2] for p in ["AGL", "Origin"]]
    br = [{"region": reg, "year": 2025, "quarter": f"Q{q}", "futures_settlement_price": round(_r.uniform(60, 160), 2), "spot_price_avg": round(_r.uniform(55, 170), 2), "basis_aud_mwh": round(_r.uniform(-15, 15), 2), "basis_volatility": round(_r.uniform(5, 25), 1), "max_basis_aud_mwh": round(_r.uniform(10, 40), 2), "min_basis_aud_mwh": round(_r.uniform(-40, -5), 2), "risk_exposure_m_aud": round(_r.uniform(1, 15), 1)} for reg in regions for q in [3, 4]]
    fp = [{"participant": p, "participant_type": pt, "region": _r.choice(regions), "contract_quarter": f"Q1-2026", "long_position_mw": round(_r.uniform(0, 2000), 0), "short_position_mw": round(_r.uniform(0, 2000), 0), "net_position_mw": round(_r.uniform(-1500, 1500), 0), "avg_entry_price": round(_r.uniform(60, 150), 2), "mark_to_market_m_aud": round(_r.uniform(-30, 30), 1), "margin_posted_m_aud": round(_r.uniform(5, 50), 1)} for p, pt in [("AGL", "Gentailer"), ("Origin", "Gentailer"), ("Snowy", "Generator"), ("Shell", "Trader"), ("Macquarie", "Financial")]]
    return {"timestamp": _dt.utcnow().isoformat() + "Z", "var_records": var_recs, "hedge_effectiveness": he, "basis_risk": br, "futures_positions": fp, "portfolio_var_95_m_aud": 42.8, "avg_hedge_ratio_pct": 74.5, "total_open_interest_mw": 18200, "avg_basis_risk_aud_mwh": 6.3}


@router.get("/api/futures-price-discovery/dashboard")
def futures_price_discovery_dashboard():
    _r.seed(7013)
    regions = ["NSW1", "QLD1", "VIC1", "SA1"]
    ts = [{"region": reg, "contract_month": f"2026-{m:02d}", "product": _r.choice(["Base", "Peak"]), "settlement_price_aud_mwh": round(_r.uniform(55, 180), 2), "open_interest_lots": _r.randint(100, 3000), "daily_volume_lots": _r.randint(10, 500), "implied_vol_pct": round(_r.uniform(15, 55), 1), "days_to_expiry": _r.randint(30, 365)} for reg in regions for m in range(1, 7)]
    basis = [{"region": reg, "month": f"2025-{m:02d}", "futures_price": round(_r.uniform(60, 160), 2), "spot_price": round(_r.uniform(55, 170), 2), "basis_aud": round(_r.uniform(-15, 20), 2), "basis_pct": round(_r.uniform(-10, 15), 2), "convergence_trend": _r.choice(["Converging", "Diverging", "Stable"]), "seasonal_factor": _r.choice(["Summer Premium", "Winter Lift", "Shoulder Dip", "Neutral"])} for reg in regions[:3] for m in range(7, 13)]
    carry = [{"region": reg, "near_contract": "Q4-2025", "far_contract": "Q1-2026", "carry_cost_aud": round(_r.uniform(-5, 15), 2), "storage_premium_aud": round(_r.uniform(0, 3), 2), "risk_premium_aud": round(_r.uniform(1, 10), 2), "convenience_yield_pct": round(_r.uniform(0, 8), 2)} for reg in regions]
    cs = [{"region": reg, "snapshot_date": "2025-12-20", "curve_shape": _r.choice(["Contango", "Backwardation", "Flat"]), "q1_price": round(_r.uniform(60, 140), 2), "q2_price": round(_r.uniform(55, 130), 2), "q3_price": round(_r.uniform(70, 160), 2), "q4_price": round(_r.uniform(65, 150), 2), "annual_slope_pct": round(_r.uniform(-15, 20), 2), "inflection_quarter": _r.choice(["Q1", "Q2", "Q3", "Q4"])} for reg in regions]
    return {"timestamp": _dt.utcnow().isoformat() + "Z", "term_structures": ts, "basis_records": basis, "carry_records": carry, "curve_shapes": cs}


@router.get("/api/electricity-options/dashboard")
def eov_dashboard():
    _r.seed(7014)
    regions = ["NSW1", "QLD1", "VIC1", "SA1"]
    ob = [{"option_id": f"OPT-{i+1:04d}", "underlying": f"BL-{reg}-Q1-2026", "region": reg, "expiry": "2026-03-31", "strike_per_mwh": round(_r.uniform(50, 200), 2), "option_type": _r.choice(["Call", "Put"]), "premium_per_mwh": round(_r.uniform(2, 30), 2), "delta": round(_r.uniform(-1, 1), 3), "gamma": round(_r.uniform(0, 0.05), 4), "theta": round(_r.uniform(-2, 0), 3), "vega": round(_r.uniform(0, 50), 2), "implied_vol_pct": round(_r.uniform(15, 60), 1), "moneyness": _r.choice(["ITM", "ATM", "OTM"]), "open_interest_mwh": round(_r.uniform(5000, 100000), 0)} for i, reg in enumerate(regions * 4)]
    vs = [{"tenor_months": t, "strike_pct_atm": s, "implied_vol_pct": round(_r.uniform(15, 65), 1), "region": reg} for t in [1, 3, 6, 12] for s in [80, 90, 100, 110, 120] for reg in regions[:2]]
    strats = [{"strategy_name": sn, "strategy_type": st, "legs": _r.randint(1, 4), "max_profit_per_mwh": round(_r.uniform(5, 50), 2), "max_loss_per_mwh": round(_r.uniform(2, 30), 2), "breakeven_low": round(_r.uniform(50, 90), 2), "breakeven_high": round(_r.uniform(110, 200), 2), "net_premium": round(_r.uniform(-10, 15), 2), "use_case": uc, "suitability": _r.choice(["Generator", "Retailer", "Trader"])} for sn, st, uc in [("Bull Call Spread", "Vertical", "Upside protection"), ("Bear Put Spread", "Vertical", "Downside hedge"), ("Collar", "Combination", "Range bound"), ("Straddle", "Volatility", "Vol play"), ("Cap", "Single", "Price ceiling")]]
    hv = [{"date": f"2025-{m:02d}-15", "region": reg, "realized_vol_30d": round(_r.uniform(15, 70), 1), "realized_vol_90d": round(_r.uniform(18, 55), 1), "implied_vol": round(_r.uniform(18, 65), 1), "vol_risk_premium": round(_r.uniform(-5, 15), 1)} for reg in regions[:3] for m in range(1, 13)]
    return {"options_book": ob, "vol_surface": vs, "strategies": strats, "hist_vol": hv, "summary": {"total_open_interest_gwh": 2.4, "avg_implied_vol_pct": 35.2, "put_call_ratio": 0.85, "most_traded_strike": 100}}


# =========================================================================
# BATCH: Hedging endpoints (10)
# =========================================================================

@router.get("/api/electricity-price-risk/dashboard")
def eprm_dashboard():
    _r.seed(7015)
    regions = ["NSW1", "QLD1", "VIC1", "SA1"]
    participants = ["AGL", "Origin", "EnergyAustralia", "Snowy Hydro", "Alinta"]
    portfolios = [{"portfolio_id": f"PF-{i+1:03d}", "entity_name": p, "entity_type": _r.choice(["Gentailer", "Generator", "Retailer"]), "region": regions[i % 4], "total_load_twh": round(_r.uniform(5, 30), 1), "total_generation_twh": round(_r.uniform(5, 35), 1), "net_position_twh": round(_r.uniform(-10, 10), 1), "hedge_ratio_pct": round(_r.uniform(50, 95), 1), "open_position_twh": round(_r.uniform(0.5, 8), 1), "var_95_m": round(_r.uniform(10, 80), 1), "cvar_95_m": round(_r.uniform(15, 100), 1), "max_loss_scenario_m": round(_r.uniform(30, 200), 1)} for i, p in enumerate(participants)]
    hedges = [{"hedge_id": f"H-{i+1:04d}", "portfolio_id": f"PF-{(i%5)+1:03d}", "product_type": _r.choice(["Swap", "Cap", "Collar", "Floor"]), "region": _r.choice(regions), "volume_mw": round(_r.uniform(50, 500), 0), "strike_price_dolpermwh": round(_r.uniform(50, 180), 2), "market_price_dolpermwh": round(_r.uniform(45, 200), 2), "start_date": "2025-07-01", "end_date": "2026-06-30", "mtm_value_m": round(_r.uniform(-20, 30), 1), "premium_paid_m": round(_r.uniform(0.5, 10), 1), "hedge_effectiveness_pct": round(_r.uniform(60, 99), 1), "counterparty": _r.choice(["Macquarie", "Shell", "BP", "Trafigura"])} for i in range(15)]
    var_recs = [{"var_id": f"V-{i+1:03d}", "portfolio_id": f"PF-{(i%5)+1:03d}", "calculation_date": "2025-12-20", "methodology": _r.choice(["Historical", "Monte Carlo", "Parametric"]), "confidence_level_pct": cl, "time_horizon_days": _r.choice([1, 10, 30]), "var_m": round(_r.uniform(5, 60), 1), "cvar_m": round(_r.uniform(8, 80), 1), "scenario_99_m": round(_r.uniform(15, 120), 1), "stressed_var_m": round(_r.uniform(20, 150), 1), "correlation_risk_m": round(_r.uniform(1, 15), 1), "basis_risk_m": round(_r.uniform(0.5, 10), 1)} for i, cl in enumerate([95, 99] * 5)]
    scenarios = [{"scenario_id": f"SC-{i+1:02d}", "scenario_name": s, "year": 2026, "avg_price_dolpermwh": round(_r.uniform(40, 300), 2), "peak_price_dolpermwh": round(_r.uniform(100, 2000), 2), "price_vol_pct": round(_r.uniform(20, 80), 1), "portfolio_pnl_m": round(_r.uniform(-200, 100), 1), "hedge_benefit_m": round(_r.uniform(0, 80), 1), "worst_30day_loss_m": round(_r.uniform(10, 150), 1)} for i, s in enumerate(["Base", "High Gas", "Drought", "Renewable Surge", "Demand Spike"])]
    corr = [{"correlation_id": f"CR-{i+1:02d}", "factor_pair": fp, "correlation_coefficient": round(_r.uniform(-0.8, 0.95), 2), "r_squared": round(_r.uniform(0.1, 0.9), 2), "lag_days": _r.randint(0, 5), "data_period_years": 5, "statistical_significance": True, "hedging_implication": _r.choice(["Natural hedge", "Basis risk", "Diversification benefit"])} for i, fp in enumerate(["Gas-Elec", "Wind-Price", "Solar-Demand", "Temp-Demand", "Coal-Elec", "Carbon-Elec"])]
    reg_cap = [{"capital_id": f"RC-{i+1:02d}", "entity_name": p, "regulatory_framework": "AEMO Prudential", "capital_requirement_m": round(_r.uniform(20, 200), 0), "current_capital_m": round(_r.uniform(25, 250), 0), "coverage_ratio_pct": round(_r.uniform(100, 180), 1), "liquidity_buffer_m": round(_r.uniform(5, 50), 0), "stressed_requirement_m": round(_r.uniform(30, 300), 0), "compliance_status": _r.choice(["Compliant", "Compliant", "Watch"]), "review_date": "2026-03-31"} for i, p in enumerate(participants)]
    return {"portfolios": portfolios, "hedges": hedges, "var_records": var_recs, "scenarios": scenarios, "correlations": corr, "regulatory_capital": reg_cap, "summary": {"total_var_95_m": 185, "avg_hedge_ratio_pct": 74, "total_open_positions_twh": 22}}


@router.get("/api/energy-market-credit-risk-x/dashboard")
def emcrx_dashboard():
    _r.seed(7016)
    ptypes = ["Gentailer", "Generator", "Retailer", "Trader", "Financial"]
    participants = [{"participant_name": p, "participant_type": pt, "credit_rating": _r.choice(["AAA", "AA", "A", "BBB", "BB"]), "credit_outlook": _r.choice(["Stable", "Positive", "Negative"]), "prudential_requirement_m": round(_r.uniform(20, 300), 0), "credit_support_lodged_m": round(_r.uniform(25, 350), 0), "net_market_exposure_m": round(_r.uniform(5, 150), 0), "leverage_ratio_pct": round(_r.uniform(20, 70), 1), "interest_coverage": round(_r.uniform(2, 10), 1)} for p, pt in [("AGL", "Gentailer"), ("Origin", "Gentailer"), ("EnergyAustralia", "Retailer"), ("Snowy Hydro", "Generator"), ("Shell Energy", "Trader"), ("Macquarie", "Financial"), ("Alinta", "Generator")]]
    exposures = [{"quarter": f"Q{q}-2025", "region": reg, "participant_type": pt, "total_market_exposure_m": round(_r.uniform(50, 500), 0), "unsecured_exposure_m": round(_r.uniform(5, 80), 0), "collateral_coverage_pct": round(_r.uniform(80, 150), 1), "largest_single_exposure_m": round(_r.uniform(10, 100), 0), "concentration_risk_score": round(_r.uniform(0.2, 0.8), 2)} for q in [3, 4] for reg in ["NSW1", "QLD1", "VIC1"] for pt in ptypes[:3]]
    defaults = [{"year": y, "event_type": _r.choice(["Payment Default", "Margin Call Failure", "Insolvency"]), "participant_type": _r.choice(ptypes[:3]), "default_amount_m": round(_r.uniform(5, 200), 0), "recovery_pct": round(_r.uniform(20, 85), 1), "days_to_resolution": _r.randint(30, 365), "aemo_intervention": _r.choice([True, False])} for y in [2020, 2021, 2022, 2023, 2024]]
    prud = [{"metric_name": mn, "region": _r.choice(["NSW1", "QLD1", "VIC1"]), "quarter": f"Q4-2025", "avg_value": round(_r.uniform(50, 200), 1), "threshold_value": round(_r.uniform(80, 250), 1), "breach_count": _r.randint(0, 5), "trend": _r.choice(["Improving", "Stable", "Deteriorating"])} for mn in ["MCL", "Trading Limit", "Prudential Margin", "Reallocation Limit"]]
    stress = [{"scenario": s, "region": _r.choice(["NSW1", "QLD1", "VIC1"]), "total_exposure_m": round(_r.uniform(200, 2000), 0), "potential_default_m": round(_r.uniform(10, 300), 0), "systemic_risk_score": round(_r.uniform(0.1, 0.9), 2), "recovery_fund_adequacy_pct": round(_r.uniform(60, 150), 1), "mitigation_available": _r.choice([True, True, False])} for s in ["Extreme Price", "Participant Failure", "Correlated Default", "Liquidity Crisis"]]
    return {"participants": participants, "exposures": exposures, "default_history": defaults, "prudential_metrics": prud, "stress_tests": stress, "summary": {"total_market_exposure_m": 2800, "avg_credit_rating": "A", "breach_count_ytd": 3}}


@router.get("/api/hedge-effectiveness/dashboard")
def hef_dashboard():
    _r.seed(7017)
    regions = ["NSW1", "QLD1", "VIC1", "SA1"]
    companies = ["AGL", "Origin", "EnergyAustralia", "Snowy Hydro"]
    positions = [{"portfolio_id": f"PF-{i+1:03d}", "company": c, "region": regions[i % 4], "contract_type": _r.choice(["Swap", "Cap", "Collar", "Floor"]), "position": _r.choice(["Long", "Short"]), "notional_mw": round(_r.uniform(50, 500), 0), "strike_price": round(_r.uniform(55, 170), 2), "market_price": round(_r.uniform(50, 190), 2), "mtm_value_m": round(_r.uniform(-15, 20), 1), "delta": round(_r.uniform(-1, 1), 3), "gamma": round(_r.uniform(-0.02, 0.02), 4), "vega": round(_r.uniform(0, 30), 2), "expiry": "2026-03-31"} for i, c in enumerate(companies * 3)]
    basis = [{"region": reg, "hedge_region": hreg, "quarter": f"Q{q}-2025", "spot_price_hedge_region": round(_r.uniform(50, 160), 2), "spot_price_physical_region": round(_r.uniform(50, 170), 2), "basis_differential": round(_r.uniform(-20, 20), 2), "basis_risk_pct": round(_r.uniform(2, 15), 1), "correlation": round(_r.uniform(0.7, 0.99), 3), "avg_interconnector_constraint_hrs": _r.randint(0, 200)} for reg in regions[:2] for hreg in regions[2:4] for q in [3, 4]]
    pnl = [{"month": f"2025-{m:02d}", "portfolio_id": f"PF-{(m%4)+1:03d}", "physical_pnl_m": round(_r.uniform(-10, 15), 1), "hedge_pnl_m": round(_r.uniform(-8, 12), 1), "net_pnl_m": round(_r.uniform(-5, 10), 1), "hedge_ratio_pct": round(_r.uniform(60, 95), 1), "var_95_m": round(_r.uniform(3, 20), 1), "cvar_95_m": round(_r.uniform(5, 25), 1), "realized_vol_annualized": round(_r.uniform(15, 60), 1)} for m in range(1, 13)]
    hr = [{"company": c, "region": reg, "quarter": f"Q{q}-2025", "optimal_hedge_ratio": round(_r.uniform(70, 95), 1), "actual_hedge_ratio": round(_r.uniform(55, 100), 1), "deviation_from_optimal_pct": round(_r.uniform(-15, 15), 1), "cost_of_over_hedging_m": round(_r.uniform(0, 5), 1), "cost_of_under_hedging_m": round(_r.uniform(0, 8), 1), "recommendation": _r.choice(["Maintain", "Increase Hedge", "Reduce Hedge"])} for c in companies[:2] for reg in regions[:2] for q in [3, 4]]
    rp = [{"year": y, "region": reg, "avg_annual_spot_price": round(_r.uniform(50, 150), 2), "avg_hedge_price": round(_r.uniform(55, 140), 2), "hedge_premium_pct": round(_r.uniform(-5, 15), 1), "hedge_savings_m": round(_r.uniform(-20, 50), 1), "unhedged_cost_m": round(_r.uniform(100, 500), 0), "hedged_cost_m": round(_r.uniform(80, 450), 0), "effectiveness_pct": round(_r.uniform(60, 98), 1)} for y in [2021, 2022, 2023, 2024, 2025] for reg in regions[:2]]
    return {"positions": positions, "basis_risk": basis, "pnl_attribution": pnl, "hedge_ratios": hr, "rolling_performance": rp, "summary": {"avg_effectiveness_pct": 82.5, "total_hedge_savings_m": 145, "optimal_ratio_deviation_pct": 4.2}}


@router.get("/api/market-power/dashboard")
def market_power_dashboard():
    _r.seed(7018)
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    fuel_types = ["Black Coal", "Gas", "Hydro", "Wind", "Solar", "Battery"]
    hhi = [{"region": reg, "fuel_type": ft, "hhi_score": _r.randint(800, 3500), "num_competitors": _r.randint(3, 15), "top3_share_pct": round(_r.uniform(40, 90), 1), "market_structure": _r.choice(["Competitive", "Moderately Concentrated", "Highly Concentrated"]), "trend_direction": _r.choice(["Improving", "Stable", "Worsening"]), "change_vs_last_year": round(_r.uniform(-200, 200), 0)} for reg in regions for ft in fuel_types[:3]]
    pivotal = [{"participant_id": f"GEN-{i+1:03d}", "participant_name": p, "region": _r.choice(regions[:4]), "pivotal_status": _r.choice(["Pivotal", "Quasi-Pivotal", "Non-Pivotal"]), "capacity_mw": round(_r.uniform(1000, 8000), 0), "residual_supply_index": round(_r.uniform(0.8, 1.5), 2), "occurrence_frequency_pct": round(_r.uniform(5, 60), 1), "strategic_capacity_mw": round(_r.uniform(200, 3000), 0), "avg_rebids_per_day": round(_r.uniform(1, 15), 1)} for i, p in enumerate(["AGL", "Origin", "EnergyAustralia", "Snowy Hydro", "CS Energy", "Stanwell", "Alinta"])]
    trends = [{"participant_name": p, "participant_type": _r.choice(["Gentailer", "Generator"]), "year": y, "quarter": f"Q{q}", "generation_share_pct": round(_r.uniform(5, 25), 1), "retail_share_pct": round(_r.uniform(5, 30), 1), "capacity_mw": round(_r.uniform(2000, 10000), 0)} for p in ["AGL", "Origin", "EnergyAustralia", "Snowy Hydro"] for y in [2024, 2025] for q in [1, 2, 3, 4]]
    return {"timestamp": _dt.utcnow().isoformat() + "Z", "nem_overall_hhi": 1850, "sa1_hhi": 2800, "concentration_trend": "Moderately Concentrated", "pivotal_suppliers_count": 3, "quasi_pivotal_count": 2, "market_review_status": "Under Review", "hhi_records": hhi, "pivotal_suppliers": pivotal, "share_trends": trends}


@router.get("/api/hedging/dashboard")
def hedging_main_dashboard():
    _r.seed(7019)
    regions = ["NSW1", "QLD1", "VIC1", "SA1"]
    contracts = [{"contract_id": f"HC-{i+1:04d}", "contract_type": _r.choice(["Swap", "Cap", "Collar", "Floor", "Asian Option"]), "region": _r.choice(regions), "counterparty": _r.choice(["Macquarie", "Shell", "BP", "Trafigura", "Origin"]), "start_date": "2025-07-01", "end_date": "2026-06-30", "strike_price": round(_r.uniform(50, 180), 2), "volume_mw": round(_r.uniform(20, 500), 0), "volume_mwh": round(_r.uniform(50000, 2000000), 0), "premium_paid_aud": round(_r.uniform(50000, 2000000), 0), "mtm_value_aud": round(_r.uniform(-500000, 800000), 0), "pnl_aud": round(_r.uniform(-300000, 500000), 0), "hedge_period": f"Q{_r.randint(1,4)}-2026", "status": _r.choice(["Active", "Active", "Expired"]), "underlying": f"BL-{_r.choice(regions)}"} for i in range(20)]
    portfolio = [{"region": reg, "total_hedged_mw": round(_r.uniform(500, 3000), 0), "expected_generation_mw": round(_r.uniform(600, 4000), 0), "hedge_ratio_pct": round(_r.uniform(50, 95), 1), "avg_swap_price": round(_r.uniform(60, 140), 2), "mtm_total_aud": round(_r.uniform(-2000000, 5000000), 0), "unrealised_pnl_aud": round(_r.uniform(-1000000, 3000000), 0), "var_95_aud": round(_r.uniform(500000, 5000000), 0), "var_99_aud": round(_r.uniform(800000, 8000000), 0), "cap_protection_pct": round(_r.uniform(30, 80), 1), "num_active_contracts": _r.randint(5, 25)} for reg in regions]
    qp = [{"quarter": f"Q{q}-2026", "hedged_mw": round(_r.uniform(1000, 5000), 0), "spot_ref": round(_r.uniform(60, 150), 2), "contract_price": round(_r.uniform(55, 140), 2)} for q in [1, 2, 3, 4]]
    return {"timestamp": _dt.utcnow().isoformat() + "Z", "total_portfolio_mtm_aud": 4850000, "total_unrealised_pnl_aud": 2100000, "portfolio_var_95_aud": 8500000, "weighted_avg_hedge_price": 92.5, "overall_hedge_ratio_pct": 74.2, "contracts": contracts, "portfolio_by_region": portfolio, "quarterly_position": qp}


@router.get("/api/hedging/contracts")
def hedging_contracts(region: str = None, contract_type: str = None, status: str = None):
    _r.seed(7020)
    regions = ["NSW1", "QLD1", "VIC1", "SA1"]
    contracts = [{"contract_id": f"HC-{i+1:04d}", "contract_type": _r.choice(["Swap", "Cap", "Collar", "Floor"]), "region": _r.choice(regions), "counterparty": _r.choice(["Macquarie", "Shell", "BP", "Trafigura"]), "start_date": "2025-07-01", "end_date": "2026-06-30", "strike_price": round(_r.uniform(50, 180), 2), "volume_mw": round(_r.uniform(20, 500), 0), "volume_mwh": round(_r.uniform(50000, 2000000), 0), "premium_paid_aud": round(_r.uniform(50000, 2000000), 0), "mtm_value_aud": round(_r.uniform(-500000, 800000), 0), "pnl_aud": round(_r.uniform(-300000, 500000), 0), "hedge_period": f"Q{_r.randint(1,4)}-2026", "status": _r.choice(["Active", "Active", "Expired"]), "underlying": f"BL-{_r.choice(regions)}"} for i in range(25)]
    if region:
        contracts = [c for c in contracts if c["region"] == region]
    if contract_type:
        contracts = [c for c in contracts if c["contract_type"] == contract_type]
    if status:
        contracts = [c for c in contracts if c["status"] == status]
    return contracts


@router.get("/api/hedging/portfolio")
def hedging_portfolio(region: str = None):
    _r.seed(7021)
    regions = ["NSW1", "QLD1", "VIC1", "SA1"]
    portfolio = [{"region": reg, "total_hedged_mw": round(_r.uniform(500, 3000), 0), "expected_generation_mw": round(_r.uniform(600, 4000), 0), "hedge_ratio_pct": round(_r.uniform(50, 95), 1), "avg_swap_price": round(_r.uniform(60, 140), 2), "mtm_total_aud": round(_r.uniform(-2000000, 5000000), 0), "unrealised_pnl_aud": round(_r.uniform(-1000000, 3000000), 0), "var_95_aud": round(_r.uniform(500000, 5000000), 0), "var_99_aud": round(_r.uniform(800000, 8000000), 0), "cap_protection_pct": round(_r.uniform(30, 80), 1), "num_active_contracts": _r.randint(5, 25)} for reg in regions]
    if region:
        portfolio = [p for p in portfolio if p["region"] == region]
    return portfolio


@router.get("/api/nem-participant-financial/dashboard")
def npfp_dashboard():
    _r.seed(7022)
    participants_data = [("AGL", "Gentailer", "NSW1", 22.5), ("Origin", "Gentailer", "QLD1", 18.3), ("EnergyAustralia", "Retailer", "VIC1", 15.1), ("Snowy Hydro", "Generator", "NSW1", 12.8), ("CS Energy", "Generator", "QLD1", 8.2), ("Stanwell", "Generator", "QLD1", 7.5), ("Alinta", "Generator", "SA1", 5.4)]
    parts = [{"participant_id": f"P-{i+1:03d}", "participant_name": p, "participant_type": pt, "region": reg, "market_share_pct": ms} for i, (p, pt, reg, ms) in enumerate(participants_data)]
    rev = [{"participant_id": f"P-{i+1:03d}", "year": y, "quarter": f"Q{q}", "trading_revenue_m_aud": round(_r.uniform(200, 2000), 0), "fcas_revenue_m_aud": round(_r.uniform(10, 150), 0), "capacity_revenue_m_aud": round(_r.uniform(0, 50), 0), "hedge_pnl_m_aud": round(_r.uniform(-100, 200), 0), "total_revenue_m_aud": round(_r.uniform(200, 2200), 0)} for i in range(7) for y in [2024, 2025] for q in [1, 2, 3, 4]]
    costs = [{"participant_id": f"P-{i+1:03d}", "year": y, "fuel_cost_m_aud": round(_r.uniform(50, 800), 0), "opex_m_aud": round(_r.uniform(30, 300), 0), "carbon_cost_m_aud": round(_r.uniform(5, 100), 0), "network_charges_m_aud": round(_r.uniform(10, 200), 0), "ebitda_m_aud": round(_r.uniform(50, 500), 0), "ebitda_margin_pct": round(_r.uniform(8, 35), 1)} for i in range(7) for y in [2024, 2025]]
    risk = [{"participant_id": f"P-{i+1:03d}", "year": 2025, "value_at_risk_m_aud": round(_r.uniform(10, 100), 0), "credit_exposure_m_aud": round(_r.uniform(20, 200), 0), "hedge_ratio_pct": round(_r.uniform(50, 95), 1), "liquidity_ratio": round(_r.uniform(1.2, 3.5), 1)} for i in range(7)]
    return {"participants": parts, "revenue": rev, "costs": costs, "risk": risk, "summary": {"total_participants": 7, "total_market_revenue_m_aud": 28500, "avg_ebitda_margin_pct": 18.5, "most_profitable_participant": "AGL"}}


@router.get("/api/energy-retailer-hedging/dashboard")
def erha_dashboard():
    _r.seed(7023)
    retailers = ["AGL Retail", "Origin Retail", "EnergyAustralia", "Simply Energy", "Red Energy"]
    regions = ["NSW1", "QLD1", "VIC1", "SA1"]
    hb = [{"retailer": ret, "region": reg, "contract_type": _r.choice(["Swap", "Cap", "Collar"]), "year": 2025, "quarter": _r.randint(1, 4), "hedge_volume_gwh": round(_r.uniform(0.5, 10), 1), "hedge_price_aud_mwh": round(_r.uniform(55, 150), 2), "market_price_aud_mwh": round(_r.uniform(50, 170), 2), "mtm_value_m": round(_r.uniform(-10, 15), 1), "hedge_ratio_pct": round(_r.uniform(50, 95), 1)} for ret in retailers[:3] for reg in regions[:3]]
    ce = [{"retailer": ret, "counterparty": cp, "contract_type": _r.choice(["Swap", "Cap"]), "notional_value_m": round(_r.uniform(10, 200), 0), "credit_rating": _r.choice(["AAA", "AA", "A", "BBB"]), "exposure_m": round(_r.uniform(2, 50), 0), "collateral_posted_m": round(_r.uniform(1, 30), 0)} for ret in retailers[:3] for cp in ["Macquarie", "Shell", "BP"]]
    hc = [{"retailer": ret, "region": reg, "year": 2025, "total_premium_m": round(_r.uniform(5, 50), 0), "realized_pnl_m": round(_r.uniform(-20, 30), 0), "unrealized_pnl_m": round(_r.uniform(-15, 25), 0), "hedging_cost_aud_mwh": round(_r.uniform(3, 15), 2)} for ret in retailers[:3] for reg in regions[:2]]
    vm = [{"region": reg, "year": 2025, "quarter": q, "spot_price_volatility_pct": round(_r.uniform(20, 80), 1), "implied_vol_cap_pct": round(_r.uniform(25, 70), 1), "hist_vol_30d_pct": round(_r.uniform(15, 65), 1), "var_95_m": round(_r.uniform(5, 40), 1)} for reg in regions for q in [1, 2, 3, 4]]
    st = [{"retailer": ret, "scenario": s, "region": _r.choice(regions), "pnl_impact_m": round(_r.uniform(-50, 20), 1), "hedge_effectiveness_pct": round(_r.uniform(50, 95), 1)} for ret in retailers[:3] for s in ["Price Spike", "Prolonged High", "Demand Surge"]]
    return {"hedge_book": hb, "counterparty_exposure": ce, "hedging_costs": hc, "volatility_metrics": vm, "stress_tests": st, "summary": {"total_hedge_book_value_m": 850, "avg_hedge_ratio_pct": 72.5, "total_counterparty_exposure_m": 320, "avg_hedging_cost_aud_mwh": 8.4, "max_var_95_m": 35.2}}


@router.get("/api/voll-analytics/dashboard")
def voll_analytics_dashboard():
    _r.seed(7024)
    voll = [{"year": y, "methodology": m, "residential_voll_aud_mwh": round(_r.uniform(15000, 40000), 0), "commercial_voll_aud_mwh": round(_r.uniform(30000, 80000), 0), "industrial_voll_aud_mwh": round(_r.uniform(10000, 50000), 0), "weighted_avg_voll_aud_mwh": round(_r.uniform(20000, 55000), 0), "nem_regulatory_voll_aud_mwh": round(_r.uniform(15000, 17500), 0), "review_body": _r.choice(["AEMC", "AEMO", "AER"])} for y in [2020, 2022, 2024] for m in ["SURVEY", "REVEALED_PREFERENCE", "HYBRID"]]
    oc = [{"year": y, "region": reg, "total_outage_hours": round(_r.uniform(10, 200), 0), "customers_affected_k": round(_r.uniform(5, 500), 0), "total_economic_cost_m_aud": round(_r.uniform(10, 500), 0), "residential_cost_m_aud": round(_r.uniform(3, 150), 0), "commercial_cost_m_aud": round(_r.uniform(5, 200), 0), "industrial_cost_m_aud": round(_r.uniform(2, 150), 0), "direct_cost_pct": round(_r.uniform(50, 80), 0), "indirect_cost_pct": round(_r.uniform(20, 50), 0)} for y in [2023, 2024, 2025] for reg in ["NSW1", "QLD1", "VIC1", "SA1"]]
    sectors = [{"sector": s, "avg_outage_cost_aud_hour": round(_r.uniform(5000, 200000), 0), "outage_sensitivity": sens, "critical_threshold_min": _r.randint(1, 120), "annual_exposure_m_aud": round(_r.uniform(10, 500), 0), "backup_power_adoption_pct": round(_r.uniform(10, 90), 0)} for s, sens in [("Data Centres", "HIGH"), ("Manufacturing", "HIGH"), ("Healthcare", "HIGH"), ("Retail", "MEDIUM"), ("Agriculture", "MEDIUM"), ("Residential", "LOW"), ("Government", "LOW")]]
    rv = [{"region": reg, "current_saidi_min": round(_r.uniform(60, 200), 0), "target_saidi_min": round(_r.uniform(40, 150), 0), "improvement_cost_m_aud": round(_r.uniform(50, 500), 0), "customers_benefited_k": round(_r.uniform(100, 2000), 0), "voll_saved_m_aud": round(_r.uniform(20, 300), 0), "benefit_cost_ratio": round(_r.uniform(0.5, 3.5), 1)} for reg in ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]]
    return {"timestamp": _dt.utcnow().isoformat() + "Z", "voll_estimates": voll, "outage_costs": oc, "industry_sectors": sectors, "reliability_values": rv}


# =========================================================================
# BATCH: Settlement endpoints (9)
# =========================================================================

@router.get("/api/nem-five-minute-settlement/dashboard")
def nfms_dashboard():
    _r.seed(7025)
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    intervals = [{"region": reg, "interval_end": f"2025-12-20T{h:02d}:{m:02d}:00Z", "dispatch_price_aud": round(_r.uniform(-50, 500), 2), "energy_mwh": round(_r.uniform(1000, 8000), 0), "demand_mw": round(_r.uniform(3000, 14000), 0)} for reg in regions[:3] for h in range(0, 24, 4) for m in [0, 5, 10]]
    generators = [{"generator_id": f"GEN-{i+1:03d}", "station_name": g, "region": _r.choice(regions[:4]), "fuel_type": ft, "capacity_mw": round(_r.uniform(100, 2800), 0), "output_mw": round(_r.uniform(50, 2500), 0), "revenue_5min_aud": round(_r.uniform(5000, 200000), 0)} for i, (g, ft) in enumerate([("Bayswater", "Black Coal"), ("Eraring", "Black Coal"), ("Loy Yang A", "Brown Coal"), ("Torrens Island", "Gas"), ("Snowy 2.0", "Hydro"), ("Hornsdale", "Wind"), ("Broken Hill Solar", "Solar")])]
    spikes = [{"region": reg, "timestamp": f"2025-{_r.randint(1,12):02d}-{_r.randint(1,28):02d}T{_r.randint(12,18):02d}:00:00Z", "price_aud": round(_r.uniform(300, 16600), 2), "duration_intervals": _r.randint(1, 12), "cause": _r.choice(["Generator Trip", "Demand Surge", "Interconnector Constraint", "Forecast Error"])} for reg in regions[:4] for _ in range(3)]
    batteries = [{"battery_name": b, "region": reg, "capacity_mw": cap, "revenue_5min_aud": round(_r.uniform(10000, 150000), 0), "cycles_today": _r.randint(1, 4), "avg_spread_aud": round(_r.uniform(30, 200), 2)} for b, reg, cap in [("Hornsdale Power Reserve", "SA1", 150), ("Victorian Big Battery", "VIC1", 300), ("Wallgrove", "NSW1", 50), ("Bouldercombe", "QLD1", 200)]]
    mi = [{"metric": m, "pre_5min_value": round(_r.uniform(10, 100), 1), "post_5min_value": round(_r.uniform(10, 100), 1), "change_pct": round(_r.uniform(-30, 50), 1)} for m in ["Price Volatility", "Battery Revenue", "Generator Ramping", "Demand Response", "Settlement Residue"]]
    return {"intervals": intervals, "generators": generators, "price_spikes": spikes, "batteries": batteries, "market_impact": mi, "summary": {"avg_5min_price_aud": 82.5, "max_5min_price_aud": 15800, "battery_total_revenue_aud": 420000, "price_spike_count": 12, "avg_demand_mw": 24500}}


@router.get("/api/aemo-5min-settlement/dashboard")
def aemo5m_dashboard():
    _r.seed(7026)
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    dispatch = [{"region": reg, "interval": f"2025-12-20T{h:02d}:{m:02d}:00Z", "dispatch_price": round(_r.uniform(-30, 400), 2), "total_demand_mw": round(_r.uniform(3000, 14000), 0), "scheduled_gen_mw": round(_r.uniform(2000, 12000), 0), "semi_scheduled_gen_mw": round(_r.uniform(500, 4000), 0)} for reg in regions[:3] for h in [8, 12, 16, 20] for m in [0, 5]]
    settlement = [{"region": reg, "trading_interval": f"2025-12-20T{h:02d}:00:00Z", "rrp_aud_mwh": round(_r.uniform(30, 300), 2), "total_energy_mwh": round(_r.uniform(5000, 30000), 0), "settlement_amount_aud": round(_r.uniform(100000, 5000000), 0)} for reg in regions[:4] for h in range(0, 24, 6)]
    intervals_data = [{"region": reg, "date": "2025-12-20", "interval_count": 288, "avg_price_aud": round(_r.uniform(40, 120), 2), "min_price_aud": round(_r.uniform(-30, 20), 2), "max_price_aud": round(_r.uniform(200, 5000), 2), "price_std_dev": round(_r.uniform(20, 150), 2)} for reg in regions]
    compliance = [{"participant": p, "region": _r.choice(regions[:4]), "dispatch_conformance_pct": round(_r.uniform(90, 100), 1), "causer_pays_factor": round(_r.uniform(0.5, 2.0), 2), "non_conformance_count": _r.randint(0, 10)} for p in ["AGL", "Origin", "EnergyAustralia", "Snowy Hydro", "CS Energy"]]
    return {"dispatch": dispatch, "settlement": settlement, "intervals": intervals_data, "compliance": compliance, "summary": {"total_settlement_aud": 45000000, "avg_price_all_regions_aud": 78.5, "max_price_aud": 4800, "dispatch_intervals_today": 288, "non_conformance_events": 8}}


@router.get("/api/settlement-analytics/dashboard")
def settlement_analytics_dashboard():
    _r.seed(7027)
    regions = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
    settlements = [{"region": reg, "period": f"2025-{m:02d}", "total_settlement_m_aud": round(_r.uniform(100, 800), 0), "energy_settlement_m_aud": round(_r.uniform(80, 700), 0), "fcas_settlement_m_aud": round(_r.uniform(5, 50), 0), "participant_count": _r.randint(50, 200), "disputes_count": _r.randint(0, 5)} for reg in regions for m in range(7, 13)]
    prudential = [{"participant": p, "mcl_m_aud": round(_r.uniform(20, 400), 0), "trading_limit_m_aud": round(_r.uniform(15, 350), 0), "prudential_margin_m_aud": round(_r.uniform(5, 100), 0), "credit_support_m_aud": round(_r.uniform(10, 200), 0), "utilization_pct": round(_r.uniform(30, 95), 1)} for p in ["AGL", "Origin", "EnergyAustralia", "Snowy Hydro", "CS Energy", "Stanwell"]]
    shortfalls = [{"region": reg, "date": f"2025-{_r.randint(7,12):02d}-{_r.randint(1,28):02d}", "shortfall_mwh": round(_r.uniform(10, 500), 0), "cost_aud": round(_r.uniform(50000, 5000000), 0), "cause": _r.choice(["Generator Trip", "Forecast Error", "Interconnector Limit"])} for reg in regions[:4] for _ in range(2)]
    exposures = [{"participant": p, "region": _r.choice(regions[:4]), "gross_exposure_m_aud": round(_r.uniform(20, 300), 0), "net_exposure_m_aud": round(_r.uniform(5, 100), 0), "collateral_held_m_aud": round(_r.uniform(10, 150), 0)} for p in ["AGL", "Origin", "EnergyAustralia", "Snowy Hydro"]]
    return {"timestamp": _dt.utcnow().isoformat() + "Z", "settlements": settlements, "prudential": prudential, "shortfalls": shortfalls, "exposures": exposures}


@router.get("/api/transmission-congestion-revenue/dashboard")
def tcra_dashboard():
    _r.seed(7028)
    interconnectors = ["NSW1-QLD1", "VIC1-NSW1", "VIC1-SA1", "VIC1-TAS1", "NSW1-QLD1(DC)"]
    ir = [{"interconnector": ic, "year": y, "quarter": f"Q{q}", "total_revenue_m_aud": round(_r.uniform(5, 80), 1), "avg_flow_mw": round(_r.uniform(100, 1000), 0), "congestion_hours": _r.randint(50, 2000), "direction": _r.choice(["Southbound", "Northbound"])} for ic in interconnectors for y in [2024, 2025] for q in [3, 4]]
    cg = [{"constraint_group": cg_name, "region": _r.choice(["NSW1", "QLD1", "VIC1", "SA1"]), "binding_hours": _r.randint(100, 3000), "cost_m_aud": round(_r.uniform(5, 100), 1), "affected_generators": _r.randint(3, 20)} for cg_name in ["N-Q_NIL", "V-SA_NIL", "T-V_NIL", "System Normal", "Outage Related"]]
    rps = [{"region_pair": rp, "avg_price_diff_aud": round(_r.uniform(-30, 50), 2), "max_price_diff_aud": round(_r.uniform(50, 500), 2), "hours_diverged": _r.randint(500, 4000)} for rp in ["NSW1-QLD1", "VIC1-SA1", "VIC1-TAS1", "NSW1-VIC1"]]
    cc = [{"region": reg, "year": 2025, "congestion_cost_m_aud": round(_r.uniform(20, 200), 0), "as_pct_of_energy_cost": round(_r.uniform(1, 8), 1)} for reg in ["NSW1", "QLD1", "VIC1", "SA1"]]
    mp = [{"project": p, "interconnector": _r.choice(interconnectors[:3]), "capacity_increase_mw": _r.randint(100, 1000), "estimated_cost_m_aud": round(_r.uniform(200, 3000), 0), "expected_saving_m_aud_yr": round(_r.uniform(20, 200), 0), "status": _r.choice(["Planning", "Approved", "Construction"])} for p in ["HumeLink", "VNI West", "Marinus Link", "Sydney Ring", "QNI Medium"]]
    return {"interconnector_revenues": ir, "constraint_groups": cg, "regional_price_splits": rps, "congestion_costs": cc, "mitigation_projects": mp, "summary": {"total_congestion_revenue_m_aud": 450, "most_congested_interconnector": "NSW1-QLD1", "total_mitigation_investment_m_aud": 8500, "avg_price_divergence_hours": 2200}}


@router.get("/api/congestion-revenue/dashboard")
def congestion_revenue_dashboard():
    _r.seed(7029)
    interconnectors = ["NSW1-QLD1", "VIC1-NSW1", "VIC1-SA1", "VIC1-TAS1"]
    sra = [{"contract_id": f"SRA-{i+1:04d}", "interconnector": _r.choice(interconnectors), "direction": _r.choice(["Forward", "Reverse"]), "quarter": f"Q{q}-2026", "units": _r.randint(10, 200), "auction_price_aud": round(_r.uniform(1000, 50000), 0), "settlement_residue_aud": round(_r.uniform(500, 60000), 0), "pnl_aud": round(_r.uniform(-20000, 40000), 0)} for i in range(12) for q in [1, 2]]
    cr = [{"interconnector": ic, "month": f"2025-{m:02d}", "congestion_rent_aud": round(_r.uniform(100000, 5000000), 0), "flow_mwh": round(_r.uniform(50000, 500000), 0), "binding_intervals": _r.randint(100, 2000)} for ic in interconnectors for m in range(7, 13)]
    np = [{"node": f"NODE-{reg}", "region": reg, "avg_price_aud": round(_r.uniform(40, 150), 2), "marginal_loss_factor": round(_r.uniform(0.85, 1.05), 3)} for reg in ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]]
    ie = [{"interconnector": ic, "avg_flow_mw": round(_r.uniform(100, 900), 0), "capacity_mw": round(_r.uniform(500, 1500), 0), "utilization_pct": round(_r.uniform(30, 85), 1), "revenue_per_mw_aud": round(_r.uniform(5000, 50000), 0)} for ic in interconnectors]
    return {"timestamp": _dt.utcnow().isoformat() + "Z", "sra_contracts": sra, "congestion_rents": cr, "nodal_prices": np, "interconnector_economics": ie}


@router.get("/api/nem-congestion-rent/dashboard")
def ncra_dashboard():
    _r.seed(7030)
    interconnectors_list = ["NSW1-QLD1", "VIC1-NSW1", "VIC1-SA1", "VIC1-TAS1"]
    ics = [{"interconnector": ic, "capacity_mw": _r.randint(500, 1500), "avg_flow_mw": round(_r.uniform(100, 900), 0), "total_rent_m_aud": round(_r.uniform(10, 150), 1), "binding_hours": _r.randint(500, 4000), "congestion_frequency_pct": round(_r.uniform(10, 60), 1)} for ic in interconnectors_list]
    constraints = [{"constraint_id": f"C-{i+1:03d}", "constraint_name": cn, "interconnector": _r.choice(interconnectors_list), "binding_hours": _r.randint(100, 2000), "marginal_value_aud": round(_r.uniform(5, 200), 2), "cost_impact_m_aud": round(_r.uniform(1, 50), 1)} for i, cn in enumerate(["N>>Q_NIL", "V>>SA_NIL", "T>>V_NIL", "System_Normal", "N>>V_Thermal", "SA_Wind_Limit"])]
    rb = [{"from_region": f, "to_region": t, "avg_price_diff_aud": round(_r.uniform(-40, 60), 2), "max_diff_aud": round(_r.uniform(50, 800), 2), "correlation": round(_r.uniform(0.5, 0.95), 2)} for f, t in [("NSW1", "QLD1"), ("VIC1", "SA1"), ("VIC1", "TAS1"), ("NSW1", "VIC1")]]
    dist = [{"participant_type": pt, "share_pct": round(_r.uniform(5, 30), 1), "total_m_aud": round(_r.uniform(10, 100), 1)} for pt in ["Generator", "Retailer", "Network", "Customer"]]
    return {"interconnectors": ics, "constraints": constraints, "regional_basis": rb, "distribution": dist, "summary": {"total_congestion_rent_m_aud": 380, "most_congested": "NSW1-QLD1", "avg_binding_hours": 1800, "yoy_change_pct": 12.5}}


@router.get("/api/sra/dashboard")
def sra_main_dashboard():
    _r.seed(7031)
    interconnectors_list = ["NSW1-QLD1", "VIC1-NSW1", "VIC1-SA1", "VIC1-TAS1"]
    ar = [{"auction_id": f"AUC-{i+1:04d}", "interconnector": ic, "direction": _r.choice(["Forward", "Counter"]), "quarter": f"Q{q}-2026", "clearing_price_aud": round(_r.uniform(2000, 80000), 0), "units_offered": _r.randint(50, 300), "units_sold": _r.randint(30, 250), "total_proceeds_m_aud": round(_r.uniform(0.5, 20), 1)} for i, (ic, q) in enumerate([(ic, q) for ic in interconnectors_list for q in [1, 2, 3, 4]])]
    units = [{"unit_id": f"SRU-{i+1:04d}", "interconnector": _r.choice(interconnectors_list), "direction": _r.choice(["Forward", "Counter"]), "holder": _r.choice(["AGL", "Origin", "Snowy Hydro", "Macquarie", "Shell"]), "acquisition_price_aud": round(_r.uniform(1000, 50000), 0), "current_value_aud": round(_r.uniform(800, 60000), 0), "quarter": f"Q1-2026"} for i in range(15)]
    icr = [{"interconnector": ic, "quarter": "Q4-2025", "total_residue_m_aud": round(_r.uniform(5, 60), 1), "distributed_m_aud": round(_r.uniform(4, 55), 1), "retained_m_aud": round(_r.uniform(0.5, 5), 1)} for ic in interconnectors_list]
    return {"timestamp": _dt.utcnow().isoformat() + "Z", "current_quarter": "Q4-2025", "total_sra_units_active": 850, "total_sra_revenue_this_quarter": 45000000, "best_performing_interconnector": "NSW1-QLD1", "total_residues_distributed_aud": 38000000, "auction_results": ar, "active_units": units, "interconnector_revenue": icr}


@router.get("/api/sra-analytics/dashboard")
def sraa_dashboard():
    _r.seed(7032)
    interconnectors_list = ["NSW1-QLD1", "VIC1-NSW1", "VIC1-SA1", "VIC1-TAS1"]
    ar = [{"auction_id": f"SRAA-{i+1:04d}", "interconnector": _r.choice(interconnectors_list), "quarter": f"Q{q}-{y}", "direction": _r.choice(["Forward", "Counter"]), "clearing_price_aud_per_unit": round(_r.uniform(1000, 60000), 0), "units_traded": _r.randint(20, 300), "participation_ratio_pct": round(_r.uniform(50, 95), 1)} for i in range(12) for q in [1, 2] for y in [2025, 2026]]
    holders = [{"holder_name": h, "holder_type": _r.choice(["Gentailer", "Financial", "Trader"]), "total_units": _r.randint(50, 500), "total_investment_m_aud": round(_r.uniform(5, 80), 1), "realized_return_pct": round(_r.uniform(-20, 50), 1)} for h in ["AGL", "Origin", "Macquarie", "Shell", "Snowy Hydro", "Alinta"]]
    residues = [{"interconnector": ic, "quarter": f"Q{q}-2025", "total_residue_m_aud": round(_r.uniform(5, 60), 1), "per_unit_residue_aud": round(_r.uniform(500, 40000), 0), "forecast_vs_actual_pct": round(_r.uniform(-30, 40), 1)} for ic in interconnectors_list for q in [3, 4]]
    ics = [{"interconnector": ic, "avg_utilization_pct": round(_r.uniform(40, 85), 1), "congestion_frequency_pct": round(_r.uniform(15, 55), 1), "sra_value_correlation": round(_r.uniform(0.4, 0.9), 2)} for ic in interconnectors_list]
    pb = [{"participant": p, "bidding_frequency": _r.randint(5, 50), "avg_bid_price_aud": round(_r.uniform(2000, 40000), 0), "success_rate_pct": round(_r.uniform(30, 85), 1)} for p in ["AGL", "Origin", "Macquarie", "Shell"]]
    return {"auction_results": ar, "holders": holders, "residues": residues, "interconnectors": ics, "participant_behaviour": pb, "summary": {"total_auction_proceeds_m_aud": 120, "avg_return_pct": 15.2, "most_active_participant": "AGL"}}


@router.get("/api/nem-settlement-residue-auction/dashboard")
def nsra_dashboard():
    _r.seed(7033)
    interconnectors_list = ["NSW1-QLD1", "VIC1-NSW1", "VIC1-SA1", "VIC1-TAS1"]
    auctions = [{"auction_id": f"NSRA-{i+1:04d}", "interconnector": ic, "quarter": f"Q{q}-2026", "auction_date": f"2025-{_r.randint(9,12):02d}-{_r.randint(1,28):02d}", "clearing_price_aud": round(_r.uniform(2000, 70000), 0), "units_available": _r.randint(50, 300), "units_sold": _r.randint(30, 280), "oversubscription_ratio": round(_r.uniform(0.8, 3.0), 1)} for i, (ic, q) in enumerate([(ic, q) for ic in interconnectors_list for q in [1, 2]])]
    holders = [{"holder_name": h, "total_units_held": _r.randint(30, 400), "interconnectors_count": _r.randint(1, 4), "total_value_m_aud": round(_r.uniform(5, 60), 1), "strategy": _r.choice(["Hedge", "Speculative", "Portfolio"])} for h in ["AGL", "Origin", "Macquarie", "Shell", "Snowy Hydro"]]
    flows = [{"interconnector": ic, "quarter": f"Q4-2025", "settlement_residue_m_aud": round(_r.uniform(5, 50), 1), "units_settled": _r.randint(50, 300), "per_unit_residue_aud": round(_r.uniform(500, 30000), 0)} for ic in interconnectors_list]
    ma = [{"quarter": f"Q{q}-2025", "total_auction_value_m_aud": round(_r.uniform(30, 150), 0), "participants_count": _r.randint(10, 30), "new_participants": _r.randint(0, 5)} for q in [1, 2, 3, 4]]
    icm = [{"interconnector": ic, "capacity_mw": _r.randint(500, 1500), "avg_utilization_pct": round(_r.uniform(40, 80), 1), "congestion_rent_correlation": round(_r.uniform(0.5, 0.9), 2), "forecast_accuracy_pct": round(_r.uniform(60, 90), 1)} for ic in interconnectors_list]
    return {"auctions": auctions, "sra_holders": holders, "settlement_residue_flows": flows, "market_activity": ma, "interconnector_metrics": icm, "summary": {"total_units_active": 1200, "total_market_value_m_aud": 95, "avg_clearing_price_aud": 25000, "quarterly_turnover_pct": 35}}


# =========================================================================
# BATCH: More endpoints (3)
# =========================================================================

@router.get("/api/carbon-price-pathway/dashboard")
def cpp_dashboard():
    _r.seed(7034)
    scenarios = [{"scenario_name": s, "year": y, "carbon_price_aud_t": round(_r.uniform(15, 80), 2), "emissions_mt": round(_r.uniform(50, 200), 0), "reduction_vs_2005_pct": round(_r.uniform(20, 65), 1), "policy_mechanism": _r.choice(["Safeguard Mechanism", "ETS", "Carbon Tax", "Baseline & Credit"])} for s in ["Net Zero 2050", "Current Policy", "Accelerated", "Delayed Action"] for y in [2025, 2028, 2030, 2035]]
    sf = [{"facility_name": f, "sector": sec, "baseline_mt": round(_r.uniform(0.1, 5), 2), "actual_emissions_mt": round(_r.uniform(0.08, 5.5), 2), "shortfall_mt": round(_r.uniform(-0.5, 1), 2), "compliance_status": _r.choice(["Compliant", "Shortfall", "Surrender Required"])} for f, sec in [("Eraring PS", "Electricity"), ("Bayswater PS", "Electricity"), ("Loy Yang A", "Electricity"), ("Tomago Aluminium", "Manufacturing"), ("BlueScope Steel", "Manufacturing"), ("Woodside NWS", "Oil & Gas")]]
    pt = [{"sector": sec, "carbon_cost_aud_mwh": round(_r.uniform(2, 20), 2), "passthrough_rate_pct": round(_r.uniform(50, 100), 1), "consumer_impact_aud_yr": round(_r.uniform(50, 300), 0)} for sec in ["Electricity", "Gas", "Transport", "Manufacturing"]]
    ab = [{"option": opt, "abatement_cost_aud_t": round(_r.uniform(10, 200), 0), "potential_mt": round(_r.uniform(1, 50), 0), "readiness": _r.choice(["Available", "Near-term", "Long-term"])} for opt in ["Renewables", "Energy Efficiency", "CCS", "Green Hydrogen", "Electrification", "Nature-based"]]
    return {"scenarios": scenarios, "safeguard_facilities": sf, "passthrough_records": pt, "abatement_options": ab, "summary": {"current_carbon_price_aud_t": 32.5, "safeguard_covered_mt": 140, "compliance_rate_pct": 88, "projected_2030_price_aud_t": 55}}


@router.get("/api/price-sensitivity/dashboard")
def epsa_dashboard():
    _r.seed(7035)
    regions = ["NSW1", "QLD1", "VIC1", "SA1"]
    drivers = ["Gas Price", "Coal Price", "Carbon Price", "Demand", "Wind Output", "Solar Output", "Interconnector", "Hydro Availability"]
    sens = [{"region": reg, "driver": d, "elasticity": round(_r.uniform(-2, 3), 2), "base_price_impact_aud_mwh": round(_r.uniform(-20, 40), 2), "r_squared": round(_r.uniform(0.1, 0.8), 2)} for reg in regions for d in drivers]
    scenarios = [{"scenario_name": s, "region": reg, "base_price_aud_mwh": round(_r.uniform(50, 120), 2), "scenario_price_aud_mwh": round(_r.uniform(30, 250), 2), "price_change_pct": round(_r.uniform(-40, 100), 1)} for s in ["High Gas", "Low Renewables", "Drought", "Demand Surge", "Carbon Increase"] for reg in regions[:2]]
    corr = [{"factor_a": fa, "factor_b": fb, "correlation": round(_r.uniform(-0.8, 0.9), 2), "significance": _r.choice(["High", "Medium", "Low"])} for fa, fb in [("Gas Price", "Electricity Price"), ("Wind Output", "Electricity Price"), ("Temperature", "Demand"), ("Solar Output", "Midday Price"), ("Coal Price", "Baseload Price")]]
    tr = [{"region": reg, "scenario": s, "tail_price_aud_mwh": round(_r.uniform(300, 5000), 0), "probability_pct": round(_r.uniform(0.5, 10), 1), "expected_loss_m_aud": round(_r.uniform(5, 200), 0)} for reg in regions[:3] for s in ["Extreme Heat", "Generator Failure", "Gas Supply Disruption"]]
    return {"sensitivity": sens, "scenarios": scenarios, "correlations": corr, "tail_risks": tr, "summary": {"most_sensitive_region": "SA1", "highest_elasticity_driver": "Gas Price", "avg_base_price_mwh": 85.5, "max_tail_risk_mwh": 4200}}


@router.get("/api/market-evolution-policy/dashboard")
def mepa_dashboard():
    _r.seed(7036)
    policies = [{"policy_id": f"POL-{i+1:03d}", "policy_name": p, "status": _r.choice(["Active", "Proposed", "Under Review"]), "implementation_date": f"202{_r.randint(4,7)}-{_r.randint(1,12):02d}-01", "impact_category": cat, "estimated_impact_m_aud": round(_r.uniform(50, 2000), 0)} for i, (p, cat) in enumerate([("Capacity Investment Scheme", "Investment"), ("Renewable Energy Target", "Generation"), ("Safeguard Mechanism", "Emissions"), ("Consumer Energy Resources", "Demand"), ("Transmission Planning", "Network"), ("ISP 2024", "Planning"), ("Battery Incentive Scheme", "Storage")])]
    mi = [{"indicator": ind, "year": y, "value": round(_r.uniform(10, 200), 1), "trend": _r.choice(["Increasing", "Decreasing", "Stable"]), "unit": u} for ind, u in [("Renewable Share", "%"), ("Wholesale Price", "AUD/MWh"), ("Emissions Intensity", "tCO2/MWh"), ("Storage Capacity", "GW"), ("DER Penetration", "%")] for y in [2023, 2024, 2025]]
    trans = [{"year": y, "coal_share_pct": round(_r.uniform(15, 45), 1), "gas_share_pct": round(_r.uniform(5, 15), 1), "renewable_share_pct": round(_r.uniform(30, 70), 1), "storage_share_pct": round(_r.uniform(2, 15), 1), "total_capacity_gw": round(_r.uniform(60, 90), 0)} for y in [2024, 2025, 2026, 2028, 2030]]
    consumer = [{"year": y, "avg_retail_price_c_kwh": round(_r.uniform(25, 45), 1), "solar_households_pct": round(_r.uniform(25, 50), 1), "battery_households_pct": round(_r.uniform(2, 20), 1), "ev_share_pct": round(_r.uniform(2, 25), 1), "demand_response_mw": round(_r.uniform(500, 3000), 0)} for y in [2024, 2025, 2026, 2028, 2030]]
    return {"policies": policies, "market_indicators": mi, "transition": trans, "consumer": consumer, "summary": {"renewable_share_current_pct": 38.5, "coal_retirement_year": 2038, "total_investment_pipeline_b_aud": 85, "consumer_savings_target_pct": 15, "policy_count_active": 5}}
