# Databricks notebook source
# =============================================================================
# 04_create_genie_spaces.py
# AUS Energy Copilot — Create Genie Spaces via Databricks SDK
#
# Creates 3 Genie Spaces for natural-language analytics over Gold tables:
#   1. NEM Prices & Demand
#   2. Generation & Fuel Mix
#   3. Interconnectors & Constraints
#
# Each space is configured with:
#   - Gold tables as data sources
#   - Semantic layer: table descriptions, column descriptions, glossary terms
#   - Sample questions / benchmark queries for accuracy validation
#   - Curated metrics definitions
#
# Prerequisites:
#   - Databricks SDK installed: pip install databricks-sdk
#   - Authenticated: DATABRICKS_HOST and DATABRICKS_TOKEN set, or use
#     profile-based auth: databricks auth login --host <workspace-url>
#   - Unity Catalog energy_copilot.gold schema populated (run 02_create_tables.sql first)
#   - Caller must have CAN MANAGE on the target workspace folder
#
# Usage (run as a Databricks notebook or locally with credentials):
#   databricks-connect run 04_create_genie_spaces.py
# =============================================================================

from __future__ import annotations

import json
import logging
from typing import Any

# Databricks SDK — install with: pip install databricks-sdk>=0.25.0
from databricks.sdk import WorkspaceClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration — update these values before running
# ---------------------------------------------------------------------------
CATALOG = "energy_copilot"
GOLD_SCHEMA = "gold"
WORKSPACE_FOLDER = "/energy_copilot/genie_spaces"  # Workspace folder path

# Databricks workspace client (auto-detects credentials from env / .databrickscfg)
w = WorkspaceClient()


# ---------------------------------------------------------------------------
# Helper: build a fully qualified table reference for Genie
# ---------------------------------------------------------------------------
def fq(table_name: str) -> str:
    """Return fully qualified Unity Catalog table name."""
    return f"{CATALOG}.{GOLD_SCHEMA}.{table_name}"


# ---------------------------------------------------------------------------
# Semantic layer helpers
# ---------------------------------------------------------------------------

def make_table_config(
    table_name: str,
    description: str,
    column_descriptions: dict[str, str],
) -> dict[str, Any]:
    """
    Build a Genie table configuration dict with semantic metadata.

    Args:
        table_name: Unqualified Gold table name.
        description: Plain-English description of the table for Genie.
        column_descriptions: Mapping of column_name -> human-readable description.

    Returns:
        Dict conforming to the Genie SDK table config schema.
    """
    return {
        "table_name": fq(table_name),
        "description": description,
        "columns": [
            {"name": col, "description": desc}
            for col, desc in column_descriptions.items()
        ],
    }


# =============================================================================
# GENIE SPACE 1: NEM Prices & Demand
# Tables: nem_prices_5min, nem_prices_30min, demand_actuals, price_forecasts,
#         demand_forecasts, anomaly_events, nem_daily_summary
# =============================================================================

PRICES_DEMAND_TABLES = [
    make_table_config(
        "nem_prices_5min",
        description=(
            "5-minute NEM spot electricity prices per region. "
            "The primary table for price queries. "
            "RRP is the Regional Reference Price — the spot price paid for electricity. "
            "Normal prices range from -$1,000 to $500/MWh; spikes can reach the market price cap of $15,500/MWh. "
            "Covers NSW1, QLD1, VIC1, SA1, and TAS1 at 5-minute resolution."
        ),
        column_descriptions={
            "interval_datetime": "Dispatch interval end time (AEST/AEDT, Australia/Sydney timezone). Primary time axis.",
            "region_id": "NEM region code: NSW1=New South Wales, QLD1=Queensland, VIC1=Victoria, SA1=South Australia, TAS1=Tasmania.",
            "rrp": "Regional Reference Price — spot electricity price ($/MWh). Range: -$1,000 to $15,500/MWh.",
            "total_demand_mw": "Total scheduled electricity demand in region (megawatts).",
            "net_interchange_mw": "Net interconnector flow into region (MW). Positive=importing from other regions, Negative=exporting.",
            "apc_flag": "Administered Price Cap active — True means AEMO has capped prices below the market cap.",
            "market_suspended": "True if NEM market is suspended (rare event indicating system emergency).",
        },
    ),
    make_table_config(
        "nem_prices_30min",
        description=(
            "30-minute NEM settlement prices per region. "
            "These are the prices actually used for energy billing — average of 6 dispatch intervals. "
            "Includes volatility statistics (min, max, std dev) and spike counts within each trading interval."
        ),
        column_descriptions={
            "interval_datetime": "Trading interval end time (30-minute resolution, AEST/AEDT).",
            "region_id": "NEM region: NSW1, QLD1, VIC1, SA1, TAS1.",
            "rrp": "Settlement price — average of 6 dispatch interval prices ($/MWh). This is the billing price.",
            "rrp_min": "Lowest 5-min price within the trading interval ($/MWh).",
            "rrp_max": "Highest 5-min price within the trading interval ($/MWh). Key spike indicator.",
            "rrp_stddev": "Standard deviation of 5-min prices within the interval — measure of price volatility.",
            "spike_count": "Number of 5-min intervals within the trading period where price exceeded $300/MWh.",
            "total_demand_mw": "Average demand over the 30-minute trading interval (MW).",
        },
    ),
    make_table_config(
        "demand_actuals",
        description=(
            "Actual electricity demand per NEM region at 5-minute resolution. "
            "Net demand (total minus rooftop solar) is the key figure for dispatchable generation requirements. "
            "Temperature is included as the primary demand driver."
        ),
        column_descriptions={
            "interval_datetime": "Dispatch interval end time (AEST/AEDT).",
            "region_id": "NEM region: NSW1, QLD1, VIC1, SA1, TAS1.",
            "total_demand_mw": "Total scheduled electricity demand (MW). Includes all scheduled loads.",
            "net_demand_mw": "Net demand = total demand minus rooftop solar output (MW). Demand that dispatchable generators must serve.",
            "solar_rooftop_mw": "Estimated rooftop solar contribution (MW). Reduces net demand particularly at midday.",
            "temperature_c": "Region representative temperature (°C). Primary demand driver — high temperatures increase cooling demand.",
            "is_peak": "True for morning peak (07:00–09:00 AEST) and evening peak (17:00–20:00 AEST) periods.",
        },
    ),
    make_table_config(
        "price_forecasts",
        description=(
            "Machine learning price forecasts per NEM region at multiple horizons (1-hour, 4-hour, 24-hour ahead). "
            "LightGBM models retrained weekly. "
            "spike_probability is the key field for predictive price spike alerting."
        ),
        column_descriptions={
            "interval_datetime": "Target interval being forecast (AEST/AEDT).",
            "region_id": "NEM region being forecast: NSW1, QLD1, VIC1, SA1, TAS1.",
            "horizon_intervals": "Forecast horizon in 5-minute intervals (12=1hr, 48=4hr, 288=24hr).",
            "predicted_rrp": "ML model predicted spot price ($/MWh).",
            "prediction_lower_80": "Lower bound of 80% prediction interval ($/MWh) — optimistic scenario.",
            "prediction_upper_80": "Upper bound of 80% prediction interval ($/MWh) — pessimistic scenario.",
            "spike_probability": "Probability of a price spike (>$300/MWh) in this interval (0=no risk, 1=certain spike).",
            "forecast_run_at": "UTC time when the ML pipeline generated this forecast.",
        },
    ),
    make_table_config(
        "demand_forecasts",
        description=(
            "Machine learning demand forecasts per NEM region at multiple horizons. "
            "Temperature and weather forecasts are the primary inputs."
        ),
        column_descriptions={
            "interval_datetime": "Target forecast interval (AEST/AEDT).",
            "region_id": "NEM region: NSW1, QLD1, VIC1, SA1, TAS1.",
            "horizon_intervals": "Forecast horizon in 5-minute intervals (12=1hr, 48=4hr, 288=24hr).",
            "predicted_demand_mw": "ML model predicted total scheduled demand (MW).",
            "prediction_lower_80": "Lower bound of 80% prediction interval (MW).",
            "prediction_upper_80": "Upper bound of 80% prediction interval (MW).",
        },
    ),
    make_table_config(
        "nem_daily_summary",
        description=(
            "Daily NEM market summary per region — one row per region per trading day. "
            "Includes average/min/max prices, demand, fuel mix percentages, and emissions intensity. "
            "The renewables_pct and avg_emissions_intensity fields track Australia's energy transition progress."
        ),
        column_descriptions={
            "trading_date": "NEM trading date (AEST). Market day runs from 04:05 to 04:00 next day.",
            "region_id": "NEM region: NSW1, QLD1, VIC1, SA1, TAS1.",
            "avg_price_aud_mwh": "Volume-weighted average price for the day ($/MWh). Headline daily price metric.",
            "max_price_aud_mwh": "Maximum 5-minute price in the day ($/MWh). Key spike severity indicator.",
            "price_spike_count": "Number of 5-minute intervals with price above $300/MWh.",
            "peak_demand_mw": "Maximum demand in the trading day (MW). Used for capacity planning.",
            "renewables_pct": "Renewables share (wind + solar + hydro) of generation (%). Decarbonisation tracker.",
            "avg_emissions_intensity": "Average grid emissions intensity (tonne CO2e per MWh). Lower is greener.",
        },
    ),
    make_table_config(
        "anomaly_events",
        description=(
            "Detected market anomalies: price spikes, demand surges, data quality issues, and model drift. "
            "Use this table to find when and where notable market events occurred."
        ),
        column_descriptions={
            "detected_at": "UTC time when anomaly was first detected.",
            "interval_datetime": "NEM interval associated with the event (AEST/AEDT).",
            "region_id": "Affected NEM region (NULL if NEM-wide).",
            "event_type": "Anomaly type: PRICE_SPIKE, PRICE_NEGATIVE, DEMAND_SURGE, DEMAND_DROP, DATA_MISSING, DATA_STALE, MODEL_DRIFT.",
            "severity": "Event severity level: LOW, MEDIUM, HIGH, or CRITICAL.",
            "metric_value": "Observed value that triggered the anomaly ($/MWh for price, MW for demand).",
            "description": "Human-readable description of what happened.",
            "is_resolved": "True if the anomaly condition has returned to normal.",
        },
    ),
]

PRICES_DEMAND_SAMPLE_QUESTIONS = [
    "What is the current electricity price in each NEM region?",
    "Show me the highest prices in NSW over the last 7 days",
    "How many price spikes (>$300/MWh) has Victoria had this month?",
    "Compare average daily prices across all regions for the last 30 days",
    "What was the peak demand in Queensland yesterday?",
    "Show net demand vs total demand for South Australia last week",
    "What is the ML price forecast for NSW for the next 4 hours?",
    "Which region had the highest average price last week?",
    "How has SA electricity price changed over the past 24 hours?",
    "Show me all price spikes above $1000/MWh in the last 30 days",
    "What was the average price during the morning peak (7am-9am) vs evening peak (5pm-8pm) for VIC?",
    "How many negative price intervals occurred in each region yesterday?",
    "What is the renewables percentage for each region today?",
    "Show the correlation between temperature and demand in NSW for the past month",
    "What was the worst price spike event in the past 90 days?",
]

PRICES_DEMAND_GLOSSARY = {
    "RRP": "Regional Reference Price — the spot electricity price per MWh for a 5-minute dispatch interval. The primary NEM price metric.",
    "Trading Interval": "30-minute settlement period in the NEM. 6 dispatch intervals per trading interval. Settlement prices are the average of the 6 dispatch interval prices.",
    "Dispatch Interval": "5-minute interval in which AEMO dispatches generators and sets prices.",
    "Market Price Cap (MPC)": "Maximum price that can be set in the NEM: $15,500/MWh. Applied to prevent price spikes that exceed the physical cost of supply.",
    "Administered Price Cap (APC)": "Temporary cap applied by AEMO when cumulative prices over 7 days exceed the cumulative price threshold ($1,359,100/MWh over 336 intervals).",
    "VWAP": "Volume-Weighted Average Price — average price weighted by demand volume. More representative than simple average.",
    "Net Demand": "Total demand minus rooftop solar generation. Represents the demand that must be met by scheduled and semi-scheduled generators.",
    "NEM": "National Electricity Market — the Australian electricity market covering NSW, VIC, QLD, SA, and TAS.",
    "AEMO": "Australian Energy Market Operator — the body that operates the NEM dispatch engine.",
    "MWh": "Megawatt-hour — unit of electrical energy. Price expressed as $/MWh.",
    "MW": "Megawatt — unit of electrical power. Demand and generation expressed in MW.",
}


# =============================================================================
# GENIE SPACE 2: Generation & Fuel Mix
# Tables: nem_generation_by_fuel, weather_nem_regions, generation_forecasts,
#         nem_daily_summary
# =============================================================================

GENERATION_TABLES = [
    make_table_config(
        "nem_generation_by_fuel",
        description=(
            "NEM generation output by fuel type and region at 5-minute resolution. "
            "Covers all fuel technologies: coal (black and brown), gas (CCGT, OCGT, steam), "
            "hydro, wind, utility-scale solar, rooftop solar, battery storage, and other. "
            "Key table for generation mix analysis and fuel stack charts."
        ),
        column_descriptions={
            "interval_datetime": "Dispatch interval end time (AEST/AEDT). 5-minute resolution.",
            "region_id": "NEM region: NSW1, QLD1, VIC1, SA1, TAS1.",
            "fuel_type": (
                "Fuel/technology type: coal_black, coal_brown, gas_ccgt, gas_ocgt, gas_steam, "
                "hydro, wind, solar_utility, solar_rooftop, battery_charging, battery_discharging, diesel, other."
            ),
            "total_mw": "Total generation output for this fuel type in the region and interval (MW).",
            "unit_count": "Number of generating units of this fuel type dispatched in this interval.",
            "capacity_factor": "Capacity factor — actual output / installed capacity (0 to 1). Higher = more efficient use of installed capacity.",
            "emissions_tco2e": "CO2-equivalent emissions from this fuel type in this interval (tonnes).",
            "emissions_intensity": "Emissions intensity for this fuel type (tonne CO2e per MWh).",
        },
    ),
    make_table_config(
        "weather_nem_regions",
        description=(
            "Weather data and forecasts at NEM region level. "
            "Covers temperature, wind speed, solar radiation, and cloud cover. "
            "Key driver features for wind and solar generation forecasting. "
            "Includes both historical actuals and 7-day Open-Meteo forecasts."
        ),
        column_descriptions={
            "forecast_datetime": "Forecast target datetime (AEST/AEDT).",
            "nem_region": "NEM region: NSW1, QLD1, VIC1, SA1, TAS1.",
            "is_historical": "True = historical observed actuals. False = Open-Meteo forecast.",
            "temperature_c": "Population-weighted average temperature in region (°C). Primary demand driver.",
            "wind_speed_100m_kmh": "Average wind speed at 100m height (km/h). Primary wind generation proxy.",
            "solar_radiation_wm2": "Global horizontal irradiance (W/m²). Primary solar generation proxy.",
            "cloud_cover_pct": "Cloud cover percentage (0–100%). Higher cloud cover reduces solar generation.",
            "cooling_degree_days": "Cooling degree days (base 18°C) — proxy for air conditioning demand.",
            "heating_degree_days": "Heating degree days (base 18°C) — proxy for heating demand.",
        },
    ),
    make_table_config(
        "generation_forecasts",
        description=(
            "ML forecasts of wind and utility-scale solar generation per region at multiple horizons. "
            "Driven by Open-Meteo weather forecasts. "
            "Used to predict variable renewable generation changes."
        ),
        column_descriptions={
            "interval_datetime": "Target forecast interval (AEST/AEDT).",
            "region_id": "NEM region: NSW1, QLD1, VIC1, SA1, TAS1.",
            "fuel_type": "Generation type being forecast: wind or solar_utility.",
            "horizon_intervals": "Forecast horizon in 5-minute intervals (12=1hr, 48=4hr, 288=24hr).",
            "predicted_mw": "ML model predicted generation output (MW).",
            "prediction_lower_80": "Lower bound of 80% prediction interval (MW).",
            "prediction_upper_80": "Upper bound of 80% prediction interval (MW).",
        },
    ),
    make_table_config(
        "nem_daily_summary",
        description=(
            "Daily NEM market summary including generation fuel mix percentages and emissions intensity. "
            "Useful for trend analysis of the energy transition."
        ),
        column_descriptions={
            "trading_date": "NEM trading date (AEST).",
            "region_id": "NEM region: NSW1, QLD1, VIC1, SA1, TAS1.",
            "coal_pct": "Coal generation share (black + brown coal combined) for the day (%).",
            "gas_pct": "Gas generation share (CCGT + OCGT + steam combined) for the day (%).",
            "wind_pct": "Wind generation share (%) for the day.",
            "solar_utility_pct": "Utility-scale solar share (%) for the day.",
            "solar_rooftop_pct": "Rooftop solar share (%) for the day.",
            "hydro_pct": "Hydro generation share (%) for the day.",
            "renewables_pct": "Total renewables = wind + solar (utility + rooftop) + hydro (%). Key decarbonisation metric.",
            "avg_emissions_intensity": "Average grid emissions intensity for the day (tonne CO2e per MWh). Lower is greener.",
        },
    ),
]

GENERATION_SAMPLE_QUESTIONS = [
    "What is the current generation mix in New South Wales?",
    "How much wind power is being generated across all NEM regions right now?",
    "What percentage of Queensland's electricity comes from renewables today?",
    "Show the fuel mix trend for South Australia over the past 7 days",
    "Compare solar generation across all regions for this month",
    "What was the highest wind output day in Victoria in the last 90 days?",
    "Show battery charging vs discharging in SA over the last 24 hours",
    "How has the coal generation share changed in NSW over the last 12 months?",
    "What is the average capacity factor for wind in Queensland?",
    "Show total NEM CO2 emissions by fuel type for today",
    "When was the last time renewables provided more than 80% of South Australia's generation?",
    "Compare the current wind forecast vs actual generation in VIC",
    "What is the wind generation forecast for NSW for the next 4 hours?",
    "How much rooftop solar is being generated in Queensland right now?",
    "What fuel type produces the most emissions per MWh in the NEM?",
]

GENERATION_GLOSSARY = {
    "CCGT": "Combined Cycle Gas Turbine — most efficient gas generation technology. High capital cost, lower operating cost.",
    "OCGT": "Open Cycle Gas Turbine — fast-start peaking gas plant. Higher emissions intensity than CCGT. Used to meet peak demand.",
    "VRE": "Variable Renewable Energy — generation types whose output varies with weather (wind and solar).",
    "Semi-scheduled": "A dispatch category for VRE generators. AEMO can curtail output if needed but cannot increase it above available resource.",
    "Capacity Factor": "Ratio of actual energy generated to maximum possible energy if operating at full capacity continuously. Wind ~35%, utility solar ~25%, baseload coal ~70-80%.",
    "GW": "Gigawatt — 1,000 megawatts. NEM total capacity approximately 55 GW.",
    "GWh": "Gigawatt-hour — unit of energy. Australian annual electricity consumption approximately 200 TWh (200,000 GWh).",
    "Emissions Intensity": "Grams of CO2-equivalent per kilowatt-hour (gCO2e/kWh) or tonnes per MWh. NEM grid average approximately 0.7 t/MWh (declining as renewables grow).",
    "Rooftop Solar": "Distributed photovoltaic systems on residential and commercial rooftops. Not dispatched by AEMO but subtracted from net demand.",
    "Pumped Hydro": "Hydroelectric storage — pumps water uphill when prices are low, generates when prices are high. Largest form of grid-scale storage in Australia.",
}


# =============================================================================
# GENIE SPACE 3: Interconnectors & Constraints
# Tables: nem_interconnectors, nem_constraints_active, nem_prices_5min
# =============================================================================

INTERCONNECTORS_TABLES = [
    make_table_config(
        "nem_interconnectors",
        description=(
            "NEM interconnector flows and utilisation at 5-minute resolution. "
            "Interconnectors are the high-voltage transmission links between NEM regions. "
            "The 5 main interconnectors are: NSW1-QLD1 (QNI), VIC1-NSW1 (VNI), "
            "VIC1-SA1 (Heywood), VIC1-TAS1 (Basslink), SA1-VIC1 (Murraylink). "
            "Congestion occurs when an interconnector reaches its physical limit."
        ),
        column_descriptions={
            "interval_datetime": "Dispatch interval end time (AEST/AEDT).",
            "interconnector_id": "Interconnector ID: NSW1-QLD1 (QNI north-south link), VIC1-NSW1 (VNI), VIC1-SA1 (Heywood 600MW AC link), VIC1-TAS1 (Basslink 500MW HVDC undersea cable), SA1-VIC1 (Murraylink 220MW HVDC).",
            "from_region": "Region that exports in the forward (positive) direction.",
            "to_region": "Region that receives in the forward (positive) direction.",
            "mw_flow": "Power flow in MW. Positive = from_region to to_region. Negative = reverse direction.",
            "mw_losses": "Transmission losses on the interconnector (MW).",
            "export_limit_mw": "Forward direction physical capacity limit (MW).",
            "import_limit_mw": "Reverse direction physical capacity limit (MW, shown as positive).",
            "utilization_pct": "Utilisation percentage: abs(flow) / applicable_limit * 100. Values above 95% indicate congestion.",
            "is_congested": "True if the interconnector is at or near its physical limit (utilisation > 95%).",
            "marginal_value": "Shadow price of the interconnector constraint ($/MWh). Non-zero when congested. High values mean high value of additional transmission capacity.",
        },
    ),
    make_table_config(
        "nem_constraints_active",
        description=(
            "Binding generic constraints per dispatch interval — only constraints that are actively limiting dispatch. "
            "Constraints represent network, thermal, voltage, and stability limits. "
            "Non-zero marginal_value means the constraint is binding and affecting prices. "
            "Often correlated with interconnector congestion and regional price separations."
        ),
        column_descriptions={
            "interval_datetime": "Dispatch interval end time (AEST/AEDT).",
            "constraint_id": "AEMO generic constraint equation identifier (e.g. N^^NIL_FCSPS, V_MSLOAD_LIM). Each ID represents a specific network or system security limit.",
            "marginal_value": "Shadow price of the binding constraint ($/MWh). Higher values mean the constraint is more costly to the market.",
            "violation_degree": "Constraint violation in MW. Non-zero values indicate the constraint was not fully satisfied (rare under normal operation).",
            "rhs": "Right-hand side limit of the constraint equation (MW or other units).",
            "affected_regions": "NEM regions affected by this constraint (array of region codes).",
            "constraint_type": "Constraint category: NETWORK (transmission), FCAS (frequency), VOLTAGE, THERMAL, or STABILITY.",
        },
    ),
    make_table_config(
        "nem_prices_5min",
        description=(
            "5-minute spot prices — included in this space to correlate price separations "
            "between regions with interconnector congestion and binding constraints."
        ),
        column_descriptions={
            "interval_datetime": "Dispatch interval end time (AEST/AEDT).",
            "region_id": "NEM region: NSW1, QLD1, VIC1, SA1, TAS1.",
            "rrp": "Regional Reference Price ($/MWh). Price differences between regions indicate interconnector congestion.",
            "net_interchange_mw": "Net interconnector import into region (MW). Positive = net importing region.",
        },
    ),
]

INTERCONNECTORS_SAMPLE_QUESTIONS = [
    "What is the current flow on the Basslink interconnector between VIC and TAS?",
    "Is any interconnector currently congested?",
    "Show the Heywood interconnector (VIC-SA) flow over the last 24 hours",
    "How often has QNI (NSW-QLD) been at its export limit this month?",
    "What constraints are currently binding in the NEM?",
    "Show the price difference between NSW and QLD over the last week — when does it correlate with QNI congestion?",
    "How many hours per day is Basslink congested on average this year?",
    "List the top 10 most frequently binding constraints in the last 30 days",
    "What was the maximum VIC1-SA1 interconnector flow last summer (Dec-Feb)?",
    "Show me intervals where SA had the highest price in the NEM — which constraints were binding?",
    "What is the average utilisation of each interconnector over the last 7 days?",
    "During yesterday's price spike in SA, what was the Heywood (VIC-SA1) flow?",
    "Which interconnector has the highest average marginal value (most congested) this month?",
    "Show all constraint violations (violation_degree > 0) in the last 7 days",
    "Compare interconnector utilisation during peak (17:00–20:00) vs off-peak hours",
]

INTERCONNECTORS_GLOSSARY = {
    "Interconnector": "High-voltage transmission line connecting two NEM regions. Enables energy trading between states.",
    "Congestion": "Condition when an interconnector reaches its physical capacity limit. Causes prices to separate between connected regions.",
    "Shadow Price": "The marginal value of relaxing a constraint by 1 MW — indicates how much additional capacity is worth to the market ($/MWh).",
    "QNI": "Queensland-NSW Interconnector — the main north-south link. Forward direction is from NSW to QLD.",
    "VNI": "Victoria-NSW Interconnector. Forward direction is VIC to NSW.",
    "Heywood": "The VIC-SA1 interconnector (AC link, ~600 MW capacity). Primary connection between South Australia and eastern states.",
    "Basslink": "The VIC-TAS1 interconnector — a 500 MW HVDC undersea cable under Bass Strait. Only connection between Tasmania and the mainland.",
    "Murraylink": "SA1-VIC1 HVDC interconnector (~220 MW). Secondary SA-VIC connection supplementing Heywood.",
    "Generic Constraint": "A linear inequality that represents a network or system security limit in AEMO's dispatch engine. When binding, it restricts generator outputs and affects prices.",
    "Marginal Value": "For a binding constraint: the $/MWh reduction in market cost if the constraint limit were relaxed by 1 MW. Non-zero only when binding.",
    "Price Separation": "When two connected regions have different spot prices, indicating congestion on the interconnector between them.",
    "Binding Constraint": "A constraint that is at its limit and actively affecting dispatch outcomes. Identified by a non-zero marginal value.",
}


# =============================================================================
# Space creation function
# =============================================================================

def create_genie_space(
    name: str,
    description: str,
    tables: list[dict[str, Any]],
    sample_questions: list[str],
    glossary: dict[str, str],
    folder_path: str,
) -> str:
    """
    Create a Genie Space using the Databricks SDK.

    Args:
        name: Display name for the Genie Space.
        description: Plain-English description of the space's purpose.
        tables: List of table config dicts (from make_table_config).
        sample_questions: List of benchmark/sample questions for the space.
        glossary: Dict of term -> definition for the space semantic layer.
        folder_path: Workspace folder path where the space will be created.

    Returns:
        Space ID of the created (or updated) Genie Space.

    Notes:
        - Databricks Genie Spaces API is in preview; SDK method names may change.
        - This stub uses the pattern available in databricks-sdk >= 0.25.0.
        - If the SDK does not yet expose GenieSpaces, use the REST API directly:
          POST /api/2.0/genie/spaces
    """
    log.info("Creating Genie Space: %s", name)

    # Build the semantic layer configuration
    semantic_layer = {
        "tables": tables,
        "glossary": [
            {"term": term, "definition": defn}
            for term, defn in glossary.items()
        ],
        "sample_questions": sample_questions,
    }

    # Attempt creation via SDK (method available in preview SDK versions)
    # If this fails, fall back to direct REST API call below.
    try:
        space = w.genie.create_space(
            display_name=name,
            description=description,
            warehouse_id=_get_or_create_warehouse(),
            table_identifiers=[t["table_name"] for t in tables],
            # Note: full semantic layer config (column descriptions, glossary, sample questions)
            # is set via a subsequent update_space call once the API stabilises.
            # For now, semantic metadata is declared in the table comments (02_create_tables.sql).
        )
        space_id = space.space_id
        log.info("Created Genie Space '%s' with ID: %s", name, space_id)

        # Patch semantic layer via update (SDK preview pattern)
        # TODO: replace with w.genie.update_space(...) when fully available in SDK
        w.api_client.do(
            "PATCH",
            f"/api/2.0/genie/spaces/{space_id}/semantic-layer",
            body=semantic_layer,
        )
        log.info("Semantic layer applied to space: %s", space_id)
        return space_id

    except AttributeError:
        # Genie spaces API not yet available in installed SDK version.
        # Fall back to direct REST API.
        log.warning(
            "w.genie.create_space not available in SDK version %s. "
            "Using direct REST API.",
            getattr(w, "_sdk_version", "unknown"),
        )
        return _create_space_via_rest(
            name=name,
            description=description,
            semantic_layer=semantic_layer,
            folder_path=folder_path,
        )


def _get_or_create_warehouse() -> str:
    """
    Return the warehouse ID to attach to the Genie Space.
    Looks for an existing Serverless warehouse; creates one if not found.

    Returns:
        Databricks SQL warehouse ID string.
    """
    warehouses = list(w.warehouses.list())
    for wh in warehouses:
        if (
            wh.name
            and "energy_copilot" in wh.name.lower()
            and wh.warehouse_type
            and wh.warehouse_type.value == "PRO"
        ):
            log.info("Using existing warehouse: %s (%s)", wh.name, wh.id)
            return wh.id

    # Create a new serverless warehouse for Genie Spaces
    log.info("Creating new Serverless SQL warehouse for Genie Spaces...")
    from databricks.sdk.service.sql import SpotInstancePolicy
    wh = w.warehouses.create(
        name="energy_copilot_genie",
        cluster_size="Small",
        min_num_clusters=1,
        max_num_clusters=1,
        auto_stop_mins=15,
        spot_instance_policy=SpotInstancePolicy.COST_OPTIMIZED,
        enable_serverless_compute=True,
    )
    log.info("Created warehouse: %s", wh.id)
    return wh.id


def _create_space_via_rest(
    name: str,
    description: str,
    semantic_layer: dict[str, Any],
    folder_path: str,
) -> str:
    """
    Create Genie Space via direct REST API (fallback when SDK method unavailable).

    Returns:
        Space ID of the created space.
    """
    response = w.api_client.do(
        "POST",
        "/api/2.0/genie/spaces",
        body={
            "display_name": name,
            "description": description,
            "warehouse_id": _get_or_create_warehouse(),
            "parent_path": folder_path,
            "tables": [t["table_name"] for t in semantic_layer["tables"]],
        },
    )
    space_id = response["space_id"]
    log.info("Created space via REST API: %s -> %s", name, space_id)

    # Apply semantic layer
    w.api_client.do(
        "PATCH",
        f"/api/2.0/genie/spaces/{space_id}/semantic-layer",
        body=semantic_layer,
    )
    return space_id


# =============================================================================
# Main: create all 3 spaces
# =============================================================================

def main() -> None:
    """Create all 3 AUS Energy Copilot Genie Spaces."""

    log.info("Starting Genie Space creation for AUS Energy Copilot...")
    log.info("Workspace: %s", w.config.host)

    # Ensure workspace folder exists
    try:
        w.workspace.mkdirs(WORKSPACE_FOLDER)
        log.info("Workspace folder ready: %s", WORKSPACE_FOLDER)
    except Exception as exc:
        log.warning("Could not create workspace folder %s: %s", WORKSPACE_FOLDER, exc)

    # Space 1: NEM Prices & Demand
    space1_id = create_genie_space(
        name="NEM Prices & Demand",
        description=(
            "Natural-language analytics for Australian NEM electricity prices and demand. "
            "Ask questions about 5-minute spot prices, 30-minute settlement prices, "
            "actual and forecast demand, price spikes, negative prices, and daily market summaries. "
            "Covers all 5 NEM regions: NSW, QLD, VIC, SA, and TAS."
        ),
        tables=PRICES_DEMAND_TABLES,
        sample_questions=PRICES_DEMAND_SAMPLE_QUESTIONS,
        glossary=PRICES_DEMAND_GLOSSARY,
        folder_path=WORKSPACE_FOLDER,
    )

    # Space 2: Generation & Fuel Mix
    space2_id = create_genie_space(
        name="Generation & Fuel Mix",
        description=(
            "Natural-language analytics for NEM electricity generation by fuel type. "
            "Ask questions about coal, gas, wind, solar, hydro, and battery generation. "
            "Includes generation forecasts driven by weather data, capacity factors, "
            "emissions intensity, and Australia's energy transition metrics."
        ),
        tables=GENERATION_TABLES,
        sample_questions=GENERATION_SAMPLE_QUESTIONS,
        glossary=GENERATION_GLOSSARY,
        folder_path=WORKSPACE_FOLDER,
    )

    # Space 3: Interconnectors & Constraints
    space3_id = create_genie_space(
        name="Interconnectors & Constraints",
        description=(
            "Natural-language analytics for NEM interconnector flows and binding network constraints. "
            "Ask questions about interstate power flows, interconnector congestion, binding constraints, "
            "price separations between regions, and shadow prices. "
            "Covers QNI (NSW-QLD), VNI (VIC-NSW), Heywood (VIC-SA), Basslink (VIC-TAS), and Murraylink (SA-VIC)."
        ),
        tables=INTERCONNECTORS_TABLES,
        sample_questions=INTERCONNECTORS_SAMPLE_QUESTIONS,
        glossary=INTERCONNECTORS_GLOSSARY,
        folder_path=WORKSPACE_FOLDER,
    )

    log.info("All 3 Genie Spaces created successfully:")
    log.info("  1. NEM Prices & Demand:          %s", space1_id)
    log.info("  2. Generation & Fuel Mix:         %s", space2_id)
    log.info("  3. Interconnectors & Constraints: %s", space3_id)

    # Persist space IDs to workspace for reference
    space_registry = {
        "nem_prices_demand": space1_id,
        "generation_fuel_mix": space2_id,
        "interconnectors_constraints": space3_id,
    }
    registry_path = f"{WORKSPACE_FOLDER}/space_ids.json"
    try:
        import io
        w.workspace.upload(
            path=registry_path,
            content=io.BytesIO(json.dumps(space_registry, indent=2).encode()),
            overwrite=True,
        )
        log.info("Space ID registry written to: %s", registry_path)
    except Exception as exc:
        log.warning("Could not write space ID registry: %s", exc)
        log.info("Space IDs: %s", json.dumps(space_registry, indent=2))


if __name__ == "__main__":
    main()
