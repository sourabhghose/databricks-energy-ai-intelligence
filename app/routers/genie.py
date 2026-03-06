from __future__ import annotations
import json
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter()

# =========================================================================
# Genie AI/BI Space proxy endpoints
# =========================================================================

_GENIE_SPACES = [
    {
        "space_id": "01f1151e4cea13f3abdca55d4894e777",
        "title": "NEM Spot Market Intelligence",
        "description": "Spot prices, demand, price spikes, negative pricing, and anomaly events across all NEM regions.",
        "icon": "zap",
        "tables": ["nem_prices_5min", "nem_prices_30min", "nem_daily_summary", "daily_market_summary", "anomaly_events", "demand_actuals"],
        "question_categories": [
            {
                "label": "Spot Prices",
                "questions": [
                    "What was the average spot price by region last week?",
                    "Show me all price spike events above $1000/MWh",
                    "What is the daily price volatility trend for SA1?",
                    "Compare average spot prices between NSW1 and VIC1 this month",
                    "What was the highest price recorded across all regions today?",
                    "Show hourly average prices for QLD1 over the past 7 days",
                    "What is the price spread between the cheapest and most expensive region right now?",
                ],
            },
            {
                "label": "Negative Pricing",
                "questions": [
                    "Which region had the most negative pricing intervals?",
                    "How many hours of negative pricing occurred in SA1 last month?",
                    "Show me all negative price events in the last 30 days by region",
                    "What time of day do negative prices most commonly occur?",
                ],
            },
            {
                "label": "Demand",
                "questions": [
                    "Show peak demand by region for the last 30 days",
                    "What was the total NEM demand today compared to yesterday?",
                    "Which region has the highest demand right now?",
                    "Show the demand pattern for VIC1 over the past week",
                    "When did NSW1 last exceed 12,000 MW demand?",
                ],
            },
            {
                "label": "Anomalies & Events",
                "questions": [
                    "Show all CRITICAL anomaly events in the last 7 days",
                    "How many anomaly events occurred by severity level this month?",
                    "What regions have the most frequent market anomalies?",
                    "List the top 10 most recent anomaly events with details",
                ],
            },
        ],
    },
    {
        "space_id": "01f1151e5c7316948adc16d6623ad6c1",
        "title": "NEM Generation & Renewables",
        "description": "Generation by fuel type, renewable penetration, emissions intensity, and capacity factors.",
        "icon": "sun",
        "tables": ["nem_generation_by_fuel", "nem_daily_summary", "generation_forecasts"],
        "question_categories": [
            {
                "label": "Renewables",
                "questions": [
                    "What is the current renewable energy share by region?",
                    "Show the daily renewables percentage trend over the last month",
                    "Which region has the highest solar generation share?",
                    "How has wind generation changed month-over-month?",
                    "What is the combined wind and solar share across the NEM today?",
                    "When did renewables last exceed 60% of total generation?",
                ],
            },
            {
                "label": "Generation Mix",
                "questions": [
                    "Show the generation mix for NSW1 over the last month",
                    "Compare the fuel mix between VIC1 and QLD1",
                    "How much coal vs gas is running in each region right now?",
                    "What is the total NEM generation output by fuel type?",
                    "Show the generation mix breakdown for SA1 today",
                ],
            },
            {
                "label": "Emissions & Efficiency",
                "questions": [
                    "Which fuel type has the highest emissions intensity?",
                    "What is the total emissions by region over the last 7 days?",
                    "Show average capacity factors by fuel type and region",
                    "Which generators have the lowest capacity factor?",
                    "How do emissions intensity compare between NSW1 and VIC1?",
                ],
            },
            {
                "label": "Trends",
                "questions": [
                    "What is the trend in coal generation share?",
                    "How does wind generation vary by time of day?",
                    "Show the daily solar generation curve for SA1",
                    "What is the battery charging vs discharging pattern today?",
                ],
            },
        ],
    },
    {
        "space_id": "01f1151e5e4012fa91e403fc47fcb0a4",
        "title": "NEM Network & Interconnectors",
        "description": "Interconnector flows, congestion events, network constraints, and inter-regional transfers.",
        "icon": "network",
        "tables": ["nem_interconnectors", "nem_constraints_active", "nem_daily_summary"],
        "question_categories": [
            {
                "label": "Interconnector Flows",
                "questions": [
                    "What is the average utilization of each interconnector?",
                    "Show the flow pattern between NSW and QLD over the last week",
                    "When does Basslink typically export to Tasmania?",
                    "What direction is power flowing on Heywood right now?",
                    "Show daily average flows for VNI over the past month",
                    "Which interconnector carries the most energy?",
                ],
            },
            {
                "label": "Congestion",
                "questions": [
                    "Which interconnector is most congested?",
                    "How often is QNI congested in the last 30 days?",
                    "Show congestion frequency by time of day for all interconnectors",
                    "What is the marginal value of congestion on Heywood?",
                    "When was Basslink last congested for more than 4 hours?",
                ],
            },
            {
                "label": "Network Constraints",
                "questions": [
                    "What are the most binding network constraints?",
                    "List all THERMAL constraints active in the last 7 days",
                    "How many STABILITY constraints have occurred this month?",
                    "Which constraint equations bind most frequently?",
                    "Show the count of active constraints by type over the past week",
                ],
            },
            {
                "label": "Inter-Regional",
                "questions": [
                    "What is the net energy transfer between regions today?",
                    "Which region is the largest net exporter this week?",
                    "Show the relationship between interconnector flows and price differentials",
                    "How much power does Tasmania import vs export?",
                ],
            },
        ],
    },
    {
        "space_id": "01f1151e60481c12abe205ed51ccc8f3",
        "title": "NEM Forecasting & Weather",
        "description": "Demand forecasts, price forecasts, generation forecasts, and weather conditions.",
        "icon": "cloud-sun",
        "tables": ["demand_forecasts", "price_forecasts", "generation_forecasts", "weather_nem_regions", "demand_actuals"],
        "question_categories": [
            {
                "label": "Demand Forecasts",
                "questions": [
                    "How accurate are the demand forecasts for NSW1?",
                    "Show forecast vs actual demand for the last 24 hours",
                    "What is the MAPE for demand forecasts by region?",
                    "Which region has the least accurate demand forecast?",
                    "Show the 1-hour ahead demand forecast for VIC1 today",
                ],
            },
            {
                "label": "Price Forecasts",
                "questions": [
                    "Show the price forecast vs actual for the last 24 hours",
                    "Which region has the highest spike probability right now?",
                    "What is the predicted price range for SA1 tomorrow?",
                    "Show confidence intervals for price forecasts across all regions",
                    "How often do actual prices fall within the 80% confidence band?",
                ],
            },
            {
                "label": "Weather Impact",
                "questions": [
                    "What is the current temperature and wind speed by region?",
                    "How does solar radiation affect price forecasts?",
                    "Show the relationship between temperature and demand for NSW1",
                    "What are the wind conditions at hub height across all regions?",
                    "Which region has the highest solar radiation right now?",
                    "How does cloud cover correlate with solar generation?",
                ],
            },
            {
                "label": "Generation Forecasts",
                "questions": [
                    "What is the wind generation forecast for SA1 tomorrow?",
                    "Show solar generation forecast vs actual for the last 3 days",
                    "How does the generation forecast accuracy vary by fuel type?",
                    "What is the expected renewable generation share for tomorrow?",
                ],
            },
        ],
    },
    {
        "space_id": "01f118f175a21101aba69ab5d26358b6",
        "title": "NEM Bidding & Trading",
        "description": "Generator bidding behaviour, bid stacks, market concentration, and trading analytics.",
        "icon": "bar-chart-2",
        "tables": ["nem_facilities", "nem_generation_by_fuel", "nem_prices_5min"],
        "question_categories": [
            {
                "label": "Generator Fleet",
                "questions": [
                    "What is the total generation capacity by fuel type across all NEM regions?",
                    "Which generators have the largest capacity in Queensland?",
                    "Show me the top 10 largest generators in the NEM by capacity",
                    "What percentage of generation capacity is renewable vs fossil fuel?",
                    "Which region has the highest battery storage capacity?",
                ],
            },
            {
                "label": "Market Concentration",
                "questions": [
                    "How does spot price correlate with total demand in NSW?",
                    "What are the average spot prices by region for the last 30 days?",
                    "When did SA experience the highest spot prices this month?",
                    "Compare coal vs gas generation output across all regions",
                    "What is the capacity factor by fuel type in Victoria?",
                ],
            },
        ],
    },
    {
        "space_id": "01f118f1805f17b5b215919bcc795eeb",
        "title": "NEM Storage & Battery Analytics",
        "description": "Battery storage fleet analytics, arbitrage opportunities, and grid integration.",
        "icon": "battery-charging",
        "tables": ["nem_facilities", "nem_generation_by_fuel", "nem_interconnectors", "nem_prices_5min"],
        "question_categories": [
            {
                "label": "Battery Fleet",
                "questions": [
                    "How many battery storage units are registered in each NEM region?",
                    "What is the total battery charging and discharging capacity by region?",
                    "Which region has the largest battery fleet by MW capacity?",
                    "List all battery storage facilities with their capacity and region",
                ],
            },
            {
                "label": "Arbitrage & Dispatch",
                "questions": [
                    "What is the daily price spread (max-min) by region for battery arbitrage?",
                    "Show me battery generation output over the last 7 days",
                    "What hours have the highest price volatility for battery arbitrage?",
                    "What is the average price during peak vs off-peak hours by region?",
                ],
            },
            {
                "label": "Grid Integration",
                "questions": [
                    "Compare battery output to wind and solar generation by region",
                    "Which interconnectors are most congested and affect battery dispatch?",
                ],
            },
        ],
    },
]


@router.get("/api/genie/spaces")
def list_genie_spaces():
    """Return the list of configured Genie spaces."""
    return {"spaces": _GENIE_SPACES}


def _genie_headers():
    """Get auth headers for Genie API calls using the app's service principal."""
    import os
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    auth = w.config.authenticate()
    host = w.config.host.rstrip("/")
    # Inside a Databricks App, w.config.host may resolve to the app URL
    # (e.g. https://app-name.databricksapps.com) instead of the workspace URL.
    # The Genie API lives on the workspace, so we must use the workspace host.
    if "databricksapps.com" in host:
        ws_host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
        if ws_host and "databricksapps.com" not in ws_host:
            host = ws_host
        else:
            host = "https://fevm-energy-copilot.cloud.databricks.com"
    return auth, host


@router.post("/api/genie/spaces/{space_id}/start-conversation")
async def genie_start_conversation(space_id: str, request: Request):
    """Start a new Genie conversation with an initial question."""
    try:
        body = await request.json()
        headers, host = _genie_headers()
        headers["Content-Type"] = "application/json"
        import httpx
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                f"{host}/api/2.0/genie/spaces/{space_id}/start-conversation",
                headers=headers,
                json=body,
            )
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        return JSONResponse(status_code=502, content={"error": str(e)})


@router.post("/api/genie/spaces/{space_id}/conversations/{conversation_id}/messages")
async def genie_send_message(space_id: str, conversation_id: str, request: Request):
    """Send a follow-up message to an existing Genie conversation."""
    try:
        body = await request.json()
        headers, host = _genie_headers()
        headers["Content-Type"] = "application/json"
        import httpx
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                f"{host}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages",
                headers=headers,
                json=body,
            )
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        return JSONResponse(status_code=502, content={"error": str(e)})


@router.get("/api/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}")
async def genie_get_message(space_id: str, conversation_id: str, message_id: str):
    """Poll a Genie message for its result (SQL query and data)."""
    try:
        headers, host = _genie_headers()
        import httpx
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                f"{host}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}",
                headers=headers,
            )
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        return JSONResponse(status_code=502, content={"error": str(e)})


@router.get("/api/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/query-result")
async def genie_get_query_result(space_id: str, conversation_id: str, message_id: str):
    """Get the SQL query result data for a Genie message."""
    try:
        headers, host = _genie_headers()
        import httpx
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                f"{host}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/query-result",
                headers=headers,
            )
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        return JSONResponse(status_code=502, content={"error": str(e)})
