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
                    "What was the volume-weighted average price for NSW1 yesterday?",
                    "Show me the top 20 highest priced intervals this week across all regions",
                    "What is the 30-day rolling average spot price by region?",
                    "How many intervals exceeded $300/MWh in each region this month?",
                    "What is the median vs mean spot price for SA1 over the past 7 days?",
                ],
            },
            {
                "label": "Negative Pricing",
                "questions": [
                    "Which region had the most negative pricing intervals?",
                    "How many hours of negative pricing occurred in SA1 last month?",
                    "Show me all negative price events in the last 30 days by region",
                    "What time of day do negative prices most commonly occur?",
                    "What is the average depth of negative prices by region (how far below zero)?",
                    "Show negative pricing frequency by day of week and region",
                    "What percentage of intervals had negative prices in each region this month?",
                    "What was the most negative price recorded and when did it occur?",
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
                    "What is the average weekday vs weekend demand for each region?",
                    "Show the daily demand range (min-max) by region for the past month",
                    "What hour of the day typically has the highest demand in each region?",
                    "How does today's demand compare to the 30-day average by region?",
                ],
            },
            {
                "label": "Anomalies & Events",
                "questions": [
                    "Show all CRITICAL anomaly events in the last 7 days",
                    "How many anomaly events occurred by severity level this month?",
                    "What regions have the most frequent market anomalies?",
                    "List the top 10 most recent anomaly events with details",
                    "What is the average price during anomaly events vs normal periods?",
                    "Which anomaly types are most common across the NEM?",
                    "Show anomaly event frequency by time of day and region",
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
                    "What is the peak solar output by region this week?",
                    "Show the hourly renewable generation profile for SA1 today",
                    "What percentage of total generation comes from rooftop solar vs utility solar?",
                    "How does the renewable share vary between weekdays and weekends?",
                    "What is the minimum renewable share recorded in each region this month?",
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
                    "What is the average hourly generation by fuel type for NSW1?",
                    "Which fuel types have increased or decreased output this week vs last?",
                    "Show the generation stack by fuel type ordered from cheapest to most expensive",
                    "What is the hydro generation output across TAS1 and VIC1?",
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
                    "What is the NEM-wide carbon intensity trend over the past month?",
                    "Which region has reduced emissions the most this month vs last?",
                    "Show the relationship between renewable share and emissions intensity by region",
                    "What is the average capacity factor for wind vs solar by region?",
                ],
            },
            {
                "label": "Trends",
                "questions": [
                    "What is the trend in coal generation share?",
                    "How does wind generation vary by time of day?",
                    "Show the daily solar generation curve for SA1",
                    "What is the battery charging vs discharging pattern today?",
                    "How has the gas generation share changed over the past 30 days?",
                    "Show the weekly trend of total generation output by region",
                    "Is coal generation declining faster in VIC1 or NSW1?",
                    "What is the year-to-date generation trend for each fuel type?",
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
                    "What is the average flow direction for each interconnector by time of day?",
                    "Show the maximum and minimum flows for each interconnector this week",
                    "How often does the flow direction reverse on QNI?",
                    "What is the total energy transferred across all interconnectors today?",
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
                    "What percentage of time is each interconnector at its flow limit?",
                    "Show the average congestion duration by interconnector this month",
                    "Which interconnector has the highest congestion cost impact?",
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
                    "What is the average marginal value of the top 10 most frequent constraints?",
                    "How many unique constraints were active today compared to the monthly average?",
                    "Which regions are most affected by binding constraints?",
                ],
            },
            {
                "label": "Inter-Regional",
                "questions": [
                    "What is the net energy transfer between regions today?",
                    "Which region is the largest net exporter this week?",
                    "Show the relationship between interconnector flows and price differentials",
                    "How much power does Tasmania import vs export?",
                    "What is the net inter-regional transfer for each region this month?",
                    "Show the correlation between VIC-SA flows and SA1 spot prices",
                    "Which region pair has the most asymmetric flow pattern?",
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
                    "What is the forecast error distribution by region?",
                    "How does forecast accuracy vary by time of day?",
                    "Show the demand forecast bias (over-forecast vs under-forecast) by region",
                    "What is the peak demand forecast for tomorrow across all regions?",
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
                    "What is the forecast accuracy for price spike events vs normal periods?",
                    "Show the price forecast error trend over the past week by region",
                    "Which region's price is most difficult to forecast accurately?",
                    "What is the predicted average price by region for the next 24 hours?",
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
                    "What is the temperature forecast for the next 48 hours by region?",
                    "Show the wind speed trend for SA1 over the past week",
                    "When was the last high-wind event in SA1 and how did it affect prices?",
                    "How does humidity affect demand in QLD1?",
                ],
            },
            {
                "label": "Generation Forecasts",
                "questions": [
                    "What is the wind generation forecast for SA1 tomorrow?",
                    "Show solar generation forecast vs actual for the last 3 days",
                    "How does the generation forecast accuracy vary by fuel type?",
                    "What is the expected renewable generation share for tomorrow?",
                    "What is the forecasted solar peak output by region for today?",
                    "Show the wind generation forecast accuracy trend over the past month",
                    "What time will solar generation peak tomorrow in each region?",
                    "How much generation capacity is expected to be offline tomorrow?",
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
                    "How many generators are registered in each region by fuel type?",
                    "What is the average generator size (MW) by fuel type?",
                    "List all generators above 500MW capacity with their region and fuel type",
                    "What is the total installed capacity by region?",
                    "How many wind farms vs solar farms are in each region?",
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
                    "Which fuel type has the highest average capacity factor this month?",
                    "Show the price duration curve for each region over the past 30 days",
                    "What is the average price by time of day for each region?",
                    "How does generation output compare to registered capacity by fuel type?",
                ],
            },
            {
                "label": "Bidding Patterns",
                "questions": [
                    "What is the average bid price by fuel type across all regions?",
                    "Which generators have the most volatile bidding behaviour?",
                    "Show the bid stack for NSW1 ordered by price band",
                    "How has coal bidding behaviour changed over the past month?",
                    "What percentage of capacity is bid below $0/MWh by fuel type?",
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
                    "What is the average battery size in MW by region?",
                    "How many batteries were registered in the last 12 months?",
                    "What is the total installed battery capacity across the NEM?",
                ],
            },
            {
                "label": "Arbitrage & Dispatch",
                "questions": [
                    "What is the daily price spread (max-min) by region for battery arbitrage?",
                    "Show me battery generation output over the last 7 days",
                    "What hours have the highest price volatility for battery arbitrage?",
                    "What is the average price during peak vs off-peak hours by region?",
                    "What is the theoretical daily arbitrage revenue per MW by region?",
                    "Show the hourly price pattern for SA1 — when should batteries charge and discharge?",
                    "What is the average price spread between 2am-6am and 5pm-8pm by region?",
                    "How many hours per day have prices above $200/MWh by region?",
                    "What is the 7-day average arbitrage opportunity (peak-trough spread) by region?",
                ],
            },
            {
                "label": "Grid Integration",
                "questions": [
                    "Compare battery output to wind and solar generation by region",
                    "Which interconnectors are most congested and affect battery dispatch?",
                    "How does battery output correlate with renewable generation dips?",
                    "What percentage of battery output occurs during peak demand hours?",
                    "Show the relationship between battery dispatch and interconnector flows",
                    "How does battery charging pattern align with solar generation in SA1?",
                ],
            },
        ],
    },
    {
        "space_id": "01f11acf42701ae38c6c58a01612ed73",
        "title": "NEM FCAS & Ancillary Services",
        "description": "FCAS market pricing, providers, regulation vs contingency services, and battery FCAS revenue analytics.",
        "icon": "shield",
        "tables": ["nem_facilities", "nem_generation_by_fuel", "nem_prices_5min"],
        "question_categories": [
            {
                "label": "FCAS Pricing",
                "questions": [
                    "What is the average FCAS price by service type?",
                    "Which FCAS service is most expensive right now?",
                    "Show the relationship between spot price volatility and FCAS costs",
                    "What is the total FCAS cost as a percentage of energy market cost?",
                    "How do FCAS prices vary by time of day?",
                ],
            },
            {
                "label": "FCAS Providers",
                "questions": [
                    "Which generators provide the most FCAS capacity?",
                    "Show all battery storage units that provide FCAS by region",
                    "What is the total FCAS-capable capacity by fuel type?",
                    "Which region has the most FCAS providers?",
                    "What is the average capacity of FCAS providers by technology?",
                ],
            },
            {
                "label": "Regulation vs Contingency",
                "questions": [
                    "What percentage of FCAS cost is regulation vs contingency?",
                    "Which service type has the highest price volatility?",
                    "How does raise regulation compare to lower regulation in price?",
                    "Show the 6-second vs 60-second vs 5-minute contingency price comparison",
                ],
            },
            {
                "label": "Battery FCAS Revenue",
                "questions": [
                    "What is the estimated annual FCAS revenue for a 100MW battery?",
                    "Which region offers the best FCAS revenue opportunity for batteries?",
                    "How does battery FCAS revenue compare to energy arbitrage revenue?",
                    "Show battery capacity registered for FCAS by region",
                ],
            },
        ],
    },
    {
        "space_id": "01f11acf42701ae38c6c58a01612ed72",
        "title": "Portfolio & P&L",
        "description": "Trade portfolio positions, mark-to-market valuations, P&L attribution, credit exposure, and risk metrics across all NEM regions.",
        "icon": "briefcase",
        "tables": ["approval_requests", "approval_rules", "counterparties", "credit_exposure", "pnl_attribution", "portfolio_mtm", "portfolios", "risk_metrics", "trades"],
        "question_categories": [
            {
                "label": "Portfolio & MtM",
                "questions": [
                    "What is the total MtM value by region?",
                    "Show me the P&L attribution breakdown for this month",
                    "Which trades have the largest mark-to-market exposure?",
                    "What is the net position in MW by region and quarter?",
                    "What is the total portfolio MtM value across all portfolios?",
                    "Show the MtM trend over the last 30 days",
                    "Which portfolio has the highest MtM value?",
                    "What is the average trade size in MW by trade type?",
                    "How many trades are in each status (CONFIRMED, DRAFT, PENDING)?",
                    "What is the total notional value of all open trades?",
                ],
            },
            {
                "label": "P&L Attribution",
                "questions": [
                    "What is the daily P&L by portfolio for the past week?",
                    "Which component drives the most P&L — price, volume, or new trades?",
                    "Show the cumulative P&L trend for the past 30 days",
                    "What is the P&L attribution by region?",
                    "Which trades generated the most profit this month?",
                    "Show the P&L waterfall — base vs price vs volume vs new trade effects",
                ],
            },
            {
                "label": "Credit & Risk",
                "questions": [
                    "Which counterparties have the highest credit exposure?",
                    "Show me VaR metrics by portfolio",
                    "What is the credit utilization for each counterparty?",
                    "List all pending approval trades with notional above $1M",
                    "Which counterparty is closest to their credit limit?",
                    "Show the credit exposure trend for AGL Energy over the past month",
                    "What is the total credit exposure across all counterparties?",
                    "How many trades are pending approval and what is their total notional?",
                    "Which approval rules have been triggered most frequently?",
                    "Show the risk metrics (VaR, CVaR) by portfolio and confidence level",
                ],
            },
            {
                "label": "Trade Analysis",
                "questions": [
                    "What is the breakdown of trades by type (SPOT, FORWARD, SWAP, PPA)?",
                    "Show all PPA trades with their strike price and volume",
                    "Which region has the most active trades?",
                    "What is the average trade duration by type?",
                    "List the 10 largest trades by volume (MW)",
                    "How many trades were created this week vs last week?",
                    "What is the total buy vs sell volume by region?",
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
            host = os.environ.get("DATABRICKS_HOST", "")
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
