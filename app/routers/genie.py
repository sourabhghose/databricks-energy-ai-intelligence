from __future__ import annotations

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
        "space_id": "01f11b0795a81b8fa0a0e20742cf5714",
        "title": "NEM FCAS & Ancillary Services",
        "description": "FCAS market pricing, providers, regulation vs contingency services, and battery FCAS revenue analytics.",
        "icon": "shield",
        "tables": ["nem_facilities", "nem_generation_by_fuel", "nem_prices_5min", "gold_nem_fcas_prices"],
        "question_categories": [
            {
                "label": "FCAS Pricing",
                "questions": [
                    "What is the average clearing price for each FCAS service type in the last 24 hours?",
                    "Which FCAS service has the highest clearing price right now?",
                    "Show the relationship between spot price volatility and FCAS clearing prices",
                    "What is the total FCAS cost as a percentage of energy market cost?",
                    "How do FCAS clearing prices vary by time of day and service?",
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
        "space_id": "01f11b079e1c1e7f81b273f49998cbe7",
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
    {
        "space_id": "01f11b079e541520b51e68fb151bf227",
        "title": "NEM Bidding & Revenue Optimisation",
        "description": "Generator bidding analytics, dispatch conformance, revenue attribution, and bid optimisation strategies.",
        "icon": "target",
        "tables": ["bids_submitted", "bid_optimization_results", "dispatch_conformance", "revenue_attribution"],
        "question_categories": [
            {
                "label": "Bid Analysis",
                "questions": [
                    "Show all bids for Bayswater in the last 7 days",
                    "What is the average bid price by band across NSW generators?",
                    "Which generators have the most rebids?",
                    "Show the bid stack by price band for QLD1",
                    "What percentage of bids are accepted vs rejected?",
                ],
            },
            {
                "label": "Conformance",
                "questions": [
                    "What is the dispatch conformance rate by generator?",
                    "Show all non-conforming events in the last 30 days",
                    "Which generators have the highest deviation from dispatch targets?",
                    "How many penalty events occurred this month?",
                ],
            },
            {
                "label": "Revenue",
                "questions": [
                    "What is the total revenue by generator this month?",
                    "Compare energy revenue vs FCAS revenue across all generators",
                    "Which generator has the highest capacity factor?",
                    "Show daily revenue trends for coal vs wind generators",
                ],
            },
        ],
    },
    {
        "space_id": "01f11b079e891767a523d81b0e1268ce",
        "title": "Gas Market Analytics",
        "description": "Eastern Australian gas market data including STTM hub prices, DWGM Victorian market, and spark spread analytics.",
        "icon": "flame",
        "tables": ["gas_sttm_prices", "gas_dwgm_prices", "gas_spark_spreads", "gas_hub_prices"],
        "question_categories": [
            {
                "label": "STTM Prices",
                "questions": [
                    "What is the average STTM gas price by hub this month?",
                    "Show the ex-ante vs ex-post price for Sydney hub",
                    "Which STTM hub has the highest gas prices?",
                    "Show the daily STTM price trend for Adelaide over 90 days",
                ],
            },
            {
                "label": "DWGM",
                "questions": [
                    "What is the average DWGM price this week?",
                    "Show DWGM price by trading interval today",
                    "How does DWGM linepack correlate with price?",
                ],
            },
            {
                "label": "Spark Spreads",
                "questions": [
                    "What is the current spark spread by NEM region?",
                    "Which region has the highest clean spark spread?",
                    "Show the spark spread trend for NSW over 90 days",
                    "How does the gas price affect electricity prices?",
                ],
            },
        ],
    },
    {
        "space_id": "01f11b079eba19ea9b3eec8e4f589f63",
        "title": "WEM Market Analytics",
        "description": "Western Australian Electricity Market — balancing prices, generation mix, demand, and NEM comparison.",
        "icon": "globe",
        "tables": ["wem_balancing_prices", "wem_generation", "wem_demand"],
        "question_categories": [
            {
                "label": "Balancing Prices",
                "questions": [
                    "What is the average WEM balancing price this week?",
                    "Show the WEM price distribution over the last 30 days",
                    "How many high-price intervals occurred in the WEM?",
                    "Compare WEM vs NEM NSW prices",
                ],
            },
            {
                "label": "Generation",
                "questions": [
                    "What is the WEM generation mix by fuel type?",
                    "Show the renewable share in WEM over the past month",
                    "How does coal vs gas generation compare in WEM?",
                ],
            },
            {
                "label": "Demand",
                "questions": [
                    "What is the peak WEM demand this month?",
                    "Show demand vs forecast accuracy in WEM",
                    "How does WEM demand correlate with temperature?",
                ],
            },
        ],
    },
    # ── Phase 4: Distribution Network Intelligence ──────────────────
    {
        "space_id": "01f11bb768841db386e1d0252b845a55",
        "title": "Network Operations & Reliability",
        "description": "Distribution network asset loading, voltage monitoring, outage events, and reliability KPIs (SAIDI/SAIFI/CAIDI) across Australian DNSPs.",
        "icon": "activity",
        "tables": ["network_assets", "asset_loading_5min", "voltage_monitoring", "outage_events", "reliability_kpis", "power_quality"],
        "question_categories": [
            {
                "label": "Asset Loading",
                "questions": [
                    "Which zone substations had the highest average utilization in the last 24 hours?",
                    "Which assets are above 90% utilization right now?",
                    "Show utilization trends for zone substations in NSW1",
                ],
            },
            {
                "label": "Voltage & Power Quality",
                "questions": [
                    "How many voltage excursion events occurred today by region?",
                    "Which monitoring points have the worst THD (total harmonic distortion)?",
                    "Show voltage profile for the last 24 hours",
                ],
            },
            {
                "label": "Outages & Reliability",
                "questions": [
                    "What is the SAIDI YTD for each region compared to AER targets?",
                    "Show the top 10 outage events by affected customers in the last 30 days",
                    "What are the worst feeders by SAIFI this month?",
                    "Break down outage causes by percentage",
                ],
            },
        ],
    },
    {
        "space_id": "01f11bb768c117a4b273d7ba33fe4660",
        "title": "DER & Hosting Capacity",
        "description": "Distributed energy resources fleet (solar, battery, EV chargers), hosting capacity by feeder, curtailment events, VPP dispatch performance, and DOE compliance.",
        "icon": "sun",
        "tables": ["der_fleet", "hosting_capacity", "curtailment_events", "vpp_dispatch_events", "doe_compliance", "der_output_estimated"],
        "question_categories": [
            {
                "label": "DER Fleet",
                "questions": [
                    "What is the total installed rooftop solar capacity by zone substation?",
                    "Show the breakdown of DER installations by technology type and region",
                    "How many EV chargers are connected per zone substation?",
                ],
            },
            {
                "label": "Hosting Capacity & Curtailment",
                "questions": [
                    "Which feeders have less than 20% remaining hosting capacity?",
                    "How much energy was curtailed this week and what was the financial impact?",
                    "What are the main reasons for curtailment events?",
                ],
            },
            {
                "label": "VPP & DOE",
                "questions": [
                    "What is the average VPP response accuracy across all programs?",
                    "Which feeders have the lowest DOE compliance rates?",
                    "Show total VPP revenue by program",
                ],
            },
        ],
    },
    {
        "space_id": "01f11bb768fa1e84925a341546ed0339",
        "title": "Network Planning & EV Impact",
        "description": "Spatial demand forecasting by scenario (BAU/high solar/high EV/combined), network constraint register with breach years, EV charging impact projections.",
        "icon": "trending-up",
        "tables": ["demand_forecast_spatial", "network_constraints_register", "ev_network_impact", "ev_charging_profiles"],
        "question_categories": [
            {
                "label": "Demand Forecasting",
                "questions": [
                    "Which zone substations have the highest peak demand growth by 2030 under BAU?",
                    "Compare peak demand across BAU vs combined scenario for 2028",
                    "Show solar impact on peak demand by zone substation",
                ],
            },
            {
                "label": "Network Constraints",
                "questions": [
                    "How many constraints are forecast to breach before 2030?",
                    "Which zone substations need augmentation before 2032?",
                    "List all thermal constraints with utilization above 85%",
                ],
            },
            {
                "label": "EV Impact",
                "questions": [
                    "What is the EV impact on peak load under the high scenario by feeder?",
                    "Show EV charging load profiles by charge point type",
                    "Which feeders need upgrades due to EV growth in the medium scenario?",
                ],
            },
        ],
    },
    # ── Phase 5: Settlement & Finance ────────────────────────────────
    {
        "space_id": "01f12112f4fb1901885d45287acee57e",
        "title": "Settlement & Finance",
        "description": "AEMO settlement processing, billing run analysis, charge reconciliation, dispute management, GL journals, and finance-ready outputs.",
        "icon": "file-check",
        "tables": ["settlement_runs", "settlement_charges", "settlement_journals", "settlement_disputes", "settlement_gl_mapping", "settlement_evidence"],
        "question_categories": [
            {
                "label": "Settlement Runs",
                "questions": [
                    "Show all settlement runs with their status, AEMO total, and variance percentage",
                    "Which settlement runs have variance above $50,000?",
                    "Compare PRELIM vs FINAL settlement amounts by region",
                    "What is the total AEMO settlement amount by region?",
                    "List all PENDING settlement runs sorted by run date",
                    "What is the average variance percentage by region and run type?",
                ],
            },
            {
                "label": "Charges & Reconciliation",
                "questions": [
                    "Show charge breakdown by charge type for the latest settlement run",
                    "What is the total variance by charge type across all runs?",
                    "How many charges are UNMAPPED vs MAPPED?",
                    "Show the top 10 charges by absolute variance amount",
                    "What is the net settlement position (AEMO minus internal) by region?",
                ],
            },
            {
                "label": "Disputes",
                "questions": [
                    "List all open disputes with their priority and workflow state",
                    "How many disputes are in each workflow state?",
                    "Show disputes with CRITICAL or HIGH priority that are not CLOSED",
                    "What is the total disputed variance amount by region?",
                    "List disputes that have been open for more than 30 days",
                ],
            },
            {
                "label": "Finance & GL",
                "questions": [
                    "Show GL journal entries by period and account code",
                    "What is the total debit and credit amount by journal type?",
                    "List unposted journals sorted by amount",
                    "Show the GL mapping configuration for all charge types",
                ],
            },
        ],
    },
    # ── Phase 5B: DNSP Enterprise Intelligence ───────────────────────
    {
        "space_id": "01f12370e4a11b38b4bc0f82d2797eab",
        "title": "DNSP Enterprise Intelligence",
        "description": "AER regulatory compliance (RIN/STPIS/revenue cap), network tariff reform, bushfire mitigation (BMP), CSO/rural programs, NER connections, and capital program delivery across Australian DNSPs.",
        "icon": "building-2",
        "tables": [
            "rin_submissions", "stpis_performance", "revenue_monitoring", "pricing_proposals",
            "regulatory_milestones", "network_tariff_structures", "tariff_migration_progress",
            "tariff_revenue_by_class", "demand_tariff_performance",
            "bmp_asset_register", "elc_inspections", "fire_risk_incidents", "bmp_spend",
            "cso_payments", "rural_feeder_performance", "raps_fleet",
            "connection_applications", "timely_connections_kpi", "large_customer_pipeline",
            "capital_projects", "maintenance_orders", "fault_response_kpis"
        ],
        "question_categories": [
            {
                "label": "AER Regulatory (RIN & STPIS)",
                "questions": [
                    "What is the current STPIS performance score for AusNet Services?",
                    "Which DNSPs are at risk of missing their AER revenue cap?",
                    "Show the RIN submission status and compliance percentage by DNSP",
                    "What regulatory milestones are due in the next 90 days?",
                    "Which STPIS indicators are underperforming vs AER targets?",
                    "Show the revenue monitoring trend vs approved allowance for each DNSP",
                    "What is the pricing proposal status for each DNSP's current regulatory period?",
                    "Which DNSPs have submitted their RIN on time vs late in the last 3 years?",
                    "What is the STPIS penalty/reward exposure by DNSP at current performance?",
                    "Show the revenue cap utilisation percentage by DNSP for the current regulatory year",
                ],
            },
            {
                "label": "Network Tariffs & Reform",
                "questions": [
                    "What is the tariff migration progress to cost-reflective demand tariffs by DNSP?",
                    "Which customer classes have the highest demand tariff adoption rate?",
                    "Show the network revenue by tariff class for the current year",
                    "How many residential customers have moved to demand tariffs this year?",
                    "What is the average demand charge per kW by DNSP?",
                    "Show the tariff reform tracker — which AER milestones have been met?",
                    "Which DNSPs are ahead of or behind schedule on tariff migration targets?",
                    "What is the revenue impact of demand tariff adoption vs flat tariffs?",
                    "Show the distribution of customers across tariff types by DNSP",
                    "What is the peak demand reduction achieved through demand tariff uptake?",
                ],
            },
            {
                "label": "Bushfire Mitigation (AusNet)",
                "questions": [
                    "What is the overall BMP asset compliance percentage for AusNet Services?",
                    "Which BMO zones have the highest number of extreme risk assets?",
                    "Show the ELC inspection compliance rate for the last 90 days",
                    "How many overdue assets are in each BMO zone?",
                    "What is the BMP capex spend vs AER allowance by category?",
                    "Which BMO zone has the lowest compliance percentage?",
                    "Show fire risk incidents by severity and BMO zone for this fire season",
                    "What is the seasonal readiness status for the upcoming fire season?",
                    "How many assets in the Kinglake and Dandenong Ranges BMO zones are non-compliant?",
                    "What is the trend in ELC non-compliance over the past 12 months?",
                ],
            },
            {
                "label": "Rural Network & CSO (Energy Queensland)",
                "questions": [
                    "What is the total CSO subsidy paid to Ergon Energy this financial year?",
                    "Which rural feeders have the worst reliability performance (SAIDI)?",
                    "Show the RAPS fleet status — how many sites are operational vs faulted?",
                    "What is the non-network alternative cost saving vs traditional augmentation?",
                    "How does the Ergon vs Energex split compare for revenue and customers?",
                    "Which rural feeders exceed the AER reliability standard?",
                    "Show the CSO payment trend over the last 5 years",
                    "What is the average RAPS site cost vs CSO subsidy threshold?",
                    "List rural feeders with fault response times exceeding the AER benchmark",
                    "What is the total RAPS fleet capacity (kW) by region?",
                ],
            },
            {
                "label": "Connections (NER Compliance)",
                "questions": [
                    "What is the timely connections compliance rate by DNSP and connection type?",
                    "How many connection applications are currently in the queue?",
                    "Which connection types have the worst on-time offer rate?",
                    "Show the average time to offer by connection category vs NER benchmark",
                    "What is the large customer pipeline by DNSP and MW capacity requested?",
                    "How many connections are at risk of breaching NER Rule 5.3.4A timeframes?",
                    "Show the connection queue aging — how many are overdue by more than 30 days?",
                    "What is the connection offer acceptance rate by DNSP?",
                    "Which DNSPs have the most capacity-constrained connection queues?",
                    "Show the trend in connection application volumes over the past 12 months",
                ],
            },
            {
                "label": "Capital Program & Delivery",
                "questions": [
                    "Which capital projects are at risk of schedule or budget overrun?",
                    "What is the capex vs opex ratio by DNSP and program type?",
                    "Show the maintenance backlog and overdue work orders by DNSP",
                    "What is the fault response compliance rate vs AER benchmark?",
                    "Which contractors have the worst delivery performance score?",
                    "Show the capital program delivery trend — planned vs actual spend by quarter",
                    "How many critical infrastructure projects are behind schedule?",
                    "What is the average fault response time vs AER target by fault type?",
                    "Show the contractor performance scorecard for top 10 contractors",
                    "What is the capex spend rate as a percentage of the annual budget?",
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
