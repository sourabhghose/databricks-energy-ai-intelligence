import { useEffect, useState } from 'react'
import { DollarSign } from 'lucide-react'
import {
  BarChart,
  Bar,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import {
  getEnergyStorageMerchantRevenueDashboard,
  ESMRDashboard,
} from '../api/client'

export default function EnergyStorageMerchantRevenueAnalytics() {
  const [data, setData] = useState<ESMRDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getEnergyStorageMerchantRevenueDashboard()
      .then(setData)
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 bg-gray-900 text-gray-400">
        <DollarSign size={24} className="animate-pulse mr-2 text-green-400" />
        <span>Loading Energy Storage Merchant Revenue data...</span>
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="flex items-center justify-center h-64 bg-gray-900 text-red-400">
        <DollarSign size={24} className="mr-2" />
        <span>Error loading data: {error ?? 'Unknown error'}</span>
      </div>
    )
  }

  const summary = data.summary

  // ---------- KPI cards ----------
  const kpis = [
    {
      label: 'Total Storage Capacity (MW)',
      value: `${Number(summary.total_storage_capacity_mw).toLocaleString()} MW`,
      color: 'text-green-400',
    },
    {
      label: 'Avg Project IRR',
      value: `${Number(summary.avg_irr_pct).toFixed(1)}%`,
      color: 'text-blue-400',
    },
    {
      label: 'Best Region',
      value: String(summary.best_region),
      color: 'text-amber-400',
    },
    {
      label: 'Dominant Revenue Stream',
      value: String(summary.dominant_revenue_stream),
      color: 'text-purple-400',
    },
  ]

  // ---------- Chart 1: Stacked bar — revenue components by region ----------
  const revenueByRegion: Record<string, { Arbitrage: number; FCAS: number; 'Cap Market': number }> = {}
  for (const r of data.revenues) {
    if (!revenueByRegion[r.region]) {
      revenueByRegion[r.region] = { Arbitrage: 0, FCAS: 0, 'Cap Market': 0 }
    }
    revenueByRegion[r.region].Arbitrage += r.arbitrage_revenue_k
    revenueByRegion[r.region].FCAS += r.fcas_revenue_k
    revenueByRegion[r.region]['Cap Market'] += r.capacity_market_revenue_k
  }
  const revenueByRegionData = Object.entries(revenueByRegion).map(([region, v]) => ({
    region,
    Arbitrage: +v.Arbitrage.toFixed(0),
    FCAS: +v.FCAS.toFixed(0),
    'Cap Market': +v['Cap Market'].toFixed(0),
  }))

  // ---------- Chart 2: Horizontal bar — top 15 assets by total revenue ----------
  const top15Assets = [...data.revenues]
    .sort((a, b) => b.total_revenue_k - a.total_revenue_k)
    .slice(0, 15)
    .map(r => ({
      asset: r.asset_name,
      'Total Revenue ($k)': r.total_revenue_k,
    }))

  // ---------- Chart 3: Line — spread_per_mwh by month for 5 regions ----------
  const spreadRegions = ['NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1']
  const spreadByMonth: Record<string, Record<string, number>> = {}
  for (const s of data.price_spreads) {
    if (!spreadByMonth[s.month]) spreadByMonth[s.month] = { month: s.month } as unknown as Record<string, number>
    spreadByMonth[s.month][s.region] = s.spread_per_mwh
  }
  const spreadChartData = Object.values(spreadByMonth).sort((a, b) =>
    String(a.month).localeCompare(String(b.month))
  )
  const regionColors: Record<string, string> = {
    NSW1: '#60a5fa',
    QLD1: '#34d399',
    VIC1: '#f59e0b',
    SA1: '#fb7185',
    TAS1: '#a78bfa',
  }

  // ---------- Chart 4: Grouped bar — IRR% and payback by technology ----------
  const econByTech: Record<string, { irr: number; payback: number; count: number }> = {}
  for (const e of data.battery_economics) {
    if (!econByTech[e.technology]) econByTech[e.technology] = { irr: 0, payback: 0, count: 0 }
    econByTech[e.technology].irr += e.irr_pct
    econByTech[e.technology].payback += e.payback_years
    econByTech[e.technology].count += 1
  }
  const econChartData = Object.entries(econByTech).map(([tech, v]) => ({
    technology: tech,
    'Avg IRR (%)': +(v.irr / v.count).toFixed(2),
    'Avg Payback (yrs)': +(v.payback / v.count).toFixed(1),
  }))

  // ---------- Chart 5: Line — merchant_revenue_b by year for 3 scenarios ----------
  const outlookScenarios = ['Base Case', 'High Renewables', 'Policy Support']
  const outlookByYear: Record<number, Record<string, number>> = {}
  for (const o of data.outlooks) {
    if (!outlookByYear[o.year]) outlookByYear[o.year] = { year: o.year }
    outlookByYear[o.year][o.scenario] = o.merchant_revenue_b
  }
  const outlookChartData = Object.values(outlookByYear).sort((a, b) => Number(a.year) - Number(b.year))
  const scenarioColors: Record<string, string> = {
    'Base Case': '#60a5fa',
    'High Renewables': '#34d399',
    'Policy Support': '#f59e0b',
  }

  return (
    <div className="p-6 bg-gray-900 min-h-screen text-gray-100">
      {/* Header */}
      <div className="flex items-center gap-3 mb-6">
        <DollarSign size={28} className="text-green-400" />
        <div>
          <h1 className="text-2xl font-bold text-white">Energy Storage Merchant Revenue Analytics</h1>
          <p className="text-sm text-gray-400 mt-0.5">
            NEM storage asset merchant revenue streams, economics, and market outlook
          </p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        {kpis.map(kpi => (
          <div key={kpi.label} className="bg-gray-800 rounded-lg p-4 border border-gray-700">
            <p className="text-xs text-gray-400 uppercase tracking-wide mb-1">{kpi.label}</p>
            <p className={`text-2xl font-bold ${kpi.color}`}>{kpi.value}</p>
          </div>
        ))}
      </div>

      {/* Row 1: Chart 1 — Revenue by Region (Stacked) + Chart 2 — Top 15 Assets (Horizontal bar) */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6 mb-6">
        <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
          <h2 className="text-sm font-semibold text-gray-300 mb-3">
            Revenue Components by Region ($k) — Arbitrage vs FCAS vs Capacity Market
          </h2>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={revenueByRegionData} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="region" tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 10 }} unit="k" />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                formatter={(value: number) => [`$${value.toLocaleString()}k`, undefined]}
              />
              <Legend wrapperStyle={{ color: '#d1d5db', fontSize: 12 }} />
              <Bar dataKey="Arbitrage" stackId="a" fill="#60a5fa" />
              <Bar dataKey="FCAS" stackId="a" fill="#34d399" />
              <Bar dataKey="Cap Market" stackId="a" fill="#f59e0b" />
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
          <h2 className="text-sm font-semibold text-gray-300 mb-3">
            Top 15 Assets by Total Revenue ($k) — Sorted Descending
          </h2>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart
              data={top15Assets}
              layout="vertical"
              margin={{ top: 4, right: 16, left: 120, bottom: 4 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis type="number" tick={{ fill: '#9ca3af', fontSize: 10 }} unit="k" />
              <YAxis
                type="category"
                dataKey="asset"
                tick={{ fill: '#9ca3af', fontSize: 9 }}
                width={115}
              />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                formatter={(value: number) => [`$${value.toLocaleString()}k`, 'Total Revenue']}
              />
              <Bar dataKey="Total Revenue ($k)" fill="#a78bfa" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Row 2: Chart 3 — Price Spread by Month per Region */}
      <div className="bg-gray-800 rounded-lg p-4 border border-gray-700 mb-6">
        <h2 className="text-sm font-semibold text-gray-300 mb-3">
          Price Spread ($/MWh) by Month — 5 NEM Regions, 2024
        </h2>
        <ResponsiveContainer width="100%" height={280}>
          <LineChart data={spreadChartData} margin={{ top: 4, right: 24, left: 0, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="month" tick={{ fill: '#9ca3af', fontSize: 10 }} />
            <YAxis tick={{ fill: '#9ca3af', fontSize: 10 }} unit=" $/MWh" />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
              formatter={(value: number) => [`$${value.toFixed(2)}/MWh`, undefined]}
            />
            <Legend wrapperStyle={{ color: '#d1d5db', fontSize: 12 }} />
            {spreadRegions.map(region => (
              <Line
                key={region}
                type="monotone"
                dataKey={region}
                stroke={regionColors[region]}
                strokeWidth={2}
                dot={{ r: 3 }}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Row 3: Chart 4 — Battery Economics by Technology + Chart 5 — Market Outlook */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6 mb-6">
        <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
          <h2 className="text-sm font-semibold text-gray-300 mb-3">
            Avg IRR% and Payback Years by Storage Technology
          </h2>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={econChartData} margin={{ top: 4, right: 16, left: 0, bottom: 40 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                dataKey="technology"
                tick={{ fill: '#9ca3af', fontSize: 9 }}
                angle={-30}
                textAnchor="end"
              />
              <YAxis yAxisId="left" tick={{ fill: '#9ca3af', fontSize: 10 }} />
              <YAxis yAxisId="right" orientation="right" tick={{ fill: '#9ca3af', fontSize: 10 }} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
              />
              <Legend wrapperStyle={{ color: '#d1d5db', fontSize: 12 }} />
              <Bar yAxisId="left" dataKey="Avg IRR (%)" fill="#34d399" />
              <Bar yAxisId="right" dataKey="Avg Payback (yrs)" fill="#fb7185" />
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
          <h2 className="text-sm font-semibold text-gray-300 mb-3">
            NEM Storage Merchant Revenue Outlook ($B) — 3 Scenarios, 2025–2035
          </h2>
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={outlookChartData} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="year" tick={{ fill: '#9ca3af', fontSize: 10 }} />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 10 }} unit="B" />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                formatter={(value: number) => [`$${value.toFixed(2)}B`, undefined]}
              />
              <Legend wrapperStyle={{ color: '#d1d5db', fontSize: 12 }} />
              {outlookScenarios.map(scenario => (
                <Line
                  key={scenario}
                  type="monotone"
                  dataKey={scenario}
                  stroke={scenarioColors[scenario]}
                  strokeWidth={2}
                  dot={{ r: 3 }}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Summary Grid */}
      <div className="bg-gray-800 rounded-lg p-5 border border-gray-700">
        <h2 className="text-sm font-semibold text-gray-300 mb-4">Platform Summary</h2>
        <dl className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
          <div>
            <dt className="text-xs text-gray-400 uppercase tracking-wide">Total Storage (MW)</dt>
            <dd className="text-lg font-bold text-green-400 mt-1">
              {Number(summary.total_storage_capacity_mw).toLocaleString()}
            </dd>
          </div>
          <div>
            <dt className="text-xs text-gray-400 uppercase tracking-wide">Avg IRR (%)</dt>
            <dd className="text-lg font-bold text-blue-400 mt-1">
              {Number(summary.avg_irr_pct).toFixed(2)}%
            </dd>
          </div>
          <div>
            <dt className="text-xs text-gray-400 uppercase tracking-wide">Best Region</dt>
            <dd className="text-lg font-bold text-amber-400 mt-1">{String(summary.best_region)}</dd>
          </div>
          <div>
            <dt className="text-xs text-gray-400 uppercase tracking-wide">Dominant Stream</dt>
            <dd className="text-lg font-bold text-purple-400 mt-1">
              {String(summary.dominant_revenue_stream)}
            </dd>
          </div>
          <div>
            <dt className="text-xs text-gray-400 uppercase tracking-wide">Total Annual Rev ($M)</dt>
            <dd className="text-lg font-bold text-pink-400 mt-1">
              ${Number(summary.total_annual_revenue_m).toFixed(2)}M
            </dd>
          </div>
        </dl>
      </div>
    </div>
  )
}
