import React, { useEffect, useState } from 'react'
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
import { Leaf } from 'lucide-react'
import {
  getCarbonOffsetMarketDashboard,
  ACOMADashboard,
} from '../api/client'

// ---------------------------------------------------------------------------
// Colour maps
// ---------------------------------------------------------------------------

const METHOD_COLOURS: Record<string, string> = {
  'Savanna Burning':             '#f59e0b',
  'Human Induced Regeneration':  '#10b981',
  'Soil Carbon':                 '#84cc16',
  'Avoided Deforestation':       '#06b6d4',
  'Reforestation':               '#3b82f6',
  'Blue Carbon':                 '#0ea5e9',
  'Landfill Gas':                '#8b5cf6',
}

const STATE_COLOURS: Record<string, string> = {
  'NSW': '#3b82f6',
  'VIC': '#8b5cf6',
  'QLD': '#f59e0b',
  'SA':  '#10b981',
  'WA':  '#f97316',
  'TAS': '#06b6d4',
  'NT':  '#ec4899',
}

const CATEGORY_COLOURS: Record<string, string> = {
  'Land':        '#10b981',
  'Agriculture': '#f59e0b',
  'Industrial':  '#6366f1',
  'Waste':       '#8b5cf6',
  'Transport':   '#06b6d4',
}

const SECTOR_COLOURS: Record<string, string> = {
  'Mining':            '#94a3b8',
  'Oil & Gas':         '#f97316',
  'Energy':            '#3b82f6',
  'Aviation':          '#06b6d4',
  'Retail':            '#ec4899',
  'Manufacturing':     '#8b5cf6',
  'Finance':           '#10b981',
  'Telecommunications':'#f59e0b',
  'Transport':         '#84cc16',
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

function kpiCard(title: string, value: string, sub: string, Icon: React.ElementType) {
  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl shadow p-5 flex flex-col gap-2">
      <div className="flex items-center gap-2 text-gray-500 dark:text-gray-400 text-sm font-medium">
        <Icon size={16} />
        {title}
      </div>
      <p className="text-3xl font-bold text-gray-900 dark:text-white">{value}</p>
      <p className="text-xs text-gray-400">{sub}</p>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export default function CarbonOffsetMarketAnalytics() {
  const [data, setData] = useState<ACOMADashboard | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getCarbonOffsetMarketDashboard()
      .then(setData)
      .catch((e: Error) => setError(e.message))
  }, [])

  if (error) {
    return (
      <div className="p-8 text-red-500 dark:text-red-400">
        Failed to load Carbon Offset Market data: {error}
      </div>
    )
  }

  if (!data) {
    return (
      <div className="p-8 text-gray-500 dark:text-gray-400 animate-pulse">
        Loading Australian Carbon Offset Market Analytics...
      </div>
    )
  }

  const { accu_market, projects, methodologies, price_trends, buyers, summary } = data

  // -------------------------------------------------------------------------
  // Chart 2: top 15 projects – accu_issued_total_k by method, coloured by state
  // -------------------------------------------------------------------------
  const top15Projects = [...projects]
    .sort((a, b) => b.accu_issued_total_k - a.accu_issued_total_k)
    .slice(0, 15)
    .map(p => ({
      name: p.project_name.length > 22 ? p.project_name.slice(0, 22) + '…' : p.project_name,
      value: p.accu_issued_total_k,
      state: p.state,
      method: p.method,
      fill: STATE_COLOURS[p.state] ?? '#6b7280',
    }))

  // -------------------------------------------------------------------------
  // Chart 3: methodologies – total_accu_issued_k by method_name, coloured by category
  // -------------------------------------------------------------------------
  const methodChart = [...methodologies]
    .sort((a, b) => b.total_accu_issued_k - a.total_accu_issued_k)
    .map(m => ({
      name: m.method_name.length > 24 ? m.method_name.slice(0, 24) + '…' : m.method_name,
      value: m.total_accu_issued_k,
      category: m.category,
      fill: CATEGORY_COLOURS[m.category] ?? '#6b7280',
    }))

  // -------------------------------------------------------------------------
  // Chart 4: buyers – total_spend_m by buyer, coloured by sector
  // -------------------------------------------------------------------------
  const buyerChart = [...buyers]
    .sort((a, b) => b.total_spend_m - a.total_spend_m)
    .map(b => ({
      name: b.buyer,
      value: b.total_spend_m,
      sector: b.buyer_sector,
      fill: SECTOR_COLOURS[b.buyer_sector] ?? '#6b7280',
    }))

  return (
    <div className="p-6 space-y-8 max-w-screen-2xl mx-auto">
      {/* Page header */}
      <div className="flex items-center gap-3">
        <Leaf className="text-green-500" size={28} />
        <div>
          <h1 className="text-2xl font-bold text-gray-900 dark:text-white">
            Australian Carbon Offset Market Analytics
          </h1>
          <p className="text-sm text-gray-500 dark:text-gray-400">
            ACCU market · ERF projects · Safeguard mechanism · Buyer landscape · Price trends
          </p>
        </div>
      </div>

      {/* KPI cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {kpiCard(
          'Total ACCU Issued',
          `${(summary.total_accu_issued_m as number).toFixed(2)} M`,
          'All quarters 2022–2024',
          Leaf,
        )}
        {kpiCard(
          'Latest Spot Price',
          `$${(summary.spot_price_latest as number).toFixed(2)}/tCO₂`,
          'Most recent quarter',
          Leaf,
        )}
        {kpiCard(
          'Total Market Value',
          `$${(summary.total_market_value_m as number).toFixed(0)} M`,
          'Cumulative 2022–2024',
          Leaf,
        )}
        {kpiCard(
          'Largest Methodology',
          summary.largest_method as string,
          `Compliance demand: ${(summary.compliance_demand_pct as number).toFixed(1)}%`,
          Leaf,
        )}
      </div>

      {/* Chart 1: Spot price & auction clearing price by month */}
      <div className="bg-white dark:bg-gray-800 rounded-xl shadow p-5">
        <h2 className="text-base font-semibold text-gray-800 dark:text-gray-100 mb-4">
          ACCU Price Trends 2020–2024 (Spot vs Auction Clearing Price)
        </h2>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={price_trends} margin={{ top: 5, right: 20, left: 0, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" className="stroke-gray-200 dark:stroke-gray-700" />
            <XAxis
              dataKey="month"
              tickFormatter={(v: string) => v.slice(0, 7)}
              tick={{ fontSize: 11 }}
              interval={5}
            />
            <YAxis
              yAxisId="left"
              tickFormatter={(v: number) => `$${v}`}
              tick={{ fontSize: 11 }}
              label={{ value: '$/tCO₂', angle: -90, position: 'insideLeft', fontSize: 11 }}
            />
            <Tooltip formatter={(v: number) => [`$${v.toFixed(2)}/tCO₂`]} />
            <Legend />
            <Line
              yAxisId="left"
              type="monotone"
              dataKey="spot_price"
              name="Spot Price"
              stroke="#10b981"
              dot={false}
              strokeWidth={2}
            />
            <Line
              yAxisId="left"
              type="monotone"
              dataKey="auction_clearing_price"
              name="Auction Clearing Price"
              stroke="#3b82f6"
              dot={false}
              strokeWidth={2}
              strokeDasharray="5 3"
            />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 2: Top 15 projects by ACCU issued, coloured by state */}
      <div className="bg-white dark:bg-gray-800 rounded-xl shadow p-5">
        <h2 className="text-base font-semibold text-gray-800 dark:text-gray-100 mb-4">
          Top 15 Carbon Projects by ACCU Issued (coloured by state)
        </h2>
        <ResponsiveContainer width="100%" height={340}>
          <BarChart
            data={top15Projects}
            layout="vertical"
            margin={{ top: 5, right: 30, left: 8, bottom: 5 }}
          >
            <CartesianGrid strokeDasharray="3 3" className="stroke-gray-200 dark:stroke-gray-700" />
            <XAxis type="number" tick={{ fontSize: 11 }} unit="k" />
            <YAxis type="category" dataKey="name" width={180} tick={{ fontSize: 10 }} />
            <Tooltip
              formatter={(v: number) => [`${v.toLocaleString()} k ACCUs`]}
              labelFormatter={(label: string) => label}
            />
            {Object.entries(STATE_COLOURS).map(([state, colour]) => (
              <Bar
                key={state}
                dataKey="value"
                name={state}
                fill={colour}
                data={top15Projects.filter(p => p.state === state)}
              />
            ))}
            <Bar
              dataKey="value"
              fill="#6b7280"
              shape={((props: React.SVGProps<SVGRectElement> & { fill?: string }) => {
                return <rect {...props} fill={props.fill} />
              }) as any}
            >
              {top15Projects.map((entry, i) => (
                <rect key={i} fill={entry.fill} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
        {/* State legend */}
        <div className="flex flex-wrap gap-3 mt-3">
          {Object.entries(STATE_COLOURS).map(([state, colour]) => (
            <span key={state} className="flex items-center gap-1 text-xs text-gray-600 dark:text-gray-300">
              <span className="w-3 h-3 rounded-sm inline-block" style={{ backgroundColor: colour }} />
              {state}
            </span>
          ))}
        </div>
      </div>

      {/* Chart 3: Methodologies – total ACCUs issued, coloured by category */}
      <div className="bg-white dark:bg-gray-800 rounded-xl shadow p-5">
        <h2 className="text-base font-semibold text-gray-800 dark:text-gray-100 mb-4">
          ACCU Issuance by ERF Methodology (coloured by category)
        </h2>
        <ResponsiveContainer width="100%" height={340}>
          <BarChart
            data={methodChart}
            layout="vertical"
            margin={{ top: 5, right: 30, left: 8, bottom: 5 }}
          >
            <CartesianGrid strokeDasharray="3 3" className="stroke-gray-200 dark:stroke-gray-700" />
            <XAxis type="number" tick={{ fontSize: 11 }} unit="k" />
            <YAxis type="category" dataKey="name" width={200} tick={{ fontSize: 10 }} />
            <Tooltip formatter={(v: number) => [`${v.toLocaleString()} k ACCUs`]} />
            <Bar dataKey="value" name="Total ACCU Issued (k)">
              {methodChart.map((entry, i) => (
                <rect key={i} fill={entry.fill} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
        <div className="flex flex-wrap gap-3 mt-3">
          {Object.entries(CATEGORY_COLOURS).map(([cat, colour]) => (
            <span key={cat} className="flex items-center gap-1 text-xs text-gray-600 dark:text-gray-300">
              <span className="w-3 h-3 rounded-sm inline-block" style={{ backgroundColor: colour }} />
              {cat}
            </span>
          ))}
        </div>
      </div>

      {/* Chart 4: Buyer spend, coloured by sector */}
      <div className="bg-white dark:bg-gray-800 rounded-xl shadow p-5">
        <h2 className="text-base font-semibold text-gray-800 dark:text-gray-100 mb-4">
          ACCU Spend by Buyer (coloured by sector)
        </h2>
        <ResponsiveContainer width="100%" height={340}>
          <BarChart
            data={buyerChart}
            layout="vertical"
            margin={{ top: 5, right: 30, left: 8, bottom: 5 }}
          >
            <CartesianGrid strokeDasharray="3 3" className="stroke-gray-200 dark:stroke-gray-700" />
            <XAxis type="number" tick={{ fontSize: 11 }} unit="M" tickFormatter={(v: number) => `$${v}`} />
            <YAxis type="category" dataKey="name" width={140} tick={{ fontSize: 11 }} />
            <Tooltip formatter={(v: number) => [`$${v.toFixed(1)} M`]} />
            <Bar dataKey="value" name="Total Spend ($M)">
              {buyerChart.map((entry, i) => (
                <rect key={i} fill={entry.fill} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
        <div className="flex flex-wrap gap-3 mt-3">
          {Object.entries(SECTOR_COLOURS).map(([sector, colour]) => (
            <span key={sector} className="flex items-center gap-1 text-xs text-gray-600 dark:text-gray-300">
              <span className="w-3 h-3 rounded-sm inline-block" style={{ backgroundColor: colour }} />
              {sector}
            </span>
          ))}
        </div>
      </div>

      {/* Chart 5: Dual-axis line – volume_traded_k and safeguard_demand_k by month */}
      <div className="bg-white dark:bg-gray-800 rounded-xl shadow p-5">
        <h2 className="text-base font-semibold text-gray-800 dark:text-gray-100 mb-4">
          ACCU Market Volume vs Safeguard Mechanism Demand (Monthly)
        </h2>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={price_trends} margin={{ top: 5, right: 30, left: 0, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" className="stroke-gray-200 dark:stroke-gray-700" />
            <XAxis
              dataKey="month"
              tickFormatter={(v: string) => v.slice(0, 7)}
              tick={{ fontSize: 11 }}
              interval={5}
            />
            <YAxis
              yAxisId="left"
              tick={{ fontSize: 11 }}
              label={{ value: 'Volume (k)', angle: -90, position: 'insideLeft', fontSize: 11 }}
            />
            <YAxis
              yAxisId="right"
              orientation="right"
              tick={{ fontSize: 11 }}
              label={{ value: 'Safeguard (k)', angle: 90, position: 'insideRight', fontSize: 11 }}
            />
            <Tooltip />
            <Legend />
            <Line
              yAxisId="left"
              type="monotone"
              dataKey="volume_traded_k"
              name="Volume Traded (k)"
              stroke="#f59e0b"
              dot={false}
              strokeWidth={2}
            />
            <Line
              yAxisId="right"
              type="monotone"
              dataKey="safeguard_demand_k"
              name="Safeguard Demand (k)"
              stroke="#ec4899"
              dot={false}
              strokeWidth={2}
              strokeDasharray="4 2"
            />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Summary dl grid */}
      <div className="bg-white dark:bg-gray-800 rounded-xl shadow p-5">
        <h2 className="text-base font-semibold text-gray-800 dark:text-gray-100 mb-4">
          Market Summary
        </h2>
        <dl className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
          {Object.entries(summary).map(([key, value]) => (
            <div key={key} className="bg-gray-50 dark:bg-gray-700 rounded-lg p-3">
              <dt className="text-xs text-gray-500 dark:text-gray-400 font-medium uppercase tracking-wide">
                {key.replace(/_/g, ' ')}
              </dt>
              <dd className="mt-1 text-sm font-semibold text-gray-900 dark:text-white">
                {typeof value === 'number'
                  ? Number.isInteger(value)
                    ? value.toLocaleString()
                    : value.toFixed(2)
                  : String(value)}
              </dd>
            </div>
          ))}
        </dl>
      </div>

      {/* ACCU market quarterly table */}
      <div className="bg-white dark:bg-gray-800 rounded-xl shadow p-5">
        <h2 className="text-base font-semibold text-gray-800 dark:text-gray-100 mb-4">
          Quarterly ACCU Market Data
        </h2>
        <div className="overflow-x-auto">
          <table className="w-full text-sm text-left">
            <thead>
              <tr className="border-b border-gray-200 dark:border-gray-600 text-gray-500 dark:text-gray-400 text-xs uppercase">
                <th className="py-2 pr-4">Quarter</th>
                <th className="py-2 pr-4">Issued (k)</th>
                <th className="py-2 pr-4">Cancelled (k)</th>
                <th className="py-2 pr-4">Spot Price</th>
                <th className="py-2 pr-4">Fwd 12m</th>
                <th className="py-2 pr-4">Mkt Value ($M)</th>
                <th className="py-2 pr-4">Compliance (k)</th>
                <th className="py-2">Voluntary (k)</th>
              </tr>
            </thead>
            <tbody>
              {accu_market.map((row, i) => (
                <tr
                  key={row.quarter}
                  className={`border-b border-gray-100 dark:border-gray-700 ${
                    i % 2 === 0 ? 'bg-gray-50 dark:bg-gray-750' : ''
                  }`}
                >
                  <td className="py-1.5 pr-4 font-medium text-gray-800 dark:text-gray-200">{row.quarter}</td>
                  <td className="py-1.5 pr-4 text-gray-600 dark:text-gray-300">{row.accu_issued_k.toLocaleString()}</td>
                  <td className="py-1.5 pr-4 text-gray-600 dark:text-gray-300">{row.accu_cancelled_k.toLocaleString()}</td>
                  <td className="py-1.5 pr-4 text-green-600 dark:text-green-400 font-medium">${row.spot_price.toFixed(2)}</td>
                  <td className="py-1.5 pr-4 text-gray-600 dark:text-gray-300">${row.forward_price_12m.toFixed(2)}</td>
                  <td className="py-1.5 pr-4 text-gray-600 dark:text-gray-300">${row.market_value_m.toFixed(1)}</td>
                  <td className="py-1.5 pr-4 text-blue-600 dark:text-blue-400">{row.compliance_demand_k.toLocaleString()}</td>
                  <td className="py-1.5 text-purple-600 dark:text-purple-400">{row.voluntary_demand_k.toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
