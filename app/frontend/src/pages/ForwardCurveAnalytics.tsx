import { useEffect, useState } from 'react'
import { TrendingUp } from 'lucide-react'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import { api } from '../api/client'
import type {
  Fwd38cDashboard,
  Fwd38cCurvePoint,
  Fwd38cCapOptionRecord,
  Fwd38cSeasonalPremiumRecord,
} from '../api/client'

// ── KPI Card ──────────────────────────────────────────────────────────────

interface KpiCardProps {
  label: string
  value: string
  sub?: string
}

function KpiCard({ label, value, sub }: KpiCardProps) {
  return (
    <div className="bg-gray-800 rounded-lg p-4 flex flex-col gap-1">
      <span className="text-xs text-gray-400 uppercase tracking-wide">{label}</span>
      <span className="text-2xl font-bold text-white">{value}</span>
      {sub && <span className="text-xs text-gray-500">{sub}</span>}
    </div>
  )
}

// ── Region colours ────────────────────────────────────────────────────────

const REGION_COLOURS: Record<string, string> = {
  NSW1: '#3b82f6',  // blue
  QLD1: '#f59e0b',  // amber
  VIC1: '#14b8a6',  // teal
  SA1:  '#ef4444',  // red
}

// ── Forward Curve Chart ───────────────────────────────────────────────────

interface ChartRow {
  product: string
  NSW1?: number
  QLD1?: number
  VIC1?: number
  SA1?: number
}

function buildChartData(points: Fwd38cCurvePoint[]): ChartRow[] {
  const calendarProducts = ['CAL25', 'CAL26', 'CAL27']
  const map: Record<string, ChartRow> = {}
  calendarProducts.forEach(p => { map[p] = { product: p } })

  points
    .filter(p => p.product_type === 'CALENDAR')
    .forEach(p => {
      if (map[p.product]) {
        ;(map[p.product] as unknown as Record<string, unknown>)[p.region] = p.settlement_price_aud_mwh
      }
    })

  return calendarProducts.map(p => map[p])
}

// ── Cap Options Table ─────────────────────────────────────────────────────

interface OptionsTableProps {
  options: Fwd38cCapOptionRecord[]
}

function OptionsTable({ options }: OptionsTableProps) {
  const [regionFilter, setRegionFilter] = useState<string>('ALL')
  const [typeFilter, setTypeFilter] = useState<string>('ALL')

  const regions = Array.from(new Set(options.map(o => o.region))).sort()
  const types = Array.from(new Set(options.map(o => o.contract_type))).sort()

  const filtered = options.filter(o => {
    const regionOk = regionFilter === 'ALL' || o.region === regionFilter
    const typeOk = typeFilter === 'ALL' || o.contract_type === typeFilter
    return regionOk && typeOk
  })

  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-sm font-semibold text-gray-200 uppercase tracking-wide">
          Cap / Floor Options
        </h2>
        <div className="flex gap-2">
          <select
            value={regionFilter}
            onChange={e => setRegionFilter(e.target.value)}
            className="text-xs bg-gray-700 text-gray-200 border border-gray-600 rounded px-2 py-1"
          >
            <option value="ALL">All Regions</option>
            {regions.map(r => (
              <option key={r} value={r}>{r}</option>
            ))}
          </select>
          <select
            value={typeFilter}
            onChange={e => setTypeFilter(e.target.value)}
            className="text-xs bg-gray-700 text-gray-200 border border-gray-600 rounded px-2 py-1"
          >
            <option value="ALL">All Types</option>
            {types.map(t => (
              <option key={t} value={t}>{t}</option>
            ))}
          </select>
        </div>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs text-gray-300">
          <thead>
            <tr className="border-b border-gray-700">
              <th className="text-left py-2 pr-3 text-gray-400 font-medium">Option ID</th>
              <th className="text-right py-2 pr-3 text-gray-400 font-medium">Strike ($/MWh)</th>
              <th className="text-right py-2 pr-3 text-gray-400 font-medium">Premium ($/MWh)</th>
              <th className="text-right py-2 pr-3 text-gray-400 font-medium">Delta</th>
              <th className="text-right py-2 pr-3 text-gray-400 font-medium">IV %</th>
              <th className="text-right py-2 pr-3 text-gray-400 font-medium">OI (MW)</th>
              <th className="text-center py-2 text-gray-400 font-medium">ITM</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map(o => (
              <tr key={o.option_id} className="border-b border-gray-700/50 hover:bg-gray-750">
                <td className="py-1.5 pr-3 font-mono">{o.option_id}</td>
                <td className="py-1.5 pr-3 text-right">{o.strike_price_aud_mwh.toLocaleString()}</td>
                <td className="py-1.5 pr-3 text-right">{o.premium_aud_mwh.toFixed(2)}</td>
                <td className="py-1.5 pr-3 text-right">{o.delta.toFixed(3)}</td>
                <td className="py-1.5 pr-3 text-right">{o.implied_vol_pct.toFixed(1)}%</td>
                <td className="py-1.5 pr-3 text-right">{o.open_interest_mw.toLocaleString()}</td>
                <td className="py-1.5 text-center">
                  {o.in_the_money ? (
                    <span className="inline-block px-1.5 py-0.5 rounded text-[10px] font-semibold bg-green-900 text-green-300">
                      ITM
                    </span>
                  ) : (
                    <span className="inline-block px-1.5 py-0.5 rounded text-[10px] font-semibold bg-gray-700 text-gray-400">
                      OTM
                    </span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {filtered.length === 0 && (
          <p className="text-center text-gray-500 py-6 text-sm">No options match the selected filters.</p>
        )}
      </div>
    </div>
  )
}

// ── Seasonal Premium Heatmap ──────────────────────────────────────────────

const SEASONS = ['SUMMER', 'AUTUMN', 'WINTER', 'SPRING'] as const
type Season = typeof SEASONS[number]

function premiumColour(prem: number): string {
  if (prem > 5) return 'bg-green-900 text-green-200'
  if (prem >= 0) return 'bg-gray-700 text-gray-300'
  return 'bg-red-900 text-red-300'
}

interface SeasonalHeatmapProps {
  records: Fwd38cSeasonalPremiumRecord[]
}

function SeasonalHeatmap({ records }: SeasonalHeatmapProps) {
  const year2024 = records.filter(r => r.year === 2024)
  const regions = Array.from(new Set(year2024.map(r => r.region))).sort()

  // Build lookup: region -> season -> forward_premium_aud_mwh
  const lookup: Record<string, Partial<Record<Season, number>>> = {}
  year2024.forEach(r => {
    if (!lookup[r.region]) lookup[r.region] = {}
    lookup[r.region][r.season as Season] = r.forward_premium_aud_mwh
  })

  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <h2 className="text-sm font-semibold text-gray-200 uppercase tracking-wide mb-4">
        Seasonal Forward Premium Heatmap — 2024 ($/MWh)
      </h2>
      <div className="overflow-x-auto">
        <table className="w-full text-xs text-gray-300">
          <thead>
            <tr className="border-b border-gray-700">
              <th className="text-left py-2 pr-4 text-gray-400 font-medium">Region</th>
              {SEASONS.map(s => (
                <th key={s} className="text-center py-2 px-4 text-gray-400 font-medium">{s}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {regions.map(region => (
              <tr key={region} className="border-b border-gray-700/50">
                <td className="py-2 pr-4 font-semibold text-gray-200">{region}</td>
                {SEASONS.map(season => {
                  const prem = lookup[region]?.[season]
                  return (
                    <td key={season} className="py-2 px-4 text-center">
                      {prem !== undefined ? (
                        <span className={`inline-block px-2 py-1 rounded text-[11px] font-medium ${premiumColour(prem)}`}>
                          {prem > 0 ? '+' : ''}{prem.toFixed(1)}
                        </span>
                      ) : (
                        <span className="text-gray-600">—</span>
                      )}
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="mt-2 text-[10px] text-gray-500">
        Green = forward premium &gt; $5/MWh &nbsp;|&nbsp; Neutral = $0–5/MWh &nbsp;|&nbsp; Red = negative premium
      </p>
    </div>
  )
}

// ── Main Page ─────────────────────────────────────────────────────────────

export default function ForwardCurveAnalytics() {
  const [dashboard, setDashboard] = useState<Fwd38cDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    api
      .getForwardCurveDashboard()
      .then(setDashboard)
      .catch(e => setError(String(e)))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full bg-gray-900">
        <span className="text-gray-400 text-sm animate-pulse">Loading forward curve data...</span>
      </div>
    )
  }

  if (error || !dashboard) {
    return (
      <div className="flex items-center justify-center h-full bg-gray-900">
        <span className="text-red-400 text-sm">{error ?? 'Unknown error'}</span>
      </div>
    )
  }

  const chartData = buildChartData(dashboard.forward_curve)

  return (
    <div className="min-h-full bg-gray-900 p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center gap-3">
        <TrendingUp className="text-blue-400" size={24} />
        <div>
          <h1 className="text-xl font-bold text-white">
            Electricity Derivatives & Forward Curve Analytics
          </h1>
          <p className="text-xs text-gray-400 mt-0.5">
            ASX energy futures · forward price curves · cap/floor options · seasonal premiums
          </p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KpiCard
          label="NSW CAL25 Spot"
          value={`$${dashboard.base_spot_nsw_aud_mwh.toFixed(2)}/MWh`}
          sub="Annual base reference"
        />
        <KpiCard
          label="Curve Steepness (CAL26–CAL25)"
          value={`${dashboard.curve_steepness_nsw > 0 ? '+' : ''}$${dashboard.curve_steepness_nsw.toFixed(2)}/MWh`}
          sub="NSW contango/backwardation"
        />
        <KpiCard
          label="Avg Implied Volatility"
          value={`${dashboard.avg_implied_vol_pct.toFixed(1)}%`}
          sub="Across all curve points"
        />
        <KpiCard
          label="Total Open Interest"
          value={`${(dashboard.total_open_interest_mw / 1000).toFixed(1)}k MW`}
          sub="Aggregate OI all products"
        />
      </div>

      {/* Forward Curve Line Chart */}
      <div className="bg-gray-800 rounded-lg p-4">
        <h2 className="text-sm font-semibold text-gray-200 uppercase tracking-wide mb-4">
          Forward Price Curve by Region — Calendar Products
        </h2>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={chartData} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis
              dataKey="product"
              tick={{ fill: '#9ca3af', fontSize: 12 }}
              axisLine={{ stroke: '#4b5563' }}
              tickLine={false}
            />
            <YAxis
              tick={{ fill: '#9ca3af', fontSize: 12 }}
              axisLine={false}
              tickLine={false}
              tickFormatter={v => `$${v}`}
              domain={['auto', 'auto']}
            />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
              labelStyle={{ color: '#e5e7eb', fontWeight: 600 }}
              itemStyle={{ color: '#d1d5db' }}
              formatter={(val: number) => [`$${val.toFixed(2)}/MWh`, '']}
            />
            <Legend
              wrapperStyle={{ color: '#9ca3af', fontSize: 12 }}
            />
            {Object.entries(REGION_COLOURS).map(([region, colour]) => (
              <Line
                key={region}
                type="monotone"
                dataKey={region}
                stroke={colour}
                strokeWidth={2}
                dot={{ r: 4, fill: colour }}
                activeDot={{ r: 6 }}
                connectNulls
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Cap Options Table */}
      <OptionsTable options={dashboard.cap_options} />

      {/* Seasonal Premium Heatmap */}
      <SeasonalHeatmap records={dashboard.seasonal_premiums} />
    </div>
  )
}
