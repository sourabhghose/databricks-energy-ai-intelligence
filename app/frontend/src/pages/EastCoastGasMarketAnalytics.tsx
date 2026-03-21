import { useEffect, useState } from 'react'
import {
  BarChart,
  Bar,
  AreaChart,
  Area,
  LineChart,
  Line,
  ComposedChart,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import { Flame, TrendingUp, DollarSign, Activity } from 'lucide-react'
import { getECGADashboard, ECGADashboard } from '../api/client'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function fmt(n: number, decimals = 0): string {
  return n.toLocaleString('en-AU', { minimumFractionDigits: decimals, maximumFractionDigits: decimals })
}

// ---------------------------------------------------------------------------
// KPI card
// ---------------------------------------------------------------------------

interface KpiCardProps {
  label: string
  value: string
  sub?: string
  icon: React.ReactNode
  iconBg: string
}

function KpiCard({ label, value, sub, icon, iconBg }: KpiCardProps) {
  return (
    <div className="bg-gray-800 rounded-xl shadow p-5 flex items-center gap-4">
      <div className={`${iconBg} rounded-lg p-3 shrink-0`}>{icon}</div>
      <div>
        <p className="text-xs text-gray-400 mb-0.5">{label}</p>
        <p className="text-2xl font-bold text-white">{value}</p>
        {sub && <p className="text-xs text-gray-500 mt-0.5">{sub}</p>}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Colours
// ---------------------------------------------------------------------------

const HUB_COLOURS: Record<string, string> = {
  SYDNEY: '#3b82f6',
  BRISBANE: '#f59e0b',
  ADELAIDE: '#22c55e',
  VICTORIA: '#ec4899',
}

const BASIN_COLOURS: Record<string, string> = {
  COOPER: '#3b82f6',
  GIPPSLAND: '#22c55e',
  SURAT_BOWEN: '#f59e0b',
  OTWAY: '#ec4899',
  BASS: '#8b5cf6',
  OTHER: '#6b7280',
}

const DEMAND_COLOURS: Record<string, string> = {
  power_gen_pj: '#3b82f6',
  industrial_pj: '#f59e0b',
  residential_pj: '#22c55e',
  commercial_pj: '#ec4899',
  lng_export_pj: '#ef4444',
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export default function EastCoastGasMarketAnalytics() {
  const [data, setData] = useState<ECGADashboard | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getECGADashboard().then(setData).catch((e) => setError(e.message))
  }, [])

  if (error) return <div className="p-6 text-red-400">Error: {error}</div>
  if (!data) return <div className="p-6 text-gray-400">Loading East Coast Gas Market Analytics...</div>

  // ---- Derived data ----
  const latestPrices = data.hub_prices.reduce<Record<string, number>>((acc, r) => {
    if (!acc[r.hub] || r.month > String(acc._month ?? '')) {
      acc[r.hub] = r.price_aud_gj
      acc._month = r.month as unknown as number
    }
    return acc
  }, {} as Record<string, number>)

  const avgSttmPrice = data.hub_prices.length > 0
    ? data.hub_prices.reduce((s, r) => s + r.price_aud_gj, 0) / data.hub_prices.length
    : 0

  const latestLng = data.lng.length > 0 ? data.lng[data.lng.length - 1] : null
  const lngNetback = latestLng ? latestLng.netback_aud_gj : 0

  const totalProduction = data.balance.reduce((s, r) => s + r.production_pj_yr, 0)

  const totalDemand = data.demand.length > 0
    ? data.demand[data.demand.length - 1].total_pj * 4
    : 0
  const supplyGap = totalDemand - totalProduction

  // ---- Hub price line chart data (pivoted by month) ----
  const hubMonths = [...new Set(data.hub_prices.map((r) => r.month))].sort()
  const hubLineData = hubMonths.map((month) => {
    const row: Record<string, string | number> = { month }
    data.hub_prices.filter((r) => r.month === month).forEach((r) => {
      row[r.hub] = r.price_aud_gj
    })
    return row
  })

  // ---- Supply stacked bar data (pivoted by quarter) ----
  const supplyQuarters = [...new Set(data.supply.map((r) => r.quarter))].sort()
  const supplyBarData = supplyQuarters.map((quarter) => {
    const row: Record<string, string | number> = { quarter }
    data.supply.filter((r) => r.quarter === quarter).forEach((r) => {
      row[r.basin] = r.production_pj
    })
    return row
  })

  // ---- Demand area chart data ----
  const demandAreaData = data.demand.map((r) => ({
    quarter: r.quarter,
    'Power Gen': r.power_gen_pj,
    Industrial: r.industrial_pj,
    Residential: r.residential_pj,
    Commercial: r.commercial_pj,
    'LNG Export': r.lng_export_pj,
  }))

  // ---- LNG composed chart data ----
  const lngComposedData = data.lng.map((r) => ({
    month: r.month,
    export_volume_pj: r.export_volume_pj,
    domestic_price_aud_gj: r.domestic_price_aud_gj,
    netback_aud_gj: r.netback_aud_gj,
  }))

  // ---- Hub summary table ----
  const hubs = [...new Set(data.hub_prices.map((r) => r.hub))]
  const hubSummary = hubs.map((hub) => {
    const rows = data.hub_prices.filter((r) => r.hub === hub)
    const prices = rows.map((r) => r.price_aud_gj)
    const avg = prices.reduce((s, p) => s + p, 0) / prices.length
    const min = Math.min(...prices)
    const max = Math.max(...prices)
    const mean = avg
    const variance = prices.reduce((s, p) => s + (p - mean) ** 2, 0) / prices.length
    const volatility = Math.sqrt(variance)
    const constraintDays = rows.filter((r) => r.supply_constraint).length
    return { hub, avg, min, max, volatility, constraintDays }
  })

  return (
    <div className="min-h-screen bg-gray-900 text-gray-100 p-6 space-y-8">
      {/* Header */}
      <div className="flex items-center gap-3">
        <Flame className="text-orange-400" size={28} />
        <div>
          <h1 className="text-2xl font-bold">East Coast Gas Market Analytics</h1>
          <p className="text-sm text-gray-400">STTM hub prices, supply-demand balance, LNG export dynamics</p>
        </div>
      </div>

      {/* KPIs */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <KpiCard
          label="STTM Hub Price (avg)"
          value={`$${fmt(avgSttmPrice, 2)}/GJ`}
          sub="Across all hubs"
          icon={<DollarSign className="text-blue-300" size={22} />}
          iconBg="bg-blue-900"
        />
        <KpiCard
          label="LNG Netback"
          value={`$${fmt(lngNetback, 2)}/GJ`}
          sub="Latest month"
          icon={<TrendingUp className="text-amber-300" size={22} />}
          iconBg="bg-amber-900"
        />
        <KpiCard
          label="Domestic Production"
          value={`${fmt(totalProduction, 1)} PJ/yr`}
          sub="All basins"
          icon={<Flame className="text-green-300" size={22} />}
          iconBg="bg-green-900"
        />
        <KpiCard
          label="Supply Gap"
          value={`${fmt(supplyGap, 1)} PJ`}
          sub="Demand minus production"
          icon={<Activity className="text-red-300" size={22} />}
          iconBg="bg-red-900"
        />
      </div>

      {/* Charts row 1 */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Gas prices across hubs */}
        <div className="bg-gray-800 rounded-xl p-5">
          <h2 className="text-sm font-semibold text-gray-300 mb-4">Gas Prices Across Hubs (18 months)</h2>
          <ResponsiveContainer width="100%" height={320}>
            <LineChart data={hubLineData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="month" tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} unit="$/GJ" />
              <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: 'none', borderRadius: 8, color: '#f3f4f6' }} />
              <Legend />
              {hubs.map((hub) => (
                <Line key={hub} type="monotone" dataKey={hub} stroke={HUB_COLOURS[hub] ?? '#6b7280'} strokeWidth={2} dot={false} />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Supply sources stacked bar */}
        <div className="bg-gray-800 rounded-xl p-5">
          <h2 className="text-sm font-semibold text-gray-300 mb-4">Gas Supply Sources by Quarter</h2>
          <ResponsiveContainer width="100%" height={320}>
            <BarChart data={supplyBarData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="quarter" tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} unit=" PJ" />
              <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: 'none', borderRadius: 8, color: '#f3f4f6' }} />
              <Legend />
              {Object.entries(BASIN_COLOURS).map(([basin, colour]) => (
                <Bar key={basin} dataKey={basin} stackId="supply" fill={colour} />
              ))}
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Charts row 2 */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Demand by sector */}
        <div className="bg-gray-800 rounded-xl p-5">
          <h2 className="text-sm font-semibold text-gray-300 mb-4">Demand by Sector Over Time</h2>
          <ResponsiveContainer width="100%" height={320}>
            <AreaChart data={demandAreaData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="quarter" tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} unit=" PJ" />
              <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: 'none', borderRadius: 8, color: '#f3f4f6' }} />
              <Legend />
              <Area type="monotone" dataKey="Power Gen" stackId="demand" fill="#3b82f6" stroke="#3b82f6" fillOpacity={0.7} />
              <Area type="monotone" dataKey="Industrial" stackId="demand" fill="#f59e0b" stroke="#f59e0b" fillOpacity={0.7} />
              <Area type="monotone" dataKey="Residential" stackId="demand" fill="#22c55e" stroke="#22c55e" fillOpacity={0.7} />
              <Area type="monotone" dataKey="Commercial" stackId="demand" fill="#ec4899" stroke="#ec4899" fillOpacity={0.7} />
              <Area type="monotone" dataKey="LNG Export" stackId="demand" fill="#ef4444" stroke="#ef4444" fillOpacity={0.7} />
            </AreaChart>
          </ResponsiveContainer>
        </div>

        {/* LNG export vs domestic price */}
        <div className="bg-gray-800 rounded-xl p-5">
          <h2 className="text-sm font-semibold text-gray-300 mb-4">LNG Export Volume vs Domestic Price</h2>
          <ResponsiveContainer width="100%" height={320}>
            <ComposedChart data={lngComposedData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="month" tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <YAxis yAxisId="left" tick={{ fill: '#9ca3af', fontSize: 11 }} unit=" PJ" />
              <YAxis yAxisId="right" orientation="right" tick={{ fill: '#9ca3af', fontSize: 11 }} unit="$/GJ" />
              <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: 'none', borderRadius: 8, color: '#f3f4f6' }} />
              <Legend />
              <Bar yAxisId="left" dataKey="export_volume_pj" fill="#3b82f6" name="Export Volume (PJ)" />
              <Line yAxisId="right" type="monotone" dataKey="domestic_price_aud_gj" stroke="#f59e0b" strokeWidth={2} name="Domestic Price ($/GJ)" dot={false} />
              <Line yAxisId="right" type="monotone" dataKey="netback_aud_gj" stroke="#22c55e" strokeWidth={2} name="LNG Netback ($/GJ)" dot={false} />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Hub price summary table */}
      <div className="bg-gray-800 rounded-xl p-5 overflow-x-auto">
        <h2 className="text-sm font-semibold text-gray-300 mb-4">Hub Price Summary</h2>
        <table className="w-full text-sm text-left">
          <thead>
            <tr className="border-b border-gray-700 text-gray-400">
              <th className="py-2 px-3">Hub</th>
              <th className="py-2 px-3 text-right">Avg Price ($/GJ)</th>
              <th className="py-2 px-3 text-right">Min</th>
              <th className="py-2 px-3 text-right">Max</th>
              <th className="py-2 px-3 text-right">Volatility</th>
              <th className="py-2 px-3 text-right">Supply Constraint Days</th>
            </tr>
          </thead>
          <tbody>
            {hubSummary.map((r) => (
              <tr key={r.hub} className="border-b border-gray-700/50 hover:bg-gray-700/30">
                <td className="py-2 px-3 font-medium text-white">{r.hub}</td>
                <td className="py-2 px-3 text-right">${fmt(r.avg, 2)}</td>
                <td className="py-2 px-3 text-right">${fmt(r.min, 2)}</td>
                <td className="py-2 px-3 text-right">${fmt(r.max, 2)}</td>
                <td className="py-2 px-3 text-right">{fmt(r.volatility, 2)}</td>
                <td className="py-2 px-3 text-right">{r.constraintDays}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Gas supply-demand balance table */}
      <div className="bg-gray-800 rounded-xl p-5 overflow-x-auto">
        <h2 className="text-sm font-semibold text-gray-300 mb-4">Gas Supply-Demand Balance by Basin</h2>
        <table className="w-full text-sm text-left">
          <thead>
            <tr className="border-b border-gray-700 text-gray-400">
              <th className="py-2 px-3">Basin</th>
              <th className="py-2 px-3 text-right">Production (PJ/yr)</th>
              <th className="py-2 px-3 text-right">Reserves 2P (PJ)</th>
              <th className="py-2 px-3 text-right">2P Remaining (yrs)</th>
              <th className="py-2 px-3 text-right">Decline Rate (%)</th>
              <th className="py-2 px-3 text-right">New Wells Needed</th>
            </tr>
          </thead>
          <tbody>
            {data.balance.map((r) => (
              <tr key={r.basin} className="border-b border-gray-700/50 hover:bg-gray-700/30">
                <td className="py-2 px-3 font-medium text-white">{r.basin}</td>
                <td className="py-2 px-3 text-right">{fmt(r.production_pj_yr, 1)}</td>
                <td className="py-2 px-3 text-right">{fmt(r.reserves_2p_pj, 0)}</td>
                <td className="py-2 px-3 text-right">{fmt(r.remaining_years, 1)}</td>
                <td className="py-2 px-3 text-right">{fmt(r.decline_rate_pct, 1)}</td>
                <td className="py-2 px-3 text-right">{r.new_wells_needed}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
