import { useEffect, useState } from 'react'
import {
  ComposedChart,
  Bar,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
} from 'recharts'
import { BatteryCharging, TrendingUp } from 'lucide-react'
import { api, BatteryEconomicsDashboard, BatteryUnit, ArbitrageOpportunity, BatteryArbitrageSlot } from '../api/client'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function fmt(n: number, decimals = 0): string {
  return n.toLocaleString('en-AU', { minimumFractionDigits: decimals, maximumFractionDigits: decimals })
}

function fmtAud(n: number): string {
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `$${(n / 1_000).toFixed(1)}K`
  return `$${fmt(n, 0)}`
}

function regionChip(region: string) {
  const colours: Record<string, string> = {
    NSW1: 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200',
    QLD1: 'bg-amber-100 text-amber-800 dark:bg-amber-900 dark:text-amber-200',
    VIC1: 'bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200',
    SA1:  'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200',
    TAS1: 'bg-cyan-100 text-cyan-800 dark:bg-cyan-900 dark:text-cyan-200',
  }
  const cls = colours[region] ?? 'bg-gray-100 text-gray-800'
  return (
    <span className={`inline-block px-2 py-0.5 rounded text-xs font-semibold ${cls}`}>
      {region}
    </span>
  )
}

function techChip(technology: string) {
  const colours: Record<string, string> = {
    'Li-Ion':       'bg-indigo-100 text-indigo-800 dark:bg-indigo-900 dark:text-indigo-200',
    'Flow Battery': 'bg-teal-100 text-teal-800 dark:bg-teal-900 dark:text-teal-200',
    'Pumped Hydro': 'bg-sky-100 text-sky-800 dark:bg-sky-900 dark:text-sky-200',
  }
  const cls = colours[technology] ?? 'bg-gray-100 text-gray-800'
  return (
    <span className={`inline-block px-2 py-0.5 rounded text-xs font-semibold ${cls}`}>
      {technology}
    </span>
  )
}

function spreadColour(spread: number): string {
  if (spread >= 350) return 'text-green-600 dark:text-green-400 font-bold'
  if (spread >= 200) return 'text-amber-600 dark:text-amber-400 font-semibold'
  return 'text-red-500 dark:text-red-400'
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
    <div className="bg-white dark:bg-gray-800 rounded-xl shadow p-5 flex items-center gap-4">
      <div className={`${iconBg} rounded-lg p-3 shrink-0`}>{icon}</div>
      <div>
        <p className="text-xs text-gray-500 dark:text-gray-400 mb-0.5">{label}</p>
        <p className="text-2xl font-bold text-gray-900 dark:text-white">{value}</p>
        {sub && <p className="text-xs text-gray-400 dark:text-gray-500 mt-0.5">{sub}</p>}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Revenue stack pie chart
// ---------------------------------------------------------------------------

const PIE_COLOURS = ['#f59e0b', '#3b82f6', '#22c55e']

interface RevenuePieProps {
  energyPct: number
  fcasPct: number
  srasPct: number
}

function RevenuePie({ energyPct, fcasPct, srasPct }: RevenuePieProps) {
  const data = [
    { name: 'Energy Arbitrage', value: energyPct },
    { name: 'FCAS',             value: fcasPct   },
    { name: 'SRAS',             value: srasPct   },
  ]
  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl shadow p-5">
      <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-200 mb-4">
        Revenue Stack (% today)
      </h2>
      <ResponsiveContainer width="100%" height={220}>
        <PieChart>
          <Pie
            data={data}
            cx="50%"
            cy="50%"
            innerRadius={55}
            outerRadius={85}
            paddingAngle={3}
            dataKey="value"
            label={({ name, value }) => `${name}: ${value}%`}
            labelLine={false}
          >
            {data.map((_, i) => (
              <Cell key={i} fill={PIE_COLOURS[i % PIE_COLOURS.length]} />
            ))}
          </Pie>
          <Tooltip formatter={(val: number) => `${val}%`} />
          <Legend />
        </PieChart>
      </ResponsiveContainer>
    </div>
  )
}

// ---------------------------------------------------------------------------
// 24-hour dispatch schedule chart
// ---------------------------------------------------------------------------

interface DispatchChartProps {
  schedule: BatteryArbitrageSlot[]
}

// Custom bar colour — green for discharge, red for charge
function BarColour(_props: unknown, schedule: BatteryArbitrageSlot[]): string[] {
  return schedule.map(s => {
    if (s.action === 'DISCHARGE') return '#22c55e'
    if (s.action === 'CHARGE') return '#ef4444'
    return '#9ca3af'
  })
}

function DispatchChart({ schedule }: DispatchChartProps) {
  const barColours = schedule.map(s => {
    if (s.action === 'DISCHARGE') return '#22c55e'
    if (s.action === 'CHARGE') return '#ef4444'
    return '#9ca3af'
  })

  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl shadow p-5">
      <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-200 mb-1">
        24-Hour Dispatch Schedule — Waratah Super Battery (NSW1)
      </h2>
      <p className="text-xs text-gray-400 dark:text-gray-500 mb-4">
        Green bars = discharge (positive MW), Red bars = charge (negative MW). Amber line = spot price (right axis).
      </p>
      <ResponsiveContainer width="100%" height={280}>
        <ComposedChart data={schedule} margin={{ top: 5, right: 40, left: 10, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.3} />
          <XAxis dataKey="time_label" tick={{ fontSize: 10 }} />
          <YAxis
            yAxisId="left"
            label={{ value: 'MW', angle: -90, position: 'insideLeft', fontSize: 11 }}
            tick={{ fontSize: 10 }}
          />
          <YAxis
            yAxisId="right"
            orientation="right"
            label={{ value: '$/MWh', angle: 90, position: 'insideRight', fontSize: 11 }}
            tick={{ fontSize: 10 }}
          />
          <Tooltip
            formatter={(val: number, name: string) => {
              if (name === 'Spot Price') return [`$${fmt(val, 0)}/MWh`, name]
              return [`${fmt(val, 0)} MW`, name]
            }}
          />
          <Legend />
          {/* Coloured bars per slot */}
          {schedule.map((slot, i) => (
            <Bar
              key={slot.hour}
              yAxisId="left"
              dataKey="power_mw"
              name="Power (MW)"
              fill={barColours[i]}
              hide={i !== 0}
              data={[slot]}
            />
          ))}
          {/* Single Bar element for all slots with custom fill */}
          <Bar
            yAxisId="left"
            dataKey="power_mw"
            name="Power (MW)"
            isAnimationActive={false}
            label={false}
          >
            {schedule.map((slot, i) => (
              <Cell key={`cell-${i}`} fill={barColours[i]} />
            ))}
          </Bar>
          <Line
            yAxisId="right"
            type="monotone"
            dataKey="spot_price"
            name="Spot Price"
            stroke="#f59e0b"
            strokeWidth={2}
            dot={false}
          />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Battery fleet table
// ---------------------------------------------------------------------------

interface BatteryFleetTableProps {
  batteries: BatteryUnit[]
}

function BatteryFleetTable({ batteries }: BatteryFleetTableProps) {
  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl shadow p-5">
      <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-200 mb-4">
        Battery Fleet
      </h2>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-xs text-gray-500 dark:text-gray-400 border-b border-gray-200 dark:border-gray-700">
              <th className="text-left pb-2 pr-3">Station Name</th>
              <th className="text-left pb-2 pr-3">Region</th>
              <th className="text-left pb-2 pr-3">Technology</th>
              <th className="text-right pb-2 pr-3">Capacity (MWh)</th>
              <th className="text-right pb-2 pr-3">Power (MW)</th>
              <th className="text-left pb-2 pr-3 w-40">SOC %</th>
              <th className="text-right pb-2 pr-3">Today Revenue</th>
              <th className="text-right pb-2">LCOE ($/MWh)</th>
            </tr>
          </thead>
          <tbody>
            {batteries.map(b => (
              <tr
                key={b.bess_id}
                className="border-b border-gray-100 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-750"
              >
                <td className="py-2.5 pr-3 font-medium text-gray-800 dark:text-gray-100 whitespace-nowrap">
                  {b.station_name}
                </td>
                <td className="py-2.5 pr-3">{regionChip(b.region)}</td>
                <td className="py-2.5 pr-3">{techChip(b.technology)}</td>
                <td className="py-2.5 pr-3 text-right tabular-nums">{fmt(b.capacity_mwh, 0)}</td>
                <td className="py-2.5 pr-3 text-right tabular-nums">{fmt(b.power_mw, 0)}</td>
                <td className="py-2.5 pr-3">
                  <div className="flex items-center gap-2">
                    <div className="flex-1 h-2 bg-gray-200 dark:bg-gray-600 rounded-full overflow-hidden">
                      <div
                        className="h-2 bg-amber-400 rounded-full"
                        style={{ width: `${b.soc_current_pct}%` }}
                      />
                    </div>
                    <span className="text-xs tabular-nums text-gray-600 dark:text-gray-300 w-8 text-right">
                      {b.soc_current_pct}%
                    </span>
                  </div>
                </td>
                <td className="py-2.5 pr-3 text-right tabular-nums text-green-600 dark:text-green-400 font-semibold">
                  {fmtAud(b.total_revenue_today)}
                </td>
                <td className="py-2.5 text-right tabular-nums text-gray-700 dark:text-gray-300">
                  ${fmt(b.lcoe_aud_mwh, 1)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Arbitrage opportunities table
// ---------------------------------------------------------------------------

interface ArbitrageTableProps {
  opportunities: ArbitrageOpportunity[]
}

function ArbitrageTable({ opportunities }: ArbitrageTableProps) {
  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl shadow p-5">
      <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-200 mb-4">
        Arbitrage Opportunities by Region
      </h2>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-xs text-gray-500 dark:text-gray-400 border-b border-gray-200 dark:border-gray-700">
              <th className="text-left pb-2 pr-3">Region</th>
              <th className="text-right pb-2 pr-3">Peak Price ($/MWh)</th>
              <th className="text-right pb-2 pr-3">Off-Peak ($/MWh)</th>
              <th className="text-right pb-2 pr-3">Spread ($/MWh)</th>
              <th className="text-right pb-2 pr-3">Optimal Cycles</th>
              <th className="text-right pb-2 pr-3">Max Rev ($/MW/day)</th>
              <th className="text-right pb-2">Captured %</th>
            </tr>
          </thead>
          <tbody>
            {opportunities.map(o => (
              <tr
                key={o.region}
                className="border-b border-gray-100 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-750"
              >
                <td className="py-2.5 pr-3">{regionChip(o.region)}</td>
                <td className="py-2.5 pr-3 text-right tabular-nums text-red-600 dark:text-red-400 font-semibold">
                  ${fmt(o.peak_price, 0)}
                </td>
                <td className="py-2.5 pr-3 text-right tabular-nums text-blue-600 dark:text-blue-400">
                  ${fmt(o.off_peak_price, 0)}
                </td>
                <td className={`py-2.5 pr-3 text-right tabular-nums ${spreadColour(o.spread)}`}>
                  ${fmt(o.spread, 0)}
                </td>
                <td className="py-2.5 pr-3 text-right tabular-nums text-gray-700 dark:text-gray-300">
                  {o.optimal_cycles.toFixed(1)}
                </td>
                <td className="py-2.5 pr-3 text-right tabular-nums text-gray-700 dark:text-gray-300">
                  ${fmt(o.theoretical_max_revenue_mw, 0)}
                </td>
                <td className="py-2.5 text-right tabular-nums">
                  <span className={o.actual_captured_pct >= 70 ? 'text-green-600 dark:text-green-400 font-semibold' : 'text-amber-600 dark:text-amber-400'}>
                    {o.actual_captured_pct.toFixed(1)}%
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main page component
// ---------------------------------------------------------------------------

export default function BatteryEconomics() {
  const [dashboard, setDashboard] = useState<BatteryEconomicsDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    api.getBatteryEconomicsDashboard()
      .then(data => { if (!cancelled) { setDashboard(data); setLoading(false) } })
      .catch(err => { if (!cancelled) { setError(String(err)); setLoading(false) } })
    return () => { cancelled = true }
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full text-gray-500 dark:text-gray-400">
        Loading battery economics data…
      </div>
    )
  }

  if (error || !dashboard) {
    return (
      <div className="p-6 text-red-500 dark:text-red-400">
        Error loading battery economics: {error ?? 'No data'}
      </div>
    )
  }

  const fleetGwh = (dashboard.total_fleet_capacity_mwh / 1000).toFixed(2)

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div className="flex items-center gap-3">
          <BatteryCharging className="text-amber-400" size={28} />
          <div>
            <h1 className="text-xl font-bold text-gray-900 dark:text-white">
              Battery Arbitrage &amp; Economics
            </h1>
            <p className="text-xs text-gray-500 dark:text-gray-400">
              NEM utility-scale BESS fleet — revenue stacking, arbitrage &amp; dispatch analytics
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2 flex-wrap">
          <span className="inline-flex items-center gap-1.5 bg-amber-50 dark:bg-amber-900/30 text-amber-700 dark:text-amber-300 text-xs font-semibold px-3 py-1.5 rounded-full border border-amber-200 dark:border-amber-700">
            <TrendingUp size={12} />
            Best Region: {dashboard.best_arbitrage_region}
          </span>
          <span className="inline-flex items-center gap-1.5 bg-green-50 dark:bg-green-900/30 text-green-700 dark:text-green-300 text-xs font-semibold px-3 py-1.5 rounded-full border border-green-200 dark:border-green-700">
            Best Spread: ${fmt(dashboard.best_spread_today, 0)}/MWh
          </span>
        </div>
      </div>

      {/* KPI cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <KpiCard
          label="Fleet Capacity"
          value={`${fleetGwh} GWh`}
          sub={`${fmt(dashboard.total_fleet_power_mw, 0)} MW total power`}
          icon={<BatteryCharging size={22} className="text-amber-500" />}
          iconBg="bg-amber-50 dark:bg-amber-900/30"
        />
        <KpiCard
          label="Fleet Revenue Today"
          value={fmtAud(dashboard.fleet_revenue_today_aud)}
          sub="Energy + FCAS + SRAS"
          icon={<TrendingUp size={22} className="text-green-500" />}
          iconBg="bg-green-50 dark:bg-green-900/30"
        />
        <KpiCard
          label="Avg Round-Trip Efficiency"
          value={`${dashboard.avg_roundtrip_efficiency_pct.toFixed(1)}%`}
          sub="Charge-to-discharge efficiency"
          icon={<BatteryCharging size={22} className="text-blue-500" />}
          iconBg="bg-blue-50 dark:bg-blue-900/30"
        />
        <KpiCard
          label="FCAS Revenue %"
          value={`${dashboard.fcas_pct.toFixed(1)}%`}
          sub={`Energy ${dashboard.energy_pct.toFixed(1)}% | SRAS ${dashboard.sras_pct.toFixed(1)}%`}
          icon={<TrendingUp size={22} className="text-purple-500" />}
          iconBg="bg-purple-50 dark:bg-purple-900/30"
        />
      </div>

      {/* Revenue pie + dispatch schedule */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-1">
          <RevenuePie
            energyPct={dashboard.energy_pct}
            fcasPct={dashboard.fcas_pct}
            srasPct={dashboard.sras_pct}
          />
        </div>
        <div className="lg:col-span-2">
          <DispatchChart schedule={dashboard.dispatch_schedule ?? []} />
        </div>
      </div>

      {/* Fleet table */}
      <BatteryFleetTable batteries={dashboard.batteries} />

      {/* Arbitrage opportunities */}
      <ArbitrageTable opportunities={dashboard.opportunities} />
    </div>
  )
}
