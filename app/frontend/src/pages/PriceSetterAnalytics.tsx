import { useEffect, useState, useCallback } from 'react'
import {
  ComposedChart,
  Bar,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
  PieChart,
  Pie,
  Legend,
} from 'recharts'
import { Target, RefreshCw, DollarSign, Zap, AlertTriangle, TrendingUp } from 'lucide-react'
import { api } from '../api/client'
import type {
  PriceSetterDashboard,
  PriceSetterRecord,
  PriceSetterFrequency,
  FuelTypePriceSetting,
} from '../api/client'

// ---------------------------------------------------------------------------
// Constants & helpers
// ---------------------------------------------------------------------------

const REGIONS = ['SA1', 'NSW1', 'QLD1', 'VIC1', 'TAS1']

const FUEL_COLORS: Record<string, string> = {
  Wind: '#22c55e',
  Solar: '#f59e0b',
  'Gas OCGT': '#f97316',
  'Gas CCGT': '#fb923c',
  Battery: '#a855f7',
  Coal: '#6b7280',
  Hydro: '#3b82f6',
}

const PIE_COLORS = ['#f97316', '#22c55e', '#f59e0b', '#a855f7', '#fb923c', '#6b7280', '#3b82f6']

function fuelBadge(fuel: string) {
  const colorMap: Record<string, string> = {
    Wind: 'bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-300',
    Solar: 'bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300',
    'Gas OCGT': 'bg-orange-100 text-orange-700 dark:bg-orange-900/40 dark:text-orange-300',
    'Gas CCGT': 'bg-orange-100 text-orange-600 dark:bg-orange-900/30 dark:text-orange-300',
    Battery: 'bg-purple-100 text-purple-700 dark:bg-purple-900/40 dark:text-purple-300',
    Coal: 'bg-gray-100 text-gray-600 dark:bg-gray-800 dark:text-gray-300',
    Hydro: 'bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300',
  }
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-semibold ${colorMap[fuel] ?? 'bg-gray-100 text-gray-600'}`}>
      {fuel}
    </span>
  )
}

function regionChip(region: string) {
  const colours: Record<string, string> = {
    SA1: 'bg-rose-100 text-rose-700 dark:bg-rose-900/40 dark:text-rose-300',
    NSW1: 'bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300',
    VIC1: 'bg-purple-100 text-purple-700 dark:bg-purple-900/40 dark:text-purple-300',
    QLD1: 'bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300',
    TAS1: 'bg-teal-100 text-teal-700 dark:bg-teal-900/40 dark:text-teal-300',
  }
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-semibold ${colours[region] ?? 'bg-gray-100 text-gray-600'}`}>
      {region}
    </span>
  )
}

function priceClass(price: number): string {
  if (price < 0) return 'text-blue-600 dark:text-blue-400 font-semibold'
  if (price < 100) return 'text-green-600 dark:text-green-400'
  if (price < 300) return 'text-amber-600 dark:text-amber-400'
  if (price < 1000) return 'text-orange-600 dark:text-orange-400 font-semibold'
  return 'text-red-600 dark:text-red-400 font-bold'
}

function fmt(n: number, dp = 0): string {
  return n.toLocaleString('en-AU', { minimumFractionDigits: dp, maximumFractionDigits: dp })
}

// ---------------------------------------------------------------------------
// KPI card
// ---------------------------------------------------------------------------

interface KpiCardProps {
  title: string
  value: string
  sub?: string
  icon: React.ReactNode
  accent?: string
}

function KpiCard({ title, value, sub, icon, accent = 'text-gray-700 dark:text-gray-200' }: KpiCardProps) {
  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4 flex items-start gap-3">
      <div className="p-2 bg-gray-50 dark:bg-gray-700 rounded-md">{icon}</div>
      <div className="min-w-0">
        <p className="text-xs text-gray-500 dark:text-gray-400 font-medium truncate">{title}</p>
        <p className={`text-xl font-bold ${accent} leading-tight`}>{value}</p>
        {sub && <p className="text-xs text-gray-400 dark:text-gray-500 mt-0.5 truncate">{sub}</p>}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Timeline bar chart tooltip
// ---------------------------------------------------------------------------

function TimelineTooltip({ active, payload, label }: any) {
  if (!active || !payload?.length) return null
  const d = payload[0]?.payload as PriceSetterRecord | undefined
  if (!d) return null
  return (
    <div className="bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-700 rounded-lg p-3 text-xs shadow-lg min-w-[200px]">
      <p className="font-bold text-gray-800 dark:text-gray-100 mb-1">{d.interval}</p>
      <p className="text-gray-600 dark:text-gray-300"><span className="font-semibold">DUID:</span> {d.duid}</p>
      <p className="text-gray-600 dark:text-gray-300"><span className="font-semibold">Station:</span> {d.station_name}</p>
      <p className="text-gray-600 dark:text-gray-300"><span className="font-semibold">Fuel:</span> {d.fuel_type}</p>
      <p className={`font-bold mt-1 ${priceClass(d.dispatch_price)}`}>Price: ${fmt(d.dispatch_price, 2)}/MWh</p>
      <p className="text-gray-600 dark:text-gray-300"><span className="font-semibold">Qty:</span> {fmt(d.dispatch_quantity_mw, 1)} MW</p>
      <p className="text-gray-600 dark:text-gray-300"><span className="font-semibold">Offer Band:</span> {d.offer_band}</p>
      <p className="text-gray-600 dark:text-gray-300"><span className="font-semibold">Offer Price:</span> ${fmt(d.offer_price, 2)}/MWh</p>
      <p className="text-gray-600 dark:text-gray-300"><span className="font-semibold">Shadow $/MW:</span> ${fmt(d.shadow_price_mw, 1)}</p>
      {d.is_strategic && (
        <p className="mt-1 text-red-600 dark:text-red-400 font-bold">Strategic Bid</p>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Pie tooltip
// ---------------------------------------------------------------------------

function PieTooltip({ active, payload }: any) {
  if (!active || !payload?.length) return null
  const d = payload[0]?.payload as FuelTypePriceSetting
  return (
    <div className="bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-700 rounded-lg p-3 text-xs shadow-lg">
      <p className="font-bold text-gray-800 dark:text-gray-100 mb-1">{d.fuel_type}</p>
      <p className="text-gray-600 dark:text-gray-300">{fmt(d.pct_of_all_intervals, 1)}% of intervals</p>
      <p className="text-gray-600 dark:text-gray-300">Avg: ${fmt(d.avg_price_aud_mwh, 0)}/MWh</p>
      <p className="text-gray-600 dark:text-gray-300">Max: ${fmt(d.max_price_aud_mwh, 0)}/MWh</p>
      <p className="text-gray-600 dark:text-gray-300">Econ Rent: ${fmt(d.economic_rent_est_m_aud, 2)}M</p>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export default function PriceSetterAnalytics() {
  const [region, setRegion] = useState('SA1')
  const [dashboard, setDashboard] = useState<PriceSetterDashboard | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [lastRefresh, setLastRefresh] = useState<Date>(new Date())

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const data = await api.getPriceSetterDashboard({ region })
      setDashboard(data)
      setLastRefresh(new Date())
    } catch (e: any) {
      setError(e.message ?? 'Failed to load price setter data')
    } finally {
      setLoading(false)
    }
  }, [region])

  useEffect(() => {
    load()
    const timer = setInterval(load, 30_000)
    return () => clearInterval(timer)
  }, [load])

  // ---------------------------------------------------------------------------
  // Derived chart data
  // ---------------------------------------------------------------------------

  const timelineData = dashboard?.price_setter_records ?? []

  const scatterStrategic = timelineData
    .filter((r) => r.is_strategic)
    .map((r) => ({ ...r }))

  const pieData = (dashboard?.fuel_type_stats ?? []).map((f) => ({
    ...f,
    name: f.fuel_type,
    value: f.pct_of_all_intervals,
  }))

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------

  return (
    <div className="p-6 space-y-6 min-h-full bg-gray-50 dark:bg-gray-900">
      {/* Header */}
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg">
            <Target size={22} className="text-orange-600 dark:text-orange-400" />
          </div>
          <div>
            <h1 className="text-xl font-bold text-gray-900 dark:text-white leading-tight">
              Price Setter &amp; Marginal Generator Analytics
            </h1>
            <p className="text-sm text-gray-500 dark:text-gray-400">
              Identifies which generator unit set the dispatch price in each interval
            </p>
          </div>
        </div>

        <div className="flex items-center gap-3 flex-wrap">
          {/* Region selector */}
          <div className="flex items-center gap-1 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg p-1">
            {REGIONS.map((r) => (
              <button
                key={r}
                onClick={() => setRegion(r)}
                className={`px-3 py-1 rounded text-xs font-semibold transition-colors ${
                  region === r
                    ? 'bg-orange-500 text-white'
                    : 'text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700'
                }`}
              >
                {r}
              </button>
            ))}
          </div>

          {/* Current price setter chip */}
          {dashboard && (
            <span className="px-3 py-1.5 rounded-lg text-sm font-semibold bg-orange-100 text-orange-700 dark:bg-orange-900/40 dark:text-orange-300 flex items-center gap-1.5">
              <Zap size={14} />
              Now: {dashboard.current_price_setter}
            </span>
          )}

          {/* Current price badge */}
          {dashboard && (
            <span className={`px-3 py-1.5 rounded-lg text-sm font-bold ${
              dashboard.current_price > 300
                ? 'bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-300'
                : 'bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-300'
            }`}>
              ${fmt(dashboard.current_price, 0)}/MWh
            </span>
          )}

          <button
            onClick={load}
            disabled={loading}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-medium bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 disabled:opacity-50 transition-colors"
          >
            <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
            Refresh
          </button>
        </div>
      </div>

      {/* Last updated */}
      <p className="text-xs text-gray-400 dark:text-gray-500 -mt-4">
        Last updated: {lastRefresh.toLocaleTimeString('en-AU', { timeZone: 'Australia/Sydney' })} AEST
      </p>

      {/* Error */}
      {error && (
        <div className="p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg text-sm text-red-700 dark:text-red-400 flex items-center gap-2">
          <AlertTriangle size={16} />
          {error}
        </div>
      )}

      {/* KPI Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <KpiCard
          title="Dominant Price Setter (Today)"
          value={dashboard?.dominant_price_setter ?? '—'}
          sub={`${dashboard?.total_intervals_today ?? 0} total intervals`}
          icon={<Target size={18} className="text-orange-500" />}
          accent="text-orange-600 dark:text-orange-400"
        />
        <KpiCard
          title="Dominant Fuel Type"
          value={dashboard?.dominant_fuel_type ?? '—'}
          sub="by interval count"
          icon={<Zap size={18} className="text-amber-500" />}
          accent="text-amber-600 dark:text-amber-400"
        />
        <KpiCard
          title="Strategic Bid Frequency"
          value={`${dashboard?.strategic_bid_frequency_pct?.toFixed(1) ?? '—'}%`}
          sub="of price-setting intervals"
          icon={<AlertTriangle size={18} className="text-red-500" />}
          accent={
            (dashboard?.strategic_bid_frequency_pct ?? 0) > 20
              ? 'text-red-600 dark:text-red-400'
              : 'text-gray-700 dark:text-gray-200'
          }
        />
        <KpiCard
          title="Avg Price Today"
          value={`$${fmt(dashboard?.avg_price_today ?? 0, 0)}/MWh`}
          sub="volume-weighted average"
          icon={<DollarSign size={18} className="text-green-500" />}
          accent={
            (dashboard?.avg_price_today ?? 0) > 200
              ? 'text-orange-600 dark:text-orange-400'
              : 'text-green-600 dark:text-green-400'
          }
        />
      </div>

      {/* Timeline + Pie row */}
      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
        {/* Price Setter Timeline */}
        <div className="xl:col-span-2 bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-5">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h2 className="text-base font-semibold text-gray-800 dark:text-white">Price Setter Timeline</h2>
              <p className="text-xs text-gray-500 dark:text-gray-400">Dispatch price by interval, coloured by fuel type. Red dots = strategic bids.</p>
            </div>
            <TrendingUp size={18} className="text-gray-400" />
          </div>
          <ResponsiveContainer width="100%" height={300}>
            <ComposedChart data={timelineData} margin={{ top: 10, right: 20, left: 10, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.2} />
              <XAxis
                dataKey="interval"
                tick={{ fontSize: 10, fill: '#6b7280' }}
                interval={3}
                tickLine={false}
              />
              <YAxis
                tickFormatter={(v) => `$${v}`}
                tick={{ fontSize: 10, fill: '#6b7280' }}
                tickLine={false}
                axisLine={false}
              />
              <Tooltip content={<TimelineTooltip />} />
              <Bar dataKey="dispatch_price" name="Dispatch Price" radius={[2, 2, 0, 0]}>
                {timelineData.map((entry, i) => (
                  <Cell
                    key={`cell-${i}`}
                    fill={FUEL_COLORS[entry.fuel_type] ?? '#6b7280'}
                    opacity={entry.is_strategic ? 1 : 0.85}
                  />
                ))}
              </Bar>
              {scatterStrategic.length > 0 && (
                <Scatter
                  data={scatterStrategic}
                  dataKey="dispatch_price"
                  fill="#ef4444"
                  shape={(props: any) => {
                    const { cx, cy } = props
                    return (
                      <circle
                        cx={cx}
                        cy={cy}
                        r={5}
                        fill="#ef4444"
                        stroke="#fff"
                        strokeWidth={1.5}
                      />
                    )
                  }}
                  name="Strategic Bid"
                />
              )}
            </ComposedChart>
          </ResponsiveContainer>
          {/* Legend */}
          <div className="flex flex-wrap gap-3 mt-3">
            {Object.entries(FUEL_COLORS).map(([fuel, color]) => (
              <span key={fuel} className="flex items-center gap-1 text-xs text-gray-500 dark:text-gray-400">
                <span className="w-3 h-3 rounded-sm inline-block" style={{ backgroundColor: color }} />
                {fuel}
              </span>
            ))}
            <span className="flex items-center gap-1 text-xs text-red-500">
              <span className="w-3 h-3 rounded-full inline-block bg-red-500" />
              Strategic Bid
            </span>
          </div>
        </div>

        {/* Fuel Type Pie */}
        <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-5">
          <div className="mb-4">
            <h2 className="text-base font-semibold text-gray-800 dark:text-white">Price-Setting by Fuel Type</h2>
            <p className="text-xs text-gray-500 dark:text-gray-400">% of intervals each fuel type set the price</p>
          </div>
          <ResponsiveContainer width="100%" height={220}>
            <PieChart>
              <Pie
                data={pieData}
                cx="50%"
                cy="50%"
                innerRadius={50}
                outerRadius={90}
                paddingAngle={2}
                dataKey="value"
                nameKey="name"
              >
                {pieData.map((entry, i) => (
                  <Cell
                    key={`pie-${i}`}
                    fill={FUEL_COLORS[entry.fuel_type] ?? PIE_COLORS[i % PIE_COLORS.length]}
                  />
                ))}
              </Pie>
              <Tooltip content={<PieTooltip />} />
              <Legend
                formatter={(value) => (
                  <span className="text-xs text-gray-600 dark:text-gray-300">{value}</span>
                )}
              />
            </PieChart>
          </ResponsiveContainer>

          {/* Fuel stats summary */}
          <div className="mt-3 space-y-2">
            {(dashboard?.fuel_type_stats ?? []).map((f) => (
              <div key={f.fuel_type} className="flex items-center justify-between text-xs">
                <div className="flex items-center gap-1.5">
                  <span
                    className="w-2.5 h-2.5 rounded-full inline-block"
                    style={{ backgroundColor: FUEL_COLORS[f.fuel_type] ?? '#6b7280' }}
                  />
                  <span className="text-gray-700 dark:text-gray-300 font-medium">{f.fuel_type}</span>
                </div>
                <div className="flex items-center gap-3 text-gray-500 dark:text-gray-400">
                  <span>{fmt(f.pct_of_all_intervals, 1)}%</span>
                  <span className={priceClass(f.avg_price_aud_mwh)}>${fmt(f.avg_price_aud_mwh, 0)}</span>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Price-Setter Frequency Table */}
      <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 overflow-hidden">
        <div className="px-5 py-4 border-b border-gray-100 dark:border-gray-700 flex items-center justify-between">
          <div>
            <h2 className="text-base font-semibold text-gray-800 dark:text-white">Generator Price-Setting Frequency</h2>
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">
              Ranked by interval count — generators that most frequently set the marginal price
            </p>
          </div>
          <span className="text-xs text-gray-400 dark:text-gray-500 font-mono">
            {dashboard?.frequency_stats?.length ?? 0} generators
          </span>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50 dark:bg-gray-700/50 text-xs text-gray-500 dark:text-gray-400 font-semibold uppercase tracking-wide">
                <th className="px-4 py-3 text-left">Station</th>
                <th className="px-4 py-3 text-left">Fuel</th>
                <th className="px-4 py-3 text-left">Region</th>
                <th className="px-4 py-3 text-right">Intervals</th>
                <th className="px-4 py-3 text-right">% of Day</th>
                <th className="px-4 py-3 text-right">Avg Price</th>
                <th className="px-4 py-3 text-right">Max Price</th>
                <th className="px-4 py-3 text-right">Strategic %</th>
                <th className="px-4 py-3 text-right">Price Power ($)</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100 dark:divide-gray-700">
              {(dashboard?.frequency_stats ?? []).map((f: PriceSetterFrequency) => (
                <tr
                  key={f.duid}
                  className="hover:bg-gray-50 dark:hover:bg-gray-700/30 transition-colors"
                >
                  <td className="px-4 py-3">
                    <div>
                      <p className="font-semibold text-gray-800 dark:text-gray-100 text-xs">{f.station_name}</p>
                      <p className="text-xs text-gray-400 dark:text-gray-500 font-mono">{f.duid}</p>
                    </div>
                  </td>
                  <td className="px-4 py-3">{fuelBadge(f.fuel_type)}</td>
                  <td className="px-4 py-3">{regionChip(f.region)}</td>
                  <td className="px-4 py-3 text-right font-semibold text-gray-800 dark:text-gray-100">
                    {f.intervals_as_price_setter}
                  </td>
                  <td className="px-4 py-3 text-right text-gray-600 dark:text-gray-300">
                    {fmt(f.pct_intervals, 1)}%
                  </td>
                  <td className={`px-4 py-3 text-right font-semibold ${priceClass(f.avg_price_when_setter)}`}>
                    ${fmt(f.avg_price_when_setter, 0)}
                  </td>
                  <td className={`px-4 py-3 text-right font-semibold ${priceClass(f.max_price_when_setter)}`}>
                    ${fmt(f.max_price_when_setter, 0)}
                  </td>
                  <td className="px-4 py-3 text-right">
                    {f.strategic_bids_pct > 0 ? (
                      <span className={`px-2 py-0.5 rounded text-xs font-semibold ${
                        f.strategic_bids_pct > 50
                          ? 'bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-300'
                          : 'bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300'
                      }`}>
                        {fmt(f.strategic_bids_pct, 1)}%
                      </span>
                    ) : (
                      <span className="text-xs text-gray-400">—</span>
                    )}
                  </td>
                  <td className="px-4 py-3 text-right font-semibold text-gray-700 dark:text-gray-200">
                    ${fmt(f.estimated_daily_price_power_aud, 0)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Individual Interval Records Table */}
      <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 overflow-hidden">
        <div className="px-5 py-4 border-b border-gray-100 dark:border-gray-700 flex items-center justify-between">
          <div>
            <h2 className="text-base font-semibold text-gray-800 dark:text-white">Interval Price-Setter Records</h2>
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">
              Individual dispatch intervals showing which generator unit set the price
            </p>
          </div>
          <span className="text-xs text-gray-400 dark:text-gray-500 font-mono">
            {dashboard?.price_setter_records?.length ?? 0} intervals
          </span>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50 dark:bg-gray-700/50 text-xs text-gray-500 dark:text-gray-400 font-semibold uppercase tracking-wide">
                <th className="px-4 py-3 text-left">Interval</th>
                <th className="px-4 py-3 text-left">DUID</th>
                <th className="px-4 py-3 text-left">Station</th>
                <th className="px-4 py-3 text-left">Fuel</th>
                <th className="px-4 py-3 text-right">Dispatch Price</th>
                <th className="px-4 py-3 text-right">Qty (MW)</th>
                <th className="px-4 py-3 text-left">Offer Band</th>
                <th className="px-4 py-3 text-right">Offer Price</th>
                <th className="px-4 py-3 text-center">Strategic</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100 dark:divide-gray-700">
              {(dashboard?.price_setter_records ?? []).map((r: PriceSetterRecord, i: number) => (
                <tr
                  key={`${r.interval}-${i}`}
                  className={`hover:bg-gray-50 dark:hover:bg-gray-700/30 transition-colors ${
                    r.is_strategic ? 'bg-red-50/40 dark:bg-red-900/10' : ''
                  }`}
                >
                  <td className="px-4 py-2.5 font-mono text-xs font-semibold text-gray-700 dark:text-gray-200">
                    {r.interval}
                  </td>
                  <td className="px-4 py-2.5 font-mono text-xs text-gray-500 dark:text-gray-400">
                    {r.duid}
                  </td>
                  <td className="px-4 py-2.5 text-xs text-gray-700 dark:text-gray-300 max-w-[160px] truncate">
                    {r.station_name}
                  </td>
                  <td className="px-4 py-2.5">{fuelBadge(r.fuel_type)}</td>
                  <td className={`px-4 py-2.5 text-right font-bold text-xs ${priceClass(r.dispatch_price)}`}>
                    ${fmt(r.dispatch_price, 2)}
                  </td>
                  <td className="px-4 py-2.5 text-right text-xs text-gray-600 dark:text-gray-300">
                    {fmt(r.dispatch_quantity_mw, 1)}
                  </td>
                  <td className="px-4 py-2.5 text-xs font-mono text-gray-500 dark:text-gray-400">
                    {r.offer_band}
                  </td>
                  <td className="px-4 py-2.5 text-right text-xs text-gray-600 dark:text-gray-300">
                    ${fmt(r.offer_price, 2)}
                  </td>
                  <td className="px-4 py-2.5 text-center">
                    {r.is_strategic ? (
                      <span className="px-2 py-0.5 rounded text-xs font-bold bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-300">
                        Strategic
                      </span>
                    ) : (
                      <span className="text-xs text-gray-400 dark:text-gray-600">—</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
