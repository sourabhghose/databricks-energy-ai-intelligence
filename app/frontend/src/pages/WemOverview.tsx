import { useEffect, useState } from 'react'
import {
  LineChart,
  Line,
  AreaChart,
  Area,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import { Zap, RefreshCw, AlertCircle, Info, ArrowLeftRight } from 'lucide-react'
import { wemApi } from '../api/client'
import type { WemPrice, WemGeneration } from '../api/client'

// ---------------------------------------------------------------------------
// Fuel type colour/label maps (match pipeline output: uppercase with spaces)
// ---------------------------------------------------------------------------
const FUEL_COLOURS: Record<string, string> = {
  'GAS CCGT': '#f59e0b',
  'GAS OCGT': '#f97316',
  'COAL': '#6b7280',
  'WIND': '#3b82f6',
  'SOLAR': '#eab308',
  'BATTERY': '#14b8a6',
  'DISTILLATE': '#a855f7',
  'HYDRO': '#06b6d4',
}

function fuelColour(key: string): string {
  return FUEL_COLOURS[key] || FUEL_COLOURS[key.toUpperCase()] || '#6b7280'
}

function fuelLabel(key: string): string {
  // "GAS CCGT" → "Gas CCGT", "WIND" → "Wind"
  if (!key) return key
  return key.charAt(0) + key.slice(1).toLowerCase().replace(/ ([a-z])/g, (_, c) => ' ' + c.toUpperCase())
}

const TOOLTIP_STYLE = {
  backgroundColor: '#1f2937',
  border: '1px solid #374151',
  borderRadius: '8px',
  fontSize: '12px',
  color: '#F9FAFB',
}

// ---------------------------------------------------------------------------
// Context note
// ---------------------------------------------------------------------------
function WemContextNote() {
  return (
    <div className="bg-purple-50 dark:bg-purple-900/20 border border-purple-200 dark:border-purple-700 rounded-xl p-4 flex gap-3">
      <Info size={18} className="text-purple-500 shrink-0 mt-0.5" />
      <p className="text-xs text-purple-800 dark:text-purple-300 leading-relaxed">
        <span className="font-semibold">WEM Context:</span> WEM is Western Australia&apos;s
        isolated electricity market operated by AEMO. Market price cap (MCAP) is{' '}
        <span className="font-semibold">$300/MWh</span> vs NEM&apos;s $15,500/MWh. The WEM
        is not interconnected with the NEM and operates under separate market rules with
        30-minute trading intervals.
      </p>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main WemOverview component
// ---------------------------------------------------------------------------
export default function WemOverview() {
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [lastRefresh, setLastRefresh] = useState<Date>(new Date())

  const [priceSummary, setPriceSummary] = useState<{ avg_price: number; max_price: number; min_price: number; vol: number; high_price_intervals: number }>({ avg_price: 0, max_price: 0, min_price: 0, vol: 0, high_price_intervals: 0 })
  const [demandSummary, setDemandSummary] = useState<{ avg_demand: number; peak_demand: number; min_demand: number }>({ avg_demand: 0, peak_demand: 0, min_demand: 0 })
  const [genMix, setGenMix] = useState<Array<{ fuel_type: string; avg_mw: number; total_mw: number }>>([])
  const [prices, setPrices] = useState<WemPrice[]>([])
  const [generation, setGeneration] = useState<WemGeneration[]>([])
  const [demand, setDemand] = useState<Array<{ trading_interval: string; total_demand_mw: number }>>([])
  const [comparison, setComparison] = useState<{
    wem: { avg_price: number; max_price: number; avg_demand_mw: number }
    nem_nsw1: { avg_price: number; max_price: number; avg_demand_mw: number }
    price_differential: number
  } | null>(null)

  const fetchAll = async () => {
    setLoading(true)
    setError(null)
    try {
      const [dashRes, pricesRes, genRes, demandRes, compRes] = await Promise.all([
        wemApi.dashboard(7),
        wemApi.prices(7),
        wemApi.generation(7),
        wemApi.demand(7),
        wemApi.comparison(7),
      ])

      const ps = dashRes.price_summary || {}
      setPriceSummary({
        avg_price: ps.avg_price ?? 0,
        max_price: ps.max_price ?? 0,
        min_price: (ps as Record<string, number>).min_price ?? 0,
        vol: ps.vol ?? 0,
        high_price_intervals: (ps as Record<string, number>).high_price_intervals ?? 0,
      })
      const ds = dashRes.demand_summary || {}
      setDemandSummary({ avg_demand: ds.avg_demand ?? 0, peak_demand: ds.peak_demand ?? 0, min_demand: ds.min_demand ?? 0 })
      setGenMix((dashRes.generation_mix || []) as Array<{ fuel_type: string; avg_mw: number; total_mw: number }>)
      setPrices(pricesRes.prices || [])
      setGeneration(genRes.generation || [])
      setDemand(demandRes.demand || [])

      if (compRes?.wem && compRes?.nem_nsw1) {
        setComparison(compRes)
      }
      setLastRefresh(new Date())
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load WEM data')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchAll()
    const timer = setInterval(fetchAll, 300_000)
    return () => clearInterval(timer)
  }, [])

  // Price chart data
  const priceChartData = prices.map((p) => ({
    time: p.trading_interval.slice(5, 16),
    price: p.balancing_price,
  }))

  // Generation mix stacked area
  const fuelTypes = [...new Set(generation.map((g) => g.fuel_type))]
  const genByInterval: Record<string, Record<string, number>> = {}
  generation.forEach((g) => {
    if (!genByInterval[g.trading_interval]) genByInterval[g.trading_interval] = {}
    genByInterval[g.trading_interval][g.fuel_type] = g.total_mw
  })
  const genChartData = Object.entries(genByInterval)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([interval, fuels]) => ({ time: interval.slice(5, 16), ...fuels }))

  // Demand chart
  const demandChartData = demand.map((d) => ({
    time: d.trading_interval.slice(5, 16),
    demand: d.total_demand_mw,
  }))

  if (loading && prices.length === 0) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="flex flex-col items-center gap-3">
          <RefreshCw size={24} className="text-purple-500 animate-spin" />
          <p className="text-sm text-gray-500 dark:text-gray-400">Loading WEM data...</p>
        </div>
      </div>
    )
  }

  if (error && prices.length === 0) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="flex flex-col items-center gap-3 max-w-sm text-center">
          <AlertCircle size={24} className="text-red-500" />
          <p className="text-sm text-red-600 dark:text-red-400">{error}</p>
          <button onClick={fetchAll} className="px-4 py-2 bg-purple-600 text-white rounded-lg text-sm hover:bg-purple-700 transition-colors">
            Retry
          </button>
        </div>
      </div>
    )
  }

  return (
    <div className="p-6 max-w-7xl mx-auto space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center gap-4">
        <div className="flex items-center gap-3 flex-1">
          <div className="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg">
            <Zap size={20} className="text-purple-600 dark:text-purple-400" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <h1 className="text-lg font-bold text-gray-900 dark:text-gray-100">
                WEM — Western Australia Energy Market
              </h1>
              <span className="px-2 py-0.5 bg-purple-600 text-white text-xs font-bold rounded-full">
                WEM
              </span>
            </div>
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">
              Operated by AEMO · Isolated from NEM · MCAP $300/MWh · 30-min intervals
            </p>
          </div>
        </div>
        <div className="flex items-center gap-3">
          <span className="text-xs text-gray-400 dark:text-gray-500">
            Updated: {lastRefresh.toLocaleTimeString('en-AU', { hour: '2-digit', minute: '2-digit', second: '2-digit' })}
          </span>
          <button
            onClick={fetchAll}
            disabled={loading}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-purple-600 hover:bg-purple-700 disabled:opacity-50 text-white rounded-lg text-xs font-medium transition-colors"
          >
            <RefreshCw size={12} className={loading ? 'animate-spin' : ''} />
            Refresh
          </button>
        </div>
      </div>

      <WemContextNote />

      {/* KPI Cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5">
          <span className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide">Avg Price</span>
          <div className="text-2xl font-bold text-green-500 mt-1">${priceSummary.avg_price.toFixed(2)}</div>
          <span className="text-xs text-gray-500 dark:text-gray-400">$/MWh (7-day)</span>
        </div>
        <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5">
          <span className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide">Max Price</span>
          <div className={`text-2xl font-bold mt-1 ${priceSummary.max_price >= 300 ? 'text-red-500' : 'text-amber-500'}`}>
            ${priceSummary.max_price.toFixed(2)}
          </div>
          <span className="text-xs text-gray-500 dark:text-gray-400">
            $/MWh · {priceSummary.high_price_intervals} high-price intervals
          </span>
        </div>
        <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5">
          <span className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide">Avg Demand</span>
          <div className="text-2xl font-bold text-blue-500 mt-1">{demandSummary.avg_demand.toLocaleString(undefined, { maximumFractionDigits: 0 })}</div>
          <span className="text-xs text-gray-500 dark:text-gray-400">MW</span>
        </div>
        <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5">
          <span className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide">Peak Demand</span>
          <div className="text-2xl font-bold text-yellow-500 mt-1">{demandSummary.peak_demand.toLocaleString(undefined, { maximumFractionDigits: 0 })}</div>
          <span className="text-xs text-gray-500 dark:text-gray-400">MW · Min: {demandSummary.min_demand.toLocaleString(undefined, { maximumFractionDigits: 0 })} MW</span>
        </div>
      </div>

      {/* Balancing Price Chart */}
      <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5">
        <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-200 mb-4">
          Balancing Price (7-Day)
        </h2>
        {priceChartData.length > 0 ? (
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={priceChartData} margin={{ top: 8, right: 20, left: 0, bottom: 8 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.3} />
              <XAxis dataKey="time" tick={{ fontSize: 10, fill: '#9CA3AF' }} interval={Math.max(Math.floor(priceChartData.length / 12), 1)} />
              <YAxis tick={{ fontSize: 10, fill: '#9CA3AF' }} tickFormatter={(v) => `$${v}`} />
              <Tooltip contentStyle={TOOLTIP_STYLE} formatter={(v: number) => [`$${v.toFixed(2)}/MWh`, 'Balancing Price']} />
              <Line type="monotone" dataKey="price" stroke="#3B82F6" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        ) : (
          <p className="text-center py-8 text-gray-400 text-sm">No price data available</p>
        )}
      </div>

      {/* Generation Mix — Stacked Area + Bar */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Stacked Area */}
        <div className="lg:col-span-2 bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5">
          <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-200 mb-4">
            Generation Mix (Stacked)
          </h2>
          {genChartData.length > 0 ? (
            <ResponsiveContainer width="100%" height={300}>
              <AreaChart data={genChartData} margin={{ top: 8, right: 20, left: 0, bottom: 8 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.3} />
                <XAxis dataKey="time" tick={{ fontSize: 10, fill: '#9CA3AF' }} interval={Math.max(Math.floor(genChartData.length / 12), 1)} />
                <YAxis tick={{ fontSize: 10, fill: '#9CA3AF' }} label={{ value: 'MW', angle: -90, fill: '#9CA3AF', position: 'insideLeft' }} />
                <Tooltip contentStyle={TOOLTIP_STYLE} formatter={(v: number, name: string) => [`${v.toFixed(0)} MW`, fuelLabel(name)]} />
                <Legend formatter={(v: string) => fuelLabel(v)} wrapperStyle={{ fontSize: '11px' }} />
                {fuelTypes.map((fuel) => (
                  <Area
                    key={fuel}
                    type="monotone"
                    dataKey={fuel}
                    stackId="gen"
                    stroke={fuelColour(fuel)}
                    fill={fuelColour(fuel)}
                    fillOpacity={0.6}
                    name={fuel}
                  />
                ))}
              </AreaChart>
            </ResponsiveContainer>
          ) : (
            <p className="text-center py-8 text-gray-400 text-sm">No generation data available</p>
          )}
        </div>

        {/* Fuel Mix Bar */}
        <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5">
          <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-200 mb-4">
            Average Generation by Fuel
          </h2>
          {genMix.length > 0 ? (
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={genMix.map((g) => ({ fuel: fuelLabel(g.fuel_type), avg_mw: Math.round(g.avg_mw) }))} layout="vertical" margin={{ top: 8, right: 20, left: 60, bottom: 8 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.3} />
                <XAxis type="number" tick={{ fontSize: 10, fill: '#9CA3AF' }} />
                <YAxis type="category" dataKey="fuel" tick={{ fontSize: 10, fill: '#9CA3AF' }} width={55} />
                <Tooltip contentStyle={TOOLTIP_STYLE} formatter={(v: number) => [`${v.toLocaleString()} MW`, 'Avg']} />
                <Bar dataKey="avg_mw" fill="#3B82F6" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <p className="text-center py-8 text-gray-400 text-sm">No fuel mix data</p>
          )}
        </div>
      </div>

      {/* Demand Chart */}
      <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5">
        <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-200 mb-4">
          Total Demand
        </h2>
        {demandChartData.length > 0 ? (
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={demandChartData} margin={{ top: 8, right: 20, left: 0, bottom: 8 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.3} />
              <XAxis dataKey="time" tick={{ fontSize: 10, fill: '#9CA3AF' }} interval={Math.max(Math.floor(demandChartData.length / 12), 1)} />
              <YAxis tick={{ fontSize: 10, fill: '#9CA3AF' }} label={{ value: 'MW', angle: -90, fill: '#9CA3AF', position: 'insideLeft' }} />
              <Tooltip contentStyle={TOOLTIP_STYLE} formatter={(v: number) => [`${v.toLocaleString()} MW`, 'Demand']} />
              <Line type="monotone" dataKey="demand" stroke="#8b5cf6" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        ) : (
          <p className="text-center py-8 text-gray-400 text-sm">No demand data available</p>
        )}
      </div>

      {/* WEM vs NEM Comparison */}
      {comparison && (
        <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5">
          <div className="flex items-center gap-2 mb-4">
            <ArrowLeftRight size={16} className="text-blue-400" />
            <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-200">WEM vs NEM (NSW1) Comparison</h2>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-5 border border-gray-200 dark:border-gray-700">
              <h3 className="text-xs font-medium text-purple-500 mb-3 uppercase tracking-wide">WEM (WA)</h3>
              <div className="space-y-3 text-sm">
                <div className="flex justify-between">
                  <span className="text-gray-500 dark:text-gray-400">Avg Price</span>
                  <span className="font-medium text-gray-800 dark:text-gray-100">${comparison.wem.avg_price.toFixed(2)}/MWh</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500 dark:text-gray-400">Max Price</span>
                  <span className="font-medium text-gray-800 dark:text-gray-100">${comparison.wem.max_price.toFixed(2)}/MWh</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500 dark:text-gray-400">Avg Demand</span>
                  <span className="font-medium text-gray-800 dark:text-gray-100">{comparison.wem.avg_demand_mw.toLocaleString(undefined, { maximumFractionDigits: 0 })} MW</span>
                </div>
              </div>
            </div>
            <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-5 border border-gray-200 dark:border-gray-700">
              <h3 className="text-xs font-medium text-teal-500 mb-3 uppercase tracking-wide">NEM (NSW1)</h3>
              <div className="space-y-3 text-sm">
                <div className="flex justify-between">
                  <span className="text-gray-500 dark:text-gray-400">Avg Price</span>
                  <span className="font-medium text-gray-800 dark:text-gray-100">${comparison.nem_nsw1.avg_price.toFixed(2)}/MWh</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500 dark:text-gray-400">Max Price</span>
                  <span className="font-medium text-gray-800 dark:text-gray-100">${comparison.nem_nsw1.max_price.toFixed(2)}/MWh</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500 dark:text-gray-400">Avg Demand</span>
                  <span className="font-medium text-gray-800 dark:text-gray-100">{comparison.nem_nsw1.avg_demand_mw.toLocaleString(undefined, { maximumFractionDigits: 0 })} MW</span>
                </div>
              </div>
            </div>
            <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-5 border border-gray-200 dark:border-gray-700 flex flex-col items-center justify-center">
              <span className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-2">Price Differential</span>
              <div className={`text-3xl font-bold ${comparison.price_differential >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                {comparison.price_differential >= 0 ? '+' : ''}{comparison.price_differential.toFixed(2)}
              </div>
              <span className="text-xs text-gray-500 dark:text-gray-400 mt-1">$/MWh (WEM - NEM)</span>
            </div>
          </div>
        </div>
      )}

      {/* Footer stats */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        {[
          { label: 'Volatility (σ)', value: `$${priceSummary.vol.toFixed(2)}/MWh` },
          { label: 'Min Price', value: `$${priceSummary.min_price.toFixed(2)}/MWh` },
          { label: 'Price Intervals', value: `${prices.length}` },
          { label: 'Data Source', value: 'OpenElectricity API' },
        ].map(({ label, value }) => (
          <div key={label} className="bg-gray-50 dark:bg-gray-800/50 rounded-lg border border-gray-200 dark:border-gray-700 px-4 py-3">
            <p className="text-xs text-gray-500 dark:text-gray-400">{label}</p>
            <p className="text-sm font-semibold text-gray-800 dark:text-gray-100 mt-0.5">{value}</p>
          </div>
        ))}
      </div>
    </div>
  )
}
