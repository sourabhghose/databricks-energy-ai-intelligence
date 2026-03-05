import React, { useState, useEffect, useCallback } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from 'recharts'
import { AlertTriangle, TrendingUp, Zap, BarChart2, RefreshCw } from 'lucide-react'
import { api, PriceSpikeEvent, SpikeAnalysisSummary } from '../api/client'

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const NEM_REGIONS = ['NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1'] as const
type Region = typeof NEM_REGIONS[number]

type SpikeFilter = 'all' | 'high' | 'voll' | 'negative'

const CPT_THRESHOLD = 1_359_100

// ---------------------------------------------------------------------------
// Helper utilities
// ---------------------------------------------------------------------------

function formatPrice(value: number): string {
  return new Intl.NumberFormat('en-AU', {
    style: 'currency',
    currency: 'AUD',
    maximumFractionDigits: 0,
  }).format(value)
}

function formatPriceDecimal(value: number): string {
  return new Intl.NumberFormat('en-AU', {
    style: 'currency',
    currency: 'AUD',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value)
}

function formatTime(isoString: string): string {
  const hasTz = /Z|[+-]\d{2}:\d{2}$/.test(isoString)
  const d = new Date(hasTz ? isoString : isoString + 'Z')
  return d.toLocaleTimeString('en-AU', {
    timeZone: 'Australia/Sydney',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  })
}

function formatDateTime(isoString: string): string {
  const hasTz = /Z|[+-]\d{2}:\d{2}$/.test(isoString)
  const d = new Date(hasTz ? isoString : isoString + 'Z')
  return d.toLocaleString('en-AU', {
    timeZone: 'Australia/Sydney',
    day: '2-digit',
    month: 'short',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  })
}

function cptColor(pct: number): string {
  if (pct >= 80) return 'text-red-600 dark:text-red-400'
  if (pct >= 50) return 'text-amber-600 dark:text-amber-400'
  return 'text-green-600 dark:text-green-400'
}

function cptBarColor(pct: number): string {
  if (pct >= 80) return 'bg-red-500'
  if (pct >= 50) return 'bg-amber-500'
  return 'bg-green-500'
}

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

interface SpikeBadgeProps {
  type: string
}

function SpikeBadge({ type }: SpikeBadgeProps) {
  if (type === 'voll') {
    return (
      <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200">
        VOLL
      </span>
    )
  }
  if (type === 'high') {
    return (
      <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200">
        High
      </span>
    )
  }
  return (
    <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200">
      Negative
    </span>
  )
}

interface StatusBadgeProps {
  resolved: boolean
}

function StatusBadge({ resolved }: StatusBadgeProps) {
  if (resolved) {
    return (
      <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-gray-100 text-gray-600 dark:bg-gray-700 dark:text-gray-300">
        Resolved
      </span>
    )
  }
  return (
    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium bg-red-100 text-red-700 dark:bg-red-900 dark:text-red-300">
      <span className="w-1.5 h-1.5 rounded-full bg-red-500 animate-pulse inline-block" />
      Active
    </span>
  )
}

// ---------------------------------------------------------------------------
// Main page component
// ---------------------------------------------------------------------------

export default function PriceAnalysis() {
  const [selectedRegion, setSelectedRegion] = useState<Region>('NSW1')
  const [spikeFilter, setSpikeFilter] = useState<SpikeFilter>('all')
  const [spikes, setSpikes] = useState<PriceSpikeEvent[]>([])
  const [summary, setSummary] = useState<SpikeAnalysisSummary | null>(null)
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)

  const fetchData = useCallback(async (isRefresh = false) => {
    if (isRefresh) setRefreshing(true)
    else setLoading(true)
    setError(null)

    try {
      const [spikesData, summaryData] = await Promise.all([
        api.getPriceSpikes(selectedRegion, 24, spikeFilter === 'all' ? undefined : spikeFilter),
        api.getVolatilityStats(),
      ])
      setSpikes(spikesData)
      setSummary(summaryData)
      setLastUpdated(new Date())
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch price analysis data')
    } finally {
      setLoading(false)
      setRefreshing(false)
    }
  }, [selectedRegion, spikeFilter])

  // Refetch when region or filter changes
  useEffect(() => {
    fetchData()
  }, [fetchData])

  // Auto-refresh every 60 seconds
  useEffect(() => {
    const timer = setInterval(() => fetchData(true), 60_000)
    return () => clearInterval(timer)
  }, [fetchData])

  // ---------------------------------------------------------------------------
  // Derive per-region stats for the selected region
  // ---------------------------------------------------------------------------
  const regionStats = summary?.regions.find(r => r.region === selectedRegion) ?? null

  // ---------------------------------------------------------------------------
  // Chart data — mean + p95 per region for bar chart
  // ---------------------------------------------------------------------------
  const chartData = summary?.regions.map(r => ({
    region: r.region,
    'Mean Price': Math.round(r.mean_price),
    'P95 Price':  Math.round(r.p95_price),
  })) ?? []

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------

  return (
    <div className="p-6 space-y-6">
      {/* ------------------------------------------------------------------ */}
      {/* Header                                                               */}
      {/* ------------------------------------------------------------------ */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div className="flex items-center gap-3">
          <div className="p-2 rounded-lg bg-red-100 dark:bg-red-900">
            <BarChart2 size={20} className="text-red-600 dark:text-red-400" />
          </div>
          <div>
            <h1 className="text-xl font-bold text-gray-900 dark:text-gray-100">
              Price Spike &amp; Volatility Analysis
            </h1>
            <p className="text-sm text-gray-500 dark:text-gray-400">
              NEM price spike events, volatility statistics, and CPT tracking
            </p>
          </div>
        </div>
        <div className="flex items-center gap-3">
          {lastUpdated && (
            <span className="text-xs text-gray-400 dark:text-gray-500">
              Updated {formatTime(lastUpdated.toISOString())}
            </span>
          )}
          <button
            onClick={() => fetchData(true)}
            disabled={refreshing}
            className="flex items-center gap-2 px-3 py-1.5 rounded-md text-sm font-medium bg-gray-100 dark:bg-gray-800 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-700 disabled:opacity-50 transition-colors"
          >
            <RefreshCw size={14} className={refreshing ? 'animate-spin' : ''} />
            Refresh
          </button>
        </div>
      </div>

      {/* ------------------------------------------------------------------ */}
      {/* Region tabs                                                          */}
      {/* ------------------------------------------------------------------ */}
      <div className="flex gap-1 border-b border-gray-200 dark:border-gray-700">
        {NEM_REGIONS.map(region => (
          <button
            key={region}
            onClick={() => setSelectedRegion(region)}
            className={[
              'px-4 py-2 text-sm font-medium rounded-t-md transition-colors',
              selectedRegion === region
                ? 'bg-white dark:bg-gray-800 border border-b-white dark:border-gray-700 dark:border-b-gray-800 text-blue-600 dark:text-blue-400 -mb-px'
                : 'text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-200',
            ].join(' ')}
          >
            {region}
          </button>
        ))}
      </div>

      {/* ------------------------------------------------------------------ */}
      {/* Error banner                                                         */}
      {/* ------------------------------------------------------------------ */}
      {error && (
        <div className="flex items-center gap-2 p-3 rounded-lg bg-amber-50 dark:bg-amber-900/30 border border-amber-200 dark:border-amber-700 text-amber-800 dark:text-amber-300 text-sm">
          <AlertTriangle size={16} />
          <span>{error} — showing cached or mock data</span>
        </div>
      )}

      {/* ------------------------------------------------------------------ */}
      {/* Loading skeleton                                                     */}
      {/* ------------------------------------------------------------------ */}
      {loading && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {[...Array(4)].map((_, i) => (
            <div key={i} className="h-24 rounded-lg bg-gray-200 dark:bg-gray-700 animate-pulse" />
          ))}
        </div>
      )}

      {/* ------------------------------------------------------------------ */}
      {/* Volatility summary cards                                             */}
      {/* ------------------------------------------------------------------ */}
      {!loading && regionStats && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {/* Mean Price */}
          <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
            <div className="flex items-center gap-2 mb-1">
              <TrendingUp size={15} className="text-blue-500" />
              <span className="text-xs font-medium text-gray-500 dark:text-gray-400">Mean Price</span>
            </div>
            <div className="text-2xl font-bold text-gray-900 dark:text-gray-100">
              {formatPriceDecimal(regionStats.mean_price)}
            </div>
            <div className="text-xs text-gray-400 dark:text-gray-500 mt-1">AUD/MWh · 7-day avg</div>
          </div>

          {/* P95 Price */}
          <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
            <div className="flex items-center gap-2 mb-1">
              <TrendingUp size={15} className="text-orange-500" />
              <span className="text-xs font-medium text-gray-500 dark:text-gray-400">95th Percentile</span>
            </div>
            <div className="text-2xl font-bold text-gray-900 dark:text-gray-100">
              {formatPrice(regionStats.p95_price)}
            </div>
            <div className="text-xs text-gray-400 dark:text-gray-500 mt-1">AUD/MWh · 7-day P95</div>
          </div>

          {/* Spike Events (24h) */}
          <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
            <div className="flex items-center gap-2 mb-1">
              <Zap size={15} className="text-red-500" />
              <span className="text-xs font-medium text-gray-500 dark:text-gray-400">Spike Events (24h)</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-2xl font-bold text-gray-900 dark:text-gray-100">
                {spikes.length}
              </span>
              {spikes.length > 5 && (
                <span className="px-1.5 py-0.5 rounded text-xs font-semibold bg-red-100 text-red-700 dark:bg-red-900 dark:text-red-300">
                  High
                </span>
              )}
            </div>
            <div className="text-xs text-gray-400 dark:text-gray-500 mt-1">Intervals &gt;$300/MWh</div>
          </div>

          {/* CPT Utilised */}
          <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
            <div className="flex items-center gap-2 mb-1">
              <AlertTriangle size={15} className="text-amber-500" />
              <span className="text-xs font-medium text-gray-500 dark:text-gray-400">CPT Utilised</span>
            </div>
            <div className={`text-2xl font-bold ${cptColor(regionStats.cpt_utilised_pct)}`}>
              {regionStats.cpt_utilised_pct.toFixed(1)}%
            </div>
            <div className="text-xs text-gray-400 dark:text-gray-500 mt-1">7-day rolling sum</div>
          </div>
        </div>
      )}

      {/* ------------------------------------------------------------------ */}
      {/* CPT Progress Bar                                                     */}
      {/* ------------------------------------------------------------------ */}
      {!loading && regionStats && (
        <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-5">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <AlertTriangle size={16} className="text-amber-500" />
              <span className="text-sm font-semibold text-gray-800 dark:text-gray-200">
                Cumulative Price Threshold (CPT) — {selectedRegion}
              </span>
            </div>
            <span className={`text-sm font-bold ${cptColor(regionStats.cpt_utilised_pct)}`}>
              {new Intl.NumberFormat('en-AU', { style: 'currency', currency: 'AUD', maximumFractionDigits: 0 }).format(regionStats.cumulative_price_current)}
              &nbsp;/&nbsp;
              {new Intl.NumberFormat('en-AU', { style: 'currency', currency: 'AUD', maximumFractionDigits: 0 }).format(CPT_THRESHOLD)}
            </span>
          </div>

          {/* Bar */}
          <div className="w-full h-4 rounded-full bg-gray-200 dark:bg-gray-700 overflow-hidden">
            <div
              className={`h-full rounded-full transition-all duration-700 ${cptBarColor(regionStats.cpt_utilised_pct)}`}
              style={{ width: `${Math.min(regionStats.cpt_utilised_pct, 100)}%` }}
            />
          </div>

          {/* Labels */}
          <div className="flex items-center justify-between mt-1.5">
            <span className="text-xs text-gray-400 dark:text-gray-500">$0</span>
            <span className="text-xs text-gray-400 dark:text-gray-500">50%</span>
            <span className="text-xs text-gray-400 dark:text-gray-500">$1,359,100</span>
          </div>

          <p className="mt-3 text-xs text-gray-500 dark:text-gray-400 leading-relaxed">
            The AEMO Cumulative Price Threshold (CPT) is a 7-day rolling sum of spot prices
            for each NEM region. When the CPT is reached, AEMO may suspend the spot market
            and invoke administered pricing to protect market participants.{' '}
            <span className="font-medium text-green-600 dark:text-green-400">Green &lt;50%</span>,{' '}
            <span className="font-medium text-amber-600 dark:text-amber-400">Amber 50–79%</span>,{' '}
            <span className="font-medium text-red-600 dark:text-red-400">Red ≥80% — market suspension risk.</span>
          </p>
        </div>
      )}

      {/* ------------------------------------------------------------------ */}
      {/* Spike Events table                                                   */}
      {/* ------------------------------------------------------------------ */}
      {!loading && (
        <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
          <div className="flex items-center justify-between px-5 py-4 border-b border-gray-200 dark:border-gray-700 flex-wrap gap-3">
            <h2 className="text-sm font-semibold text-gray-800 dark:text-gray-200">
              Price Spike Events — {selectedRegion} (last 24h)
            </h2>
            {/* Filter buttons */}
            <div className="flex gap-1">
              {(['all', 'high', 'voll', 'negative'] as SpikeFilter[]).map(f => (
                <button
                  key={f}
                  onClick={() => setSpikeFilter(f)}
                  className={[
                    'px-3 py-1 rounded-full text-xs font-medium transition-colors',
                    spikeFilter === f
                      ? 'bg-blue-600 text-white'
                      : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400 hover:bg-gray-200 dark:hover:bg-gray-600',
                  ].join(' ')}
                >
                  {f === 'all' ? 'All' : f === 'voll' ? 'VOLL' : f.charAt(0).toUpperCase() + f.slice(1)}
                </button>
              ))}
            </div>
          </div>

          {spikes.length === 0 ? (
            <div className="py-12 text-center text-sm text-gray-400 dark:text-gray-500">
              No spike events found for the selected filter.
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
                <thead>
                  <tr className="bg-gray-50 dark:bg-gray-900/50">
                    {['Time', 'Type', 'Price', 'Duration', 'Cause', 'Status'].map(col => (
                      <th
                        key={col}
                        className="px-4 py-3 text-left text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider"
                      >
                        {col}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100 dark:divide-gray-700/50">
                  {spikes.map(spike => (
                    <tr
                      key={spike.event_id}
                      className="hover:bg-gray-50 dark:hover:bg-gray-700/30 transition-colors"
                    >
                      <td className="px-4 py-3 text-sm text-gray-700 dark:text-gray-300 whitespace-nowrap">
                        {formatDateTime(spike.interval_datetime)}
                      </td>
                      <td className="px-4 py-3">
                        <SpikeBadge type={spike.spike_type} />
                      </td>
                      <td className="px-4 py-3 text-sm font-semibold whitespace-nowrap">
                        <span className={
                          spike.rrp_aud_mwh < 0
                            ? 'text-blue-600 dark:text-blue-400'
                            : spike.rrp_aud_mwh >= 15000
                            ? 'text-red-600 dark:text-red-400'
                            : 'text-orange-600 dark:text-orange-400'
                        }>
                          {formatPrice(spike.rrp_aud_mwh)}/MWh
                        </span>
                      </td>
                      <td className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400 whitespace-nowrap">
                        {spike.duration_minutes} min
                      </td>
                      <td className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400 max-w-xs truncate">
                        {spike.cause}
                      </td>
                      <td className="px-4 py-3">
                        <StatusBadge resolved={spike.resolved} />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      {/* ------------------------------------------------------------------ */}
      {/* Regional Volatility comparison bar chart                             */}
      {/* ------------------------------------------------------------------ */}
      {!loading && chartData.length > 0 && (
        <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-5">
          <div className="flex items-center gap-2 mb-4">
            <BarChart2 size={16} className="text-gray-500 dark:text-gray-400" />
            <h2 className="text-sm font-semibold text-gray-800 dark:text-gray-200">
              Regional Volatility Comparison
            </h2>
          </div>
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={chartData} margin={{ top: 8, right: 16, left: 8, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis
                dataKey="region"
                tick={{ fontSize: 12, fill: '#6b7280' }}
                axisLine={false}
                tickLine={false}
              />
              <YAxis
                tick={{ fontSize: 11, fill: '#6b7280' }}
                axisLine={false}
                tickLine={false}
                tickFormatter={(v: number) => `$${v}`}
              />
              <Tooltip
                formatter={(value: number, name: string) => [`$${value}/MWh`, name]}
                contentStyle={{
                  backgroundColor: '#1f2937',
                  border: '1px solid #374151',
                  borderRadius: '6px',
                  color: '#f9fafb',
                  fontSize: '12px',
                }}
              />
              <Legend
                wrapperStyle={{ fontSize: '12px', paddingTop: '8px' }}
              />
              <Bar dataKey="Mean Price" fill="#3b82f6" radius={[3, 3, 0, 0]} />
              <Bar dataKey="P95 Price"  fill="#f97316" radius={[3, 3, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
          <p className="text-xs text-gray-400 dark:text-gray-500 mt-2">
            SA1 shows highest volatility due to high wind penetration and reliance on gas peakers.
            TAS1 shows lowest volatility due to predominantly hydro dispatch.
          </p>
        </div>
      )}

      {/* ------------------------------------------------------------------ */}
      {/* Regional stats table                                                 */}
      {/* ------------------------------------------------------------------ */}
      {!loading && summary && (
        <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
          <div className="px-5 py-4 border-b border-gray-200 dark:border-gray-700">
            <h2 className="text-sm font-semibold text-gray-800 dark:text-gray-200">
              NEM Regional Volatility Summary (7-day)
            </h2>
          </div>
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
              <thead>
                <tr className="bg-gray-50 dark:bg-gray-900/50">
                  {['Region', 'Mean', 'Std Dev', 'P5', 'P95', 'Spikes', 'Negative', 'VOLL', 'CPT%'].map(col => (
                    <th
                      key={col}
                      className="px-4 py-3 text-left text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider"
                    >
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100 dark:divide-gray-700/50">
                {summary.regions.map(r => (
                  <tr
                    key={r.region}
                    className={[
                      'hover:bg-gray-50 dark:hover:bg-gray-700/30 transition-colors cursor-pointer',
                      r.region === selectedRegion ? 'bg-blue-50 dark:bg-blue-900/20' : '',
                    ].join(' ')}
                    onClick={() => setSelectedRegion(r.region as Region)}
                  >
                    <td className="px-4 py-3 text-sm font-semibold text-gray-800 dark:text-gray-200">
                      {r.region}
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-700 dark:text-gray-300">
                      {formatPriceDecimal(r.mean_price)}
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-700 dark:text-gray-300">
                      ±{formatPrice(r.std_dev)}
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400">
                      {formatPrice(r.p5_price)}
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-600 dark:text-gray-400">
                      {formatPrice(r.p95_price)}
                    </td>
                    <td className="px-4 py-3 text-sm">
                      <span className={r.spike_count > 20 ? 'font-semibold text-orange-600 dark:text-orange-400' : 'text-gray-700 dark:text-gray-300'}>
                        {r.spike_count}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-sm text-blue-600 dark:text-blue-400">
                      {r.negative_count}
                    </td>
                    <td className="px-4 py-3 text-sm text-red-600 dark:text-red-400 font-medium">
                      {r.voll_count}
                    </td>
                    <td className="px-4 py-3 text-sm">
                      <span className={`font-semibold ${cptColor(r.cpt_utilised_pct)}`}>
                        {r.cpt_utilised_pct.toFixed(1)}%
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="px-5 py-3 border-t border-gray-200 dark:border-gray-700">
            <p className="text-xs text-gray-400 dark:text-gray-500">
              Spikes = intervals &gt;$300/MWh · VOLL = intervals &gt;$15,000/MWh ·
              CPT = 7-day cumulative price threshold ($1,359,100). Click a row to select region.
            </p>
          </div>
        </div>
      )}
    </div>
  )
}
