import { useState, useEffect, useRef, useMemo } from 'react'
import GenerationChart from '../components/GenerationChart'
import InterconnectorMap from '../components/InterconnectorMap'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
} from 'recharts'
import { usePriceHistory, useGeneration, useInterconnectors } from '../hooks/useMarketData'
import { exportToCSV } from '../api/client'
import { RefreshCw, AlertCircle } from 'lucide-react'

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const REGIONS = ['NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1'] as const
type Region = typeof REGIONS[number]

const FUEL_COLORS: Record<string, string> = {
  coal:    '#374151',
  gas:     '#F59E0B',
  wind:    '#3B82F6',
  solar:   '#EAB308',
  hydro:   '#06B6D4',
  battery: '#8B5CF6',
  other:   '#9CA3AF',
}

const LS_REGION_KEY = 'liveMarketRegion'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function now24hWindow(): { start: string; end: string } {
  const end   = new Date()
  const start = new Date(end.getTime() - 24 * 60 * 60_000)
  return { start: start.toISOString(), end: end.toISOString() }
}

function formatTime(ts: string): string {
  return new Date(ts).toLocaleTimeString('en-AU', {
    hour:     '2-digit',
    minute:   '2-digit',
    timeZone: 'Australia/Sydney',
  })
}

// ---------------------------------------------------------------------------
// Price spike badge
// ---------------------------------------------------------------------------

interface SpikeBadgeProps {
  price: number
}

function SpikeBadge({ price }: SpikeBadgeProps) {
  if (price > 5000) {
    return (
      <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-bold
                        bg-red-100 text-red-700 border border-red-300 animate-pulse">
        EXTREME SPIKE
      </span>
    )
  }
  if (price > 300) {
    return (
      <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-bold
                        bg-amber-100 text-amber-700 border border-amber-300 animate-pulse">
        SPIKE
      </span>
    )
  }
  return null
}

// ---------------------------------------------------------------------------
// Empty / error state card
// ---------------------------------------------------------------------------

function EmptyStateCard() {
  return (
    <div className="flex flex-col items-center justify-center gap-3 h-52 rounded-lg
                    bg-gray-50 border border-dashed border-gray-200 text-gray-500">
      <AlertCircle size={28} className="text-gray-400" />
      <p className="text-sm text-center px-6">
        Market data temporarily unavailable &mdash; showing indicative values
      </p>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

export default function LiveMarket() {
  // Region state persisted to localStorage
  const [region, setRegion] = useState<Region>(
    () => (localStorage.getItem(LS_REGION_KEY) as Region | null) ?? 'NSW1'
  )

  useEffect(() => {
    localStorage.setItem(LS_REGION_KEY, region)
  }, [region])

  // --- Time window for price history (refreshes every 30 s) ---
  const [timeWindow, setTimeWindow] = useState(now24hWindow)
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)

  useEffect(() => {
    intervalRef.current = setInterval(() => {
      setTimeWindow(now24hWindow())
    }, 30_000)
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current)
    }
  }, [])

  // --- Price history ---
  const { data: priceHistory, loading: priceLoading } = usePriceHistory(
    region,
    timeWindow.start,
    timeWindow.end
  )

  // Latest price (for spike indicator)
  const latestPrice = useMemo(() => {
    if (priceHistory.length > 0) {
      return priceHistory[priceHistory.length - 1].price
    }
    return null
  }, [priceHistory])

  // Fallback synthetic price data when API is unavailable
  const priceData = useMemo(() => {
    if (priceHistory.length > 0) {
      return priceHistory.map(p => ({
        time:  formatTime(p.timestamp),
        price: p.price,
      }))
    }
    // Generate 24 hourly points as placeholder
    return Array.from({ length: 24 }, (_, i) => ({
      time:  `${String(i).padStart(2, '0')}:00`,
      price: 60 + Math.sin(i * 0.5) * 40 + Math.random() * 20,
    }))
  }, [priceHistory])

  const dataUnavailable = !priceLoading && priceHistory.length === 0

  // --- Generation mix (polls every 60 s) ---
  const { data: genData, loading: genLoading } = useGeneration(region, 60_000)

  // --- Interconnector flows (polls every 30 s) ---
  const { data: interconnectors, loading: interLoading } = useInterconnectors(30_000)

  // --- Last refresh indicator ---
  const [lastRefresh, setLastRefresh] = useState<Date>(new Date())
  useEffect(() => {
    if (!priceLoading) setLastRefresh(new Date())
  }, [priceLoading, priceHistory])

  return (
    <div className="p-6 space-y-6 max-w-7xl mx-auto">
      {/* Header row */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div>
          <h2 className="text-xl font-bold text-gray-900">Live Market</h2>
          <p className="text-sm text-gray-500 mt-0.5">
            Real-time NEM generation, prices, and interconnector flows
          </p>
        </div>

        <div className="flex items-center gap-3">
          {/* Refresh indicator */}
          <div className="flex items-center gap-1 text-xs text-gray-400">
            <RefreshCw size={11} className={priceLoading ? 'animate-spin' : ''} />
            <span>
              {priceLoading
                ? 'Refreshing...'
                : `Updated ${lastRefresh.toLocaleTimeString('en-AU', {
                    hour: '2-digit', minute: '2-digit', second: '2-digit',
                    timeZone: 'Australia/Sydney',
                  })} AEST`}
            </span>
          </div>

          {/* Region selector — persisted to localStorage */}
          <select
            value={region}
            onChange={e => setRegion(e.target.value as Region)}
            className="border border-gray-300 rounded-md px-3 py-1.5 text-sm bg-white
                       text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            {REGIONS.map(r => (
              <option key={r} value={r}>{r}</option>
            ))}
          </select>
        </div>
      </div>

      {/* 24-hr price chart with $300 and $5000 reference lines */}
      <section className="bg-white rounded-lg border border-gray-200 p-4">
        <div className="flex items-center gap-3 mb-1">
          <h3 className="text-sm font-semibold text-gray-700">
            {region} &mdash; Spot Price (last 24 hrs)
          </h3>
          {latestPrice !== null && <SpikeBadge price={latestPrice} />}
        </div>
        <p className="text-xs text-gray-400 mb-3">$/MWh &middot; auto-refreshes every 30 s</p>

        {priceLoading && priceHistory.length === 0 ? (
          <div className="h-52 bg-gray-50 rounded animate-pulse flex items-center
                          justify-center text-sm text-gray-400">
            Loading price data...
          </div>
        ) : dataUnavailable ? (
          <EmptyStateCard />
        ) : (
          <ResponsiveContainer width="100%" height={220}>
            <LineChart data={priceData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis
                dataKey="time"
                tick={{ fontSize: 11 }}
                interval={Math.floor(priceData.length / 8)}
              />
              <YAxis
                tick={{ fontSize: 11 }}
                tickFormatter={v => `$${(v as number).toFixed(0)}`}
                width={58}
              />
              <Tooltip
                formatter={(v: number) => [`$${v.toFixed(2)}/MWh`, 'Spot Price']}
                labelFormatter={l => `Time: ${l}`}
              />
              {/* Amber threshold -- $300/MWh */}
              <ReferenceLine
                y={300}
                stroke="#F59E0B"
                strokeDasharray="4 2"
                label={{
                  value: '$300',
                  position: 'insideTopRight',
                  fontSize: 10,
                  fill: '#F59E0B',
                }}
              />
              {/* Red threshold -- $5000/MWh (market price cap) */}
              <ReferenceLine
                y={5000}
                stroke="#EF4444"
                strokeDasharray="4 2"
                label={{
                  value: '$5,000 (cap)',
                  position: 'insideTopRight',
                  fontSize: 10,
                  fill: '#EF4444',
                }}
              />
              <Line
                type="monotone"
                dataKey="price"
                stroke="#3B82F6"
                strokeWidth={2}
                dot={false}
                isAnimationActive={false}
              />
            </LineChart>
          </ResponsiveContainer>
        )}
        {!priceLoading && priceData.length > 0 && (
          <div className="mt-3 flex justify-end">
            <button
              onClick={() => exportToCSV(priceData as unknown as Record<string, unknown>[], 'nem_price_history.csv')}
              className="px-3 py-1.5 rounded-md text-sm font-medium border border-gray-300
                         text-gray-700 bg-white hover:bg-gray-50 transition-colors"
            >
              Export CSV
            </button>
          </div>
        )}
      </section>

      {/* Generation chart */}
      <section className="bg-white rounded-lg border border-gray-200 p-4">
        {genLoading && genData.length === 0 ? (
          <div className="h-72 bg-gray-50 rounded animate-pulse flex items-center
                          justify-center text-sm text-gray-400">
            Loading generation data...
          </div>
        ) : !genLoading && genData.length === 0 ? (
          <EmptyStateCard />
        ) : (
          <GenerationChart region={region} data={genData} />
        )}
      </section>

      {/* Interconnector map */}
      <section className="bg-white rounded-lg border border-gray-200 p-4">
        <h3 className="text-sm font-semibold text-gray-700 mb-3">Interconnector Flows</h3>
        {interLoading && interconnectors.length === 0 ? (
          <div className="h-48 bg-gray-50 rounded animate-pulse flex items-center
                          justify-center text-sm text-gray-400">
            Loading interconnector data...
          </div>
        ) : !interLoading && interconnectors.length === 0 ? (
          <EmptyStateCard />
        ) : (
          <InterconnectorMap flows={interconnectors} />
        )}
      </section>
    </div>
  )
}

// Named export of FUEL_COLORS so GenerationChart and other components can
// import it from this module if needed.
export { FUEL_COLORS }
