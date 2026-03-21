import { useEffect, useState, useMemo } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer, ReferenceLine, Cell,
} from 'recharts'
import { Gauge, TrendingUp, Users, DollarSign, Award, RefreshCw, Filter } from 'lucide-react'
import { api } from '../api/client'
import type {
  CauserPaysDashboard,
  CauserPaysContributor,
  FcasPerformanceRecord,
  FcasMarketSummary,
} from '../api/client'

// ---------------------------------------------------------------------------
// Utility helpers
// ---------------------------------------------------------------------------
function fmtAud(val: number): string {
  if (val >= 1_000_000) return `$${(val / 1_000_000).toFixed(2)}M`
  if (val >= 1_000) return `$${(val / 1_000).toFixed(1)}K`
  return `$${val.toFixed(2)}`
}

function fmtMW(val: number): string {
  return `${val.toFixed(2)} MW`
}

function pfColour(pf: number): string {
  if (pf >= 0.9) return 'text-green-600 dark:text-green-400'
  if (pf >= 0.7) return 'text-amber-600 dark:text-amber-400'
  return 'text-red-600 dark:text-red-400'
}

function pfBadgeBg(pf: number): string {
  if (pf >= 0.9) return 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300'
  if (pf >= 0.7) return 'bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300'
  return 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300'
}

function pfBarColour(pf: number): string {
  if (pf >= 0.9) return '#22c55e'
  if (pf >= 0.7) return '#f59e0b'
  return '#ef4444'
}

// ---------------------------------------------------------------------------
// KPI Card
// ---------------------------------------------------------------------------
interface KpiCardProps {
  title: string
  value: string
  sub?: string
  Icon: React.ElementType
  iconClass?: string
}

function KpiCard({ title, value, sub, Icon, iconClass = 'text-blue-500' }: KpiCardProps) {
  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5 flex items-start gap-4 shadow-sm">
      <div className={`p-2.5 rounded-lg bg-gray-100 dark:bg-gray-700 ${iconClass}`}>
        <Icon size={22} />
      </div>
      <div className="min-w-0">
        <p className="text-xs text-gray-500 dark:text-gray-400 font-medium uppercase tracking-wide truncate">{title}</p>
        <p className="text-2xl font-bold text-gray-900 dark:text-white mt-0.5">{value}</p>
        {sub && <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">{sub}</p>}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Performance Chart
// ---------------------------------------------------------------------------
interface PerformanceChartProps {
  records: FcasPerformanceRecord[]
}

function PerformanceChart({ records }: PerformanceChartProps) {
  const data = records.map(r => ({
    name: r.unit_id,
    performance_factor: r.performance_factor,
    actual_mw: r.actual_response_mw,
    required_mw: r.required_response_mw,
    participant: r.participant_name,
    service: r.service,
    region: r.region,
  }))

  interface TooltipPayloadEntry {
    name: string
    value: number
    payload: typeof data[0]
  }

  interface ChartTooltipProps {
    active?: boolean
    payload?: TooltipPayloadEntry[]
    label?: string
  }

  const CustomTooltip = ({ active, payload, label }: ChartTooltipProps) => {
    if (!active || !payload?.length) return null
    const d = payload[0].payload
    return (
      <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg p-3 shadow-lg text-xs">
        <p className="font-semibold text-gray-900 dark:text-white mb-1">{label} — {d.participant}</p>
        <p className="text-gray-600 dark:text-gray-300">Region: {d.region}</p>
        <p className="text-gray-600 dark:text-gray-300">Service: {d.service}</p>
        <p className="text-gray-600 dark:text-gray-300">Required: {d.required_mw.toFixed(2)} MW</p>
        <p className="text-gray-600 dark:text-gray-300">Actual: {d.actual_mw.toFixed(2)} MW</p>
        <p className={`font-bold mt-1 ${pfColour(d.performance_factor)}`}>
          PF: {d.performance_factor.toFixed(4)}
        </p>
      </div>
    )
  }

  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5 shadow-sm">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-semibold text-gray-900 dark:text-white">FCAS Unit Performance Factors</h3>
        <div className="flex items-center gap-3 text-xs">
          <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-sm inline-block bg-green-500"></span>High ≥0.9</span>
          <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-sm inline-block bg-amber-500"></span>Medium 0.7–0.9</span>
          <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-sm inline-block bg-red-500"></span>Low &lt;0.7</span>
        </div>
      </div>
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={data} margin={{ top: 10, right: 20, left: 0, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
          <XAxis dataKey="name" tick={{ fontSize: 11 }} />
          <YAxis domain={[0, 1.05]} tickFormatter={(v: number) => v.toFixed(1)} tick={{ fontSize: 11 }} />
          <Tooltip content={<CustomTooltip />} />
          <ReferenceLine y={0.8} stroke="#6366f1" strokeDasharray="4 4" label={{ value: 'Threshold 0.8', position: 'insideTopRight', fontSize: 10, fill: '#6366f1' }} />
          <Bar dataKey="performance_factor" name="Performance Factor" radius={[3, 3, 0, 0]}>
            {data.map((entry, index) => (
              <Cell key={`cell-${index}`} fill={pfBarColour(entry.performance_factor)} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Contributors Table
// ---------------------------------------------------------------------------
interface ContributorsTableProps {
  contributors: CauserPaysContributor[]
}

const FCAS_SERVICES = ['All', 'RAISE6SEC', 'RAISE60SEC', 'RAISE5MIN', 'LOWER6SEC', 'LOWER60SEC', 'LOWER5MIN']
const REGIONS = ['All', 'NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1']

function ContributorsTable({ contributors }: ContributorsTableProps) {
  const [regionFilter, setRegionFilter] = useState('All')
  const [serviceFilter, setServiceFilter] = useState('All')

  const filtered = useMemo(() => {
    return contributors.filter(c => {
      const regionOk = regionFilter === 'All' || c.region === regionFilter
      const serviceOk = serviceFilter === 'All' || c.fcas_service === serviceFilter
      return regionOk && serviceOk
    })
  }, [contributors, regionFilter, serviceFilter])

  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5 shadow-sm">
      <div className="flex flex-wrap items-center justify-between gap-3 mb-4">
        <h3 className="text-sm font-semibold text-gray-900 dark:text-white flex items-center gap-2">
          <Filter size={15} className="text-gray-400" />
          Causer Pays Contributors
        </h3>
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-1.5">
            <label className="text-xs text-gray-500 dark:text-gray-400">Region:</label>
            <select
              value={regionFilter}
              onChange={e => setRegionFilter(e.target.value)}
              className="text-xs border border-gray-300 dark:border-gray-600 rounded-md px-2 py-1 bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
            >
              {REGIONS.map(r => <option key={r} value={r}>{r}</option>)}
            </select>
          </div>
          <div className="flex items-center gap-1.5">
            <label className="text-xs text-gray-500 dark:text-gray-400">Service:</label>
            <select
              value={serviceFilter}
              onChange={e => setServiceFilter(e.target.value)}
              className="text-xs border border-gray-300 dark:border-gray-600 rounded-md px-2 py-1 bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
            >
              {FCAS_SERVICES.map(s => <option key={s} value={s}>{s}</option>)}
            </select>
          </div>
        </div>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-gray-200 dark:border-gray-700">
              <th className="text-left py-2 px-3 text-gray-500 dark:text-gray-400 font-medium">Participant</th>
              <th className="text-left py-2 px-3 text-gray-500 dark:text-gray-400 font-medium">Region</th>
              <th className="text-left py-2 px-3 text-gray-500 dark:text-gray-400 font-medium">Fuel</th>
              <th className="text-left py-2 px-3 text-gray-500 dark:text-gray-400 font-medium">Service</th>
              <th className="text-right py-2 px-3 text-gray-500 dark:text-gray-400 font-medium">Contribution MW</th>
              <th className="text-right py-2 px-3 text-gray-500 dark:text-gray-400 font-medium">Deviation MW</th>
              <th className="text-center py-2 px-3 text-gray-500 dark:text-gray-400 font-medium">Perf Factor</th>
              <th className="text-right py-2 px-3 text-gray-500 dark:text-gray-400 font-medium">Amount AUD</th>
            </tr>
          </thead>
          <tbody>
            {filtered.length === 0 ? (
              <tr>
                <td colSpan={8} className="py-8 text-center text-gray-400 dark:text-gray-500">No records match filters</td>
              </tr>
            ) : (
              filtered.map((c, idx) => (
                <tr
                  key={`${c.participant_id}-${idx}`}
                  className="border-b border-gray-100 dark:border-gray-700/50 hover:bg-gray-50 dark:hover:bg-gray-700/30 transition-colors"
                >
                  <td className="py-2.5 px-3 font-medium text-gray-900 dark:text-white">{c.participant_name}</td>
                  <td className="py-2.5 px-3 text-gray-600 dark:text-gray-300">{c.region}</td>
                  <td className="py-2.5 px-3 text-gray-600 dark:text-gray-300">{c.fuel_type}</td>
                  <td className="py-2.5 px-3">
                    <span className="inline-block px-1.5 py-0.5 rounded text-[10px] font-medium bg-blue-100 text-blue-800 dark:bg-blue-900/40 dark:text-blue-300">
                      {c.fcas_service}
                    </span>
                  </td>
                  <td className="py-2.5 px-3 text-right text-gray-900 dark:text-white">{fmtMW(c.contribution_mw)}</td>
                  <td className={`py-2.5 px-3 text-right font-medium ${c.deviation_mw >= 0 ? 'text-red-600 dark:text-red-400' : 'text-green-600 dark:text-green-400'}`}>
                    {c.deviation_mw >= 0 ? '+' : ''}{c.deviation_mw.toFixed(2)} MW
                  </td>
                  <td className="py-2.5 px-3 text-center">
                    <span className={`inline-block px-2 py-0.5 rounded-full text-[10px] font-bold ${pfBadgeBg(c.performance_factor)}`}>
                      {c.performance_factor.toFixed(4)}
                    </span>
                  </td>
                  <td className="py-2.5 px-3 text-right font-medium text-gray-900 dark:text-white">{fmtAud(c.causer_pays_amount_aud)}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
      <p className="text-xs text-gray-400 dark:text-gray-500 mt-3">Showing {filtered.length} of {contributors.length} contributors. Period: Q4-2025</p>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Market Summary Table
// ---------------------------------------------------------------------------
interface MarketSummaryTableProps {
  summaries: FcasMarketSummary[]
}

function MarketSummaryTable({ summaries }: MarketSummaryTableProps) {
  function hhiLabel(hhi: number): string {
    if (hhi < 1500) return 'Competitive'
    if (hhi < 2500) return 'Moderate'
    return 'Concentrated'
  }
  function hhiColour(hhi: number): string {
    if (hhi < 1500) return 'text-green-600 dark:text-green-400'
    if (hhi < 2500) return 'text-amber-600 dark:text-amber-400'
    return 'text-red-600 dark:text-red-400'
  }

  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5 shadow-sm">
      <h3 className="text-sm font-semibold text-gray-900 dark:text-white mb-4">FCAS Market Summary — Q4 2025</h3>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-gray-200 dark:border-gray-700">
              <th className="text-left py-2 px-3 text-gray-500 dark:text-gray-400 font-medium">Service</th>
              <th className="text-left py-2 px-3 text-gray-500 dark:text-gray-400 font-medium">Region</th>
              <th className="text-right py-2 px-3 text-gray-500 dark:text-gray-400 font-medium">Volume MW</th>
              <th className="text-right py-2 px-3 text-gray-500 dark:text-gray-400 font-medium">Total Cost</th>
              <th className="text-right py-2 px-3 text-gray-500 dark:text-gray-400 font-medium">Avg Price $/MWh</th>
              <th className="text-right py-2 px-3 text-gray-500 dark:text-gray-400 font-medium">Causer Pays Pool</th>
              <th className="text-center py-2 px-3 text-gray-500 dark:text-gray-400 font-medium">Providers</th>
              <th className="text-right py-2 px-3 text-gray-500 dark:text-gray-400 font-medium">HHI</th>
            </tr>
          </thead>
          <tbody>
            {summaries.map((s, idx) => (
              <tr
                key={`${s.service}-${idx}`}
                className="border-b border-gray-100 dark:border-gray-700/50 hover:bg-gray-50 dark:hover:bg-gray-700/30 transition-colors"
              >
                <td className="py-2.5 px-3">
                  <span className="inline-block px-1.5 py-0.5 rounded text-[10px] font-medium bg-indigo-100 text-indigo-800 dark:bg-indigo-900/40 dark:text-indigo-300">
                    {s.service}
                  </span>
                </td>
                <td className="py-2.5 px-3 text-gray-600 dark:text-gray-300">{s.region}</td>
                <td className="py-2.5 px-3 text-right text-gray-900 dark:text-white">{fmtMW(s.total_volume_mw)}</td>
                <td className="py-2.5 px-3 text-right text-gray-900 dark:text-white">{fmtAud(s.total_cost_aud)}</td>
                <td className="py-2.5 px-3 text-right text-gray-900 dark:text-white">${s.avg_price_aud_mwh.toFixed(2)}</td>
                <td className="py-2.5 px-3 text-right font-medium text-blue-700 dark:text-blue-300">{fmtAud(s.causer_pays_pool_aud)}</td>
                <td className="py-2.5 px-3 text-center text-gray-900 dark:text-white">{s.num_providers}</td>
                <td className="py-2.5 px-3 text-right">
                  <span className={`font-medium ${hhiColour(s.concentration_hhi)}`} title={hhiLabel(s.concentration_hhi)}>
                    {s.concentration_hhi.toFixed(0)}
                    <span className="ml-1 text-[9px] opacity-70">({hhiLabel(s.concentration_hhi)})</span>
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
// Main Page
// ---------------------------------------------------------------------------
export default function CauserPays() {
  const [dashboard, setDashboard] = useState<CauserPaysDashboard | null>(null)
  const [contributors, setContributors] = useState<CauserPaysContributor[]>([])
  const [performance, setPerformance] = useState<FcasPerformanceRecord[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [lastRefresh, setLastRefresh] = useState<Date>(new Date())

  async function fetchAll() {
    setLoading(true)
    setError(null)
    try {
      const [dash, contribs, perf] = await Promise.all([
        api.getCauserPaysDashboard(),
        api.getCauserPaysContributors(),
        api.getFcasPerformance(),
      ])
      setDashboard(dash)
      setContributors(contribs)
      setPerformance(perf)
      setLastRefresh(new Date())
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load data')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchAll()
  }, [])

  return (
    <div className="p-6 space-y-6 max-w-screen-2xl mx-auto">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="p-2.5 rounded-xl bg-indigo-100 dark:bg-indigo-900/40 text-indigo-600 dark:text-indigo-400">
            <Gauge size={24} />
          </div>
          <div>
            <h1 className="text-xl font-bold text-gray-900 dark:text-white">Causer Pays & FCAS Performance</h1>
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">
              NEM frequency control ancillary services performance analytics — Q4 2025
            </p>
          </div>
        </div>
        <div className="flex items-center gap-3">
          <span className="text-xs text-gray-400 dark:text-gray-500">
            Updated: {lastRefresh.toLocaleTimeString('en-AU')}
          </span>
          <button
            onClick={fetchAll}
            disabled={loading}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-lg bg-indigo-600 hover:bg-indigo-700 disabled:opacity-50 text-white transition-colors"
          >
            <RefreshCw size={13} className={loading ? 'animate-spin' : ''} />
            Refresh
          </button>
        </div>
      </div>

      {/* Error */}
      {error && (
        <div className="rounded-lg bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-400">
          {error}
        </div>
      )}

      {/* Loading skeleton */}
      {loading && !dashboard && (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          {[...Array(4)].map((_, i) => (
            <div key={i} className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5 h-28 animate-pulse">
              <div className="h-3 bg-gray-200 dark:bg-gray-700 rounded w-1/2 mb-3"></div>
              <div className="h-7 bg-gray-200 dark:bg-gray-700 rounded w-3/4"></div>
            </div>
          ))}
        </div>
      )}

      {/* KPI Cards */}
      {dashboard && (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          <KpiCard
            title="Causer Pays Pool YTD"
            value={fmtAud(dashboard.total_causer_pays_pool_ytd_aud)}
            sub="Year to date 2025"
            Icon={DollarSign}
            iconClass="text-green-500"
          />
          <KpiCard
            title="Avg Performance Factor"
            value={dashboard.avg_performance_factor.toFixed(4)}
            sub={dashboard.avg_performance_factor >= 0.9 ? 'High compliance' : dashboard.avg_performance_factor >= 0.7 ? 'Moderate compliance' : 'Low compliance'}
            Icon={TrendingUp}
            iconClass={pfColour(dashboard.avg_performance_factor)}
          />
          <KpiCard
            title="Active Providers"
            value={String(dashboard.num_active_providers)}
            sub="Registered FCAS participants"
            Icon={Users}
            iconClass="text-blue-500"
          />
          <KpiCard
            title="Top Performer"
            value={dashboard.highest_performing_participant}
            sub="Highest performance factor"
            Icon={Award}
            iconClass="text-amber-500"
          />
        </div>
      )}

      {/* Performance Chart */}
      {performance.length > 0 && <PerformanceChart records={performance} />}

      {/* Contributors Table */}
      {contributors.length > 0 && <ContributorsTable contributors={contributors} />}

      {/* Market Summary Table */}
      {dashboard && dashboard.market_summaries.length > 0 && (
        <MarketSummaryTable summaries={dashboard.market_summaries} />
      )}

      {/* Info footer */}
      <div className="text-xs text-gray-400 dark:text-gray-500 text-center pb-2">
        Causer Pays data sourced from AEMO FCAS settlement records. Performance factors calculated per FCAS specification.
        HHI (Herfindahl-Hirschman Index): &lt;1500 competitive, 1500–2500 moderate, &gt;2500 concentrated.
      </div>
    </div>
  )
}
