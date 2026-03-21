import { useEffect, useState } from 'react'
import { AlertTriangle, Zap, Users, DollarSign, Activity } from 'lucide-react'
import {
  ComposedChart,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ReferenceLine,
  Line,
} from 'recharts'
import {
  getPowerSystemEventsDashboard,
  PSEDashboard,
  PSEEventRecord,
  PSEFrequencyRecord,
  PSELoadSheddingRecord,
  PSEAemoActionRecord,
} from '../api/client'

// ── Colour helpers ─────────────────────────────────────────────────────────────

const EVENT_TYPE_COLORS: Record<string, string> = {
  FREQUENCY_EXCURSION: 'bg-purple-700 text-purple-100',
  LOAD_SHEDDING: 'bg-red-700 text-red-100',
  VOLTAGE_COLLAPSE: 'bg-orange-700 text-orange-100',
  BLACKOUT: 'bg-red-900 text-red-100',
  ISLANDING: 'bg-yellow-700 text-yellow-100',
  SEPARATION: 'bg-amber-700 text-amber-100',
}

const SEVERITY_COLORS: Record<string, string> = {
  EXTREME: 'bg-red-600 text-red-100',
  MAJOR: 'bg-orange-600 text-orange-100',
  MODERATE: 'bg-yellow-600 text-yellow-100',
  MINOR: 'bg-green-600 text-green-100',
}

const AEMO_ACTION_COLORS: Record<string, string> = {
  MARKET_SUSPENSION: 'bg-red-700 text-red-100',
  DIRECTION: 'bg-orange-700 text-orange-100',
  RESERVE_ACTIVATION: 'bg-yellow-700 text-yellow-100',
  EMERGENCY_FREQUENCY: 'bg-purple-700 text-purple-100',
  CONSTRAINT_RELAXATION: 'bg-blue-700 text-blue-100',
}

const TRIGGER_COLORS: Record<string, string> = {
  INSUFFICIENT_GENERATION: 'bg-red-700 text-red-100',
  DIRECTION: 'bg-orange-700 text-orange-100',
  NETWORK_CONSTRAINT: 'bg-yellow-700 text-yellow-100',
  VOLUNTARY: 'bg-green-700 text-green-100',
}

const REGION_COLORS: Record<string, string> = {
  NSW1: 'bg-blue-700 text-blue-100',
  QLD1: 'bg-yellow-700 text-yellow-100',
  VIC1: 'bg-indigo-700 text-indigo-100',
  SA1: 'bg-pink-700 text-pink-100',
  NEM: 'bg-gray-600 text-gray-100',
}

// ── Helper components ──────────────────────────────────────────────────────────

function Badge({ label, colorClass }: { label: string; colorClass: string }) {
  return (
    <span className={`inline-block px-2 py-0.5 rounded text-xs font-semibold ${colorClass}`}>
      {label}
    </span>
  )
}

function KpiCard({
  icon,
  label,
  value,
  sub,
  iconColor,
}: {
  icon: React.ReactNode
  label: string
  value: string
  sub?: string
  iconColor: string
}) {
  return (
    <div className="bg-gray-800 rounded-lg p-4 flex items-start gap-4">
      <div className={`p-2 rounded-lg ${iconColor}`}>{icon}</div>
      <div>
        <p className="text-gray-400 text-sm">{label}</p>
        <p className="text-white text-2xl font-bold">{value}</p>
        {sub && <p className="text-gray-400 text-xs mt-0.5">{sub}</p>}
      </div>
    </div>
  )
}

function truncate(s: string, n: number): string {
  return s.length > n ? s.slice(0, n) + '…' : s
}

// ── Frequency chart dot colour ─────────────────────────────────────────────────

function getFreqDotColor(rec: PSEFrequencyRecord): string {
  if (rec.time_outside_emergency_band_s > 0) return '#ef4444'
  if (rec.time_outside_normal_band_s > 0) return '#f97316'
  return '#22c55e'
}

// ── Sub-components ─────────────────────────────────────────────────────────────

function EventsTable({ events }: { events: PSEEventRecord[] }) {
  const sorted = [...events].sort((a, b) => b.date.localeCompare(a.date))
  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <h2 className="text-white font-semibold text-lg mb-3">Security Events Timeline</h2>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-gray-400 border-b border-gray-700">
              <th className="text-left py-2 pr-3">Event ID</th>
              <th className="text-left py-2 pr-3">Date / Time</th>
              <th className="text-left py-2 pr-3">Region</th>
              <th className="text-left py-2 pr-3">Type</th>
              <th className="text-left py-2 pr-3">Severity</th>
              <th className="text-right py-2 pr-3">Duration (min)</th>
              <th className="text-right py-2 pr-3">MW Lost</th>
              <th className="text-right py-2 pr-3">Load Shed (MW)</th>
              <th className="text-right py-2 pr-3">Customers</th>
              <th className="text-right py-2 pr-3">Cost ($M)</th>
              <th className="text-left py-2">Root Cause</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((e) => (
              <tr key={e.event_id} className="border-b border-gray-700 hover:bg-gray-750">
                <td className="py-2 pr-3 text-gray-300 font-mono">{e.event_id}</td>
                <td className="py-2 pr-3 text-gray-300 whitespace-nowrap">
                  {e.date} {e.time}
                </td>
                <td className="py-2 pr-3">
                  <Badge
                    label={e.region}
                    colorClass={REGION_COLORS[e.region] ?? 'bg-gray-600 text-gray-100'}
                  />
                </td>
                <td className="py-2 pr-3">
                  <Badge
                    label={e.event_type.replace(/_/g, ' ')}
                    colorClass={EVENT_TYPE_COLORS[e.event_type] ?? 'bg-gray-600 text-gray-100'}
                  />
                </td>
                <td className="py-2 pr-3">
                  <Badge
                    label={e.severity}
                    colorClass={SEVERITY_COLORS[e.severity] ?? 'bg-gray-600 text-gray-100'}
                  />
                </td>
                <td className="py-2 pr-3 text-right text-gray-300">{e.duration_min}</td>
                <td className="py-2 pr-3 text-right text-gray-300">{e.mw_lost.toLocaleString()}</td>
                <td className="py-2 pr-3 text-right text-gray-300">
                  {e.load_shed_mw > 0 ? e.load_shed_mw.toLocaleString() : '—'}
                </td>
                <td className="py-2 pr-3 text-right text-gray-300">
                  {e.customers_affected > 0 ? e.customers_affected.toLocaleString() : '—'}
                </td>
                <td className="py-2 pr-3 text-right text-gray-300">
                  {e.cost_estimate_m.toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 })}
                </td>
                <td className="py-2 text-gray-400 max-w-xs">{truncate(e.root_cause, 50)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function FrequencyChart({ records }: { records: PSEFrequencyRecord[] }) {
  const regions = ['NSW1', 'QLD1', 'VIC1', 'SA1']
  const [activeRegion, setActiveRegion] = useState<string>('NSW1')

  const filtered = records.filter((r) => r.region === activeRegion)

  // Build scatter data: one point per record with min and max
  const minData = filtered.map((r) => ({
    date: r.date,
    freq: r.min_frequency_hz,
    color: getFreqDotColor(r),
  }))
  const maxData = filtered.map((r) => ({
    date: r.date,
    freq: r.max_frequency_hz,
    color: '#60a5fa',
  }))

  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <h2 className="text-white font-semibold text-lg mb-3">Frequency Performance by Region</h2>
      <div className="flex gap-2 mb-4 flex-wrap">
        {regions.map((r) => (
          <button
            key={r}
            onClick={() => setActiveRegion(r)}
            className={`px-3 py-1 rounded text-sm font-medium transition-colors ${
              activeRegion === r
                ? 'bg-blue-600 text-white'
                : 'bg-gray-700 text-gray-300 hover:bg-gray-600'
            }`}
          >
            {r}
          </button>
        ))}
      </div>
      <div className="flex gap-4 mb-3 text-xs">
        <span className="flex items-center gap-1">
          <span className="w-3 h-3 rounded-full bg-green-500 inline-block" /> Normal band
        </span>
        <span className="flex items-center gap-1">
          <span className="w-3 h-3 rounded-full bg-orange-500 inline-block" /> Outside normal band
        </span>
        <span className="flex items-center gap-1">
          <span className="w-3 h-3 rounded-full bg-red-500 inline-block" /> Outside emergency band
        </span>
        <span className="flex items-center gap-1">
          <span className="w-3 h-3 rounded-full bg-blue-400 inline-block" /> Max frequency
        </span>
      </div>
      <ResponsiveContainer width="100%" height={300}>
        <ComposedChart margin={{ top: 10, right: 20, left: 0, bottom: 10 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis
            dataKey="date"
            type="category"
            allowDuplicatedCategory={false}
            {...({ data: minData } as any)}
            stroke="#9ca3af"
            tick={{ fill: '#9ca3af', fontSize: 11 }}
          />
          <YAxis
            domain={[49.0, 51.0]}
            stroke="#9ca3af"
            tick={{ fill: '#9ca3af', fontSize: 11 }}
            tickFormatter={(v) => `${v.toFixed(1)} Hz`}
          />
          <Tooltip
            contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', color: '#f9fafb' }}
            formatter={(value: number) => [`${value.toFixed(3)} Hz`]}
          />
          <Legend wrapperStyle={{ color: '#9ca3af' }} />
          <ReferenceLine y={49.5} stroke="#ef4444" strokeDasharray="4 4" label={{ value: '49.5 Emergency Low', fill: '#ef4444', fontSize: 10 }} />
          <ReferenceLine y={50.5} stroke="#ef4444" strokeDasharray="4 4" label={{ value: '50.5 Emergency High', fill: '#ef4444', fontSize: 10 }} />
          <ReferenceLine y={49.85} stroke="#f97316" strokeDasharray="2 4" label={{ value: '49.85 Normal Low', fill: '#f97316', fontSize: 10 }} />
          <ReferenceLine y={50.15} stroke="#f97316" strokeDasharray="2 4" label={{ value: '50.15 Normal High', fill: '#f97316', fontSize: 10 }} />
          <Scatter
            name="Min Frequency"
            data={minData}
            dataKey="freq"
            fill="#22c55e"
            shape={(props: any) => {
              const { cx, cy, payload } = props
              return <circle cx={cx} cy={cy} r={5} fill={payload.color} opacity={0.85} />
            }}
          />
          <Scatter
            name="Max Frequency"
            data={maxData}
            dataKey="freq"
            fill="#60a5fa"
          />
          <Line
            data={minData}
            dataKey="freq"
            stroke="#22c55e"
            strokeWidth={1}
            dot={false}
            name="Min Freq trend"
            legendType="none"
          />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  )
}

function LoadSheddingPanel({ records }: { records: PSELoadSheddingRecord[] }) {
  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <h2 className="text-white font-semibold text-lg mb-3">Load Shedding Events</h2>
      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
        {records.map((ls) => (
          <div key={ls.event_id} className="bg-gray-700 rounded-lg p-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-gray-300 font-mono text-sm font-semibold">{ls.event_id}</span>
              <div className="flex gap-2">
                <Badge
                  label={ls.trigger.replace(/_/g, ' ')}
                  colorClass={TRIGGER_COLORS[ls.trigger] ?? 'bg-gray-600 text-gray-100'}
                />
                <Badge
                  label={ls.planned ? 'PLANNED' : 'UNPLANNED'}
                  colorClass={ls.planned ? 'bg-green-700 text-green-100' : 'bg-red-700 text-red-100'}
                />
              </div>
            </div>
            <p className="text-gray-400 text-xs mb-3">
              {ls.date} — {ls.state}
            </p>
            <div className="grid grid-cols-2 gap-2 text-sm">
              <div>
                <p className="text-gray-400 text-xs">MW Shed</p>
                <p className="text-white font-semibold">{ls.total_shed_mw.toLocaleString()} MW</p>
              </div>
              <div>
                <p className="text-gray-400 text-xs">Customers</p>
                <p className="text-white font-semibold">{ls.customers_affected.toLocaleString()}</p>
              </div>
              <div>
                <p className="text-gray-400 text-xs">Duration</p>
                <p className="text-white font-semibold">{ls.duration_min} min</p>
              </div>
              <div>
                <p className="text-gray-400 text-xs">Advance Notice</p>
                <p className="text-white font-semibold">
                  {ls.advance_notice_min > 0 ? `${ls.advance_notice_min} min` : 'None'}
                </p>
              </div>
            </div>
            <div className="mt-2 pt-2 border-t border-gray-600">
              <p className="text-gray-400 text-xs">Area: {ls.rotating_area}</p>
              <p className="text-orange-300 text-sm font-semibold mt-1">
                Financial Cost: ${ls.financial_cost_m.toFixed(1)}M
              </p>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

function AemoActionsTable({ actions }: { actions: PSEAemoActionRecord[] }) {
  const sorted = [...actions].sort((a, b) => b.date.localeCompare(a.date))
  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <h2 className="text-white font-semibold text-lg mb-3">AEMO Emergency Actions</h2>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-gray-400 border-b border-gray-700">
              <th className="text-left py-2 pr-3">Action ID</th>
              <th className="text-left py-2 pr-3">Date</th>
              <th className="text-left py-2 pr-3">Type</th>
              <th className="text-left py-2 pr-3">Region</th>
              <th className="text-left py-2 pr-3">Trigger</th>
              <th className="text-right py-2 pr-3">Instructed MW</th>
              <th className="text-right py-2 pr-3">Cost ($M)</th>
              <th className="text-center py-2 pr-3">Market Suspended</th>
              <th className="text-left py-2">Outcome</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((a) => (
              <tr key={a.action_id} className="border-b border-gray-700 hover:bg-gray-750">
                <td className="py-2 pr-3 text-gray-300 font-mono">{a.action_id}</td>
                <td className="py-2 pr-3 text-gray-300 whitespace-nowrap">{a.date}</td>
                <td className="py-2 pr-3">
                  <Badge
                    label={a.action_type.replace(/_/g, ' ')}
                    colorClass={AEMO_ACTION_COLORS[a.action_type] ?? 'bg-gray-600 text-gray-100'}
                  />
                </td>
                <td className="py-2 pr-3">
                  <Badge
                    label={a.region}
                    colorClass={REGION_COLORS[a.region] ?? 'bg-gray-600 text-gray-100'}
                  />
                </td>
                <td className="py-2 pr-3 text-gray-400 max-w-xs">
                  {truncate(a.trigger_condition, 50)}
                </td>
                <td className="py-2 pr-3 text-right text-gray-300">
                  {a.instructed_mw > 0 ? a.instructed_mw.toLocaleString() : '—'}
                </td>
                <td className="py-2 pr-3 text-right text-gray-300">
                  {a.cost_m.toFixed(1)}
                </td>
                <td className="py-2 pr-3 text-center">
                  {a.market_suspended ? (
                    <Badge label="YES" colorClass="bg-red-700 text-red-100" />
                  ) : (
                    <Badge label="NO" colorClass="bg-gray-600 text-gray-300" />
                  )}
                </td>
                <td className="py-2 text-gray-400">{truncate(a.outcome, 60)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── Main page ──────────────────────────────────────────────────────────────────

export default function PowerSystemEventsAnalytics() {
  const [data, setData] = useState<PSEDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getPowerSystemEventsDashboard()
      .then(setData)
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-900 flex items-center justify-center">
        <p className="text-gray-400 text-lg animate-pulse">Loading Power System Security Events...</p>
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="min-h-screen bg-gray-900 flex items-center justify-center">
        <p className="text-red-400 text-lg">Error: {error ?? 'No data'}</p>
      </div>
    )
  }

  const summary = data.summary as Record<string, number>
  const totalCustomers = summary.total_customers_affected ?? 0
  const totalCost = summary.total_cost_estimate_m ?? 0

  return (
    <div className="min-h-screen bg-gray-900 text-white">
      {/* Header */}
      <div className="bg-gray-800 border-b border-gray-700 px-6 py-4">
        <div className="flex items-center gap-3">
          <AlertTriangle className="text-red-400" size={28} />
          <div>
            <h1 className="text-2xl font-bold text-white">Power System Security Events Analytics</h1>
            <p className="text-gray-400 text-sm mt-0.5">
              NEM frequency excursions, voltage incidents, load shedding events, and AEMO emergency actions
            </p>
          </div>
        </div>
      </div>

      <div className="px-6 py-6 space-y-6">
        {/* KPI Cards */}
        <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-4">
          <KpiCard
            icon={<AlertTriangle size={20} />}
            label="Total Security Events"
            value={String(summary.total_events ?? 0)}
            sub={`${summary.extreme_events ?? 0} extreme severity`}
            iconColor="bg-red-900 text-red-400"
          />
          <KpiCard
            icon={<Zap size={20} />}
            label="Extreme Events"
            value={String(summary.extreme_events ?? 0)}
            sub="System-wide blackouts"
            iconColor="bg-red-800 text-red-300"
          />
          <KpiCard
            icon={<Users size={20} />}
            label="Customers Affected"
            value={
              totalCustomers >= 1_000_000
                ? `${(totalCustomers / 1_000_000).toFixed(1)}M`
                : totalCustomers.toLocaleString()
            }
            sub="Cumulative across all events"
            iconColor="bg-orange-900 text-orange-400"
          />
          <KpiCard
            icon={<DollarSign size={20} />}
            label="Total Cost Estimate"
            value={
              totalCost >= 1000
                ? `$${(totalCost / 1000).toFixed(1)}B`
                : `$${totalCost.toFixed(0)}M`
            }
            sub="Economic impact across events"
            iconColor="bg-yellow-900 text-yellow-400"
          />
        </div>

        {/* Secondary KPI row */}
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
          <div className="bg-gray-800 rounded-lg p-4 flex items-center gap-4">
            <Activity className="text-purple-400" size={24} />
            <div>
              <p className="text-gray-400 text-sm">Market Suspension Events</p>
              <p className="text-white text-xl font-bold">{summary.market_suspension_events ?? 0}</p>
            </div>
          </div>
          <div className="bg-gray-800 rounded-lg p-4 flex items-center gap-4">
            <Zap className="text-orange-400" size={24} />
            <div>
              <p className="text-gray-400 text-sm">Load Shedding Events</p>
              <p className="text-white text-xl font-bold">{summary.load_shedding_events ?? 0}</p>
            </div>
          </div>
          <div className="bg-gray-800 rounded-lg p-4 flex items-center gap-4">
            <Activity className="text-blue-400" size={24} />
            <div>
              <p className="text-gray-400 text-sm">Frequency Records Analysed</p>
              <p className="text-white text-xl font-bold">{summary.frequency_records ?? 0}</p>
            </div>
          </div>
        </div>

        {/* Events Timeline Table */}
        <EventsTable events={data.events} />

        {/* Frequency Performance Chart */}
        <FrequencyChart records={data.frequency_records} />

        {/* Load Shedding Panel */}
        <LoadSheddingPanel records={data.load_shedding} />

        {/* AEMO Actions Table */}
        <AemoActionsTable actions={data.aemo_actions} />
      </div>
    </div>
  )
}
