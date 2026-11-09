import { useState, useEffect } from 'react'
import {
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  BarChart,
  Bar,
  Legend,
  Cell,
} from 'recharts'
import { TrendingUp, AlertTriangle, DollarSign, Clock, MapPin } from 'lucide-react'
import { api } from '../api/client'
import type {
  PSADashboard,
  PSAEventRecord,
  PSAContributorRecord,
  PSAConsumerImpact,
} from '../api/client'

// ---------------------------------------------------------------------------
// Badge helpers
// ---------------------------------------------------------------------------

function RootCauseBadge({ cause }: { cause: string }) {
  const map: Record<string, string> = {
    GENERATION_SHORTFALL: 'bg-red-900/60 text-red-300 border border-red-700',
    NETWORK_CONSTRAINT: 'bg-orange-900/60 text-orange-300 border border-orange-700',
    DEMAND_SPIKE: 'bg-amber-900/60 text-amber-300 border border-amber-700',
    STRATEGIC_BIDDING: 'bg-purple-900/60 text-purple-300 border border-purple-700',
    WEATHER: 'bg-blue-900/60 text-blue-300 border border-blue-700',
  }
  const label: Record<string, string> = {
    GENERATION_SHORTFALL: 'Gen Shortfall',
    NETWORK_CONSTRAINT: 'Network Constraint',
    DEMAND_SPIKE: 'Demand Spike',
    STRATEGIC_BIDDING: 'Strategic Rebid',
    WEATHER: 'Weather',
  }
  const cls = map[cause] ?? 'bg-gray-700 text-gray-300'
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${cls}`}>
      {label[cause] ?? cause}
    </span>
  )
}

function SeverityBadge({ severity }: { severity: string }) {
  const map: Record<string, string> = {
    MODERATE: 'bg-yellow-900/60 text-yellow-300 border border-yellow-700',
    HIGH: 'bg-orange-900/60 text-orange-300 border border-orange-700',
    EXTREME: 'bg-red-900/60 text-red-300 border border-red-700',
    MARKET_SUSPENSION: 'bg-purple-900/60 text-purple-300 border border-purple-700 animate-pulse',
  }
  const cls = map[severity] ?? 'bg-gray-700 text-gray-300'
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${cls}`}>
      {severity.replace('_', ' ')}
    </span>
  )
}

function RegionBadge({ region }: { region: string }) {
  const map: Record<string, string> = {
    NSW1: 'bg-blue-900/60 text-blue-300',
    VIC1: 'bg-teal-900/60 text-teal-300',
    QLD1: 'bg-amber-900/60 text-amber-300',
    SA1: 'bg-pink-900/60 text-pink-300',
    TAS1: 'bg-indigo-900/60 text-indigo-300',
    'NEM-WIDE': 'bg-purple-900/60 text-purple-300',
  }
  const cls = map[region] ?? 'bg-gray-700 text-gray-300'
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${cls}`}>
      {region}
    </span>
  )
}

function ContributionTypeBadge({ type }: { type: string }) {
  const map: Record<string, string> = {
    WITHDREW_CAPACITY: 'bg-red-900/60 text-red-300',
    REBID_HIGH: 'bg-orange-900/60 text-orange-300',
    FCAS_RESPONSE: 'bg-green-900/60 text-green-300',
    DEMAND_REDUCTION: 'bg-teal-900/60 text-teal-300',
    CONSTRAINT_BINDING: 'bg-yellow-900/60 text-yellow-300',
  }
  const label: Record<string, string> = {
    WITHDREW_CAPACITY: 'Withdrew Capacity',
    REBID_HIGH: 'High Rebid',
    FCAS_RESPONSE: 'FCAS Response',
    DEMAND_REDUCTION: 'Demand Reduction',
    CONSTRAINT_BINDING: 'Constraint Binding',
  }
  const cls = map[type] ?? 'bg-gray-700 text-gray-300'
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${cls}`}>
      {label[type] ?? type}
    </span>
  )
}

function RegulatoryActionBadge({ action }: { action: string | null }) {
  if (!action) return <span className="text-gray-500 text-xs">—</span>
  const map: Record<string, string> = {
    INVESTIGATED: 'bg-yellow-900/60 text-yellow-300',
    CAUTIONED: 'bg-orange-900/60 text-orange-300',
    FINED: 'bg-red-900/60 text-red-300',
    CLEARED: 'bg-green-900/60 text-green-300',
  }
  const cls = map[action] ?? 'bg-gray-700 text-gray-300'
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${cls}`}>
      {action}
    </span>
  )
}

// ---------------------------------------------------------------------------
// KPI Card
// ---------------------------------------------------------------------------

function KpiCard({
  icon: Icon,
  label,
  value,
  sub,
  color,
}: {
  icon: React.ElementType
  label: string
  value: string
  sub?: string
  color: string
}) {
  return (
    <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
      <div className="flex items-center gap-2 mb-2">
        <Icon className={`w-5 h-5 ${color}`} />
        <span className="text-gray-400 text-sm">{label}</span>
      </div>
      <div className="text-2xl font-bold text-white">{value}</div>
      {sub && <div className="text-xs text-gray-500 mt-1">{sub}</div>}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Severity scatter color
// ---------------------------------------------------------------------------

function severityColor(severity: string): string {
  const m: Record<string, string> = {
    MODERATE: '#fbbf24',
    HIGH: '#f97316',
    EXTREME: '#ef4444',
    MARKET_SUSPENSION: '#a855f7',
  }
  return m[severity] ?? '#6b7280'
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

export default function PriceSpikeAnalysis() {
  const [data, setData] = useState<PSADashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [selectedSpikeId, setSelectedSpikeId] = useState<string | null>(null)

  useEffect(() => {
    api
      .getSpikeAnalysisDashboard()
      .then((d) => {
        setData(d)
        if (d.spike_events.length > 0) setSelectedSpikeId(d.spike_events[0].spike_id)
      })
      .catch((e) => setError(e.message ?? 'Failed to load data'))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-gray-400">
        Loading spike analysis data...
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="flex items-center justify-center h-64 text-red-400">
        {error ?? 'Unknown error'}
      </div>
    )
  }

  const { spike_events, contributors, consumer_impacts } = data

  // Filter contributors and consumer impacts for the selected spike
  const selectedContributors: PSAContributorRecord[] = selectedSpikeId
    ? contributors.filter((c) => c.spike_id === selectedSpikeId)
    : []
  const selectedImpacts: PSAConsumerImpact[] = selectedSpikeId
    ? consumer_impacts.filter((i) => i.spike_id === selectedSpikeId)
    : []

  // Scatter chart data — encode date as epoch days for X axis
  const scatterData = spike_events.map((e) => ({
    x: new Date(e.event_date).getTime(),
    y: e.peak_price_aud_mwh,
    z: Math.min(Math.sqrt(e.duration_minutes) * 4, 60), // bubble size
    severity: e.severity,
    event_name: e.event_name,
    region: e.region,
    spike_id: e.spike_id,
  }))

  // Consumer impact bar data
  const impactBarData = selectedImpacts.map((i) => ({
    segment: i.consumer_segment,
    hedged: parseFloat((i.unhedged_cost_m_aud * (i.hedged_exposure_pct / 100)).toFixed(2)),
    unhedged: parseFloat(i.unhedged_cost_m_aud.toFixed(2)),
    demand_response: parseFloat(i.demand_response_mw.toFixed(0)),
  }))

  return (
    <div className="p-6 space-y-6 bg-gray-900 min-h-screen text-white">
      {/* Header */}
      <div className="flex items-center gap-3">
        <TrendingUp className="w-7 h-7 text-orange-400" />
        <div>
          <h1 className="text-2xl font-bold text-white">NEM Price Spike Post-Event Analysis</h1>
          <p className="text-gray-400 text-sm mt-0.5">
            Deep-dive root cause analysis, market participant behaviour, consumer impact, and
            regulatory response for high-price events (2022–2024)
          </p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <KpiCard
          icon={AlertTriangle}
          label="Spike Events"
          value={String(data.total_spike_events_2024)}
          sub="runs of >$300/MWh intervals in the data window"
          color="text-red-400"
        />
        <KpiCard
          icon={DollarSign}
          label="Total Consumer Cost"
          value={`$${data.total_consumer_cost_m_aud.toFixed(0)}M`}
          sub="Across all recorded events"
          color="text-amber-400"
        />
        <KpiCard
          icon={Clock}
          label="Avg Spike Duration"
          value={`${data.avg_spike_duration_min.toFixed(0)} min`}
          sub="Excluding market suspension"
          color="text-blue-400"
        />
        <KpiCard
          icon={MapPin}
          label="Most Affected Region"
          value={data.most_affected_region}
          sub="By consumer cost exposure"
          color="text-teal-400"
        />
      </div>

      {/* Spike Severity Timeline Scatter */}
      <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
        <h2 className="text-lg font-semibold mb-1">Spike Severity Timeline</h2>
        <p className="text-xs text-gray-400 mb-4">
          Peak price ($/MWh) by event date — bubble size proportional to duration; colour by
          severity
        </p>
        <div className="flex flex-wrap gap-4 mb-3 text-xs">
          {['MODERATE', 'HIGH', 'EXTREME', 'MARKET_SUSPENSION'].map((s) => (
            <span key={s} className="flex items-center gap-1">
              <span
                className="inline-block w-3 h-3 rounded-full"
                style={{ background: severityColor(s) }}
              />
              <span className="text-gray-300">{s.replace('_', ' ')}</span>
            </span>
          ))}
        </div>
        <ResponsiveContainer width="100%" height={280}>
          <ScatterChart margin={{ top: 10, right: 20, bottom: 30, left: 20 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis
              dataKey="x"
              type="number"
              domain={['auto', 'auto']}
              tickFormatter={(v) => new Date(v).getFullYear().toString()}
              stroke="#9ca3af"
              tick={{ fontSize: 11 }}
              label={{ value: 'Event Date', position: 'insideBottom', offset: -15, fill: '#9ca3af', fontSize: 11 }}
            />
            <YAxis
              dataKey="y"
              stroke="#9ca3af"
              tick={{ fontSize: 11 }}
              tickFormatter={(v) => `$${(v / 1000).toFixed(0)}k`}
              label={{ value: 'Peak Price ($/MWh)', angle: -90, position: 'insideLeft', fill: '#9ca3af', fontSize: 11 }}
            />
            <Tooltip
              cursor={{ strokeDasharray: '3 3' }}
              contentStyle={{ background: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
              content={({ payload }) => {
                if (!payload?.length) return null
                const d = payload[0].payload
                return (
                  <div className="bg-gray-800 border border-gray-600 rounded p-3 text-xs space-y-1">
                    <div className="font-semibold text-white">{d.event_name}</div>
                    <div className="text-gray-300">Region: {d.region}</div>
                    <div className="text-orange-300">
                      Peak: ${d.y.toLocaleString()} /MWh
                    </div>
                    <div className="text-gray-400">Date: {new Date(d.x).toLocaleDateString()}</div>
                  </div>
                )
              }}
            />
            <Scatter
              data={scatterData}
              shape={(props: any) => {
                const { cx, cy, payload } = props
                const r = payload.z / 2
                return (
                  <circle
                    cx={cx}
                    cy={cy}
                    r={r}
                    fill={severityColor(payload.severity)}
                    fillOpacity={0.75}
                    stroke={severityColor(payload.severity)}
                    strokeWidth={1.5}
                  />
                )
              }}
            />
          </ScatterChart>
        </ResponsiveContainer>
      </div>

      {/* Spike Events Table */}
      <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
        <h2 className="text-lg font-semibold mb-4">
          Spike Events
          <span className="ml-2 text-sm text-gray-400 font-normal">
            — click a row to see contributor and consumer details
          </span>
        </h2>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-700 text-gray-400 text-xs uppercase tracking-wide">
                <th className="text-left py-2 pr-4">Event</th>
                <th className="text-left py-2 pr-4">Region</th>
                <th className="text-left py-2 pr-4">Date</th>
                <th className="text-right py-2 pr-4">Duration (min)</th>
                <th className="text-right py-2 pr-4">Peak ($/MWh)</th>
                <th className="text-right py-2 pr-4">x Baseline</th>
                <th className="text-right py-2 pr-4">Consumer Cost ($M)</th>
                <th className="text-left py-2 pr-4">Root Cause</th>
                <th className="text-left py-2">Severity</th>
              </tr>
            </thead>
            <tbody>
              {spike_events.map((e: PSAEventRecord) => {
                const isSelected = e.spike_id === selectedSpikeId
                return (
                  <tr
                    key={e.spike_id}
                    onClick={() => setSelectedSpikeId(e.spike_id)}
                    className={`border-b border-gray-700/50 cursor-pointer transition-colors ${
                      isSelected
                        ? 'bg-orange-900/20 border-orange-800/40'
                        : 'hover:bg-gray-700/40'
                    }`}
                  >
                    <td className="py-2.5 pr-4 text-white font-medium max-w-xs truncate">
                      {e.event_name}
                    </td>
                    <td className="py-2.5 pr-4">
                      <RegionBadge region={e.region} />
                    </td>
                    <td className="py-2.5 pr-4 text-gray-300">{e.event_date}</td>
                    <td className="py-2.5 pr-4 text-right text-gray-300">
                      {e.duration_minutes.toLocaleString()}
                    </td>
                    <td className="py-2.5 pr-4 text-right text-orange-300 font-semibold">
                      ${e.peak_price_aud_mwh.toLocaleString()}
                    </td>
                    <td className="py-2.5 pr-4 text-right text-amber-300">
                      {e.price_multiple != null ? `${e.price_multiple.toFixed(1)}x` : '—'}
                    </td>
                    <td className="py-2.5 pr-4 text-right text-red-300">
                      ${e.consumer_cost_m_aud.toFixed(1)}M
                    </td>
                    <td className="py-2.5 pr-4">
                      <RootCauseBadge cause={e.root_cause} />
                    </td>
                    <td className="py-2.5">
                      <SeverityBadge severity={e.severity} />
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Detail panels — shown when a spike is selected */}
      {selectedSpikeId && (
        <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
          {/* Top Contributors */}
          <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
            <h2 className="text-lg font-semibold mb-4">
              Top Contributors
              <span className="ml-2 text-sm text-gray-400 font-normal">
                — {selectedSpikeId}
              </span>
            </h2>
            {selectedContributors.length === 0 ? (
              <p className="text-gray-500 text-sm">No contributor data for this event.</p>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-gray-700 text-gray-400 text-xs uppercase tracking-wide">
                      <th className="text-left py-2 pr-3">Participant</th>
                      <th className="text-left py-2 pr-3">Technology</th>
                      <th className="text-left py-2 pr-3">Contribution</th>
                      <th className="text-right py-2 pr-3">MW Impact</th>
                      <th className="text-right py-2 pr-3">Price Contrib ($/MWh)</th>
                      <th className="text-right py-2 pr-3">Revenue ($M)</th>
                      <th className="text-left py-2">Regulatory</th>
                    </tr>
                  </thead>
                  <tbody>
                    {selectedContributors.map((c: PSAContributorRecord, idx: number) => (
                      <tr key={idx} className="border-b border-gray-700/50">
                        <td className="py-2.5 pr-3 text-white font-medium">{c.participant_name}</td>
                        <td className="py-2.5 pr-3 text-gray-300">{c.technology}</td>
                        <td className="py-2.5 pr-3">
                          <ContributionTypeBadge type={c.contribution_type} />
                        </td>
                        <td className="py-2.5 pr-3 text-right text-gray-300">
                          {c.mw_impact > 0 ? '+' : ''}
                          {c.mw_impact.toFixed(0)} MW
                        </td>
                        <td className="py-2.5 pr-3 text-right text-orange-300">
                          ${c.price_contribution_aud_mwh.toLocaleString()}
                        </td>
                        <td className="py-2.5 pr-3 text-right text-amber-300">
                          ${c.revenue_gained_m_aud.toFixed(1)}M
                        </td>
                        <td className="py-2.5">
                          <RegulatoryActionBadge action={c.regulatory_action} />
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          {/* Consumer Impact */}
          <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
            <h2 className="text-lg font-semibold mb-4">
              Consumer Impact
              <span className="ml-2 text-sm text-gray-400 font-normal">
                — hedged vs unhedged cost by segment
              </span>
            </h2>
            {selectedImpacts.length === 0 ? (
              <p className="text-gray-500 text-sm">No consumer impact data for this event.</p>
            ) : (
              <>
                <ResponsiveContainer width="100%" height={200}>
                  <BarChart data={impactBarData} margin={{ top: 5, right: 10, bottom: 5, left: 10 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                    <XAxis dataKey="segment" stroke="#9ca3af" tick={{ fontSize: 11 }} />
                    <YAxis stroke="#9ca3af" tick={{ fontSize: 11 }} tickFormatter={(v) => `$${v}M`} />
                    <Tooltip
                      contentStyle={{
                        background: '#1f2937',
                        border: '1px solid #374151',
                        borderRadius: 6,
                      }}
                      formatter={(value: number, name: string) => [
                        `$${value.toFixed(1)}M`,
                        name === 'hedged' ? 'Hedged Cost' : 'Unhedged Cost',
                      ]}
                    />
                    <Legend
                      formatter={(value) =>
                        value === 'hedged' ? 'Hedged Cost ($M)' : 'Unhedged Cost ($M)'
                      }
                      wrapperStyle={{ fontSize: 11 }}
                    />
                    <Bar dataKey="hedged" stackId="cost" fill="#3b82f6" name="hedged" />
                    <Bar dataKey="unhedged" stackId="cost" fill="#ef4444" name="unhedged" />
                  </BarChart>
                </ResponsiveContainer>

                {/* Table supplement */}
                <div className="mt-4 overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-gray-700 text-gray-400 text-xs uppercase tracking-wide">
                        <th className="text-left py-2 pr-4">Segment</th>
                        <th className="text-right py-2 pr-4">Hedged (%)</th>
                        <th className="text-right py-2 pr-4">Unhedged Cost ($M)</th>
                        <th className="text-right py-2 pr-4">DR Response (MW)</th>
                        <th className="text-right py-2 pr-4">A/C Curtail (MW)</th>
                        <th className="text-right py-2">Price Response (%)</th>
                      </tr>
                    </thead>
                    <tbody>
                      {selectedImpacts.map((i: PSAConsumerImpact, idx: number) => (
                        <tr key={idx} className="border-b border-gray-700/50">
                          <td className="py-2 pr-4 text-gray-300">{i.consumer_segment}</td>
                          <td className="py-2 pr-4 text-right text-green-400">
                            {i.hedged_exposure_pct.toFixed(0)}%
                          </td>
                          <td className="py-2 pr-4 text-right text-red-300">
                            ${i.unhedged_cost_m_aud.toFixed(1)}M
                          </td>
                          <td className="py-2 pr-4 text-right text-teal-300">
                            {i.demand_response_mw.toFixed(0)} MW
                          </td>
                          <td className="py-2 pr-4 text-right text-amber-300">
                            {i.air_con_curtailment_mw.toFixed(0)} MW
                          </td>
                          <td className="py-2 text-right text-blue-300">
                            {i.price_signal_response_pct.toFixed(1)}%
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            )}
          </div>
        </div>
      )}

      {/* Legend / Notes */}
      <div className="bg-gray-800/50 rounded-lg p-4 border border-gray-700/50 text-xs text-gray-400 space-y-1">
        <div className="font-semibold text-gray-300 mb-2">Notes</div>
        <div>
          Root causes: Generation Shortfall (unplanned outages/withholding), Network Constraint
          (binding transmission limits), Demand Spike (extreme weather load), Strategic Bidding
          (high-price rebids by generators), Weather (forecast error / extreme event).
        </div>
        <div>
          Consumer costs represent total settlement exposure. Hedged cost indicates exposure covered
          by financial contracts (cap contracts, swaps). Unhedged cost is direct market exposure.
        </div>
        <div>
          Regulatory actions sourced from AER Market Surveillance and Compliance reports. FINED
          indicates final determination; INVESTIGATED / CAUTIONED are interim or advisory outcomes.
        </div>
      </div>
    </div>
  )
}
