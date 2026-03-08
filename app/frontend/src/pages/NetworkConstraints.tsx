import { useEffect, useState, useCallback } from 'react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from 'recharts'
import { GitBranch, AlertTriangle, RefreshCw, Zap, DollarSign, MapPin, Activity } from 'lucide-react'
import { api, constraintsApi } from '../api/client'
import type { ConstraintDashboard, ConstraintEquation, ConstraintViolationRecord, BindingHeatmapCell, PriceSeparation, ConstraintForecast, ConstraintTimelineEntry } from '../api/client'

// ---------------------------------------------------------------------------
// Types & helpers
// ---------------------------------------------------------------------------

const REGIONS = ['All', 'SA1', 'NSW1', 'VIC1', 'QLD1', 'TAS1']

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

function constraintTypeBadge(type: string) {
  const map: Record<string, string> = {
    THERMAL: 'bg-orange-100 text-orange-700 dark:bg-orange-900/40 dark:text-orange-300',
    VOLTAGE: 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/40 dark:text-yellow-300',
    STABILITY: 'bg-indigo-100 text-indigo-700 dark:bg-indigo-900/40 dark:text-indigo-300',
    NETWORK: 'bg-cyan-100 text-cyan-700 dark:bg-cyan-900/40 dark:text-cyan-300',
  }
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-medium ${map[type] ?? 'bg-gray-100 text-gray-600'}`}>
      {type}
    </span>
  )
}

function causeBadge(cause: string) {
  const map: Record<string, string> = {
    WIND_RAMP: 'bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300',
    HIGH_DEMAND: 'bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-300',
    OUTAGE: 'bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300',
    NEMDE_INTERVENTION: 'bg-purple-100 text-purple-700 dark:bg-purple-900/40 dark:text-purple-300',
  }
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-medium ${map[cause] ?? 'bg-gray-100 text-gray-600'}`}>
      {cause.replace(/_/g, ' ')}
    </span>
  )
}

function slackColor(slack: number): string {
  if (slack < 0) return 'text-red-600 dark:text-red-400 font-semibold'
  if (slack < 10) return 'text-amber-600 dark:text-amber-400 font-semibold'
  return 'text-green-600 dark:text-green-400'
}

function barFill(eq: ConstraintEquation): string {
  if (eq.binding) return '#ef4444'
  if (eq.slack_mw < 20) return '#f59e0b'
  return '#3b82f6'
}

// ---------------------------------------------------------------------------
// KPI card
// ---------------------------------------------------------------------------

interface KpiCardProps {
  label: string
  value: string | number
  icon: React.ReactNode
  alert?: boolean
  sub?: string
}

function KpiCard({ label, value, icon, alert, sub }: KpiCardProps) {
  return (
    <div className={`bg-white dark:bg-gray-800 rounded-xl p-5 shadow-sm border ${alert ? 'border-red-300 dark:border-red-700' : 'border-gray-200 dark:border-gray-700'}`}>
      <div className="flex items-center justify-between mb-3">
        <span className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide">{label}</span>
        <span className={`p-1.5 rounded-lg ${alert ? 'bg-red-100 dark:bg-red-900/30 text-red-600 dark:text-red-400' : 'bg-blue-100 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400'}`}>
          {icon}
        </span>
      </div>
      <div className={`text-2xl font-bold ${alert ? 'text-red-600 dark:text-red-400' : 'text-gray-900 dark:text-gray-100'}`}>
        {value}
      </div>
      {sub && <div className="text-xs text-gray-500 dark:text-gray-400 mt-1">{sub}</div>}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export default function NetworkConstraints() {
  const [dashboard, setDashboard] = useState<ConstraintDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)

  // Filters for equations table
  const [regionFilter, setRegionFilter] = useState<string>('All')
  const [bindingOnly, setBindingOnly] = useState(false)

  // Binding heatmap + price separation (competitive gap feature 3)
  const [heatmapData, setHeatmapData] = useState<BindingHeatmapCell[]>([])
  const [separationData, setSeparationData] = useState<PriceSeparation[]>([])
  const [heatmapRegion, setHeatmapRegion] = useState('NSW1')

  // E15 — Predictive constraint analytics
  const [forecastData, setForecastData] = useState<ConstraintForecast[]>([])
  const [timelineData, setTimelineData] = useState<ConstraintTimelineEntry[]>([])
  const [forecastRegion, setForecastRegion] = useState('NSW1')
  const [forecastModelInfo, setForecastModelInfo] = useState<{ version: string; accuracy_pct: number } | null>(null)

  const fetchData = useCallback(async () => {
    try {
      const data = await api.getConstraintDashboard()
      setDashboard(data)
      setLastUpdated(new Date())
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load constraint data')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    fetchData()
    const id = setInterval(fetchData, 30_000)
    return () => clearInterval(id)
  }, [fetchData])

  // Load heatmap + separation
  useEffect(() => {
    constraintsApi.bindingHeatmap(heatmapRegion, 7).then(d => setHeatmapData(d.grid || [])).catch(() => {})
    constraintsApi.priceSeparation(24).then(d => setSeparationData(d.spreads || [])).catch(() => {})
  }, [heatmapRegion])

  // E15 — Load constraint forecasts
  useEffect(() => {
    constraintsApi.forecast(forecastRegion, 24, 0.2).then(d => {
      setForecastData(d.forecasts || [])
      if (d.model_info) setForecastModelInfo({ version: d.model_info.version, accuracy_pct: d.model_info.accuracy_pct })
    }).catch(() => {})
    constraintsApi.forecastTimeline(forecastRegion, 48).then(d => {
      setTimelineData(d.timeline || [])
    }).catch(() => {})
  }, [forecastRegion])

  // ---------------------------------------------------------------------------
  // Derived data
  // ---------------------------------------------------------------------------

  const allEquations = dashboard?.constraint_equations ?? []
  const filteredEquations: ConstraintEquation[] = allEquations.filter(e => {
    if (regionFilter !== 'All' && e.region !== regionFilter) return false
    if (bindingOnly && !e.binding) return false
    return true
  })

  const chartData = [...allEquations]
    .sort((a, b) => b.annual_cost_est_m_aud - a.annual_cost_est_m_aud)
    .slice(0, 8)
    .map(e => ({ name: e.constraint_id, cost: e.annual_cost_est_m_aud, binding: e.binding, slack: e.slack_mw }))

  const violations: ConstraintViolationRecord[] = dashboard?.violations ?? []

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <RefreshCw className="animate-spin text-blue-500" size={28} />
        <span className="ml-3 text-gray-500 dark:text-gray-400">Loading constraint data...</span>
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex items-center gap-3 p-6 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-xl m-6">
        <AlertTriangle className="text-red-500" size={20} />
        <span className="text-red-700 dark:text-red-300 text-sm">{error}</span>
        <button onClick={fetchData} className="ml-auto px-3 py-1.5 bg-red-100 hover:bg-red-200 dark:bg-red-800 dark:hover:bg-red-700 text-red-700 dark:text-red-300 rounded-lg text-xs font-medium">
          Retry
        </button>
      </div>
    )
  }

  if (!dashboard) return null

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
            <GitBranch className="text-blue-600 dark:text-blue-400" size={22} />
          </div>
          <div>
            <h1 className="text-xl font-bold text-gray-900 dark:text-gray-100">
              Network Constraint Equations &amp; Binding Constraints
            </h1>
            <p className="text-sm text-gray-500 dark:text-gray-400">
              Active NEMDE constraints, shadow prices, and dispatch impact
            </p>
          </div>
        </div>
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-1.5 px-3 py-1.5 bg-green-100 dark:bg-green-900/30 rounded-full">
            <span className="w-2 h-2 rounded-full bg-green-500 animate-pulse inline-block" />
            <span className="text-xs font-medium text-green-700 dark:text-green-300">Live • 30s</span>
          </div>
          {lastUpdated && (
            <span className="text-xs text-gray-400 dark:text-gray-500">
              Updated {lastUpdated.toLocaleTimeString('en-AU')}
            </span>
          )}
          <button
            onClick={fetchData}
            className="p-2 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-700 text-gray-500 dark:text-gray-400 transition-colors"
            title="Refresh"
          >
            <RefreshCw size={16} />
          </button>
        </div>
      </div>

      {/* KPI cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <KpiCard
          label="Binding Constraints Now"
          value={dashboard.binding_constraints_now}
          icon={<Zap size={16} />}
          alert={dashboard.binding_constraints_now > 3}
          sub={`of ${dashboard.total_active_constraints} active`}
        />
        <KpiCard
          label="Annual Constraint Cost"
          value={`$${dashboard.total_annual_constraint_cost_m_aud.toFixed(1)}M`}
          icon={<DollarSign size={16} />}
          sub="estimated AUD/year"
        />
        <KpiCard
          label="Most Constrained Region"
          value={dashboard.most_constrained_region}
          icon={<MapPin size={16} />}
          sub="by binding count"
        />
        <KpiCard
          label="Violations Today"
          value={dashboard.violations_today}
          icon={<AlertTriangle size={16} />}
          alert={dashboard.violations_today > 0}
          sub="dispatch interval breaches"
        />
      </div>

      {/* E15 — Constraint Forecast Panel */}
      <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-200 dark:border-gray-700 overflow-hidden">
        <div className="px-5 py-4 border-b border-gray-200 dark:border-gray-700 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Activity className="text-blue-600 dark:text-blue-400" size={18} />
            <div>
              <h3 className="text-sm font-semibold text-gray-800 dark:text-gray-200">Constraint Binding Forecast</h3>
              <p className="text-xs text-gray-500 dark:text-gray-400">
                ML-predicted binding events for the next 24-48 hours
                {forecastModelInfo && <span className="ml-2 text-blue-500">({forecastModelInfo.version}, {forecastModelInfo.accuracy_pct}% accuracy)</span>}
              </p>
            </div>
          </div>
          <select
            value={forecastRegion}
            onChange={e => setForecastRegion(e.target.value)}
            className="px-2 py-1 text-xs rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800"
          >
            {REGIONS.filter(r => r !== 'All').map(r => <option key={r} value={r}>{r}</option>)}
          </select>
        </div>

        {/* Forecast summary banner */}
        {forecastData.length > 0 && (
          <div className="px-5 py-3 bg-amber-50 dark:bg-amber-900/20 border-b border-amber-200 dark:border-amber-800 flex items-center gap-3">
            <AlertTriangle className="text-amber-600 dark:text-amber-400" size={16} />
            <span className="text-sm font-medium text-amber-800 dark:text-amber-200">
              Next 24h: {forecastData.length} constraint{forecastData.length !== 1 ? 's' : ''} predicted to bind
            </span>
            <span className={`ml-2 px-2 py-0.5 rounded text-xs font-semibold ${
              forecastData.some(f => f.confidence === 'HIGH') ? 'bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-300' :
              forecastData.some(f => f.confidence === 'MEDIUM') ? 'bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300' :
              'bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-300'
            }`}>
              {forecastData.some(f => f.confidence === 'HIGH') ? 'HIGH RISK' : forecastData.some(f => f.confidence === 'MEDIUM') ? 'MEDIUM RISK' : 'LOW RISK'}
            </span>
          </div>
        )}

        {/* 48h Timeline bar chart */}
        {timelineData.length > 0 && (
          <div className="p-4">
            <h4 className="text-xs font-semibold text-gray-600 dark:text-gray-400 mb-2 uppercase">48-Hour Binding Forecast Timeline</h4>
            <ResponsiveContainer width="100%" height={160}>
              <BarChart data={timelineData.slice(0, 48)} margin={{ left: 0, right: 10, top: 5, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" opacity={0.2} />
                <XAxis
                  dataKey="hour"
                  tickFormatter={(v: string) => v.slice(11, 16)}
                  tick={{ fontSize: 9 }}
                  interval={5}
                />
                <YAxis tick={{ fontSize: 10 }} allowDecimals={false} />
                <Tooltip
                  formatter={(v: number, name: string) => [v, name === 'predicted_binding_count' ? 'Binding Count' : name === 'total_expected_cost' ? `$${v.toFixed(2)}` : v]}
                  labelFormatter={(v: string) => `Hour: ${v}`}
                />
                <Bar dataKey="predicted_binding_count" name="Predicted Bindings">
                  {timelineData.slice(0, 48).map((entry, idx) => (
                    <Cell
                      key={idx}
                      fill={entry.severity === 'high' ? '#ef4444' : entry.severity === 'medium' ? '#f59e0b' : '#22c55e'}
                    />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}

        {/* Top predicted constraints table */}
        {forecastData.length > 0 && (
          <div className="px-4 pb-4">
            <h4 className="text-xs font-semibold text-gray-600 dark:text-gray-400 mb-2 uppercase">Top Predicted Constraints</h4>
            <div className="overflow-x-auto">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-gray-200 dark:border-gray-700">
                    <th className="px-3 py-2 text-left text-gray-500 font-medium">Constraint</th>
                    <th className="px-3 py-2 text-left text-gray-500 font-medium">Region</th>
                    <th className="px-3 py-2 text-left text-gray-500 font-medium">Hour</th>
                    <th className="px-3 py-2 text-right text-gray-500 font-medium">Probability</th>
                    <th className="px-3 py-2 text-right text-gray-500 font-medium">Exp. MV</th>
                    <th className="px-3 py-2 text-right text-gray-500 font-medium">Price Impact</th>
                    <th className="px-3 py-2 text-center text-gray-500 font-medium">Confidence</th>
                  </tr>
                </thead>
                <tbody>
                  {forecastData.slice(0, 10).map((f, i) => (
                    <tr key={i} className="border-b border-gray-100 dark:border-gray-700/50 hover:bg-gray-50 dark:hover:bg-gray-700/30">
                      <td className="px-3 py-2 font-mono text-gray-800 dark:text-gray-200">{f.constraint_id.slice(0, 30)}</td>
                      <td className="px-3 py-2">{regionChip(f.region)}</td>
                      <td className="px-3 py-2 text-gray-600 dark:text-gray-400">{f.target_hour.slice(11, 16)}</td>
                      <td className="px-3 py-2 text-right font-semibold">
                        <span className={f.binding_probability > 0.6 ? 'text-red-600 dark:text-red-400' : f.binding_probability > 0.4 ? 'text-amber-600 dark:text-amber-400' : 'text-gray-600 dark:text-gray-400'}>
                          {(f.binding_probability * 100).toFixed(1)}%
                        </span>
                      </td>
                      <td className="px-3 py-2 text-right text-gray-700 dark:text-gray-300">${f.expected_marginal_value.toFixed(1)}</td>
                      <td className="px-3 py-2 text-right text-gray-700 dark:text-gray-300">${f.price_impact_estimate.toFixed(2)}</td>
                      <td className="px-3 py-2 text-center">
                        <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                          f.confidence === 'HIGH' ? 'bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-300' :
                          f.confidence === 'MEDIUM' ? 'bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300' :
                          'bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-300'
                        }`}>
                          {f.confidence}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {forecastData.length === 0 && timelineData.length === 0 && (
          <div className="px-5 py-8 text-center text-gray-400 dark:text-gray-500 text-sm">
            No constraint binding predictions available. Historical constraint data may still be loading.
          </div>
        )}

        {/* Contributing factors mini-cards */}
        {forecastData.length > 0 && forecastData[0].contributing_factors && (
          <div className="px-4 pb-4">
            <h4 className="text-xs font-semibold text-gray-600 dark:text-gray-400 mb-2 uppercase">Contributing Factors (Top Constraint)</h4>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
              {forecastData[0].contributing_factors.demand_level != null && (
                <div className="bg-blue-50 dark:bg-blue-900/20 rounded-lg p-3">
                  <div className="text-xs text-blue-600 dark:text-blue-400 font-medium">Demand</div>
                  <div className="text-sm font-bold text-blue-800 dark:text-blue-200">{forecastData[0].contributing_factors.demand_level.toLocaleString()} MW</div>
                </div>
              )}
              {forecastData[0].contributing_factors.wind_forecast != null && (
                <div className="bg-cyan-50 dark:bg-cyan-900/20 rounded-lg p-3">
                  <div className="text-xs text-cyan-600 dark:text-cyan-400 font-medium">Wind</div>
                  <div className="text-sm font-bold text-cyan-800 dark:text-cyan-200">{forecastData[0].contributing_factors.wind_forecast} km/h</div>
                </div>
              )}
              {forecastData[0].contributing_factors.solar_forecast != null && (
                <div className="bg-amber-50 dark:bg-amber-900/20 rounded-lg p-3">
                  <div className="text-xs text-amber-600 dark:text-amber-400 font-medium">Solar</div>
                  <div className="text-sm font-bold text-amber-800 dark:text-amber-200">{forecastData[0].contributing_factors.solar_forecast} W/m2</div>
                </div>
              )}
              {forecastData[0].contributing_factors.temperature != null && (
                <div className="bg-red-50 dark:bg-red-900/20 rounded-lg p-3">
                  <div className="text-xs text-red-600 dark:text-red-400 font-medium">Temperature</div>
                  <div className="text-sm font-bold text-red-800 dark:text-red-200">{forecastData[0].contributing_factors.temperature}°C</div>
                </div>
              )}
            </div>
          </div>
        )}
      </div>

      {/* Region Summary Cards */}
      <div>
        <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-3 uppercase tracking-wide">
          Region Summary
        </h2>
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3">
          {dashboard.region_summaries.map(rs => (
            <div
              key={rs.region}
              className="bg-white dark:bg-gray-800 rounded-xl p-4 shadow-sm border border-gray-200 dark:border-gray-700"
            >
              <div className="flex items-center justify-between mb-2">
                {regionChip(rs.region)}
                {rs.interconnector_limited && (
                  <span className="text-xs text-amber-600 dark:text-amber-400 font-medium">IC-Ltd</span>
                )}
              </div>
              <div className="space-y-1 text-sm">
                <div className="flex justify-between">
                  <span className="text-gray-500 dark:text-gray-400 text-xs">Active</span>
                  <span className="font-semibold text-gray-900 dark:text-gray-100">{rs.active_constraints}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500 dark:text-gray-400 text-xs">Binding</span>
                  <span className={`font-semibold ${rs.binding_constraints > 0 ? 'text-red-600 dark:text-red-400' : 'text-gray-400 dark:text-gray-500'}`}>
                    {rs.binding_constraints}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500 dark:text-gray-400 text-xs">Cost/yr</span>
                  <span className="font-semibold text-gray-900 dark:text-gray-100">${rs.total_cost_m_aud_yr.toFixed(1)}M</span>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Constraint Cost Bar Chart */}
      <div className="bg-white dark:bg-gray-800 rounded-xl p-5 shadow-sm border border-gray-200 dark:border-gray-700">
        <div className="flex items-center gap-2 mb-4">
          <Activity size={16} className="text-blue-500" />
          <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-300">
            Annual Constraint Cost by Equation (Top 8)
          </h2>
          <div className="ml-auto flex items-center gap-3 text-xs text-gray-500 dark:text-gray-400">
            <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-sm bg-red-500 inline-block" />Binding</span>
            <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-sm bg-amber-500 inline-block" />Tight (&lt;20 MW)</span>
            <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-sm bg-blue-500 inline-block" />Normal</span>
          </div>
        </div>
        <ResponsiveContainer width="100%" height={260}>
          <BarChart data={chartData} margin={{ top: 4, right: 16, bottom: 60, left: 8 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
            <XAxis
              dataKey="name"
              tick={{ fontSize: 10, fill: '#6b7280' }}
              angle={-35}
              textAnchor="end"
              interval={0}
            />
            <YAxis
              tick={{ fontSize: 11, fill: '#6b7280' }}
              tickFormatter={v => `$${v}M`}
              label={{ value: '$M/yr', angle: -90, position: 'insideLeft', style: { fontSize: 10, fill: '#9ca3af' } }}
            />
            <Tooltip
              formatter={(v: number) => [`$${v.toFixed(1)}M`, 'Annual Cost']}
              labelStyle={{ fontSize: 12 }}
              contentStyle={{ background: '#1f2937', border: 'none', borderRadius: 8, color: '#f9fafb' }}
            />
            <Bar dataKey="cost" radius={[4, 4, 0, 0]}>
              {chartData.map((entry, i) => (
                <Cell
                  key={`cell-${i}`}
                  fill={entry.binding ? '#ef4444' : entry.slack < 20 ? '#f59e0b' : '#3b82f6'}
                />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Constraint Equations Table */}
      <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-200 dark:border-gray-700">
        <div className="flex items-center justify-between p-4 border-b border-gray-200 dark:border-gray-700">
          <div className="flex items-center gap-2">
            <GitBranch size={16} className="text-blue-500" />
            <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-300">
              Constraint Equations
            </h2>
            <span className="text-xs text-gray-400 dark:text-gray-500">
              ({filteredEquations.length} shown)
            </span>
          </div>
          <div className="flex items-center gap-3">
            {/* Region filter */}
            <select
              value={regionFilter}
              onChange={e => setRegionFilter(e.target.value)}
              className="text-xs border border-gray-300 dark:border-gray-600 rounded-lg px-2 py-1.5 bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-300 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              {REGIONS.map(r => (
                <option key={r} value={r}>{r === 'All' ? 'All Regions' : r}</option>
              ))}
            </select>
            {/* Binding toggle */}
            <button
              onClick={() => setBindingOnly(v => !v)}
              className={`px-3 py-1.5 rounded-lg text-xs font-medium border transition-colors ${
                bindingOnly
                  ? 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300 border-red-300 dark:border-red-700'
                  : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400 border-gray-300 dark:border-gray-600 hover:bg-gray-200 dark:hover:bg-gray-600'
              }`}
            >
              {bindingOnly ? 'Binding Only' : 'Show All'}
            </button>
          </div>
        </div>
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead>
              <tr className="bg-gray-50 dark:bg-gray-900/50">
                <th className="px-4 py-2.5 text-left text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">Constraint ID</th>
                <th className="px-4 py-2.5 text-left text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">Name</th>
                <th className="px-4 py-2.5 text-left text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">Type</th>
                <th className="px-4 py-2.5 text-left text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">Region</th>
                <th className="px-4 py-2.5 text-center text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">Binding</th>
                <th className="px-4 py-2.5 text-right text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">RHS (MW)</th>
                <th className="px-4 py-2.5 text-right text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">LHS (MW)</th>
                <th className="px-4 py-2.5 text-right text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">Slack (MW)</th>
                <th className="px-4 py-2.5 text-right text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">Shadow $/MWh</th>
                <th className="px-4 py-2.5 text-right text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">Bind Freq %</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100 dark:divide-gray-700">
              {filteredEquations.map(eq => (
                <tr key={eq.constraint_id} className="hover:bg-gray-50 dark:hover:bg-gray-700/40 transition-colors">
                  <td className="px-4 py-3">
                    <code className="text-xs font-mono text-blue-700 dark:text-blue-300 bg-blue-50 dark:bg-blue-900/20 px-1.5 py-0.5 rounded">
                      {eq.constraint_id}
                    </code>
                  </td>
                  <td className="px-4 py-3 text-xs text-gray-700 dark:text-gray-300 max-w-[180px] truncate" title={eq.constraint_name}>
                    {eq.constraint_name}
                  </td>
                  <td className="px-4 py-3">{constraintTypeBadge(eq.constraint_type)}</td>
                  <td className="px-4 py-3">{regionChip(eq.region)}</td>
                  <td className="px-4 py-3 text-center">
                    {eq.binding ? (
                      <span className="px-2 py-0.5 rounded text-xs font-semibold bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-300">YES</span>
                    ) : (
                      <span className="px-2 py-0.5 rounded text-xs font-semibold bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-300">NO</span>
                    )}
                  </td>
                  <td className="px-4 py-3 text-right text-xs text-gray-700 dark:text-gray-300 font-mono">
                    {eq.rhs_value.toFixed(1)}
                  </td>
                  <td className="px-4 py-3 text-right text-xs text-gray-700 dark:text-gray-300 font-mono">
                    {eq.lhs_value.toFixed(1)}
                  </td>
                  <td className={`px-4 py-3 text-right text-xs font-mono ${slackColor(eq.slack_mw)}`}>
                    {eq.slack_mw.toFixed(2)}
                  </td>
                  <td className={`px-4 py-3 text-right text-xs font-mono ${eq.marginal_value > 50 ? 'text-red-600 dark:text-red-400 font-semibold' : 'text-gray-700 dark:text-gray-300'}`}>
                    {eq.marginal_value > 0 ? `$${eq.marginal_value.toFixed(1)}` : '—'}
                  </td>
                  <td className="px-4 py-3 text-right text-xs text-gray-700 dark:text-gray-300">
                    {eq.frequency_binding_pct.toFixed(1)}%
                  </td>
                </tr>
              ))}
              {filteredEquations.length === 0 && (
                <tr>
                  <td colSpan={10} className="px-4 py-8 text-center text-sm text-gray-400 dark:text-gray-500">
                    No constraint equations match the current filters.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Violations Table */}
      <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-200 dark:border-gray-700">
        <div className="flex items-center gap-2 p-4 border-b border-gray-200 dark:border-gray-700">
          <AlertTriangle size={16} className="text-red-500" />
          <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-300">
            Constraint Violations Today
          </h2>
          <span className="text-xs text-gray-400 dark:text-gray-500">({violations.length})</span>
        </div>
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead>
              <tr className="bg-gray-50 dark:bg-gray-900/50">
                <th className="px-4 py-2.5 text-left text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">Violation ID</th>
                <th className="px-4 py-2.5 text-left text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">Constraint</th>
                <th className="px-4 py-2.5 text-left text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">Region</th>
                <th className="px-4 py-2.5 text-left text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">Interval</th>
                <th className="px-4 py-2.5 text-right text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">Violation MW</th>
                <th className="px-4 py-2.5 text-right text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">Price Impact $/MWh</th>
                <th className="px-4 py-2.5 text-left text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">Cause</th>
                <th className="px-4 py-2.5 text-center text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">Resolved</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100 dark:divide-gray-700">
              {violations.map(v => (
                <tr key={v.violation_id} className="hover:bg-gray-50 dark:hover:bg-gray-700/40 transition-colors">
                  <td className="px-4 py-3">
                    <code className="text-xs font-mono text-gray-600 dark:text-gray-400">{v.violation_id}</code>
                  </td>
                  <td className="px-4 py-3">
                    <code className="text-xs font-mono text-blue-700 dark:text-blue-300 bg-blue-50 dark:bg-blue-900/20 px-1.5 py-0.5 rounded">
                      {v.constraint_id}
                    </code>
                  </td>
                  <td className="px-4 py-3">{regionChip(v.region)}</td>
                  <td className="px-4 py-3 text-xs text-gray-600 dark:text-gray-400">
                    {new Date(v.dispatch_interval).toLocaleTimeString('en-AU', { hour: '2-digit', minute: '2-digit' })} AEST
                  </td>
                  <td className="px-4 py-3 text-right text-xs font-semibold text-red-600 dark:text-red-400 font-mono">
                    {v.violation_mw.toFixed(1)} MW
                  </td>
                  <td className="px-4 py-3 text-right text-xs font-mono text-gray-700 dark:text-gray-300">
                    ${v.dispatch_price_impact.toFixed(1)}
                  </td>
                  <td className="px-4 py-3">{causeBadge(v.cause)}</td>
                  <td className="px-4 py-3 text-center">
                    {v.resolved ? (
                      <span className="px-2 py-0.5 rounded text-xs font-semibold bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-300">Resolved</span>
                    ) : (
                      <span className="px-2 py-0.5 rounded text-xs font-semibold bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-300">Active</span>
                    )}
                  </td>
                </tr>
              ))}
              {violations.length === 0 && (
                <tr>
                  <td colSpan={8} className="px-4 py-8 text-center text-sm text-gray-400 dark:text-gray-500">
                    No violations recorded today.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Binding Heatmap */}
      <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-200 dark:border-gray-700 overflow-hidden">
        <div className="px-5 py-4 border-b border-gray-200 dark:border-gray-700 flex items-center justify-between">
          <h3 className="text-sm font-semibold text-gray-800 dark:text-gray-200">Binding Heatmap (Hour × Day-of-Week)</h3>
          <select
            value={heatmapRegion}
            onChange={e => setHeatmapRegion(e.target.value)}
            className="px-2 py-1 text-xs rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800"
          >
            {REGIONS.filter(r => r !== 'All').map(r => <option key={r} value={r}>{r}</option>)}
          </select>
        </div>
        <div className="p-4 overflow-x-auto">
          {heatmapData.length > 0 ? (
            <table className="text-xs">
              <thead>
                <tr>
                  <th className="px-1 py-1 text-gray-500">Hour</th>
                  {['Mon','Tue','Wed','Thu','Fri','Sat','Sun'].map((d, i) => (
                    <th key={d} className="px-1 py-1 text-gray-500 w-10 text-center">{d}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {Array.from({ length: 24 }, (_, hour) => (
                  <tr key={hour}>
                    <td className="px-1 py-0.5 text-gray-500 font-mono text-right">{String(hour).padStart(2, '0')}</td>
                    {Array.from({ length: 7 }, (_, dayIdx) => {
                      // Spark DAYOFWEEK: 1=Sun,2=Mon,...,7=Sat. dayIdx: 0=Mon,...,6=Sun
                      const day = dayIdx === 6 ? 1 : dayIdx + 2
                      const cell = heatmapData.find(c => c.hour === hour && c.day === day)
                      const pct = cell?.binding_pct ?? 0
                      const bg = pct > 30 ? 'bg-red-500' : pct > 15 ? 'bg-orange-400' : pct > 5 ? 'bg-yellow-300' : 'bg-green-100 dark:bg-green-900/30'
                      const text = pct > 15 ? 'text-white' : 'text-gray-700 dark:text-gray-300'
                      return (
                        <td key={dayIdx} className={`px-1 py-0.5 text-center rounded ${bg} ${text}`} title={`${pct}% binding`}>
                          {pct > 0 ? pct.toFixed(0) : ''}
                        </td>
                      )
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <div className="text-center text-gray-400 py-8 text-sm">No binding heatmap data available</div>
          )}
        </div>
      </div>

      {/* Price Separation */}
      <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-200 dark:border-gray-700 overflow-hidden">
        <div className="px-5 py-4 border-b border-gray-200 dark:border-gray-700">
          <h3 className="text-sm font-semibold text-gray-800 dark:text-gray-200">Inter-Regional Price Separation (24h)</h3>
        </div>
        <div className="p-4">
          {separationData.length > 0 ? (
            <ResponsiveContainer width="100%" height={250}>
              <BarChart data={separationData} margin={{ left: 10, right: 10, top: 5, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
                <XAxis dataKey="region_a" tickFormatter={(v: string, i: number) => `${separationData[i]?.region_a}-${separationData[i]?.region_b}`} tick={{ fontSize: 10 }} />
                <YAxis tick={{ fontSize: 10 }} label={{ value: '$/MWh', angle: -90, position: 'insideLeft', style: { fontSize: 10 } }} />
                <Tooltip formatter={(v: number) => [`$${v.toFixed(2)}`, '']} />
                <Bar dataKey="avg_spread" name="Avg Spread" fill="#3b82f6" />
                <Bar dataKey="max_spread" name="Max Spread" fill="#ef4444" opacity={0.6} />
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <div className="text-center text-gray-400 py-8 text-sm">No price separation data available</div>
          )}
        </div>
      </div>

      {/* Footer note */}
      <p className="text-xs text-gray-400 dark:text-gray-500 text-right">
        Constraint data sourced from NEMDE dispatch engine. Shadow prices are Lagrange multipliers from the LP solution.
      </p>
    </div>
  )
}
