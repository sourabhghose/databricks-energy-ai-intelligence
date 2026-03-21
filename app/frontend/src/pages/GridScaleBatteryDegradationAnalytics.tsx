import { useEffect, useState } from 'react'
import {
  BarChart,
  Bar,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import { Battery } from 'lucide-react'
import {
  getGridScaleBatteryDegradationDashboard,
  GSBDDashboard,
  GSBDBattery,
} from '../api/client'

// ── Helpers ───────────────────────────────────────────────────────────────

const CHEMISTRY_COLOURS: Record<string, string> = {
  LFP: '#22c55e',
  NMC: '#3b82f6',
  NCA: '#f59e0b',
}

const EVENT_TYPE_COLOURS: Record<string, string> = {
  'Cell Replacement': '#ef4444',
  'BMS Upgrade': '#3b82f6',
  'Thermal Management Repair': '#f97316',
  'Module Swap': '#a855f7',
  Inspection: '#6b7280',
}

function KpiCard({
  label,
  value,
  sub,
}: {
  label: string
  value: string
  sub?: string
}) {
  return (
    <div className="bg-gray-800 rounded-xl p-4 flex flex-col gap-1 min-w-0">
      <span className="text-xs text-gray-400 uppercase tracking-wide">{label}</span>
      <span className="text-2xl font-bold text-white truncate">{value}</span>
      {sub && <span className="text-xs text-gray-500">{sub}</span>}
    </div>
  )
}

// ── Chart 1 helpers — latest SoH by battery ───────────────────────────────

function buildSohByBattery(dashboard: GSBDDashboard) {
  return dashboard.batteries.map((b: GSBDBattery) => {
    const metrics = dashboard.health_metrics.filter(
      (m) => m.battery_id === b.battery_id,
    )
    const latest = metrics.sort(
      (a, b) => a.year !== b.year ? b.year - a.year : b.quarter - a.quarter,
    )[0]
    return {
      name: b.battery_name.replace(' Energy Storage', '').replace(' Power Reserve', ' PR').replace(' Super Battery', ' SB').replace(' Big Battery', ' BB'),
      soh: latest?.state_of_health_pct ?? 0,
      chemistry: b.chemistry,
      fill: CHEMISTRY_COLOURS[b.chemistry] ?? '#6b7280',
    }
  })
}

// ── Chart 2 helpers — SoH trend for top-4 oldest batteries ───────────────

function buildSohTrend(dashboard: GSBDDashboard) {
  const top4 = [...dashboard.batteries]
    .sort((a, b) => b.operating_years - a.operating_years)
    .slice(0, 4)

  const pointMap: Record<string, Record<string, number>> = {}
  for (const batt of top4) {
    const battMetrics = dashboard.health_metrics
      .filter((m) => m.battery_id === batt.battery_id)
      .sort((a, b) => a.year !== b.year ? a.year - b.year : a.quarter - b.quarter)
    for (const m of battMetrics) {
      const key = `${m.year} Q${m.quarter}`
      if (!pointMap[key]) pointMap[key] = { period: key as unknown as number }
      pointMap[key][batt.battery_name] = m.state_of_health_pct
    }
  }
  return { data: Object.values(pointMap), batteries: top4 }
}

// ── Chart 3 helpers — degradation model curves (Combined, 25°C) ──────────

function buildDegradationCurves(dashboard: GSBDDashboard) {
  const filtered = dashboard.degradation_models.filter(
    (m) => m.model_type === 'Combined' && m.temperature_c === 25,
  )
  const byYear: Record<number, Record<string, number>> = {}
  for (const m of filtered) {
    if (!byYear[m.operating_years]) byYear[m.operating_years] = { operating_years: m.operating_years }
    byYear[m.operating_years][m.chemistry] = m.predicted_soh_pct
  }
  return Object.values(byYear).sort((a, b) => a.operating_years - b.operating_years)
}

// ── Chart 4 helpers — avg DoD by battery (2024) ──────────────────────────

function buildDoDByBattery(dashboard: GSBDDashboard) {
  return dashboard.batteries.map((b: GSBDBattery) => {
    const patterns = dashboard.cycling_patterns.filter(
      (p) => p.battery_id === b.battery_id && p.year === 2024,
    )
    const avgDod =
      patterns.length > 0
        ? patterns.reduce((s, p) => s + p.avg_dod_pct, 0) / patterns.length
        : 0
    const avgStress =
      patterns.length > 0
        ? patterns.reduce((s, p) => s + p.high_stress_cycles, 0) / patterns.length
        : 0
    return {
      name: b.battery_name
        .replace(' Energy Storage', '')
        .replace(' Power Reserve', ' PR')
        .replace(' Super Battery', ' SB')
        .replace(' Big Battery', ' BB'),
      avg_dod: Math.round(avgDod * 10) / 10,
      stress_intensity: Math.round(avgStress),
    }
  })
}

// ── Chart 5 helpers — maintenance cost by event type ─────────────────────

function buildMaintenanceCostByType(dashboard: GSBDDashboard) {
  const costMap: Record<string, number> = {}
  for (const ev of dashboard.maintenance_events) {
    costMap[ev.event_type] = (costMap[ev.event_type] ?? 0) + ev.cost_m
  }
  return Object.entries(costMap)
    .map(([event_type, total_cost]) => ({
      event_type,
      total_cost: Math.round(total_cost * 100) / 100,
      fill: EVENT_TYPE_COLOURS[event_type] ?? '#6b7280',
    }))
    .sort((a, b) => b.total_cost - a.total_cost)
}

// ── Main component ────────────────────────────────────────────────────────

export default function GridScaleBatteryDegradationAnalytics() {
  const [data, setData] = useState<GSBDDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getGridScaleBatteryDegradationDashboard()
      .then(setData)
      .catch((e: unknown) =>
        setError(e instanceof Error ? e.message : 'Failed to load data'),
      )
      .finally(() => setLoading(false))
  }, [])

  if (loading)
    return (
      <div className="flex items-center justify-center h-64 text-gray-400">
        Loading Grid-Scale Battery Degradation Analytics…
      </div>
    )
  if (error || !data)
    return (
      <div className="flex items-center justify-center h-64 text-red-400">
        {error ?? 'No data available.'}
      </div>
    )

  const { summary } = data
  const sohByBattery = buildSohByBattery(data)
  const { data: sohTrend, batteries: top4Batteries } = buildSohTrend(data)
  const degradationCurves = buildDegradationCurves(data)
  const dodByBattery = buildDoDByBattery(data)
  const maintenanceCostByType = buildMaintenanceCostByType(data)

  return (
    <div className="p-6 space-y-8 bg-gray-950 min-h-screen text-white">
      {/* Page header */}
      <div className="flex items-center gap-3">
        <Battery size={28} className="text-green-400" />
        <div>
          <h1 className="text-2xl font-bold">Grid-Scale Battery Degradation Analytics</h1>
          <p className="text-sm text-gray-400">
            Fleet health monitoring, degradation modelling, and maintenance analytics for grid-scale BESS
          </p>
        </div>
      </div>

      {/* KPI cards */}
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-4">
        <KpiCard
          label="Avg Fleet SoH"
          value={`${summary.avg_fleet_soh_pct}%`}
          sub="State of Health"
        />
        <KpiCard
          label="Total Capacity Loss"
          value={`${summary.total_capacity_loss_mwh.toLocaleString()} MWh`}
          sub="vs. nameplate"
        />
        <KpiCard
          label="Oldest Battery"
          value={summary.oldest_battery_name}
          sub="Longest in service"
        />
        <KpiCard
          label="Highest Deg. Rate"
          value={`${summary.highest_degradation_rate_pa_pct}%/yr`}
          sub="Worst-case battery"
        />
        <KpiCard
          label="Total Maintenance Cost"
          value={`$${summary.total_maintenance_cost_m.toFixed(1)}M`}
          sub="All events"
        />
      </div>

      {/* Charts — row 1 */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
        {/* Chart 1 — SoH by battery */}
        <div className="bg-gray-800 rounded-xl p-4">
          <h2 className="text-sm font-semibold text-gray-300 mb-4">
            State of Health by Battery (Latest Reading)
          </h2>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={sohByBattery} margin={{ top: 8, right: 16, left: 0, bottom: 60 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                dataKey="name"
                tick={{ fill: '#9ca3af', fontSize: 10 }}
                angle={-35}
                textAnchor="end"
                interval={0}
              />
              <YAxis domain={[70, 100]} tick={{ fill: '#9ca3af', fontSize: 11 }} unit="%" />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                labelStyle={{ color: '#f9fafb' }}
                formatter={((v: number, _n: string, entry: { payload: { chemistry: string } }) => [
                  `${v}%`,
                  `SoH (${entry.payload.chemistry})`,
                ]) as any}
              />
              <Bar dataKey="soh" name="SoH %" radius={[4, 4, 0, 0]}>
                {sohByBattery.map((entry, index) => (
                  <rect key={index} fill={entry.fill} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
          {/* Chemistry legend */}
          <div className="flex gap-4 mt-2 justify-center flex-wrap">
            {Object.entries(CHEMISTRY_COLOURS).map(([chem, colour]) => (
              <div key={chem} className="flex items-center gap-1 text-xs text-gray-400">
                <span className="w-3 h-3 rounded-sm inline-block" style={{ backgroundColor: colour }} />
                {chem}
              </div>
            ))}
          </div>
        </div>

        {/* Chart 2 — SoH over time for top-4 oldest */}
        <div className="bg-gray-800 rounded-xl p-4">
          <h2 className="text-sm font-semibold text-gray-300 mb-4">
            SoH Trend — Top 4 Oldest Batteries (Quarterly)
          </h2>
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={sohTrend} margin={{ top: 8, right: 16, left: 0, bottom: 60 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                dataKey="period"
                tick={{ fill: '#9ca3af', fontSize: 9 }}
                angle={-45}
                textAnchor="end"
                interval={3}
              />
              <YAxis domain={[70, 100]} tick={{ fill: '#9ca3af', fontSize: 11 }} unit="%" />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                labelStyle={{ color: '#f9fafb' }}
              />
              <Legend
                wrapperStyle={{ fontSize: 10, color: '#9ca3af', paddingTop: 8 }}
                iconType="line"
              />
              {top4Batteries.map((batt, i) => {
                const colours = ['#22c55e', '#3b82f6', '#f59e0b', '#ec4899']
                return (
                  <Line
                    key={batt.battery_id}
                    type="monotone"
                    dataKey={batt.battery_name}
                    stroke={colours[i % colours.length]}
                    strokeWidth={2}
                    dot={false}
                  />
                )
              })}
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Charts — row 2 */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
        {/* Chart 3 — Degradation model curves */}
        <div className="bg-gray-800 rounded-xl p-4">
          <h2 className="text-sm font-semibold text-gray-300 mb-1">
            Predicted SoH by Operating Years — Combined Model @ 25°C
          </h2>
          <p className="text-xs text-gray-500 mb-4">Chemistry comparison (LFP / NMC / NCA)</p>
          <ResponsiveContainer width="100%" height={260}>
            <LineChart data={degradationCurves} margin={{ top: 8, right: 16, left: 0, bottom: 8 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                dataKey="operating_years"
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                label={{ value: 'Years', position: 'insideBottomRight', offset: -4, fill: '#6b7280', fontSize: 11 }}
              />
              <YAxis domain={[60, 100]} tick={{ fill: '#9ca3af', fontSize: 11 }} unit="%" />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                labelStyle={{ color: '#f9fafb' }}
                formatter={(v: number) => [`${v}%`, 'Predicted SoH']}
              />
              <Legend wrapperStyle={{ fontSize: 11, color: '#9ca3af' }} iconType="line" />
              {['LFP', 'NMC', 'NCA'].map((chem) => (
                <Line
                  key={chem}
                  type="monotone"
                  dataKey={chem}
                  stroke={CHEMISTRY_COLOURS[chem]}
                  strokeWidth={2.5}
                  dot={{ r: 4, fill: CHEMISTRY_COLOURS[chem] }}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Chart 4 — Avg DoD by battery */}
        <div className="bg-gray-800 rounded-xl p-4">
          <h2 className="text-sm font-semibold text-gray-300 mb-1">
            Average Depth of Discharge by Battery (2024)
          </h2>
          <p className="text-xs text-gray-500 mb-4">Bar colour intensity indicates high-stress cycle count</p>
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={dodByBattery} margin={{ top: 8, right: 16, left: 0, bottom: 60 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                dataKey="name"
                tick={{ fill: '#9ca3af', fontSize: 10 }}
                angle={-35}
                textAnchor="end"
                interval={0}
              />
              <YAxis domain={[0, 100]} tick={{ fill: '#9ca3af', fontSize: 11 }} unit="%" />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                labelStyle={{ color: '#f9fafb' }}
                formatter={(v: number, name: string) => [
                  name === 'avg_dod' ? `${v}%` : `${v} cycles`,
                  name === 'avg_dod' ? 'Avg DoD' : 'High-Stress Cycles/mo',
                ]}
              />
              <Bar dataKey="avg_dod" name="Avg DoD %" fill="#3b82f6" radius={[4, 4, 0, 0]} />
              <Bar dataKey="stress_intensity" name="High-Stress Cycles/mo" fill="#ef4444" radius={[4, 4, 0, 0]} />
              <Legend wrapperStyle={{ fontSize: 11, color: '#9ca3af' }} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Chart 5 — Maintenance cost by event type */}
      <div className="bg-gray-800 rounded-xl p-4">
        <h2 className="text-sm font-semibold text-gray-300 mb-4">
          Total Maintenance Cost by Event Type ($M)
        </h2>
        <ResponsiveContainer width="100%" height={240}>
          <BarChart
            data={maintenanceCostByType}
            layout="vertical"
            margin={{ top: 8, right: 24, left: 160, bottom: 8 }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis
              type="number"
              tick={{ fill: '#9ca3af', fontSize: 11 }}
              tickFormatter={(v: number) => `$${v.toFixed(1)}M`}
            />
            <YAxis
              type="category"
              dataKey="event_type"
              tick={{ fill: '#9ca3af', fontSize: 11 }}
              width={155}
            />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
              labelStyle={{ color: '#f9fafb' }}
              formatter={(v: number) => [`$${v.toFixed(2)}M`, 'Total Cost']}
            />
            <Bar dataKey="total_cost" name="Total Cost ($M)" radius={[0, 4, 4, 0]}>
              {maintenanceCostByType.map((entry, index) => (
                <rect key={index} fill={entry.fill} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Summary grid */}
      <div className="bg-gray-800 rounded-xl p-5">
        <h2 className="text-sm font-semibold text-gray-300 mb-4">Fleet Summary</h2>
        <dl className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-x-6 gap-y-4 text-sm">
          <div>
            <dt className="text-gray-500 text-xs uppercase tracking-wide">Avg Fleet SoH</dt>
            <dd className="text-white font-semibold mt-1">{summary.avg_fleet_soh_pct}%</dd>
          </div>
          <div>
            <dt className="text-gray-500 text-xs uppercase tracking-wide">Total Capacity Loss</dt>
            <dd className="text-white font-semibold mt-1">{summary.total_capacity_loss_mwh.toLocaleString()} MWh</dd>
          </div>
          <div>
            <dt className="text-gray-500 text-xs uppercase tracking-wide">Oldest Battery</dt>
            <dd className="text-white font-semibold mt-1">{summary.oldest_battery_name}</dd>
          </div>
          <div>
            <dt className="text-gray-500 text-xs uppercase tracking-wide">Highest Deg. Rate</dt>
            <dd className="text-white font-semibold mt-1">{summary.highest_degradation_rate_pa_pct}%/yr</dd>
          </div>
          <div>
            <dt className="text-gray-500 text-xs uppercase tracking-wide">Total Maintenance Cost</dt>
            <dd className="text-white font-semibold mt-1">${summary.total_maintenance_cost_m.toFixed(2)}M</dd>
          </div>
        </dl>
      </div>

      {/* Battery fleet table */}
      <div className="bg-gray-800 rounded-xl p-4 overflow-x-auto">
        <h2 className="text-sm font-semibold text-gray-300 mb-4">Battery Fleet Register</h2>
        <table className="w-full text-sm text-left min-w-[700px]">
          <thead>
            <tr className="text-xs text-gray-400 uppercase border-b border-gray-700">
              <th className="pb-2 pr-4">Battery</th>
              <th className="pb-2 pr-4">Region</th>
              <th className="pb-2 pr-4">Chemistry</th>
              <th className="pb-2 pr-4">Capacity</th>
              <th className="pb-2 pr-4">Power</th>
              <th className="pb-2 pr-4">Age (yrs)</th>
              <th className="pb-2 pr-4">Manufacturer</th>
              <th className="pb-2">Warranty SoH</th>
            </tr>
          </thead>
          <tbody>
            {data.batteries.map((b) => (
              <tr key={b.battery_id} className="border-b border-gray-700 hover:bg-gray-750 transition-colors">
                <td className="py-2 pr-4 text-white font-medium">{b.battery_name}</td>
                <td className="py-2 pr-4 text-gray-300">{b.region}</td>
                <td className="py-2 pr-4">
                  <span
                    className="px-2 py-0.5 rounded text-xs font-semibold"
                    style={{
                      backgroundColor: `${CHEMISTRY_COLOURS[b.chemistry] ?? '#6b7280'}30`,
                      color: CHEMISTRY_COLOURS[b.chemistry] ?? '#9ca3af',
                    }}
                  >
                    {b.chemistry}
                  </span>
                </td>
                <td className="py-2 pr-4 text-gray-300">{b.capacity_mwh.toLocaleString()} MWh</td>
                <td className="py-2 pr-4 text-gray-300">{b.power_mw.toLocaleString()} MW</td>
                <td className="py-2 pr-4 text-gray-300">{b.operating_years}</td>
                <td className="py-2 pr-4 text-gray-300">{b.manufacturer}</td>
                <td className="py-2 text-gray-300">{b.warranty_soh_pct}% @ {b.warranty_years} yr</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
