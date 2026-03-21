import { useEffect, useState } from 'react'
import { AlertTriangle } from 'lucide-react'
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
  Cell,
} from 'recharts'
import {
  getNetworkCongestionReliefDashboard,
  ENCRDashboard,
  ENCRZone,
  ENCRSolution,
  ENCRCurtailment,
  ENCRFinancial,
  ENCRForecast,
} from '../api/client'

// ---------------------------------------------------------------------------
// Colour maps
// ---------------------------------------------------------------------------

const SEVERITY_COLORS: Record<string, string> = {
  Critical: '#ef4444',
  High:     '#f97316',
  Medium:   '#f59e0b',
  Low:      '#22c55e',
}

const SOLUTION_COLORS: Record<string, string> = {
  'Transmission Upgrade':   '#6366f1',
  'BESS':                   '#22c55e',
  'Flexible Load':          '#f59e0b',
  'Demand Response':        '#ec4899',
  'Network Reconfiguration':'#06b6d4',
  'DER Aggregation':        '#8b5cf6',
  'StatCom':                '#14b8a6',
  'New Line':               '#f43f5e',
}

const LINE_COLORS = ['#6366f1', '#22c55e', '#f59e0b', '#ec4899']

// ---------------------------------------------------------------------------
// KPI card
// ---------------------------------------------------------------------------

function KpiCard({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="bg-gray-800 rounded-xl p-4 flex flex-col gap-1 border border-gray-700">
      <span className="text-xs text-gray-400 uppercase tracking-wide">{label}</span>
      <span className="text-2xl font-bold text-white">{value}</span>
      {sub && <span className="text-xs text-gray-500">{sub}</span>}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Chart 1 — Zone energy_curtailed_gwh_pa coloured by congestion_severity
// ---------------------------------------------------------------------------

function ZoneCurtailmentChart({ zones }: { zones: ENCRZone[] }) {
  const data = zones.map(z => ({
    name: z.zone_name.length > 20 ? z.zone_name.slice(0, 20) + '…' : z.zone_name,
    fullName: z.zone_name,
    energy_curtailed_gwh_pa: z.energy_curtailed_gwh_pa,
    severity: z.congestion_severity,
  })).sort((a, b) => b.energy_curtailed_gwh_pa - a.energy_curtailed_gwh_pa)

  return (
    <div className="bg-gray-800 rounded-xl p-4 border border-gray-700">
      <h3 className="text-sm font-semibold text-gray-300 mb-3">
        Zone Energy Curtailed (GWh/yr) by Congestion Severity
      </h3>
      <ResponsiveContainer width="100%" height={290}>
        <BarChart data={data} margin={{ top: 8, right: 16, left: 0, bottom: 80 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis dataKey="name" tick={{ fill: '#9ca3af', fontSize: 9 }} angle={-40} textAnchor="end" />
          <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} label={{ value: 'GWh/yr', angle: -90, position: 'insideLeft', fill: '#6b7280', fontSize: 11 }} />
          <Tooltip
            contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
            labelStyle={{ color: '#e5e7eb' }}
            formatter={(val: number, _: string, props: { payload?: { fullName?: string; severity?: string } }) => [
              `${val.toFixed(1)} GWh/yr`,
              props?.payload?.fullName ?? '',
            ]}
          />
          <Bar dataKey="energy_curtailed_gwh_pa" name="GWh Curtailed/yr" radius={[4, 4, 0, 0]} isAnimationActive={false}>
            {data.map((entry, idx) => (
              <Cell key={`cell-${idx}`} fill={SEVERITY_COLORS[entry.severity] ?? '#6b7280'} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
      <div className="flex flex-wrap gap-3 mt-2">
        {Object.entries(SEVERITY_COLORS).map(([sev, col]) => (
          <span key={sev} className="flex items-center gap-1 text-xs text-gray-400">
            <span className="inline-block w-2.5 h-2.5 rounded-full" style={{ backgroundColor: col }} />
            {sev}
          </span>
        ))}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Chart 2 — Solution energy_unlocked_gwh_pa vs cost_m_aud grouped by solution_type
// ---------------------------------------------------------------------------

function SolutionGroupedChart({ solutions }: { solutions: ENCRSolution[] }) {
  // aggregate by solution_type
  const byType: Record<string, { energy_unlocked_gwh_pa: number; cost_m_aud: number; count: number }> = {}
  for (const s of solutions) {
    if (!byType[s.solution_type]) byType[s.solution_type] = { energy_unlocked_gwh_pa: 0, cost_m_aud: 0, count: 0 }
    byType[s.solution_type].energy_unlocked_gwh_pa += s.energy_unlocked_gwh_pa
    byType[s.solution_type].cost_m_aud += s.cost_m_aud
    byType[s.solution_type].count += 1
  }
  const data = Object.entries(byType).map(([stype, vals]) => ({
    name: stype.length > 18 ? stype.slice(0, 18) + '…' : stype,
    fullName: stype,
    energy_unlocked_gwh_pa: Math.round(vals.energy_unlocked_gwh_pa * 10) / 10,
    cost_m_aud: Math.round(vals.cost_m_aud * 10) / 10,
  })).sort((a, b) => b.energy_unlocked_gwh_pa - a.energy_unlocked_gwh_pa)

  return (
    <div className="bg-gray-800 rounded-xl p-4 border border-gray-700">
      <h3 className="text-sm font-semibold text-gray-300 mb-3">
        Solution Type: Energy Unlocked (GWh/yr) vs Cost ($M) — Grouped by Type
      </h3>
      <ResponsiveContainer width="100%" height={290}>
        <BarChart data={data} margin={{ top: 8, right: 16, left: 0, bottom: 60 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis dataKey="name" tick={{ fill: '#9ca3af', fontSize: 10 }} angle={-30} textAnchor="end" />
          <YAxis yAxisId="left" tick={{ fill: '#9ca3af', fontSize: 11 }} label={{ value: 'GWh/yr', angle: -90, position: 'insideLeft', fill: '#6b7280', fontSize: 11 }} />
          <YAxis yAxisId="right" orientation="right" tick={{ fill: '#9ca3af', fontSize: 11 }} label={{ value: '$M', angle: 90, position: 'insideRight', fill: '#6b7280', fontSize: 11 }} />
          <Tooltip
            contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
            labelStyle={{ color: '#e5e7eb' }}
          />
          <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 12 }} />
          <Bar yAxisId="left" dataKey="energy_unlocked_gwh_pa" name="Energy Unlocked (GWh/yr)" fill="#22c55e" radius={[4, 4, 0, 0]} isAnimationActive={false} />
          <Bar yAxisId="right" dataKey="cost_m_aud" name="Cost ($M)" fill="#f59e0b" radius={[4, 4, 0, 0]} isAnimationActive={false} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Chart 3 — Stacked bar: quarterly solar + wind curtailment by zone 2022–2024
// ---------------------------------------------------------------------------

function QuarterlyCurtailmentChart({ curtailment, zones }: { curtailment: ENCRCurtailment[]; zones: ENCRZone[] }) {
  const zoneIds = Array.from(new Set(curtailment.map(c => c.zone_id))).slice(0, 3)
  const zoneNames: Record<string, string> = {}
  for (const z of zones) {
    zoneNames[z.zone_id] = z.zone_name.length > 18 ? z.zone_name.slice(0, 18) + '…' : z.zone_name
  }

  const rows: Record<string, Record<string, number> & { label: string }> = {}
  for (const c of curtailment) {
    const key = `${c.year}-${c.quarter}`
    if (!rows[key]) rows[key] = { label: key } as Record<string, number> & { label: string }
    rows[key][`${c.zone_id}_solar`] = (rows[key][`${c.zone_id}_solar`] ?? 0) + c.solar_curtailed_gwh
    rows[key][`${c.zone_id}_wind`] = (rows[key][`${c.zone_id}_wind`] ?? 0) + c.wind_curtailed_gwh
  }
  const data = Object.values(rows).sort((a, b) => a.label.localeCompare(b.label))

  const solarColors = ['#f59e0b', '#f97316', '#ef4444']
  const windColors  = ['#3b82f6', '#6366f1', '#8b5cf6']

  return (
    <div className="bg-gray-800 rounded-xl p-4 border border-gray-700">
      <h3 className="text-sm font-semibold text-gray-300 mb-3">
        Quarterly Solar + Wind Curtailment by Zone (2022–2024)
      </h3>
      <ResponsiveContainer width="100%" height={290}>
        <BarChart data={data} margin={{ top: 8, right: 16, left: 0, bottom: 60 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis dataKey="label" tick={{ fill: '#9ca3af', fontSize: 9 }} angle={-45} textAnchor="end" />
          <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} label={{ value: 'GWh', angle: -90, position: 'insideLeft', fill: '#6b7280', fontSize: 11 }} />
          <Tooltip
            contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
            labelStyle={{ color: '#e5e7eb' }}
          />
          <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 10 }} />
          {zoneIds.map((zid, idx) => [
            <Bar key={`${zid}_solar`} dataKey={`${zid}_solar`} name={`${zoneNames[zid] ?? zid} Solar`} stackId="stack" fill={solarColors[idx] ?? '#f59e0b'} isAnimationActive={false} />,
            <Bar key={`${zid}_wind`} dataKey={`${zid}_wind`} name={`${zoneNames[zid] ?? zid} Wind`} stackId="stack" fill={windColors[idx] ?? '#3b82f6'} isAnimationActive={false} />,
          ])}
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Chart 4 — Line: regional total_congestion_cost_m_aud quarterly trend 2022–2024
// ---------------------------------------------------------------------------

function RegionalCongestionCostChart({ financial }: { financial: ENCRFinancial[] }) {
  const regions = Array.from(new Set(financial.map(f => f.region)))
  const quarters: string[] = []
  const seen = new Set<string>()
  for (const f of financial) {
    const key = `${f.year}-${f.quarter}`
    if (!seen.has(key)) { seen.add(key); quarters.push(key) }
  }
  quarters.sort()

  const dataMap: Record<string, Record<string, number>> = {}
  for (const q of quarters) dataMap[q] = { label: q } as unknown as Record<string, number>
  for (const f of financial) {
    const key = `${f.year}-${f.quarter}`
    dataMap[key][f.region] = f.total_congestion_cost_m_aud
  }
  const data = quarters.map(q => dataMap[q])

  const rColors = ['#6366f1', '#22c55e', '#f59e0b', '#ec4899', '#06b6d4']

  return (
    <div className="bg-gray-800 rounded-xl p-4 border border-gray-700">
      <h3 className="text-sm font-semibold text-gray-300 mb-3">
        Regional Total Congestion Cost ($M) — Quarterly Trend 2022–2024
      </h3>
      <ResponsiveContainer width="100%" height={290}>
        <LineChart data={data} margin={{ top: 8, right: 16, left: 0, bottom: 60 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis dataKey="label" tick={{ fill: '#9ca3af', fontSize: 9 }} angle={-45} textAnchor="end" />
          <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} label={{ value: '$M', angle: -90, position: 'insideLeft', fill: '#6b7280', fontSize: 11 }} />
          <Tooltip
            contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
            labelStyle={{ color: '#e5e7eb' }}
          />
          <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 12 }} />
          {regions.map((reg, idx) => (
            <Line
              key={reg}
              type="monotone"
              dataKey={reg}
              name={reg}
              stroke={rColors[idx % rColors.length]}
              strokeWidth={2}
              dot={{ r: 3 }}
              isAnimationActive={false}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Chart 5 — Line: zone forecast_curtailment_gwh trajectory 2025–2029 top 4 zones
// ---------------------------------------------------------------------------

function ForecastCurtailmentChart({ forecast, zones }: { forecast: ENCRForecast[]; zones: ENCRZone[] }) {
  const zoneIds = Array.from(new Set(forecast.map(f => f.zone_id))).slice(0, 4)
  const zoneNames: Record<string, string> = {}
  for (const z of zones) {
    zoneNames[z.zone_id] = z.zone_name.length > 22 ? z.zone_name.slice(0, 22) + '…' : z.zone_name
  }

  const years = [2025, 2026, 2027, 2028, 2029]
  const dataMap: Record<number, Record<string, number | string>> = {}
  for (const yr of years) dataMap[yr] = { year: yr }
  for (const f of forecast) {
    if (zoneIds.includes(f.zone_id)) {
      dataMap[f.year][f.zone_id] = f.forecast_curtailment_gwh
    }
  }
  const data = years.map(yr => dataMap[yr])

  return (
    <div className="bg-gray-800 rounded-xl p-4 border border-gray-700">
      <h3 className="text-sm font-semibold text-gray-300 mb-3">
        Forecast Curtailment (GWh) Trajectory 2025–2029 — Top 4 Zones
      </h3>
      <ResponsiveContainer width="100%" height={290}>
        <LineChart data={data} margin={{ top: 8, right: 16, left: 0, bottom: 8 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis dataKey="year" tick={{ fill: '#9ca3af', fontSize: 11 }} />
          <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} label={{ value: 'GWh', angle: -90, position: 'insideLeft', fill: '#6b7280', fontSize: 11 }} />
          <Tooltip
            contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
            labelStyle={{ color: '#e5e7eb' }}
          />
          <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 12 }} />
          {zoneIds.map((zid, idx) => (
            <Line
              key={zid}
              type="monotone"
              dataKey={zid}
              name={zoneNames[zid] ?? zid}
              stroke={LINE_COLORS[idx % LINE_COLORS.length]}
              strokeWidth={2}
              dot={{ r: 4 }}
              isAnimationActive={false}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

export default function NetworkCongestionReliefAnalytics() {
  const [data, setData] = useState<ENCRDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getNetworkCongestionReliefDashboard()
      .then(setData)
      .catch((err: unknown) => setError(String(err)))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 bg-gray-900">
        <span className="text-gray-400 text-sm animate-pulse">Loading congestion relief data…</span>
      </div>
    )
  }
  if (error || !data) {
    return (
      <div className="flex items-center justify-center h-64 bg-gray-900">
        <span className="text-red-400 text-sm">{error ?? 'Failed to load data'}</span>
      </div>
    )
  }

  const { zones, solutions, curtailment, financial, forecast, summary } = data

  return (
    <div className="min-h-screen bg-gray-900 p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center gap-3">
        <AlertTriangle className="text-amber-400" size={28} />
        <div>
          <h1 className="text-xl font-bold text-white">Electricity Network Congestion Relief Analytics</h1>
          <p className="text-xs text-gray-400 mt-0.5">
            NEM network congestion zones, relief solutions, curtailment trends and financial impacts
          </p>
        </div>
      </div>

      {/* KPI cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KpiCard
          label="Total Zones"
          value={String(summary.total_zones)}
          sub="NEM congestion zones tracked"
        />
        <KpiCard
          label="Total Curtailed GWh (2024)"
          value={`${summary.total_curtailed_gwh_2024.toLocaleString()} GWh`}
          sub="Renewable energy lost to congestion"
        />
        <KpiCard
          label="Congestion Cost 2024"
          value={`$${summary.total_congestion_cost_m_aud_2024.toFixed(1)}M`}
          sub="Total NEM congestion costs"
        />
        <KpiCard
          label="Relief Investment"
          value={`$${summary.total_relief_investment_m_aud.toFixed(0)}M`}
          sub="Committed network relief spend"
        />
      </div>

      {/* Secondary KPIs */}
      <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
        <KpiCard
          label="Most Congested Zone"
          value={summary.most_congested_zone.length > 22 ? summary.most_congested_zone.slice(0, 22) + '…' : summary.most_congested_zone}
          sub="Highest annual curtailment"
        />
        <KpiCard
          label="Energy Unlocked (Solutions)"
          value={`${summary.total_energy_unlocked_gwh_pa.toLocaleString()} GWh/yr`}
          sub="Across all relief solutions"
        />
        <KpiCard
          label="Avg Benefit-Cost Ratio"
          value={summary.avg_benefit_cost_ratio.toFixed(2)}
          sub="Average BCR across solutions"
        />
      </div>

      {/* Charts row 1 */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <ZoneCurtailmentChart zones={zones} />
        <SolutionGroupedChart solutions={solutions} />
      </div>

      {/* Charts row 2 */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <QuarterlyCurtailmentChart curtailment={curtailment} zones={zones} />
        <RegionalCongestionCostChart financial={financial} />
      </div>

      {/* Chart row 3 */}
      <div className="grid grid-cols-1 gap-6">
        <ForecastCurtailmentChart forecast={forecast} zones={zones} />
      </div>

      {/* Zone table */}
      <div className="bg-gray-800 rounded-xl border border-gray-700 overflow-hidden">
        <div className="px-4 py-3 border-b border-gray-700">
          <h3 className="text-sm font-semibold text-gray-300">Congestion Zone Summary</h3>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-xs text-gray-300">
            <thead className="bg-gray-750">
              <tr className="border-b border-gray-700">
                <th className="px-3 py-2 text-left font-medium text-gray-400">Zone</th>
                <th className="px-3 py-2 text-left font-medium text-gray-400">Region</th>
                <th className="px-3 py-2 text-left font-medium text-gray-400">Type</th>
                <th className="px-3 py-2 text-left font-medium text-gray-400">Severity</th>
                <th className="px-3 py-2 text-right font-medium text-gray-400">Curtailed GWh/yr</th>
                <th className="px-3 py-2 text-right font-medium text-gray-400">Cost $M/yr</th>
                <th className="px-3 py-2 text-left font-medium text-gray-400">Relief Status</th>
              </tr>
            </thead>
            <tbody>
              {zones.map(z => (
                <tr key={z.zone_id} className="border-b border-gray-700 hover:bg-gray-750">
                  <td className="px-3 py-2 font-medium text-white">{z.zone_name}</td>
                  <td className="px-3 py-2">{z.region}</td>
                  <td className="px-3 py-2">{z.zone_type}</td>
                  <td className="px-3 py-2">
                    <span
                      className="px-2 py-0.5 rounded-full text-xs font-medium"
                      style={{ backgroundColor: `${SEVERITY_COLORS[z.congestion_severity] ?? '#6b7280'}22`, color: SEVERITY_COLORS[z.congestion_severity] ?? '#9ca3af' }}
                    >
                      {z.congestion_severity}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-right">{z.energy_curtailed_gwh_pa.toFixed(1)}</td>
                  <td className="px-3 py-2 text-right">{z.congestion_cost_m_aud_pa.toFixed(1)}</td>
                  <td className="px-3 py-2">{z.relief_status}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
