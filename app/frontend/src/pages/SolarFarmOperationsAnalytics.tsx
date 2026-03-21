// ============================================================
// Sprint 105b — Solar Farm Operations & Maintenance Analytics
// Prefix: SFOA  |  Path: /solar-farm-operations
// ============================================================

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
import { Sun } from 'lucide-react'
import {
  getSolarFarmOperationsDashboard,
  SFOADashboard,
  SFOAInverterRecord,
  SFOAFaultEventRecord,
} from '../api/client'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function fmt1(v: number): string {
  return v.toFixed(1)
}

function fmt2(v: number): string {
  return v.toFixed(2)
}

function fmtPct(v: number): string {
  return `${v.toFixed(1)}%`
}

function conditionBadge(cond: string): string {
  switch (cond) {
    case 'Excellent': return 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/40'
    case 'Good':      return 'bg-blue-500/20 text-blue-400 border border-blue-500/40'
    case 'Fair':      return 'bg-amber-500/20 text-amber-400 border border-amber-500/40'
    case 'Poor':      return 'bg-red-500/20 text-red-400 border border-red-500/40'
    default:          return 'bg-gray-500/20 text-gray-400 border border-gray-500/40'
  }
}

function severityBadge(sev: string): string {
  switch (sev) {
    case 'Critical': return 'bg-red-600/25 text-red-400 border border-red-600/50'
    case 'Major':    return 'bg-orange-500/20 text-orange-400 border border-orange-500/40'
    case 'Moderate': return 'bg-amber-500/20 text-amber-400 border border-amber-500/40'
    case 'Minor':    return 'bg-green-500/20 text-green-400 border border-green-500/40'
    default:         return 'bg-gray-500/20 text-gray-400 border border-gray-500/40'
  }
}

function prColor(pr: number): string {
  if (pr >= 80) return '#10b981'
  if (pr >= 75) return '#3b82f6'
  if (pr >= 70) return '#f59e0b'
  return '#ef4444'
}

const MONTH_LABELS = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

// ---------------------------------------------------------------------------
// KPI Card
// ---------------------------------------------------------------------------

function KpiCard({
  label,
  value,
  unit,
  sub,
  highlight,
}: {
  label: string
  value: string
  unit?: string
  sub?: string
  highlight?: boolean
}) {
  return (
    <div className={`rounded-lg p-4 ${highlight ? 'bg-red-900/30 border border-red-700/40' : 'bg-gray-800 border border-gray-700'}`}>
      <p className="text-xs text-gray-400 mb-1">{label}</p>
      <p className={`text-2xl font-bold ${highlight ? 'text-red-400' : 'text-white'}`}>
        {value}
        {unit && <span className="text-sm font-normal text-gray-400 ml-1">{unit}</span>}
      </p>
      {sub && <p className="text-xs text-gray-500 mt-1">{sub}</p>}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------

export default function SolarFarmOperationsAnalytics() {
  const [data, setData] = useState<SFOADashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getSolarFarmOperationsDashboard()
      .then(setData)
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 bg-gray-900">
        <div className="text-gray-400 animate-pulse">Loading Solar Farm O&M data…</div>
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="flex items-center justify-center h-64 bg-gray-900">
        <div className="text-red-400">Error loading data: {error}</div>
      </div>
    )
  }

  const { farms, inverters, strings, maintenance, fault_events, performance, summary } = data

  // ------------------------------------------------------------------
  // Derived chart data
  // ------------------------------------------------------------------

  // Farm performance ratio vs 79% benchmark
  const farmPrData = farms.map((f) => ({
    name: f.farm_name.replace(' Solar Farm', '').replace(' Solar Park', '').replace(' Solar Project', '').replace(' Solar', ''),
    pr: f.performance_ratio_pct,
    benchmark: 79.0,
    fill: prColor(f.performance_ratio_pct),
    condition: f.asset_condition,
  }))

  // Monthly PR & capacity factor trend for 5 farms
  const farmIds5 = farms.slice(0, 5).map((f) => f.farm_id)
  const farmNames5 = farms.slice(0, 5).map((f) =>
    f.farm_name.replace(' Solar Farm', '').replace(' Solar Park', '').replace(' Solar Project', '').replace(' Solar', '')
  )

  const monthlyMap: Record<string, Record<string, number>> = {}
  performance.forEach((p) => {
    if (!farmIds5.includes(p.farm_id)) return
    const key = `${MONTH_LABELS[p.month]}`
    if (!monthlyMap[key]) monthlyMap[key] = { month: p.month }
    const fname = farmNames5[farmIds5.indexOf(p.farm_id)]
    monthlyMap[key][`${fname}_pr`] = p.performance_ratio_pct
    monthlyMap[key][`${fname}_cf`] = p.capacity_factor_pct
  })
  const monthlyTrendData = Object.entries(monthlyMap)
    .map(([label, vals]) => ({ label, ...vals }))
    .sort((a, b) => ((a as any).month as number) - ((b as any).month as number))

  // Fault events by category and severity (stacked bar)
  const faultCats = ["Inverter", "String", "Panel", "Tracker", "SCADA", "Grid Connection", "Weather", "Vegetation", "Bird Strike"]
  const faultByCat: Record<string, Record<string, number>> = {}
  faultCats.forEach((cat) => {
    faultByCat[cat] = { Minor: 0, Moderate: 0, Major: 0, Critical: 0 }
  })
  fault_events.forEach((fe) => {
    if (faultByCat[fe.fault_category]) {
      faultByCat[fe.fault_category][fe.severity] = (faultByCat[fe.fault_category][fe.severity] || 0) + 1
    }
  })
  const faultStackData = Object.entries(faultByCat)
    .filter(([, vals]) => Object.values(vals).some((v) => v > 0))
    .map(([cat, vals]) => ({ cat: cat.replace(' Connection', '').replace('Bird ', 'Bird\n'), ...vals }))

  // Maintenance costs by type (annual)
  const maintCostByType: Record<string, number> = {}
  maintenance.forEach((m) => {
    maintCostByType[m.maintenance_type] = (maintCostByType[m.maintenance_type] || 0) + m.cost_aud
  })
  const maintCostData = Object.entries(maintCostByType)
    .map(([type, cost]) => ({ type: type.replace(' Inspection', ' Insp.').replace(' Control', ' Ctrl'), cost: Math.round(cost / 1000) }))
    .sort((a, b) => b.cost - a.cost)

  // Active fault count
  const activeFaultCount = summary['active_inverter_faults'] as number

  // Recent fault events (last 8)
  const recentFaults: SFOAFaultEventRecord[] = [...fault_events]
    .sort((a, b) => b.fault_date.localeCompare(a.fault_date))
    .slice(0, 8)

  // Inverter overview (first 12)
  const inverterSample: SFOAInverterRecord[] = inverters.slice(0, 12)

  return (
    <div className="min-h-screen bg-gray-900 text-gray-100 p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center gap-3">
        <Sun className="text-amber-400" size={28} />
        <div>
          <h1 className="text-xl font-bold text-white">Solar Farm Operations & Maintenance Analytics</h1>
          <p className="text-sm text-gray-400">
            {farms.length} farms · {inverters.length} inverters · {strings.length} strings monitored
          </p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KpiCard
          label="Total Portfolio Capacity"
          value={fmt1(summary['total_capacity_mwp'] as number)}
          unit="MWp"
          sub={`${farms.length} farms across QLD/NSW/VIC/SA/WA`}
        />
        <KpiCard
          label="Avg Performance Ratio"
          value={fmtPct(summary['avg_performance_ratio_pct'] as number)}
          sub="Benchmark: 79%"
        />
        <KpiCard
          label="Avg Availability"
          value={fmtPct(summary['avg_availability_pct'] as number)}
          sub="Fleet-wide uptime"
        />
        <KpiCard
          label="Active Inverter Faults"
          value={String(activeFaultCount)}
          unit="alarms"
          sub="Requires attention"
          highlight={activeFaultCount > 0}
        />
      </div>

      {/* Row 1: Farm PR vs Benchmark + Monthly PR Trend */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Farm Performance Ratio vs 79% Benchmark */}
        <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
          <h2 className="text-sm font-semibold text-gray-200 mb-3">Farm Performance Ratio vs Benchmark (79%)</h2>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={farmPrData} layout="vertical" margin={{ left: 100, right: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis type="number" domain={[60, 90]} tick={{ fontSize: 11, fill: '#9ca3af' }} unit="%" />
              <YAxis dataKey="name" type="category" tick={{ fontSize: 10, fill: '#9ca3af' }} width={95} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                labelStyle={{ color: '#f3f4f6' }}
                formatter={(v: number) => [`${v.toFixed(1)}%`, 'PR']}
              />
              <Bar dataKey="pr" name="Performance Ratio" radius={[0, 3, 3, 0]}>
                {farmPrData.map((entry, idx) => (
                  <rect key={`cell-${idx}`} fill={entry.fill} />
                ))}
              </Bar>
              <Bar dataKey="benchmark" name="Benchmark" fill="#6b7280" opacity={0.4} radius={[0, 3, 3, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Monthly PR Trend */}
        <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
          <h2 className="text-sm font-semibold text-gray-200 mb-3">Monthly Performance Ratio Trend — Top 5 Farms (2024)</h2>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={monthlyTrendData} margin={{ right: 10 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="label" tick={{ fontSize: 11, fill: '#9ca3af' }} />
              <YAxis domain={[60, 95]} tick={{ fontSize: 11, fill: '#9ca3af' }} unit="%" />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                labelStyle={{ color: '#f3f4f6' }}
                formatter={(v: number) => [`${v?.toFixed(1)}%`]}
              />
              <Legend wrapperStyle={{ fontSize: 10 }} />
              {farmNames5.map((name, idx) => {
                const colours = ['#10b981', '#3b82f6', '#f59e0b', '#8b5cf6', '#ef4444']
                return (
                  <Line
                    key={name}
                    type="monotone"
                    dataKey={`${name}_pr`}
                    name={name}
                    stroke={colours[idx % colours.length]}
                    dot={false}
                    strokeWidth={2}
                    connectNulls
                  />
                )
              })}
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Row 2: Fault Events by Category + Maintenance Costs */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Fault Events by Category and Severity */}
        <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
          <h2 className="text-sm font-semibold text-gray-200 mb-3">Fault Events by Category & Severity (Stacked)</h2>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={faultStackData} margin={{ bottom: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="cat" tick={{ fontSize: 10, fill: '#9ca3af' }} angle={-30} textAnchor="end" interval={0} />
              <YAxis tick={{ fontSize: 11, fill: '#9ca3af' }} allowDecimals={false} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                labelStyle={{ color: '#f3f4f6' }}
              />
              <Legend wrapperStyle={{ fontSize: 10 }} />
              <Bar dataKey="Minor"    stackId="s" fill="#10b981" name="Minor"    />
              <Bar dataKey="Moderate" stackId="s" fill="#f59e0b" name="Moderate" />
              <Bar dataKey="Major"    stackId="s" fill="#f97316" name="Major"    />
              <Bar dataKey="Critical" stackId="s" fill="#ef4444" name="Critical" radius={[3, 3, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Maintenance Costs by Type */}
        <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
          <h2 className="text-sm font-semibold text-gray-200 mb-3">Maintenance Costs by Type (A$'000)</h2>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={maintCostData} margin={{ bottom: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="type" tick={{ fontSize: 10, fill: '#9ca3af' }} angle={-30} textAnchor="end" interval={0} />
              <YAxis tick={{ fontSize: 11, fill: '#9ca3af' }} unit="k" />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                labelStyle={{ color: '#f3f4f6' }}
                formatter={(v: number) => [`A$${v}k`, 'Cost']}
              />
              <Bar dataKey="cost" name="Cost (A$k)" fill="#3b82f6" radius={[3, 3, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Inverter Overview Table */}
      <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
        <h2 className="text-sm font-semibold text-gray-200 mb-3">Inverter Overview — Status & Performance</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="text-gray-400 border-b border-gray-700">
                <th className="text-left pb-2 pr-3">Inverter</th>
                <th className="text-left pb-2 pr-3">Farm</th>
                <th className="text-left pb-2 pr-3">Type</th>
                <th className="text-left pb-2 pr-3">Manufacturer</th>
                <th className="text-right pb-2 pr-3">Output kW</th>
                <th className="text-right pb-2 pr-3">Efficiency</th>
                <th className="text-right pb-2 pr-3">Temp °C</th>
                <th className="text-right pb-2 pr-3">Uptime</th>
                <th className="text-left pb-2">Status</th>
              </tr>
            </thead>
            <tbody>
              {inverterSample.map((inv) => {
                const farm = farms.find((f) => f.farm_id === inv.farm_id)
                return (
                  <tr key={inv.inverter_id} className="border-b border-gray-700/50 hover:bg-gray-700/30">
                    <td className="py-2 pr-3 font-mono text-gray-300">{inv.inverter_name}</td>
                    <td className="py-2 pr-3 text-gray-400 truncate max-w-[120px]">
                      {farm?.farm_name.replace(' Solar Farm', '').replace(' Solar Park', '').replace(' Solar Project', '') ?? inv.farm_id}
                    </td>
                    <td className="py-2 pr-3 text-gray-400">{inv.inverter_type}</td>
                    <td className="py-2 pr-3 text-gray-400">{inv.manufacturer}</td>
                    <td className="py-2 pr-3 text-right text-blue-300">{fmt1(inv.current_output_kw)}</td>
                    <td className="py-2 pr-3 text-right text-green-300">{fmtPct(inv.efficiency_pct)}</td>
                    <td className={`py-2 pr-3 text-right ${inv.temperature_c > 60 ? 'text-red-400' : 'text-gray-300'}`}>
                      {fmt1(inv.temperature_c)}
                    </td>
                    <td className="py-2 pr-3 text-right text-gray-300">{fmtPct(inv.uptime_pct)}</td>
                    <td className="py-2">
                      {inv.alarm_active ? (
                        <span className="px-2 py-0.5 rounded text-xs bg-red-600/25 text-red-400 border border-red-600/50">
                          ALARM {inv.fault_code ?? ''}
                        </span>
                      ) : (
                        <span className="px-2 py-0.5 rounded text-xs bg-green-600/20 text-green-400 border border-green-600/40">
                          Normal
                        </span>
                      )}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Recent Fault Events Table */}
      <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
        <h2 className="text-sm font-semibold text-gray-200 mb-3">Recent Fault Events</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="text-gray-400 border-b border-gray-700">
                <th className="text-left pb-2 pr-3">ID</th>
                <th className="text-left pb-2 pr-3">Date</th>
                <th className="text-left pb-2 pr-3">Farm</th>
                <th className="text-left pb-2 pr-3">Category</th>
                <th className="text-left pb-2 pr-3">Severity</th>
                <th className="text-right pb-2 pr-3">Cap Affected kW</th>
                <th className="text-right pb-2 pr-3">Energy Lost MWh</th>
                <th className="text-right pb-2 pr-3">Resolution hrs</th>
                <th className="text-left pb-2 pr-3">Detection</th>
                <th className="text-right pb-2">Rev. Impact A$</th>
              </tr>
            </thead>
            <tbody>
              {recentFaults.map((fe) => {
                const farm = farms.find((f) => f.farm_id === fe.farm_id)
                return (
                  <tr key={fe.fault_id} className="border-b border-gray-700/50 hover:bg-gray-700/30">
                    <td className="py-2 pr-3 font-mono text-gray-300">{fe.fault_id}</td>
                    <td className="py-2 pr-3 text-gray-400">{fe.fault_date}</td>
                    <td className="py-2 pr-3 text-gray-400 truncate max-w-[100px]">
                      {farm?.farm_name.replace(' Solar Farm', '').replace(' Solar Park', '').replace(' Solar Project', '') ?? fe.farm_id}
                    </td>
                    <td className="py-2 pr-3 text-gray-300">{fe.fault_category}</td>
                    <td className="py-2 pr-3">
                      <span className={`px-2 py-0.5 rounded text-xs ${severityBadge(fe.severity)}`}>
                        {fe.severity}
                      </span>
                    </td>
                    <td className="py-2 pr-3 text-right text-orange-300">{fmt1(fe.capacity_affected_kw)}</td>
                    <td className="py-2 pr-3 text-right text-amber-300">{fmt2(fe.energy_lost_mwh)}</td>
                    <td className="py-2 pr-3 text-right text-blue-300">{fmt1(fe.resolution_time_hours)}</td>
                    <td className="py-2 pr-3 text-gray-400">{fe.detection_method}</td>
                    <td className="py-2 text-right text-red-300">
                      A${fe.revenue_impact_aud.toLocaleString()}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Farm Summary Table */}
      <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
        <h2 className="text-sm font-semibold text-gray-200 mb-3">Farm Fleet Summary</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="text-gray-400 border-b border-gray-700">
                <th className="text-left pb-2 pr-3">Farm</th>
                <th className="text-left pb-2 pr-3">State</th>
                <th className="text-left pb-2 pr-3">Technology</th>
                <th className="text-right pb-2 pr-3">Capacity MWp</th>
                <th className="text-right pb-2 pr-3">PR %</th>
                <th className="text-right pb-2 pr-3">Avail %</th>
                <th className="text-right pb-2 pr-3">Gen GWh/yr</th>
                <th className="text-right pb-2 pr-3">O&M A$M</th>
                <th className="text-left pb-2">Condition</th>
              </tr>
            </thead>
            <tbody>
              {farms.map((f) => (
                <tr key={f.farm_id} className="border-b border-gray-700/50 hover:bg-gray-700/30">
                  <td className="py-2 pr-3 text-gray-200 font-medium">{f.farm_name}</td>
                  <td className="py-2 pr-3 text-gray-400">{f.state}</td>
                  <td className="py-2 pr-3 text-gray-400">{f.technology}</td>
                  <td className="py-2 pr-3 text-right text-blue-300">{f.capacity_mwp.toFixed(0)}</td>
                  <td className="py-2 pr-3 text-right" style={{ color: prColor(f.performance_ratio_pct) }}>
                    {fmtPct(f.performance_ratio_pct)}
                  </td>
                  <td className={`py-2 pr-3 text-right ${f.availability_pct >= 97 ? 'text-green-400' : f.availability_pct >= 95 ? 'text-amber-400' : 'text-red-400'}`}>
                    {fmtPct(f.availability_pct)}
                  </td>
                  <td className="py-2 pr-3 text-right text-gray-300">{fmt2(f.annual_generation_gwh)}</td>
                  <td className="py-2 pr-3 text-right text-gray-300">{f.annual_om_cost_m.toFixed(3)}</td>
                  <td className="py-2">
                    <span className={`px-2 py-0.5 rounded text-xs ${conditionBadge(f.asset_condition)}`}>
                      {f.asset_condition}
                    </span>
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
