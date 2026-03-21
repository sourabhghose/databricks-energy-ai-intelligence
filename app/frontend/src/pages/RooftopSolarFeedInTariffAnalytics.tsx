import { useState, useEffect } from 'react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  LineChart,
  Line,
  Cell,
} from 'recharts'
import { Sun } from 'lucide-react'
import {
  getRooftopSolarFeedInTariffDashboard,
  type RSFTDashboard,
} from '../api/client'

// ── Colour helpers ──────────────────────────────────────────────────────────

const REGION_COLOURS: Record<string, string> = {
  NSW1: '#3b82f6',
  QLD1: '#f59e0b',
  VIC1: '#10b981',
  SA1:  '#8b5cf6',
  TAS1: '#06b6d4',
}

const FIT_TYPE_COLOURS: Record<string, string> = {
  Flat:         '#f59e0b',
  'Time-Varying': '#3b82f6',
  Premium:      '#10b981',
  Transitional: '#6b7280',
}

// ── KPI card ────────────────────────────────────────────────────────────────

function KpiCard({ label, value, sub }: { label: string; value: string | number; sub?: string }) {
  return (
    <div className="bg-gray-800 rounded-lg p-4 flex flex-col gap-1">
      <span className="text-xs text-gray-400 uppercase tracking-wide">{label}</span>
      <span className="text-2xl font-bold text-white">{value}</span>
      {sub && <span className="text-xs text-gray-500">{sub}</span>}
    </div>
  )
}

// ── Main page ───────────────────────────────────────────────────────────────

export default function RooftopSolarFeedInTariffAnalytics() {
  const [data, setData] = useState<RSFTDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getRooftopSolarFeedInTariffDashboard()
      .then(setData)
      .catch((e: unknown) => setError(e instanceof Error ? e.message : 'Failed to load'))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-gray-400">
        <Sun size={28} className="animate-spin mr-3 text-yellow-400" />
        Loading rooftop solar & FiT data…
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="flex items-center justify-center h-64 text-red-400">
        Error: {error ?? 'No data'}
      </div>
    )
  }

  const { regions, growth_trends, fit_policies, export_impact, household_economics, summary } = data

  // ── Chart 1: Installed capacity by region with penetration tooltip ─────
  const regionCapData = regions.map(r => ({
    region: r.region,
    installed_capacity_mw: r.installed_capacity_mw,
    penetration_pct: r.penetration_pct_households,
    avg_fit_rate: r.avg_fit_rate_c_per_kwh,
  }))

  // ── Chart 2: Cumulative capacity monthly growth 2024 across 5 regions ──
  const monthLabels: Record<number, string> = {
    1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
    7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec',
  }
  const growthByMonth: Record<string, Record<string, number>> = {}
  for (const gt of growth_trends.filter(g => g.year === 2024)) {
    const key = monthLabels[gt.month] ?? String(gt.month)
    if (!growthByMonth[key]) growthByMonth[key] = { month: gt.month }
    growthByMonth[key][gt.region] = gt.cumulative_capacity_mw
  }
  const growthChartData = Object.entries(growthByMonth)
    .sort((a, b) => (a[1].month as number) - (b[1].month as number))
    .map(([label, vals]) => ({ label, ...vals }))

  // ── Chart 3: FiT rate by retailer coloured by fit_type ─────────────────
  const fitChartData = [...fit_policies]
    .sort((a, b) => b.fit_rate_c_per_kwh - a.fit_rate_c_per_kwh)
    .map(p => ({
      retailer: p.retailer.length > 16 ? p.retailer.slice(0, 14) + '…' : p.retailer,
      fit_rate: p.fit_rate_c_per_kwh,
      fit_type: p.fit_type,
      state: p.state,
    }))

  // ── Chart 4: Solar export GWh quarterly 2022-2024 by region (grouped) ──
  const exportRegions = ['NSW1', 'QLD1', 'SA1']
  const quarterLabels: Record<number, string> = { 1: 'Q1', 4: 'Q2', 7: 'Q3', 10: 'Q4' }
  type ExportPoint = { key: string } & Record<string, number>
  const exportMap: Record<string, ExportPoint> = {}
  for (const ei of export_impact) {
    const key = `${ei.year} ${quarterLabels[ei.month] ?? `M${ei.month}`}`
    if (!exportMap[key]) exportMap[key] = { key, _year: ei.year, _month: ei.month } as unknown as ExportPoint
    exportMap[key][ei.region] = ei.solar_export_gwh
  }
  const exportChartData = Object.values(exportMap).sort((a, b) => {
    const ay = a._year as number, am = a._month as number
    const by = b._year as number, bm = b._month as number
    return ay !== by ? ay - by : am - bm
  })

  // ── Chart 5: Payback years by state coloured by battery_included ────────
  const paybackData = household_economics.map(h => ({
    scenario_id: h.scenario_id,
    state: h.state,
    payback_years: h.payback_years,
    battery_included: h.battery_included,
    system_size_kw: h.system_size_kw,
  }))

  return (
    <div className="bg-gray-900 min-h-screen p-6 text-white">
      {/* Header */}
      <div className="flex items-center gap-3 mb-6">
        <Sun size={28} className="text-yellow-400" />
        <div>
          <h1 className="text-xl font-bold">Rooftop Solar &amp; Feed-in Tariff Analytics</h1>
          <p className="text-sm text-gray-400">
            Australian rooftop PV penetration, FiT policy benchmarking &amp; household economics
          </p>
        </div>
      </div>

      {/* KPI cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
        <KpiCard
          label="Total Installed"
          value={`${summary.total_installed_capacity_gw} GW`}
          sub="rooftop solar capacity"
        />
        <KpiCard
          label="Total Systems"
          value={summary.total_system_count.toLocaleString()}
          sub="residential & commercial"
        />
        <KpiCard
          label="National Avg FiT"
          value={`${summary.national_avg_fit_rate_c} c/kWh`}
          sub="across NEM regions"
        />
        <KpiCard
          label="Avg Household Savings"
          value={`$${summary.avg_household_savings_aud.toFixed(0)}`}
          sub="annual FiT + bill savings"
        />
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">

        {/* Chart 1: Installed capacity by region */}
        <div className="bg-gray-800 rounded-lg p-4">
          <h2 className="text-sm font-semibold text-gray-300 mb-1">
            Installed Capacity by NEM Region
          </h2>
          <p className="text-xs text-gray-500 mb-4">
            Bar = capacity (MW); tooltip shows household penetration %
          </p>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={regionCapData} margin={{ top: 5, right: 20, left: 0, bottom: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="region" tick={{ fill: '#9ca3af', fontSize: 12 }} />
              <YAxis
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                label={{ value: 'MW', angle: -90, position: 'insideLeft', fill: '#9ca3af', fontSize: 11 }}
              />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                labelStyle={{ color: '#f9fafb' }}
                itemStyle={{ color: '#d1d5db' }}
                formatter={(value: number, name: string, props: { payload?: { penetration_pct?: number; avg_fit_rate?: number } }) => {
                  if (name === 'installed_capacity_mw') {
                    const pen = props.payload?.penetration_pct ?? 0
                    const fit = props.payload?.avg_fit_rate ?? 0
                    return [`${value.toLocaleString()} MW (penetration: ${pen}%, avg FiT: ${fit}c)`, 'Installed Capacity']
                  }
                  return [value, name]
                }}
              />
              <Bar dataKey="installed_capacity_mw" name="Installed Capacity (MW)" radius={[3, 3, 0, 0]}>
                {regionCapData.map((entry, idx) => (
                  <Cell key={idx} fill={REGION_COLOURS[entry.region] ?? '#6b7280'} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
          <div className="flex flex-wrap gap-3 mt-2">
            {Object.entries(REGION_COLOURS).map(([reg, col]) => (
              <span key={reg} className="flex items-center gap-1 text-xs text-gray-400">
                <span className="w-3 h-3 rounded-sm inline-block" style={{ backgroundColor: col }} />
                {reg}
              </span>
            ))}
          </div>
        </div>

        {/* Chart 2: Cumulative capacity monthly growth 2024 */}
        <div className="bg-gray-800 rounded-lg p-4">
          <h2 className="text-sm font-semibold text-gray-300 mb-1">
            Cumulative Capacity Growth — 2024 (Monthly)
          </h2>
          <p className="text-xs text-gray-500 mb-4">
            Cumulative installed capacity (MW) per region across 2024
          </p>
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={growthChartData} margin={{ top: 5, right: 20, left: 0, bottom: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="label" tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <YAxis
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                label={{ value: 'MW', angle: -90, position: 'insideLeft', fill: '#9ca3af', fontSize: 11 }}
              />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                labelStyle={{ color: '#f9fafb' }}
                itemStyle={{ color: '#d1d5db' }}
              />
              <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 12 }} />
              {['NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1'].map(reg => (
                <Line
                  key={reg}
                  type="monotone"
                  dataKey={reg}
                  stroke={REGION_COLOURS[reg]}
                  strokeWidth={2}
                  dot={false}
                  name={reg}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Chart 3: FiT rate by retailer */}
        <div className="bg-gray-800 rounded-lg p-4">
          <h2 className="text-sm font-semibold text-gray-300 mb-1">
            Feed-in Tariff Rate by Retailer
          </h2>
          <p className="text-xs text-gray-500 mb-4">
            FiT rate (c/kWh) by retailer, coloured by tariff type
          </p>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart
              data={fitChartData}
              layout="vertical"
              margin={{ top: 5, right: 20, left: 80, bottom: 5 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                type="number"
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                label={{ value: 'c/kWh', position: 'insideBottom', offset: -2, fill: '#9ca3af', fontSize: 11 }}
              />
              <YAxis
                type="category"
                dataKey="retailer"
                tick={{ fill: '#9ca3af', fontSize: 10 }}
                width={80}
              />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                labelStyle={{ color: '#f9fafb' }}
                itemStyle={{ color: '#d1d5db' }}
                formatter={(value: number, _name: string, props: { payload?: { state?: string; fit_type?: string } }) => [
                  `${value.toFixed(3)} c/kWh (${props.payload?.state ?? ''} — ${props.payload?.fit_type ?? ''})`,
                  'FiT Rate',
                ]}
              />
              <Bar dataKey="fit_rate" name="FiT Rate (c/kWh)" radius={[0, 3, 3, 0]}>
                {fitChartData.map((entry, idx) => (
                  <Cell key={idx} fill={FIT_TYPE_COLOURS[entry.fit_type] ?? '#6b7280'} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
          <div className="flex flex-wrap gap-3 mt-2">
            {Object.entries(FIT_TYPE_COLOURS).map(([type, col]) => (
              <span key={type} className="flex items-center gap-1 text-xs text-gray-400">
                <span className="w-3 h-3 rounded-sm inline-block" style={{ backgroundColor: col }} />
                {type}
              </span>
            ))}
          </div>
        </div>

        {/* Chart 4: Solar export GWh quarterly 2022-2024 by region */}
        <div className="bg-gray-800 rounded-lg p-4">
          <h2 className="text-sm font-semibold text-gray-300 mb-1">
            Solar Export GWh — Quarterly 2022–2024 (Grouped by Region)
          </h2>
          <p className="text-xs text-gray-500 mb-4">
            Quarterly solar export volumes for NSW1, QLD1 &amp; SA1
          </p>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={exportChartData} margin={{ top: 5, right: 20, left: 0, bottom: 40 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                dataKey="key"
                tick={{ fill: '#9ca3af', fontSize: 9 }}
                angle={-45}
                textAnchor="end"
                interval={0}
              />
              <YAxis
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                label={{ value: 'GWh', angle: -90, position: 'insideLeft', fill: '#9ca3af', fontSize: 11 }}
              />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                labelStyle={{ color: '#f9fafb' }}
                itemStyle={{ color: '#d1d5db' }}
              />
              <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 11 }} />
              {exportRegions.map(reg => (
                <Bar
                  key={reg}
                  dataKey={reg}
                  fill={REGION_COLOURS[reg]}
                  name={reg}
                  radius={[2, 2, 0, 0]}
                />
              ))}
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Chart 5: Household payback years by state */}
        <div className="bg-gray-800 rounded-lg p-4 xl:col-span-2">
          <h2 className="text-sm font-semibold text-gray-300 mb-1">
            Household Solar Payback Period by State
          </h2>
          <p className="text-xs text-gray-500 mb-4">
            Payback years per scenario, coloured by battery inclusion (orange = with battery, blue = without)
          </p>
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={paybackData} margin={{ top: 5, right: 20, left: 0, bottom: 30 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                dataKey="scenario_id"
                tick={{ fill: '#9ca3af', fontSize: 9 }}
                angle={-45}
                textAnchor="end"
                interval={0}
              />
              <YAxis
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                label={{ value: 'Years', angle: -90, position: 'insideLeft', fill: '#9ca3af', fontSize: 11 }}
              />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                labelStyle={{ color: '#f9fafb' }}
                itemStyle={{ color: '#d1d5db' }}
                formatter={(value: number, _name: string, props: { payload?: { state?: string; system_size_kw?: number; battery_included?: boolean } }) => [
                  `${value} yrs — ${props.payload?.state ?? ''} ${props.payload?.system_size_kw ?? ''}kW${props.payload?.battery_included ? ' + battery' : ''}`,
                  'Payback Period',
                ]}
              />
              <Bar dataKey="payback_years" name="Payback Years" radius={[3, 3, 0, 0]}>
                {paybackData.map((entry, idx) => (
                  <Cell key={idx} fill={entry.battery_included ? '#f97316' : '#3b82f6'} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
          <div className="flex gap-4 mt-2">
            <span className="flex items-center gap-1 text-xs text-gray-400">
              <span className="w-3 h-3 rounded-sm inline-block bg-blue-500" />
              Without Battery
            </span>
            <span className="flex items-center gap-1 text-xs text-gray-400">
              <span className="w-3 h-3 rounded-sm inline-block bg-orange-500" />
              With Battery
            </span>
          </div>
        </div>

      </div>
    </div>
  )
}
