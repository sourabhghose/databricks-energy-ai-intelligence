// Workforce & Contractor Analytics
import { useEffect, useState } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
  ComposedChart, Area, Line,
} from 'recharts'
import { Users, DollarSign, TrendingDown, Activity, Lightbulb, type LucideIcon } from 'lucide-react'
import { api } from '../api/client'

interface KpiCardProps {
  label: string; value: string; sub?: string
  Icon: LucideIcon; color: string
}
function KpiCard({ label, value, sub, Icon, color }: KpiCardProps) {
  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5 flex items-start gap-4">
      <div className={`p-2.5 rounded-lg ${color}`}><Icon size={20} className="text-white" /></div>
      <div className="min-w-0">
        <p className="text-xs text-gray-500 dark:text-gray-400 mb-0.5">{label}</p>
        <p className="text-2xl font-bold text-gray-900 dark:text-gray-100">{value}</p>
        {sub && <p className="text-xs text-gray-400 dark:text-gray-500 mt-0.5">{sub}</p>}
      </div>
    </div>
  )
}

const FALLBACK_OPEX = [
  { category: 'Field Operations', actual_m: 84.2, aer_allowed_m: 81.4 },
  { category: 'Network Planning', actual_m: 22.6, aer_allowed_m: 24.0 },
  { category: 'Vegetation Mgmt', actual_m: 34.1, aer_allowed_m: 31.8 },
  { category: 'Customer Service', actual_m: 18.8, aer_allowed_m: 19.2 },
  { category: 'IT/OT Systems', actual_m: 28.6, aer_allowed_m: 24.2 },
  { category: 'Corporate Overhead', actual_m: 41.4, aer_allowed_m: 43.8 },
  { category: 'Safety & Environment', actual_m: 12.2, aer_allowed_m: 11.8 },
  { category: 'Emergency Response', actual_m: 18.5, aer_allowed_m: 16.0 },
]

export default function WorkforceAnalyticsHub() {
  const [summary, setSummary] = useState<Record<string, any>>({})
  const [opex, setOpex] = useState<any[]>([])
  const [forecastData, setForecastData] = useState<Record<string, any> | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    Promise.all([
      api.getWorkforceSummary(),
      api.getWorkforceOpexBenchmark(),
      api.getWorkforceForecast(),
    ]).then(([s, o, f]) => {
      setSummary(s ?? {})
      setOpex(Array.isArray(o?.items) ? o.items : Array.isArray(o) ? o : FALLBACK_OPEX)
      setForecastData(f ?? null)
      setLoading(false)
    }).catch(() => {
      setOpex(FALLBACK_OPEX)
      setLoading(false)
    })
  }, [])

  if (loading) return <div className="p-8 text-gray-500 dark:text-gray-400">Loading...</div>

  const totalFte = summary.total_workforce_fte ?? 1842
  const contractorRatio = summary.contractor_ratio_pct ?? 34.2
  const costPerCustomer = summary.cost_per_customer ?? 412
  const actualOpex = opex.reduce((a, r) => a + (r.actual_m ?? 0), 0)
  const allowedOpex = opex.reduce((a, r) => a + (r.aer_allowed_m ?? 0), 0)
  const opexGap = actualOpex - allowedOpex

  // Biggest gap category
  const gaps = opex.map(r => ({ category: r.category, gap: (r.actual_m ?? 0) - (r.aer_allowed_m ?? 0) }))
  const biggestGap = gaps.sort((a, b) => Math.abs(b.gap) - Math.abs(a.gap))[0]

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900 dark:text-gray-100">Workforce & Contractor Analytics</h1>
          <p className="text-sm text-gray-500 dark:text-gray-400">Total workforce, contractor ratio, opex benchmarking and AER efficiency gap analysis</p>
        </div>
        <span className="text-xs px-2 py-1 rounded-full bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-400">Synthetic</span>
      </div>

      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <KpiCard label="Total Workforce" value={`${totalFte.toLocaleString()} FTE`} sub="Employee + equivalent contractor" Icon={Users} color="bg-blue-500" />
        <KpiCard label="Contractor Ratio" value={`${contractorRatio.toFixed(1)}%`} sub="Contracted labour proportion" Icon={Activity} color={contractorRatio > 40 ? 'bg-orange-500' : 'bg-green-500'} />
        <KpiCard label="Cost / Customer" value={`$${costPerCustomer.toLocaleString()}`} sub="Total opex per customer per year" Icon={DollarSign} color="bg-purple-500" />
        <KpiCard label="Opex vs AER Allowed" value={`${opexGap >= 0 ? '+' : ''}$${opexGap.toFixed(1)}M`} sub={opexGap > 0 ? 'Over AER allowance' : 'Under AER allowance'} Icon={TrendingDown} color={Math.abs(opexGap) <= 5 ? 'bg-green-500' : opexGap > 0 ? 'bg-red-500' : 'bg-green-500'} />
      </div>

      {/* Key efficiency gap alert */}
      {biggestGap && Math.abs(biggestGap.gap) > 2 && (
        <div className={`rounded-xl border p-4 ${biggestGap.gap > 0 ? 'bg-red-50 dark:bg-red-900/10 border-red-100 dark:border-red-800/40' : 'bg-green-50 dark:bg-green-900/10 border-green-100 dark:border-green-800/40'}`}>
          <p className="text-sm font-semibold text-gray-800 dark:text-gray-100">
            Key Efficiency Gap: <span className="font-bold">{biggestGap.category}</span>
          </p>
          <p className={`text-xs mt-1 ${biggestGap.gap > 0 ? 'text-red-700 dark:text-red-400' : 'text-green-700 dark:text-green-400'}`}>
            {biggestGap.gap > 0 ? '↑ Over' : '↓ Under'} AER allowance by ${Math.abs(biggestGap.gap).toFixed(1)}M — {biggestGap.gap > 0 ? 'AER efficiency question risk at reset' : 'potential capex to opex reclassification needed'}
          </p>
        </div>
      )}

      <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5">
        <h2 className="text-sm font-semibold text-gray-800 dark:text-gray-100 mb-4">Opex by Category — Actual vs AER Allowed ($M)</h2>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={opex} margin={{ bottom: 40, left: 10 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.3} />
            <XAxis dataKey="category" tick={{ fontSize: 10, fill: '#9CA3AF' }} angle={-20} textAnchor="end" interval={0} />
            <YAxis tick={{ fontSize: 11, fill: '#9CA3AF' }} unit=" M" />
            <Tooltip
              contentStyle={{ backgroundColor: '#1F2937', border: '1px solid #374151', borderRadius: 8 }}
              labelStyle={{ color: '#F9FAFB' }}
              itemStyle={{ color: '#D1D5DB' }}
            />
            <Legend wrapperStyle={{ fontSize: 11, color: '#9CA3AF' }} />
            <Bar dataKey="actual_m" fill="#3B82F6" name="Actual ($M)" radius={[3, 3, 0, 0]} />
            <Bar dataKey="aer_allowed_m" fill="#6B7280" name="AER Allowed ($M)" radius={[3, 3, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>

      <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5">
        <h2 className="text-sm font-semibold text-gray-800 dark:text-gray-100 mb-4">Opex Category Detail</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-100 dark:border-gray-700">
                <th className="text-left text-xs font-medium text-gray-500 dark:text-gray-400 pb-2 pr-4">Category</th>
                <th className="text-left text-xs font-medium text-gray-500 dark:text-gray-400 pb-2 pr-4">Actual $M</th>
                <th className="text-left text-xs font-medium text-gray-500 dark:text-gray-400 pb-2 pr-4">AER Allowed $M</th>
                <th className="text-left text-xs font-medium text-gray-500 dark:text-gray-400 pb-2 pr-4">Variance $M</th>
                <th className="text-left text-xs font-medium text-gray-500 dark:text-gray-400 pb-2">Efficiency</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50 dark:divide-gray-700/50">
              {opex.map((row, i) => {
                const variance = (row.actual_m ?? 0) - (row.aer_allowed_m ?? 0)
                const effPct = row.aer_allowed_m > 0 ? ((row.aer_allowed_m - row.actual_m) / row.aer_allowed_m) * 100 : 0
                return (
                  <tr key={i} className="hover:bg-gray-50 dark:hover:bg-gray-700/30">
                    <td className="py-2.5 pr-4 text-gray-900 dark:text-gray-100 font-medium">{row.category}</td>
                    <td className="py-2.5 pr-4 text-gray-900 dark:text-gray-100">{(row.actual_m ?? 0).toFixed(1)}</td>
                    <td className="py-2.5 pr-4 text-gray-500 dark:text-gray-400">{(row.aer_allowed_m ?? 0).toFixed(1)}</td>
                    <td className="py-2.5 pr-4">
                      <span className={`font-semibold ${variance > 0 ? 'text-red-600 dark:text-red-400' : 'text-green-600 dark:text-green-400'}`}>
                        {variance > 0 ? '+' : ''}{variance.toFixed(1)}
                      </span>
                    </td>
                    <td className="py-2.5">
                      <span className={`text-xs font-semibold ${effPct >= 0 ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'}`}>
                        {effPct >= 0 ? '+' : ''}{effPct.toFixed(1)}%
                      </span>
                    </td>
                  </tr>
                )
              })}
              <tr className="border-t-2 border-gray-200 dark:border-gray-600 font-semibold">
                <td className="py-2.5 pr-4 text-gray-900 dark:text-gray-100">Total</td>
                <td className="py-2.5 pr-4 text-gray-900 dark:text-gray-100">{actualOpex.toFixed(1)}</td>
                <td className="py-2.5 pr-4 text-gray-500 dark:text-gray-400">{allowedOpex.toFixed(1)}</td>
                <td className="py-2.5 pr-4">
                  <span className={`font-bold ${opexGap > 0 ? 'text-red-600 dark:text-red-400' : 'text-green-600 dark:text-green-400'}`}>
                    {opexGap > 0 ? '+' : ''}{opexGap.toFixed(1)}
                  </span>
                </td>
                <td className="py-2.5 text-gray-400 dark:text-gray-500">—</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      {/* ── ML Demand Forecasting Section ────────────────────────── */}
      {forecastData && (
        <>
          <div className="flex items-center gap-3 pt-2">
            <h2 className="text-base font-bold text-gray-900 dark:text-gray-100">ML Demand Forecasting</h2>
            <span className="text-xs px-2 py-0.5 rounded-full bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400">Prophet + XGBoost</span>
          </div>

          {/* Model card */}
          <div className="bg-gray-900 dark:bg-gray-950 rounded-xl border border-gray-700 p-5">
            <div className="flex items-start justify-between mb-3">
              <div>
                <p className="text-xs text-gray-400 mb-0.5">Model</p>
                <p className="text-sm font-semibold text-white">{forecastData.model_metadata?.model_name} — {forecastData.model_metadata?.algorithm}</p>
              </div>
              <span className="text-xs px-2 py-1 rounded bg-orange-500/20 text-orange-400 border border-orange-500/30 font-medium">
                Powered by Databricks MLflow
              </span>
            </div>
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 mt-3">
              <div>
                <p className="text-xs text-gray-500">MLflow Run ID</p>
                <p className="text-xs font-mono text-gray-300 mt-0.5">{String(forecastData.model_metadata?.mlflow_run_id ?? '').slice(0, 12)}…</p>
              </div>
              <div>
                <p className="text-xs text-gray-500">MAE (hours)</p>
                <p className="text-sm font-bold text-green-400">{forecastData.model_metadata?.mae_hours}</p>
              </div>
              <div>
                <p className="text-xs text-gray-500">MAPE</p>
                <p className="text-sm font-bold text-blue-400">{forecastData.model_metadata?.mape_pct}%</p>
              </div>
              <div>
                <p className="text-xs text-gray-500">Training Date</p>
                <p className="text-xs text-gray-300 mt-0.5">{forecastData.model_metadata?.training_date}</p>
              </div>
            </div>
          </div>

          {/* Forecast area chart */}
          <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5">
            <h3 className="text-sm font-semibold text-gray-800 dark:text-gray-100 mb-1">Field Crew Demand — Historical & Forecast (Hours/Month)</h3>
            <p className="text-xs text-gray-500 dark:text-gray-400 mb-4">Shaded band = 80% prediction interval · Dashed = forecast months</p>
            <ResponsiveContainer width="100%" height={300}>
              <ComposedChart data={forecastData.forecast ?? []} margin={{ left: 10, right: 10, bottom: 20 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.3} />
                <XAxis dataKey="month" tick={{ fontSize: 10, fill: '#9CA3AF' }} angle={-30} textAnchor="end" interval={1} />
                <YAxis tick={{ fontSize: 11, fill: '#9CA3AF' }} tickFormatter={(v) => `${(v / 1000).toFixed(0)}k`} />
                <Tooltip
                  contentStyle={{ backgroundColor: '#1F2937', border: '1px solid #374151', borderRadius: 8 }}
                  labelStyle={{ color: '#F9FAFB' }}
                  itemStyle={{ color: '#D1D5DB' }}
                  formatter={(v: number, name: string) => [v ? v.toLocaleString() : '—', name]}
                />
                <Legend wrapperStyle={{ fontSize: 11, color: '#9CA3AF' }} />
                {/* Prediction interval band */}
                <Area
                  type="monotone"
                  dataKey="upper_bound"
                  fill="#3B82F6"
                  fillOpacity={0.08}
                  stroke="none"
                  name="Upper Bound"
                  legendType="none"
                />
                <Area
                  type="monotone"
                  dataKey="lower_bound"
                  fill="#fff"
                  fillOpacity={1}
                  stroke="none"
                  name="Lower Bound"
                  legendType="none"
                />
                {/* Actual hours (historical) */}
                <Line
                  type="monotone"
                  dataKey="actual_hours"
                  stroke="#3B82F6"
                  strokeWidth={2}
                  dot={{ r: 3, fill: '#3B82F6' }}
                  name="Actual Hours"
                  connectNulls={false}
                />
                {/* Forecast line */}
                <Line
                  type="monotone"
                  dataKey="forecast_hours"
                  stroke="#F97316"
                  strokeWidth={2}
                  strokeDasharray="5 3"
                  dot={{ r: 3, fill: '#F97316' }}
                  name="Forecast Hours"
                />
              </ComposedChart>
            </ResponsiveContainer>
          </div>

          {/* Skills gap table */}
          <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5">
            <h3 className="text-sm font-semibold text-gray-800 dark:text-gray-100 mb-4">Skills Demand Forecast — 6-Month Gap Analysis</h3>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-100 dark:border-gray-700">
                    <th className="text-left text-xs font-medium text-gray-500 dark:text-gray-400 pb-2 pr-4">Skill</th>
                    <th className="text-left text-xs font-medium text-gray-500 dark:text-gray-400 pb-2 pr-4">Current FTE</th>
                    <th className="text-left text-xs font-medium text-gray-500 dark:text-gray-400 pb-2 pr-4">Forecast FTE (6m)</th>
                    <th className="text-left text-xs font-medium text-gray-500 dark:text-gray-400 pb-2 pr-4">Gap</th>
                    <th className="text-left text-xs font-medium text-gray-500 dark:text-gray-400 pb-2">Risk</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-50 dark:divide-gray-700/50">
                  {(forecastData.skill_demand_forecast ?? []).map((row: any, i: number) => {
                    const gapColor = row.gap > 0
                      ? 'text-red-600 dark:text-red-400'
                      : 'text-green-600 dark:text-green-400'
                    const riskBadge = row.risk === 'High'
                      ? 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400'
                      : row.risk === 'Medium'
                      ? 'bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-400'
                      : 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
                    return (
                      <tr key={i} className="hover:bg-gray-50 dark:hover:bg-gray-700/30">
                        <td className="py-2.5 pr-4 font-medium text-gray-900 dark:text-gray-100">{row.skill}</td>
                        <td className="py-2.5 pr-4 text-gray-900 dark:text-gray-100">{row.current_fte}</td>
                        <td className="py-2.5 pr-4 text-gray-900 dark:text-gray-100">{row.forecast_fte_6m}</td>
                        <td className="py-2.5 pr-4">
                          <span className={`font-bold ${gapColor}`}>{row.gap > 0 ? '+' : ''}{row.gap}</span>
                        </td>
                        <td className="py-2.5">
                          <span className={`px-2 py-0.5 text-xs rounded-full font-semibold ${riskBadge}`}>{row.risk}</span>
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          </div>

          {/* AI insights */}
          <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5">
            <h3 className="text-sm font-semibold text-gray-800 dark:text-gray-100 mb-3">Model Insights</h3>
            <ul className="space-y-2">
              {(forecastData.insights ?? []).map((insight: string, i: number) => (
                <li key={i} className="flex items-start gap-2.5">
                  <Lightbulb size={15} className="text-amber-500 flex-shrink-0 mt-0.5" />
                  <span className="text-sm text-gray-700 dark:text-gray-300">{insight}</span>
                </li>
              ))}
            </ul>
          </div>
        </>
      )}
    </div>
  )
}
