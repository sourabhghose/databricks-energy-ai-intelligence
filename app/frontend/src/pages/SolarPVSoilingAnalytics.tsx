import { useEffect, useState } from 'react'
import { Sun } from 'lucide-react'
import {
  LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer,
} from 'recharts'
import {
  getSolarPVSoilingDashboard,
  SPSLDashboard,
  SPSLFarmRecord,
  SPSLSoilingRecord,
  SPSLCleaningRecord,
  SPSLDegradationRecord,
  SPSLWeatherImpactRecord,
  SPSLOptimisationRecord,
} from '../api/client'

// ---------------------------------------------------------------------------
// KPI Card
// ---------------------------------------------------------------------------
function KpiCard({ label, value, unit, sub }: { label: string; value: string; unit?: string; sub?: string }) {
  return (
    <div className="bg-gray-800 rounded-lg p-4 flex flex-col gap-1">
      <p className="text-xs text-gray-400 uppercase tracking-wide">{label}</p>
      <p className="text-2xl font-bold text-white">
        {value}
        {unit && <span className="text-sm text-gray-400 ml-1">{unit}</span>}
      </p>
      {sub && <p className="text-xs text-gray-500">{sub}</p>}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------
export default function SolarPVSoilingAnalytics() {
  const [data, setData] = useState<SPSLDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getSolarPVSoilingDashboard()
      .then(setData)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full bg-gray-900">
        <div className="text-gray-400 text-sm animate-pulse">Loading Solar PV Soiling & Performance Analytics...</div>
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="flex items-center justify-center h-full bg-gray-900">
        <div className="text-red-400 text-sm">Error: {error ?? 'No data returned'}</div>
      </div>
    )
  }

  const { farms, soiling_records, cleaning_records, degradation, weather_impacts, optimisations, summary } = data

  // ---- Chart Data ----

  // Soiling ratio over time — group by farm, sort by days_since_last_clean
  const farmIds = [...new Set(soiling_records.map((r: SPSLSoilingRecord) => r.farm_id))].slice(0, 4)
  const soilingChartData: Record<string, number | string>[] = []
  const maxDays = Math.max(...soiling_records.map((r: SPSLSoilingRecord) => r.days_since_last_clean))
  const dayBuckets = [10, 20, 30, 40, 50, 60, 70, 80]
  dayBuckets.forEach((d) => {
    const row: Record<string, number | string> = { days: d }
    farmIds.forEach((fid) => {
      const recs = soiling_records
        .filter((r: SPSLSoilingRecord) => r.farm_id === fid && Math.abs(r.days_since_last_clean - d) <= 8)
      if (recs.length > 0) {
        const avg = recs.reduce((s: number, r: SPSLSoilingRecord) => s + r.soiling_ratio, 0) / recs.length
        const farm = farms.find((f: SPSLFarmRecord) => f.farm_id === fid)
        row[(farm?.farm_name?.split(' ')[0] ?? fid) + ' ' + fid.slice(-2)] = parseFloat(avg.toFixed(4))
      }
    })
    soilingChartData.push(row)
  })

  // Optimisation bar chart — net energy gain vs cleaning cost (first 4 scenarios across all farms, grouped by scenario)
  const scenarioNames = ['Current', 'Cleaning 4x pa', 'Cleaning 12x pa', 'Robotic Daily']
  const optChartData = scenarioNames.map((sc) => {
    const recs = optimisations.filter((o: SPSLOptimisationRecord) => o.scenario_name === sc)
    const avgGain = recs.length ? recs.reduce((s: number, o: SPSLOptimisationRecord) => s + o.net_energy_gain_mwh, 0) / recs.length : 0
    const avgCost = recs.length ? recs.reduce((s: number, o: SPSLOptimisationRecord) => s + o.cleaning_cost_pa_aud, 0) / recs.length : 0
    return {
      scenario: sc,
      'Net Energy Gain (MWh)': parseFloat(avgGain.toFixed(1)),
      'Cleaning Cost (k AUD)': parseFloat((avgCost / 1000).toFixed(1)),
    }
  })

  // Weather impact — monthly soiling index and rainfall (aggregate across farms)
  const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun']
  const weatherChartData = monthNames.map((mon) => {
    const recs = weather_impacts.filter((w: SPSLWeatherImpactRecord) => w.month === mon)
    const avgSoiling = recs.length ? recs.reduce((s: number, w: SPSLWeatherImpactRecord) => s + w.soiling_index, 0) / recs.length : 0
    const avgRain = recs.length ? recs.reduce((s: number, w: SPSLWeatherImpactRecord) => s + w.rainfall_mm, 0) / recs.length : 0
    return {
      month: mon,
      'Soiling Index': parseFloat(avgSoiling.toFixed(3)),
      'Rainfall (mm)': parseFloat(avgRain.toFixed(1)),
    }
  })

  // Degradation breakdown stacked bar by year (aggregate across farms)
  const years = [2022, 2023, 2024]
  const degradChartData = years.map((yr) => {
    const recs = degradation.filter((d: SPSLDegradationRecord) => d.year === yr)
    const avg = (key: keyof SPSLDegradationRecord) =>
      recs.length ? parseFloat((recs.reduce((s, r) => s + (r[key] as number), 0) / recs.length).toFixed(2)) : 0
    return {
      year: yr.toString(),
      'LID Loss': avg('lid_loss_pct'),
      'Thermal Loss': avg('thermal_loss_pct'),
      'Soiling Loss': avg('soiling_annual_loss_pct'),
      'Mechanical Loss': avg('mechanical_loss_pct'),
    }
  })

  const COLORS = ['#f59e0b', '#3b82f6', '#10b981', '#ef4444', '#8b5cf6', '#ec4899']

  return (
    <div className="min-h-full bg-gray-900 text-gray-100 p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center gap-3">
        <div className="p-2 bg-amber-500/20 rounded-lg">
          <Sun className="text-amber-400" size={24} />
        </div>
        <div>
          <h1 className="text-xl font-bold text-white">Solar PV Soiling &amp; Performance Analytics</h1>
          <p className="text-xs text-gray-400">Soiling loss, cleaning optimisation, degradation &amp; weather impacts across Australian solar farms</p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <KpiCard
          label="Total Solar Capacity"
          value={(summary.total_solar_capacity_mwp as number).toLocaleString()}
          unit="MWp"
          sub={`${farms.length} farms tracked`}
        />
        <KpiCard
          label="Avg Soiling Loss"
          value={(summary.avg_soiling_loss_pct as number).toFixed(1)}
          unit="%"
          sub="Annual generation loss"
        />
        <KpiCard
          label="Annual Energy Loss"
          value={(summary.total_annual_energy_loss_gwh as number).toFixed(1)}
          unit="GWh"
          sub="From soiling across fleet"
        />
        <KpiCard
          label="Avg Cleaning ROI"
          value={(summary.avg_cleaning_roi_days as number).toString()}
          unit="days"
          sub="Payback period"
        />
      </div>

      {/* Row 1: Soiling Ratio + Optimisation */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="bg-gray-800 rounded-lg p-4">
          <h2 className="text-sm font-semibold text-gray-200 mb-3">Soiling Ratio vs Days Since Last Clean</h2>
          <ResponsiveContainer width="100%" height={250}>
            <LineChart data={soilingChartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="days" tick={{ fill: '#9ca3af', fontSize: 11 }} label={{ value: 'Days Since Clean', fill: '#9ca3af', fontSize: 11, position: 'insideBottom', offset: -2 }} />
              <YAxis domain={[0.9, 1.01]} tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: 'none', fontSize: 12 }} />
              <Legend wrapperStyle={{ fontSize: 11 }} />
              {Object.keys(soilingChartData[0] || {}).filter((k) => k !== 'days').map((key, i) => (
                <Line key={key} type="monotone" dataKey={key} stroke={COLORS[i % COLORS.length]} dot={false} strokeWidth={2} />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>

        <div className="bg-gray-800 rounded-lg p-4">
          <h2 className="text-sm font-semibold text-gray-200 mb-3">Optimisation Scenarios: Net Energy Gain vs Cleaning Cost</h2>
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={optChartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="scenario" tick={{ fill: '#9ca3af', fontSize: 10 }} />
              <YAxis yAxisId="left" tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <YAxis yAxisId="right" orientation="right" tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: 'none', fontSize: 12 }} />
              <Legend wrapperStyle={{ fontSize: 11 }} />
              <Bar yAxisId="left" dataKey="Net Energy Gain (MWh)" fill="#f59e0b" radius={[3, 3, 0, 0]} />
              <Bar yAxisId="right" dataKey="Cleaning Cost (k AUD)" fill="#3b82f6" radius={[3, 3, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Row 2: Weather Impact + Degradation */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="bg-gray-800 rounded-lg p-4">
          <h2 className="text-sm font-semibold text-gray-200 mb-3">Monthly Weather Impact: Soiling Index &amp; Rainfall</h2>
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={weatherChartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="month" tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <YAxis yAxisId="left" domain={[0.9, 1.0]} tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <YAxis yAxisId="right" orientation="right" tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: 'none', fontSize: 12 }} />
              <Legend wrapperStyle={{ fontSize: 11 }} />
              <Bar yAxisId="left" dataKey="Soiling Index" fill="#10b981" radius={[3, 3, 0, 0]} />
              <Bar yAxisId="right" dataKey="Rainfall (mm)" fill="#3b82f6" radius={[3, 3, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div className="bg-gray-800 rounded-lg p-4">
          <h2 className="text-sm font-semibold text-gray-200 mb-3">Degradation Breakdown by Loss Type (Stacked)</h2>
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={degradChartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="year" tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: 'none', fontSize: 12 }} />
              <Legend wrapperStyle={{ fontSize: 11 }} />
              <Bar dataKey="LID Loss" stackId="a" fill="#f59e0b" />
              <Bar dataKey="Thermal Loss" stackId="a" fill="#ef4444" />
              <Bar dataKey="Soiling Loss" stackId="a" fill="#8b5cf6" />
              <Bar dataKey="Mechanical Loss" stackId="a" fill="#6b7280" radius={[3, 3, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Farm Performance Table */}
      <div className="bg-gray-800 rounded-lg p-4">
        <h2 className="text-sm font-semibold text-gray-200 mb-3">Farm Performance Summary</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="text-gray-400 border-b border-gray-700">
                <th className="text-left py-2 pr-3">Farm</th>
                <th className="text-left py-2 pr-3">State</th>
                <th className="text-left py-2 pr-3">Technology</th>
                <th className="text-right py-2 pr-3">Capacity (MWp)</th>
                <th className="text-right py-2 pr-3">PR (%)</th>
                <th className="text-right py-2 pr-3">Soiling Loss (%)</th>
                <th className="text-right py-2 pr-3">Annual Gen (GWh)</th>
                <th className="text-right py-2">Cleans/yr</th>
              </tr>
            </thead>
            <tbody>
              {farms.map((f: SPSLFarmRecord) => (
                <tr key={f.farm_id} className="border-b border-gray-700/50 hover:bg-gray-700/30">
                  <td className="py-2 pr-3 text-amber-300 font-medium">{f.farm_name}</td>
                  <td className="py-2 pr-3 text-gray-300">{f.state}</td>
                  <td className="py-2 pr-3 text-gray-300">{f.technology}</td>
                  <td className="py-2 pr-3 text-right text-gray-200">{f.capacity_mwp.toLocaleString()}</td>
                  <td className="py-2 pr-3 text-right">
                    <span className={f.avg_performance_ratio_pct >= 82 ? 'text-green-400' : f.avg_performance_ratio_pct >= 78 ? 'text-yellow-400' : 'text-red-400'}>
                      {f.avg_performance_ratio_pct.toFixed(1)}
                    </span>
                  </td>
                  <td className="py-2 pr-3 text-right">
                    <span className={f.soiling_loss_annual_pct <= 2.5 ? 'text-green-400' : f.soiling_loss_annual_pct <= 3.5 ? 'text-yellow-400' : 'text-red-400'}>
                      {f.soiling_loss_annual_pct.toFixed(1)}
                    </span>
                  </td>
                  <td className="py-2 pr-3 text-right text-gray-200">{f.annual_generation_gwh.toFixed(1)}</td>
                  <td className="py-2 text-right text-gray-200">{f.cleaning_frequency_per_year}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Cleaning Records Table */}
      <div className="bg-gray-800 rounded-lg p-4">
        <h2 className="text-sm font-semibold text-gray-200 mb-3">Recent Cleaning Records</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="text-gray-400 border-b border-gray-700">
                <th className="text-left py-2 pr-3">ID</th>
                <th className="text-left py-2 pr-3">Farm</th>
                <th className="text-left py-2 pr-3">Date</th>
                <th className="text-left py-2 pr-3">Method</th>
                <th className="text-right py-2 pr-3">Panels</th>
                <th className="text-right py-2 pr-3">Cost (AUD)</th>
                <th className="text-right py-2 pr-3">Energy Recovery (kWh)</th>
                <th className="text-right py-2 pr-3">Pre Ratio</th>
                <th className="text-right py-2 pr-3">Post Ratio</th>
                <th className="text-right py-2">ROI (days)</th>
              </tr>
            </thead>
            <tbody>
              {cleaning_records.slice(0, 15).map((c: SPSLCleaningRecord) => {
                const farm = farms.find((f: SPSLFarmRecord) => f.farm_id === c.farm_id)
                return (
                  <tr key={c.cleaning_id} className="border-b border-gray-700/50 hover:bg-gray-700/30">
                    <td className="py-2 pr-3 text-gray-500">{c.cleaning_id}</td>
                    <td className="py-2 pr-3 text-blue-300">{farm?.farm_name.split(' ').slice(0, 2).join(' ') ?? c.farm_id}</td>
                    <td className="py-2 pr-3 text-gray-300">{c.cleaning_date}</td>
                    <td className="py-2 pr-3 text-gray-300">{c.cleaning_method}</td>
                    <td className="py-2 pr-3 text-right text-gray-200">{c.panels_cleaned_count.toLocaleString()}</td>
                    <td className="py-2 pr-3 text-right text-gray-200">${c.cost_aud.toLocaleString(undefined, { maximumFractionDigits: 0 })}</td>
                    <td className="py-2 pr-3 text-right text-green-400">{c.energy_recovery_kwh.toFixed(1)}</td>
                    <td className="py-2 pr-3 text-right text-red-400">{c.pre_clean_soiling_ratio.toFixed(3)}</td>
                    <td className="py-2 pr-3 text-right text-green-400">{c.post_clean_soiling_ratio.toFixed(3)}</td>
                    <td className="py-2 text-right">
                      <span className={c.roi_days <= 30 ? 'text-green-400' : c.roi_days <= 90 ? 'text-yellow-400' : 'text-red-400'}>
                        {c.roi_days}
                      </span>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
