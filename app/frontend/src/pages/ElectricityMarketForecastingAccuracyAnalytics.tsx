import { useEffect, useState } from 'react'
import { Target } from 'lucide-react'
import {
  BarChart,
  Bar,
  LineChart,
  Line,
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ReferenceLine,
  Cell,
} from 'recharts'
import {
  getElectricityMarketForecastingAccuracyDashboard,
  EMFADashboard,
  EMFAPriceRecord,
  EMFADemandRecord,
  EMFARenewableRecord,
  EMFAModelRecord,
  EMFAImprovementRecord,
  EMFAEventRecord,
} from '../api/client'

const REGION_COLORS: Record<string, string> = {
  NSW: '#6366f1',
  VIC: '#22d3ee',
  QLD: '#f59e0b',
  SA:  '#34d399',
  TAS: '#f87171',
}

const HORIZON_COLORS: Record<string, string> = {
  '5min':       '#22d3ee',
  '30min':      '#6366f1',
  '4h':         '#f59e0b',
  'Day-ahead':  '#34d399',
  'Week-ahead': '#f87171',
}

const FORECAST_TYPE_COLORS: Record<string, string> = {
  Price:     '#6366f1',
  Demand:    '#22d3ee',
  Renewable: '#34d399',
}

export default function ElectricityMarketForecastingAccuracyAnalytics() {
  const [data, setData] = useState<EMFADashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getElectricityMarketForecastingAccuracyDashboard()
      .then(setData)
      .catch((e) => setError(e.message ?? 'Failed to load data'))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 bg-gray-900 text-gray-300">
        <span className="text-lg">Loading Electricity Market Forecasting Accuracy Analytics...</span>
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="flex items-center justify-center h-64 bg-gray-900 text-red-400">
        <span className="text-lg">Error: {error ?? 'No data available'}</span>
      </div>
    )
  }

  const {
    summary,
    price_forecasts,
    demand_forecasts,
    renewable_forecasts,
    models,
    improvement_trend,
    event_predictions,
  } = data

  // ---- KPIs ----
  const kpis = [
    {
      label: 'Avg Price MAPE',
      value: typeof summary.avg_price_mape_pct === 'number' ? summary.avg_price_mape_pct.toFixed(2) : '—',
      unit: '%',
      color: 'text-indigo-400',
    },
    {
      label: 'Avg Demand MAPE',
      value: typeof summary.avg_demand_mape_pct === 'number' ? summary.avg_demand_mape_pct.toFixed(2) : '—',
      unit: '%',
      color: 'text-cyan-400',
    },
    {
      label: 'Spike Recall (1h, SA1 only had test-window spikes)',
      value: typeof summary.spike_prediction_accuracy_pct === 'number' ? summary.spike_prediction_accuracy_pct.toFixed(1) : '—',
      unit: '%',
      color: 'text-amber-400',
    },
    {
      label: 'YoY Improvement',
      value: typeof summary.yoy_improvement_pct === 'number' ? summary.yoy_improvement_pct.toFixed(2) : '—',
      unit: '%',
      color: 'text-emerald-400',
    },
  ]

  // ---- Chart 1: Forecast Error by Horizon — grouped BarChart, avg absolute_error_mwh by horizon × model_type ----
  const horizonModelMap: Record<string, Record<string, number[]>> = {}
  price_forecasts.forEach((p: EMFAPriceRecord) => {
    if (!horizonModelMap[p.horizon_type]) horizonModelMap[p.horizon_type] = {}
    if (!horizonModelMap[p.horizon_type][p.model_type]) horizonModelMap[p.horizon_type][p.model_type] = []
    horizonModelMap[p.horizon_type][p.model_type].push(p.absolute_error_mwh)
  })
  const horizonOrder = ['5min', '30min', '4h', 'Day-ahead', 'Week-ahead']
  const allModelTypes = [...new Set(price_forecasts.map((p) => p.model_type))]
  const avg = (arr: number[]) => arr.reduce((a, b) => a + b, 0) / arr.length
  const horizonChartData = horizonOrder
    .filter((h) => horizonModelMap[h])
    .map((h) => {
      const row: Record<string, string | number> = { horizon: h }
      allModelTypes.forEach((mt) => {
        const vals = horizonModelMap[h]?.[mt]
        row[mt] = vals && vals.length > 0 ? +avg(vals).toFixed(2) : 0
      })
      return row
    })
  const modelTypeBarColors = ['#6366f1', '#22d3ee', '#f59e0b', '#34d399', '#f87171', '#a78bfa']

  // ---- Chart 2: Price Forecast Accuracy — ScatterChart, forecast vs actual coloured by region ----
  const scatterByRegion: Record<string, { forecast: number; actual: number }[]> = {}
  price_forecasts.forEach((p: EMFAPriceRecord) => {
    if (!scatterByRegion[p.region]) scatterByRegion[p.region] = []
    scatterByRegion[p.region].push({ forecast: p.forecast_price_mwh, actual: p.actual_price_mwh })
  })
  const allPrices = price_forecasts.flatMap((p) => [p.forecast_price_mwh, p.actual_price_mwh])
  const priceMin = Math.max(0, Math.floor(Math.min(...allPrices) - 10))
  const priceMax = Math.ceil(Math.max(...allPrices) + 10)

  // ---- Chart 3: Renewable Forecast Accuracy — LineChart, solar_mae_mw and wind_mae_mw quarterly ----
  const quarterMap: Record<string, { solar_mae: number[]; wind_mae: number[] }> = {}
  renewable_forecasts.forEach((r: EMFARenewableRecord) => {
    if (!quarterMap[r.quarter]) quarterMap[r.quarter] = { solar_mae: [], wind_mae: [] }
    quarterMap[r.quarter].solar_mae.push(r.solar_mae_mw)
    quarterMap[r.quarter].wind_mae.push(r.wind_mae_mw)
  })
  const renewableChartData = Object.entries(quarterMap)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([quarter, v]) => ({
      quarter,
      'Solar MAE (MW)': +avg(v.solar_mae).toFixed(1),
      'Wind MAE (MW)': +avg(v.wind_mae).toFixed(1),
    }))

  // ---- Chart 4: Model Comparison Leaderboard — horizontal BarChart, mape_pct sorted ascending ----
  const leaderboardData = [...models]
    .sort((a: EMFAModelRecord, b: EMFAModelRecord) => a.mape_pct - b.mape_pct)
    .map((m: EMFAModelRecord) => ({
      name: m.model_name.length > 22 ? m.model_name.slice(0, 21) + '…' : m.model_name,
      fullName: m.model_name,
      mape_pct: m.mape_pct,
      model_type: m.model_type,
      is_production: m.is_production,
    }))

  // ---- Chart 5: Improvement Over Time — LineChart, mape_pct by year × forecast_type ----
  const improvementYears = [...new Set(improvement_trend.map((r: EMFAImprovementRecord) => r.year))].sort()
  const improvementTypes = [...new Set(improvement_trend.map((r: EMFAImprovementRecord) => r.forecast_type))]
  const improvementByYear: Record<number, Record<string, number[]>> = {}
  improvement_trend.forEach((r: EMFAImprovementRecord) => {
    if (!improvementByYear[r.year]) improvementByYear[r.year] = {}
    if (!improvementByYear[r.year][r.forecast_type]) improvementByYear[r.year][r.forecast_type] = []
    improvementByYear[r.year][r.forecast_type].push(r.mape_pct)
  })
  const improvementChartData = improvementYears.map((yr) => {
    const row: Record<string, string | number> = { year: yr }
    improvementTypes.forEach((ft) => {
      const vals = improvementByYear[yr]?.[ft]
      row[ft] = vals && vals.length > 0 ? +avg(vals).toFixed(2) : 0
    })
    return row
  })

  // ---- Chart 6: Event Prediction Table — top 10 events ----
  const topEvents = [...event_predictions]
    .sort((a: EMFAEventRecord, b: EMFAEventRecord) => b.financial_impact_m - a.financial_impact_m)
    .slice(0, 10)

  return (
    <div className="p-6 bg-gray-900 min-h-screen text-gray-100">
      {/* Header */}
      <div className="flex items-center gap-3 mb-6">
        <Target size={28} className="text-indigo-400" />
        <div>
          <h1 className="text-2xl font-bold text-white">Electricity Market Forecasting Accuracy Analytics</h1>
          <p className="text-sm text-gray-400">
            AEMO pre-dispatch accuracy, ML vs statistical benchmarking, renewable forecast errors,
            high-price event prediction and model improvement trends — Australia NEM
          </p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
        {kpis.map((kpi) => (
          <div key={kpi.label} className="bg-gray-800 rounded-xl border border-gray-700 p-4">
            <p className="text-xs text-gray-400 mb-1">{kpi.label}</p>
            <p className={`text-2xl font-bold ${kpi.color}`}>
              {kpi.value}
              <span className="text-sm font-normal text-gray-400 ml-1">{kpi.unit}</span>
            </p>
          </div>
        ))}
      </div>

      {/* Best Model Banner */}
      <div className="bg-indigo-900/30 border border-indigo-700/50 rounded-xl p-3 mb-6 flex items-center gap-2">
        <Target size={16} className="text-indigo-400 shrink-0" />
        <p className="text-sm text-indigo-300">
          <span className="font-semibold text-white">Best Model:</span>{' '}
          {summary.best_model_name}
          {' — '}
          Avg Renewable Error:{' '}
          <span className="font-semibold text-emerald-400">{typeof summary.avg_renewable_error_pct === 'number' ? `${summary.avg_renewable_error_pct.toFixed(2)}%` : 'n/a (no renewable model)'}</span>
        </p>
      </div>

      {/* Charts Row 1 */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-4">

        {/* Chart 1: Forecast Error by Horizon */}
        <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
          <h2 className="text-sm font-semibold text-gray-200 mb-1">Forecast Error by Horizon &amp; Model Type</h2>
          <p className="text-xs text-gray-500 mb-3">Avg absolute price error ($/MWh) — shorter horizons show lower error</p>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={horizonChartData} margin={{ top: 5, right: 10, bottom: 5, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="horizon" tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} unit=" $/MWh" width={72} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                labelStyle={{ color: '#f9fafb' }}
                itemStyle={{ color: '#9ca3af' }}
                formatter={(v: number) => [`${v.toFixed(2)} $/MWh`]}
              />
              <Legend wrapperStyle={{ fontSize: 10, color: '#9ca3af' }} />
              {allModelTypes.map((mt, i) => (
                <Bar key={mt} dataKey={mt} fill={modelTypeBarColors[i % modelTypeBarColors.length]} radius={[2, 2, 0, 0]} />
              ))}
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Chart 2: Price Forecast Accuracy Scatter */}
        <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
          <h2 className="text-sm font-semibold text-gray-200 mb-1">Price Forecast vs Actual ($/MWh)</h2>
          <p className="text-xs text-gray-500 mb-3">Points near the 1:1 line indicate higher forecast accuracy; coloured by region</p>
          <ResponsiveContainer width="100%" height={280}>
            <ScatterChart margin={{ top: 10, right: 10, bottom: 20, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                dataKey="forecast"
                type="number"
                name="Forecast Price"
                domain={[priceMin, priceMax]}
                label={{ value: 'Forecast ($/MWh)', position: 'insideBottom', offset: -12, fill: '#9ca3af', fontSize: 11 }}
                tick={{ fill: '#9ca3af', fontSize: 11 }}
              />
              <YAxis
                dataKey="actual"
                type="number"
                name="Actual Price"
                domain={[priceMin, priceMax]}
                label={{ value: 'Actual ($/MWh)', angle: -90, position: 'insideLeft', fill: '#9ca3af', fontSize: 11 }}
                tick={{ fill: '#9ca3af', fontSize: 11 }}
              />
              <Tooltip
                cursor={{ strokeDasharray: '3 3' }}
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                labelStyle={{ color: '#f9fafb' }}
                itemStyle={{ color: '#9ca3af' }}
                formatter={(v: number, name: string) => [`${v.toFixed(2)} $/MWh`, name]}
              />
              <ReferenceLine
                segment={[
                  { x: priceMin, y: priceMin },
                  { x: priceMax, y: priceMax },
                ]}
                stroke="#6b7280"
                strokeDasharray="5 3"
                label={{ value: '1:1', fill: '#6b7280', fontSize: 10 }}
              />
              <Legend wrapperStyle={{ fontSize: 11, color: '#9ca3af' }} />
              {Object.entries(scatterByRegion).map(([region, pts]) => (
                <Scatter
                  key={region}
                  name={region}
                  data={pts}
                  fill={REGION_COLORS[region] ?? '#9ca3af'}
                  opacity={0.8}
                />
              ))}
            </ScatterChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Charts Row 2 */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-4">

        {/* Chart 3: Renewable Forecast Accuracy */}
        <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
          <h2 className="text-sm font-semibold text-gray-200 mb-1">Renewable Forecast MAE by Quarter</h2>
          <p className="text-xs text-gray-500 mb-3">Average solar and wind mean absolute error (MW) across all regions</p>
          <ResponsiveContainer width="100%" height={260}>
            <LineChart data={renewableChartData} margin={{ top: 5, right: 10, bottom: 20, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                dataKey="quarter"
                tick={{ fill: '#9ca3af', fontSize: 10 }}
                angle={-30}
                textAnchor="end"
                interval={0}
              />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} unit=" MW" width={68} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                labelStyle={{ color: '#f9fafb' }}
                itemStyle={{ color: '#9ca3af' }}
                formatter={(v: number) => [`${v.toFixed(1)} MW`]}
              />
              <Legend wrapperStyle={{ fontSize: 11, color: '#9ca3af' }} />
              <Line
                type="monotone"
                dataKey="Solar MAE (MW)"
                stroke="#f59e0b"
                strokeWidth={2}
                dot={{ r: 3, fill: '#f59e0b' }}
                activeDot={{ r: 5 }}
              />
              <Line
                type="monotone"
                dataKey="Wind MAE (MW)"
                stroke="#22d3ee"
                strokeWidth={2}
                dot={{ r: 3, fill: '#22d3ee' }}
                activeDot={{ r: 5 }}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Chart 4: Model Comparison Leaderboard */}
        <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
          <h2 className="text-sm font-semibold text-gray-200 mb-1">Model MAPE Leaderboard</h2>
          <p className="text-xs text-gray-500 mb-3">Best performers at top — lower MAPE is better</p>
          <ResponsiveContainer width="100%" height={260}>
            <BarChart
              data={leaderboardData}
              layout="vertical"
              margin={{ top: 5, right: 30, bottom: 5, left: 120 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis type="number" tick={{ fill: '#9ca3af', fontSize: 11 }} unit="%" />
              <YAxis
                type="category"
                dataKey="name"
                tick={{ fill: '#9ca3af', fontSize: 10 }}
                width={115}
              />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                labelStyle={{ color: '#f9fafb' }}
                itemStyle={{ color: '#9ca3af' }}
                formatter={(v: number, _name: string, props: { payload?: { fullName?: string } }) => [
                  `${v.toFixed(2)}% MAPE`,
                  props?.payload?.fullName ?? 'Model',
                ]}
              />
              <Bar dataKey="mape_pct" radius={[0, 3, 3, 0]}>
                {leaderboardData.map((entry, idx) => (
                  <Cell
                    key={`cell-${idx}`}
                    fill={entry.is_production ? '#34d399' : '#6366f1'}
                    opacity={0.85}
                  />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
          <p className="text-xs text-gray-500 mt-1">
            <span className="inline-block w-3 h-3 bg-emerald-400 rounded-sm mr-1" />Production model
            <span className="inline-block w-3 h-3 bg-indigo-500 rounded-sm ml-3 mr-1" />Research model
          </p>
        </div>
      </div>

      {/* Charts Row 3 */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-4">

        {/* Chart 5: Improvement Over Time */}
        <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
          <h2 className="text-sm font-semibold text-gray-200 mb-1">Forecast Accuracy Improvement Over Time</h2>
          <p className="text-xs text-gray-500 mb-3">MAPE (%) by forecast type — declining trend reflects model improvements</p>
          <ResponsiveContainer width="100%" height={260}>
            <LineChart data={improvementChartData} margin={{ top: 5, right: 10, bottom: 5, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="year" tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} unit="%" width={50} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                labelStyle={{ color: '#f9fafb' }}
                itemStyle={{ color: '#9ca3af' }}
                formatter={(v: number) => [`${v.toFixed(2)}%`]}
              />
              <Legend wrapperStyle={{ fontSize: 11, color: '#9ca3af' }} />
              {improvementTypes.map((ft) => (
                <Line
                  key={ft}
                  type="monotone"
                  dataKey={ft}
                  stroke={FORECAST_TYPE_COLORS[ft] ?? '#9ca3af'}
                  strokeWidth={2}
                  dot={{ r: 3 }}
                  activeDot={{ r: 5 }}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Demand model horizon summary */}
        <div className="bg-gray-800 rounded-xl border border-gray-700 p-4">
          <h2 className="text-sm font-semibold text-gray-200 mb-1">Demand Forecast MAPE by Season</h2>
          <p className="text-xs text-gray-500 mb-3">Average MAPE (%) across demand forecast records grouped by season</p>
          <ResponsiveContainer width="100%" height={260}>
            <BarChart
              data={(() => {
                const seasonMape: Record<string, number[]> = {}
                demand_forecasts.forEach((d: EMFADemandRecord) => {
                  if (!seasonMape[d.season]) seasonMape[d.season] = []
                  seasonMape[d.season].push(d.mape_pct)
                })
                return Object.entries(seasonMape).map(([season, vals]) => ({
                  season,
                  'Avg MAPE (%)': +avg(vals).toFixed(2),
                }))
              })()}
              margin={{ top: 5, right: 10, bottom: 5, left: 0 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="season" tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} unit="%" width={50} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                labelStyle={{ color: '#f9fafb' }}
                itemStyle={{ color: '#9ca3af' }}
                formatter={(v: number) => [`${v.toFixed(2)}%`]}
              />
              <Bar dataKey="Avg MAPE (%)" fill="#f59e0b" radius={[3, 3, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Chart 6: Event Prediction Table */}
      <div className="bg-gray-800 rounded-xl border border-gray-700 p-4 mb-4">
        <h2 className="text-sm font-semibold text-gray-200 mb-1">High-Impact Event Prediction Results (Top 10)</h2>
        <p className="text-xs text-gray-500 mb-3">Events ranked by financial impact — forecast vs actual severity, lead time and accuracy</p>
        <div className="overflow-x-auto">
          <table className="w-full text-xs text-gray-300">
            <thead>
              <tr className="border-b border-gray-700">
                <th className="text-left py-2 px-3 text-gray-400 font-medium">Date</th>
                <th className="text-left py-2 px-3 text-gray-400 font-medium">Region</th>
                <th className="text-left py-2 px-3 text-gray-400 font-medium">Event Type</th>
                <th className="text-right py-2 px-3 text-gray-400 font-medium">F.Severity</th>
                <th className="text-right py-2 px-3 text-gray-400 font-medium">A.Severity</th>
                <th className="text-right py-2 px-3 text-gray-400 font-medium">Lead Time (h)</th>
                <th className="text-right py-2 px-3 text-gray-400 font-medium">Accuracy (%)</th>
                <th className="text-right py-2 px-3 text-gray-400 font-medium">Fin. Impact ($M)</th>
                <th className="text-left py-2 px-3 text-gray-400 font-medium">Recommendation</th>
              </tr>
            </thead>
            <tbody>
              {topEvents.map((evt: EMFAEventRecord, i) => {
                const severityDiff = Math.abs(evt.forecast_severity - evt.actual_severity)
                const accuracyColor =
                  evt.forecast_accuracy_pct >= 80
                    ? 'text-emerald-400'
                    : evt.forecast_accuracy_pct >= 60
                    ? 'text-amber-400'
                    : 'text-red-400'
                return (
                  <tr
                    key={i}
                    className="border-b border-gray-700/50 hover:bg-gray-700/30 transition-colors"
                  >
                    <td className="py-2 px-3 text-gray-300">{evt.event_date}</td>
                    <td className="py-2 px-3">
                      <span
                        className="px-1.5 py-0.5 rounded text-xs font-medium"
                        style={{
                          backgroundColor: (REGION_COLORS[evt.region] ?? '#6366f1') + '33',
                          color: REGION_COLORS[evt.region] ?? '#6366f1',
                        }}
                      >
                        {evt.region}
                      </span>
                    </td>
                    <td className="py-2 px-3 text-gray-300">{evt.event_type}</td>
                    <td className="py-2 px-3 text-right font-mono">{evt.forecast_severity}</td>
                    <td className={`py-2 px-3 text-right font-mono ${severityDiff > 1 ? 'text-red-400' : 'text-gray-300'}`}>
                      {evt.actual_severity}
                    </td>
                    <td className="py-2 px-3 text-right font-mono text-cyan-300">{evt.prediction_lead_time_h.toFixed(1)}</td>
                    <td className={`py-2 px-3 text-right font-mono font-semibold ${accuracyColor}`}>
                      {evt.forecast_accuracy_pct.toFixed(1)}%
                    </td>
                    <td className="py-2 px-3 text-right font-mono text-amber-300">
                      ${evt.financial_impact_m.toFixed(2)}M
                    </td>
                    <td className="py-2 px-3 text-gray-400 text-xs max-w-xs truncate" title={evt.improvement_recommendation}>
                      {evt.improvement_recommendation}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Horizon legend */}
      <div className="flex flex-wrap gap-3 justify-center mt-2">
        {Object.entries(HORIZON_COLORS).map(([h, color]) => (
          <div key={h} className="flex items-center gap-1.5">
            <div className="w-3 h-3 rounded-sm" style={{ backgroundColor: color }} />
            <span className="text-xs text-gray-400">{h}</span>
          </div>
        ))}
      </div>
    </div>
  )
}
