import { useEffect, useState } from 'react'
import {
  BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer, Cell,
} from 'recharts'
import { Target } from 'lucide-react'
import {
  getNemDemandForecastingAccuracyDashboard,
  NDFADashboard,
} from '../api/client'

// ── Colour palettes ──────────────────────────────────────────────────────────
const REGION_COLOURS: Record<string, string> = {
  NSW1: '#f59e0b',
  QLD1: '#3b82f6',
  VIC1: '#10b981',
  SA1:  '#8b5cf6',
  TAS1: '#ef4444',
}

const MODEL_COLOURS: Record<string, string> = {
  'AEMO Pre-dispatch':  '#f59e0b',
  'ML Gradient Boost':  '#3b82f6',
  'Neural Network':     '#10b981',
  'Ensemble':           '#8b5cf6',
  'Statistical ARIMA':  '#ef4444',
}

const HORIZONS = ['30min', '1hour', '4hour', 'day_ahead', 'week_ahead']
const REGIONS  = ['NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1']
const MODEL_NAMES = ['AEMO Pre-dispatch', 'ML Gradient Boost', 'Neural Network', 'Ensemble', 'Statistical ARIMA']

// ── KPI Card ─────────────────────────────────────────────────────────────────
function KpiCard({ label, value, unit, sub }: { label: string; value: string; unit?: string; sub?: string }) {
  return (
    <div className="bg-gray-800 rounded-xl p-4 flex flex-col gap-1 min-w-0">
      <span className="text-xs text-gray-400 truncate">{label}</span>
      <span className="text-2xl font-bold text-white leading-none">
        {value}
        {unit && <span className="text-sm font-normal text-gray-400 ml-1">{unit}</span>}
      </span>
      {sub && <span className="text-xs text-gray-500 mt-0.5">{sub}</span>}
    </div>
  )
}

// ── Chart Section Wrapper ─────────────────────────────────────────────────────
function ChartSection({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="bg-gray-800 rounded-xl p-5">
      <h3 className="text-sm font-semibold text-gray-200 mb-4">{title}</h3>
      {children}
    </div>
  )
}

// ── Main Page ─────────────────────────────────────────────────────────────────
export default function NemDemandForecastingAccuracyAnalytics() {
  const [data, setData] = useState<NDFADashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getNemDemandForecastingAccuracyDashboard()
      .then(setData)
      .catch((e: unknown) => setError(String(e)))
      .finally(() => setLoading(false))
  }, [])

  if (loading)
    return (
      <div className="flex items-center justify-center h-64 text-gray-400">
        Loading NEM Demand Forecasting Accuracy Analytics…
      </div>
    )
  if (error || !data)
    return (
      <div className="flex items-center justify-center h-64 text-red-400">
        {error ?? 'Failed to load data.'}
      </div>
    )

  const { forecast_accuracy, hourly_profiles, model_comparison, extreme_events, feature_importance, summary } = data

  // ── Chart 1: avg mape_pct by horizon × region ─────────────────────────────
  const mapeByHorizonRegion: Record<string, Record<string, number[]>> = {}
  for (const h of HORIZONS) mapeByHorizonRegion[h] = {}
  for (const fa of forecast_accuracy) {
    if (!mapeByHorizonRegion[fa.forecast_horizon]) continue
    if (!mapeByHorizonRegion[fa.forecast_horizon][fa.region]) {
      mapeByHorizonRegion[fa.forecast_horizon][fa.region] = []
    }
    mapeByHorizonRegion[fa.forecast_horizon][fa.region].push(fa.mape_pct)
  }
  const chart1Data = HORIZONS.map(h => {
    const row: Record<string, string | number> = { horizon: h }
    for (const reg of REGIONS) {
      const vals = mapeByHorizonRegion[h][reg] ?? []
      row[reg] = vals.length > 0 ? Math.round((vals.reduce((a, b) => a + b, 0) / vals.length) * 100) / 100 : 0
    }
    return row
  })

  // ── Chart 2: mae_mw by model over years (NSW1 only) ───────────────────────
  const maeByModelYear: Record<string, Record<number, number[]>> = {}
  for (const m of MODEL_NAMES) maeByModelYear[m] = { 2022: [], 2023: [], 2024: [] }
  for (const mc of model_comparison) {
    if (mc.region !== 'NSW1') continue
    if (!maeByModelYear[mc.model_name]) continue
    maeByModelYear[mc.model_name][mc.year].push(mc.mae_mw)
  }
  const chart2Data = [2022, 2023, 2024].map(yr => {
    const row: Record<string, string | number> = { year: String(yr) }
    for (const m of MODEL_NAMES) {
      const vals = maeByModelYear[m][yr] ?? []
      row[m] = vals.length > 0 ? Math.round((vals.reduce((a, b) => a + b, 0) / vals.length) * 10) / 10 : 0
    }
    return row
  })

  // ── Chart 3: actual vs forecast by hour for Summer × NSW1 ─────────────────
  const summerNSW = hourly_profiles.filter(p => p.region === 'NSW1' && p.season === 'Summer')
  const chart3Data = Array.from({ length: 24 }, (_, h) => {
    const rows = summerNSW.filter(p => p.hour_of_day === h)
    if (rows.length === 0) return { hour: `${String(h).padStart(2, '0')}:00`, actual: 0, forecast: 0 }
    const actual = Math.round(rows.reduce((a, b) => a + b.actual_demand_mw, 0) / rows.length)
    const forecast = Math.round(rows.reduce((a, b) => a + b.forecast_demand_mw, 0) / rows.length)
    return { hour: `${String(h).padStart(2, '0')}:00`, actual, forecast }
  })

  // ── Chart 4: avg error_pct by event_type ──────────────────────────────────
  const eventTypes = [...new Set(extreme_events.map(e => e.event_type))]
  const chart4Data = eventTypes.map(evt => {
    const rows = extreme_events.filter(e => e.event_type === evt)
    const avg = rows.reduce((a, b) => a + b.error_pct, 0) / rows.length
    return { event_type: evt, error_pct: Math.round(avg * 100) / 100 }
  }).sort((a, b) => a.error_pct - b.error_pct)

  // ── Chart 5: avg importance_score by feature ──────────────────────────────
  const featureNames = [...new Set(feature_importance.map(f => f.feature_name))]
  const chart5Data = featureNames.map(feat => {
    const rows = feature_importance.filter(f => f.feature_name === feat)
    const avg = rows.reduce((a, b) => a + b.importance_score, 0) / rows.length
    return { feature: feat, score: Math.round(avg * 10000) / 10000 }
  }).sort((a, b) => b.score - a.score)

  return (
    <div className="p-6 space-y-6 bg-gray-900 min-h-screen text-gray-100">
      {/* Header */}
      <div className="flex items-center gap-3">
        <Target className="text-amber-400" size={26} />
        <div>
          <h1 className="text-xl font-bold text-white">NEM Demand Forecasting Accuracy Analytics</h1>
          <p className="text-sm text-gray-400 mt-0.5">Model performance, error profiles, and feature importance across NEM regions</p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
        <KpiCard label="Best Performing Model" value={summary.best_model} />
        <KpiCard label="Avg 4h-Ahead MAPE" value={String(summary.avg_day_ahead_mape_pct)} unit="%" />
        <KpiCard label="Avg 30-min MAE" value={String(summary.avg_30min_mae_mw)} unit="MW" />
        <KpiCard label="Worst Region (Day-Ahead)" value={summary.worst_performing_region} sub="Highest avg MAPE" />
        <KpiCard label="Extreme Event Avg Error" value={String(summary.extreme_event_avg_error_pct)} unit="%" sub="Absolute avg" />
      </div>

      {/* Chart 1: MAPE by Horizon × Region */}
      <ChartSection title="Chart 1 — Avg MAPE (%) by Forecast Horizon and Region">
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={chart1Data} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="horizon" tick={{ fill: '#9ca3af', fontSize: 12 }} />
            <YAxis tick={{ fill: '#9ca3af', fontSize: 12 }} unit="%" />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: 'none', borderRadius: 8 }}
              labelStyle={{ color: '#f9fafb' }}
              formatter={(v: number) => [`${v}%`, '']}
            />
            <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 12 }} />
            {REGIONS.map(reg => (
              <Bar key={reg} dataKey={reg} fill={REGION_COLOURS[reg]} radius={[3, 3, 0, 0]} />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </ChartSection>

      {/* Chart 2: MAE by Model over Years (NSW1) */}
      <ChartSection title="Chart 2 — Avg MAE (MW) by Model and Year — NSW1">
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={chart2Data} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="year" tick={{ fill: '#9ca3af', fontSize: 12 }} />
            <YAxis tick={{ fill: '#9ca3af', fontSize: 12 }} unit=" MW" />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: 'none', borderRadius: 8 }}
              labelStyle={{ color: '#f9fafb' }}
              formatter={(v: number) => [`${v} MW`, '']}
            />
            <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 12 }} />
            {MODEL_NAMES.map(m => (
              <Line
                key={m}
                type="monotone"
                dataKey={m}
                stroke={MODEL_COLOURS[m]}
                strokeWidth={2}
                dot={{ r: 4, fill: MODEL_COLOURS[m] }}
                activeDot={{ r: 6 }}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </ChartSection>

      {/* Chart 3: Actual vs Forecast by Hour (Summer, NSW1) */}
      <ChartSection title="Chart 3 — Actual vs Forecast Demand by Hour of Day — Summer, NSW1">
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={chart3Data} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="hour" tick={{ fill: '#9ca3af', fontSize: 10 }} interval={2} />
            <YAxis tick={{ fill: '#9ca3af', fontSize: 12 }} unit=" MW" />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: 'none', borderRadius: 8 }}
              labelStyle={{ color: '#f9fafb' }}
              formatter={(v: number) => [`${v.toLocaleString()} MW`, '']}
            />
            <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 12 }} />
            <Line type="monotone" dataKey="actual" stroke="#f59e0b" strokeWidth={2} dot={false} name="Actual Demand" />
            <Line type="monotone" dataKey="forecast" stroke="#3b82f6" strokeWidth={2} strokeDasharray="5 5" dot={false} name="Forecast Demand" />
          </LineChart>
        </ResponsiveContainer>
      </ChartSection>

      {/* Chart 4: Error % by Event Type */}
      <ChartSection title="Chart 4 — Avg Forecast Error (%) by Extreme Event Type">
        <ResponsiveContainer width="100%" height={280}>
          <BarChart data={chart4Data} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="event_type" tick={{ fill: '#9ca3af', fontSize: 12 }} />
            <YAxis tick={{ fill: '#9ca3af', fontSize: 12 }} unit="%" />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: 'none', borderRadius: 8 }}
              labelStyle={{ color: '#f9fafb' }}
              formatter={(v: number) => [`${v}%`, 'Avg Error']}
            />
            <Bar dataKey="error_pct" radius={[4, 4, 0, 0]} name="Avg Error %">
              {chart4Data.map((entry, idx) => (
                <Cell key={idx} fill={entry.error_pct >= 0 ? '#ef4444' : '#10b981'} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
        <p className="text-xs text-gray-500 mt-2">Red = over-forecast (positive error), Green = under-forecast (negative error)</p>
      </ChartSection>

      {/* Chart 5: Feature Importance (Horizontal Bar) */}
      <ChartSection title="Chart 5 — Feature Importance Score (Avg across All Regions)">
        <ResponsiveContainer width="100%" height={280}>
          <BarChart
            layout="vertical"
            data={chart5Data}
            margin={{ top: 4, right: 16, left: 100, bottom: 4 }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis type="number" domain={[0, 1]} tick={{ fill: '#9ca3af', fontSize: 12 }} />
            <YAxis type="category" dataKey="feature" tick={{ fill: '#9ca3af', fontSize: 12 }} width={130} />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: 'none', borderRadius: 8 }}
              labelStyle={{ color: '#f9fafb' }}
              formatter={(v: number) => [v.toFixed(4), 'Importance Score']}
            />
            <Bar dataKey="score" fill="#8b5cf6" radius={[0, 4, 4, 0]} name="Importance Score" />
          </BarChart>
        </ResponsiveContainer>
      </ChartSection>

      {/* Summary Grid */}
      <div className="bg-gray-800 rounded-xl p-5">
        <h3 className="text-sm font-semibold text-gray-200 mb-4">Dashboard Summary</h3>
        <dl className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
          {[
            { label: 'Best Model', value: summary.best_model },
            { label: 'Avg 4h-Ahead MAPE', value: `${summary.avg_day_ahead_mape_pct}%` },
            { label: 'Avg 30-min MAE', value: `${summary.avg_30min_mae_mw} MW` },
            { label: 'Worst Region', value: summary.worst_performing_region },
            { label: 'Extreme Event Avg Error', value: `${summary.extreme_event_avg_error_pct}%` },
          ].map(({ label, value }) => (
            <div key={label} className="flex flex-col gap-1">
              <dt className="text-xs text-gray-500">{label}</dt>
              <dd className="text-sm font-semibold text-gray-100 break-words">{value}</dd>
            </div>
          ))}
        </dl>
      </div>
    </div>
  )
}
