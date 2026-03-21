import { useEffect, useState } from 'react'
import { Target } from 'lucide-react'
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
  getElectricityPriceForecastingDashboard,
  EPFMDashboard,
} from '../api/client'

// ---------------------------------------------------------------------------
// Colour helpers
// ---------------------------------------------------------------------------

const MODEL_TYPE_COLORS: Record<string, string> = {
  ARIMA:              '#6366f1',
  XGBoost:            '#22c55e',
  LSTM:               '#f59e0b',
  Transformer:        '#ec4899',
  Ensemble:           '#06b6d4',
  'Linear Regression':'#8b5cf6',
  'Random Forest':    '#f97316',
  Prophet:            '#84cc16',
  Hybrid:             '#14b8a6',
  'Neural ODE':       '#a78bfa',
}

const LINE_COLORS = ['#6366f1', '#22c55e', '#f59e0b', '#ec4899', '#06b6d4']

const PREDICTED_COLORS: Record<string, string> = {
  true:  '#22c55e',
  false: '#ef4444',
}

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
// Chart: 1 — Model MAPE sorted ascending (bar, coloured by model_type)
// ---------------------------------------------------------------------------

function ModelMapeChart({ data }: { data: EPFMDashboard['models'] }) {
  const sorted = [...data].sort((a, b) => a.mape_pct - b.mape_pct)
  return (
    <div className="bg-gray-800 rounded-xl p-4 border border-gray-700">
      <h3 className="text-sm font-semibold text-gray-200 mb-3">Model MAPE % — Best to Worst</h3>
      <ResponsiveContainer width="100%" height={320}>
        <BarChart data={sorted} layout="vertical" margin={{ left: 20, right: 20 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis type="number" tick={{ fill: '#9ca3af', fontSize: 11 }} unit="%" />
          <YAxis
            type="category"
            dataKey="model_name"
            tick={{ fill: '#9ca3af', fontSize: 10 }}
            width={160}
          />
          <Tooltip
            contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', color: '#f9fafb' }}
            formatter={(v: number) => [`${v.toFixed(2)}%`, 'MAPE']}
          />
          <Bar dataKey="mape_pct" radius={[0, 4, 4, 0]}>
            {sorted.map((entry, idx) => (
              <Cell
                key={idx}
                fill={MODEL_TYPE_COLORS[entry.model_type] ?? '#6366f1'}
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Chart: 2 — Monthly MAPE accuracy trend 2022-2024 for 5 models
// ---------------------------------------------------------------------------

function MapeAccuracyTrendChart({ data }: { data: EPFMDashboard['forecast_accuracy'] }) {
  const modelIds = Array.from(new Set(data.map(d => d.model_id))).slice(0, 5)

  // Build series: one entry per (year, month) combo, value for each model
  type Entry = { period: string } & Record<string, number>
  const periodMap = new Map<string, Entry>()
  data
    .filter(d => modelIds.includes(d.model_id))
    .forEach(d => {
      const key = `${d.year}-${String(d.month).padStart(2, '0')}`
      if (!periodMap.has(key)) periodMap.set(key, { period: key } as unknown as Entry)
      const entry = periodMap.get(key)!
      entry[d.model_id] = d.mape_pct
    })

  const chartData = Array.from(periodMap.values()).sort((a, b) =>
    a.period.localeCompare(b.period)
  )

  return (
    <div className="bg-gray-800 rounded-xl p-4 border border-gray-700">
      <h3 className="text-sm font-semibold text-gray-200 mb-3">Monthly MAPE Accuracy Trend (2022–2024)</h3>
      <ResponsiveContainer width="100%" height={300}>
        <LineChart data={chartData} margin={{ left: 10, right: 10 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis dataKey="period" tick={{ fill: '#9ca3af', fontSize: 10 }} />
          <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} unit="%" />
          <Tooltip
            contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', color: '#f9fafb' }}
            formatter={(v: number) => [`${v.toFixed(2)}%`, 'MAPE']}
          />
          <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 12 }} />
          {modelIds.map((mid, i) => (
            <Line
              key={mid}
              type="monotone"
              dataKey={mid}
              stroke={LINE_COLORS[i % LINE_COLORS.length]}
              dot={false}
              strokeWidth={2}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Chart: 3 — Feature importance for top 2 models (grouped bar)
// ---------------------------------------------------------------------------

function FeatureImportanceChart({ data }: { data: EPFMDashboard['feature_importance'] }) {
  const modelIds = Array.from(new Set(data.map(d => d.model_id))).slice(0, 2)

  type Entry = { feature_name: string } & Record<string, number>
  const featureMap = new Map<string, Entry>()
  data
    .filter(d => modelIds.includes(d.model_id))
    .forEach(d => {
      if (!featureMap.has(d.feature_name))
        featureMap.set(d.feature_name, { feature_name: d.feature_name } as unknown as Entry)
      const entry = featureMap.get(d.feature_name)!
      entry[d.model_id] = d.importance_score
    })

  const chartData = Array.from(featureMap.values())

  return (
    <div className="bg-gray-800 rounded-xl p-4 border border-gray-700">
      <h3 className="text-sm font-semibold text-gray-200 mb-3">Feature Importance — Top 2 Models</h3>
      <ResponsiveContainer width="100%" height={320}>
        <BarChart data={chartData} margin={{ left: 10, right: 10 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis dataKey="feature_name" tick={{ fill: '#9ca3af', fontSize: 9 }} {...({ angle: -30 } as any)} textAnchor="end" height={60} />
          <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} />
          <Tooltip
            contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', color: '#f9fafb' }}
            formatter={(v: number) => [v.toFixed(4), 'Importance']}
          />
          <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 12 }} />
          {modelIds.map((mid, i) => (
            <Bar key={mid} dataKey={mid} fill={LINE_COLORS[i % LINE_COLORS.length]} radius={[3, 3, 0, 0]} />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Chart: 4 — Forecast vs Actual quarterly by region (NSW1 vs VIC1)
// ---------------------------------------------------------------------------

function ForecastVsActualChart({ data }: { data: EPFMDashboard['forecast_vs_actual'] }) {
  // Use first model, both regions
  const modelId = data.length > 0 ? data[0].model_id : ''
  const regions = ['NSW1', 'VIC1']

  type Entry = { period: string } & Record<string, number>
  const periodMap = new Map<string, Entry>()
  data
    .filter(d => d.model_id === modelId && regions.includes(d.region))
    .forEach(d => {
      const key = `${d.year} ${d.quarter}`
      if (!periodMap.has(key)) periodMap.set(key, { period: key } as unknown as Entry)
      const entry = periodMap.get(key)!
      entry[`${d.region}_forecast`] = d.avg_forecast_mwh
      entry[`${d.region}_actual`] = d.avg_actual_mwh
    })

  const chartData = Array.from(periodMap.values()).sort((a, b) =>
    a.period.localeCompare(b.period)
  )

  return (
    <div className="bg-gray-800 rounded-xl p-4 border border-gray-700">
      <h3 className="text-sm font-semibold text-gray-200 mb-3">Forecast vs Actual $/MWh — NSW1 vs VIC1 (Quarterly)</h3>
      <ResponsiveContainer width="100%" height={300}>
        <LineChart data={chartData} margin={{ left: 10, right: 10 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis dataKey="period" tick={{ fill: '#9ca3af', fontSize: 10 }} />
          <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} unit=" $" />
          <Tooltip
            contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', color: '#f9fafb' }}
            formatter={(v: number) => [`$${v.toFixed(2)}/MWh`]}
          />
          <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 12 }} />
          <Line type="monotone" dataKey="NSW1_forecast" stroke="#6366f1" dot={false} strokeWidth={2} name="NSW1 Forecast" />
          <Line type="monotone" dataKey="NSW1_actual"   stroke="#818cf8" dot={false} strokeWidth={1} strokeDasharray="4 4" name="NSW1 Actual" />
          <Line type="monotone" dataKey="VIC1_forecast" stroke="#22c55e" dot={false} strokeWidth={2} name="VIC1 Forecast" />
          <Line type="monotone" dataKey="VIC1_actual"   stroke="#4ade80" dot={false} strokeWidth={1} strokeDasharray="4 4" name="VIC1 Actual" />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Chart: 5 — Extreme event forecast error coloured by was_predicted
// ---------------------------------------------------------------------------

function ExtremeEventChart({ data }: { data: EPFMDashboard['extreme_events'] }) {
  const chartData = data.map(d => ({
    event_id: d.event_id,
    forecast_error_mwh: d.forecast_error_mwh,
    was_predicted: String(d.was_predicted),
    event_type: d.event_type,
  }))

  return (
    <div className="bg-gray-800 rounded-xl p-4 border border-gray-700">
      <h3 className="text-sm font-semibold text-gray-200 mb-3">Extreme Event Forecast Error $/MWh (green = predicted, red = missed)</h3>
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={chartData} margin={{ left: 10, right: 10 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis dataKey="event_id" tick={{ fill: '#9ca3af', fontSize: 9 }} angle={-45} textAnchor="end" height={55} />
          <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} unit=" $" />
          <Tooltip
            contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', color: '#f9fafb' }}
            formatter={(v: number, _name, props) => [
              `$${v.toFixed(2)}/MWh — ${props.payload.event_type}`,
              'Forecast Error',
            ]}
          />
          <Bar dataKey="forecast_error_mwh" radius={[3, 3, 0, 0]}>
            {chartData.map((entry, idx) => (
              <Cell
                key={idx}
                fill={PREDICTED_COLORS[entry.was_predicted] ?? '#94a3b8'}
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

export default function ElectricityPriceForecastingAnalytics() {
  const [dashboard, setDashboard] = useState<EPFMDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getElectricityPriceForecastingDashboard()
      .then(d => { setDashboard(d); setLoading(false) })
      .catch(err => { setError(String(err)); setLoading(false) })
  }, [])

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-900 flex items-center justify-center">
        <span className="text-gray-400 text-sm animate-pulse">Loading Electricity Price Forecasting Analytics…</span>
      </div>
    )
  }

  if (error || !dashboard) {
    return (
      <div className="min-h-screen bg-gray-900 flex items-center justify-center">
        <span className="text-red-400 text-sm">Error: {error ?? 'No data'}</span>
      </div>
    )
  }

  const { summary } = dashboard

  return (
    <div className="min-h-screen bg-gray-900 text-white p-6">
      {/* Header */}
      <div className="flex items-center gap-3 mb-6">
        <div className="p-2 bg-indigo-600 rounded-lg">
          <Target size={22} className="text-white" />
        </div>
        <div>
          <h1 className="text-xl font-bold text-white">Electricity Price Forecasting Model Analytics</h1>
          <p className="text-xs text-gray-400">NEM price forecasting model performance &amp; extreme event detection</p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
        <KpiCard
          label="Total Models"
          value={String(summary.total_models)}
          sub="Active forecasting models"
        />
        <KpiCard
          label="Best Model MAE $/MWh"
          value={`$${summary.best_model_mae.toFixed(2)}`}
          sub={summary.best_model_name}
        />
        <KpiCard
          label="Avg MAPE %"
          value={`${summary.avg_mape_pct.toFixed(2)}%`}
          sub="Mean absolute pct error"
        />
        <KpiCard
          label="Best Spike Recall %"
          value={`${summary.best_spike_recall_pct.toFixed(2)}%`}
          sub={`${summary.total_extreme_events_predicted} extreme events predicted`}
        />
      </div>

      {/* Charts — 2 column layout */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <ModelMapeChart data={dashboard.models} />
        <MapeAccuracyTrendChart data={dashboard.forecast_accuracy} />
        <FeatureImportanceChart data={dashboard.feature_importance} />
        <ForecastVsActualChart data={dashboard.forecast_vs_actual} />
        <div className="lg:col-span-2">
          <ExtremeEventChart data={dashboard.extreme_events} />
        </div>
      </div>
    </div>
  )
}
