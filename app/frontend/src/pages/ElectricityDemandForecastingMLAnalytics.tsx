import { useEffect, useState } from 'react'
import { Brain } from 'lucide-react'
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
  ReferenceLine,
} from 'recharts'
import {
  getElectricityDemandForecastingMLXDashboard,
  EDFMXDashboard,
} from '../api/client'

const MODEL_TYPE_COLORS: Record<string, string> = {
  XGBoost:             '#6366f1',
  LSTM:                '#22c55e',
  Transformer:         '#f59e0b',
  Prophet:             '#06b6d4',
  SARIMA:              '#ef4444',
  'Gradient Boost':    '#a855f7',
  Ensemble:            '#f97316',
  'Linear Regression': '#78716c',
}

const FEATURE_TYPE_COLORS: Record<string, string> = {
  Weather:  '#06b6d4',
  Temporal: '#6366f1',
  Demand:   '#22c55e',
  DER:      '#f59e0b',
  Economic: '#a855f7',
}

const HORIZON_COLORS: Record<string, string> = {
  'Short-Term (≤4h)':     '#6366f1',
  'Medium-Term (4-24h)':  '#f59e0b',
  'Long-Term (>24h)':     '#ef4444',
}

function KpiCard({ label, value, sub }: { label: string; value: string | number; sub?: string }) {
  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg p-4 shadow-sm border border-gray-100 dark:border-gray-700">
      <p className="text-xs text-gray-500 dark:text-gray-400 mb-1">{label}</p>
      <p className="text-2xl font-bold text-gray-900 dark:text-white">{value}</p>
      {sub && <p className="text-xs text-gray-400 dark:text-gray-500 mt-1">{sub}</p>}
    </div>
  )
}

export default function ElectricityDemandForecastingMLAnalytics() {
  const [data, setData] = useState<EDFMXDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getElectricityDemandForecastingMLXDashboard()
      .then(setData)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-gray-400">
        <Brain size={24} className="mr-2 animate-pulse" />
        Loading Demand Forecasting ML Analytics...
      </div>
    )
  }
  if (error || !data) {
    return (
      <div className="p-6 text-red-500">
        Error: {error ?? 'No data returned'}
      </div>
    )
  }

  const { models, feature_importance, accuracy_records, structural_breaks, drift_monitoring, forecasts, summary } = data

  // Chart 1: Model MAPE Leaderboard — sorted ascending
  const mapeLeaderboard = [...models]
    .sort((a, b) => a.mape_pct - b.mape_pct)
    .map((m) => ({ name: m.model_name, mape: m.mape_pct, type: m.model_type }))

  // Chart 2: Feature Importance — top production model (lowest mape among in_production)
  const topProdModel = models
    .filter((m) => m.in_production)
    .sort((a, b) => a.mape_pct - b.mape_pct)[0]
  const topModelFeatures = feature_importance
    .filter((f) => f.model_id === topProdModel?.model_id)
    .sort((a, b) => b.feature_importance_pct - a.feature_importance_pct)
    .map((f) => ({ name: f.feature_name, importance: f.feature_importance_pct, type: f.feature_type }))

  // Chart 3: Accuracy by Horizon — grouped by horizon_type x model_type
  const horizonTypes = ['Short-Term (≤4h)', 'Medium-Term (4-24h)', 'Long-Term (>24h)']
  const modelTypesUniq = [...new Set(accuracy_records.map((r) => r.model_id.replace('EDFMX-M-0', 'M')))]
  const accByHorizon = horizonTypes.map((ht) => {
    const row: Record<string, string | number> = { horizon: ht }
    const htRecs = accuracy_records.filter((r) => r.horizon_type === ht)
    const byModelType: Record<string, number[]> = {}
    htRecs.forEach((r) => {
      const mtype = models.find((m) => m.model_id === r.model_id)?.model_type ?? 'Unknown'
      if (!byModelType[mtype]) byModelType[mtype] = []
      byModelType[mtype].push(r.mape_pct)
    })
    Object.entries(byModelType).forEach(([mtype, vals]) => {
      row[mtype] = parseFloat((vals.reduce((a, b) => a + b, 0) / vals.length).toFixed(2))
    })
    return row
  })
  const accModelTypes = [...new Set(accuracy_records.map((r) => models.find((m) => m.model_id === r.model_id)?.model_type ?? 'Unknown'))]

  // Chart 4: Forecast vs Actual — most recent 12 records
  const recentForecasts = [...forecasts]
    .sort((a, b) => a.forecast_date.localeCompare(b.forecast_date))
    .slice(-12)
    .map((f) => ({
      date: f.forecast_date.slice(5),
      forecast: f.forecast_demand_mw,
      actual: f.actual_demand_mw,
    }))

  // Chart 5: Model Drift Monitoring — psi_score by month per model
  const driftMonths = [...new Set(drift_monitoring.map((d) => d.monitor_date))].sort()
  const driftModels = [...new Set(drift_monitoring.map((d) => d.model_id))]
  const driftChartData = driftMonths.map((mon) => {
    const row: Record<string, string | number> = { month: mon }
    driftModels.forEach((mid) => {
      const rec = drift_monitoring.find((d) => d.monitor_date === mon && d.model_id === mid)
      if (rec) {
        const mname = models.find((m) => m.model_id === mid)?.model_name ?? mid
        row[mname] = rec.psi_score
      }
    })
    return row
  })
  const driftModelNames = driftModels.map((mid) => models.find((m) => m.model_id === mid)?.model_name ?? mid)
  const driftColors = ['#6366f1', '#22c55e', '#f59e0b', '#ef4444']

  // Chart 6: Structural Break Impact — by break_type x region
  const breakTypes = [...new Set(structural_breaks.map((b) => b.break_type))]
  const breakRegions = [...new Set(structural_breaks.map((b) => b.region))]
  const breakChartData = breakTypes.map((bt) => {
    const row: Record<string, string | number> = { type: bt }
    breakRegions.forEach((reg) => {
      const rec = structural_breaks.find((b) => b.break_type === bt && b.region === reg)
      if (rec) row[reg] = rec.model_impact_mape_increase_pct
    })
    return row
  })
  const breakRegionColors: Record<string, string> = {
    NSW: '#6366f1', VIC: '#22c55e', QLD: '#f59e0b', SA: '#ef4444', WA: '#06b6d4', TAS: '#a855f7',
  }

  return (
    <div className="p-6 space-y-6 bg-gray-50 dark:bg-gray-900 min-h-screen">
      {/* Header */}
      <div className="flex items-center gap-3">
        <Brain size={28} className="text-indigo-500" />
        <div>
          <h1 className="text-xl font-bold text-gray-900 dark:text-white">
            Electricity Demand Forecasting ML Model Analytics
          </h1>
          <p className="text-sm text-gray-500 dark:text-gray-400">
            Model performance comparison, feature importance, drift monitoring, and structural break detection
          </p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KpiCard
          label="Best Model MAPE (%)"
          value={`${summary.best_model_mape_pct}%`}
          sub={summary.best_model_name}
        />
        <KpiCard
          label="Production Models"
          value={summary.production_models_count}
          sub="Currently deployed"
        />
        <KpiCard
          label="Models with Drift"
          value={summary.models_with_drift}
          sub="PSI drift detected"
        />
        <KpiCard
          label="Structural Breaks YTD"
          value={summary.structural_breaks_ytd}
          sub={`Avg forecast error: ${summary.avg_forecast_error_pct}%`}
        />
      </div>

      {/* Chart 1: Model MAPE Leaderboard */}
      <div className="bg-white dark:bg-gray-800 rounded-lg p-4 shadow-sm border border-gray-100 dark:border-gray-700">
        <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-200 mb-4">
          Model MAPE Leaderboard (ascending — lower is better)
        </h2>
        <ResponsiveContainer width="100%" height={420}>
          <BarChart
            data={mapeLeaderboard}
            layout="vertical"
            margin={{ left: 140, right: 30, top: 4, bottom: 4 }}
          >
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis type="number" tickFormatter={(v) => `${v}%`} domain={[0, 'auto']} />
            <YAxis type="category" dataKey="name" width={135} tick={{ fontSize: 11 }} />
            <Tooltip formatter={(v: number) => [`${v}%`, 'MAPE']} />
            <Bar dataKey="mape" name="MAPE (%)">
              {mapeLeaderboard.map((entry, idx) => (
                <Cell key={idx} fill={MODEL_TYPE_COLORS[entry.type] ?? '#6366f1'} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
        <div className="flex flex-wrap gap-3 mt-2 justify-center">
          {Object.entries(MODEL_TYPE_COLORS).map(([mtype, color]) => (
            <span key={mtype} className="flex items-center gap-1 text-xs text-gray-500 dark:text-gray-400">
              <span className="w-3 h-3 rounded-sm inline-block" style={{ backgroundColor: color }} />
              {mtype}
            </span>
          ))}
        </div>
      </div>

      {/* Chart 2: Feature Importance */}
      <div className="bg-white dark:bg-gray-800 rounded-lg p-4 shadow-sm border border-gray-100 dark:border-gray-700">
        <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-200 mb-1">
          Feature Importance — {topProdModel?.model_name ?? 'Top Production Model'}
        </h2>
        <p className="text-xs text-gray-400 dark:text-gray-500 mb-4">
          Feature contribution (%) coloured by feature type
        </p>
        <ResponsiveContainer width="100%" height={320}>
          <BarChart
            data={topModelFeatures}
            layout="vertical"
            margin={{ left: 160, right: 30, top: 4, bottom: 4 }}
          >
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis type="number" tickFormatter={(v) => `${v}%`} />
            <YAxis type="category" dataKey="name" width={155} tick={{ fontSize: 11 }} />
            <Tooltip formatter={(v: number) => [`${v}%`, 'Importance']} />
            <Bar dataKey="importance" name="Importance (%)">
              {topModelFeatures.map((entry, idx) => (
                <Cell key={idx} fill={FEATURE_TYPE_COLORS[entry.type] ?? '#6366f1'} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
        <div className="flex flex-wrap gap-3 mt-2 justify-center">
          {Object.entries(FEATURE_TYPE_COLORS).map(([ft, color]) => (
            <span key={ft} className="flex items-center gap-1 text-xs text-gray-500 dark:text-gray-400">
              <span className="w-3 h-3 rounded-sm inline-block" style={{ backgroundColor: color }} />
              {ft}
            </span>
          ))}
        </div>
      </div>

      {/* Chart 3: Accuracy by Horizon */}
      <div className="bg-white dark:bg-gray-800 rounded-lg p-4 shadow-sm border border-gray-100 dark:border-gray-700">
        <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-200 mb-4">
          Average MAPE by Forecast Horizon and Model Type
        </h2>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={accByHorizon} margin={{ left: 10, right: 10, top: 4, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="horizon" tick={{ fontSize: 11 }} />
            <YAxis tickFormatter={(v) => `${v}%`} />
            <Tooltip formatter={(v: number) => [`${v}%`, 'Avg MAPE']} />
            <Legend />
            {accModelTypes.map((mtype, idx) => (
              <Bar
                key={mtype}
                dataKey={mtype}
                fill={MODEL_TYPE_COLORS[mtype] ?? '#6366f1'}
                name={mtype}
              />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 4: Forecast vs Actual */}
      <div className="bg-white dark:bg-gray-800 rounded-lg p-4 shadow-sm border border-gray-100 dark:border-gray-700">
        <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-200 mb-4">
          Forecast vs Actual Demand (Recent 12 Days, MW)
        </h2>
        <ResponsiveContainer width="100%" height={280}>
          <LineChart data={recentForecasts} margin={{ left: 10, right: 10, top: 4, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="date" tick={{ fontSize: 11 }} />
            <YAxis tickFormatter={(v) => `${(v / 1000).toFixed(1)}GW`} />
            <Tooltip formatter={(v: number) => [`${v.toFixed(0)} MW`]} />
            <Legend />
            <Line type="monotone" dataKey="actual" stroke="#22c55e" strokeWidth={2} dot={false} name="Actual Demand" />
            <Line type="monotone" dataKey="forecast" stroke="#6366f1" strokeWidth={2} strokeDasharray="5 5" dot={false} name="Forecast Demand" />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 5: Model Drift Monitoring */}
      <div className="bg-white dark:bg-gray-800 rounded-lg p-4 shadow-sm border border-gray-100 dark:border-gray-700">
        <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-200 mb-1">
          Model Drift Monitoring — PSI Score by Month
        </h2>
        <p className="text-xs text-gray-400 dark:text-gray-500 mb-4">
          PSI above 0.2 threshold indicates significant distribution shift
        </p>
        <ResponsiveContainer width="100%" height={280}>
          <LineChart data={driftChartData} margin={{ left: 10, right: 20, top: 4, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="month" tick={{ fontSize: 10 }} />
            <YAxis domain={[0, 0.45]} tickFormatter={(v) => v.toFixed(2)} />
            <Tooltip formatter={(v: number) => [v.toFixed(3), 'PSI']} />
            <Legend />
            <ReferenceLine y={0.2} stroke="#ef4444" strokeDasharray="4 4" label={{ value: 'Drift Threshold', fill: '#ef4444', fontSize: 10 }} />
            {driftModelNames.map((mname, idx) => (
              <Line
                key={mname}
                type="monotone"
                dataKey={mname}
                stroke={driftColors[idx % driftColors.length]}
                strokeWidth={1.5}
                dot={{ r: 3 }}
                name={mname}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 6: Structural Break Impact */}
      <div className="bg-white dark:bg-gray-800 rounded-lg p-4 shadow-sm border border-gray-100 dark:border-gray-700">
        <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-200 mb-4">
          Structural Break — MAPE Impact by Break Type and Region (%)
        </h2>
        <ResponsiveContainer width="100%" height={340}>
          <BarChart
            data={breakChartData}
            margin={{ left: 10, right: 10, top: 4, bottom: 60 }}
          >
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="type" tick={{ fontSize: 10, angle: -30, textAnchor: 'end' } as any} />
            <YAxis tickFormatter={(v) => `+${v}%`} />
            <Tooltip formatter={(v: number) => [`+${v}%`, 'MAPE Increase']} />
            <Legend />
            {breakRegions.map((reg) => (
              <Bar key={reg} dataKey={reg} fill={breakRegionColors[reg] ?? '#6366f1'} name={reg} />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Model Detail Table */}
      <div className="bg-white dark:bg-gray-800 rounded-lg p-4 shadow-sm border border-gray-100 dark:border-gray-700">
        <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-200 mb-4">
          All Models — Performance Summary
        </h2>
        <div className="overflow-x-auto">
          <table className="w-full text-xs text-left">
            <thead>
              <tr className="text-gray-400 dark:text-gray-500 border-b border-gray-100 dark:border-gray-700">
                <th className="py-2 pr-3">Model</th>
                <th className="py-2 pr-3">Type</th>
                <th className="py-2 pr-3">Horizon</th>
                <th className="py-2 pr-3">Region</th>
                <th className="py-2 pr-3 text-right">MAPE (%)</th>
                <th className="py-2 pr-3 text-right">MAE (MW)</th>
                <th className="py-2 pr-3 text-right">R²</th>
                <th className="py-2 pr-3 text-right">Features</th>
                <th className="py-2">Production</th>
              </tr>
            </thead>
            <tbody>
              {[...models].sort((a, b) => a.mape_pct - b.mape_pct).map((m) => (
                <tr key={m.model_id} className="border-b border-gray-50 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-750">
                  <td className="py-1.5 pr-3 font-medium text-gray-800 dark:text-gray-200">{m.model_name}</td>
                  <td className="py-1.5 pr-3">
                    <span
                      className="px-1.5 py-0.5 rounded text-white text-[10px]"
                      style={{ backgroundColor: MODEL_TYPE_COLORS[m.model_type] ?? '#6366f1' }}
                    >
                      {m.model_type}
                    </span>
                  </td>
                  <td className="py-1.5 pr-3 text-gray-600 dark:text-gray-400">{m.forecast_horizon}</td>
                  <td className="py-1.5 pr-3 text-gray-600 dark:text-gray-400">{m.region}</td>
                  <td className="py-1.5 pr-3 text-right font-mono">{m.mape_pct.toFixed(1)}</td>
                  <td className="py-1.5 pr-3 text-right font-mono">{m.mae_mw.toFixed(0)}</td>
                  <td className="py-1.5 pr-3 text-right font-mono">{m.r_squared.toFixed(3)}</td>
                  <td className="py-1.5 pr-3 text-right">{m.features_count}</td>
                  <td className="py-1.5">
                    <span className={`px-1.5 py-0.5 rounded text-[10px] ${m.in_production ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400' : 'bg-gray-100 text-gray-500 dark:bg-gray-700 dark:text-gray-400'}`}>
                      {m.in_production ? 'Live' : 'Staging'}
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
