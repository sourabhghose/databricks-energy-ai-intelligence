import { useEffect, useState, useMemo } from 'react'
import { Activity, RefreshCw, AlertTriangle } from 'lucide-react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ScatterChart,
  Scatter,
  LineChart,
  Line,
  ReferenceLine,
} from 'recharts'
import {
  getElectricityPriceForecastingModelsDashboard,
  type EPFDashboard,
  type EPFModelRecord,
  type EPFForecastAccuracyRecord,
  type EPFFeatureImportanceRecord,
  type EPFCalibrationRecord,
  type EPFEnsembleWeightRecord,
} from '../api/client'

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const MODEL_TYPE_COLORS: Record<string, string> = {
  GRADIENT_BOOST:  '#10b981',
  NEURAL_NET:      '#3b82f6',
  LSTM:            '#8b5cf6',
  ARIMA:           '#f59e0b',
  ENSEMBLE:        '#ef4444',
  LINEAR:          '#6b7280',
  RANDOM_FOREST:   '#06b6d4',
}

const MODEL_TYPE_BADGE: Record<string, string> = {
  GRADIENT_BOOST:  'bg-emerald-800 text-emerald-200',
  NEURAL_NET:      'bg-blue-800 text-blue-200',
  LSTM:            'bg-violet-800 text-violet-200',
  ARIMA:           'bg-amber-800 text-amber-200',
  ENSEMBLE:        'bg-red-800 text-red-200',
  LINEAR:          'bg-gray-700 text-gray-300',
  RANDOM_FOREST:   'bg-cyan-800 text-cyan-200',
}

const CATEGORY_COLORS: Record<string, string> = {
  MARKET:   '#ef4444',
  WEATHER:  '#3b82f6',
  TEMPORAL: '#f59e0b',
  FUEL:     '#10b981',
  GRID:     '#8b5cf6',
}

const REGIONS = ['NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1']

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function fmt2(v: number | null): string { return v == null ? '—' : v.toFixed(2) }
function fmt1(v: number | null): string { return v == null ? '—' : v.toFixed(1) }
function fmtPct(v: number | null): string { return v == null ? '—' : `${v.toFixed(1)}%` }
function fmtAUD(v: number | null): string { return v == null ? '—' : `$${v.toFixed(2)}` }

// ---------------------------------------------------------------------------
// KPI Card
// ---------------------------------------------------------------------------

interface KpiCardProps {
  label: string
  value: string
  sub?: string
  accent?: string
}

function KpiCard({ label, value, sub, accent = 'text-blue-400' }: KpiCardProps) {
  return (
    <div className="bg-gray-800 border border-gray-700 rounded-xl p-4 flex flex-col gap-1">
      <span className="text-xs text-gray-400 uppercase tracking-wide">{label}</span>
      <span className={`text-2xl font-bold ${accent}`}>{value}</span>
      {sub && <span className="text-xs text-gray-500">{sub}</span>}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Model Performance Table
// ---------------------------------------------------------------------------

type SortDir = 'asc' | 'desc'
type SortCol = keyof EPFModelRecord

interface SortState {
  col: SortCol
  dir: SortDir
}

function SortHeader({
  label,
  col,
  sort,
  onSort,
}: {
  label: string
  col: SortCol
  sort: SortState
  onSort: (col: SortCol) => void
}) {
  const active = sort.col === col
  return (
    <th
      className="px-3 py-2 text-left text-xs font-semibold text-gray-400 uppercase tracking-wide cursor-pointer select-none hover:text-white transition-colors"
      onClick={() => onSort(col)}
    >
      {label}
      <span className="ml-1 text-gray-600">
        {active ? (sort.dir === 'asc' ? '▲' : '▼') : '⇅'}
      </span>
    </th>
  )
}

function ModelPerformanceTable({ models }: { models: EPFModelRecord[] }) {
  const [sort, setSort] = useState<SortState>({ col: 'mae_aud_mwh', dir: 'asc' })
  const [regionFilter, setRegionFilter] = useState<string>('NSW1')

  function handleSort(col: SortCol) {
    setSort(prev =>
      prev.col === col
        ? { col, dir: prev.dir === 'asc' ? 'desc' : 'asc' }
        : { col, dir: 'asc' }
    )
  }

  const regions = useMemo(
    () => Array.from(new Set(models.map(m => m.region))).sort(),
    [models]
  )

  const filtered = useMemo(
    () => models.filter(m => m.region === regionFilter),
    [models, regionFilter]
  )

  const bestMae = useMemo(
    () => Math.min(...filtered.map(m => m.mae_aud_mwh)),
    [filtered]
  )

  const sorted = useMemo(() => {
    const copy = [...filtered]
    copy.sort((a, b) => {
      const av = a[sort.col]
      const bv = b[sort.col]
      const cmp =
        typeof av === 'number' && typeof bv === 'number'
          ? av - bv
          : String(av).localeCompare(String(bv))
      return sort.dir === 'asc' ? cmp : -cmp
    })
    return copy
  }, [filtered, sort])

  return (
    <div className="bg-gray-800 border border-gray-700 rounded-xl p-4">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-base font-semibold text-white">Model Performance Comparison</h2>
        <div className="flex gap-2 flex-wrap">
          {regions.map(r => (
            <button
              key={r}
              onClick={() => setRegionFilter(r)}
              className={`px-3 py-1 text-xs rounded-full font-medium transition-colors ${
                regionFilter === r
                  ? 'bg-blue-600 text-white'
                  : 'bg-gray-700 text-gray-400 hover:bg-gray-600'
              }`}
            >
              {r}
            </button>
          ))}
        </div>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-700">
              <SortHeader label="Model"        col="model_name"  sort={sort} onSort={handleSort} />
              <th className="px-3 py-2 text-left text-xs font-semibold text-gray-400 uppercase tracking-wide">Type</th>
              <SortHeader label="Horizon"      col="horizon"     sort={sort} onSort={handleSort} />
              <SortHeader label="MAE $/MWh"    col="mae_aud_mwh" sort={sort} onSort={handleSort} />
              <SortHeader label="RMSE $/MWh"   col="rmse_aud_mwh" sort={sort} onSort={handleSort} />
              <SortHeader label="MAPE %"       col="mape_pct"   sort={sort} onSort={handleSort} />
              <SortHeader label="R²"           col="r2_score"   sort={sort} onSort={handleSort} />
              <SortHeader label="Samples"      col="training_samples" sort={sort} onSort={handleSort} />
              <SortHeader label="Last Trained" col="last_trained" sort={sort} onSort={handleSort} />
              <th className="px-3 py-2 text-left text-xs font-semibold text-gray-400 uppercase tracking-wide">Status</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map(m => {
              const isBest = m.mae_aud_mwh === bestMae
              const badgeCls = MODEL_TYPE_BADGE[m.model_type] ?? 'bg-gray-700 text-gray-300'
              return (
                <tr
                  key={m.model_id}
                  className={`border-b border-gray-700/40 hover:bg-gray-700/30 transition-colors ${isBest ? 'bg-green-900/10' : ''}`}
                >
                  <td className="px-3 py-2 font-medium text-white">
                    {m.model_name}
                    {isBest && (
                      <span className="ml-2 text-xs bg-green-700 text-green-100 px-1.5 py-0.5 rounded">Best</span>
                    )}
                  </td>
                  <td className="px-3 py-2">
                    <span className={`text-xs px-2 py-0.5 rounded font-medium ${badgeCls}`}>
                      {m.model_type.replace('_', ' ')}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-gray-400 text-xs font-mono">{m.horizon}</td>
                  <td className={`px-3 py-2 font-semibold ${isBest ? 'text-green-400' : 'text-gray-300'}`}>
                    {fmtAUD(m.mae_aud_mwh)}
                  </td>
                  <td className="px-3 py-2 text-gray-300">{fmtAUD(m.rmse_aud_mwh)}</td>
                  <td className="px-3 py-2 text-gray-300">{fmtPct(m.mape_pct)}</td>
                  <td className="px-3 py-2 text-gray-300">{fmt2(m.r2_score)}</td>
                  <td className="px-3 py-2 text-gray-400 text-xs">{m.training_samples.toLocaleString()}</td>
                  <td className="px-3 py-2 text-gray-400 text-xs">{m.last_trained}</td>
                  <td className="px-3 py-2">
                    <span className={`text-xs px-2 py-0.5 rounded ${m.active ? 'bg-green-800 text-green-200' : 'bg-gray-700 text-gray-500'}`}>
                      {m.active ? 'Active' : 'Inactive'}
                    </span>
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Forecast Accuracy — Actual vs Forecast Scatter
// ---------------------------------------------------------------------------

function ForecastAccuracyChart({ data }: { data: EPFForecastAccuracyRecord[] }) {
  const [regionFilter, setRegionFilter] = useState<string>('NSW1')

  const filtered = useMemo(
    () => data.filter(d => d.region === regionFilter).map(d => ({
      actual: d.actual_aud_mwh,
      forecast: d.forecast_aud_mwh,
      within: d.within_10pct,
    })),
    [data, regionFilter]
  )

  const withinCount = useMemo(() => filtered.filter(d => d.within).length, [filtered])
  const withinPct = filtered.length ? ((withinCount / filtered.length) * 100).toFixed(1) : '0'

  const withinBand = useMemo(() => filtered.filter(d => d.within), [filtered])
  const outsideBand = useMemo(() => filtered.filter(d => !d.within), [filtered])

  const CustomTooltip = ({ active, payload }: any) => {
    if (!active || !payload?.length) return null
    const d = payload[0]?.payload
    return (
      <div className="bg-gray-900 border border-gray-600 rounded-lg p-3 text-xs shadow-xl">
        <p className="text-gray-300 font-semibold mb-1">Forecast vs Actual</p>
        <p className="text-gray-400">Actual: <span className="text-white">${d?.actual?.toFixed(2)}/MWh</span></p>
        <p className="text-gray-400">Forecast: <span className="text-white">${d?.forecast?.toFixed(2)}/MWh</span></p>
      </div>
    )
  }

  const diagonalMin = useMemo(() => Math.min(...filtered.map(d => Math.min(d.actual, d.forecast))), [filtered])
  const diagonalMax = useMemo(() => Math.max(...filtered.map(d => Math.max(d.actual, d.forecast))), [filtered])

  return (
    <div className="bg-gray-800 border border-gray-700 rounded-xl p-4">
      <div className="flex items-center justify-between mb-2">
        <div>
          <h2 className="text-base font-semibold text-white">Forecast Accuracy — Actual vs Forecast</h2>
          <p className="text-xs text-gray-400 mt-0.5">
            {withinPct}% of forecasts within 10% of actual price
          </p>
        </div>
        <div className="flex gap-2">
          {REGIONS.slice(0, 3).map(r => (
            <button
              key={r}
              onClick={() => setRegionFilter(r)}
              className={`px-3 py-1 text-xs rounded-full font-medium transition-colors ${
                regionFilter === r
                  ? 'bg-blue-600 text-white'
                  : 'bg-gray-700 text-gray-400 hover:bg-gray-600'
              }`}
            >
              {r}
            </button>
          ))}
        </div>
      </div>
      <ResponsiveContainer width="100%" height={300}>
        <ScatterChart margin={{ top: 10, right: 20, left: 20, bottom: 10 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis
            type="number"
            dataKey="actual"
            name="Actual"
            tick={{ fill: '#9ca3af', fontSize: 11 }}
            label={{ value: 'Actual $/MWh', position: 'insideBottom', offset: -5, fill: '#6b7280', fontSize: 11 }}
            domain={[diagonalMin - 10, diagonalMax + 10]}
          />
          <YAxis
            type="number"
            dataKey="forecast"
            name="Forecast"
            tick={{ fill: '#9ca3af', fontSize: 11 }}
            label={{ value: 'Forecast $/MWh', angle: -90, position: 'insideLeft', fill: '#6b7280', fontSize: 11 }}
            domain={[diagonalMin - 10, diagonalMax + 10]}
          />
          <Tooltip content={<CustomTooltip />} />
          <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 12, paddingTop: 8 }} />
          <ReferenceLine
            segment={[{ x: diagonalMin, y: diagonalMin }, { x: diagonalMax, y: diagonalMax }]}
            stroke="#6b7280"
            strokeDasharray="6 3"
            label={{ value: 'Perfect', fill: '#6b7280', fontSize: 10 }}
          />
          <Scatter name="Within 10%" data={withinBand} fill="#10b981" opacity={0.7} />
          <Scatter name="Outside 10%" data={outsideBand} fill="#ef4444" opacity={0.6} />
        </ScatterChart>
      </ResponsiveContainer>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Error Over Time Line Chart
// ---------------------------------------------------------------------------

function ErrorOverTimeChart({ data }: { data: EPFForecastAccuracyRecord[] }) {
  const [regionFilter, setRegionFilter] = useState<string>('NSW1')

  const chartData = useMemo(() => {
    const filtered = data.filter(d => d.region === regionFilter)
    const byDate: Record<string, number[]> = {}
    for (const d of filtered) {
      if (!byDate[d.date]) byDate[d.date] = []
      byDate[d.date].push(d.error_pct)
    }
    return Object.entries(byDate)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([date, errs]) => ({
        date: date.slice(5), // MM-DD
        avg_error_pct: parseFloat((errs.reduce((s, v) => s + v, 0) / errs.length).toFixed(2)),
      }))
  }, [data, regionFilter])

  const CustomTooltip = ({ active, payload, label }: any) => {
    if (!active || !payload?.length) return null
    return (
      <div className="bg-gray-900 border border-gray-600 rounded-lg p-3 text-xs shadow-xl">
        <p className="text-gray-300 font-semibold mb-1">{label}</p>
        <p className="text-blue-400">Avg Error: {payload[0]?.value?.toFixed(2)}%</p>
      </div>
    )
  }

  return (
    <div className="bg-gray-800 border border-gray-700 rounded-xl p-4">
      <div className="flex items-center justify-between mb-2">
        <div>
          <h2 className="text-base font-semibold text-white">Forecast Error Over Time</h2>
          <p className="text-xs text-gray-400 mt-0.5">Average absolute percentage error by date</p>
        </div>
        <div className="flex gap-2">
          {REGIONS.slice(0, 3).map(r => (
            <button
              key={r}
              onClick={() => setRegionFilter(r)}
              className={`px-3 py-1 text-xs rounded-full font-medium transition-colors ${
                regionFilter === r
                  ? 'bg-purple-600 text-white'
                  : 'bg-gray-700 text-gray-400 hover:bg-gray-600'
              }`}
            >
              {r}
            </button>
          ))}
        </div>
      </div>
      <ResponsiveContainer width="100%" height={280}>
        <LineChart data={chartData} margin={{ top: 5, right: 20, left: 10, bottom: 20 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis
            dataKey="date"
            tick={{ fill: '#9ca3af', fontSize: 10 }}
            interval={3}
            angle={-30}
            textAnchor="end"
          />
          <YAxis
            tick={{ fill: '#9ca3af', fontSize: 11 }}
            tickFormatter={v => `${v}%`}
            label={{ value: 'Error %', angle: -90, position: 'insideLeft', fill: '#6b7280', fontSize: 11 }}
          />
          <Tooltip content={<CustomTooltip />} />
          <ReferenceLine y={10} stroke="#ef4444" strokeDasharray="4 3" label={{ value: '10% threshold', fill: '#ef4444', fontSize: 10 }} />
          <Line
            type="monotone"
            dataKey="avg_error_pct"
            stroke="#8b5cf6"
            strokeWidth={2}
            dot={false}
            name="Avg Error %"
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Feature Importance Horizontal Bar Chart
// ---------------------------------------------------------------------------

function FeatureImportanceChart({ data }: { data: EPFFeatureImportanceRecord[] }) {
  const modelId = 'EPF-001-NSW1'

  const chartData = useMemo(() => {
    const modelData = data
      .filter(d => d.model_id === modelId)
      .sort((a, b) => b.importance_score - a.importance_score)
      .slice(0, 12)
      .map(d => ({
        feature: d.feature.replace(/_/g, ' '),
        importance: parseFloat((d.importance_score * 100).toFixed(2)),
        category: d.feature_category,
      }))
    return modelData
  }, [data])

  const CustomTooltip = ({ active, payload, label }: any) => {
    if (!active || !payload?.length) return null
    const d = payload[0]?.payload
    return (
      <div className="bg-gray-900 border border-gray-600 rounded-lg p-3 text-xs shadow-xl">
        <p className="text-gray-300 font-semibold mb-1">{label}</p>
        <p style={{ color: CATEGORY_COLORS[d?.category] ?? '#9ca3af' }}>
          {d?.category}: {payload[0]?.value?.toFixed(2)}%
        </p>
      </div>
    )
  }

  return (
    <div className="bg-gray-800 border border-gray-700 rounded-xl p-4">
      <h2 className="text-base font-semibold text-white mb-1">Feature Importance — XGBoost-v3 (NSW1)</h2>
      <p className="text-xs text-gray-400 mb-4">Top 12 input features by relative importance score</p>
      <div className="flex gap-3 mb-3 flex-wrap">
        {Object.entries(CATEGORY_COLORS).map(([cat, color]) => (
          <span key={cat} className="flex items-center gap-1 text-xs text-gray-400">
            <span className="w-2.5 h-2.5 rounded-sm inline-block" style={{ backgroundColor: color }} />
            {cat}
          </span>
        ))}
      </div>
      <ResponsiveContainer width="100%" height={320}>
        <BarChart
          layout="vertical"
          data={chartData}
          margin={{ top: 5, right: 30, left: 130, bottom: 5 }}
        >
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" horizontal={false} />
          <XAxis
            type="number"
            tick={{ fill: '#9ca3af', fontSize: 11 }}
            tickFormatter={v => `${v}%`}
          />
          <YAxis
            type="category"
            dataKey="feature"
            tick={{ fill: '#9ca3af', fontSize: 11 }}
            width={125}
          />
          <Tooltip content={<CustomTooltip />} />
          <Bar dataKey="importance" name="Importance %" radius={[0, 3, 3, 0]}>
            {chartData.map((entry, idx) => (
              <rect
                key={idx}
                fill={CATEGORY_COLORS[entry.category] ?? '#6b7280'}
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Calibration Reliability Diagram
// ---------------------------------------------------------------------------

function CalibrationDiagram({ data }: { data: EPFCalibrationRecord[] }) {
  const [regionFilter, setRegionFilter] = useState<string>('NSW1')

  const chartData = useMemo(() => {
    const filtered = data.filter(d => d.region === regionFilter)
    return filtered.map(d => ({
      decile: `D${d.decile}`,
      predicted: parseFloat((d.predicted_probability * 100).toFixed(1)),
      actual: parseFloat((d.actual_frequency * 100).toFixed(1)),
      error: parseFloat((d.calibration_error * 100).toFixed(2)),
      perfect: parseFloat((d.predicted_probability * 100).toFixed(1)),
    }))
  }, [data, regionFilter])

  const CustomTooltip = ({ active, payload, label }: any) => {
    if (!active || !payload?.length) return null
    return (
      <div className="bg-gray-900 border border-gray-600 rounded-lg p-3 text-xs shadow-xl">
        <p className="text-gray-300 font-semibold mb-1">{label}</p>
        {payload.map((p: any) => (
          <p key={p.dataKey} style={{ color: p.color }} className="mb-0.5">
            {p.name}: {p.value?.toFixed(1)}%
          </p>
        ))}
      </div>
    )
  }

  return (
    <div className="bg-gray-800 border border-gray-700 rounded-xl p-4">
      <div className="flex items-center justify-between mb-2">
        <div>
          <h2 className="text-base font-semibold text-white">Calibration Reliability Diagram</h2>
          <p className="text-xs text-gray-400 mt-0.5">Predicted probability vs actual frequency — closer to diagonal = better calibrated</p>
        </div>
        <div className="flex gap-2 flex-wrap">
          {REGIONS.map(r => (
            <button
              key={r}
              onClick={() => setRegionFilter(r)}
              className={`px-3 py-1 text-xs rounded-full font-medium transition-colors ${
                regionFilter === r
                  ? 'bg-amber-600 text-white'
                  : 'bg-gray-700 text-gray-400 hover:bg-gray-600'
              }`}
            >
              {r}
            </button>
          ))}
        </div>
      </div>
      <ResponsiveContainer width="100%" height={280}>
        <LineChart data={chartData} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis
            dataKey="decile"
            tick={{ fill: '#9ca3af', fontSize: 11 }}
            label={{ value: 'Probability Decile', position: 'insideBottom', offset: -5, fill: '#6b7280', fontSize: 11 }}
          />
          <YAxis
            tick={{ fill: '#9ca3af', fontSize: 11 }}
            tickFormatter={v => `${v}%`}
            domain={[0, 100]}
            label={{ value: 'Frequency %', angle: -90, position: 'insideLeft', fill: '#6b7280', fontSize: 11 }}
          />
          <Tooltip content={<CustomTooltip />} />
          <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 12, paddingTop: 8 }} />
          <Line
            type="monotone"
            dataKey="perfect"
            stroke="#6b7280"
            strokeDasharray="6 3"
            strokeWidth={1.5}
            dot={false}
            name="Perfect Calibration"
          />
          <Line
            type="monotone"
            dataKey="actual"
            stroke="#f59e0b"
            strokeWidth={2.5}
            dot={{ r: 4, fill: '#f59e0b' }}
            name="Actual Frequency"
          />
          <Line
            type="monotone"
            dataKey="predicted"
            stroke="#3b82f6"
            strokeWidth={2}
            strokeDasharray="4 2"
            dot={false}
            name="Predicted Probability"
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Ensemble Weights Bar Chart by Region
// ---------------------------------------------------------------------------

function EnsembleWeightsChart({ data }: { data: EPFEnsembleWeightRecord[] }) {
  const [horizonFilter, setHorizonFilter] = useState<string>('1H')

  const horizons = useMemo(
    () => Array.from(new Set(data.map(d => d.horizon))).sort(),
    [data]
  )

  const chartData = useMemo(() => {
    const filtered = data.filter(d => d.horizon === horizonFilter)
    const byRegion: Record<string, Record<string, number>> = {}
    for (const d of filtered) {
      if (!byRegion[d.region]) byRegion[d.region] = {}
      byRegion[d.region][d.model_name] = parseFloat((d.contribution_pct).toFixed(1))
    }
    return REGIONS.map(r => ({
      region: r,
      ...byRegion[r],
    }))
  }, [data, horizonFilter])

  const modelNames = useMemo(
    () => Array.from(new Set(data.map(d => d.model_name))),
    [data]
  )

  const MODEL_COLORS_ENSEMBLE: Record<string, string> = {
    'XGBoost-v3':      '#10b981',
    'LSTM-Attention':  '#3b82f6',
    'Ensemble-Opt':    '#ef4444',
    'LightGBM-v2':     '#f59e0b',
  }

  const CustomTooltip = ({ active, payload, label }: any) => {
    if (!active || !payload?.length) return null
    return (
      <div className="bg-gray-900 border border-gray-600 rounded-lg p-3 text-xs shadow-xl">
        <p className="text-gray-300 font-semibold mb-1">{label} — {horizonFilter}</p>
        {payload.map((p: any) => (
          <p key={p.dataKey} style={{ color: p.fill }} className="mb-0.5">
            {p.name}: {p.value?.toFixed(1)}%
          </p>
        ))}
      </div>
    )
  }

  return (
    <div className="bg-gray-800 border border-gray-700 rounded-xl p-4">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h2 className="text-base font-semibold text-white">Ensemble Model Weights by Region</h2>
          <p className="text-xs text-gray-400 mt-0.5">Contribution percentage of each base model in the ensemble</p>
        </div>
        <div className="flex gap-2 flex-wrap">
          {horizons.map(h => (
            <button
              key={h}
              onClick={() => setHorizonFilter(h)}
              className={`px-3 py-1 text-xs rounded-full font-medium transition-colors ${
                horizonFilter === h
                  ? 'bg-cyan-600 text-white'
                  : 'bg-gray-700 text-gray-400 hover:bg-gray-600'
              }`}
            >
              {h}
            </button>
          ))}
        </div>
      </div>
      <ResponsiveContainer width="100%" height={280}>
        <BarChart data={chartData} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis dataKey="region" tick={{ fill: '#9ca3af', fontSize: 11 }} />
          <YAxis
            tick={{ fill: '#9ca3af', fontSize: 11 }}
            tickFormatter={v => `${v}%`}
            domain={[0, 100]}
            label={{ value: 'Weight %', angle: -90, position: 'insideLeft', fill: '#6b7280', fontSize: 11 }}
          />
          <Tooltip content={<CustomTooltip />} />
          <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 12, paddingTop: 8 }} />
          {modelNames.map(mn => (
            <Bar
              key={mn}
              dataKey={mn}
              name={mn}
              stackId="a"
              fill={MODEL_COLORS_ENSEMBLE[mn] ?? MODEL_TYPE_COLORS.ENSEMBLE}
            />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------

export default function ElectricityPriceForecastingModelAnalytics() {
  const [data, setData] = useState<EPFDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  async function load() {
    setLoading(true)
    setError(null)
    try {
      const d = await getElectricityPriceForecastingModelsDashboard()
      setData(d)
    } catch (e: any) {
      setError(e?.message ?? 'Failed to load data')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load() }, [])

  const summary = data?.summary ?? {}

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-gray-400">
        <RefreshCw className="animate-spin mr-2 w-5 h-5" />
        Loading price forecasting model analytics...
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex items-center gap-2 p-6 text-red-400">
        <AlertTriangle className="w-5 h-5" />
        {error}
      </div>
    )
  }

  if (!data) return null

  return (
    <div className="p-6 space-y-6 text-white">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div className="flex items-start gap-3">
          <div className="mt-0.5 p-2 bg-blue-900/40 rounded-lg">
            <Activity className="w-6 h-6 text-blue-400" />
          </div>
          <div>
            <h1 className="text-xl font-bold text-white">
              Electricity Price Forecasting Model Analytics
            </h1>
            <p className="text-sm text-gray-400 mt-0.5">
              Ensemble and individual model performance for NEM electricity price forecasting —
              XGBoost, LSTM, LightGBM, TFT, ARIMA-X, and Ensemble-Opt evaluated across all regions and horizons.
            </p>
          </div>
        </div>
        <button
          onClick={load}
          className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-700 hover:bg-gray-600 rounded-lg text-xs text-gray-300 transition-colors"
        >
          <RefreshCw className="w-3.5 h-3.5" />
          Refresh
        </button>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <KpiCard
          label="Total Models"
          value={String(summary.total_models ?? '—')}
          sub={`${summary.active_models ?? 0} active`}
          accent="text-blue-400"
        />
        <KpiCard
          label="Best Model"
          value={String(summary.best_model ?? '—')}
          sub={`MAE $${summary.best_mae_aud_mwh ?? '—'}/MWh`}
          accent="text-green-400"
        />
        <KpiCard
          label="Avg MAPE"
          value={`${summary.avg_mape_pct ?? '—'}%`}
          sub="Across all active models"
          accent="text-amber-400"
        />
        <KpiCard
          label="Within 10% Accuracy"
          value={`${summary.within_10pct_accuracy ?? '—'}%`}
          sub={`${summary.regions ?? 5} regions · ${summary.horizons_supported ?? 6} horizons`}
          accent="text-purple-400"
        />
      </div>

      {/* Model Performance Table */}
      <ModelPerformanceTable models={data.models} />

      {/* Accuracy charts row */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
        <ForecastAccuracyChart data={data.forecast_accuracy} />
        <ErrorOverTimeChart data={data.forecast_accuracy} />
      </div>

      {/* Feature Importance and Calibration */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
        <FeatureImportanceChart data={data.feature_importance} />
        <CalibrationDiagram data={data.calibration} />
      </div>

      {/* Ensemble Weights */}
      <EnsembleWeightsChart data={data.ensemble_weights} />
    </div>
  )
}
