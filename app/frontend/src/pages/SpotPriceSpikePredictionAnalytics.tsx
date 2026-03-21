import { useEffect, useState } from 'react'
import {
  BarChart, Bar,
  ScatterChart, Scatter,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer, ReferenceLine,
} from 'recharts'
import { AlertOctagon, Activity, Shield, Clock } from 'lucide-react'
import {
  getSpotPriceSpikePredictionDashboard,
  SPPDashboard,
  SPPAlertRecord,
  SPPModelPerformanceRecord,
  SPPSpikeHistoryRecord,
  SPPFeatureRecord,
  SPPPredictionRecord,
} from '../api/client'

// ─── Colour helpers ───────────────────────────────────────────────────────────
const STATUS_COLOURS: Record<string, string> = {
  ACTIVE:              'bg-red-700',
  RESOLVED_SPIKE:      'bg-orange-700',
  RESOLVED_NO_SPIKE:   'bg-green-700',
  EXPIRED:             'bg-gray-600',
}

const CATEGORY_COLOURS: Record<string, string> = {
  MARKET:   '#60a5fa',
  WEATHER:  '#4ade80',
  GRID:     '#facc15',
  TEMPORAL: '#a78bfa',
  FUEL:     '#fb923c',
}

// ─── KPI Card ─────────────────────────────────────────────────────────────────
function KpiCard({ label, value, sub, Icon }: {
  label: string; value: string; sub?: string; Icon: React.ElementType
}) {
  return (
    <div className="bg-gray-800 rounded-xl p-5 flex items-start gap-4 shadow-lg">
      <div className="p-3 bg-gray-700 rounded-lg">
        <Icon className="w-6 h-6 text-red-400" />
      </div>
      <div>
        <p className="text-xs text-gray-400 uppercase tracking-wide">{label}</p>
        <p className="text-2xl font-bold text-white mt-0.5">{value}</p>
        {sub && <p className="text-xs text-gray-400 mt-0.5">{sub}</p>}
      </div>
    </div>
  )
}

// ─── Badge ────────────────────────────────────────────────────────────────────
function Badge({ label, colourClass }: { label: string; colourClass: string }) {
  return (
    <span className={`inline-block px-2 py-0.5 rounded text-xs font-semibold text-white ${colourClass}`}>
      {label}
    </span>
  )
}

// ─── Probability Bar ──────────────────────────────────────────────────────────
function ProbBar({ value }: { value: number }) {
  const pct = Math.round(value * 100)
  const colour =
    pct >= 80 ? 'bg-red-500' :
    pct >= 60 ? 'bg-orange-500' :
    pct >= 40 ? 'bg-yellow-500' : 'bg-green-600'
  return (
    <div className="flex items-center gap-2">
      <div className="w-24 h-2 bg-gray-700 rounded-full overflow-hidden">
        <div className={`h-full rounded-full ${colour}`} style={{ width: `${pct}%` }} />
      </div>
      <span className="text-xs text-gray-300 w-8 text-right">{pct}%</span>
    </div>
  )
}

// ─── Main page ────────────────────────────────────────────────────────────────
export default function SpotPriceSpikePredictionAnalytics() {
  const [data, setData] = useState<SPPDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getSpotPriceSpikePredictionDashboard()
      .then(setData)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-gray-400 bg-gray-900">
        Loading Spot Price Spike Prediction Analytics...
      </div>
    )
  }
  if (error || !data) {
    return (
      <div className="flex items-center justify-center h-64 text-red-400 bg-gray-900">
        Error: {error ?? 'No data'}
      </div>
    )
  }

  const summary = data.summary as Record<string, number | string>

  // ─── Feature importance (top 10) ───────────────────────────────────────────
  const topFeatures: SPPFeatureRecord[] = [...data.features]
    .sort((a, b) => b.importance - a.importance)
    .slice(0, 10)
    .map(f => ({ ...f, importance: Math.round(f.importance * 1000) / 10 }))

  // ─── Model performance (Ensemble only across all regions) ──────────────────
  const ensemblePerf: SPPModelPerformanceRecord[] = data.model_performance
    .filter(m => m.model_name === 'Ensemble-Spike')
    .slice(0, 8)

  // ─── Active alerts (sorted by probability desc) ────────────────────────────
  const sortedAlerts: SPPAlertRecord[] = [...data.alerts].sort(
    (a, b) => b.spike_probability - a.spike_probability
  )

  // ─── Prediction scatter (predicted prob vs actual, for actuals only) ───────
  const scatterData = data.predictions
    .filter(p => p.actual_price_aud_mwh !== null)
    .map((p: SPPPredictionRecord) => ({
      prob: Math.round(p.predicted_spike_probability * 100),
      actual: Math.round(p.actual_price_aud_mwh as number),
      spike: (p.actual_price_aud_mwh as number) >= p.threshold_aud_mwh ? 1 : 0,
    }))

  const spikeScatter = scatterData.filter(d => d.spike === 1)
  const noSpikeScatter = scatterData.filter(d => d.spike === 0)

  return (
    <div className="bg-gray-900 min-h-screen p-6 space-y-8">
      {/* Header */}
      <div className="flex items-center gap-3">
        <AlertOctagon className="w-8 h-8 text-red-400" />
        <div>
          <h1 className="text-2xl font-bold text-white">
            Spot Price Spike Prediction Analytics
          </h1>
          <p className="text-sm text-gray-400 mt-0.5">
            NEM ML model performance, spike alerts and feature attribution
          </p>
        </div>
      </div>

      {/* KPI cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <KpiCard
          label="Active Spike Alerts"
          value={String(summary.active_alerts ?? 0)}
          sub="Across all NEM regions"
          Icon={AlertOctagon}
        />
        <KpiCard
          label="Best AUC-ROC"
          value={`${((summary.best_auc_roc as number) * 100).toFixed(1)}%`}
          sub={`Model: ${summary.best_model}`}
          Icon={Activity}
        />
        <KpiCard
          label="Spike Detection Rate"
          value={`${summary.spike_detection_rate_pct}%`}
          sub={`FP Rate: ${summary.false_positive_rate_pct}%`}
          Icon={Shield}
        />
        <KpiCard
          label="Avg Warning Lead Time"
          value={`${summary.avg_warning_lead_time_minutes} min`}
          sub={`${summary.total_spikes_ytd} spikes YTD`}
          Icon={Clock}
        />
      </div>

      {/* Active Alerts table */}
      <div className="bg-gray-800 rounded-xl shadow-lg overflow-hidden">
        <div className="px-5 py-4 border-b border-gray-700">
          <h2 className="text-base font-semibold text-white">Spike Alerts</h2>
          <p className="text-xs text-gray-400 mt-0.5">
            Sorted by spike probability — all regions
          </p>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-xs text-gray-400 uppercase tracking-wide border-b border-gray-700">
                <th className="px-4 py-3">Alert ID</th>
                <th className="px-4 py-3">Region</th>
                <th className="px-4 py-3">Issued</th>
                <th className="px-4 py-3">Window</th>
                <th className="px-4 py-3">Probability</th>
                <th className="px-4 py-3">Expected Price</th>
                <th className="px-4 py-3">Triggers</th>
                <th className="px-4 py-3">Status</th>
              </tr>
            </thead>
            <tbody>
              {sortedAlerts.map(alert => (
                <tr key={alert.alert_id} className="border-b border-gray-700/50 hover:bg-gray-700/30">
                  <td className="px-4 py-2.5 font-mono text-xs text-gray-300">{alert.alert_id}</td>
                  <td className="px-4 py-2.5 font-semibold text-cyan-400">{alert.region}</td>
                  <td className="px-4 py-2.5 text-gray-300 text-xs">{alert.issued_at}</td>
                  <td className="px-4 py-2.5 text-gray-300">{alert.forecast_window_minutes} min</td>
                  <td className="px-4 py-2.5">
                    <ProbBar value={alert.spike_probability} />
                  </td>
                  <td className="px-4 py-2.5 text-white font-semibold">
                    ${alert.expected_price_aud_mwh.toLocaleString()}/MWh
                  </td>
                  <td className="px-4 py-2.5">
                    <div className="flex flex-wrap gap-1">
                      {alert.trigger_factors.map(t => (
                        <span key={t} className="inline-block px-1.5 py-0.5 bg-gray-700 rounded text-xs text-gray-300">{t}</span>
                      ))}
                    </div>
                  </td>
                  <td className="px-4 py-2.5">
                    <Badge
                      label={alert.status.replace(/_/g, ' ')}
                      colourClass={STATUS_COLOURS[alert.status] ?? 'bg-gray-600'}
                    />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Model Performance table */}
      <div className="bg-gray-800 rounded-xl shadow-lg overflow-hidden">
        <div className="px-5 py-4 border-b border-gray-700">
          <h2 className="text-base font-semibold text-white">Model Performance — Ensemble-Spike</h2>
          <p className="text-xs text-gray-400 mt-0.5">Precision / Recall / F1 / AUC-ROC per region</p>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-xs text-gray-400 uppercase tracking-wide border-b border-gray-700">
                <th className="px-4 py-3">Region</th>
                <th className="px-4 py-3">Period</th>
                <th className="px-4 py-3">Precision</th>
                <th className="px-4 py-3">Recall</th>
                <th className="px-4 py-3">F1 Score</th>
                <th className="px-4 py-3">AUC-ROC</th>
                <th className="px-4 py-3">FP Rate</th>
                <th className="px-4 py-3">Total Spikes</th>
                <th className="px-4 py-3">Predicted</th>
              </tr>
            </thead>
            <tbody>
              {ensemblePerf.map((m, i) => (
                <tr key={i} className="border-b border-gray-700/50 hover:bg-gray-700/30">
                  <td className="px-4 py-2.5 font-semibold text-cyan-400">{m.region}</td>
                  <td className="px-4 py-2.5 text-gray-300">{m.period}</td>
                  <td className="px-4 py-2.5 text-white">{(m.precision * 100).toFixed(1)}%</td>
                  <td className="px-4 py-2.5 text-white">{(m.recall * 100).toFixed(1)}%</td>
                  <td className="px-4 py-2.5">
                    <span className={`font-semibold ${m.f1_score >= 0.80 ? 'text-green-400' : m.f1_score >= 0.70 ? 'text-yellow-400' : 'text-red-400'}`}>
                      {(m.f1_score * 100).toFixed(1)}%
                    </span>
                  </td>
                  <td className="px-4 py-2.5">
                    <span className={`font-semibold ${m.auc_roc >= 0.90 ? 'text-green-400' : 'text-yellow-400'}`}>
                      {m.auc_roc.toFixed(3)}
                    </span>
                  </td>
                  <td className="px-4 py-2.5 text-orange-400">{(m.false_positive_rate * 100).toFixed(1)}%</td>
                  <td className="px-4 py-2.5 text-gray-300">{m.total_spikes}</td>
                  <td className="px-4 py-2.5 text-gray-300">{m.predicted_spikes}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Feature Importance + Scatter charts row */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
        {/* Feature Importance */}
        <div className="bg-gray-800 rounded-xl shadow-lg p-5">
          <h2 className="text-base font-semibold text-white mb-1">Feature Importance (Top 10)</h2>
          <p className="text-xs text-gray-400 mb-4">Contribution to spike probability prediction</p>
          <ResponsiveContainer width="100%" height={320}>
            <BarChart
              data={topFeatures}
              layout="vertical"
              margin={{ top: 0, right: 20, left: 8, bottom: 0 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" horizontal={false} />
              <XAxis type="number" tick={{ fill: '#9ca3af', fontSize: 11 }} unit="%" />
              <YAxis
                type="category"
                dataKey="feature"
                tick={{ fill: '#d1d5db', fontSize: 11 }}
                width={160}
              />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                labelStyle={{ color: '#f9fafb' }}
                formatter={((val: number, _name: string, entry: { payload: SPPFeatureRecord }) => [
                  `${val}% importance | correlation: ${entry.payload.spike_correlation}`,
                  entry.payload.category,
                ]) as any}
              />
              <Bar
                dataKey="importance"
                name="Importance %"
                radius={[0, 4, 4, 0]}
                fill="#60a5fa"
                label={false}
              >
                {topFeatures.map((f, i) => (
                  <rect key={i} fill={CATEGORY_COLOURS[f.category] ?? '#60a5fa'} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
          {/* Legend */}
          <div className="flex flex-wrap gap-3 mt-3">
            {Object.entries(CATEGORY_COLOURS).map(([cat, colour]) => (
              <span key={cat} className="flex items-center gap-1.5 text-xs text-gray-400">
                <span className="w-3 h-3 rounded-sm inline-block" style={{ backgroundColor: colour }} />
                {cat}
              </span>
            ))}
          </div>
        </div>

        {/* Prediction Scatter */}
        <div className="bg-gray-800 rounded-xl shadow-lg p-5">
          <h2 className="text-base font-semibold text-white mb-1">Predicted Probability vs Actual Price</h2>
          <p className="text-xs text-gray-400 mb-4">Each dot is a dispatch interval — red = actual spike</p>
          <ResponsiveContainer width="100%" height={320}>
            <ScatterChart margin={{ top: 10, right: 20, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                type="number"
                dataKey="prob"
                name="Predicted Probability"
                unit="%"
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                label={{ value: 'Spike Probability (%)', position: 'insideBottom', offset: -2, fill: '#6b7280', fontSize: 11 }}
              />
              <YAxis
                type="number"
                dataKey="actual"
                name="Actual Price"
                unit=" $/MWh"
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                width={65}
              />
              <Tooltip
                cursor={{ strokeDasharray: '3 3' }}
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                formatter={(val: number, name: string) =>
                  name === 'Actual Price' ? [`$${val}/MWh`, name] : [`${val}%`, name]
                }
              />
              <ReferenceLine x={50} stroke="#facc15" strokeDasharray="4 2" label={{ value: '50% threshold', fill: '#facc15', fontSize: 10 }} />
              <ReferenceLine y={300} stroke="#f87171" strokeDasharray="4 2" label={{ value: '$300 spike', fill: '#f87171', fontSize: 10, position: 'insideTopRight' }} />
              <Legend wrapperStyle={{ fontSize: 12, color: '#9ca3af', paddingTop: 8 }} />
              <Scatter name="No Spike" data={noSpikeScatter} fill="#4ade80" opacity={0.6} />
              <Scatter name="Spike" data={spikeScatter} fill="#f87171" opacity={0.8} />
            </ScatterChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Spike History table */}
      <div className="bg-gray-800 rounded-xl shadow-lg overflow-hidden">
        <div className="px-5 py-4 border-b border-gray-700">
          <h2 className="text-base font-semibold text-white">Spike History</h2>
          <p className="text-xs text-gray-400 mt-0.5">Historical spike events with prediction outcome</p>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-xs text-gray-400 uppercase tracking-wide border-b border-gray-700">
                <th className="px-4 py-3">Date</th>
                <th className="px-4 py-3">Region</th>
                <th className="px-4 py-3">Hour</th>
                <th className="px-4 py-3">Max Price</th>
                <th className="px-4 py-3">Duration</th>
                <th className="px-4 py-3">Cause</th>
                <th className="px-4 py-3">Predicted</th>
                <th className="px-4 py-3">Warning Lead Time</th>
              </tr>
            </thead>
            <tbody>
              {data.spike_history.map((s: SPPSpikeHistoryRecord, i) => (
                <tr key={i} className="border-b border-gray-700/50 hover:bg-gray-700/30">
                  <td className="px-4 py-2.5 text-gray-300">{s.date}</td>
                  <td className="px-4 py-2.5 font-semibold text-cyan-400">{s.region}</td>
                  <td className="px-4 py-2.5 text-gray-300">{s.hour}:00</td>
                  <td className="px-4 py-2.5 font-semibold text-red-400">
                    ${s.max_price_aud_mwh.toLocaleString()}/MWh
                  </td>
                  <td className="px-4 py-2.5 text-gray-300">{s.duration_intervals} intervals</td>
                  <td className="px-4 py-2.5 text-gray-300 text-xs">{s.cause}</td>
                  <td className="px-4 py-2.5">
                    {s.predicted ? (
                      <Badge label="Predicted" colourClass="bg-green-700" />
                    ) : (
                      <Badge label="Missed" colourClass="bg-red-800" />
                    )}
                  </td>
                  <td className="px-4 py-2.5 text-gray-300">
                    {s.warning_lead_time_minutes != null
                      ? `${s.warning_lead_time_minutes} min`
                      : <span className="text-gray-600">—</span>}
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
