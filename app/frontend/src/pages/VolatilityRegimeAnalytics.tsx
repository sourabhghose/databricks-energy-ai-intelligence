import { useEffect, useState } from 'react'
import {
  AreaChart,
  Area,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ReferenceLine,
} from 'recharts'
import { TrendingUp, AlertOctagon, DollarSign, Activity } from 'lucide-react'
import { getVolatilityRegimeDashboard, VolatilityRegimeDashboard } from '../api/client'

// ---- colour maps ----
const REGIME_COLORS: Record<string, string> = {
  LOW:     '#10b981',
  NORMAL:  '#6366f1',
  HIGH:    '#f59e0b',
  EXTREME: '#ef4444',
}

const REGIME_BG: Record<string, string> = {
  LOW:     'bg-green-900/40 border-green-700 text-green-300',
  NORMAL:  'bg-indigo-900/40 border-indigo-700 text-indigo-300',
  HIGH:    'bg-amber-900/40 border-amber-700 text-amber-300',
  EXTREME: 'bg-red-900/40 border-red-700 text-red-300',
}

const TRIGGER_COLORS: Record<string, string> = {
  HEATWAVE:               'bg-red-900/60 text-red-300 border border-red-700',
  GAS_SHORTAGE:           'bg-amber-900/60 text-amber-300 border border-amber-700',
  LOW_WIND:               'bg-cyan-900/60 text-cyan-300 border border-cyan-700',
  GENERATOR_OUTAGE:       'bg-orange-900/60 text-orange-300 border border-orange-700',
  INTERCONNECTOR_FAILURE: 'bg-purple-900/60 text-purple-300 border border-purple-700',
  MARKET_POWER:           'bg-pink-900/60 text-pink-300 border border-pink-700',
}

const HEDGE_REGIME_STYLES: Record<string, { border: string; text: string; accent: string }> = {
  LOW:     { border: 'border-green-700',  text: 'text-green-400',  accent: 'bg-green-900/30'  },
  NORMAL:  { border: 'border-indigo-700', text: 'text-indigo-400', accent: 'bg-indigo-900/30' },
  HIGH:    { border: 'border-amber-700',  text: 'text-amber-400',  accent: 'bg-amber-900/30'  },
  EXTREME: { border: 'border-red-700',    text: 'text-red-400',    accent: 'bg-red-900/30'    },
}

// Map probability to a Tailwind bg opacity class
function probToCell(pct: number): string {
  if (pct >= 60) return 'bg-red-800/70 text-red-200'
  if (pct >= 35) return 'bg-amber-800/60 text-amber-200'
  if (pct >= 15) return 'bg-indigo-800/50 text-indigo-200'
  if (pct > 0)  return 'bg-slate-700/50 text-slate-300'
  return 'bg-slate-800/30 text-slate-600'
}

function fmt(n: number, d = 0) {
  return n.toLocaleString('en-AU', { minimumFractionDigits: d, maximumFractionDigits: d })
}

// ---- derived builders ----
function buildTimelineData(data: VolatilityRegimeDashboard) {
  const nsw = data.regimes.filter(r => r.region === 'NSW1')
  const sa  = data.regimes.filter(r => r.region === 'SA1')
  const months = [...new Set(data.regimes.map(r => r.month))].sort()
  return months.map(m => {
    const nswRec = nsw.find(r => r.month === m)
    const saRec  = sa.find(r => r.month === m)
    return {
      month: m,
      nsw_vi: nswRec?.volatility_index ?? 0,
      sa_vi:  saRec?.volatility_index ?? 0,
      nsw_regime: nswRec?.regime ?? 'NORMAL',
      sa_regime:  saRec?.regime ?? 'NORMAL',
    }
  })
}

function buildTransitionMatrix(data: VolatilityRegimeDashboard) {
  const regimes = ['LOW', 'NORMAL', 'HIGH', 'EXTREME']
  const matrix: Record<string, Record<string, { pct: number; count: number; trigger: string }>> = {}
  for (const fr of regimes) {
    matrix[fr] = {}
    for (const to of regimes) {
      matrix[fr][to] = { pct: 0, count: 0, trigger: '—' }
    }
  }
  for (const t of data.transitions) {
    matrix[t.from_regime][t.to_regime] = {
      pct:     t.probability_pct,
      count:   t.transition_count,
      trigger: t.typical_trigger,
    }
  }
  return { matrix, regimes }
}

function buildClusterBarData(data: VolatilityRegimeDashboard) {
  return data.clusters.map(c => ({
    id:      c.cluster_id,
    region:  c.region,
    cost:    c.total_cost_impact_m_aud,
    maxP:    c.max_price,
    trigger: c.trigger,
  }))
}

// ---- KPI computation ----
function computeKpis(data: VolatilityRegimeDashboard) {
  const extremeMonths = data.regimes.filter(r => r.regime === 'EXTREME').length
  const highestVI     = Math.max(...data.regimes.map(r => r.volatility_index))
  const totalClusterCost = data.clusters.reduce((s, c) => s + c.total_cost_impact_m_aud, 0)
  const extremeHedge  = data.hedging.find(h => h.regime === 'EXTREME')
  const optimalCap    = extremeHedge?.cap_strike_optimal_aud ?? null
  const peak          = data.regimes.find(r => r.volatility_index === highestVI)
  const months        = [...new Set(data.regimes.map(r => r.month))].sort()
  const period        = months.length ? `${months[0]} to ${months[months.length - 1]}` : 'no data'
  return { extremeMonths, highestVI, totalClusterCost, optimalCap, peak, period }
}

export default function VolatilityRegimeAnalytics() {
  const [data, setData] = useState<VolatilityRegimeDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError]   = useState<string | null>(null)

  useEffect(() => {
    getVolatilityRegimeDashboard()
      .then(d => { setData(d); setLoading(false) })
      .catch(e => { setError(String(e)); setLoading(false) })
  }, [])

  if (loading) return (
    <div className="flex items-center justify-center h-64">
      <div className="animate-spin rounded-full h-10 w-10 border-b-2 border-indigo-400" />
    </div>
  )
  if (error || !data) return (
    <div className="p-6 text-red-400">Failed to load volatility regime data: {error}</div>
  )

  const kpi         = computeKpis(data)
  const timelineD   = buildTimelineData(data)
  const { matrix, regimes: rList } = buildTransitionMatrix(data)
  const barD        = buildClusterBarData(data)

  return (
    <div className="p-6 space-y-8 text-slate-100">
      {/* ---- Header ---- */}
      <div className="flex items-center gap-3">
        <TrendingUp className="text-indigo-400 w-7 h-7" />
        <div>
          <h1 className="text-2xl font-bold text-white">Wholesale Price Volatility Regime Analytics</h1>
          <p className="text-slate-400 text-sm mt-0.5">
            Regime detection and spike clustering from real AEMO NEMWEB 5-minute prices — {kpi.period}
          </p>
        </div>
      </div>

      {/* ---- KPI Cards ---- */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="bg-slate-800 border border-red-700/50 rounded-xl p-4">
          <div className="flex items-center gap-2 text-red-400 mb-2">
            <AlertOctagon className="w-5 h-5" />
            <span className="text-xs font-semibold uppercase tracking-wide">EXTREME Regime Months</span>
          </div>
          <p className="text-3xl font-bold text-white">{kpi.extremeMonths}</p>
          <p className="text-xs text-slate-400 mt-1">region-months across 5 NEM regions ({kpi.period})</p>
        </div>

        <div className="bg-slate-800 border border-amber-700/50 rounded-xl p-4">
          <div className="flex items-center gap-2 text-amber-400 mb-2">
            <Activity className="w-5 h-5" />
            <span className="text-xs font-semibold uppercase tracking-wide">Highest Volatility Index</span>
          </div>
          <p className="text-3xl font-bold text-white">{fmt(kpi.highestVI, 2)}</p>
          <p className="text-xs text-slate-400 mt-1">peak VI observed ({kpi.peak ? `${kpi.peak.region}, ${kpi.peak.month}` : 'n/a'})</p>
        </div>

        <div className="bg-slate-800 border border-orange-700/50 rounded-xl p-4">
          <div className="flex items-center gap-2 text-orange-400 mb-2">
            <DollarSign className="w-5 h-5" />
            <span className="text-xs font-semibold uppercase tracking-wide">Cluster Cost Impact</span>
          </div>
          <p className="text-3xl font-bold text-white">${fmt(kpi.totalClusterCost, 1)}M</p>
          <p className="text-xs text-slate-400 mt-1">energy cost above $300/MWh across {data.clusters.length} spike clusters</p>
        </div>

        <div className="bg-slate-800 border border-indigo-700/50 rounded-xl p-4">
          <div className="flex items-center gap-2 text-indigo-400 mb-2">
            <TrendingUp className="w-5 h-5" />
            <span className="text-xs font-semibold uppercase tracking-wide">Optimal Cap Strike</span>
          </div>
          <p className="text-3xl font-bold text-white">{kpi.optimalCap === null ? 'n/a' : `$${fmt(kpi.optimalCap)}/MWh`}</p>
          <p className="text-xs text-slate-400 mt-1">{kpi.optimalCap === null ? 'hedge optimisation model not deployed' : 'recommended strike in EXTREME regime'}</p>
        </div>
      </div>

      {/* ---- Regime Timeline Chart ---- */}
      <div className="bg-slate-800 rounded-xl p-5 border border-slate-700">
        <h2 className="text-lg font-semibold text-white mb-1">Volatility Index Timeline — NSW1 &amp; SA1 ({kpi.period})</h2>
        <p className="text-xs text-slate-400 mb-4">Monthly volatility index (price standard deviation ÷ |mean price|) by region; regimes are quartiles across all region-months.</p>
        <ResponsiveContainer width="100%" height={280}>
          <AreaChart data={timelineD} margin={{ top: 10, right: 20, left: 0, bottom: 0 }}>
            <defs>
              <linearGradient id="viNSW" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%"  stopColor="#6366f1" stopOpacity={0.5} />
                <stop offset="95%" stopColor="#6366f1" stopOpacity={0.0} />
              </linearGradient>
              <linearGradient id="viSA" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%"  stopColor="#ef4444" stopOpacity={0.5} />
                <stop offset="95%" stopColor="#ef4444" stopOpacity={0.0} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
            <XAxis dataKey="month" tick={{ fill: '#94a3b8', fontSize: 12 }} />
            <YAxis tick={{ fill: '#94a3b8', fontSize: 12 }} domain={[0, 'auto']} />
            <Tooltip
              contentStyle={{ background: '#1e293b', border: '1px solid #475569', borderRadius: 8 }}
              labelStyle={{ color: '#e2e8f0' }}
              formatter={(val: number, name: string) => [`${val.toFixed(2)}`, name]}
            />
            <Legend wrapperStyle={{ color: '#94a3b8', fontSize: 13 }} />
            <ReferenceLine y={0.5} stroke="#f59e0b" strokeDasharray="4 4" label={{ value: 'HIGH threshold', fill: '#f59e0b', fontSize: 11 }} />
            <ReferenceLine y={1.0} stroke="#ef4444" strokeDasharray="4 4" label={{ value: 'EXTREME threshold', fill: '#ef4444', fontSize: 11 }} />
            <Area type="monotone" dataKey="nsw_vi" name="NSW1 VI" stroke="#6366f1" fill="url(#viNSW)" strokeWidth={2} dot={{ fill: '#6366f1', r: 3 }} />
            <Area type="monotone" dataKey="sa_vi"  name="SA1 VI"  stroke="#ef4444" fill="url(#viSA)"  strokeWidth={2} dot={{ fill: '#ef4444', r: 3 }} />
          </AreaChart>
        </ResponsiveContainer>
      </div>

      {/* ---- Volatility Cluster Cost Bar Chart ---- */}
      <div className="bg-slate-800 rounded-xl p-5 border border-slate-700">
        <h2 className="text-lg font-semibold text-white mb-1">Cluster Event Cost Impact (M AUD)</h2>
        <p className="text-xs text-slate-400 mb-4">{data.clusters.length} runs of consecutive days with a &gt;$300/MWh interval, ranked by energy cost above $300/MWh.</p>
        <ResponsiveContainer width="100%" height={220}>
          <BarChart data={barD} margin={{ top: 5, right: 20, left: 0, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
            <XAxis dataKey="id" tick={{ fill: '#94a3b8', fontSize: 12 }} />
            <YAxis tick={{ fill: '#94a3b8', fontSize: 12 }} unit="M" />
            <Tooltip
              contentStyle={{ background: '#1e293b', border: '1px solid #475569', borderRadius: 8 }}
              labelStyle={{ color: '#e2e8f0' }}
              formatter={(val: number) => [`$${fmt(val, 1)}M AUD`, 'Cost Impact']}
            />
            <Bar dataKey="cost" name="Cost Impact (M AUD)" radius={[4, 4, 0, 0]} fill="#f59e0b" />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* ---- Cluster Event Table ---- */}
      <div className="bg-slate-800 rounded-xl p-5 border border-slate-700">
        <h2 className="text-lg font-semibold text-white mb-4">Volatility Cluster Events</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-slate-400 border-b border-slate-700">
                <th className="text-left pb-2 pr-4">Event ID</th>
                <th className="text-left pb-2 pr-4">Region</th>
                <th className="text-left pb-2 pr-4">Period</th>
                <th className="text-left pb-2 pr-4">Trigger</th>
                <th className="text-right pb-2 pr-4">Duration</th>
                <th className="text-right pb-2 pr-4">Max Price (AUD)</th>
                <th className="text-right pb-2 pr-4">Avg Price (AUD)</th>
                <th className="text-right pb-2">Cost Impact</th>
              </tr>
            </thead>
            <tbody>
              {data.clusters.map(c => (
                <tr key={c.cluster_id} className="border-b border-slate-700/50 hover:bg-slate-700/30 transition-colors">
                  <td className="py-3 pr-4 font-mono text-slate-300 text-xs">{c.cluster_id}</td>
                  <td className="py-3 pr-4">
                    <span className="bg-slate-700 text-slate-200 px-2 py-0.5 rounded text-xs font-medium">{c.region}</span>
                  </td>
                  <td className="py-3 pr-4 text-slate-300 text-xs">{c.start_date} → {c.end_date}</td>
                  <td className="py-3 pr-4">
                    <span className={`px-2 py-0.5 rounded text-xs font-semibold ${TRIGGER_COLORS[c.trigger] ?? 'bg-slate-700 text-slate-300'}`}>
                      {c.trigger.replace(/_/g, ' ')}
                    </span>
                  </td>
                  <td className="py-3 pr-4 text-right text-slate-200">{c.duration_days}d</td>
                  <td className="py-3 pr-4 text-right text-red-300 font-semibold">${fmt(c.max_price)}</td>
                  <td className="py-3 pr-4 text-right text-amber-300">${fmt(c.avg_price)}</td>
                  <td className="py-3 text-right text-white font-bold">${fmt(c.total_cost_impact_m_aud, 1)}M</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* ---- Hedging Implication Cards ---- */}
      <div>
        <h2 className="text-lg font-semibold text-white mb-4">Hedging Implications by Regime</h2>
        {data.hedging.length === 0 && (
          <p className="text-sm text-slate-400">No hedge optimisation model is deployed in this build, so no regime-specific hedging recommendations are shown.</p>
        )}
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
          {data.hedging.map(h => {
            const style = HEDGE_REGIME_STYLES[h.regime] ?? HEDGE_REGIME_STYLES['NORMAL']
            return (
              <div key={h.regime} className={`bg-slate-800 rounded-xl p-5 border ${style.border}`}>
                <div className="flex items-center gap-2 mb-3">
                  <span className={`px-2 py-0.5 rounded-full text-xs font-bold uppercase border ${REGIME_BG[h.regime]}`}>{h.regime}</span>
                </div>
                <div className="space-y-2.5 text-sm">
                  <div className="flex justify-between">
                    <span className="text-slate-400">Hedge Ratio</span>
                    <span className={`font-bold ${style.text}`}>{h.recommended_hedge_ratio_pct}%</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-slate-400">Cap Strike</span>
                    <span className="text-white font-semibold">${fmt(h.cap_strike_optimal_aud)}/MWh</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-slate-400">Swap Volume</span>
                    <span className="text-slate-200">{h.swap_volume_twh_yr} TWh/yr</span>
                  </div>
                  <div className={`mt-2 p-2 rounded ${style.accent}`}>
                    <div className="flex justify-between text-xs mb-1">
                      <span className="text-slate-400">VaR 95%</span>
                      <span className={`font-bold ${style.text}`}>${fmt(h.var_95_m_aud, 1)}M</span>
                    </div>
                    <div className="flex justify-between text-xs">
                      <span className="text-slate-400">Hedging Cost</span>
                      <span className="text-slate-200">${fmt(h.cost_of_hedging_m_aud_twh, 1)}M/TWh</span>
                    </div>
                  </div>
                </div>
              </div>
            )
          })}
        </div>
      </div>

      {/* ---- Regime Transition Probability Matrix ---- */}
      <div className="bg-slate-800 rounded-xl p-5 border border-slate-700">
        <h2 className="text-lg font-semibold text-white mb-1">Regime Transition Probability Matrix</h2>
        <p className="text-xs text-slate-400 mb-4">
          Row = current regime, Column = next regime. Cell shows transition probability (%). Darker red = higher probability.
        </p>
        <div className="overflow-x-auto">
          <table className="text-sm border-collapse">
            <thead>
              <tr>
                <th className="p-3 text-slate-400 text-left font-medium">From \ To</th>
                {rList.map(to => (
                  <th key={to} className="p-3 text-center min-w-[130px]">
                    <span className={`px-2 py-0.5 rounded text-xs font-bold border ${REGIME_BG[to]}`}>{to}</span>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rList.map(fr => (
                <tr key={fr} className="border-t border-slate-700">
                  <td className="p-3">
                    <span className={`px-2 py-0.5 rounded text-xs font-bold border ${REGIME_BG[fr]}`}>{fr}</span>
                  </td>
                  {rList.map(to => {
                    const cell = matrix[fr][to]
                    const isSelf = fr === to
                    return (
                      <td key={to} className="p-2 text-center">
                        {isSelf ? (
                          <span className="text-slate-600 text-xs">—</span>
                        ) : (
                          <div
                            className={`rounded px-2 py-2 ${probToCell(cell.pct)}`}
                            title={`${fr} → ${to}: ${cell.count} events, trigger: ${cell.trigger}`}
                          >
                            <div className="font-bold text-sm">{cell.pct > 0 ? `${fmt(cell.pct, 1)}%` : '—'}</div>
                            {cell.count > 0 && (
                              <div className="text-xs opacity-70">{cell.count} events</div>
                            )}
                          </div>
                        )}
                      </td>
                    )
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="mt-4 flex flex-wrap gap-3 text-xs text-slate-400">
          <span className="flex items-center gap-1.5"><span className="inline-block w-4 h-3 rounded bg-red-800/70" /> High (&ge;60%)</span>
          <span className="flex items-center gap-1.5"><span className="inline-block w-4 h-3 rounded bg-amber-800/60" /> Medium (35–59%)</span>
          <span className="flex items-center gap-1.5"><span className="inline-block w-4 h-3 rounded bg-indigo-800/50" /> Low (15–34%)</span>
          <span className="flex items-center gap-1.5"><span className="inline-block w-4 h-3 rounded bg-slate-700/50" /> Rare (&lt;15%)</span>
        </div>
      </div>

      {/* ---- Regional Summary Table ---- */}
      <div className="bg-slate-800 rounded-xl p-5 border border-slate-700">
        <h2 className="text-lg font-semibold text-white mb-4">Regional Regime Summary ({kpi.period})</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-slate-400 border-b border-slate-700">
                <th className="text-left pb-2 pr-4">Region</th>
                <th className="text-right pb-2 pr-4">LOW months</th>
                <th className="text-right pb-2 pr-4">NORMAL months</th>
                <th className="text-right pb-2 pr-4">HIGH months</th>
                <th className="text-right pb-2 pr-4">EXTREME months</th>
                <th className="text-right pb-2 pr-4">Avg VI</th>
                <th className="text-right pb-2">Avg Price (AUD)</th>
              </tr>
            </thead>
            <tbody>
              {['NSW1','VIC1','QLD1','SA1','TAS1'].map(region => {
                const recs = data.regimes.filter(r => r.region === region)
                const counts = { LOW: 0, NORMAL: 0, HIGH: 0, EXTREME: 0 }
                recs.forEach(r => { counts[r.regime as keyof typeof counts]++ })
                const avgVI   = recs.reduce((s, r) => s + r.volatility_index, 0) / (recs.length || 1)
                const avgPrc  = recs.reduce((s, r) => s + r.avg_price_aud_mwh, 0) / (recs.length || 1)
                return (
                  <tr key={region} className="border-b border-slate-700/50 hover:bg-slate-700/30 transition-colors">
                    <td className="py-3 pr-4">
                      <span className="bg-slate-700 text-slate-200 px-2 py-0.5 rounded text-xs font-medium">{region}</span>
                    </td>
                    <td className="py-3 pr-4 text-right text-green-400">{counts.LOW}</td>
                    <td className="py-3 pr-4 text-right text-indigo-400">{counts.NORMAL}</td>
                    <td className="py-3 pr-4 text-right text-amber-400">{counts.HIGH}</td>
                    <td className="py-3 pr-4 text-right text-red-400 font-bold">{counts.EXTREME}</td>
                    <td className="py-3 pr-4 text-right text-slate-200">{fmt(avgVI, 2)}</td>
                    <td className="py-3 text-right text-white font-semibold">${fmt(avgPrc, 0)}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>

      <p className="text-xs text-slate-500 text-right">
        Data: AEMO NEMWEB 5-minute prices (gold.nem_prices_5min), {kpi.period}. Last updated: {new Date(data.timestamp).toLocaleString('en-AU')}
      </p>
    </div>
  )
}
