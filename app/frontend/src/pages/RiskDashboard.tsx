import { useEffect, useState } from 'react'
import { Shield, RefreshCw, AlertTriangle, TrendingUp, DollarSign, Gauge } from 'lucide-react'
import {
  LineChart,
  Line,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  Cell,
} from 'recharts'
import { riskApi, riskLimitsApi, advancedRiskApi } from '../api/client'
import type {
  MtMResult,
  PnLAttribution,
  VaRResult,
  GreeksResult,
  CreditExposure,
  LimitMonitorResult,
} from '../api/client'

const TABS = ['MtM & P&L', 'VaR & Greeks', 'Credit Risk', 'Advanced VaR', 'Vol Surface', 'Scenarios'] as const
type Tab = typeof TABS[number]

const fmt = (n: number) =>
  n >= 1_000_000 ? `$${(n / 1_000_000).toFixed(1)}M`
  : n >= 1_000 ? `$${(n / 1_000).toFixed(0)}K`
  : n <= -1_000_000 ? `-$${(Math.abs(n) / 1_000_000).toFixed(1)}M`
  : n <= -1_000 ? `-$${(Math.abs(n) / 1_000).toFixed(0)}K`
  : `$${n.toFixed(0)}`

const fmtPct = (n: number) => `${(n * 100).toFixed(1)}%`

const tooltipStyle = { backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }
const tooltipLabelStyle = { color: '#d1d5db' }

export default function RiskDashboard() {
  const [tab, setTab] = useState<Tab>('MtM & P&L')
  const [loading, setLoading] = useState(false)
  const [portfolios, setPortfolios] = useState<Array<{ portfolio_id: string; name: string }>>([])
  const [selectedPortfolio, setSelectedPortfolio] = useState<string>('')

  // MtM state
  const [mtmResults, setMtmResults] = useState<MtMResult[]>([])
  const [mtmTotal, setMtmTotal] = useState(0)
  const [mtmTradesValued, setMtmTradesValued] = useState(0)
  const [mtmDate, setMtmDate] = useState('')
  const [pnlAttribution, setPnlAttribution] = useState<PnLAttribution[]>([])
  const [mtmHistory, setMtmHistory] = useState<Array<{ valuation_date: string; total_mtm: number }>>([])
  const [runMsg, setRunMsg] = useState<string | null>(null)

  // VaR state
  const [varResults, setVarResults] = useState<VaRResult[]>([])
  const [greeksResults, setGreeksResults] = useState<GreeksResult[]>([])
  const [varHistory, setVarHistory] = useState<Array<{ valuation_date: string; var_95_1d: number; var_99_1d: number }>>([])

  // Credit state
  const [creditExposures, setCreditExposures] = useState<CreditExposure[]>([])
  const [creditAlerts, setCreditAlerts] = useState<CreditExposure[]>([])

  useEffect(() => {
    ;(async () => {
      try {
        const res = await fetch('/api/deals/portfolios')
        const data = await res.json()
        setPortfolios(data.portfolios || [])
        if (data.portfolios?.length) setSelectedPortfolio(data.portfolios[0].portfolio_id)
      } catch { /* ignore */ }
    })()
  }, [])

  useEffect(() => {
    if (!selectedPortfolio && tab !== 'Credit Risk') return
    const load = async () => {
      setLoading(true)
      try {
        if (tab === 'MtM & P&L') {
          const [latest, attr] = await Promise.all([
            riskApi.getMtmLatest(selectedPortfolio),
            riskApi.getPnlAttribution(selectedPortfolio),
          ])
          setMtmResults(latest.results || [])
          setMtmTotal(latest.total_mtm || 0)
          setMtmTradesValued(latest.trades_valued || 0)
          setMtmDate(latest.valuation_date || '')
          setPnlAttribution(attr.attribution || [])
          try { setMtmHistory((await riskApi.getMtmHistory(selectedPortfolio, 30)).history || []) } catch { /* */ }
        } else if (tab === 'VaR & Greeks') {
          const [vl, gr] = await Promise.all([
            riskApi.getVarLatest(selectedPortfolio),
            riskApi.getGreeks(selectedPortfolio),
          ])
          setVarResults(vl.metrics || [])
          setGreeksResults(gr.greeks || [])
          try { setVarHistory((await riskApi.getVarHistory(selectedPortfolio, 30)).history || []) } catch { /* */ }
        } else {
          const [summary, alerts] = await Promise.all([riskApi.getCreditSummary(), riskApi.getCreditAlerts()])
          setCreditExposures(summary.exposures || [])
          setCreditAlerts(alerts.alerts || [])
        }
      } catch { /* ignore */ }
      setLoading(false)
    }
    load()
  }, [tab, selectedPortfolio])

  const handleRunMtm = async () => {
    setRunMsg('Running MtM valuation...')
    try {
      const result = await riskApi.runMtm(selectedPortfolio || undefined)
      setRunMsg(`Valued ${result.trades_valued} trades. Total MtM: ${fmt(result.total_mtm)}`)
      const latest = await riskApi.getMtmLatest(selectedPortfolio)
      setMtmResults(latest.results || [])
      setMtmTotal(latest.total_mtm || 0)
      setMtmTradesValued(latest.trades_valued || 0)
    } catch { setRunMsg('MtM run failed') }
    setTimeout(() => setRunMsg(null), 5000)
  }

  const handleCalcVar = async () => {
    if (!selectedPortfolio) return
    setLoading(true)
    try {
      await riskApi.calculateVar(selectedPortfolio)
      const [vl, gr] = await Promise.all([riskApi.getVarLatest(selectedPortfolio), riskApi.getGreeks(selectedPortfolio)])
      setVarResults(vl.metrics || [])
      setGreeksResults(gr.greeks || [])
    } catch { /* */ }
    setLoading(false)
  }

  const handleCalcCredit = async () => {
    setLoading(true)
    try {
      await riskApi.calculateCredit()
      const [summary, alerts] = await Promise.all([riskApi.getCreditSummary(), riskApi.getCreditAlerts()])
      setCreditExposures(summary.exposures || [])
      setCreditAlerts(alerts.alerts || [])
    } catch { /* */ }
    setLoading(false)
  }

  const totalPnl = pnlAttribution.reduce((s, a) => s + (a.total_pnl || 0), 0)
  const totalUnrealised = mtmResults.reduce((s, r) => s + (r.unrealised_pnl || 0), 0)

  const waterfallData = (() => {
    const totals = { price: 0, volume: 0, newTrades: 0, timeDecay: 0 }
    for (const a of pnlAttribution) {
      totals.price += a.price_effect || 0
      totals.volume += a.volume_effect || 0
      totals.newTrades += a.new_trades_effect || 0
      totals.timeDecay += a.time_decay || 0
    }
    return [
      { name: 'Price', value: Math.round(totals.price), fill: totals.price >= 0 ? '#22c55e' : '#ef4444' },
      { name: 'Volume', value: Math.round(totals.volume), fill: totals.volume >= 0 ? '#22c55e' : '#ef4444' },
      { name: 'New Trades', value: Math.round(totals.newTrades), fill: totals.newTrades >= 0 ? '#22c55e' : '#ef4444' },
      { name: 'Time Decay', value: Math.round(totals.timeDecay), fill: totals.timeDecay >= 0 ? '#22c55e' : '#ef4444' },
      { name: 'Total', value: Math.round(totalPnl), fill: '#3b82f6' },
    ]
  })()

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between flex-wrap gap-4">
        <div className="flex items-center gap-3">
          <Shield className="w-6 h-6 text-blue-400" />
          <h1 className="text-xl font-bold text-white">Risk Dashboard</h1>
        </div>
        <div className="flex items-center gap-3">
          <select
            value={selectedPortfolio}
            onChange={e => setSelectedPortfolio(e.target.value)}
            className="text-sm bg-gray-700 text-gray-200 border border-gray-600 rounded px-3 py-1.5"
          >
            <option value="">All Portfolios</option>
            {portfolios.map(p => (
              <option key={p.portfolio_id} value={p.portfolio_id}>{p.name}</option>
            ))}
          </select>
        </div>
      </div>

      {/* Limit Monitor */}
      <LimitMonitorSection portfolioId={selectedPortfolio} />

      {/* Tabs */}
      <div className="flex gap-1 border-b border-gray-700">
        {TABS.map(t => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`px-5 py-2 text-sm font-medium border-b-2 -mb-px transition-colors ${
              tab === t
                ? 'border-blue-500 text-blue-400'
                : 'border-transparent text-gray-400 hover:text-gray-200'
            }`}
          >
            {t}
          </button>
        ))}
        {loading && <span className="ml-auto text-xs text-gray-500 self-center">Loading...</span>}
      </div>

      {/* Tab 1: MtM & P&L */}
      {tab === 'MtM & P&L' && (
        <div className="space-y-6">
          {/* Actions */}
          <div className="flex items-center gap-3">
            <button
              onClick={handleRunMtm}
              className="flex items-center gap-1.5 text-sm bg-blue-600 hover:bg-blue-500 text-white rounded px-4 py-1.5"
            >
              <RefreshCw className="w-3.5 h-3.5" /> Run MtM
            </button>
            {runMsg && <span className="text-xs text-green-400">{runMsg}</span>}
          </div>

          {/* KPI Cards */}
          <div className="grid grid-cols-4 gap-4">
            {[
              { label: 'Total MtM', value: fmt(mtmTotal), Icon: DollarSign, color: 'text-blue-400' },
              { label: 'Daily P&L', value: fmt(totalPnl), Icon: TrendingUp, color: totalPnl >= 0 ? 'text-green-400' : 'text-red-400' },
              { label: 'Unrealised P&L', value: fmt(totalUnrealised), Icon: TrendingUp, color: totalUnrealised >= 0 ? 'text-green-400' : 'text-red-400' },
              { label: 'Trades Valued', value: String(mtmTradesValued), Icon: Shield, color: 'text-purple-400' },
            ].map((kpi, i) => (
              <div key={i} className="bg-gray-800 rounded-lg p-4">
                <div className="flex items-center gap-2 mb-2">
                  <kpi.Icon className={`w-4 h-4 ${kpi.color}`} />
                  <span className="text-xs text-gray-400 uppercase tracking-wide">{kpi.label}</span>
                </div>
                <div className={`text-2xl font-bold ${kpi.color}`}>{kpi.value}</div>
              </div>
            ))}
          </div>

          {/* Charts row */}
          <div className="grid grid-cols-2 gap-4">
            {/* P&L Attribution */}
            <div className="bg-gray-800 rounded-lg p-4">
              <h3 className="text-sm font-semibold text-gray-200 uppercase tracking-wide mb-4">P&L Attribution</h3>
              <ResponsiveContainer width="100%" height={250}>
                <BarChart data={waterfallData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                  <XAxis dataKey="name" tick={{ fill: '#9ca3af', fontSize: 12 }} />
                  <YAxis tick={{ fill: '#9ca3af', fontSize: 12 }} tickFormatter={v => fmt(v)} />
                  <Tooltip contentStyle={tooltipStyle} labelStyle={tooltipLabelStyle} formatter={(v: number) => [fmt(v), 'P&L']} />
                  <Bar dataKey="value">
                    {waterfallData.map((e, i) => <Cell key={i} fill={e.fill} />)}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>

            {/* MtM History */}
            <div className="bg-gray-800 rounded-lg p-4">
              <h3 className="text-sm font-semibold text-gray-200 uppercase tracking-wide mb-4">MtM History</h3>
              {mtmHistory.length > 0 ? (
                <ResponsiveContainer width="100%" height={250}>
                  <LineChart data={mtmHistory}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                    <XAxis dataKey="valuation_date" tick={{ fill: '#9ca3af', fontSize: 11 }} />
                    <YAxis tick={{ fill: '#9ca3af', fontSize: 12 }} tickFormatter={v => fmt(v)} />
                    <Tooltip contentStyle={tooltipStyle} labelStyle={tooltipLabelStyle} formatter={(v: number) => [fmt(v), 'MtM']} />
                    <Line type="monotone" dataKey="total_mtm" stroke="#3b82f6" strokeWidth={2} dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              ) : (
                <div className="h-[250px] flex items-center justify-center text-gray-500 text-sm">
                  Run MtM to populate history
                </div>
              )}
            </div>
          </div>

          {/* Trade-Level MtM Table */}
          <div className="bg-gray-800 rounded-lg p-4">
            <h3 className="text-sm font-semibold text-gray-200 uppercase tracking-wide mb-4">Trade-Level MtM</h3>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-700">
                    {['Region', 'Type', 'Dir', 'Vol MW', 'Contract $/MWh', 'Market $/MWh', 'MtM $', 'Days', 'DF'].map(h => (
                      <th key={h} className="text-left px-3 py-2 text-xs text-gray-400 font-medium uppercase tracking-wide">{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {mtmResults.slice(0, 30).map((r, i) => (
                    <tr key={i} className="border-b border-gray-700/50 hover:bg-gray-700/30">
                      <td className="px-3 py-2 text-gray-200">{r.region}</td>
                      <td className="px-3 py-2 text-gray-300">{r.trade_type}</td>
                      <td className={`px-3 py-2 font-medium ${r.direction === 'BUY' ? 'text-green-400' : 'text-red-400'}`}>{r.direction}</td>
                      <td className="px-3 py-2 text-gray-300">{r.volume_mw}</td>
                      <td className="px-3 py-2 text-gray-300">${r.contract_price?.toFixed(2)}</td>
                      <td className="px-3 py-2 text-gray-300">${r.market_price?.toFixed(2)}</td>
                      <td className={`px-3 py-2 font-bold ${r.mtm_value >= 0 ? 'text-green-400' : 'text-red-400'}`}>{fmt(r.mtm_value)}</td>
                      <td className="px-3 py-2 text-gray-400">{r.remaining_days}</td>
                      <td className="px-3 py-2 text-gray-400">{r.discount_factor?.toFixed(4)}</td>
                    </tr>
                  ))}
                  {mtmResults.length === 0 && (
                    <tr><td colSpan={9} className="px-3 py-8 text-center text-gray-500">No MtM data — click "Run MtM" to value trades</td></tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* Tab 2: VaR & Greeks */}
      {tab === 'VaR & Greeks' && (
        <div className="space-y-6">
          <div className="flex items-center gap-3">
            <button
              onClick={handleCalcVar}
              className="flex items-center gap-1.5 text-sm bg-purple-600 hover:bg-purple-500 text-white rounded px-4 py-1.5"
            >
              <RefreshCw className="w-3.5 h-3.5" /> Calculate VaR
            </button>
          </div>

          {/* VaR Cards */}
          {(() => {
            const totals = varResults.reduce((acc, v) => ({
              var_95_1d: acc.var_95_1d + (v.var_95_1d || 0),
              var_99_1d: acc.var_99_1d + (v.var_99_1d || 0),
              var_95_10d: acc.var_95_10d + (v.var_95_10d || 0),
              var_99_10d: acc.var_99_10d + (v.var_99_10d || 0),
            }), { var_95_1d: 0, var_99_1d: 0, var_95_10d: 0, var_99_10d: 0 })
            return (
              <div className="grid grid-cols-4 gap-4">
                {[
                  { label: 'VaR 95% 1-Day', value: fmt(totals.var_95_1d), color: 'text-amber-400' },
                  { label: 'VaR 99% 1-Day', value: fmt(totals.var_99_1d), color: 'text-red-400' },
                  { label: 'VaR 95% 10-Day', value: fmt(totals.var_95_10d), color: 'text-amber-400' },
                  { label: 'VaR 99% 10-Day', value: fmt(totals.var_99_10d), color: 'text-red-400' },
                ].map((kpi, i) => (
                  <div key={i} className="bg-gray-800 rounded-lg p-4">
                    <span className="text-xs text-gray-400 uppercase tracking-wide">{kpi.label}</span>
                    <div className={`text-2xl font-bold mt-2 ${kpi.color}`}>{kpi.value}</div>
                  </div>
                ))}
              </div>
            )
          })()}

          <div className="grid grid-cols-2 gap-4">
            {/* VaR History */}
            <div className="bg-gray-800 rounded-lg p-4">
              <h3 className="text-sm font-semibold text-gray-200 uppercase tracking-wide mb-4">VaR History</h3>
              {varHistory.length > 0 ? (
                <ResponsiveContainer width="100%" height={250}>
                  <LineChart data={varHistory}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                    <XAxis dataKey="valuation_date" tick={{ fill: '#9ca3af', fontSize: 11 }} />
                    <YAxis tick={{ fill: '#9ca3af', fontSize: 12 }} tickFormatter={v => fmt(v)} />
                    <Tooltip contentStyle={tooltipStyle} labelStyle={tooltipLabelStyle} formatter={(v: number) => [fmt(v)]} />
                    <Legend wrapperStyle={{ color: '#d1d5db' }} />
                    <Line type="monotone" dataKey="var_95_1d" name="95% 1d" stroke="#f59e0b" strokeWidth={2} dot={false} />
                    <Line type="monotone" dataKey="var_99_1d" name="99% 1d" stroke="#ef4444" strokeWidth={2} dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              ) : (
                <div className="h-[250px] flex items-center justify-center text-gray-500 text-sm">
                  Calculate VaR to populate history
                </div>
              )}
            </div>

            {/* Greeks Table */}
            <div className="bg-gray-800 rounded-lg p-4">
              <h3 className="text-sm font-semibold text-gray-200 uppercase tracking-wide mb-4">Portfolio Greeks</h3>
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-700">
                    {['Region', 'Delta (MW)', 'Gamma', 'Vega', 'Theta', 'Vol'].map(h => (
                      <th key={h} className="text-left px-3 py-2 text-xs text-gray-400 font-medium uppercase tracking-wide">{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {greeksResults.map((g, i) => (
                    <tr key={i} className="border-b border-gray-700/50 hover:bg-gray-700/30">
                      <td className="px-3 py-2 text-gray-200 font-medium">{g.region}</td>
                      <td className={`px-3 py-2 font-medium ${g.delta_mw >= 0 ? 'text-green-400' : 'text-red-400'}`}>{g.delta_mw?.toFixed(1)}</td>
                      <td className="px-3 py-2 text-gray-300">{g.gamma?.toFixed(4)}</td>
                      <td className="px-3 py-2 text-gray-300">{g.vega?.toFixed(1)}</td>
                      <td className="px-3 py-2 text-red-400">{g.theta?.toFixed(1)}</td>
                      <td className="px-3 py-2 text-gray-300">{fmtPct(g.volatility_annual || 0)}</td>
                    </tr>
                  ))}
                  {greeksResults.length === 0 && (
                    <tr><td colSpan={6} className="px-3 py-8 text-center text-gray-500">Calculate VaR to see Greeks</td></tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* Tab 3: Credit Risk */}
      {tab === 'Credit Risk' && (
        <div className="space-y-6">
          <div className="flex items-center gap-3">
            <button
              onClick={handleCalcCredit}
              className="flex items-center gap-1.5 text-sm bg-red-600 hover:bg-red-500 text-white rounded px-4 py-1.5"
            >
              <RefreshCw className="w-3.5 h-3.5" /> Recalculate
            </button>
          </div>

          {/* Alerts */}
          {creditAlerts.length > 0 && (
            <div className="space-y-2">
              {creditAlerts.map((a, i) => (
                <div
                  key={i}
                  className={`flex items-center gap-3 px-4 py-3 rounded-lg border ${
                    a.alert_level === 'CRITICAL'
                      ? 'bg-red-900/30 border-red-700'
                      : 'bg-amber-900/30 border-amber-700'
                  }`}
                >
                  <AlertTriangle className={`w-4 h-4 ${a.alert_level === 'CRITICAL' ? 'text-red-400' : 'text-amber-400'}`} />
                  <span className={`font-semibold text-sm ${a.alert_level === 'CRITICAL' ? 'text-red-400' : 'text-amber-400'}`}>
                    {a.alert_level}
                  </span>
                  <span className="text-gray-200 text-sm">{a.counterparty_name}</span>
                  <span className="ml-auto text-sm text-gray-300 font-medium">
                    Utilisation: {fmtPct(a.credit_utilization)} | Exposure: {fmt(a.current_exposure)}
                  </span>
                </div>
              ))}
            </div>
          )}

          {/* Credit Utilization Bars */}
          <div className="bg-gray-800 rounded-lg p-4">
            <h3 className="text-sm font-semibold text-gray-200 uppercase tracking-wide mb-5">Credit Utilisation by Counterparty</h3>
            {creditExposures.map((cp, i) => {
              const pct = Math.min(cp.credit_utilization * 100, 100)
              const barColor = cp.alert_level === 'CRITICAL' ? 'bg-red-500' : cp.alert_level === 'WARNING' ? 'bg-amber-500' : 'bg-green-500'
              return (
                <div key={i} className="mb-4">
                  <div className="flex justify-between text-sm mb-1">
                    <span className="text-gray-200 font-medium">{cp.counterparty_name}</span>
                    <span className="text-gray-400">{fmtPct(cp.credit_utilization)} ({fmt(cp.current_exposure)} / {fmt(cp.credit_limit)})</span>
                  </div>
                  <div className="h-2.5 bg-gray-700 rounded-full overflow-hidden">
                    <div className={`h-full ${barColor} rounded-full transition-all duration-300`} style={{ width: `${pct}%` }} />
                  </div>
                </div>
              )
            })}
            {creditExposures.length === 0 && (
              <div className="py-8 text-center text-gray-500 text-sm">Click "Recalculate" to compute credit exposures</div>
            )}
          </div>

          {/* Exposure Aging Table */}
          <div className="bg-gray-800 rounded-lg p-4">
            <h3 className="text-sm font-semibold text-gray-200 uppercase tracking-wide mb-4">Exposure Aging</h3>
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-700">
                  {['Counterparty', 'Rating', '0-30d', '30-90d', '90+d', 'Current', 'PFE', 'Limit', 'Alert'].map(h => (
                    <th key={h} className="text-left px-3 py-2 text-xs text-gray-400 font-medium uppercase tracking-wide">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {creditExposures.map((cp, i) => (
                  <tr key={i} className="border-b border-gray-700/50 hover:bg-gray-700/30">
                    <td className="px-3 py-2 text-gray-200 font-medium">{cp.counterparty_name}</td>
                    <td className="px-3 py-2 text-gray-400">{cp.credit_rating || '-'}</td>
                    <td className="px-3 py-2 text-gray-300">{fmt(cp.exposure_current_bucket)}</td>
                    <td className="px-3 py-2 text-gray-300">{fmt(cp.exposure_30_90_bucket)}</td>
                    <td className="px-3 py-2 text-gray-300">{fmt(cp.exposure_90plus_bucket)}</td>
                    <td className="px-3 py-2 text-white font-bold">{fmt(cp.current_exposure)}</td>
                    <td className="px-3 py-2 text-gray-300">{fmt(cp.potential_future_exposure)}</td>
                    <td className="px-3 py-2 text-gray-300">{fmt(cp.credit_limit)}</td>
                    <td className="px-3 py-2">
                      <span className={`inline-block px-2 py-0.5 rounded-full text-xs font-semibold ${
                        cp.alert_level === 'CRITICAL' ? 'bg-red-900/50 text-red-400'
                        : cp.alert_level === 'WARNING' ? 'bg-amber-900/50 text-amber-400'
                        : 'bg-green-900/50 text-green-400'
                      }`}>
                        {cp.alert_level}
                      </span>
                    </td>
                  </tr>
                ))}
                {creditExposures.length === 0 && (
                  <tr><td colSpan={9} className="px-3 py-8 text-center text-gray-500">No credit data</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Tab 4: Advanced VaR (Phase 3) */}
      {tab === 'Advanced VaR' && <AdvancedVarSection portfolioId={selectedPortfolio} />}

      {/* Tab 5: Vol Surface (Phase 3) */}
      {tab === 'Vol Surface' && <VolSurfaceSection />}

      {/* Tab 6: Scenarios (Phase 3) */}
      {tab === 'Scenarios' && <ScenarioLibrarySection />}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Limit Monitor Section
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Advanced VaR Section (Phase 3 WS2)
// ---------------------------------------------------------------------------

function AdvancedVarSection({ portfolioId }: { portfolioId: string }) {
  const [histVar, setHistVar] = useState<any>(null)
  const [mcVar, setMcVar] = useState<any>(null)
  const [loading, setLoading] = useState(false)

  const load = async () => {
    setLoading(true)
    try {
      const [h, m] = await Promise.all([
        advancedRiskApi.historicalVar(portfolioId || undefined, 10, 0.95),
        advancedRiskApi.monteCarloVar(portfolioId || undefined, 10, 0.95),
      ])
      setHistVar(h)
      setMcVar(m)
    } catch { /* */ }
    setLoading(false)
  }

  useEffect(() => { load() }, [portfolioId])

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <button onClick={load} className="flex items-center gap-1.5 text-sm bg-amber-600 hover:bg-amber-500 text-white rounded px-4 py-1.5">
          <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} /> Recalculate
        </button>
      </div>
      <div className="grid grid-cols-2 gap-4">
        <div className="bg-gray-800 rounded-lg p-4">
          <h3 className="text-sm font-semibold text-gray-200 uppercase tracking-wide mb-4">Historical VaR</h3>
          {histVar ? (
            <div className="space-y-3">
              <div className="flex justify-between"><span className="text-gray-400">Method</span><span className="text-white">{histVar.method}</span></div>
              <div className="flex justify-between"><span className="text-gray-400">Window</span><span className="text-white">{histVar.window_days} days</span></div>
              <div className="flex justify-between"><span className="text-gray-400">VaR (95%)</span><span className="text-amber-400 font-bold">{fmt(histVar.var_amount || 0)}</span></div>
              <div className="flex justify-between"><span className="text-gray-400">CVaR (95%)</span><span className="text-red-400 font-bold">{fmt(histVar.cvar_amount || 0)}</span></div>
              <div className="flex justify-between"><span className="text-gray-400">Worst Loss</span><span className="text-red-400">{fmt(histVar.worst_loss || 0)}</span></div>
              <div className="flex justify-between"><span className="text-gray-400">Best Return</span><span className="text-green-400">{fmt(histVar.best_return || 0)}</span></div>
            </div>
          ) : (
            <p className="text-gray-500 text-sm">Loading...</p>
          )}
        </div>
        <div className="bg-gray-800 rounded-lg p-4">
          <h3 className="text-sm font-semibold text-gray-200 uppercase tracking-wide mb-4">Monte Carlo VaR</h3>
          {mcVar ? (
            <div className="space-y-3">
              <div className="flex justify-between"><span className="text-gray-400">Method</span><span className="text-white">{mcVar.method}</span></div>
              <div className="flex justify-between"><span className="text-gray-400">Simulations</span><span className="text-white">{(mcVar.num_simulations || 0).toLocaleString()}</span></div>
              <div className="flex justify-between"><span className="text-gray-400">VaR (95%)</span><span className="text-amber-400 font-bold">{fmt(mcVar.var_amount || 0)}</span></div>
              <div className="flex justify-between"><span className="text-gray-400">CVaR (95%)</span><span className="text-red-400 font-bold">{fmt(mcVar.cvar_amount || 0)}</span></div>
              <div className="flex justify-between"><span className="text-gray-400">Mean Return</span><span className="text-blue-400">{fmt(mcVar.mean_return || 0)}</span></div>
              <div className="flex justify-between"><span className="text-gray-400">Std Dev</span><span className="text-gray-300">{fmt(mcVar.std_dev || 0)}</span></div>
            </div>
          ) : (
            <p className="text-gray-500 text-sm">Loading...</p>
          )}
        </div>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Vol Surface Section (Phase 3 WS2)
// ---------------------------------------------------------------------------

function VolSurfaceSection() {
  const [surface, setSurface] = useState<any[]>([])
  const [region, setRegion] = useState('NSW1')
  const [loading, setLoading] = useState(false)

  const load = async () => {
    setLoading(true)
    try {
      const res = await advancedRiskApi.volSurface(region)
      setSurface(res.surface || [])
    } catch { /* */ }
    setLoading(false)
  }

  useEffect(() => { load() }, [region])

  const tenors = [...new Set(surface.map(s => s.tenor_days))].sort((a, b) => a - b)
  const strikes = [...new Set(surface.map(s => s.strike_pct))].sort((a, b) => a - b)

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <select value={region} onChange={e => setRegion(e.target.value)} className="text-sm bg-gray-700 text-gray-200 border border-gray-600 rounded px-3 py-1.5">
          {['NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1'].map(r => <option key={r} value={r}>{r}</option>)}
        </select>
        <button onClick={load} className="flex items-center gap-1.5 text-sm bg-purple-600 hover:bg-purple-500 text-white rounded px-4 py-1.5">
          <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} /> Refresh
        </button>
      </div>
      <div className="bg-gray-800 rounded-lg p-4">
        <h3 className="text-sm font-semibold text-gray-200 uppercase tracking-wide mb-4">Implied Volatility Surface — {region}</h3>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-700">
                <th className="text-left px-3 py-2 text-xs text-gray-400">Tenor (days)</th>
                {strikes.map(s => <th key={s} className="text-right px-3 py-2 text-xs text-gray-400">{s}%</th>)}
              </tr>
            </thead>
            <tbody>
              {tenors.map(t => (
                <tr key={t} className="border-b border-gray-700/50">
                  <td className="px-3 py-2 text-gray-200 font-medium">{t}d</td>
                  {strikes.map(s => {
                    const pt = surface.find(p => p.tenor_days === t && p.strike_pct === s)
                    const vol = pt?.implied_vol ?? 0
                    const bg = vol > 0.5 ? 'text-red-400' : vol > 0.3 ? 'text-amber-400' : 'text-green-400'
                    return <td key={s} className={`px-3 py-2 text-right ${bg}`}>{(vol * 100).toFixed(1)}%</td>
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {surface.length === 0 && <p className="py-8 text-center text-gray-500 text-sm">No vol surface data</p>}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Scenario Library Section (Phase 3 WS2)
// ---------------------------------------------------------------------------

function ScenarioLibrarySection() {
  const [scenarios, setScenarios] = useState<any[]>([])
  const [loading, setLoading] = useState(false)
  const [stressResult, setStressResult] = useState<any>(null)

  const load = async () => {
    setLoading(true)
    try {
      const res = await advancedRiskApi.stressScenarios()
      setScenarios(res.scenarios || [])
    } catch { /* */ }
    setLoading(false)
  }

  useEffect(() => { load() }, [])

  const handleReverseStress = async () => {
    try {
      const res = await advancedRiskApi.reverseStressTest(1_000_000)
      setStressResult(res)
    } catch { /* */ }
  }

  const CATEGORY_COLORS: Record<string, string> = {
    MARKET: 'bg-blue-500/20 text-blue-400',
    SUPPLY: 'bg-orange-500/20 text-orange-400',
    DEMAND: 'bg-green-500/20 text-green-400',
    REGULATORY: 'bg-purple-500/20 text-purple-400',
    WEATHER: 'bg-teal-500/20 text-teal-400',
    EXTREME: 'bg-red-500/20 text-red-400',
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <button onClick={load} className="flex items-center gap-1.5 text-sm bg-gray-600 hover:bg-gray-500 text-white rounded px-4 py-1.5">
          <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} /> Refresh
        </button>
        <button onClick={handleReverseStress} className="flex items-center gap-1.5 text-sm bg-red-600 hover:bg-red-500 text-white rounded px-4 py-1.5">
          Run Reverse Stress Test
        </button>
      </div>

      {stressResult && (
        <div className="bg-gray-800 rounded-lg p-4 border border-red-700/50">
          <h3 className="text-sm font-semibold text-red-400 uppercase tracking-wide mb-3">Reverse Stress Test Result</h3>
          <div className="grid grid-cols-3 gap-4 text-sm">
            <div><span className="text-gray-400">Target Loss</span><br /><span className="text-white font-bold">{fmt(stressResult.target_loss || 0)}</span></div>
            <div><span className="text-gray-400">Scenarios Found</span><br /><span className="text-white font-bold">{stressResult.scenarios_breaching || 0}</span></div>
            <div><span className="text-gray-400">Worst Scenario</span><br /><span className="text-red-400 font-bold">{stressResult.worst_scenario?.scenario_name || '—'}</span></div>
          </div>
        </div>
      )}

      <div className="bg-gray-800 rounded-lg p-4">
        <h3 className="text-sm font-semibold text-gray-200 uppercase tracking-wide mb-4">Stress Scenario Library ({scenarios.length})</h3>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-700">
                {['Scenario', 'Category', 'Price', 'Demand', 'Wind', 'Solar', 'Gas'].map(h => (
                  <th key={h} className="text-left px-3 py-2 text-xs text-gray-400 font-medium uppercase tracking-wide">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {scenarios.map((s, i) => (
                <tr key={i} className="border-b border-gray-700/50 hover:bg-gray-700/30">
                  <td className="px-3 py-2 text-gray-200 font-medium">{s.scenario_name}</td>
                  <td className="px-3 py-2">
                    <span className={`px-2 py-0.5 rounded text-xs ${CATEGORY_COLORS[s.category] || 'bg-gray-500/20 text-gray-400'}`}>{s.category}</span>
                  </td>
                  <td className={`px-3 py-2 ${s.price_shock_pct >= 0 ? 'text-red-400' : 'text-green-400'}`}>{s.price_shock_pct > 0 ? '+' : ''}{s.price_shock_pct}%</td>
                  <td className={`px-3 py-2 ${s.demand_shock_pct >= 0 ? 'text-red-400' : 'text-green-400'}`}>{s.demand_shock_pct > 0 ? '+' : ''}{s.demand_shock_pct}%</td>
                  <td className={`px-3 py-2 text-gray-300`}>{s.wind_shock_pct > 0 ? '+' : ''}{s.wind_shock_pct}%</td>
                  <td className={`px-3 py-2 text-gray-300`}>{s.solar_shock_pct > 0 ? '+' : ''}{s.solar_shock_pct}%</td>
                  <td className={`px-3 py-2 text-gray-300`}>{s.gas_shock_pct > 0 ? '+' : ''}{s.gas_shock_pct}%</td>
                </tr>
              ))}
              {scenarios.length === 0 && (
                <tr><td colSpan={7} className="px-3 py-8 text-center text-gray-500">No scenarios loaded</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Limit Monitor Section
// ---------------------------------------------------------------------------

function LimitMonitorSection({ portfolioId }: { portfolioId: string }) {
  const [monitors, setMonitors] = useState<LimitMonitorResult[]>([])
  const [loading, setLoading] = useState(false)

  const loadMonitors = async () => {
    setLoading(true)
    try {
      const res = await riskLimitsApi.monitor(portfolioId || undefined)
      setMonitors(res.monitors || [])
    } catch { /* ignore */ }
    setLoading(false)
  }

  useEffect(() => { loadMonitors() }, [portfolioId])

  // Always show the section (even empty) so users know it exists

  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2">
          <Gauge className="w-4 h-4 text-blue-400" />
          <h2 className="text-sm font-semibold text-gray-200 uppercase tracking-wide">Limit Monitor</h2>
          <span className="text-xs text-gray-500">{monitors.length} limits</span>
        </div>
        <button
          onClick={loadMonitors}
          className="flex items-center gap-1 text-xs text-gray-400 hover:text-gray-200"
        >
          <RefreshCw className={`w-3 h-3 ${loading ? 'animate-spin' : ''}`} /> Refresh
        </button>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-700">
              {['Type', 'Region', 'Limit', 'Current', 'Utilisation', 'Status'].map(h => (
                <th key={h} className="text-left px-3 py-2 text-xs text-gray-400 font-medium uppercase tracking-wide">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {monitors.map((m, i) => {
              const pct = Math.min(m.utilization_pct, 100)
              const barColor = m.status === 'BREACH' ? 'bg-red-500' : m.status === 'WARNING' ? 'bg-amber-500' : 'bg-green-500'
              const statusColor = m.status === 'BREACH' ? 'bg-red-900/50 text-red-400' : m.status === 'WARNING' ? 'bg-amber-900/50 text-amber-400' : 'bg-green-900/50 text-green-400'
              const rowBg = m.status === 'BREACH' ? 'bg-red-950/20' : m.status === 'WARNING' ? 'bg-amber-950/10' : ''
              return (
                <tr key={i} className={`border-b border-gray-700/50 hover:bg-gray-700/30 ${rowBg}`}>
                  <td className="px-3 py-2 text-gray-200 font-medium text-xs">{m.limit_type.replace(/_/g, ' ')}</td>
                  <td className="px-3 py-2 text-gray-400 text-xs">{m.region || 'All'}</td>
                  <td className="px-3 py-2 text-gray-300 font-mono text-xs">{fmt(m.limit_value)}</td>
                  <td className="px-3 py-2 text-gray-200 font-mono font-bold text-xs">{fmt(m.current_value)}</td>
                  <td className="px-3 py-2 w-40">
                    <div className="flex items-center gap-2">
                      <div className="flex-1 h-2 bg-gray-700 rounded-full overflow-hidden">
                        <div className={`h-full ${barColor} rounded-full`} style={{ width: `${pct}%` }} />
                      </div>
                      <span className="text-xs text-gray-400 w-12 text-right">{m.utilization_pct.toFixed(0)}%</span>
                    </div>
                  </td>
                  <td className="px-3 py-2">
                    <span className={`inline-block px-2 py-0.5 rounded-full text-xs font-semibold ${statusColor}`}>
                      {m.status}
                    </span>
                  </td>
                </tr>
              )
            })}
            {monitors.length === 0 && loading && (
              <tr><td colSpan={6} className="px-3 py-6 text-center text-gray-500 text-sm">Loading...</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
