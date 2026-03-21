import { useEffect, useState } from 'react'
import { PieChart, TrendingUp, Shield, DollarSign, Users } from 'lucide-react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from 'recharts'
import { getPortfolioRiskOptimisationDashboard } from '../api/client'
import type { EPRODashboard } from '../api/client'

// ---------------------------------------------------------------------------
// KPI Card
// ---------------------------------------------------------------------------

function KpiCard({
  title,
  value,
  sub,
  icon: Icon,
  color,
}: {
  title: string
  value: string
  sub?: string
  icon: React.ElementType
  color: string
}) {
  return (
    <div className="bg-gray-800 rounded-xl p-5 flex items-start gap-4">
      <div className={`p-2 rounded-lg ${color}`}>
        <Icon size={20} className="text-white" />
      </div>
      <div>
        <p className="text-xs text-gray-400 mb-1">{title}</p>
        <p className="text-2xl font-bold text-white">{value}</p>
        {sub && <p className="text-xs text-gray-500 mt-1">{sub}</p>}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Chart Card wrapper
// ---------------------------------------------------------------------------

function ChartCard({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="bg-gray-800 rounded-xl p-5">
      <h3 className="text-sm font-semibold text-gray-200 mb-4">{title}</h3>
      {children}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Chart 1 — Stacked Bar: Asset allocation % by asset_type per participant (2024 Q4)
// ---------------------------------------------------------------------------

const ASSET_COLORS = ['#10b981', '#3b82f6', '#f59e0b', '#8b5cf6', '#ef4444', '#06b6d4']
const ASSET_TYPES = ['Baseload', 'Peaking', 'Renewable', 'Storage', 'Hedges', 'Transmission']

function AssetAllocationChart({ data }: { data: EPRODashboard }) {
  const filtered = data.asset_allocations.filter(a => a.year === 2024 && a.quarter === 'Q4')
  const participants = Array.from(new Set(filtered.map(a => a.participant)))

  const chartData = participants.map(part => {
    const row: Record<string, number | string> = { participant: part.replace(' Energy', '').replace(' Hydro', ' Hyd') }
    for (const at of ASSET_TYPES) {
      const rec = filtered.find(a => a.participant === part && a.asset_type === at)
      row[at] = rec ? rec.allocation_pct : 0
    }
    return row
  })

  return (
    <ChartCard title="Asset Allocation % by Type per Participant — 2024 Q4 (Stacked)">
      <ResponsiveContainer width="100%" height={320}>
        <BarChart data={chartData} margin={{ top: 5, right: 20, left: 0, bottom: 60 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis dataKey="participant" tick={{ fill: '#9ca3af', fontSize: 10 }} angle={-30} textAnchor="end" interval={0} />
          <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} unit="%" />
          <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: 'none', color: '#f9fafb' }} formatter={(v: number) => [`${v.toFixed(1)}%`]} />
          <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 11 }} />
          {ASSET_TYPES.map((at, i) => (
            <Bar key={at} dataKey={at} stackId="a" fill={ASSET_COLORS[i % ASSET_COLORS.length]} />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </ChartCard>
  )
}

// ---------------------------------------------------------------------------
// Chart 2 — Grouped Bar: Sharpe ratio by participant and objective (2024)
// ---------------------------------------------------------------------------

const OBJ_COLORS = ['#10b981', '#3b82f6', '#f59e0b', '#8b5cf6', '#ef4444']
const OBJ_TYPES = ['Max Return', 'Min Risk', 'Sharpe', 'CVaR', 'Target Return']

function SharpeRatioChart({ data }: { data: EPRODashboard }) {
  const filtered = data.optimisation_results.filter(o => o.year === 2024)
  const participants = Array.from(new Set(filtered.map(o => o.participant)))

  const chartData = participants.map(part => {
    const row: Record<string, number | string> = { participant: part.replace(' Energy', '').replace(' Hydro', ' Hyd') }
    for (const obj of OBJ_TYPES) {
      const rec = filtered.find(o => o.participant === part && o.optimisation_objective === obj)
      row[obj] = rec ? rec.sharpe_ratio : 0
    }
    return row
  })

  return (
    <ChartCard title="Sharpe Ratio by Participant & Optimisation Objective — 2024">
      <ResponsiveContainer width="100%" height={320}>
        <BarChart data={chartData} margin={{ top: 5, right: 20, left: 0, bottom: 60 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis dataKey="participant" tick={{ fill: '#9ca3af', fontSize: 10 }} angle={-30} textAnchor="end" interval={0} />
          <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} />
          <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: 'none', color: '#f9fafb' }} formatter={(v: number) => [v.toFixed(3), 'Sharpe']} />
          <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 11 }} />
          {OBJ_TYPES.map((obj, i) => (
            <Bar key={obj} dataKey={obj} fill={OBJ_COLORS[i % OBJ_COLORS.length]} radius={[3, 3, 0, 0]} />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </ChartCard>
  )
}

// ---------------------------------------------------------------------------
// Chart 3 — Bar: Risk factor exposure vs residual_risk by risk_factor (2024, AGL Energy)
// ---------------------------------------------------------------------------

function RiskFactorExposureChart({ data }: { data: EPRODashboard }) {
  const filtered = data.risk_factors.filter(r => r.year === 2024 && r.participant === 'AGL Energy')

  const chartData = filtered.map(r => ({
    risk_factor: r.risk_factor,
    exposure: r.exposure_m_aud,
    residual: r.residual_risk_m_aud,
  }))

  return (
    <ChartCard title="Risk Factor Exposure vs Residual Risk — AGL Energy 2024 (A$M)">
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={chartData} margin={{ top: 5, right: 20, left: 0, bottom: 40 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis dataKey="risk_factor" tick={{ fill: '#9ca3af', fontSize: 11 }} angle={-20} textAnchor="end" interval={0} />
          <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} />
          <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: 'none', color: '#f9fafb' }} formatter={(v: number) => [`$${v.toFixed(1)}M`]} />
          <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 11 }} />
          <Bar dataKey="exposure" name="Exposure" fill="#ef4444" radius={[3, 3, 0, 0]} />
          <Bar dataKey="residual" name="Residual Risk" fill="#f97316" radius={[3, 3, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </ChartCard>
  )
}

// ---------------------------------------------------------------------------
// Chart 4 — Bar: Scenario pnl_impact_m_aud by scenario (2024, all participants averaged)
// ---------------------------------------------------------------------------

const SCENARIO_COLORS: Record<string, string> = {
  'Base': '#10b981',
  'High Price': '#3b82f6',
  'Low Price': '#ef4444',
  'Carbon Tax': '#f59e0b',
  'Tech Disruption': '#8b5cf6',
}

function ScenarioPnlChart({ data }: { data: EPRODashboard }) {
  const filtered = data.scenario_analysis.filter(s => s.year === 2024)
  const scenarios = ['Base', 'High Price', 'Low Price', 'Carbon Tax', 'Tech Disruption']

  const chartData = scenarios.map(sc => {
    const recs = filtered.filter(s => s.scenario === sc)
    const avg = recs.length > 0 ? recs.reduce((sum, s) => sum + s.pnl_impact_m_aud, 0) / recs.length : 0
    return { scenario: sc, avg_pnl: Math.round(avg * 10) / 10, fill: SCENARIO_COLORS[sc] ?? '#6b7280' }
  })

  return (
    <ChartCard title="Avg Scenario P&amp;L Impact by Scenario — 2024, All Participants (A$M)">
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={chartData} margin={{ top: 5, right: 20, left: 0, bottom: 40 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis dataKey="scenario" tick={{ fill: '#9ca3af', fontSize: 11 }} angle={-20} textAnchor="end" interval={0} />
          <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} />
          <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: 'none', color: '#f9fafb' }} formatter={(v: number) => [`$${v.toFixed(1)}M`, 'Avg P&L Impact']} />
          {chartData.map(d => (
            <Bar key={d.scenario} dataKey="avg_pnl" data={[d]} fill={d.fill} radius={[4, 4, 0, 0]} />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </ChartCard>
  )
}

// ---------------------------------------------------------------------------
// Chart 5 — Bar: Avg volatility_pct by asset_type (2024)
// ---------------------------------------------------------------------------

function VolatilityByAssetTypeChart({ data }: { data: EPRODashboard }) {
  const filtered = data.asset_allocations.filter(a => a.year === 2024)

  const chartData = ASSET_TYPES.map(at => {
    const recs = filtered.filter(a => a.asset_type === at)
    const avg = recs.length > 0 ? recs.reduce((sum, a) => sum + a.volatility_pct, 0) / recs.length : 0
    return { asset_type: at, avg_volatility: Math.round(avg * 10) / 10 }
  })

  return (
    <ChartCard title="Avg Volatility % by Asset Type — 2024">
      <ResponsiveContainer width="100%" height={280}>
        <BarChart data={chartData} margin={{ top: 5, right: 20, left: 0, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis dataKey="asset_type" tick={{ fill: '#9ca3af', fontSize: 11 }} />
          <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} unit="%" />
          <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: 'none', color: '#f9fafb' }} formatter={(v: number) => [`${v.toFixed(1)}%`, 'Avg Volatility']} />
          {ASSET_TYPES.map((at, i) => (
            <Bar key={at} dataKey="avg_volatility" data={chartData.filter(d => d.asset_type === at) as any} fill={ASSET_COLORS[i % ASSET_COLORS.length]} radius={[4, 4, 0, 0]} />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </ChartCard>
  )
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

export default function PortfolioRiskOptimisationAnalytics() {
  const [data, setData] = useState<EPRODashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getPortfolioRiskOptimisationDashboard()
      .then(setData)
      .catch(e => setError(e.message ?? 'Failed to load data'))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-900 flex items-center justify-center">
        <p className="text-gray-400 text-sm animate-pulse">Loading Portfolio Risk Optimisation Analytics...</p>
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="min-h-screen bg-gray-900 flex items-center justify-center">
        <p className="text-red-400 text-sm">{error ?? 'No data available'}</p>
      </div>
    )
  }

  const { summary } = data

  return (
    <div className="min-h-screen bg-gray-900 text-gray-100 p-6">
      {/* Header */}
      <div className="flex items-center gap-3 mb-6">
        <div className="p-2 rounded-lg bg-blue-600">
          <PieChart size={22} className="text-white" />
        </div>
        <div>
          <h1 className="text-xl font-bold text-white">Energy Portfolio Risk Optimisation Analytics</h1>
          <p className="text-xs text-gray-400">Asset allocation, efficient frontier, risk factors &amp; scenario analysis across NEM participants</p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 xl:grid-cols-4 gap-4 mb-6">
        <KpiCard
          title="Total Participants"
          value={String(summary.total_participants)}
          sub="NEM energy market participants"
          icon={Users}
          color="bg-blue-600"
        />
        <KpiCard
          title="Avg Portfolio Sharpe"
          value={summary.avg_portfolio_sharpe.toFixed(3)}
          sub="Across all objectives & participants"
          icon={TrendingUp}
          color="bg-emerald-600"
        />
        <KpiCard
          title="Total VaR 95% (2024)"
          value={`$${summary.total_var_95_m_aud.toFixed(1)}M`}
          sub="AUD — sum of all optimisation results"
          icon={DollarSign}
          color="bg-amber-600"
        />
        <KpiCard
          title="Avg Hedge Ratio"
          value={`${summary.avg_hedge_ratio_pct.toFixed(1)}%`}
          sub="Across all risk factors & participants"
          icon={Shield}
          color="bg-purple-600"
        />
      </div>

      {/* Charts — 2 column grid */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
        <div className="xl:col-span-2">
          <AssetAllocationChart data={data} />
        </div>
        <SharpeRatioChart data={data} />
        <RiskFactorExposureChart data={data} />
        <ScenarioPnlChart data={data} />
        <VolatilityByAssetTypeChart data={data} />
      </div>
    </div>
  )
}
