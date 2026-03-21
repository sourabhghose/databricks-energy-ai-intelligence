import { useEffect, useState } from 'react'
import {
  LineChart, Line,
  BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer,
} from 'recharts'
import {
  getNetworkRegulatoryFrameworkDashboard,
  NRFDashboard,
  NRFNetworkBusinessRecord,
  NRFEfficiencyRecord,
} from '../api/client'

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------
const KPI_CARD_BG = 'bg-gray-800 border border-gray-700 rounded-lg p-4'

function KpiCard({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className={KPI_CARD_BG}>
      <p className="text-gray-400 text-xs uppercase tracking-wide mb-1">{label}</p>
      <p className="text-white text-2xl font-bold">{value}</p>
      {sub && <p className="text-gray-500 text-xs mt-1">{sub}</p>}
    </div>
  )
}

function SectionHeader({ title }: { title: string }) {
  return (
    <h2 className="text-lg font-semibold text-white mb-4 border-b border-gray-700 pb-2">
      {title}
    </h2>
  )
}

// ---------------------------------------------------------------------------
// Performance rating badge
// ---------------------------------------------------------------------------
function RatingBadge({ rating }: { rating: string }) {
  const COLOR_MAP: Record<string, string> = {
    EXCELLENT:    'bg-green-900 text-green-300 border border-green-700',
    GOOD:         'bg-blue-900 text-blue-300 border border-blue-700',
    SATISFACTORY: 'bg-yellow-900 text-yellow-300 border border-yellow-700',
    POOR:         'bg-red-900 text-red-300 border border-red-700',
  }
  const cls = COLOR_MAP[rating] ?? 'bg-gray-700 text-gray-300 border border-gray-600'
  return (
    <span className={`inline-block px-2 py-0.5 rounded text-xs font-semibold uppercase ${cls}`}>
      {rating}
    </span>
  )
}

// ---------------------------------------------------------------------------
// Network type badge
// ---------------------------------------------------------------------------
function TypeBadge({ type }: { type: string }) {
  const cls =
    type === 'TNSP'
      ? 'bg-purple-900 text-purple-300 border border-purple-700'
      : 'bg-teal-900 text-teal-300 border border-teal-700'
  return (
    <span className={`inline-block px-2 py-0.5 rounded text-xs font-semibold ${cls}`}>
      {type}
    </span>
  )
}

// ---------------------------------------------------------------------------
// Efficiency delta cell — green = outperforming (negative = under allowed)
// ---------------------------------------------------------------------------
function EffDelta({ val }: { val: number }) {
  const cls = val <= 0 ? 'text-green-400' : 'text-red-400'
  const sign = val <= 0 ? '' : '+'
  return <span className={cls}>{sign}{val.toFixed(1)}%</span>
}

// ---------------------------------------------------------------------------
// Section: Network Businesses table
// ---------------------------------------------------------------------------
function NetworkBusinessesTable({ businesses }: { businesses: NRFNetworkBusinessRecord[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="text-gray-400 border-b border-gray-700 text-xs uppercase">
            <th className="text-left pb-2 pr-3">Business</th>
            <th className="text-left pb-2 pr-3">Type</th>
            <th className="text-left pb-2 pr-3">State</th>
            <th className="text-right pb-2 pr-3">RAB ($Bn)</th>
            <th className="text-right pb-2 pr-3">Allowed Rev ($M)</th>
            <th className="text-right pb-2 pr-3">Actual Rev ($M)</th>
            <th className="text-right pb-2 pr-3">WACC Real %</th>
            <th className="text-right pb-2 pr-3">Capex Allowed</th>
            <th className="text-right pb-2 pr-3">Capex Actual</th>
            <th className="text-right pb-2">Eff. Benefit ($M)</th>
          </tr>
        </thead>
        <tbody>
          {businesses.map((b) => {
            const revDiff = b.actual_revenue_m - b.allowed_revenue_m
            const revCls = revDiff <= 0 ? 'text-green-400' : 'text-red-400'
            return (
              <tr key={b.business_id} className="border-b border-gray-800 hover:bg-gray-750">
                <td className="py-2 pr-3 text-white font-medium">{b.name}</td>
                <td className="py-2 pr-3"><TypeBadge type={b.type} /></td>
                <td className="py-2 pr-3 text-gray-300">{b.state}</td>
                <td className="py-2 pr-3 text-right text-amber-300 font-semibold">{b.rab_bn.toFixed(1)}</td>
                <td className="py-2 pr-3 text-right text-gray-300">{b.allowed_revenue_m.toLocaleString()}</td>
                <td className={`py-2 pr-3 text-right font-medium ${revCls}`}>{b.actual_revenue_m.toLocaleString()}</td>
                <td className="py-2 pr-3 text-right text-cyan-300">{b.wacc_real_pct.toFixed(1)}%</td>
                <td className="py-2 pr-3 text-right text-gray-300">{b.capex_allowed_m.toLocaleString()}</td>
                <td className="py-2 pr-3 text-right text-gray-300">{b.capex_actual_m.toLocaleString()}</td>
                <td className={`py-2 text-right font-semibold ${b.efficiency_benefit_m >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                  {b.efficiency_benefit_m >= 0 ? '+' : ''}{b.efficiency_benefit_m.toFixed(1)}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Section: WACC History Line Chart
// ---------------------------------------------------------------------------
function WACCHistoryChart({ waccHistory }: { waccHistory: NRFDashboard['wacc_history'] }) {
  const years = Array.from(new Set(waccHistory.map((w) => w.determination_year))).sort()
  const chartData = years.map((yr) => {
    const tnsp = waccHistory.find((w) => w.determination_year === yr && w.network_type === 'TNSP')
    const dnsp = waccHistory.find((w) => w.determination_year === yr && w.network_type === 'DNSP')
    return {
      year: yr,
      TNSP_real: tnsp?.real_post_tax_wacc_pct ?? null,
      DNSP_real: dnsp?.real_post_tax_wacc_pct ?? null,
      TNSP_nom: tnsp?.nominal_post_tax_wacc_pct ?? null,
      DNSP_nom: dnsp?.nominal_post_tax_wacc_pct ?? null,
    }
  })

  return (
    <ResponsiveContainer width="100%" height={320}>
      <LineChart data={chartData} margin={{ top: 10, right: 20, left: 0, bottom: 5 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
        <XAxis dataKey="year" stroke="#9CA3AF" tick={{ fill: '#9CA3AF', fontSize: 12 }} />
        <YAxis stroke="#9CA3AF" tick={{ fill: '#9CA3AF', fontSize: 12 }} tickFormatter={(v) => `${v}%`} domain={[0, 12]} />
        <Tooltip
          contentStyle={{ backgroundColor: '#1F2937', border: '1px solid #374151', borderRadius: 8 }}
          labelStyle={{ color: '#F9FAFB' }}
          formatter={(val: number) => `${val?.toFixed(2)}%`}
        />
        <Legend wrapperStyle={{ color: '#9CA3AF', fontSize: 12 }} />
        <Line type="monotone" dataKey="TNSP_real" name="TNSP Real Post-Tax WACC" stroke="#A78BFA" strokeWidth={2} dot={{ r: 5, fill: '#A78BFA' }} connectNulls />
        <Line type="monotone" dataKey="DNSP_real" name="DNSP Real Post-Tax WACC" stroke="#34D399" strokeWidth={2} dot={{ r: 5, fill: '#34D399' }} connectNulls />
        <Line type="monotone" dataKey="TNSP_nom"  name="TNSP Nominal Post-Tax WACC" stroke="#818CF8" strokeWidth={1.5} strokeDasharray="5 3" dot={false} connectNulls />
        <Line type="monotone" dataKey="DNSP_nom"  name="DNSP Nominal Post-Tax WACC" stroke="#6EE7B7" strokeWidth={1.5} strokeDasharray="5 3" dot={false} connectNulls />
      </LineChart>
    </ResponsiveContainer>
  )
}

// ---------------------------------------------------------------------------
// Section: RAB Growth Stacked Bar Chart (by business, latest 4 years stacked)
// ---------------------------------------------------------------------------
function RABGrowthChart({ rabGrowth, businesses }: { rabGrowth: NRFDashboard['rab_growth']; businesses: NRFNetworkBusinessRecord[] }) {
  // Build chart data grouped by business_id: show closing_rab_bn per year
  const bidToName = Object.fromEntries(businesses.map((b) => [b.business_id, b.name.replace(' (Transmission)', ' T').replace(' (Distribution)', ' D')]))
  const years = [2021, 2022, 2023, 2024]
  const chartData = years.map((yr) => {
    const entry: Record<string, number | string> = { year: String(yr) }
    rabGrowth
      .filter((r) => r.year === yr)
      .forEach((r) => {
        entry[bidToName[r.business_id] ?? r.business_id] = r.closing_rab_bn
      })
    return entry
  })

  const COLORS = ['#60A5FA', '#34D399', '#FBBF24', '#F87171', '#A78BFA', '#38BDF8', '#4ADE80', '#FB923C', '#C084FC', '#F472B6']
  const bizNames = businesses.map((b) => b.name.replace(' (Transmission)', ' T').replace(' (Distribution)', ' D'))

  return (
    <ResponsiveContainer width="100%" height={340}>
      <BarChart data={chartData} margin={{ top: 10, right: 20, left: 0, bottom: 5 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
        <XAxis dataKey="year" stroke="#9CA3AF" tick={{ fill: '#9CA3AF', fontSize: 12 }} />
        <YAxis stroke="#9CA3AF" tick={{ fill: '#9CA3AF', fontSize: 12 }} tickFormatter={(v) => `$${v}B`} />
        <Tooltip
          contentStyle={{ backgroundColor: '#1F2937', border: '1px solid #374151', borderRadius: 8 }}
          labelStyle={{ color: '#F9FAFB' }}
          formatter={(val: number) => `$${val.toFixed(2)}B`}
        />
        <Legend wrapperStyle={{ color: '#9CA3AF', fontSize: 10 }} />
        {bizNames.map((name, i) => (
          <Bar key={name} dataKey={name} stackId="rab" fill={COLORS[i % COLORS.length]} />
        ))}
      </BarChart>
    </ResponsiveContainer>
  )
}

// ---------------------------------------------------------------------------
// Section: Efficiency Performance Table
// ---------------------------------------------------------------------------
function EfficiencyTable({ efficiency, businesses }: { efficiency: NRFEfficiencyRecord[]; businesses: NRFNetworkBusinessRecord[] }) {
  const bidToName = Object.fromEntries(businesses.map((b) => [b.business_id, b.name]))
  const bidToType = Object.fromEntries(businesses.map((b) => [b.business_id, b.type]))

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="text-gray-400 border-b border-gray-700 text-xs uppercase">
            <th className="text-left pb-2 pr-3">Business</th>
            <th className="text-left pb-2 pr-3">Type</th>
            <th className="text-left pb-2 pr-3">Period</th>
            <th className="text-right pb-2 pr-3">Capex Eff.</th>
            <th className="text-right pb-2 pr-3">Opex Eff.</th>
            <th className="text-right pb-2 pr-3">SAIDI (min)</th>
            <th className="text-right pb-2 pr-3">SAIDI Target</th>
            <th className="text-right pb-2 pr-3">Incentive ($M)</th>
            <th className="text-left pb-2">Rating</th>
          </tr>
        </thead>
        <tbody>
          {efficiency.map((e) => (
            <tr key={e.business_id} className="border-b border-gray-800 hover:bg-gray-750">
              <td className="py-2 pr-3 text-white font-medium">{bidToName[e.business_id] ?? e.business_id}</td>
              <td className="py-2 pr-3"><TypeBadge type={bidToType[e.business_id] ?? ''} /></td>
              <td className="py-2 pr-3 text-gray-400 text-xs">{e.regulatory_period}</td>
              <td className="py-2 pr-3 text-right"><EffDelta val={e.capex_efficiency_pct} /></td>
              <td className="py-2 pr-3 text-right"><EffDelta val={e.opex_efficiency_pct} /></td>
              <td className="py-2 pr-3 text-right text-gray-300">{e.reliability_performance.toFixed(1)}</td>
              <td className="py-2 pr-3 text-right text-gray-400">{e.reliability_target.toFixed(1)}</td>
              <td className={`py-2 pr-3 text-right font-semibold ${e.incentive_payment_m >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                {e.incentive_payment_m >= 0 ? '+' : ''}{e.incentive_payment_m.toFixed(1)}
              </td>
              <td className="py-2"><RatingBadge rating={e.performance_rating} /></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Section: Capex Categories Stacked Bar (by business)
// ---------------------------------------------------------------------------
function CapexCategoriesChart({ capexCategories, businesses }: { capexCategories: NRFDashboard['capex_categories']; businesses: NRFNetworkBusinessRecord[] }) {
  const bizNames = businesses.map((b) => b.name.replace(' (Transmission)', ' T').replace(' (Distribution)', ' D'))
  const bidToShortName = Object.fromEntries(
    businesses.map((b) => [b.business_id, b.name.replace(' (Transmission)', ' T').replace(' (Distribution)', ' D')])
  )
  const categories = ['AUGMENTATION', 'REPLACEMENT', 'CONNECTIONS', 'RELIABILITY', 'OPEX_CAPEX_TRADEOFF']
  const CAT_COLORS: Record<string, string> = {
    AUGMENTATION:        '#60A5FA',
    REPLACEMENT:         '#FBBF24',
    CONNECTIONS:         '#34D399',
    RELIABILITY:         '#F87171',
    OPEX_CAPEX_TRADEOFF: '#A78BFA',
  }

  const chartData = businesses.map((b) => {
    const entry: Record<string, number | string> = { name: bidToShortName[b.business_id] }
    categories.forEach((cat) => {
      const rec = capexCategories.find((r) => r.business_id === b.business_id && r.category === cat)
      entry[cat] = rec?.capex_m ?? 0
    })
    return entry
  })

  return (
    <ResponsiveContainer width="100%" height={340}>
      <BarChart data={chartData} margin={{ top: 10, right: 20, left: 0, bottom: 60 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
        <XAxis dataKey="name" stroke="#9CA3AF" tick={{ fill: '#9CA3AF', fontSize: 10, angle: -30, textAnchor: 'end' } as any} interval={0} />
        <YAxis stroke="#9CA3AF" tick={{ fill: '#9CA3AF', fontSize: 12 }} tickFormatter={(v) => `$${v}M`} />
        <Tooltip
          contentStyle={{ backgroundColor: '#1F2937', border: '1px solid #374151', borderRadius: 8 }}
          labelStyle={{ color: '#F9FAFB' }}
          formatter={(val: number) => `$${val.toFixed(0)}M`}
        />
        <Legend wrapperStyle={{ color: '#9CA3AF', fontSize: 11 }} />
        {categories.map((cat) => (
          <Bar key={cat} dataKey={cat} stackId="capex" fill={CAT_COLORS[cat]} name={cat.replace('_', ' ')} />
        ))}
      </BarChart>
    </ResponsiveContainer>
  )
}

// ---------------------------------------------------------------------------
// WACC Detail Table
// ---------------------------------------------------------------------------
function WACCDetailTable({ waccHistory }: { waccHistory: NRFDashboard['wacc_history'] }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="text-gray-400 border-b border-gray-700 text-xs uppercase">
            <th className="text-left pb-2 pr-3">Year</th>
            <th className="text-left pb-2 pr-3">Type</th>
            <th className="text-right pb-2 pr-3">Nom. Pre-Tax WACC</th>
            <th className="text-right pb-2 pr-3">Nom. Post-Tax WACC</th>
            <th className="text-right pb-2 pr-3">Real Post-Tax WACC</th>
            <th className="text-right pb-2 pr-3">RFR</th>
            <th className="text-right pb-2 pr-3">ERP</th>
            <th className="text-right pb-2 pr-3">DRP</th>
            <th className="text-right pb-2 pr-3">Gamma</th>
            <th className="text-right pb-2">Gearing</th>
          </tr>
        </thead>
        <tbody>
          {waccHistory.map((w, i) => (
            <tr key={i} className="border-b border-gray-800 hover:bg-gray-750">
              <td className="py-2 pr-3 text-white font-semibold">{w.determination_year}</td>
              <td className="py-2 pr-3"><TypeBadge type={w.network_type} /></td>
              <td className="py-2 pr-3 text-right text-gray-300">{w.nominal_pre_tax_wacc_pct.toFixed(2)}%</td>
              <td className="py-2 pr-3 text-right text-gray-300">{w.nominal_post_tax_wacc_pct.toFixed(2)}%</td>
              <td className="py-2 pr-3 text-right text-cyan-300 font-semibold">{w.real_post_tax_wacc_pct.toFixed(2)}%</td>
              <td className="py-2 pr-3 text-right text-gray-400">{w.risk_free_rate_pct.toFixed(2)}%</td>
              <td className="py-2 pr-3 text-right text-gray-400">{w.equity_risk_premium_pct.toFixed(2)}%</td>
              <td className="py-2 pr-3 text-right text-gray-400">{w.debt_risk_premium_pct.toFixed(2)}%</td>
              <td className="py-2 pr-3 text-right text-gray-400">{w.gamma.toFixed(3)}</td>
              <td className="py-2 text-right text-gray-400">{w.gearing_pct.toFixed(0)}%</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main page component
// ---------------------------------------------------------------------------
export default function NetworkRegulatoryFrameworkAnalytics() {
  const [data, setData] = useState<NRFDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getNetworkRegulatoryFrameworkDashboard()
      .then(setData)
      .catch((e) => setError(e.message ?? 'Failed to load'))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-gray-400">
        Loading Network Regulatory Framework data...
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="flex items-center justify-center h-64 text-red-400">
        Error: {error ?? 'No data'}
      </div>
    )
  }

  const s = data.summary as Record<string, number>

  return (
    <div className="p-6 space-y-8 bg-gray-900 min-h-screen text-white">
      {/* Page header */}
      <div>
        <h1 className="text-2xl font-bold text-white">
          Electricity Network Investment Regulatory Framework
        </h1>
        <p className="text-gray-400 text-sm mt-1">
          AER regulatory determinations, WACC, Regulatory Asset Base growth, and network business financial performance
        </p>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
        <KpiCard
          label="Total RAB"
          value={`$${s.total_rab_bn?.toFixed(1)}B`}
          sub="Across all NEM networks"
        />
        <KpiCard
          label="Allowed Revenue"
          value={`$${(s.total_allowed_revenue_m / 1000).toFixed(1)}B`}
          sub="Annual aggregate"
        />
        <KpiCard
          label="Avg Real WACC"
          value={`${s.avg_wacc_real_pct?.toFixed(1)}%`}
          sub="AER 2024 determination"
        />
        <KpiCard
          label="Total Capex 2024"
          value={`$${(s.total_capex_2024_m / 1000).toFixed(1)}B`}
          sub="Network capital investment"
        />
        <KpiCard
          label="Avg Capex Efficiency"
          value={`${s.avg_capex_efficiency_pct?.toFixed(1)}%`}
          sub="vs allowed (negative = outperform)"
        />
        <KpiCard
          label="Outperforming"
          value={`${s.businesses_outperforming} / ${data.businesses.length}`}
          sub="Businesses vs allowed capex"
        />
      </div>

      {/* Network Businesses */}
      <div className="bg-gray-800 border border-gray-700 rounded-xl p-5">
        <SectionHeader title="Network Businesses — RAB, Revenue & Efficiency" />
        <NetworkBusinessesTable businesses={data.businesses} />
      </div>

      {/* WACC History */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
        <div className="bg-gray-800 border border-gray-700 rounded-xl p-5">
          <SectionHeader title="WACC History — Real Post-Tax WACC by Determination Year" />
          <WACCHistoryChart waccHistory={data.wacc_history} />
        </div>
        <div className="bg-gray-800 border border-gray-700 rounded-xl p-5">
          <SectionHeader title="AER WACC Determinations — Parameter Detail" />
          <WACCDetailTable waccHistory={data.wacc_history} />
        </div>
      </div>

      {/* RAB Growth */}
      <div className="bg-gray-800 border border-gray-700 rounded-xl p-5">
        <SectionHeader title="Regulatory Asset Base (RAB) Growth 2021–2024 ($Bn)" />
        <RABGrowthChart rabGrowth={data.rab_growth} businesses={data.businesses} />
      </div>

      {/* Efficiency Performance */}
      <div className="bg-gray-800 border border-gray-700 rounded-xl p-5">
        <SectionHeader title="Efficiency Performance — STPIS / EBSS & Reliability" />
        <p className="text-gray-500 text-xs mb-3">
          Capex/Opex efficiency: negative % = spent less than allowed (outperformance).
          Incentive payments reflect STPIS (reliability) and EBSS (opex) schemes.
        </p>
        <EfficiencyTable efficiency={data.efficiency} businesses={data.businesses} />
      </div>

      {/* Capex Categories */}
      <div className="bg-gray-800 border border-gray-700 rounded-xl p-5">
        <SectionHeader title="Capex Categories by Network Business — FY2024 ($M)" />
        <p className="text-gray-500 text-xs mb-3">
          Stacked by investment driver: Augmentation (network growth), Replacement (ageing assets), Connections (DER/customer), Reliability, and Opex-Capex trade-offs.
        </p>
        <CapexCategoriesChart capexCategories={data.capex_categories} businesses={data.businesses} />
      </div>
    </div>
  )
}
