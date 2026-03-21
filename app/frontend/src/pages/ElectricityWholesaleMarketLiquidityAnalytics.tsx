import { useEffect, useState } from 'react'
import {
  BarChart, Bar,
  LineChart, Line,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer, Cell,
} from 'recharts'
import { BarChart2, TrendingUp, DollarSign, Activity } from 'lucide-react'
import {
  getElectricityWholesaleMarketLiquidityDashboard,
  EWMLDashboard,
} from '../api/client'

// ─── Colour palettes ──────────────────────────────────────────────────────────
const REGION_COLOURS: Record<string, string> = {
  NSW1: '#60a5fa',
  QLD1: '#facc15',
  VIC1: '#34d399',
  SA1:  '#fb923c',
  TAS1: '#a78bfa',
}

const YEAR_COLOURS: Record<string, string> = {
  '2022': '#60a5fa',
  '2023': '#34d399',
  '2024': '#fb923c',
}

const PARTICIPANT_TYPE_COLOURS: Record<string, string> = {
  Gentailer:              '#22d3ee',
  'Independent Generator': '#34d399',
  Retailer:               '#a78bfa',
  Trader:                 '#fb923c',
  Aggregator:             '#f87171',
}

const PRODUCT_COLOURS: Record<string, string> = {
  NSW1: '#60a5fa',
  VIC1: '#34d399',
}

// ─── KPI Card ─────────────────────────────────────────────────────────────────
function KpiCard({ label, value, sub, Icon }: {
  label: string; value: string; sub?: string; Icon: React.ElementType
}) {
  return (
    <div className="bg-gray-800 rounded-xl p-5 flex items-start gap-4 shadow-lg">
      <div className="p-3 bg-gray-700 rounded-lg">
        <Icon className="w-6 h-6 text-cyan-400" />
      </div>
      <div>
        <p className="text-xs text-gray-400 uppercase tracking-wide">{label}</p>
        <p className="text-2xl font-bold text-white mt-0.5">{value}</p>
        {sub && <p className="text-xs text-gray-400 mt-0.5">{sub}</p>}
      </div>
    </div>
  )
}

// ─── Main page ────────────────────────────────────────────────────────────────
export default function ElectricityWholesaleMarketLiquidityAnalytics() {
  const [data, setData] = useState<EWMLDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getElectricityWholesaleMarketLiquidityDashboard()
      .then(setData)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-gray-400 bg-gray-900">
        Loading Electricity Wholesale Market Liquidity Analytics...
      </div>
    )
  }
  if (error || !data) {
    return (
      <div className="flex items-center justify-center h-64 text-red-400 bg-gray-900">
        {error ?? 'No data available'}
      </div>
    )
  }

  const { region_liquidity, price_formation, participants, forward_curve, settlement, summary } = data

  // ── Chart 1: Churn ratio by region × year (grouped bar) ──────────────────
  const regions = ['NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1']
  const churnByRegion = regions.map(region => {
    const entry: Record<string, string | number> = { region }
    for (const year of [2022, 2023, 2024]) {
      const rec = region_liquidity.find(r => r.region === region && r.year === year)
      entry[String(year)] = rec ? rec.churn_ratio : 0
    }
    return entry
  })

  // ── Chart 2: Monthly VWAP & volatility for NSW1, VIC1, QLD1 in 2024 ─────
  const keyRegions = ['NSW1', 'VIC1', 'QLD1']
  const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
  const vwapByMonth = monthNames.map((mn, idx) => {
    const entry: Record<string, string | number> = { month: mn }
    for (const region of keyRegions) {
      const rec = price_formation.find(p => p.region === region && p.month === idx + 1)
      if (rec) {
        entry[`${region}_vwap`] = rec.vwap_mwh
        entry[`${region}_vol`] = rec.price_volatility_pct
      }
    }
    return entry
  })

  // ── Chart 3: Participant traded volume (top 15) coloured by type ──────────
  const sortedParticipants = [...participants]
    .sort((a, b) => b.traded_volume_twh_2024 - a.traded_volume_twh_2024)
    .slice(0, 15)

  // ── Chart 4: Forward price by product/year for NSW1 vs VIC1 ──────────────
  // Use "Cal Year" product across 3 years for NSW1 and VIC1
  const fwdYears = [2025, 2026, 2027]
  const fwdData = fwdYears.map(cy => {
    const entry: Record<string, string | number> = { contract_year: String(cy) }
    for (const region of ['NSW1', 'VIC1']) {
      const rec = forward_curve.find(f => f.product === 'Cal Year' && f.contract_year === cy && f.region === region)
      entry[region] = rec ? rec.forward_price_mwh : 0
    }
    return entry
  })

  // ── Chart 5: Settlement amount quarterly by region (2022-2024) ───────────
  const settleRegions = ['NSW1', 'VIC1']
  const settleLabels: string[] = []
  for (const yr of [2022, 2023, 2024]) {
    for (const q of ['Q1', 'Q2', 'Q3', 'Q4']) {
      settleLabels.push(`${yr} ${q}`)
    }
  }
  const settlementData = settleLabels.map(label => {
    const [yr, q] = label.split(' ')
    const entry: Record<string, string | number> = { period: label }
    for (const region of settleRegions) {
      const rec = settlement.find(s => s.year === Number(yr) && s.quarter === q && s.region === region)
      entry[region] = rec ? rec.settlement_amount_m_aud : 0
    }
    return entry
  })

  return (
    <div className="min-h-screen bg-gray-900 p-6 text-white">
      {/* Header */}
      <div className="flex items-center gap-3 mb-8">
        <div className="p-2 bg-gray-800 rounded-lg">
          <BarChart2 className="w-7 h-7 text-cyan-400" />
        </div>
        <div>
          <h1 className="text-2xl font-bold text-white">
            Electricity Wholesale Market Liquidity Analytics
          </h1>
          <p className="text-sm text-gray-400 mt-0.5">
            NEM Wholesale Market Depth, Price Formation, Participants & Settlement
          </p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
        <KpiCard
          label="Total Traded Volume"
          value={`${summary.total_traded_volume_twh.toFixed(1)} TWh`}
          sub="2024 across all regions"
          Icon={TrendingUp}
        />
        <KpiCard
          label="Avg Churn Ratio"
          value={summary.avg_churn_ratio.toFixed(2)}
          sub="traded ÷ physical volume"
          Icon={Activity}
        />
        <KpiCard
          label="Most Liquid Region"
          value={summary.most_liquid_region}
          sub="highest churn ratio 2024"
          Icon={BarChart2}
        />
        <KpiCard
          label="Avg HHI"
          value={summary.avg_hhi.toFixed(0)}
          sub={`${summary.total_active_participants} active participants`}
          Icon={DollarSign}
        />
      </div>

      {/* Charts grid */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">

        {/* Chart 1: Churn ratio by region × year */}
        <div className="bg-gray-800 rounded-xl p-5 shadow-lg">
          <h2 className="text-base font-semibold text-white mb-4">
            Churn Ratio by Region and Year
          </h2>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={churnByRegion} margin={{ top: 5, right: 20, left: 0, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="region" tick={{ fill: '#9ca3af', fontSize: 12 }} />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 12 }} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                labelStyle={{ color: '#f3f4f6' }}
              />
              <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 12 }} />
              {[2022, 2023, 2024].map(yr => (
                <Bar key={yr} dataKey={String(yr)} fill={YEAR_COLOURS[String(yr)]} radius={[3, 3, 0, 0]} />
              ))}
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Chart 2: Monthly VWAP for 3 regions */}
        <div className="bg-gray-800 rounded-xl p-5 shadow-lg">
          <h2 className="text-base font-semibold text-white mb-4">
            Monthly VWAP 2024 — NSW1, VIC1, QLD1
          </h2>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={vwapByMonth} margin={{ top: 5, right: 20, left: 0, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="month" tick={{ fill: '#9ca3af', fontSize: 12 }} />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 12 }} unit=" $/MWh" />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                labelStyle={{ color: '#f3f4f6' }}
              />
              <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 12 }} />
              {keyRegions.map(region => (
                <Line
                  key={`${region}_vwap`}
                  type="monotone"
                  dataKey={`${region}_vwap`}
                  name={`${region} VWAP`}
                  stroke={REGION_COLOURS[region]}
                  dot={false}
                  strokeWidth={2}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Chart 3: Participant traded volume top 15 */}
        <div className="bg-gray-800 rounded-xl p-5 shadow-lg">
          <h2 className="text-base font-semibold text-white mb-4">
            Top 15 Participants — Traded Volume 2024 (TWh)
          </h2>
          <ResponsiveContainer width="100%" height={340}>
            <BarChart
              data={sortedParticipants}
              layout="vertical"
              margin={{ top: 5, right: 20, left: 120, bottom: 5 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis type="number" tick={{ fill: '#9ca3af', fontSize: 11 }} unit=" TWh" />
              <YAxis
                dataKey="participant_name"
                type="category"
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                width={115}
              />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                labelStyle={{ color: '#f3f4f6' }}
              />
              <Bar dataKey="traded_volume_twh_2024" radius={[0, 3, 3, 0]}>
                {sortedParticipants.map((entry, index) => (
                  <Cell
                    key={`cell-${index}`}
                    fill={PARTICIPANT_TYPE_COLOURS[entry.participant_type] ?? '#60a5fa'}
                  />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
          {/* Legend for participant types */}
          <div className="flex flex-wrap gap-3 mt-3">
            {Object.entries(PARTICIPANT_TYPE_COLOURS).map(([type, colour]) => (
              <div key={type} className="flex items-center gap-1.5 text-xs text-gray-400">
                <span className="w-3 h-3 rounded-sm inline-block" style={{ backgroundColor: colour }} />
                {type}
              </div>
            ))}
          </div>
        </div>

        {/* Chart 4: Forward curve NSW1 vs VIC1 */}
        <div className="bg-gray-800 rounded-xl p-5 shadow-lg">
          <h2 className="text-base font-semibold text-white mb-4">
            Forward Curve (Cal Year) — NSW1 vs VIC1
          </h2>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={fwdData} margin={{ top: 5, right: 20, left: 0, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="contract_year" tick={{ fill: '#9ca3af', fontSize: 12 }} />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 12 }} unit=" $/MWh" />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                labelStyle={{ color: '#f3f4f6' }}
              />
              <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 12 }} />
              {(['NSW1', 'VIC1'] as const).map(region => (
                <Line
                  key={region}
                  type="monotone"
                  dataKey={region}
                  stroke={PRODUCT_COLOURS[region]}
                  dot={{ r: 5, fill: PRODUCT_COLOURS[region] }}
                  strokeWidth={2}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Chart 5: Settlement quarterly by region */}
        <div className="bg-gray-800 rounded-xl p-5 shadow-lg xl:col-span-2">
          <h2 className="text-base font-semibold text-white mb-4">
            Settlement Amount Quarterly by Region (A$M) — 2022–2024
          </h2>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={settlementData} margin={{ top: 5, right: 20, left: 0, bottom: 50 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                dataKey="period"
                tick={{ fill: '#9ca3af', fontSize: 10, angle: -45, textAnchor: 'end' } as any}
                interval={0}
              />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 12 }} unit=" A$M" />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                labelStyle={{ color: '#f3f4f6' }}
              />
              <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 12 }} />
              {settleRegions.map(region => (
                <Bar
                  key={region}
                  dataKey={region}
                  fill={REGION_COLOURS[region]}
                  radius={[3, 3, 0, 0]}
                />
              ))}
            </BarChart>
          </ResponsiveContainer>
        </div>

      </div>

      {/* Footer summary row */}
      <div className="mt-6 bg-gray-800 rounded-xl p-4 flex flex-wrap gap-6 text-sm text-gray-400">
        <span>
          <span className="text-white font-medium">Avg Bid-Ask Spread: </span>
          {summary.avg_bid_ask_spread.toFixed(2)} $/MWh
        </span>
        <span>
          <span className="text-white font-medium">Total Settlement: </span>
          A${summary.total_settlement_m_aud.toFixed(0)}M
        </span>
        <span>
          <span className="text-white font-medium">Avg HHI: </span>
          {summary.avg_hhi.toFixed(0)}
        </span>
        <span>
          <span className="text-white font-medium">Avg Churn: </span>
          {summary.avg_churn_ratio.toFixed(2)}x
        </span>
      </div>
    </div>
  )
}
