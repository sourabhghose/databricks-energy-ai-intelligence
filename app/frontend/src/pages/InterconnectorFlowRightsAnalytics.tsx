import { useEffect, useState } from 'react'
import { GitMerge, TrendingUp, DollarSign, Activity, AlertTriangle } from 'lucide-react'
import {
  BarChart,
  Bar,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from 'recharts'
import { getInterconnectorFlowRightsDashboard } from '../api/client'
import type { NIFRDashboard } from '../api/client'

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
// Colour palette for interconnectors
// ---------------------------------------------------------------------------

const IC_COLORS: Record<string, string> = {
  QNI:        '#6366f1',
  'VIC1-NSW1':'#f59e0b',
  'V-SA':     '#22c55e',
  Murraylink: '#ef4444',
  Heywood:    '#3b82f6',
  Basslink:   '#a855f7',
}

const QUARTER_COLORS: Record<string, string> = {
  Q1: '#6366f1',
  Q2: '#f59e0b',
  Q3: '#22c55e',
  Q4: '#ef4444',
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

export default function InterconnectorFlowRightsAnalytics() {
  const [data, setData] = useState<NIFRDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getInterconnectorFlowRightsDashboard()
      .then(setData)
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full bg-gray-900">
        <p className="text-gray-400 animate-pulse">Loading NIFR dashboard…</p>
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="flex items-center justify-center h-full bg-gray-900">
        <p className="text-red-400">Error: {error ?? 'No data'}</p>
      </div>
    )
  }

  const { contracts, utilisation, auction_results, congestion_costs, summary } = data

  // ── Chart 1: Total contracted capacity_mw per interconnector (2024) ─────
  const capacityByIc: Record<string, number> = {}
  for (const c of contracts) {
    if (c.year === 2024) {
      capacityByIc[c.interconnector_id] = (capacityByIc[c.interconnector_id] ?? 0) + c.capacity_mw
    }
  }
  const capacityChartData = Object.entries(capacityByIc).map(([id, mw]) => ({
    id,
    capacity_mw: Math.round(mw),
  }))

  // ── Chart 2: Monthly avg_flow_mw by interconnector (2024) ────────────────
  const flowByMonth: Record<number, Record<string, number>> = {}
  for (const u of utilisation) {
    if (u.year === 2024) {
      if (!flowByMonth[u.month]) flowByMonth[u.month] = { month: u.month }
      flowByMonth[u.month][u.interconnector_id] = Math.round(u.avg_flow_mw)
    }
  }
  const flowChartData = Object.values(flowByMonth).sort((a, b) => (a.month as number) - (b.month as number))
  const MONTH_LABELS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
  const flowChartDataLabelled = flowChartData.map(row => ({
    ...row,
    label: MONTH_LABELS[(row.month as number) - 1],
  }))

  // ── Chart 3: Auction clearing_price_mwh by interconnector (grouped) ──────
  // Average clearing price per round per interconnector → average across rounds
  const priceByIc: Record<string, { sum: number; count: number }> = {}
  for (const ar of auction_results) {
    if (!priceByIc[ar.interconnector_id]) priceByIc[ar.interconnector_id] = { sum: 0, count: 0 }
    priceByIc[ar.interconnector_id].sum += ar.clearing_price_mwh
    priceByIc[ar.interconnector_id].count += 1
  }
  const auctionChartData = Object.entries(priceByIc).map(([id, { sum, count }]) => ({
    id,
    avg_clearing_price: Math.round((sum / count) * 100) / 100,
  }))

  // ── Chart 4: Quarterly congestion_cost_m_aud by interconnector (stacked) ─
  const quarters = ['Q1', 'Q2', 'Q3', 'Q4']
  const congByQuarter: Record<string, Record<string, number>> = {}
  for (const cc of congestion_costs) {
    if (!congByQuarter[cc.quarter]) congByQuarter[cc.quarter] = { quarter: cc.quarter } as unknown as Record<string, number>
    congByQuarter[cc.quarter][cc.interconnector_id] =
      ((congByQuarter[cc.quarter][cc.interconnector_id] as number) ?? 0) + cc.congestion_cost_m_aud
  }
  const congChartData = quarters
    .filter(q => congByQuarter[q])
    .map(q => {
      const row = { ...congByQuarter[q] }
      // round values
      for (const k of Object.keys(row)) {
        if (k !== 'quarter') row[k] = Math.round((row[k] as number) * 10) / 10
      }
      return row
    })

  // ── Chart 5: Utilisation_pct by interconnector averaged ──────────────────
  const utilByIc: Record<string, { sum: number; count: number }> = {}
  for (const u of utilisation) {
    if (!utilByIc[u.interconnector_id]) utilByIc[u.interconnector_id] = { sum: 0, count: 0 }
    utilByIc[u.interconnector_id].sum += u.utilisation_pct
    utilByIc[u.interconnector_id].count += 1
  }
  const utilisationChartData = Object.entries(utilByIc).map(([id, { sum, count }]) => ({
    id,
    avg_utilisation_pct: Math.round((sum / count) * 10) / 10,
  }))

  const interconnectors = ['QNI', 'VIC1-NSW1', 'V-SA', 'Murraylink', 'Heywood', 'Basslink']

  return (
    <div className="min-h-full bg-gray-900 text-gray-100 p-6 space-y-8">
      {/* Header */}
      <div className="flex items-center gap-3">
        <div className="p-2 rounded-lg bg-indigo-600">
          <GitMerge size={22} className="text-white" />
        </div>
        <div>
          <h1 className="text-xl font-bold text-white">NEM Interconnector Flow Rights Analytics</h1>
          <p className="text-xs text-gray-400 mt-0.5">
            SRA / IRU / FTR / CfD contracts, utilisation, auctions &amp; congestion costs across NEM interconnectors
          </p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <KpiCard
          title="Total Right Holders"
          value={summary.total_right_holders.toString()}
          sub="Unique market participants"
          icon={Activity}
          color="bg-indigo-600"
        />
        <KpiCard
          title="Contracted Capacity (2024)"
          value={`${(summary.total_contracted_capacity_mw / 1000).toFixed(1)} GW`}
          sub="Total MW under contract"
          icon={TrendingUp}
          color="bg-amber-600"
        />
        <KpiCard
          title="Avg Utilisation"
          value={`${summary.avg_utilisation_pct}%`}
          sub="Across all interconnectors"
          icon={GitMerge}
          color="bg-green-600"
        />
        <KpiCard
          title="Total Congestion Cost"
          value={`$${summary.total_congestion_cost_m_aud.toFixed(0)}M`}
          sub="All years combined"
          icon={AlertTriangle}
          color="bg-red-600"
        />
      </div>

      {/* Chart 1: Contracted capacity per interconnector (2024) */}
      <div className="bg-gray-800 rounded-xl p-5">
        <h2 className="text-sm font-semibold text-gray-200 mb-4">
          Total Contracted Capacity by Interconnector — 2024 (MW)
        </h2>
        <ResponsiveContainer width="100%" height={280}>
          <BarChart data={capacityChartData} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="id" tick={{ fill: '#9ca3af', fontSize: 12 }} />
            <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
              labelStyle={{ color: '#f9fafb' }}
              itemStyle={{ color: '#d1d5db' }}
            />
            <Bar dataKey="capacity_mw" name="Capacity (MW)" radius={[4, 4, 0, 0]}>
              {capacityChartData.map(entry => (
                <rect key={entry.id} fill={IC_COLORS[entry.id] ?? '#6366f1'} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 2: Monthly avg flow (2024) */}
      <div className="bg-gray-800 rounded-xl p-5">
        <h2 className="text-sm font-semibold text-gray-200 mb-4">
          Monthly Average Flow by Interconnector — 2024 (MW)
        </h2>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={flowChartDataLabelled} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="label" tick={{ fill: '#9ca3af', fontSize: 12 }} />
            <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
              labelStyle={{ color: '#f9fafb' }}
              itemStyle={{ color: '#d1d5db' }}
            />
            <Legend wrapperStyle={{ fontSize: 12, color: '#9ca3af' }} />
            {interconnectors.map(ic => (
              <Line
                key={ic}
                type="monotone"
                dataKey={ic}
                name={ic}
                stroke={IC_COLORS[ic] ?? '#6366f1'}
                dot={false}
                strokeWidth={2}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 3: Auction clearing price by interconnector */}
      <div className="bg-gray-800 rounded-xl p-5">
        <h2 className="text-sm font-semibold text-gray-200 mb-4">
          Average Auction Clearing Price by Interconnector ($/MWh)
        </h2>
        <ResponsiveContainer width="100%" height={280}>
          <BarChart data={auctionChartData} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="id" tick={{ fill: '#9ca3af', fontSize: 12 }} />
            <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
              labelStyle={{ color: '#f9fafb' }}
              itemStyle={{ color: '#d1d5db' }}
            />
            <Bar dataKey="avg_clearing_price" name="Avg Clearing Price ($/MWh)" fill="#f59e0b" radius={[4, 4, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 4: Quarterly congestion cost stacked */}
      <div className="bg-gray-800 rounded-xl p-5">
        <h2 className="text-sm font-semibold text-gray-200 mb-4">
          Quarterly Congestion Cost by Interconnector — Stacked ($M AUD, all years)
        </h2>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={congChartData} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="quarter" tick={{ fill: '#9ca3af', fontSize: 12 }} />
            <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
              labelStyle={{ color: '#f9fafb' }}
              itemStyle={{ color: '#d1d5db' }}
            />
            <Legend wrapperStyle={{ fontSize: 12, color: '#9ca3af' }} />
            {interconnectors.map(ic => (
              <Bar
                key={ic}
                dataKey={ic}
                name={ic}
                stackId="cong"
                fill={IC_COLORS[ic] ?? '#6366f1'}
              />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 5: Average utilisation % by interconnector */}
      <div className="bg-gray-800 rounded-xl p-5">
        <h2 className="text-sm font-semibold text-gray-200 mb-4">
          Average Utilisation by Interconnector (%)
        </h2>
        <ResponsiveContainer width="100%" height={280}>
          <BarChart data={utilisationChartData} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="id" tick={{ fill: '#9ca3af', fontSize: 12 }} />
            <YAxis domain={[0, 100]} tick={{ fill: '#9ca3af', fontSize: 11 }} />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
              labelStyle={{ color: '#f9fafb' }}
              itemStyle={{ color: '#d1d5db' }}
              formatter={(v: number) => [`${v}%`, 'Avg Utilisation']}
            />
            <Bar dataKey="avg_utilisation_pct" name="Avg Utilisation (%)" fill="#22c55e" radius={[4, 4, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Footer */}
      <p className="text-xs text-gray-600 text-center pb-4">
        NIFR data sourced from AEMO SRA / interconnector flow-rights registry. Synthetic seed data for demonstration.
      </p>
    </div>
  )
}
