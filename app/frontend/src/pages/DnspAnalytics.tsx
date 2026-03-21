// ---------------------------------------------------------------------------
// Sprint 33a — DNSP Distribution Network Analytics
// ---------------------------------------------------------------------------

import { useEffect, useState } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer, ReferenceLine, Scatter, ScatterChart,
} from 'recharts'
import { Network, Users, Activity, Zap, Sun } from 'lucide-react'
import { api, DnspDashboard, DnspRecord, DnspInvestmentRecord } from '../api/client'

// ---------------------------------------------------------------------------
// KPI Card
// ---------------------------------------------------------------------------

interface KpiCardProps {
  label: string
  value: string
  sub?: string
  Icon: React.ElementType
  color: string
}

function KpiCard({ label, value, sub, Icon, color }: KpiCardProps) {
  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5 flex items-start gap-4">
      <div className={`p-2.5 rounded-lg ${color}`}>
        <Icon size={20} className="text-white" />
      </div>
      <div className="min-w-0">
        <p className="text-xs text-gray-500 dark:text-gray-400 mb-0.5">{label}</p>
        <p className="text-2xl font-bold text-gray-900 dark:text-gray-100">{value}</p>
        {sub && <p className="text-xs text-gray-400 dark:text-gray-500 mt-0.5">{sub}</p>}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// SAIDI Chart — bar chart vs regulatory target reference line
// ---------------------------------------------------------------------------

interface SaidiChartProps {
  records: DnspRecord[]
}

function SaidiChart({ records }: SaidiChartProps) {
  // Build chart data — sorted by saidi_minutes descending
  const data = [...records]
    .sort((a, b) => b.saidi_minutes - a.saidi_minutes)
    .map(r => ({
      name: r.dnsp.replace(' Energy', '').replace(' Services', '').replace(' Networks', '').replace('Networks', ''),
      saidi: r.saidi_minutes,
      target: r.regulatory_target_saidi,
      fullName: r.dnsp,
    }))

  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5">
      <h2 className="text-sm font-semibold text-gray-800 dark:text-gray-100 mb-4">
        SAIDI Performance vs Regulatory Target (minutes/year)
      </h2>
      <ResponsiveContainer width="100%" height={320}>
        <BarChart data={data} margin={{ top: 10, right: 20, bottom: 60, left: 10 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.3} />
          <XAxis
            dataKey="name"
            tick={{ fontSize: 10, fill: '#9CA3AF' }}
            {...({ angle: -40 } as any)}
            textAnchor="end"
            interval={0}
          />
          <YAxis tick={{ fontSize: 11, fill: '#9CA3AF' }} unit=" min" />
          <Tooltip
            contentStyle={{ backgroundColor: '#1F2937', border: '1px solid #374151', borderRadius: 8 }}
            labelStyle={{ color: '#F9FAFB', fontWeight: 600 }}
            itemStyle={{ color: '#D1D5DB' }}
            formatter={(value: number, name: string) => [
              `${value.toFixed(1)} min`,
              name === 'saidi' ? 'SAIDI (actual)' : 'Reg. Target SAIDI',
            ]}
          />
          <Legend
            wrapperStyle={{ fontSize: 11, color: '#9CA3AF', paddingTop: 8 }}
            formatter={(value) => value === 'saidi' ? 'SAIDI Actual' : 'Regulatory Target'}
          />
          <Bar dataKey="saidi" fill="#3B82F6" radius={[3, 3, 0, 0]} name="saidi" />
          <Bar dataKey="target" fill="transparent" name="target" />
          {data.map((entry, idx) => (
            <ReferenceLine
              key={idx}
              x={entry.name}
              y={entry.target}
              stroke="#EF4444"
              strokeDasharray="4 2"
              strokeWidth={1.5}
              {...({ dot: { r: 3, fill: '#EF4444' } } as any)}
            />
          ))}
        </BarChart>
      </ResponsiveContainer>
      <p className="text-xs text-gray-400 dark:text-gray-500 mt-2">
        Red dashed lines indicate AER regulatory target SAIDI for each DNSP. Bars above the line indicate underperformance.
      </p>
    </div>
  )
}

// ---------------------------------------------------------------------------
// DNSP Table
// ---------------------------------------------------------------------------

const STATE_OPTIONS = ['ALL', 'NSW', 'VIC', 'QLD', 'SA', 'WA', 'TAS']

interface DnspTableProps {
  records: DnspRecord[]
}

function DnspTable({ records }: DnspTableProps) {
  const [stateFilter, setStateFilter] = useState<string>('ALL')

  const filtered = stateFilter === 'ALL'
    ? records
    : records.filter(r => r.state === stateFilter)

  const sorted = [...filtered].sort((a, b) => b.saidi_minutes - a.saidi_minutes)

  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5">
      <div className="flex items-center justify-between mb-4 flex-wrap gap-3">
        <h2 className="text-sm font-semibold text-gray-800 dark:text-gray-100">
          DNSP Reliability & Revenue Metrics
        </h2>
        <div className="flex flex-wrap gap-1.5">
          {STATE_OPTIONS.map(s => (
            <button
              key={s}
              onClick={() => setStateFilter(s)}
              className={[
                'px-2.5 py-1 rounded-md text-xs font-medium transition-colors',
                stateFilter === s
                  ? 'bg-blue-600 text-white'
                  : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600',
              ].join(' ')}
            >
              {s}
            </button>
          ))}
        </div>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-gray-200 dark:border-gray-700">
              {[
                'DNSP', 'State', 'Customers', 'SAIDI (vs Target)',
                'SAIFI', 'Network KM', 'RAB $M', 'Capex $M',
                'Solar %', 'EV Chargers', 'Tariff c/kWh',
              ].map(h => (
                <th key={h} className="text-left py-2 px-2 font-semibold text-gray-500 dark:text-gray-400 whitespace-nowrap">
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sorted.map((rec, i) => {
              const saidiOver = rec.saidi_minutes > rec.regulatory_target_saidi
              return (
                <tr
                  key={i}
                  className="border-b border-gray-100 dark:border-gray-700/50 hover:bg-gray-50 dark:hover:bg-gray-700/30 transition-colors"
                >
                  <td className="py-2 px-2 font-medium text-gray-900 dark:text-gray-100 whitespace-nowrap">
                    {rec.dnsp}
                  </td>
                  <td className="py-2 px-2">
                    <span className="px-1.5 py-0.5 rounded text-xs font-medium bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300">
                      {rec.state}
                    </span>
                  </td>
                  <td className="py-2 px-2 text-gray-700 dark:text-gray-300">
                    {(rec.customers / 1_000_000).toFixed(2)}M
                  </td>
                  <td className="py-2 px-2">
                    <span className={[
                      'font-semibold',
                      saidiOver ? 'text-red-600 dark:text-red-400' : 'text-green-600 dark:text-green-400',
                    ].join(' ')}>
                      {rec.saidi_minutes.toFixed(1)}
                    </span>
                    <span className="text-gray-400 dark:text-gray-500 ml-1">
                      / {rec.regulatory_target_saidi.toFixed(0)}
                    </span>
                    <span className={[
                      'ml-1 text-xs',
                      saidiOver ? 'text-red-500' : 'text-green-500',
                    ].join(' ')}>
                      {saidiOver ? '▲' : '▼'}
                    </span>
                  </td>
                  <td className="py-2 px-2 text-gray-700 dark:text-gray-300">
                    {rec.saifi_count.toFixed(2)}
                  </td>
                  <td className="py-2 px-2 text-gray-700 dark:text-gray-300">
                    {rec.network_km >= 1000
                      ? `${(rec.network_km / 1000).toFixed(0)}k`
                      : rec.network_km.toLocaleString()}
                  </td>
                  <td className="py-2 px-2 text-gray-700 dark:text-gray-300">
                    ${rec.rab_m_aud.toFixed(0)}M
                  </td>
                  <td className="py-2 px-2 text-gray-700 dark:text-gray-300">
                    ${rec.capex_m_aud.toFixed(0)}M
                  </td>
                  <td className="py-2 px-2 text-gray-700 dark:text-gray-300">
                    {rec.rooftop_solar_pct.toFixed(1)}%
                  </td>
                  <td className="py-2 px-2 text-gray-700 dark:text-gray-300">
                    {rec.ev_charger_connections.toLocaleString()}
                  </td>
                  <td className="py-2 px-2 text-gray-700 dark:text-gray-300">
                    {(rec.network_tariff_aud_kwh * 100).toFixed(2)}c
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
        {sorted.length === 0 && (
          <p className="text-center text-gray-400 dark:text-gray-500 py-6 text-sm">
            No records for selected state.
          </p>
        )}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Investment Table
// ---------------------------------------------------------------------------

const CATEGORY_OPTIONS = ['ALL', 'AUGMENTATION', 'REPLACEMENT', 'DEMAND_MANAGEMENT', 'DIGITISATION']

const CATEGORY_COLOURS: Record<string, string> = {
  AUGMENTATION: 'bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300',
  REPLACEMENT: 'bg-orange-100 text-orange-700 dark:bg-orange-900/40 dark:text-orange-300',
  DEMAND_MANAGEMENT: 'bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-300',
  DIGITISATION: 'bg-purple-100 text-purple-700 dark:bg-purple-900/40 dark:text-purple-300',
}

const STATUS_COLOURS: Record<string, string> = {
  APPROVED: 'bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300',
  UNDERWAY: 'bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300',
  COMPLETE: 'bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-300',
}

interface InvestmentTableProps {
  records: DnspInvestmentRecord[]
}

function InvestmentTable({ records }: InvestmentTableProps) {
  const [catFilter, setCatFilter] = useState<string>('ALL')

  const filtered = catFilter === 'ALL'
    ? records
    : records.filter(r => r.category === catFilter)

  const sorted = [...filtered].sort((a, b) => b.capex_m_aud - a.capex_m_aud)

  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5">
      <div className="flex items-center justify-between mb-4 flex-wrap gap-3">
        <h2 className="text-sm font-semibold text-gray-800 dark:text-gray-100">
          DNSP Capital Investment Program
        </h2>
        <div className="flex flex-wrap gap-1.5">
          {CATEGORY_OPTIONS.map(c => (
            <button
              key={c}
              onClick={() => setCatFilter(c)}
              className={[
                'px-2.5 py-1 rounded-md text-xs font-medium transition-colors',
                catFilter === c
                  ? 'bg-blue-600 text-white'
                  : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600',
              ].join(' ')}
            >
              {c === 'DEMAND_MANAGEMENT' ? 'DEMAND MGT' : c}
            </button>
          ))}
        </div>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-gray-200 dark:border-gray-700">
              {['DNSP', 'Project', 'Category', 'Capex $M', 'Year', 'Benefit', 'Status'].map(h => (
                <th key={h} className="text-left py-2 px-2 font-semibold text-gray-500 dark:text-gray-400 whitespace-nowrap">
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sorted.map((rec, i) => (
              <tr
                key={i}
                className="border-b border-gray-100 dark:border-gray-700/50 hover:bg-gray-50 dark:hover:bg-gray-700/30 transition-colors"
              >
                <td className="py-2 px-2 font-medium text-gray-900 dark:text-gray-100 whitespace-nowrap">
                  {rec.dnsp}
                </td>
                <td className="py-2 px-2 text-gray-700 dark:text-gray-300 max-w-[200px]">
                  {rec.project_name}
                </td>
                <td className="py-2 px-2">
                  <span className={[
                    'px-2 py-0.5 rounded text-xs font-medium',
                    CATEGORY_COLOURS[rec.category] ?? 'bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300',
                  ].join(' ')}>
                    {rec.category.replace('_', ' ')}
                  </span>
                </td>
                <td className="py-2 px-2 font-semibold text-gray-900 dark:text-gray-100">
                  ${rec.capex_m_aud.toFixed(0)}M
                </td>
                <td className="py-2 px-2 text-gray-700 dark:text-gray-300">
                  {rec.year}
                </td>
                <td className="py-2 px-2 text-gray-700 dark:text-gray-300">
                  {rec.customer_benefit}
                </td>
                <td className="py-2 px-2">
                  <span className={[
                    'px-2 py-0.5 rounded text-xs font-medium',
                    STATUS_COLOURS[rec.status] ?? 'bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300',
                  ].join(' ')}>
                    {rec.status}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {sorted.length === 0 && (
          <p className="text-center text-gray-400 dark:text-gray-500 py-6 text-sm">
            No investment records for selected category.
          </p>
        )}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

export default function DnspAnalytics() {
  const [dashboard, setDashboard] = useState<DnspDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    setLoading(true)
    api.getDnspDashboard()
      .then(d => {
        setDashboard(d)
        setLoading(false)
      })
      .catch(err => {
        setError(err?.message ?? 'Failed to load DNSP data')
        setLoading(false)
      })
  }, [])

  if (loading) {
    return (
      <div className="p-6 space-y-4">
        <div className="h-8 w-72 bg-gray-200 dark:bg-gray-700 rounded animate-pulse" />
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          {[...Array(4)].map((_, i) => (
            <div key={i} className="h-24 bg-gray-200 dark:bg-gray-700 rounded-xl animate-pulse" />
          ))}
        </div>
        <div className="h-80 bg-gray-200 dark:bg-gray-700 rounded-xl animate-pulse" />
        <div className="h-64 bg-gray-200 dark:bg-gray-700 rounded-xl animate-pulse" />
      </div>
    )
  }

  if (error) {
    return (
      <div className="p-6">
        <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-xl p-4 text-red-700 dark:text-red-300 text-sm">
          {error}
        </div>
      </div>
    )
  }

  if (!dashboard) return null

  const fmtCustomers = (n: number) => {
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`
    if (n >= 1_000) return `${(n / 1_000).toFixed(0)}k`
    return String(n)
  }

  const fmtKm = (n: number) => {
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`
    if (n >= 1_000) return `${(n / 1_000).toFixed(0)}k`
    return String(n)
  }

  return (
    <div className="p-6 space-y-6 min-h-screen bg-gray-50 dark:bg-gray-900">
      {/* Header */}
      <div className="flex items-center gap-3">
        <div className="p-2 bg-blue-600 rounded-lg">
          <Network size={20} className="text-white" />
        </div>
        <div>
          <h1 className="text-xl font-bold text-gray-900 dark:text-gray-100">
            Distribution Network (DNSP) Analytics
          </h1>
          <p className="text-xs text-gray-500 dark:text-gray-400">
            14 DNSPs across all NEM states — reliability, revenue & investment. Updated {dashboard.timestamp}
          </p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <KpiCard
          label="Total Distribution Customers"
          value={fmtCustomers(dashboard.total_distribution_customers)}
          sub="All states"
          Icon={Users}
          color="bg-blue-600"
        />
        <KpiCard
          label="National Avg SAIDI"
          value={`${dashboard.national_avg_saidi.toFixed(1)} min`}
          sub="Unplanned outage minutes/year"
          Icon={Activity}
          color="bg-amber-500"
        />
        <KpiCard
          label="Total Network KM"
          value={fmtKm(dashboard.total_network_km)}
          sub="Distribution lines"
          Icon={Network}
          color="bg-purple-600"
        />
        <KpiCard
          label="Avg Rooftop Solar"
          value={`${dashboard.total_rooftop_solar_pct.toFixed(1)}%`}
          sub="Of residential customers"
          Icon={Sun}
          color="bg-green-600"
        />
      </div>

      {/* SAIDI Chart */}
      <SaidiChart records={dashboard.dnsp_records} />

      {/* DNSP Table */}
      <DnspTable records={dashboard.dnsp_records} />

      {/* Investment Table */}
      <InvestmentTable records={dashboard.investment_records} />

      {/* Footer note */}
      <p className="text-xs text-gray-400 dark:text-gray-600 pb-2">
        Data sourced from AER Regulatory Determinations, DNSP Annual Performance Reports, and AEMO DER Register (2025).
        SAIDI = System Average Interruption Duration Index. SAIFI = System Average Interruption Frequency Index.
        RAB = Regulatory Asset Base. All financials in AUD millions.
      </p>
    </div>
  )
}
