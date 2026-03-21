import { useEffect, useState } from 'react'
import { Flame } from 'lucide-react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import { api } from '../api/client'
import type {
  CoalRetirementDashboard,
  CoalRetirementRecord,
  CapacityGapRecord,
  TransitionInvestmentRecord,
} from '../api/client'

// ── KPI Card ──────────────────────────────────────────────────────────────

interface KpiCardProps {
  label: string
  value: string
  sub?: string
}

function KpiCard({ label, value, sub }: KpiCardProps) {
  return (
    <div className="bg-gray-800 rounded-lg p-4 flex flex-col gap-1">
      <span className="text-xs text-gray-400 uppercase tracking-wide">{label}</span>
      <span className="text-2xl font-bold text-white">{value}</span>
      {sub && <span className="text-xs text-gray-500">{sub}</span>}
    </div>
  )
}

// ── Capacity Gap Chart ────────────────────────────────────────────────────

const TARGET_YEARS = [2025, 2027, 2030, 2032, 2035]

interface GapChartRow {
  label: string
  NSW_retirements?: number
  NSW_renewables?: number
  NSW_storage?: number
  VIC_retirements?: number
  VIC_renewables?: number
  VIC_storage?: number
  QLD_retirements?: number
  QLD_renewables?: number
  QLD_storage?: number
}

function buildGapChartData(gaps: CapacityGapRecord[]): GapChartRow[] {
  return TARGET_YEARS.map(yr => {
    const row: GapChartRow = { label: String(yr) }
    const states = ['NSW', 'VIC', 'QLD'] as const
    states.forEach(state => {
      const rec = gaps.find(g => g.year === yr && g.state === state)
      if (rec) {
        ;(row as unknown as Record<string, unknown>)[`${state}_retirements`] = rec.retirements_mw
        ;(row as unknown as Record<string, unknown>)[`${state}_renewables`] = rec.new_renewables_mw
        ;(row as unknown as Record<string, unknown>)[`${state}_storage`] = rec.new_storage_mw
      }
    })
    return row
  })
}

interface CapacityGapChartProps {
  gaps: CapacityGapRecord[]
}

function CapacityGapChart({ gaps }: CapacityGapChartProps) {
  const data = buildGapChartData(gaps)
  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <h2 className="text-sm font-semibold text-gray-200 uppercase tracking-wide mb-4">
        Capacity Gap Analysis by State — Retirements vs New Capacity (MW)
      </h2>
      <ResponsiveContainer width="100%" height={340}>
        <BarChart data={data} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis
            dataKey="label"
            tick={{ fill: '#9ca3af', fontSize: 12 }}
            axisLine={{ stroke: '#4b5563' }}
            tickLine={false}
          />
          <YAxis
            tick={{ fill: '#9ca3af', fontSize: 12 }}
            axisLine={false}
            tickLine={false}
            tickFormatter={v => `${v} MW`}
          />
          <Tooltip
            contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
            labelStyle={{ color: '#e5e7eb', fontWeight: 600 }}
            itemStyle={{ color: '#d1d5db' }}
            formatter={(val: number, name: string) => [`${val.toLocaleString()} MW`, name]}
          />
          <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 11 }} />
          {/* NSW */}
          <Bar dataKey="NSW_retirements" name="NSW Retirements" fill="#ef4444" />
          <Bar dataKey="NSW_renewables" name="NSW New Renewables" fill="#22c55e" />
          <Bar dataKey="NSW_storage" name="NSW New Storage" fill="#3b82f6" />
          {/* VIC */}
          <Bar dataKey="VIC_retirements" name="VIC Retirements" fill="#dc2626" />
          <Bar dataKey="VIC_renewables" name="VIC New Renewables" fill="#16a34a" />
          <Bar dataKey="VIC_storage" name="VIC New Storage" fill="#2563eb" />
          {/* QLD */}
          <Bar dataKey="QLD_retirements" name="QLD Retirements" fill="#b91c1c" />
          <Bar dataKey="QLD_renewables" name="QLD New Renewables" fill="#15803d" />
          <Bar dataKey="QLD_storage" name="QLD New Storage" fill="#1d4ed8" />
        </BarChart>
      </ResponsiveContainer>
      <p className="mt-2 text-[10px] text-gray-500">
        Red = retirements &nbsp;|&nbsp; Green = new renewables &nbsp;|&nbsp; Blue = new storage
      </p>
    </div>
  )
}

// ── Technology Badge ──────────────────────────────────────────────────────

function TechBadge({ tech }: { tech: string }) {
  const cls =
    tech === 'BLACK_COAL'
      ? 'bg-gray-700 text-gray-300'
      : tech === 'BROWN_COAL'
      ? 'bg-amber-900 text-amber-300'
      : 'bg-gray-700 text-gray-400'
  return (
    <span className={`inline-block px-1.5 py-0.5 rounded text-[10px] font-semibold ${cls}`}>
      {tech.replace('_', ' ')}
    </span>
  )
}

// ── Status Badge ──────────────────────────────────────────────────────────

function StatusBadge({ status }: { status: string }) {
  const cls =
    status === 'OPERATING'
      ? 'bg-green-900 text-green-300'
      : status === 'RETIRING_ANNOUNCED'
      ? 'bg-orange-900 text-orange-300'
      : status === 'RETIRED'
      ? 'bg-red-900 text-red-300'
      : status === 'EXTENDED'
      ? 'bg-blue-900 text-blue-300'
      : 'bg-gray-700 text-gray-400'
  return (
    <span className={`inline-block px-1.5 py-0.5 rounded text-[10px] font-semibold ${cls}`}>
      {status.replace('_', ' ')}
    </span>
  )
}

// ── Retirement Units Table ────────────────────────────────────────────────

interface RetirementTableProps {
  units: CoalRetirementRecord[]
}

function RetirementTable({ units }: RetirementTableProps) {
  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <h2 className="text-sm font-semibold text-gray-200 uppercase tracking-wide mb-4">
        Coal Unit Retirement Schedule
      </h2>
      <div className="overflow-x-auto">
        <table className="w-full text-xs text-gray-300">
          <thead>
            <tr className="border-b border-gray-700">
              <th className="text-left py-2 pr-3 text-gray-400 font-medium">Unit</th>
              <th className="text-left py-2 pr-3 text-gray-400 font-medium">Station</th>
              <th className="text-left py-2 pr-3 text-gray-400 font-medium">Owner</th>
              <th className="text-center py-2 pr-3 text-gray-400 font-medium">State</th>
              <th className="text-center py-2 pr-3 text-gray-400 font-medium">Technology</th>
              <th className="text-right py-2 pr-3 text-gray-400 font-medium">Capacity (MW)</th>
              <th className="text-right py-2 pr-3 text-gray-400 font-medium">Commissioned</th>
              <th className="text-right py-2 pr-3 text-gray-400 font-medium">Retirement</th>
              <th className="text-right py-2 pr-3 text-gray-400 font-medium">Age (yrs)</th>
              <th className="text-center py-2 pr-3 text-gray-400 font-medium">Status</th>
              <th className="text-center py-2 pr-3 text-gray-400 font-medium">Reason</th>
              <th className="text-left py-2 text-gray-400 font-medium">Replacement Tech</th>
            </tr>
          </thead>
          <tbody>
            {units.map(u => (
              <tr key={u.unit_id} className="border-b border-gray-700/50 hover:bg-gray-750">
                <td className="py-1.5 pr-3 font-mono text-gray-200">{u.unit_name}</td>
                <td className="py-1.5 pr-3">{u.station}</td>
                <td className="py-1.5 pr-3">{u.owner}</td>
                <td className="py-1.5 pr-3 text-center font-semibold">{u.state}</td>
                <td className="py-1.5 pr-3 text-center">
                  <TechBadge tech={u.technology} />
                </td>
                <td className="py-1.5 pr-3 text-right">{u.registered_capacity_mw.toLocaleString()}</td>
                <td className="py-1.5 pr-3 text-right">{u.commissioning_year}</td>
                <td className="py-1.5 pr-3 text-right font-semibold text-amber-300">{u.planned_retirement_year}</td>
                <td className="py-1.5 pr-3 text-right">{u.age_years}</td>
                <td className="py-1.5 pr-3 text-center">
                  <StatusBadge status={u.status} />
                </td>
                <td className="py-1.5 pr-3 text-center text-gray-400">{u.retirement_reason}</td>
                <td className="py-1.5 text-gray-400">
                  {u.replacement_technologies.join(' / ') || '—'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── Investment Type Badge ─────────────────────────────────────────────────

function InvTypeBadge({ type }: { type: string }) {
  const cls =
    type === 'SOLAR'
      ? 'bg-yellow-900 text-yellow-300'
      : type === 'WIND'
      ? 'bg-cyan-900 text-cyan-300'
      : type === 'STORAGE'
      ? 'bg-purple-900 text-purple-300'
      : type === 'TRANSMISSION'
      ? 'bg-blue-900 text-blue-300'
      : type === 'HYDROGEN'
      ? 'bg-teal-900 text-teal-300'
      : 'bg-gray-700 text-gray-400'
  return (
    <span className={`inline-block px-1.5 py-0.5 rounded text-[10px] font-semibold ${cls}`}>
      {type}
    </span>
  )
}

// ── Transition Investment Table ───────────────────────────────────────────

interface InvestmentTableProps {
  investments: TransitionInvestmentRecord[]
}

function InvestmentTable({ investments }: InvestmentTableProps) {
  const [stateFilter, setStateFilter] = useState<string>('ALL')
  const states = Array.from(new Set(investments.map(i => i.state))).sort()
  const filtered = stateFilter === 'ALL' ? investments : investments.filter(i => i.state === stateFilter)

  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-sm font-semibold text-gray-200 uppercase tracking-wide">
          Transition Investment Pipeline
        </h2>
        <select
          value={stateFilter}
          onChange={e => setStateFilter(e.target.value)}
          className="text-xs bg-gray-700 text-gray-200 border border-gray-600 rounded px-2 py-1"
        >
          <option value="ALL">All States</option>
          {states.map(s => (
            <option key={s} value={s}>{s}</option>
          ))}
        </select>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs text-gray-300">
          <thead>
            <tr className="border-b border-gray-700">
              <th className="text-center py-2 pr-3 text-gray-400 font-medium">State</th>
              <th className="text-center py-2 pr-3 text-gray-400 font-medium">Type</th>
              <th className="text-right py-2 pr-3 text-gray-400 font-medium">CAPEX Committed ($M)</th>
              <th className="text-right py-2 pr-3 text-gray-400 font-medium">CAPEX Pipeline ($M)</th>
              <th className="text-right py-2 pr-3 text-gray-400 font-medium">MW Committed</th>
              <th className="text-right py-2 text-gray-400 font-medium">MW Pipeline</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map(inv => (
              <tr key={inv.record_id} className="border-b border-gray-700/50 hover:bg-gray-750">
                <td className="py-1.5 pr-3 text-center font-semibold text-gray-200">{inv.state}</td>
                <td className="py-1.5 pr-3 text-center">
                  <InvTypeBadge type={inv.investment_type} />
                </td>
                <td className="py-1.5 pr-3 text-right text-green-400 font-semibold">
                  ${inv.capex_committed_m_aud.toLocaleString()}M
                </td>
                <td className="py-1.5 pr-3 text-right text-blue-400">
                  ${inv.capex_pipeline_m_aud.toLocaleString()}M
                </td>
                <td className="py-1.5 pr-3 text-right">
                  {inv.mw_committed > 0 ? `${inv.mw_committed.toLocaleString()} MW` : '—'}
                </td>
                <td className="py-1.5 text-right">
                  {inv.mw_pipeline > 0 ? `${inv.mw_pipeline.toLocaleString()} MW` : '—'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {filtered.length === 0 && (
          <p className="text-center text-gray-500 py-6 text-sm">No investment records match the selected filter.</p>
        )}
      </div>
    </div>
  )
}

// ── Main Page ─────────────────────────────────────────────────────────────

export default function CoalRetirement() {
  const [dashboard, setDashboard] = useState<CoalRetirementDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    api
      .getCoalRetirementDashboard()
      .then(setDashboard)
      .catch(e => setError(String(e)))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full bg-gray-900">
        <span className="text-gray-400 text-sm animate-pulse">Loading coal retirement data...</span>
      </div>
    )
  }

  if (error || !dashboard) {
    return (
      <div className="flex items-center justify-center h-full bg-gray-900">
        <span className="text-red-400 text-sm">{error ?? 'Unknown error'}</span>
      </div>
    )
  }

  return (
    <div className="min-h-full bg-gray-900 p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center gap-3">
        <Flame className="text-orange-400" size={24} />
        <div>
          <h1 className="text-xl font-bold text-white">
            Coal Fleet Retirement &amp; Energy Transition Analytics
          </h1>
          <p className="text-xs text-gray-400 mt-0.5">
            Retirement schedule &middot; capacity gap analysis &middot; replacement investment needs
          </p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KpiCard
          label="Operating Coal Units"
          value={String(dashboard.operating_coal_units)}
          sub="Excludes retired units"
        />
        <KpiCard
          label="Total Coal Capacity"
          value={`${dashboard.total_coal_capacity_mw.toLocaleString()} MW`}
          sub="Operating fleet capacity"
        />
        <KpiCard
          label="Retirements by 2030"
          value={`${dashboard.retirements_by_2030_mw.toLocaleString()} MW`}
          sub="Announced & planned"
        />
        <KpiCard
          label="Avg Coal Age"
          value={`${dashboard.avg_coal_age_years} yrs`}
          sub="Operating fleet average"
        />
      </div>

      {/* Capacity Gap Grouped Bar Chart */}
      <CapacityGapChart gaps={dashboard.capacity_gaps} />

      {/* Retirement Units Table */}
      <RetirementTable units={dashboard.retirement_records} />

      {/* Transition Investment Table */}
      <InvestmentTable investments={dashboard.transition_investments} />
    </div>
  )
}
