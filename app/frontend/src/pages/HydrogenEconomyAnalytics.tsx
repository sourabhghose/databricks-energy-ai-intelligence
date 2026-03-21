import { useEffect, useState } from 'react'
import { Atom } from 'lucide-react'
import {
  getHydrogenEconomyAnalyticsDashboard,
  HEADashboard,
  HEAProductionRecord,
  HEAEndUseRecord,
  HEASupplyChainRecord,
  HEACostProjectionRecord,
  HEAExportRecord,
} from '../api/client'
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
} from 'recharts'

// ── KPI Card ──────────────────────────────────────────────────────────────────

function KpiCard({
  label,
  value,
  unit,
  sub,
  valueColor,
}: {
  label: string
  value: string | number
  unit?: string
  sub?: string
  valueColor?: string
}) {
  return (
    <div className="bg-gray-800 rounded-lg p-4 flex flex-col gap-1">
      <span className="text-xs text-gray-400 uppercase tracking-wide">{label}</span>
      <span className="text-2xl font-bold" style={{ color: valueColor ?? '#fff' }}>
        {value}
        {unit && <span className="text-sm font-normal text-gray-400 ml-1">{unit}</span>}
      </span>
      {sub && <span className="text-xs text-gray-500">{sub}</span>}
    </div>
  )
}

// ── Status badge ──────────────────────────────────────────────────────────────

const STATUS_COLORS: Record<string, string> = {
  OPERATING: 'bg-emerald-600 text-white',
  CONSTRUCTION: 'bg-blue-600 text-white',
  COMMITTED: 'bg-indigo-600 text-white',
  FEASIBILITY: 'bg-amber-600 text-white',
  ANNOUNCED: 'bg-gray-500 text-white',
}

function StatusBadge({ status }: { status: string }) {
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-medium ${STATUS_COLORS[status] ?? 'bg-gray-700 text-gray-200'}`}>
      {status}
    </span>
  )
}

// ── Pathway colour map ────────────────────────────────────────────────────────

const PATHWAY_COLORS: Record<string, string> = {
  GREEN_ELECTROLYSIS: '#34d399',
  BLUE_SMR_CCS: '#60a5fa',
  BROWN_SMR: '#f59e0b',
  BIOMASS_GASIFICATION: '#a78bfa',
}

const DEST_COLORS: Record<string, string> = {
  JAPAN: '#f87171',
  KOREA: '#fb923c',
  GERMANY: '#facc15',
  SINGAPORE: '#4ade80',
  GLOBAL: '#60a5fa',
}

// ── Section header ────────────────────────────────────────────────────────────

function SectionHeader({ title, sub }: { title: string; sub?: string }) {
  return (
    <div className="mb-4">
      <h2 className="text-lg font-semibold text-white">{title}</h2>
      {sub && <p className="text-xs text-gray-400 mt-0.5">{sub}</p>}
    </div>
  )
}

// ── Production Pipeline table ─────────────────────────────────────────────────

function ProductionPipelineSection({ records }: { records: HEAProductionRecord[] }) {
  return (
    <div className="bg-gray-800 rounded-lg p-5">
      <SectionHeader
        title="Production Pipeline"
        sub="18 projects across all pathways — capacity, LCOH and CO2 intensity"
      />
      <div className="overflow-x-auto">
        <table className="w-full text-xs text-gray-300">
          <thead>
            <tr className="border-b border-gray-700 text-gray-400">
              <th className="text-left py-2 pr-3">Project</th>
              <th className="text-left py-2 pr-3">State</th>
              <th className="text-left py-2 pr-3">Pathway</th>
              <th className="text-right py-2 pr-3">Cap. (tpd)</th>
              <th className="text-right py-2 pr-3">LCOH $/kg</th>
              <th className="text-right py-2 pr-3">CO2 kg/kgH2</th>
              <th className="text-left py-2 pr-3">Export Dest.</th>
              <th className="text-left py-2">Status</th>
            </tr>
          </thead>
          <tbody>
            {records.map((r) => (
              <tr key={r.project_id} className="border-b border-gray-700/50 hover:bg-gray-700/30">
                <td className="py-1.5 pr-3 font-medium text-white">{r.name}</td>
                <td className="py-1.5 pr-3">{r.state}</td>
                <td className="py-1.5 pr-3">
                  <span
                    className="px-1.5 py-0.5 rounded text-xs font-medium"
                    style={{
                      background: (PATHWAY_COLORS[r.production_pathway] ?? '#6b7280') + '33',
                      color: PATHWAY_COLORS[r.production_pathway] ?? '#d1d5db',
                    }}
                  >
                    {r.production_pathway.replace(/_/g, ' ')}
                  </span>
                </td>
                <td className="py-1.5 pr-3 text-right">{r.capacity_tpd.toLocaleString()}</td>
                <td className="py-1.5 pr-3 text-right font-mono">{r.lcoh_per_kg.toFixed(2)}</td>
                <td className="py-1.5 pr-3 text-right font-mono">{r.co2_intensity_kg_per_kg_h2.toFixed(2)}</td>
                <td className="py-1.5 pr-3">{r.export_destination}</td>
                <td className="py-1.5">
                  <StatusBadge status={r.status} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── Export Trajectory (Stacked Bar by destination/year) ───────────────────────

function ExportTrajectorySection({ records }: { records: HEAExportRecord[] }) {
  const years = [...new Set(records.map((r) => r.year))].sort()
  const destinations = [...new Set(records.map((r) => r.destination))]

  const data = years.map((yr) => {
    const row: Record<string, number | string> = { year: yr }
    destinations.forEach((dest) => {
      const match = records.find((r) => r.year === yr && r.destination === dest)
      row[dest] = match ? match.volume_kt : 0
    })
    return row
  })

  return (
    <div className="bg-gray-800 rounded-lg p-5">
      <SectionHeader
        title="Export Trajectory 2025–2030"
        sub="Hydrogen export volume by destination (kt/yr)"
      />
      <ResponsiveContainer width="100%" height={280}>
        <BarChart data={data} margin={{ top: 10, right: 20, left: 0, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis dataKey="year" stroke="#9ca3af" tick={{ fill: '#9ca3af', fontSize: 11 }} />
          <YAxis stroke="#9ca3af" tick={{ fill: '#9ca3af', fontSize: 11 }} unit=" kt" />
          <Tooltip
            contentStyle={{ background: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
            labelStyle={{ color: '#fff' }}
            itemStyle={{ color: '#d1d5db' }}
          />
          <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 11 }} />
          {destinations.map((dest) => (
            <Bar key={dest} dataKey={dest} stackId="a" fill={DEST_COLORS[dest] ?? '#6b7280'} />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

// ── End Use Demand (Grouped Bar: current / 2030 / 2050) ───────────────────────

function EndUseDemandSection({ records }: { records: HEAEndUseRecord[] }) {
  const data = records.map((r) => ({
    name: r.use_case.length > 22 ? r.use_case.slice(0, 20) + '…' : r.use_case,
    Current: r.current_demand_kt_yr,
    '2030': r.demand_2030_kt_yr,
    '2050': r.demand_2050_kt_yr,
  }))

  return (
    <div className="bg-gray-800 rounded-lg p-5">
      <SectionHeader
        title="End Use Demand Outlook"
        sub="Current vs. 2030 vs. 2050 hydrogen demand by use case (kt/yr)"
      />
      <ResponsiveContainer width="100%" height={320}>
        <BarChart data={data} layout="vertical" margin={{ top: 5, right: 30, left: 140, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis type="number" stroke="#9ca3af" tick={{ fill: '#9ca3af', fontSize: 11 }} unit=" kt" />
          <YAxis type="category" dataKey="name" stroke="#9ca3af" tick={{ fill: '#9ca3af', fontSize: 10 }} width={130} />
          <Tooltip
            contentStyle={{ background: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
            labelStyle={{ color: '#fff' }}
            itemStyle={{ color: '#d1d5db' }}
          />
          <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 11 }} />
          <Bar dataKey="Current" fill="#6b7280" barSize={6} />
          <Bar dataKey="2030" fill="#34d399" barSize={6} />
          <Bar dataKey="2050" fill="#60a5fa" barSize={6} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

// ── Supply Chain Gaps (Bar: required_2030 vs current capacity) ────────────────

function SupplyChainSection({ records }: { records: HEASupplyChainRecord[] }) {
  const DEPENDENCY_COLOR: Record<string, string> = {
    HIGH: '#f87171',
    MEDIUM: '#fb923c',
    LOW: '#34d399',
  }

  const data = records.map((r) => ({
    component: r.component.replace(/_/g, ' '),
    Current: r.current_aus_capacity,
    'Required 2030': r.required_2030,
    'Required 2040': r.required_2040,
    dep: r.import_dependency,
  }))

  return (
    <div className="bg-gray-800 rounded-lg p-5">
      <SectionHeader
        title="Supply Chain Capacity Gaps"
        sub="Current Australian manufacturing capacity vs. requirements (normalised to component unit)"
      />
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={data} margin={{ top: 10, right: 20, left: 0, bottom: 40 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis
            dataKey="component"
            stroke="#9ca3af"
            tick={{ fill: '#9ca3af', fontSize: 10, angle: -25, textAnchor: 'end' } as any}
          />
          <YAxis stroke="#9ca3af" tick={{ fill: '#9ca3af', fontSize: 11 }} />
          <Tooltip
            contentStyle={{ background: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
            labelStyle={{ color: '#fff' }}
            itemStyle={{ color: '#d1d5db' }}
          />
          <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 11 }} />
          <Bar dataKey="Current" fill="#6b7280" />
          <Bar dataKey="Required 2030" fill="#f59e0b" />
          <Bar dataKey="Required 2040" fill="#ef4444" />
        </BarChart>
      </ResponsiveContainer>

      <div className="mt-4 grid grid-cols-2 sm:grid-cols-4 gap-2">
        {records.map((r) => (
          <div key={r.component} className="bg-gray-700 rounded p-2">
            <div className="text-xs font-semibold text-white">{r.component.replace(/_/g, ' ')}</div>
            <div className="text-xs text-gray-400 mt-0.5">
              Local content: <span className="text-gray-200">{r.local_content_pct}%</span>
            </div>
            <div className="text-xs text-gray-400">
              Import dep:{' '}
              <span
                className="font-semibold"
                style={{ color: DEPENDENCY_COLOR[r.import_dependency] ?? '#fff' }}
              >
                {r.import_dependency}
              </span>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

// ── LCOH Cost Curves (Line: 3 pathways 2024-2036) ────────────────────────────

function LcohCostCurvesSection({ records }: { records: HEACostProjectionRecord[] }) {
  const years = [...new Set(records.map((r) => r.year))].sort()
  const pathways = [...new Set(records.map((r) => r.pathway))]

  const data = years.map((yr) => {
    const row: Record<string, number | string> = { year: yr }
    pathways.forEach((p) => {
      const match = records.find((r) => r.year === yr && r.pathway === p)
      if (match) row[p] = match.lcoh_per_kg
    })
    return row
  })

  return (
    <div className="bg-gray-800 rounded-lg p-5">
      <SectionHeader
        title="LCOH Cost Curves 2024–2036"
        sub="Levelised cost of hydrogen ($/kg) by production pathway"
      />
      <ResponsiveContainer width="100%" height={280}>
        <LineChart data={data} margin={{ top: 10, right: 20, left: 0, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis dataKey="year" stroke="#9ca3af" tick={{ fill: '#9ca3af', fontSize: 11 }} />
          <YAxis stroke="#9ca3af" tick={{ fill: '#9ca3af', fontSize: 11 }} unit=" $/kg" domain={[1, 8]} />
          <Tooltip
            contentStyle={{ background: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
            labelStyle={{ color: '#fff' }}
            itemStyle={{ color: '#d1d5db' }}
            formatter={(v: number) => [`$${v.toFixed(2)}/kg`]}
          />
          <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 11 }} />
          {pathways.map((p) => (
            <Line
              key={p}
              type="monotone"
              dataKey={p}
              stroke={PATHWAY_COLORS[p] ?? '#9ca3af'}
              strokeWidth={2}
              dot={false}
              name={p.replace(/_/g, ' ')}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

// ── Main page ─────────────────────────────────────────────────────────────────

export default function HydrogenEconomyAnalytics() {
  const [data, setData] = useState<HEADashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getHydrogenEconomyAnalyticsDashboard()
      .then((d) => {
        setData(d)
        setLoading(false)
      })
      .catch((e) => {
        setError(e?.message ?? 'Failed to load Hydrogen Economy Analytics data')
        setLoading(false)
      })
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-gray-400">
        Loading Hydrogen Economy Analytics...
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="flex items-center justify-center h-64 text-red-400">
        {error ?? 'Unknown error'}
      </div>
    )
  }

  const summary = data.summary as Record<string, number>

  return (
    <div className="p-6 bg-gray-900 min-h-screen text-white">
      {/* Page header */}
      <div className="flex items-center gap-3 mb-6">
        <Atom className="text-emerald-400" size={28} />
        <div>
          <h1 className="text-2xl font-bold text-white">Hydrogen Economy Analytics</h1>
          <p className="text-sm text-gray-400 mt-0.5">
            Sprint 76b — Supply chain, export markets, end uses and production pathways
          </p>
        </div>
      </div>

      {/* KPI cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-6">
        <KpiCard
          label="Total Production Capacity"
          value={summary.total_production_capacity_kt_yr?.toLocaleString() ?? '—'}
          unit="kt/yr"
          sub="All pathways combined"
          valueColor="#34d399"
        />
        <KpiCard
          label="Operating Projects"
          value={summary.operating_projects ?? '—'}
          sub={`${summary.committed_projects ?? '—'} committed`}
          valueColor="#60a5fa"
        />
        <KpiCard
          label="Export Revenue 2030"
          value={`A$${summary.total_export_revenue_2030_bn ?? '—'}B`}
          sub="Projected annual export revenue"
          valueColor="#facc15"
        />
        <KpiCard
          label="Green H2 LCOH 2030"
          value={`$${summary.green_h2_lcoh_2030 ?? '—'}`}
          unit="/kg"
          sub={`$${summary.green_h2_lcoh_2040 ?? '—'}/kg by 2040`}
          valueColor="#a78bfa"
        />
        <KpiCard
          label="Decarbonisation Potential"
          value={summary.decarbonisation_potential_mt ?? '—'}
          unit="Mt CO2"
          sub="Across all end-use sectors"
          valueColor="#f87171"
        />
        <KpiCard
          label="Production Projects"
          value={data.production.length}
          sub="18 tracked projects (76b)"
          valueColor="#fb923c"
        />
        <KpiCard
          label="Export Records"
          value={data.exports.length}
          sub="5 markets × 6 years"
          valueColor="#34d399"
        />
        <KpiCard
          label="Cost Projections"
          value={data.cost_projections.length}
          sub="3 pathways × 13 years"
          valueColor="#60a5fa"
        />
      </div>

      {/* Charts grid */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-5 mb-5">
        <ExportTrajectorySection records={data.exports} />
        <LcohCostCurvesSection records={data.cost_projections} />
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-5 mb-5">
        <EndUseDemandSection records={data.end_uses} />
        <SupplyChainSection records={data.supply_chain} />
      </div>

      {/* Full-width production table */}
      <ProductionPipelineSection records={data.production} />
    </div>
  )
}
