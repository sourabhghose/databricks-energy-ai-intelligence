import { useEffect, useState } from 'react'
import { Wind, Zap, DollarSign, MapPin, Package } from 'lucide-react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
  Cell,
} from 'recharts'
import { getOffshoreWindDevelopmentDashboard } from '../api/client'
import type { OWDAXdashboard } from '../api/client'

// ---------------------------------------------------------------------------
// Colour palettes
// ---------------------------------------------------------------------------
const STATUS_COLOURS: Record<string, string> = {
  'Planning':            '#6366f1',
  'Feasibility':         '#f59e0b',
  'Approved':            '#10b981',
  'Under Construction':  '#3b82f6',
  'Operating':           '#22c55e',
}

const COMPONENT_COLOURS: Record<string, string> = {
  'Turbines':     '#3b82f6',
  'Foundations':  '#10b981',
  'Cables':       '#f59e0b',
  'Installation': '#6366f1',
  'Substation':   '#ef4444',
}

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
// Main Page
// ---------------------------------------------------------------------------
export default function OffshoreWindDevelopmentAnalytics() {
  const [data, setData] = useState<OWDAXdashboard | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    getOffshoreWindDevelopmentDashboard()
      .then(setData)
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-900 flex items-center justify-center">
        <p className="text-gray-400 text-sm">Loading Offshore Wind Development Analytics...</p>
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="min-h-screen bg-gray-900 flex items-center justify-center">
        <p className="text-red-400 text-sm">{error ?? 'Unknown error'}</p>
      </div>
    )
  }

  const { projects, investment, generation, supply_chain, summary } = data

  // Chart 1: All 14 projects by capacity_mw, sorted descending, coloured by status
  const projectsByCapacity = [...projects]
    .sort((a, b) => b.capacity_mw - a.capacity_mw)
    .map(p => ({
      name: p.project_name.length > 22 ? p.project_name.slice(0, 20) + '…' : p.project_name,
      capacity_mw: p.capacity_mw,
      status: p.status,
    }))

  // Chart 2: Stacked investment capex by project for 2024 – top 10 sorted by total capex
  const capex2024Map: Record<string, number> = {}
  for (const inv of investment) {
    if (inv.year === 2024) {
      capex2024Map[inv.project_id] = (capex2024Map[inv.project_id] ?? 0) + inv.capex_m_aud
    }
  }
  const capexByProject = projects
    .map(p => ({
      name: p.project_name.length > 16 ? p.project_name.slice(0, 14) + '…' : p.project_name,
      capex_2024: Math.round(capex2024Map[p.project_id] ?? 0),
    }))
    .sort((a, b) => b.capex_2024 - a.capex_2024)
    .slice(0, 10)

  // Chart 3: Projected capacity factor % by project, Q1 only
  const cfQ1Map: Record<string, number> = {}
  for (const gen of generation) {
    if (gen.quarter === 'Q1') {
      cfQ1Map[gen.project_id] = gen.capacity_factor_pct
    }
  }
  const cfByProject = projects.map(p => ({
    name: p.project_name.length > 16 ? p.project_name.slice(0, 14) + '…' : p.project_name,
    capacity_factor_pct: cfQ1Map[p.project_id] ?? 0,
  }))

  // Chart 4: Supply chain local content % by component
  const localContentByComp = supply_chain.map(sc => ({
    component: sc.component,
    local_content_pct: sc.local_content_pct,
  }))

  // Chart 5: Local jobs by supply chain component
  const localJobsByComp = supply_chain.map(sc => ({
    component: sc.component,
    local_jobs: sc.local_jobs,
  }))

  return (
    <div className="min-h-screen bg-gray-900 text-white">
      {/* Header */}
      <div className="bg-blue-600 border-b border-blue-700 px-6 py-4 flex items-center gap-3">
        <div className="p-2 bg-blue-800 rounded-lg">
          <Wind size={22} className="text-white" />
        </div>
        <div>
          <h1 className="text-lg font-bold text-white">Offshore Wind Development Analytics</h1>
          <p className="text-xs text-blue-200">Australian offshore wind project pipeline and investment intelligence</p>
        </div>
      </div>

      <div className="p-6 space-y-6">
        {/* KPI Cards */}
        <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
          <KpiCard
            title="Total Projects"
            value={String(summary.total_projects)}
            sub="In pipeline"
            icon={Wind}
            color="bg-blue-600"
          />
          <KpiCard
            title="Total Pipeline MW"
            value={`${summary.total_pipeline_mw.toLocaleString()} MW`}
            sub="Across all states"
            icon={Zap}
            color="bg-indigo-600"
          />
          <KpiCard
            title="Avg Capex per MW"
            value={`$${summary.avg_capex_per_mw_m_aud.toFixed(1)}M`}
            sub="AUD per MW (2024)"
            icon={DollarSign}
            color="bg-green-600"
          />
          <KpiCard
            title="Leading State"
            value={summary.leading_state}
            sub="By pipeline MW"
            icon={MapPin}
            color="bg-amber-600"
          />
        </div>

        {/* Chart 1: Projects by Capacity MW */}
        <ChartCard title="Offshore Wind Projects — Capacity MW (All 14, sorted)">
          <ResponsiveContainer width="100%" height={320}>
            <BarChart
              layout="vertical"
              data={projectsByCapacity}
              margin={{ top: 4, right: 30, left: 150, bottom: 4 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis type="number" tick={{ fill: '#9ca3af', fontSize: 11 }} unit=" MW" />
              <YAxis
                dataKey="name"
                type="category"
                tick={{ fill: '#d1d5db', fontSize: 11 }}
                width={145}
              />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                labelStyle={{ color: '#f9fafb' }}
                formatter={((v: number, _n: string, entry: { payload: { status: string } }) => [
                  `${v} MW`,
                  entry.payload.status,
                ]) as any}
              />
              <Bar dataKey="capacity_mw" name="Capacity (MW)" radius={[0, 4, 4, 0]}>
                {projectsByCapacity.map((entry, idx) => (
                  <Cell key={idx} fill={STATUS_COLOURS[entry.status] ?? '#6b7280'} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
          <div className="flex flex-wrap gap-3 mt-3">
            {Object.entries(STATUS_COLOURS).map(([status, color]) => (
              <span key={status} className="flex items-center gap-1 text-xs text-gray-400">
                <span className="inline-block w-3 h-3 rounded-sm" style={{ backgroundColor: color }} />
                {status}
              </span>
            ))}
          </div>
        </ChartCard>

        {/* Chart 2: Investment CAPEX 2024 — top 10 projects */}
        <ChartCard title="Investment CAPEX 2024 — Top 10 Projects (A$M)">
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={capexByProject} margin={{ top: 4, right: 20, left: 10, bottom: 60 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                dataKey="name"
                tick={{ fill: '#9ca3af', fontSize: 10 }}
                angle={-35}
                textAnchor="end"
                interval={0}
              />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} unit=" M" />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                labelStyle={{ color: '#f9fafb' }}
                formatter={(v: number) => [`$${v}M AUD`, 'CAPEX 2024']}
              />
              <Bar dataKey="capex_2024" name="CAPEX 2024 (A$M)" fill="#3b82f6" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        {/* Chart 3: Projected capacity factor % — Q1 by project */}
        <ChartCard title="Projected Capacity Factor % — Q1 by Project">
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={cfByProject} margin={{ top: 4, right: 20, left: 10, bottom: 60 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                dataKey="name"
                tick={{ fill: '#9ca3af', fontSize: 10 }}
                angle={-35}
                textAnchor="end"
                interval={0}
              />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} unit="%" domain={[0, 60]} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                labelStyle={{ color: '#f9fafb' }}
                formatter={(v: number) => [`${v.toFixed(1)}%`, 'Capacity Factor Q1']}
              />
              <Bar dataKey="capacity_factor_pct" name="Capacity Factor %" fill="#10b981" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
          {/* Chart 4: Supply chain local content % by component */}
          <ChartCard title="Supply Chain Local Content % by Component">
            <ResponsiveContainer width="100%" height={240}>
              <BarChart data={localContentByComp} margin={{ top: 4, right: 20, left: 10, bottom: 20 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                <XAxis dataKey="component" tick={{ fill: '#9ca3af', fontSize: 11 }} />
                <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} unit="%" domain={[0, 60]} />
                <Tooltip
                  contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                  labelStyle={{ color: '#f9fafb' }}
                  formatter={(v: number) => [`${v.toFixed(1)}%`, 'Local Content']}
                />
                <Bar dataKey="local_content_pct" name="Local Content %" radius={[4, 4, 0, 0]}>
                  {localContentByComp.map((entry, idx) => (
                    <Cell key={idx} fill={COMPONENT_COLOURS[entry.component] ?? '#6b7280'} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </ChartCard>

          {/* Chart 5: Local jobs by supply chain component */}
          <ChartCard title="Local Jobs by Supply Chain Component">
            <ResponsiveContainer width="100%" height={240}>
              <BarChart data={localJobsByComp} margin={{ top: 4, right: 20, left: 10, bottom: 20 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                <XAxis dataKey="component" tick={{ fill: '#9ca3af', fontSize: 11 }} />
                <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} />
                <Tooltip
                  contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                  labelStyle={{ color: '#f9fafb' }}
                  formatter={(v: number) => [v.toLocaleString(), 'Local Jobs']}
                />
                <Bar dataKey="local_jobs" name="Local Jobs" radius={[4, 4, 0, 0]}>
                  {localJobsByComp.map((entry, idx) => (
                    <Cell key={idx} fill={COMPONENT_COLOURS[entry.component] ?? '#6b7280'} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </ChartCard>
        </div>
      </div>
    </div>
  )
}
