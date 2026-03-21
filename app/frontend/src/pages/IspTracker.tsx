import { useEffect, useState } from 'react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from 'recharts'
import { MapPin, ChevronDown, ChevronRight, Hammer } from 'lucide-react'
import { api } from '../api/client'
import type { IspDashboard, IspMajorProject, IspProjectMilestone, TnspCapexProgram } from '../api/client'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function statusColor(status: string): string {
  switch (status) {
    case 'UNDER_CONSTRUCTION': return '#22c55e'
    case 'APPROVED':           return '#3b82f6'
    case 'PLANNING':           return '#f59e0b'
    case 'COMMISSIONING':      return '#8b5cf6'
    case 'OPERATIONAL':        return '#6b7280'
    default:                   return '#94a3b8'
  }
}

function ispActionBadge(action: string) {
  const map: Record<string, string> = {
    COMMITTED:       'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300',
    ACTIONABLE_ISP:  'bg-blue-100 text-blue-800 dark:bg-blue-900/40 dark:text-blue-300',
    IDENTIFIED_NEED: 'bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300',
    FUTURE_ISP:      'bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300',
  }
  const label = action.replace(/_/g, ' ')
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-medium ${map[action] ?? 'bg-gray-100 text-gray-700'}`}>
      {label}
    </span>
  )
}

function statusBadge(status: string) {
  const map: Record<string, string> = {
    UNDER_CONSTRUCTION: 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300',
    APPROVED:           'bg-blue-100 text-blue-800 dark:bg-blue-900/40 dark:text-blue-300',
    PLANNING:           'bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300',
    COMMISSIONING:      'bg-purple-100 text-purple-800 dark:bg-purple-900/40 dark:text-purple-300',
    OPERATIONAL:        'bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300',
  }
  const label = status.replace(/_/g, ' ')
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-medium ${map[status] ?? 'bg-gray-100 text-gray-700'}`}>
      {label}
    </span>
  )
}

function milestoneBadge(m: IspProjectMilestone) {
  const map: Record<string, string> = {
    COMPLETE:    'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300',
    IN_PROGRESS: 'bg-blue-100 text-blue-800 dark:bg-blue-900/40 dark:text-blue-300',
    UPCOMING:    'bg-gray-100 text-gray-600 dark:bg-gray-700 dark:text-gray-300',
    DELAYED:     'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300',
  }
  const cls = map[m.status] ?? 'bg-gray-100 text-gray-600'
  const delayLabel = m.delay_months > 0 ? ` (+${m.delay_months}m)` : ''
  return (
    <span key={m.milestone_id} className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium mr-1 mb-1 ${cls}`}>
      {m.milestone_name}{delayLabel}
    </span>
  )
}

function bcrColor(bcr: number): string {
  if (bcr >= 2) return 'text-green-600 dark:text-green-400 font-semibold'
  if (bcr >= 1.2) return 'text-amber-600 dark:text-amber-400 font-semibold'
  return 'text-red-600 dark:text-red-400 font-semibold'
}

function typeBadge(type: string) {
  const map: Record<string, string> = {
    NEW_LINE:       'bg-indigo-100 text-indigo-800 dark:bg-indigo-900/40 dark:text-indigo-300',
    UPGRADE:        'bg-cyan-100 text-cyan-800 dark:bg-cyan-900/40 dark:text-cyan-300',
    SUBMARINE_CABLE:'bg-teal-100 text-teal-800 dark:bg-teal-900/40 dark:text-teal-300',
    SUBSEA_HVDC:    'bg-violet-100 text-violet-800 dark:bg-violet-900/40 dark:text-violet-300',
  }
  const label = type.replace(/_/g, ' ')
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-medium ${map[type] ?? 'bg-gray-100 text-gray-700'}`}>
      {label}
    </span>
  )
}

// ---------------------------------------------------------------------------
// KPI Card
// ---------------------------------------------------------------------------

interface KpiCardProps {
  title: string
  value: string
  sub?: string
  variant?: 'default' | 'red' | 'green'
  Icon: React.ElementType
}

function KpiCard({ title, value, sub, variant = 'default', Icon }: KpiCardProps) {
  const border = variant === 'red' ? 'border-red-400' : variant === 'green' ? 'border-green-400' : 'border-blue-400'
  const iconCls = variant === 'red' ? 'text-red-500' : variant === 'green' ? 'text-green-500' : 'text-blue-500'
  return (
    <div className={`bg-white dark:bg-gray-800 rounded-xl border-l-4 ${border} p-4 shadow-sm`}>
      <div className="flex items-center justify-between mb-1">
        <span className="text-xs text-gray-500 dark:text-gray-400 font-medium">{title}</span>
        <Icon size={16} className={iconCls} />
      </div>
      <div className="text-2xl font-bold text-gray-900 dark:text-white">{value}</div>
      {sub && <div className="text-xs text-gray-400 mt-0.5">{sub}</div>}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Capex BarChart
// ---------------------------------------------------------------------------

function CapexChart({ projects }: { projects: IspMajorProject[] }) {
  const data = projects.map(p => ({
    name: p.project_name.length > 12 ? p.project_name.substring(0, 12) + '…' : p.project_name,
    capex: parseFloat((p.total_capex_m_aud / 1000).toFixed(2)),
    color: statusColor(p.current_status),
    fullName: p.project_name,
    status: p.current_status,
  }))

  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl p-4 shadow-sm">
      <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-200 mb-1">
        Project Capex Pipeline ($B AUD)
      </h2>
      <p className="text-xs text-gray-400 mb-3">Total capital expenditure by project, colour-coded by construction status</p>
      <ResponsiveContainer width="100%" height={220}>
        <BarChart data={data} margin={{ top: 4, right: 16, left: 0, bottom: 60 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
          <XAxis
            dataKey="name"
            tick={{ fontSize: 10 }}
            angle={-35}
            textAnchor="end"
            interval={0}
          />
          <YAxis tick={{ fontSize: 11 }} tickFormatter={v => `$${v}B`} />
          <Tooltip
            formatter={(value: number, _name: string, props: { payload?: { fullName?: string; status?: string } }) => [
              `$${value}B`,
              props.payload?.fullName ?? 'Capex',
            ]}
            labelFormatter={(_label, payload) => payload?.[0]?.payload?.fullName ?? _label}
          />
          <Bar dataKey="capex" radius={[4, 4, 0, 0]}>
            {data.map((entry, idx) => (
              <Cell key={idx} fill={entry.color} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
      {/* Legend */}
      <div className="flex flex-wrap gap-3 mt-2">
        {[
          { label: 'Under Construction', color: '#22c55e' },
          { label: 'Approved', color: '#3b82f6' },
          { label: 'Planning', color: '#f59e0b' },
          { label: 'Commissioning', color: '#8b5cf6' },
          { label: 'Operational', color: '#6b7280' },
        ].map(({ label, color }) => (
          <span key={label} className="flex items-center gap-1 text-xs text-gray-500 dark:text-gray-400">
            <span className="inline-block w-3 h-3 rounded-sm" style={{ backgroundColor: color }} />
            {label}
          </span>
        ))}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Milestone Row (expandable)
// ---------------------------------------------------------------------------

function MilestoneRow({ project }: { project: IspMajorProject }) {
  const [open, setOpen] = useState(false)
  return (
    <>
      <tr className="border-t border-gray-100 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-700/40">
        <td className="py-2.5 px-3">
          <button
            onClick={() => setOpen(o => !o)}
            className="flex items-center gap-1 text-sm font-medium text-gray-900 dark:text-white hover:text-blue-600 dark:hover:text-blue-400"
          >
            {open ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
            {project.project_name}
          </button>
        </td>
        <td className="py-2.5 px-3">
          <span className="px-2 py-0.5 rounded bg-gray-100 dark:bg-gray-700 text-xs font-medium text-gray-700 dark:text-gray-300">
            {project.tnsp}
          </span>
        </td>
        <td className="py-2.5 px-3">
          <div className="flex flex-wrap gap-1">
            {project.regions_connected.map(r => (
              <span key={r} className="px-1.5 py-0.5 rounded bg-blue-50 dark:bg-blue-900/30 text-xs text-blue-700 dark:text-blue-300">
                {r}
              </span>
            ))}
          </div>
        </td>
        <td className="py-2.5 px-3">{typeBadge(project.project_type)}</td>
        <td className="py-2.5 px-3">{ispActionBadge(project.isp_action)}</td>
        <td className="py-2.5 px-3 text-sm text-gray-700 dark:text-gray-300">
          ${(project.total_capex_m_aud / 1000).toFixed(2)}B
        </td>
        <td className="py-2.5 px-3">
          <div className="flex items-center gap-2">
            <div className="w-20 bg-gray-200 dark:bg-gray-600 rounded-full h-1.5">
              <div
                className="h-1.5 rounded-full bg-blue-500"
                style={{ width: `${Math.min(100, project.overall_progress_pct)}%` }}
              />
            </div>
            <span className="text-xs text-gray-500 dark:text-gray-400 whitespace-nowrap">
              {project.overall_progress_pct.toFixed(0)}%
            </span>
          </div>
        </td>
        <td className="py-2.5 px-3 text-sm text-gray-600 dark:text-gray-400">
          {project.commissioning_date}
        </td>
        <td className={`py-2.5 px-3 text-sm ${bcrColor(project.bcr)}`}>
          {project.bcr.toFixed(2)}x
        </td>
        <td className="py-2.5 px-3">{statusBadge(project.current_status)}</td>
      </tr>
      {open && (
        <tr className="bg-gray-50 dark:bg-gray-700/20">
          <td colSpan={10} className="py-3 px-6">
            <div className="text-xs text-gray-500 dark:text-gray-400 mb-1.5 font-medium">
              Project Milestones — {project.circuit_km} km / {project.voltage_kv} kV / {project.thermal_limit_mw.toLocaleString()} MW
              {project.rit_t_complete ? (
                <span className="ml-3 px-2 py-0.5 rounded bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300">RIT-T Complete</span>
              ) : (
                <span className="ml-3 px-2 py-0.5 rounded bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-300">RIT-T Pending</span>
              )}
              <span className="ml-3 text-gray-400">BCR Benefit: ${(project.net_market_benefit_m_aud / 1000).toFixed(2)}B NPV</span>
            </div>
            <div className="flex flex-wrap">
              {project.milestones.map(m => milestoneBadge(m))}
            </div>
          </td>
        </tr>
      )}
    </>
  )
}

// ---------------------------------------------------------------------------
// ISP Projects Table
// ---------------------------------------------------------------------------

function IspProjectsTable({ projects }: { projects: IspMajorProject[] }) {
  const [statusFilter, setStatusFilter] = useState('')
  const [actionFilter, setActionFilter] = useState('')

  const statuses = [...new Set(projects.map(p => p.current_status))]
  const actions = [...new Set(projects.map(p => p.isp_action))]

  const filtered = projects
    .filter(p => !statusFilter || p.current_status === statusFilter)
    .filter(p => !actionFilter || p.isp_action === actionFilter)

  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm">
      <div className="flex flex-wrap items-center justify-between gap-3 px-4 py-3 border-b border-gray-100 dark:border-gray-700">
        <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-200">
          ISP Major Projects ({filtered.length})
        </h2>
        <div className="flex gap-2">
          <select
            value={statusFilter}
            onChange={e => setStatusFilter(e.target.value)}
            className="text-xs border border-gray-200 dark:border-gray-600 rounded px-2 py-1 bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-200"
          >
            <option value="">All Statuses</option>
            {statuses.map(s => <option key={s} value={s}>{s.replace(/_/g, ' ')}</option>)}
          </select>
          <select
            value={actionFilter}
            onChange={e => setActionFilter(e.target.value)}
            className="text-xs border border-gray-200 dark:border-gray-600 rounded px-2 py-1 bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-200"
          >
            <option value="">All ISP Actions</option>
            {actions.map(a => <option key={a} value={a}>{a.replace(/_/g, ' ')}</option>)}
          </select>
        </div>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-left min-w-[900px]">
          <thead>
            <tr className="text-xs text-gray-500 dark:text-gray-400 border-b border-gray-100 dark:border-gray-700">
              <th className="py-2 px-3 font-medium">Project</th>
              <th className="py-2 px-3 font-medium">TNSP</th>
              <th className="py-2 px-3 font-medium">Regions</th>
              <th className="py-2 px-3 font-medium">Type</th>
              <th className="py-2 px-3 font-medium">ISP Action</th>
              <th className="py-2 px-3 font-medium">Capex</th>
              <th className="py-2 px-3 font-medium">Progress</th>
              <th className="py-2 px-3 font-medium">Commissioning</th>
              <th className="py-2 px-3 font-medium">BCR</th>
              <th className="py-2 px-3 font-medium">Status</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map(p => (
              <MilestoneRow key={p.project_id} project={p} />
            ))}
            {filtered.length === 0 && (
              <tr>
                <td colSpan={10} className="py-8 text-center text-sm text-gray-400">
                  No projects match the selected filters.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// TNSP Programs Table
// ---------------------------------------------------------------------------

function TnspProgramsTable({ programs }: { programs: TnspCapexProgram[] }) {
  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm">
      <div className="px-4 py-3 border-b border-gray-100 dark:border-gray-700">
        <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-200">
          TNSP Regulatory Capex Programs
        </h2>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-left min-w-[700px]">
          <thead>
            <tr className="text-xs text-gray-500 dark:text-gray-400 border-b border-gray-100 dark:border-gray-700">
              <th className="py-2 px-3 font-medium">TNSP</th>
              <th className="py-2 px-3 font-medium">Period</th>
              <th className="py-2 px-3 font-medium">States</th>
              <th className="py-2 px-3 font-medium">Approved ($M)</th>
              <th className="py-2 px-3 font-medium">Spent ($M)</th>
              <th className="py-2 px-3 font-medium">Remaining ($M)</th>
              <th className="py-2 px-3 font-medium">Spend Progress</th>
              <th className="py-2 px-3 font-medium">Major Projects</th>
              <th className="py-2 px-3 font-medium">Regulator</th>
            </tr>
          </thead>
          <tbody>
            {programs.map(prog => {
              const spendPct = ((prog.spent_to_date_m_aud / prog.total_approved_capex_m_aud) * 100).toFixed(0)
              return (
                <tr
                  key={prog.tnsp}
                  className="border-t border-gray-100 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-700/40"
                >
                  <td className="py-2.5 px-3 text-sm font-medium text-gray-900 dark:text-white">
                    {prog.tnsp}
                  </td>
                  <td className="py-2.5 px-3 text-sm text-gray-600 dark:text-gray-400">
                    {prog.regulatory_period}
                  </td>
                  <td className="py-2.5 px-3">
                    <div className="flex flex-wrap gap-1">
                      {prog.states.map(s => (
                        <span key={s} className="px-1.5 py-0.5 rounded bg-blue-50 dark:bg-blue-900/30 text-xs text-blue-700 dark:text-blue-300">
                          {s}
                        </span>
                      ))}
                    </div>
                  </td>
                  <td className="py-2.5 px-3 text-sm text-gray-700 dark:text-gray-300">
                    ${prog.total_approved_capex_m_aud.toLocaleString()}M
                  </td>
                  <td className="py-2.5 px-3 text-sm text-gray-700 dark:text-gray-300">
                    ${prog.spent_to_date_m_aud.toLocaleString()}M
                  </td>
                  <td className="py-2.5 px-3 text-sm text-gray-700 dark:text-gray-300">
                    ${prog.remaining_m_aud.toLocaleString()}M
                  </td>
                  <td className="py-2.5 px-3">
                    <div className="flex items-center gap-2">
                      <div className="w-24 bg-gray-200 dark:bg-gray-600 rounded-full h-1.5">
                        <div
                          className="h-1.5 rounded-full bg-blue-500"
                          style={{ width: `${Math.min(100, parseFloat(spendPct))}%` }}
                        />
                      </div>
                      <span className="text-xs text-gray-500 dark:text-gray-400">{spendPct}%</span>
                      <span className="text-xs text-gray-400">(rate {prog.spend_rate_pct}%)</span>
                    </div>
                  </td>
                  <td className="py-2.5 px-3">
                    <div className="flex flex-wrap gap-1">
                      {prog.major_projects.map(mp => (
                        <span key={mp} className="px-1.5 py-0.5 rounded bg-gray-100 dark:bg-gray-700 text-xs text-gray-600 dark:text-gray-300">
                          {mp}
                        </span>
                      ))}
                    </div>
                  </td>
                  <td className="py-2.5 px-3">
                    <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                      prog.regulatory_body === 'AER'
                        ? 'bg-indigo-100 text-indigo-800 dark:bg-indigo-900/40 dark:text-indigo-300'
                        : 'bg-teal-100 text-teal-800 dark:bg-teal-900/40 dark:text-teal-300'
                    }`}>
                      {prog.regulatory_body}
                    </span>
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------

export default function IspTracker() {
  const [dashboard, setDashboard] = useState<IspDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    setLoading(true)
    api.getIspDashboard()
      .then(d => {
        setDashboard(d)
        setLoading(false)
      })
      .catch(err => {
        setError(err.message ?? 'Failed to load ISP data')
        setLoading(false)
      })
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-sm text-gray-500 dark:text-gray-400 animate-pulse">Loading ISP data…</div>
      </div>
    )
  }

  if (error || !dashboard) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-sm text-red-500">{error ?? 'No data available'}</div>
      </div>
    )
  }

  return (
    <div className="p-6 space-y-6 max-w-[1400px] mx-auto">
      {/* Header */}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-3">
          <MapPin size={22} className="text-blue-500" />
          <div>
            <h1 className="text-xl font-bold text-gray-900 dark:text-white">
              ISP Transmission Investment Tracker
            </h1>
            <p className="text-xs text-gray-400 mt-0.5">
              AEMO Integrated System Plan — major augmentation projects &amp; TNSP capital programs
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <span className="px-3 py-1 rounded-full bg-blue-100 dark:bg-blue-900/40 text-blue-800 dark:text-blue-300 text-xs font-semibold">
            AEMO 2024 ISP
          </span>
          <span className="text-xs text-gray-400">{dashboard.timestamp}</span>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KpiCard
          title="Total Pipeline Capex"
          value={`$${dashboard.total_pipeline_capex_bn_aud.toFixed(1)}B`}
          sub="AUD across all ISP projects"
          Icon={Hammer}
        />
        <KpiCard
          title="Projects Under Construction"
          value={String(dashboard.projects_under_construction)}
          sub={`of ${dashboard.isp_projects.length} total projects`}
          variant="green"
          Icon={MapPin}
        />
        <KpiCard
          title="Delayed Projects"
          value={String(dashboard.delayed_projects)}
          sub="projects with milestone delays"
          variant={dashboard.delayed_projects > 0 ? 'red' : 'default'}
          Icon={MapPin}
        />
        <KpiCard
          title="Total New Capacity"
          value={`${(dashboard.total_new_capacity_mw / 1000).toFixed(1)} GW`}
          sub={`${dashboard.total_new_km.toLocaleString()} km new / upgraded circuit`}
          Icon={MapPin}
        />
      </div>

      {/* Capex BarChart */}
      <CapexChart projects={dashboard.isp_projects} />

      {/* ISP Projects Table with Milestone Timeline */}
      <IspProjectsTable projects={dashboard.isp_projects} />

      {/* TNSP Programs Table */}
      <TnspProgramsTable programs={dashboard.tnsp_programs} />

      {/* Footer note */}
      <p className="text-xs text-gray-400 text-right">
        Data sourced from AEMO 2024 ISP and AER regulatory decisions. Mock data for demonstration.
      </p>
    </div>
  )
}
