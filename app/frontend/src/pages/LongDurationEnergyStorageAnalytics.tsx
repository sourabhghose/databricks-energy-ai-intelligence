import { useEffect, useState } from 'react'
import { Database, TrendingDown, Zap, BarChart2 } from 'lucide-react'
import {
  ScatterChart,
  Scatter,
  LineChart,
  Line,
  BarChart,
  Bar,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  Cell,
  LabelList,
} from 'recharts'
import {
  getLongDurationEnergyStorageDashboard,
  LDESXDashboard,
  LDESXTechnologyRecord,
  LDESXProjectRecord,
  LDESXEconomicsRecord,
  LDESXGridValueRecord,
  LDESXPolicyRecord,
  LDESXScenarioRecord,
} from '../api/client'

// ── KPI Card ───────────────────────────────────────────────────────────────
function KpiCard({
  label,
  value,
  sub,
  icon: Icon,
  colour,
}: {
  label: string
  value: string
  sub?: string
  icon: React.ElementType
  colour: string
}) {
  return (
    <div className="bg-gray-800 rounded-xl p-5 flex items-start gap-4 shadow">
      <div className={`p-3 rounded-lg ${colour}`}>
        <Icon size={22} className="text-white" />
      </div>
      <div>
        <p className="text-xs text-gray-400 uppercase tracking-wide">{label}</p>
        <p className="text-2xl font-bold text-white mt-0.5">{value}</p>
        {sub && <p className="text-xs text-gray-400 mt-0.5">{sub}</p>}
      </div>
    </div>
  )
}

// ── Section Header ─────────────────────────────────────────────────────────
function SectionHeader({ title, subtitle }: { title: string; subtitle?: string }) {
  return (
    <div className="mb-4">
      <h2 className="text-lg font-semibold text-white">{title}</h2>
      {subtitle && <p className="text-xs text-gray-400 mt-0.5">{subtitle}</p>}
    </div>
  )
}

const COLOURS = [
  '#f59e0b', '#3b82f6', '#10b981', '#ef4444', '#8b5cf6',
  '#06b6d4', '#f97316', '#84cc16', '#ec4899', '#14b8a6',
]

const TECH_COLOURS: Record<string, string> = {
  'Iron-Air Battery':    '#f59e0b',
  'Pumped Hydro New':    '#3b82f6',
  'Flow Battery VRF':    '#10b981',
  'Liquid Air (LAES)':   '#8b5cf6',
  'Hydrogen Storage':    '#06b6d4',
}

// ── Main Page ──────────────────────────────────────────────────────────────
export default function LongDurationEnergyStorageAnalytics() {
  const [data, setData] = useState<LDESXDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getLongDurationEnergyStorageDashboard()
      .then(setData)
      .catch((e) => setError(e.message ?? 'Failed to load data'))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full text-gray-400 text-sm">
        Loading Long Duration Energy Storage data...
      </div>
    )
  }
  if (error || !data) {
    return (
      <div className="flex items-center justify-center h-full text-red-400 text-sm">
        {error ?? 'No data available'}
      </div>
    )
  }

  const { technologies, projects, economics, grid_value, policies, scenarios, summary } = data

  // ── Chart 1: Technology Duration vs Efficiency (scatter) ──────────────
  const scatterData = technologies.map((t) => ({
    x: t.duration_h,
    y: t.round_trip_efficiency_pct,
    z: Math.max(20, Math.min(80, t.capex_kwh_2024 / 5)),
    name: t.technology,
  }))

  // ── Chart 2: LCOES Trajectories (line) ────────────────────────────────
  const econTechs = [...new Set(economics.map((e) => e.technology))]
  const lcoesByYear: Record<number, Record<string, number>> = {}
  economics.forEach((e) => {
    if (!lcoesByYear[e.year]) lcoesByYear[e.year] = { year: e.year }
    lcoesByYear[e.year][e.technology] = e.lcoes_aud_per_mwh
  })
  const lcoesData = Object.values(lcoesByYear).sort((a, b) => (a.year as number) - (b.year as number))

  // ── Chart 3: Project Pipeline by Status × State (stacked bar) ─────────
  const statusList = ['Concept', 'Development', 'Permitted', 'Construction', 'Operating']
  const stateMap: Record<string, Record<string, number>> = {}
  projects.forEach((p) => {
    if (!stateMap[p.state]) stateMap[p.state] = {}
    stateMap[p.state][p.status] = (stateMap[p.state][p.status] ?? 0) + p.energy_capacity_mwh
  })
  const pipelineData = Object.entries(stateMap).map(([state, sv]) => ({ state, ...sv }))

  // ── Chart 4: Grid Value by Service (horizontal bar) ──────────────────
  const serviceMap: Record<string, number> = {}
  grid_value.forEach((g) => {
    serviceMap[g.service_type] = (serviceMap[g.service_type] ?? 0) + g.annual_value_m_per_gw
  })
  const gridValueData = Object.entries(serviceMap)
    .map(([service, value]) => ({ service, value: Math.round(value) }))
    .sort((a, b) => b.value - a.value)

  // ── Chart 5: Policy Support Overview (bar) ───────────────────────────
  const policyTypeMap: Record<string, Record<string, number>> = {}
  policies.forEach((p) => {
    if (!policyTypeMap[p.policy_type]) policyTypeMap[p.policy_type] = {}
    policyTypeMap[p.policy_type][p.jurisdiction] = (policyTypeMap[p.policy_type][p.jurisdiction] ?? 0) + p.budget_m
  })
  const jurisdictions = [...new Set(policies.map((p) => p.jurisdiction))]
  const policyData = Object.entries(policyTypeMap).map(([ptype, jmap]) => ({
    policy_type: ptype,
    ...jmap,
  }))

  // ── Chart 6: Scenario Installed Capacity (area) ──────────────────────
  const scenYears = [...new Set(scenarios.map((s) => s.year))].sort()
  const scenDataMap: Record<number, Record<string, number>> = {}
  scenarios.forEach((s) => {
    if (!scenDataMap[s.year]) scenDataMap[s.year] = { year: s.year }
    scenDataMap[s.year][s.scenario] = s.installed_capacity_gw
  })
  const scenarioData = scenYears.map((y) => scenDataMap[y])

  return (
    <div className="p-6 space-y-8 bg-gray-900 min-h-full text-white">
      {/* Header */}
      <div className="flex items-center gap-3">
        <div className="p-2 bg-amber-500 rounded-lg">
          <Database size={24} className="text-white" />
        </div>
        <div>
          <h1 className="text-xl font-bold text-white">Long Duration Energy Storage Analytics</h1>
          <p className="text-xs text-gray-400 mt-0.5">
            LDES technologies (&gt;4 hours) — project pipeline, economics, grid services &amp; policy
          </p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KpiCard
          label="Total Technologies"
          value={String(summary.total_technologies)}
          sub="tracked LDES tech types"
          icon={Database}
          colour="bg-amber-500"
        />
        <KpiCard
          label="Total Pipeline"
          value={`${(Number(summary.total_pipeline_mwh) / 1000).toFixed(1)} GWh`}
          sub={`${summary.total_projects} projects`}
          icon={Zap}
          colour="bg-blue-500"
        />
        <KpiCard
          label="Avg LCOES (2024)"
          value={`$${summary.avg_lcoes_current_aud_mwh} /MWh`}
          sub="AUD, across 5 key techs"
          icon={TrendingDown}
          colour="bg-emerald-500"
        />
        <KpiCard
          label="Projected 2030 Capacity"
          value={`${summary.projected_2030_capacity_gw} GW`}
          sub="Base scenario installed"
          icon={BarChart2}
          colour="bg-purple-500"
        />
      </div>

      {/* Chart 1: Technology Duration vs Efficiency */}
      <div className="bg-gray-800 rounded-xl p-5 shadow">
        <SectionHeader
          title="Technology Duration vs Round-Trip Efficiency"
          subtitle="Bubble size proportional to capex ($/kWh). X = duration (hours), Y = RTE (%)"
        />
        <ResponsiveContainer width="100%" height={360}>
          <ScatterChart margin={{ top: 10, right: 30, left: 0, bottom: 30 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis
              dataKey="x"
              name="Duration (h)"
              type="number"
              label={{ value: 'Duration (hours)', position: 'insideBottom', offset: -20, fill: '#9ca3af', fontSize: 11 }}
              tick={{ fill: '#9ca3af', fontSize: 11 }}
              domain={[0, 750]}
            />
            <YAxis
              dataKey="y"
              name="RTE (%)"
              type="number"
              label={{ value: 'Round-Trip Efficiency (%)', angle: -90, position: 'insideLeft', offset: 10, fill: '#9ca3af', fontSize: 11 }}
              tick={{ fill: '#9ca3af', fontSize: 11 }}
              domain={[30, 95]}
            />
            <Tooltip
              cursor={{ strokeDasharray: '3 3' }}
              content={({ active, payload }) => {
                if (active && payload && payload.length) {
                  const d = payload[0].payload
                  return (
                    <div className="bg-gray-900 text-white text-xs p-2 rounded shadow border border-gray-700">
                      <p className="font-bold">{d.name}</p>
                      <p>Duration: {d.x}h</p>
                      <p>RTE: {d.y}%</p>
                    </div>
                  )
                }
                return null
              }}
            />
            <Scatter data={scatterData} fill="#f59e0b">
              {scatterData.map((entry, idx) => (
                <Cell key={idx} fill={COLOURS[idx % COLOURS.length]} />
              ))}
            </Scatter>
          </ScatterChart>
        </ResponsiveContainer>
        <div className="mt-2 flex flex-wrap gap-2">
          {scatterData.map((d, i) => (
            <span key={d.name} className="text-xs flex items-center gap-1">
              <span className="w-2 h-2 rounded-full inline-block" style={{ backgroundColor: COLOURS[i % COLOURS.length] }} />
              <span className="text-gray-300">{d.name}</span>
            </span>
          ))}
        </div>
      </div>

      {/* Chart 2: LCOES Trajectories */}
      <div className="bg-gray-800 rounded-xl p-5 shadow">
        <SectionHeader
          title="LCOES Cost Trajectories 2024–2029"
          subtitle="Levelised cost of energy storage (AUD/MWh) declining across 5 key technologies"
        />
        <ResponsiveContainer width="100%" height={320}>
          <LineChart data={lcoesData} margin={{ top: 10, right: 30, left: 0, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="year" tick={{ fill: '#9ca3af', fontSize: 11 }} />
            <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} label={{ value: 'AUD/MWh', angle: -90, position: 'insideLeft', fill: '#9ca3af', fontSize: 11 }} />
            <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }} labelStyle={{ color: '#f9fafb' }} itemStyle={{ color: '#d1d5db' }} />
            <Legend wrapperStyle={{ fontSize: 11, color: '#9ca3af' }} />
            {econTechs.map((tech, idx) => (
              <Line
                key={tech}
                type="monotone"
                dataKey={tech}
                stroke={TECH_COLOURS[tech] ?? COLOURS[idx % COLOURS.length]}
                strokeWidth={2}
                dot={{ r: 4 }}
                activeDot={{ r: 6 }}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 3: Project Pipeline by Status */}
      <div className="bg-gray-800 rounded-xl p-5 shadow">
        <SectionHeader
          title="Project Pipeline by Status and State"
          subtitle="Energy capacity (MWh) stacked by development status across NEM regions"
        />
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={pipelineData} margin={{ top: 10, right: 20, left: 0, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="state" tick={{ fill: '#9ca3af', fontSize: 11 }} />
            <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} label={{ value: 'MWh', angle: -90, position: 'insideLeft', fill: '#9ca3af', fontSize: 11 }} />
            <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }} labelStyle={{ color: '#f9fafb' }} itemStyle={{ color: '#d1d5db' }} />
            <Legend wrapperStyle={{ fontSize: 11, color: '#9ca3af' }} />
            {statusList.map((status, idx) => (
              <Bar key={status} dataKey={status} stackId="a" fill={COLOURS[idx % COLOURS.length]} />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 4: Grid Value by Service */}
      <div className="bg-gray-800 rounded-xl p-5 shadow">
        <SectionHeader
          title="Grid Value by Service Type"
          subtitle="Total annual value (AUD M/GW) summed across all regions per service type"
        />
        <ResponsiveContainer width="100%" height={280}>
          <BarChart
            data={gridValueData}
            layout="vertical"
            margin={{ top: 5, right: 40, left: 140, bottom: 5 }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" horizontal={false} />
            <XAxis type="number" tick={{ fill: '#9ca3af', fontSize: 11 }} label={{ value: 'AUD M/GW', position: 'insideBottom', offset: -5, fill: '#9ca3af', fontSize: 11 }} />
            <YAxis dataKey="service" type="category" tick={{ fill: '#9ca3af', fontSize: 11 }} width={135} />
            <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }} labelStyle={{ color: '#f9fafb' }} itemStyle={{ color: '#d1d5db' }} />
            <Bar dataKey="value" name="Annual Value (AUD M/GW)" radius={[0, 4, 4, 0]}>
              {gridValueData.map((_, idx) => (
                <Cell key={idx} fill={COLOURS[idx % COLOURS.length]} />
              ))}
              <LabelList dataKey="value" position="right" style={{ fill: '#9ca3af', fontSize: 10 }} />
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 5: Policy Support Overview */}
      <div className="bg-gray-800 rounded-xl p-5 shadow">
        <SectionHeader
          title="Policy Support Overview"
          subtitle="Budget (AUD M) by policy type and jurisdiction"
        />
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={policyData} margin={{ top: 10, right: 20, left: 0, bottom: 30 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="policy_type" tick={{ fill: '#9ca3af', fontSize: 10 }} angle={-20} textAnchor="end" interval={0} />
            <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} label={{ value: 'AUD M', angle: -90, position: 'insideLeft', fill: '#9ca3af', fontSize: 11 }} />
            <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }} labelStyle={{ color: '#f9fafb' }} itemStyle={{ color: '#d1d5db' }} />
            <Legend wrapperStyle={{ fontSize: 11, color: '#9ca3af' }} />
            {jurisdictions.slice(0, 8).map((jur, idx) => (
              <Bar key={jur} dataKey={jur} stackId="a" fill={COLOURS[idx % COLOURS.length]} />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 6: Scenario Installed Capacity */}
      <div className="bg-gray-800 rounded-xl p-5 shadow">
        <SectionHeader
          title="Scenario: LDES Installed Capacity 2025–2040"
          subtitle="Base vs High LDES scenario — installed capacity (GW)"
        />
        <ResponsiveContainer width="100%" height={320}>
          <AreaChart data={scenarioData} margin={{ top: 10, right: 30, left: 0, bottom: 5 }}>
            <defs>
              <linearGradient id="ldesxBase" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.5} />
                <stop offset="95%" stopColor="#3b82f6" stopOpacity={0.05} />
              </linearGradient>
              <linearGradient id="ldesxHigh" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#f59e0b" stopOpacity={0.5} />
                <stop offset="95%" stopColor="#f59e0b" stopOpacity={0.05} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="year" tick={{ fill: '#9ca3af', fontSize: 11 }} />
            <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} label={{ value: 'Installed GW', angle: -90, position: 'insideLeft', fill: '#9ca3af', fontSize: 11 }} />
            <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }} labelStyle={{ color: '#f9fafb' }} itemStyle={{ color: '#d1d5db' }} />
            <Legend wrapperStyle={{ fontSize: 11, color: '#9ca3af' }} />
            <Area type="monotone" dataKey="Base" stroke="#3b82f6" fill="url(#ldesxBase)" strokeWidth={2} />
            <Area type="monotone" dataKey="High LDES" stroke="#f59e0b" fill="url(#ldesxHigh)" strokeWidth={2} />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}
