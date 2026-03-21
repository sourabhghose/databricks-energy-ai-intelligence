import { useEffect, useState } from 'react'
import {
  BarChart, Bar, LineChart, Line,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from 'recharts'
import { Thermometer } from 'lucide-react'
import {
  getGeothermalEnergyDevelopmentDashboard,
  GEDADashboard,
  GEDAGlobalBenchmarkRecord,
} from '../api/client'

// ── colour palette ──────────────────────────────────────────────────────────
const COLOURS = {
  hdr:         '#ef4444',
  hydrothermal:'#3b82f6',
  geopressured:'#f59e0b',
  volcanic:    '#a78bfa',
  egs:         '#ef4444',
  directHeat:  '#10b981',
  coproduced:  '#f59e0b',
  districtHeat:'#6366f1',
  binaryCycle: '#3b82f6',
  proven:      '#10b981',
  emerging:    '#f59e0b',
  experimental:'#ef4444',
  slow:        '#64748b',
  accel:       '#22c55e',
  suitable:    '#10b981',
  unsuitable:  '#ef4444',
}

const RESOURCE_TYPE_COLOURS: Record<string, string> = {
  'Hot Dry Rock':  COLOURS.hdr,
  'Hydrothermal':  COLOURS.hydrothermal,
  'Geopressured':  COLOURS.geopressured,
  'Volcanic':      COLOURS.volcanic,
}

const MATURITY_COLOURS: Record<string, string> = {
  'Proven':       COLOURS.proven,
  'Emerging':     COLOURS.emerging,
  'Experimental': COLOURS.experimental,
}

const SCENARIO_COLOURS: Record<string, string> = {
  'Slow Development': COLOURS.slow,
  'Accelerated':      COLOURS.accel,
}

// ── KPI card ────────────────────────────────────────────────────────────────
function KPICard({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="bg-gray-800 rounded-xl p-5 border border-gray-700">
      <p className="text-xs text-gray-400 uppercase tracking-wider mb-1">{label}</p>
      <p className="text-2xl font-bold text-white">{value}</p>
      {sub && <p className="text-xs text-gray-500 mt-1">{sub}</p>}
    </div>
  )
}

// ── chart wrapper ────────────────────────────────────────────────────────────
function ChartCard({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="bg-gray-800 rounded-xl p-5 border border-gray-700">
      <h3 className="text-sm font-semibold text-gray-300 mb-4">{title}</h3>
      {children}
    </div>
  )
}

// ── main component ───────────────────────────────────────────────────────────
export default function GeothermalEnergyDevelopmentAnalytics() {
  const [data, setData] = useState<GEDADashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getGeothermalEnergyDevelopmentDashboard()
      .then(setData)
      .catch((e: unknown) => setError(String(e)))
      .finally(() => setLoading(false))
  }, [])

  if (loading) return (
    <div className="flex items-center justify-center h-64 text-gray-400">
      Loading Geothermal Energy Development data…
    </div>
  )
  if (error || !data) return (
    <div className="flex items-center justify-center h-64 text-red-400">
      {error ?? 'No data available'}
    </div>
  )

  const { resources, projects, costs, global_benchmarks, heat_applications, scenarios, summary } = data

  // ── Chart 1: Resource potential by state × resource_type ────────────────
  const stateResourceMap: Record<string, Record<string, number>> = {}
  for (const r of resources) {
    if (!stateResourceMap[r.state]) stateResourceMap[r.state] = {}
    stateResourceMap[r.state][r.resource_type] = (stateResourceMap[r.state][r.resource_type] ?? 0) + r.estimated_potential_mw
  }
  const resourceByState = Object.entries(stateResourceMap).map(([state, types]) => ({ state, ...types }))
  const resourceTypes = [...new Set(resources.map(r => r.resource_type))]

  // ── Chart 2: Cost trajectory lcoe by technology_variant ─────────────────
  const costByYear: Record<number, Record<string, number>> = {}
  for (const c of costs) {
    if (!costByYear[c.year]) costByYear[c.year] = { year: c.year }
    costByYear[c.year][c.technology_variant] = c.lcoe_aud_per_mwh
  }
  const costTrajectory = Object.values(costByYear).sort((a, b) => (a.year as number) - (b.year as number))
  const techVariants = [...new Set(costs.map(c => c.technology_variant))]
  const TECH_COLOURS = ['#ef4444', '#3b82f6', '#f59e0b', '#10b981', '#a78bfa', '#f472b6']

  // ── Chart 3: Global installed capacity by country ────────────────────────
  const globalSorted = [...global_benchmarks].sort((a, b) => b.installed_capacity_mw - a.installed_capacity_mw)

  // ── Chart 4: Project pipeline capacity by status × project_type ─────────
  const pipelineMap: Record<string, Record<string, number>> = {}
  for (const p of projects) {
    if (!pipelineMap[p.status]) pipelineMap[p.status] = {}
    pipelineMap[p.status][p.project_type] = (pipelineMap[p.status][p.project_type] ?? 0) + p.capacity_mw_e
  }
  const pipeline = Object.entries(pipelineMap).map(([status, types]) => ({ status, ...types }))
  const projTypes = [...new Set(projects.map(p => p.project_type))]
  const PROJ_COLOURS = ['#ef4444', '#3b82f6', '#f59e0b', '#10b981', '#a78bfa']

  // ── Chart 5: Heat applications potential sites ───────────────────────────
  const heatChartData = heat_applications.map(h => ({
    application: h.application,
    potential_sites: h.potential_sites,
    suitable: h.geothermal_suitable,
    existing_installations: h.existing_installations,
  }))

  // ── Chart 6: Scenario capacity 2025-2050 ────────────────────────────────
  const scenarioByYear: Record<number, Record<string, number>> = {}
  for (const s of scenarios) {
    if (!scenarioByYear[s.year]) scenarioByYear[s.year] = { year: s.year }
    scenarioByYear[s.year][s.scenario] = s.installed_capacity_mw
  }
  const scenarioChart = Object.values(scenarioByYear).sort((a, b) => (a.year as number) - (b.year as number))
  const scenarioNames = [...new Set(scenarios.map(s => s.scenario))]

  return (
    <div className="p-6 space-y-6 bg-gray-900 min-h-screen">
      {/* Header */}
      <div className="flex items-center gap-3 mb-2">
        <Thermometer className="text-red-400" size={28} />
        <div>
          <h1 className="text-xl font-bold text-white">Geothermal Energy Development Analytics</h1>
          <p className="text-sm text-gray-400">
            Australia HDR/EGS, Hydrothermal, Direct-Use Heat — Resource Assessment, Project Economics &amp; Global Benchmarks
          </p>
        </div>
      </div>

      {/* KPI cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KPICard
          label="Total Resource Potential"
          value={`${((summary.total_resource_potential_mw as number) / 1000).toFixed(1)} GW`}
          sub="Identified geothermal potential"
        />
        <KPICard
          label="Total Projects"
          value={String(summary.total_projects)}
          sub="All stages"
        />
        <KPICard
          label="Avg LCOE (AUD/MWh)"
          value={`$${Number(summary.avg_lcoe_current_aud_per_mwh).toFixed(0)}`}
          sub="Current cost estimate"
        />
        <KPICard
          label="Leading Country (MW)"
          value={`${Number(summary.leading_country_mw).toLocaleString()} MW`}
          sub="Global installed capacity leader"
        />
      </div>

      {/* Charts row 1 */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Chart 1: Resource Map by State */}
        <ChartCard title="Resource Map by State (Estimated Potential MW)">
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={resourceByState} margin={{ top: 4, right: 16, left: 0, bottom: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="state" tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} unit=" MW" />
              <Tooltip
                contentStyle={{ background: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                labelStyle={{ color: '#f9fafb' }}
              />
              <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 11 }} />
              {resourceTypes.map((rt, idx) => (
                <Bar key={rt} dataKey={rt} stackId="a"
                  fill={RESOURCE_TYPE_COLOURS[rt] ?? TECH_COLOURS[idx % TECH_COLOURS.length]}
                />
              ))}
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        {/* Chart 2: Cost Trajectory */}
        <ChartCard title="Cost Trajectory — LCOE by Technology Variant (AUD/MWh)">
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={costTrajectory} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="year" tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} unit=" $" />
              <Tooltip
                contentStyle={{ background: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                labelStyle={{ color: '#f9fafb' }}
              />
              <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 11 }} />
              {techVariants.map((tv, idx) => (
                <Line key={tv} type="monotone" dataKey={tv} dot={false}
                  stroke={TECH_COLOURS[idx % TECH_COLOURS.length]}
                  strokeWidth={2}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      {/* Charts row 2 */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Chart 3: Global Installed Capacity (horizontal) */}
        <ChartCard title="Global Installed Geothermal Capacity (MW) by Country">
          <ResponsiveContainer width="100%" height={320}>
            <BarChart
              layout="vertical"
              data={globalSorted}
              margin={{ top: 4, right: 24, left: 80, bottom: 4 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis type="number" tick={{ fill: '#9ca3af', fontSize: 11 }} unit=" MW" />
              <YAxis dataKey="country" type="category" tick={{ fill: '#9ca3af', fontSize: 11 }} width={76} />
              <Tooltip
                contentStyle={{ background: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                labelStyle={{ color: '#f9fafb' }}
                formatter={(val: number, _name: string, props: { payload?: GEDAGlobalBenchmarkRecord }) => [
                  `${val.toLocaleString()} MW`,
                  props.payload?.technology_maturity ?? '',
                ]}
              />
              <Bar dataKey="installed_capacity_mw" name="Installed MW">
                {globalSorted.map((entry) => (
                  <rect key={entry.country}
                    fill={MATURITY_COLOURS[entry.technology_maturity] ?? '#6b7280'}
                  />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
          <div className="flex gap-4 mt-2 text-xs text-gray-400">
            {Object.entries(MATURITY_COLOURS).map(([label, color]) => (
              <span key={label} className="flex items-center gap-1">
                <span className="w-3 h-3 rounded-sm inline-block" style={{ background: color }} />
                {label}
              </span>
            ))}
          </div>
        </ChartCard>

        {/* Chart 4: Project Pipeline */}
        <ChartCard title="Project Pipeline — Capacity (MWe) by Status and Type">
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={pipeline} margin={{ top: 4, right: 16, left: 0, bottom: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="status" tick={{ fill: '#9ca3af', fontSize: 11 }} angle={-20} textAnchor="end" />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} unit=" MW" />
              <Tooltip
                contentStyle={{ background: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                labelStyle={{ color: '#f9fafb' }}
              />
              <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 11 }} />
              {projTypes.map((pt, idx) => (
                <Bar key={pt} dataKey={pt} stackId="b"
                  fill={PROJ_COLOURS[idx % PROJ_COLOURS.length]}
                />
              ))}
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      {/* Charts row 3 */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Chart 5: Heat Applications */}
        <ChartCard title="Direct-Use Heat Applications — Potential Sites">
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={heatChartData} margin={{ top: 4, right: 16, left: 0, bottom: 40 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="application" tick={{ fill: '#9ca3af', fontSize: 10 }} angle={-30} textAnchor="end" />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <Tooltip
                contentStyle={{ background: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                labelStyle={{ color: '#f9fafb' }}
                formatter={(val: number, name: string, props: { payload?: { suitable: boolean } }) => [
                  val,
                  `${name} (${props.payload?.suitable ? 'Suitable' : 'Not Suitable'})`,
                ]}
              />
              <Bar dataKey="potential_sites" name="Potential Sites">
                {heatChartData.map((entry, idx) => (
                  <rect key={idx}
                    fill={entry.suitable ? COLOURS.suitable : COLOURS.unsuitable}
                  />
                ))}
              </Bar>
              <Bar dataKey="existing_installations" name="Existing Installations" fill="#6b7280" />
            </BarChart>
          </ResponsiveContainer>
          <div className="flex gap-4 mt-2 text-xs text-gray-400">
            <span className="flex items-center gap-1">
              <span className="w-3 h-3 rounded-sm inline-block" style={{ background: COLOURS.suitable }} />
              Geothermally Suitable
            </span>
            <span className="flex items-center gap-1">
              <span className="w-3 h-3 rounded-sm inline-block" style={{ background: COLOURS.unsuitable }} />
              Not Suitable
            </span>
          </div>
        </ChartCard>

        {/* Chart 6: Scenario Capacity */}
        <ChartCard title="Scenario: Installed Geothermal Capacity 2025–2050 (MW)">
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={scenarioChart} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="year" tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} unit=" MW" />
              <Tooltip
                contentStyle={{ background: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                labelStyle={{ color: '#f9fafb' }}
              />
              <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 11 }} />
              {scenarioNames.map(sn => (
                <Line key={sn} type="monotone" dataKey={sn} dot
                  stroke={SCENARIO_COLOURS[sn] ?? '#6b7280'}
                  strokeWidth={2}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      {/* Summary footer */}
      <div className="bg-gray-800 rounded-xl p-5 border border-gray-700 grid grid-cols-2 md:grid-cols-3 gap-4 text-sm">
        <div>
          <p className="text-gray-400 text-xs uppercase tracking-wider">Largest Resource Basin</p>
          <p className="text-white font-semibold">{String(summary.largest_resource_basin)}</p>
        </div>
        <div>
          <p className="text-gray-400 text-xs uppercase tracking-wider">Projected 2040 Capacity (Accelerated)</p>
          <p className="text-white font-semibold">{Number(summary.projected_2040_capacity_mw).toLocaleString()} MW</p>
        </div>
        <div>
          <p className="text-gray-400 text-xs uppercase tracking-wider">Total Resource Potential</p>
          <p className="text-white font-semibold">{((summary.total_resource_potential_mw as number) / 1000).toFixed(1)} GW identified</p>
        </div>
      </div>
    </div>
  )
}
