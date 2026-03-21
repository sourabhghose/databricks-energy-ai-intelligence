import { useEffect, useState } from 'react'
import { Radio, AlertTriangle } from 'lucide-react'
import {
  BarChart,
  Bar,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import {
  getElectricityExportCableDashboard,
  ECAIDashboard,
  ECAIProjectRecord,
  ECAICompetitorRecord,
} from '../api/client'

// ---- Helpers ----------------------------------------------------------------

function KpiCard({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="bg-gray-800 rounded-xl p-5 flex flex-col gap-1 border border-gray-700">
      <span className="text-xs text-gray-400 uppercase tracking-wider">{label}</span>
      <span className="text-2xl font-bold text-white">{value}</span>
      {sub && <span className="text-xs text-gray-500">{sub}</span>}
    </div>
  )
}

const STATUS_COLOURS: Record<string, string> = {
  Operating:    'bg-green-500 text-white',
  Construction: 'bg-blue-500 text-white',
  Approved:     'bg-purple-500 text-white',
  FEED:         'bg-yellow-500 text-gray-900',
  'Pre-FEED':   'bg-orange-500 text-white',
  Concept:      'bg-gray-500 text-white',
}

function StatusBadge({ status }: { status: string }) {
  const cls = STATUS_COLOURS[status] ?? 'bg-gray-600 text-white'
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-medium ${cls}`}>{status}</span>
  )
}

// ---- Main Page --------------------------------------------------------------

export default function ElectricityExportCableAnalytics() {
  const [data, setData] = useState<ECAIDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getElectricityExportCableDashboard()
      .then(setData)
      .catch(e => setError(e.message ?? 'Failed to load data'))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 bg-gray-900 text-gray-400">
        Loading Electricity Export Cable data...
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="flex items-center gap-2 justify-center h-64 bg-gray-900 text-red-400">
        <AlertTriangle className="w-5 h-5" />
        <span>{error ?? 'No data available'}</span>
      </div>
    )
  }

  const { projects, economics, technologies, markets, grid_impacts, competitors, summary } = data

  // ---- Derived chart data ----

  // Bar chart: capacity by destination country
  const capacityByCountry = projects.map(p => ({
    country: p.destination_country.split(' ')[0],
    capacity_gw: p.capacity_gw,
    status: p.project_status,
  }))

  // Line chart: IRR by scenario for top 4 projects
  const top4Projects = projects.slice(0, 4)
  const scenarioColors = ['#60a5fa', '#34d399', '#f87171', '#fbbf24']
  const scenarios = ['Base', 'High Price', 'Low Price', 'Delayed']

  // Build scenario IRR data keyed by project
  type ScenarioIrrRow = { project: string } & Record<string, number>
  const irrByScenario: ScenarioIrrRow[] = top4Projects.map(proj => {
    const row = { project: proj.project_name.split('—')[0].trim().replace('Sun Cable ', 'Sun Cable') } as unknown as ScenarioIrrRow
    scenarios.forEach(sc => {
      const rec = economics.find(e => e.project_id === proj.project_id && e.scenario === sc)
      row[sc] = rec ? rec.irr_pct : 0
    })
    return row
  })

  // Bar chart: technology losses vs capacity
  const techChartData = technologies.map(t => ({
    name: t.technology_name.replace(' (', '\n('),
    losses_pct: t.losses_pct_per_1000km,
    capacity_gw: t.max_capacity_gw,
  }))

  // Bar chart: market demand vs price
  const marketChartData = markets.map(m => ({
    country: m.destination_country.split(' ')[0],
    peak_demand_gw: m.peak_demand_gw,
    price_dolpermwh: m.current_electricity_price_dolpermwh,
    deficit_gw: m.renewable_deficit_gw,
  }))

  return (
    <div className="min-h-screen bg-gray-900 text-gray-100 p-6">
      {/* Page Header */}
      <div className="flex items-center gap-3 mb-6">
        <div className="p-2 bg-blue-600 rounded-lg">
          <Radio className="w-6 h-6 text-white" />
        </div>
        <div>
          <h1 className="text-2xl font-bold text-white">Electricity Export Cable Infrastructure Analytics</h1>
          <p className="text-sm text-gray-400">Australia-Asia HVDC Export Cable Pipeline</p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
        <KpiCard
          label="Pipeline Capacity"
          value={`${summary.total_pipeline_capacity_gw} GW`}
          sub="Total proposed export capacity"
        />
        <KpiCard
          label="Projects in Development"
          value={`${summary.projects_in_development}`}
          sub="Active projects"
        />
        <KpiCard
          label="Total CapEx"
          value={`A$${summary.total_capex_bn}B`}
          sub="Aggregate project capital"
        />
        <KpiCard
          label="Export Potential"
          value={`${summary.potential_export_twh_pa} TWh/yr`}
          sub={`Avg IRR (Base): ${summary.avg_irr_base_case_pct}%`}
        />
      </div>

      {/* Charts Row 1 */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
        {/* Project Capacity by Destination Country */}
        <div className="bg-gray-800 rounded-xl p-5 border border-gray-700">
          <h2 className="text-sm font-semibold text-gray-300 mb-4 uppercase tracking-wider">
            Project Capacity by Destination Country (GW)
          </h2>
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={capacityByCountry} margin={{ top: 4, right: 16, left: 0, bottom: 40 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="country" tick={{ fill: '#9ca3af', fontSize: 11 }} angle={-35} textAnchor="end" />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} unit=" GW" />
              <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', color: '#f9fafb' }} />
              <Bar dataKey="capacity_gw" fill="#3b82f6" name="Capacity (GW)" radius={[3, 3, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* IRR by Scenario — Top 4 Projects */}
        <div className="bg-gray-800 rounded-xl p-5 border border-gray-700">
          <h2 className="text-sm font-semibold text-gray-300 mb-4 uppercase tracking-wider">
            IRR by Scenario — Top 4 Projects (%)
          </h2>
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={irrByScenario} margin={{ top: 4, right: 16, left: 0, bottom: 60 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="project" tick={{ fill: '#9ca3af', fontSize: 10 }} angle={-30} textAnchor="end" />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} unit="%" />
              <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', color: '#f9fafb' }} />
              <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 11 }} />
              {scenarios.map((sc, i) => (
                <Bar key={sc} dataKey={sc} fill={scenarioColors[i]} name={sc} radius={[2, 2, 0, 0]} />
              ))}
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Charts Row 2 */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
        {/* Technology Comparison: Losses vs Capacity */}
        <div className="bg-gray-800 rounded-xl p-5 border border-gray-700">
          <h2 className="text-sm font-semibold text-gray-300 mb-4 uppercase tracking-wider">
            Technology Comparison — Losses (%) vs Max Capacity (GW)
          </h2>
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={techChartData} margin={{ top: 4, right: 16, left: 0, bottom: 60 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="name" tick={{ fill: '#9ca3af', fontSize: 9 }} angle={-30} textAnchor="end" />
              <YAxis yAxisId="left" tick={{ fill: '#9ca3af', fontSize: 11 }} unit="%" />
              <YAxis yAxisId="right" orientation="right" tick={{ fill: '#9ca3af', fontSize: 11 }} unit=" GW" />
              <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', color: '#f9fafb' }} />
              <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 11 }} />
              <Bar yAxisId="left" dataKey="losses_pct" fill="#f87171" name="Losses %/1000km" radius={[3, 3, 0, 0]} />
              <Bar yAxisId="right" dataKey="capacity_gw" fill="#34d399" name="Max Capacity GW" radius={[3, 3, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Market Attractiveness */}
        <div className="bg-gray-800 rounded-xl p-5 border border-gray-700">
          <h2 className="text-sm font-semibold text-gray-300 mb-4 uppercase tracking-wider">
            Market Attractiveness — Peak Demand (GW) vs Price ($/MWh)
          </h2>
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={marketChartData} margin={{ top: 4, right: 16, left: 0, bottom: 40 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="country" tick={{ fill: '#9ca3af', fontSize: 11 }} angle={-25} textAnchor="end" />
              <YAxis yAxisId="left" tick={{ fill: '#9ca3af', fontSize: 11 }} unit=" GW" />
              <YAxis yAxisId="right" orientation="right" tick={{ fill: '#9ca3af', fontSize: 11 }} unit=" $/MWh" />
              <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', color: '#f9fafb' }} />
              <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 11 }} />
              <Bar yAxisId="left" dataKey="peak_demand_gw" fill="#8b5cf6" name="Peak Demand GW" radius={[3, 3, 0, 0]} />
              <Bar yAxisId="right" dataKey="price_dolpermwh" fill="#fbbf24" name="Price $/MWh" radius={[3, 3, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Projects Table */}
      <div className="bg-gray-800 rounded-xl p-5 border border-gray-700 mb-6">
        <h2 className="text-sm font-semibold text-gray-300 mb-4 uppercase tracking-wider">
          All Projects
        </h2>
        <div className="overflow-x-auto">
          <table className="w-full text-sm text-left">
            <thead>
              <tr className="border-b border-gray-700 text-gray-400 text-xs uppercase">
                <th className="pb-2 pr-4">Project</th>
                <th className="pb-2 pr-4">Developer</th>
                <th className="pb-2 pr-4">Destination</th>
                <th className="pb-2 pr-4">State</th>
                <th className="pb-2 pr-4">Capacity</th>
                <th className="pb-2 pr-4">Length</th>
                <th className="pb-2 pr-4">CapEx</th>
                <th className="pb-2 pr-4">COD</th>
                <th className="pb-2 pr-4">Export TWh/yr</th>
                <th className="pb-2">Status</th>
              </tr>
            </thead>
            <tbody>
              {projects.map((p: ECAIProjectRecord) => (
                <tr key={p.project_id} className="border-b border-gray-700/50 hover:bg-gray-700/30 transition-colors">
                  <td className="py-2 pr-4 text-white font-medium max-w-xs truncate">{p.project_name}</td>
                  <td className="py-2 pr-4 text-gray-300">{p.developer}</td>
                  <td className="py-2 pr-4 text-gray-300">{p.destination_country}</td>
                  <td className="py-2 pr-4 text-gray-400">{p.origin_state}</td>
                  <td className="py-2 pr-4 text-blue-400 font-medium">{p.capacity_gw} GW</td>
                  <td className="py-2 pr-4 text-gray-300">{p.length_km.toLocaleString()} km</td>
                  <td className="py-2 pr-4 text-yellow-400">A${p.capex_bn}B</td>
                  <td className="py-2 pr-4 text-gray-300">{p.target_cod_year}</td>
                  <td className="py-2 pr-4 text-green-400">{p.estimated_export_twh_pa}</td>
                  <td className="py-2"><StatusBadge status={p.project_status} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Competitor Comparison Table */}
      <div className="bg-gray-800 rounded-xl p-5 border border-gray-700">
        <h2 className="text-sm font-semibold text-gray-300 mb-4 uppercase tracking-wider">
          Global Competitor Comparison
        </h2>
        <div className="overflow-x-auto">
          <table className="w-full text-sm text-left">
            <thead>
              <tr className="border-b border-gray-700 text-gray-400 text-xs uppercase">
                <th className="pb-2 pr-4">Country</th>
                <th className="pb-2 pr-4">Project</th>
                <th className="pb-2 pr-4">Capacity (GW)</th>
                <th className="pb-2 pr-4">Cost $/MWh</th>
                <th className="pb-2 pr-4">Maturity</th>
                <th className="pb-2 pr-4">AU Advantage</th>
                <th className="pb-2">Key Differentiator</th>
              </tr>
            </thead>
            <tbody>
              {competitors.map((c: ECAICompetitorRecord) => (
                <tr key={c.competitor_id} className="border-b border-gray-700/50 hover:bg-gray-700/30 transition-colors">
                  <td className="py-2 pr-4 text-white font-medium">{c.competitor_country}</td>
                  <td className="py-2 pr-4 text-gray-300">{c.project_name}</td>
                  <td className="py-2 pr-4 text-blue-400">{c.capacity_gw} GW</td>
                  <td className="py-2 pr-4 text-yellow-400">${c.cost_dolpermwh_export}</td>
                  <td className="py-2 pr-4">
                    <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                      c.maturity_level === 'Operating' ? 'bg-green-500 text-white' :
                      c.maturity_level === 'Advanced' ? 'bg-blue-500 text-white' :
                      'bg-gray-500 text-white'
                    }`}>{c.maturity_level}</span>
                  </td>
                  <td className="py-2 pr-4 text-green-400 font-semibold">+{c.australia_advantage_pct}%</td>
                  <td className="py-2 text-gray-400 text-xs">{c.key_differentiator}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
