import { useEffect, useState } from 'react'
import { MapPin } from 'lucide-react'
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
  getREZADashboard,
  REZADashboard,
} from '../api/client'

const TECH_COLORS: Record<string, string> = {
  Wind: '#6366f1',
  Solar: '#f59e0b',
  Battery: '#22c55e',
  Hybrid: '#ec4899',
}

const CAPACITY_COLORS = {
  declared: '#6366f1',
  connected: '#22c55e',
  pipeline: '#f59e0b',
}

const TX_STATUS_COLORS: Record<string, string> = {
  OPERATIONAL: '#22c55e',
  UNDER_CONSTRUCTION: '#6366f1',
  PLANNING: '#f59e0b',
  PROPOSED: '#9ca3af',
}

const STATUS_COLORS: Record<string, string> = {
  OPERATIONAL: '#22c55e',
  CONSTRUCTION: '#6366f1',
  APPROVED: '#22d3ee',
  APPLICATION: '#f59e0b',
}

export default function RenewableEnergyZoneDevelopmentAnalytics() {
  const [data, setData] = useState<REZADashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getREZADashboard()
      .then(setData)
      .catch((e) => setError(e.message ?? 'Failed to load data'))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 bg-gray-900 text-gray-300">
        <span className="text-lg">Loading REZ Development Analytics...</span>
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="flex items-center justify-center h-64 bg-gray-900 text-red-400">
        <span className="text-lg">Error: {error ?? 'No data available'}</span>
      </div>
    )
  }

  const { zones, projects, timeline, tech_mix } = data

  // ---- KPI cards ----
  const totalCapacityGW = zones.reduce((s, z) => s + z.declared_capacity_gw, 0)
  const rezsDeclared = zones.length
  const totalTransmissionInvestment = zones.reduce((s, z) => s + z.transmission_investment_b_aud, 0)
  const totalGeneratorApps = zones.reduce((s, z) => s + z.generator_applications, 0)

  const kpis = [
    { label: 'Total REZ Capacity', value: `${totalCapacityGW.toFixed(1)} GW`, sub: 'Declared capacity across all REZs', color: 'text-indigo-400' },
    { label: 'REZs Declared', value: rezsDeclared.toString(), sub: 'Active renewable energy zones', color: 'text-emerald-400' },
    { label: 'Transmission Investment', value: `${totalTransmissionInvestment.toFixed(1)} B AUD`, sub: 'Total network investment', color: 'text-amber-400' },
    { label: 'Generator Applications', value: totalGeneratorApps.toLocaleString(), sub: 'Connection applications across REZs', color: 'text-cyan-400' },
  ]

  // ---- Chart 1: Horizontal BarChart — REZ capacity (declared vs connected vs pipeline) ----
  const capacityChartData = zones.map((z) => ({
    name: z.rez_name,
    declared: z.declared_capacity_gw,
    connected: z.connected_capacity_gw,
    pipeline: z.pipeline_capacity_gw,
  }))

  // ---- Chart 2: LineChart — REZ development timeline (cumulative connected capacity) ----
  const years = [...new Set(timeline.map((t) => t.year))].sort()
  const rezNames = [...new Set(timeline.map((t) => t.rez_name))]
  const timelineChartData = years.map((yr) => {
    const row: Record<string, string | number> = { year: yr.toString() }
    let cumTotal = 0
    for (const rn of rezNames) {
      const rec = timeline.find((t) => t.year === yr && t.rez_name === rn)
      cumTotal += rec?.connected_gw ?? 0
    }
    row['cumulative_connected_gw'] = Math.round(cumTotal * 100) / 100
    return row
  })

  // ---- Chart 3: Stacked BarChart — Technology mix per REZ ----
  const techMixChartData = tech_mix.map((t) => ({
    name: t.rez_name,
    Wind: t.wind_gw,
    Solar: t.solar_gw,
    Battery: t.battery_gw,
    Hybrid: t.hybrid_gw,
  }))

  // ---- Chart 4: BarChart — Transmission investment vs hosting capacity ----
  const txInvestmentChartData = zones.map((z) => ({
    name: z.rez_name,
    transmission_investment: z.transmission_investment_b_aud,
    hosting_capacity: z.declared_capacity_gw,
  }))

  return (
    <div className="p-6 bg-gray-900 min-h-screen text-gray-100">
      {/* Header */}
      <div className="flex items-center gap-3 mb-6">
        <MapPin className="w-8 h-8 text-indigo-400" />
        <div>
          <h1 className="text-2xl font-bold text-white">
            Renewable Energy Zone Development Analytics
          </h1>
          <p className="text-gray-400 text-sm">
            REZ capacity, development timelines, technology mix and transmission investment across Australian renewable energy zones
          </p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
        {kpis.map((k) => (
          <div key={k.label} className="bg-gray-800 rounded-xl p-4 border border-gray-700">
            <p className="text-gray-400 text-xs uppercase tracking-wide mb-1">{k.label}</p>
            <p className={`text-2xl font-bold ${k.color}`}>{k.value}</p>
            <p className="text-gray-500 text-xs mt-1">{k.sub}</p>
          </div>
        ))}
      </div>

      {/* Chart 1: REZ Capacity — Declared vs Connected vs Pipeline */}
      <div className="bg-gray-800 rounded-xl p-5 border border-gray-700 mb-6">
        <h2 className="text-lg font-semibold text-white mb-1">REZ Capacity: Declared vs Connected vs Pipeline</h2>
        <p className="text-gray-400 text-xs mb-4">
          Horizontal bar chart showing declared, connected, and pipeline capacity (GW) for each major REZ.
        </p>
        <div className="flex flex-wrap gap-3 mb-3">
          {Object.entries(CAPACITY_COLORS).map(([key, col]) => (
            <span key={key} className="flex items-center gap-1 text-xs text-gray-300">
              <span className="inline-block w-3 h-3 rounded-sm" style={{ backgroundColor: col }} />
              {key.charAt(0).toUpperCase() + key.slice(1)}
            </span>
          ))}
        </div>
        <ResponsiveContainer width="100%" height={400}>
          <BarChart layout="vertical" data={capacityChartData} margin={{ left: 120 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" horizontal={false} />
            <XAxis type="number" stroke="#9ca3af" tick={{ fontSize: 11 }} tickFormatter={(v) => `${v} GW`} />
            <YAxis type="category" dataKey="name" stroke="#9ca3af" tick={{ fontSize: 10 }} width={120} />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }}
              formatter={(v: number) => [`${v.toFixed(2)} GW`, '']}
            />
            <Legend />
            <Bar dataKey="declared" name="Declared" fill={CAPACITY_COLORS.declared} />
            <Bar dataKey="connected" name="Connected" fill={CAPACITY_COLORS.connected} />
            <Bar dataKey="pipeline" name="Pipeline" fill={CAPACITY_COLORS.pipeline} />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 2: REZ Development Timeline */}
      <div className="bg-gray-800 rounded-xl p-5 border border-gray-700 mb-6">
        <h2 className="text-lg font-semibold text-white mb-1">REZ Development Timeline</h2>
        <p className="text-gray-400 text-xs mb-4">
          Cumulative connected capacity (GW) across all REZs from 2022 to 2030.
        </p>
        <ResponsiveContainer width="100%" height={320}>
          <LineChart data={timelineChartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="year" stroke="#9ca3af" tick={{ fontSize: 12 }} />
            <YAxis stroke="#9ca3af" tick={{ fontSize: 11 }} tickFormatter={(v) => `${v} GW`} />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }}
              formatter={(v: number) => [`${v.toFixed(2)} GW`, 'Connected Capacity']}
            />
            <Legend />
            <Line
              type="monotone"
              dataKey="cumulative_connected_gw"
              name="Cumulative Connected (GW)"
              stroke="#22c55e"
              strokeWidth={2}
              dot={{ fill: '#22c55e', r: 4 }}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 3: Technology Mix per REZ */}
      <div className="bg-gray-800 rounded-xl p-5 border border-gray-700 mb-6">
        <h2 className="text-lg font-semibold text-white mb-1">Technology Mix per REZ</h2>
        <p className="text-gray-400 text-xs mb-4">
          Stacked bar chart showing Wind, Solar, Battery, and Hybrid capacity (GW) for each REZ.
        </p>
        <div className="flex flex-wrap gap-3 mb-3">
          {Object.entries(TECH_COLORS).map(([tech, col]) => (
            <span key={tech} className="flex items-center gap-1 text-xs text-gray-300">
              <span className="inline-block w-3 h-3 rounded-sm" style={{ backgroundColor: col }} />
              {tech}
            </span>
          ))}
        </div>
        <ResponsiveContainer width="100%" height={360}>
          <BarChart data={techMixChartData} margin={{ bottom: 60 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="name" stroke="#9ca3af" tick={{ fontSize: 9, angle: -35, textAnchor: 'end' } as any} interval={0} />
            <YAxis stroke="#9ca3af" tick={{ fontSize: 11 }} tickFormatter={(v) => `${v} GW`} />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }}
              formatter={(v: number) => [`${v.toFixed(2)} GW`, '']}
            />
            <Legend />
            <Bar dataKey="Wind" stackId="tech" fill={TECH_COLORS.Wind} />
            <Bar dataKey="Solar" stackId="tech" fill={TECH_COLORS.Solar} />
            <Bar dataKey="Battery" stackId="tech" fill={TECH_COLORS.Battery} />
            <Bar dataKey="Hybrid" stackId="tech" fill={TECH_COLORS.Hybrid} />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 4: Transmission Investment vs Hosting Capacity */}
      <div className="bg-gray-800 rounded-xl p-5 border border-gray-700 mb-6">
        <h2 className="text-lg font-semibold text-white mb-1">Transmission Investment vs Hosting Capacity</h2>
        <p className="text-gray-400 text-xs mb-4">
          Transmission investment (B AUD) and declared hosting capacity (GW) per REZ.
        </p>
        <div className="flex gap-4 mb-3">
          <span className="flex items-center gap-1 text-xs text-gray-300">
            <span className="inline-block w-3 h-3 rounded-sm bg-indigo-500" /> Transmission Investment (B AUD)
          </span>
          <span className="flex items-center gap-1 text-xs text-gray-300">
            <span className="inline-block w-3 h-3 rounded-sm bg-emerald-500" /> Hosting Capacity (GW)
          </span>
        </div>
        <ResponsiveContainer width="100%" height={360}>
          <BarChart data={txInvestmentChartData} margin={{ bottom: 60 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="name" stroke="#9ca3af" tick={{ fontSize: 9, angle: -35, textAnchor: 'end' } as any} interval={0} />
            <YAxis stroke="#9ca3af" tick={{ fontSize: 11 }} />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }}
              formatter={(v: number, name: string) => [
                name === 'transmission_investment' ? `${v.toFixed(2)} B AUD` : `${v.toFixed(2)} GW`,
                name === 'transmission_investment' ? 'Tx Investment' : 'Hosting Capacity',
              ]}
            />
            <Legend />
            <Bar dataKey="transmission_investment" name="Tx Investment (B AUD)" fill="#6366f1" />
            <Bar dataKey="hosting_capacity" name="Hosting Capacity (GW)" fill="#22c55e" />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Table 1: REZ Summary */}
      <div className="bg-gray-800 rounded-xl p-5 border border-gray-700 mb-6">
        <h2 className="text-lg font-semibold text-white mb-1">REZ Summary</h2>
        <p className="text-gray-400 text-xs mb-4">
          Overview of all declared Renewable Energy Zones with capacity, transmission status and access scheme details.
        </p>
        <div className="overflow-x-auto">
          <table className="w-full text-xs text-gray-300">
            <thead>
              <tr className="border-b border-gray-700">
                {['REZ Name', 'State', 'Declared (GW)', 'Connected (GW)', 'Pipeline (GW)', 'Tx Investment (B AUD)', 'Transmission Status', 'Access Scheme', 'Applications'].map((h) => (
                  <th key={h} className="text-left p-2 text-gray-400 whitespace-nowrap">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {zones.map((z) => (
                <tr key={z.rez_name} className="border-b border-gray-700/50 hover:bg-gray-750">
                  <td className="p-2 font-medium text-indigo-300">{z.rez_name}</td>
                  <td className="p-2">{z.state}</td>
                  <td className="p-2 text-right">{z.declared_capacity_gw.toFixed(2)}</td>
                  <td className="p-2 text-right">{z.connected_capacity_gw.toFixed(2)}</td>
                  <td className="p-2 text-right">{z.pipeline_capacity_gw.toFixed(2)}</td>
                  <td className="p-2 text-right">{z.transmission_investment_b_aud.toFixed(2)}</td>
                  <td className="p-2">
                    <span
                      className="px-1.5 py-0.5 rounded text-xs font-medium"
                      style={{
                        backgroundColor: (TX_STATUS_COLORS[z.transmission_status] ?? '#9ca3af') + '33',
                        color: TX_STATUS_COLORS[z.transmission_status] ?? '#9ca3af',
                      }}
                    >
                      {z.transmission_status.replace(/_/g, ' ')}
                    </span>
                  </td>
                  <td className="p-2">{z.access_scheme}</td>
                  <td className="p-2 text-right">{z.generator_applications}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Table 2: Major Projects within REZs */}
      <div className="bg-gray-800 rounded-xl p-5 border border-gray-700">
        <h2 className="text-lg font-semibold text-white mb-1">Major Projects within REZs</h2>
        <p className="text-gray-400 text-xs mb-4">
          Key generation and storage projects across all Renewable Energy Zones.
        </p>
        <div className="overflow-x-auto">
          <table className="w-full text-xs text-gray-300">
            <thead>
              <tr className="border-b border-gray-700">
                {['Project', 'REZ', 'Developer', 'Technology', 'Capacity (MW)', 'Status', 'Connection Year'].map((h) => (
                  <th key={h} className="text-left p-2 text-gray-400 whitespace-nowrap">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {projects.map((p) => (
                <tr key={p.project_name} className="border-b border-gray-700/50 hover:bg-gray-750">
                  <td className="p-2 font-medium text-indigo-300">{p.project_name}</td>
                  <td className="p-2">{p.rez_name}</td>
                  <td className="p-2">{p.developer}</td>
                  <td className="p-2">
                    <span
                      className="px-1.5 py-0.5 rounded text-xs font-medium"
                      style={{
                        backgroundColor: (TECH_COLORS[p.technology] ?? '#9ca3af') + '33',
                        color: TECH_COLORS[p.technology] ?? '#9ca3af',
                      }}
                    >
                      {p.technology}
                    </span>
                  </td>
                  <td className="p-2 text-right">{p.capacity_mw.toLocaleString()}</td>
                  <td className="p-2">
                    <span
                      className="px-1.5 py-0.5 rounded text-xs font-medium"
                      style={{
                        backgroundColor: (STATUS_COLORS[p.status] ?? '#9ca3af') + '33',
                        color: STATUS_COLORS[p.status] ?? '#9ca3af',
                      }}
                    >
                      {p.status}
                    </span>
                  </td>
                  <td className="p-2 text-center">{p.connection_year}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
