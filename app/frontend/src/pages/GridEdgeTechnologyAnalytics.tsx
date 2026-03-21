import { useEffect, useState } from 'react'
import {
  BarChart, Bar,
  LineChart, Line,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer,
} from 'recharts'
import { Cpu, Activity, Zap, Shield } from 'lucide-react'
import {
  getGridEdgeTechnologyDashboard,
  GEDDashboard,
  GEDTechnologyRecord,
  GEDMicrogridRecord,
  GEDSmartInverterRecord,
  GEDEdgeDeploymentRecord,
  GEDGridServiceRecord,
} from '../api/client'

// ─── Colour maps ──────────────────────────────────────────────────────────────
const CATEGORY_COLOURS: Record<string, string> = {
  SMART_INVERTER:  'bg-cyan-700',
  GRID_FORMING:    'bg-violet-700',
  MICROGRID:       'bg-emerald-700',
  EDGE_COMPUTING:  'bg-amber-700',
  V2G:             'bg-rose-700',
  AGGREGATION:     'bg-blue-700',
}

const GRID_SERVICE_COLOURS: Record<string, string> = {
  VOLTAGE_SUPPORT:  'bg-cyan-800',
  FREQUENCY:        'bg-violet-800',
  INERTIA:          'bg-purple-800',
  ISLANDING:        'bg-emerald-800',
  DEMAND_RESPONSE:  'bg-amber-800',
}

const STATE_LINE_COLOURS: Record<string, string> = {
  NSW: '#60a5fa',
  VIC: '#4ade80',
  QLD: '#facc15',
  SA:  '#f87171',
  WA:  '#fb923c',
  TAS: '#a78bfa',
}

// ─── KPI Card ─────────────────────────────────────────────────────────────────
function KpiCard({ label, value, sub, Icon }: {
  label: string; value: string; sub?: string; Icon: React.ElementType
}) {
  return (
    <div className="bg-gray-800 rounded-xl p-5 flex items-start gap-4 shadow-lg">
      <div className="p-3 bg-gray-700 rounded-lg">
        <Icon className="w-6 h-6 text-cyan-400" />
      </div>
      <div>
        <p className="text-xs text-gray-400 uppercase tracking-wide">{label}</p>
        <p className="text-2xl font-bold text-white mt-0.5">{value}</p>
        {sub && <p className="text-xs text-gray-400 mt-0.5">{sub}</p>}
      </div>
    </div>
  )
}

// ─── Badge ────────────────────────────────────────────────────────────────────
function Badge({ label, colourClass }: { label: string; colourClass: string }) {
  return (
    <span className={`inline-block px-2 py-0.5 rounded text-xs font-semibold text-white ${colourClass}`}>
      {label.replace(/_/g, ' ')}
    </span>
  )
}

// ─── TRL Progress Bar ─────────────────────────────────────────────────────────
function TrlBar({ value }: { value: number }) {
  const pct = Math.round((value / 9) * 100)
  const colour =
    value >= 8 ? 'bg-green-500' :
    value >= 6 ? 'bg-yellow-500' : 'bg-orange-500'
  return (
    <div className="flex items-center gap-2">
      <div className="w-20 h-2 bg-gray-700 rounded-full overflow-hidden">
        <div className={`h-full rounded-full ${colour}`} style={{ width: `${pct}%` }} />
      </div>
      <span className="text-xs text-gray-300 w-4 text-right">{value}</span>
    </div>
  )
}

// ─── Main page ────────────────────────────────────────────────────────────────
export default function GridEdgeTechnologyAnalytics() {
  const [data, setData] = useState<GEDDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getGridEdgeTechnologyDashboard()
      .then(setData)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-gray-400 bg-gray-900">
        Loading Grid Edge Technology Analytics...
      </div>
    )
  }
  if (error || !data) {
    return (
      <div className="flex items-center justify-center h-64 text-red-400 bg-gray-900">
        Error: {error ?? 'No data'}
      </div>
    )
  }

  const summary = data.summary as Record<string, number | string>

  // ─── Smart inverter market share bar data ────────────────────────────────
  const inverterBarData: GEDSmartInverterRecord[] = [...data.smart_inverters]
    .sort((a, b) => b.australia_installs - a.australia_installs)

  // ─── Edge deployment line chart — smart inverters by state over years ────
  const years = Array.from(new Set(data.edge_deployments.map(d => d.year))).sort()
  const states = Array.from(new Set(data.edge_deployments.map(d => d.state))).sort()

  const lineChartData = years.map(yr => {
    const row: Record<string, number | string> = { year: yr }
    states.forEach(st => {
      const rec = data.edge_deployments.find(
        (d: GEDEdgeDeploymentRecord) => d.state === st && d.year === yr
      )
      row[st] = rec ? rec.smart_inverters_k : 0
    })
    return row
  })

  return (
    <div className="bg-gray-900 min-h-screen p-6 space-y-8">
      {/* Header */}
      <div className="flex items-center gap-3">
        <Cpu className="w-8 h-8 text-cyan-400" />
        <div>
          <h1 className="text-2xl font-bold text-white">
            Grid Edge Technology Analytics
          </h1>
          <p className="text-sm text-gray-400 mt-0.5">
            Microgrids, smart inverters, grid-forming technology and edge computing for the NEM
          </p>
        </div>
      </div>

      {/* KPI cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <KpiCard
          label="Technologies Tracked"
          value={String(summary.total_technologies ?? 0)}
          sub="Across 6 categories"
          Icon={Cpu}
        />
        <KpiCard
          label="Smart Inverters Installed"
          value={`${summary.smart_inverters_installed_m}M`}
          sub="AS4777.2:2020 compliant"
          Icon={Zap}
        />
        <KpiCard
          label="Total Microgrids"
          value={String(summary.total_microgrids ?? 0)}
          sub={`${summary.total_microgrid_capacity_mw} MW total capacity`}
          Icon={Activity}
        />
        <KpiCard
          label="Fastest Growing Tech"
          value="V2G"
          sub={`${summary.fastest_growing_technology}`}
          Icon={Shield}
        />
      </div>

      {/* Technology Matrix table */}
      <div className="bg-gray-800 rounded-xl shadow-lg overflow-hidden">
        <div className="px-5 py-4 border-b border-gray-700">
          <h2 className="text-base font-semibold text-white">Technology Matrix</h2>
          <p className="text-xs text-gray-400 mt-0.5">
            TRL, market size, CAGR and grid service by technology category
          </p>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-xs text-gray-400 uppercase tracking-wide border-b border-gray-700">
                <th className="px-4 py-3">ID</th>
                <th className="px-4 py-3">Technology</th>
                <th className="px-4 py-3">Category</th>
                <th className="px-4 py-3">TRL</th>
                <th className="px-4 py-3">Deployments (AU)</th>
                <th className="px-4 py-3">Market Size (A$M)</th>
                <th className="px-4 py-3">CAGR %</th>
                <th className="px-4 py-3">Key Capability</th>
                <th className="px-4 py-3">Grid Service</th>
              </tr>
            </thead>
            <tbody>
              {data.technologies.map((t: GEDTechnologyRecord) => (
                <tr key={t.tech_id} className="border-b border-gray-700/50 hover:bg-gray-700/30">
                  <td className="px-4 py-2.5 font-mono text-xs text-gray-400">{t.tech_id}</td>
                  <td className="px-4 py-2.5 text-white font-medium">{t.name}</td>
                  <td className="px-4 py-2.5">
                    <Badge label={t.category} colourClass={CATEGORY_COLOURS[t.category] ?? 'bg-gray-600'} />
                  </td>
                  <td className="px-4 py-2.5">
                    <TrlBar value={t.trl} />
                  </td>
                  <td className="px-4 py-2.5 text-gray-300">{t.deployments_australia.toLocaleString()}</td>
                  <td className="px-4 py-2.5 text-cyan-400 font-semibold">
                    ${t.market_size_aud_m.toLocaleString()}M
                  </td>
                  <td className="px-4 py-2.5">
                    <span className={`font-semibold ${t.cagr_pct >= 50 ? 'text-green-400' : t.cagr_pct >= 25 ? 'text-yellow-400' : 'text-gray-300'}`}>
                      {t.cagr_pct}%
                    </span>
                  </td>
                  <td className="px-4 py-2.5 text-gray-300 text-xs">{t.key_capability}</td>
                  <td className="px-4 py-2.5">
                    <Badge label={t.grid_service} colourClass={GRID_SERVICE_COLOURS[t.grid_service] ?? 'bg-gray-700'} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Microgrid Map table */}
      <div className="bg-gray-800 rounded-xl shadow-lg overflow-hidden">
        <div className="px-5 py-4 border-b border-gray-700">
          <h2 className="text-base font-semibold text-white">Australian Microgrid Deployments</h2>
          <p className="text-xs text-gray-400 mt-0.5">
            Capacity, renewable mix, islanding capability and annual savings
          </p>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-xs text-gray-400 uppercase tracking-wide border-b border-gray-700">
                <th className="px-4 py-3">Name</th>
                <th className="px-4 py-3">State</th>
                <th className="px-4 py-3">Type</th>
                <th className="px-4 py-3">Capacity (MW)</th>
                <th className="px-4 py-3">Storage (MWh)</th>
                <th className="px-4 py-3">Renewable %</th>
                <th className="px-4 py-3">Islanding</th>
                <th className="px-4 py-3">Annual Savings</th>
                <th className="px-4 py-3">Resilience</th>
              </tr>
            </thead>
            <tbody>
              {data.microgrids.map((m: GEDMicrogridRecord) => (
                <tr key={m.microgrid_id} className="border-b border-gray-700/50 hover:bg-gray-700/30">
                  <td className="px-4 py-2.5 text-white font-medium">{m.name}</td>
                  <td className="px-4 py-2.5 font-semibold text-cyan-400">{m.state}</td>
                  <td className="px-4 py-2.5">
                    <Badge label={m.type} colourClass="bg-gray-700" />
                  </td>
                  <td className="px-4 py-2.5 text-gray-300">{m.capacity_mw} MW</td>
                  <td className="px-4 py-2.5 text-gray-300">{m.storage_mwh} MWh</td>
                  <td className="px-4 py-2.5">
                    <span className={`font-semibold ${m.renewable_pct >= 80 ? 'text-green-400' : m.renewable_pct >= 60 ? 'text-yellow-400' : 'text-orange-400'}`}>
                      {m.renewable_pct}%
                    </span>
                  </td>
                  <td className="px-4 py-2.5">
                    {m.islanding_capable ? (
                      <Badge label="Capable" colourClass="bg-emerald-700" />
                    ) : (
                      <Badge label="Grid-tied" colourClass="bg-gray-600" />
                    )}
                  </td>
                  <td className="px-4 py-2.5 text-emerald-400 font-semibold">
                    A${m.annual_savings_aud_k.toLocaleString()}k
                  </td>
                  <td className="px-4 py-2.5 text-gray-300">
                    {m.resilience_hours > 0 ? `${m.resilience_hours}h` : <span className="text-gray-600">—</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Charts row — inverter market share + edge deployment */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
        {/* Smart Inverter Market Share BarChart */}
        <div className="bg-gray-800 rounded-xl shadow-lg p-5">
          <h2 className="text-base font-semibold text-white mb-1">Smart Inverter Market Share</h2>
          <p className="text-xs text-gray-400 mb-4">Australian installs by manufacturer (AS4777.2:2020)</p>
          <ResponsiveContainer width="100%" height={320}>
            <BarChart
              data={inverterBarData}
              layout="vertical"
              margin={{ top: 0, right: 20, left: 8, bottom: 0 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" horizontal={false} />
              <XAxis
                type="number"
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                tickFormatter={v => v >= 1000000 ? `${(v / 1000000).toFixed(1)}M` : v >= 1000 ? `${(v / 1000).toFixed(0)}k` : String(v)}
              />
              <YAxis
                type="category"
                dataKey="manufacturer"
                tick={{ fill: '#d1d5db', fontSize: 12 }}
                width={80}
              />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                labelStyle={{ color: '#f9fafb' }}
                formatter={((val: number, _name: string, entry: { payload: GEDSmartInverterRecord }) => [
                  `${val.toLocaleString()} units | ${entry.payload.model} | ${entry.payload.capacity_kva} kVA`,
                  'Installations',
                ]) as any}
              />
              <Bar dataKey="australia_installs" name="AU Installs" fill="#22d3ee" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Edge Deployment LineChart */}
        <div className="bg-gray-800 rounded-xl shadow-lg p-5">
          <h2 className="text-base font-semibold text-white mb-1">Smart Inverter Deployment by State</h2>
          <p className="text-xs text-gray-400 mb-4">Thousands of units installed 2022–2030</p>
          <ResponsiveContainer width="100%" height={320}>
            <LineChart data={lineChartData} margin={{ top: 5, right: 20, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="year" tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <YAxis
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                tickFormatter={v => `${v}k`}
              />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                labelStyle={{ color: '#f9fafb' }}
                formatter={(val: number, name: string) => [`${val}k units`, name]}
              />
              <Legend wrapperStyle={{ fontSize: 12, color: '#9ca3af', paddingTop: 8 }} />
              {states.map(st => (
                <Line
                  key={st}
                  type="monotone"
                  dataKey={st}
                  stroke={STATE_LINE_COLOURS[st] ?? '#6b7280'}
                  strokeWidth={2}
                  dot={false}
                  activeDot={{ r: 4 }}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Grid Services table */}
      <div className="bg-gray-800 rounded-xl shadow-lg overflow-hidden">
        <div className="px-5 py-4 border-b border-gray-700">
          <h2 className="text-base font-semibold text-white">Grid Services from Edge Technologies</h2>
          <p className="text-xs text-gray-400 mt-0.5">
            Response time, revenue per MW/year and growth potential by service type
          </p>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-xs text-gray-400 uppercase tracking-wide border-b border-gray-700">
                <th className="px-4 py-3">Service</th>
                <th className="px-4 py-3">Technology</th>
                <th className="px-4 py-3">Region</th>
                <th className="px-4 py-3">Capacity (MW)</th>
                <th className="px-4 py-3">Response (ms)</th>
                <th className="px-4 py-3">Revenue A$/MW/yr</th>
                <th className="px-4 py-3">Providers</th>
                <th className="px-4 py-3">Growth Potential</th>
              </tr>
            </thead>
            <tbody>
              {data.grid_services.map((gs: GEDGridServiceRecord, i) => (
                <tr key={i} className="border-b border-gray-700/50 hover:bg-gray-700/30">
                  <td className="px-4 py-2.5 text-white font-medium">{gs.service}</td>
                  <td className="px-4 py-2.5">
                    <Badge label={gs.technology} colourClass={CATEGORY_COLOURS[gs.technology] ?? 'bg-gray-600'} />
                  </td>
                  <td className="px-4 py-2.5 font-semibold text-cyan-400">{gs.region}</td>
                  <td className="px-4 py-2.5 text-gray-300">{gs.capacity_mw.toLocaleString()} MW</td>
                  <td className="px-4 py-2.5">
                    <span className={`font-semibold ${gs.response_time_ms <= 100 ? 'text-green-400' : gs.response_time_ms <= 500 ? 'text-yellow-400' : 'text-orange-400'}`}>
                      {gs.response_time_ms} ms
                    </span>
                  </td>
                  <td className="px-4 py-2.5 text-emerald-400 font-semibold">
                    ${gs.revenue_aud_per_mw_year.toLocaleString()}
                  </td>
                  <td className="px-4 py-2.5 text-gray-300">{gs.current_providers}</td>
                  <td className="px-4 py-2.5 text-violet-400 font-semibold">
                    +{gs.growth_potential_mw.toLocaleString()} MW
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
