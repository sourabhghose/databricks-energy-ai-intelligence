import { useEffect, useState } from 'react'
import {
  BarChart, Bar, LineChart, Line,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer, ReferenceLine,
} from 'recharts'
import {
  getEVFleetGridImpactDashboard,
  EFGDashboard,
  EFGFleetRecord,
  EFGChargingProfileRecord,
  EFGNetworkImpactRecord,
  EFGV2GServiceRecord,
  EFGEmissionRecord,
} from '../api/client'
import { Car, Zap, TrendingUp, Activity, Leaf, DollarSign } from 'lucide-react'

// ---- Colour maps ---------------------------------------------------------------

const SCENARIO_COLOURS: Record<string, string> = {
  UNMANAGED:      '#f43f5e',
  SMART_CHARGING: '#3b82f6',
  V2G_OPTIMISED:  '#10b981',
  DSO_CONTROL:    '#f59e0b',
}

const VEHICLE_CLASS_COLOURS: Record<string, string> = {
  PASSENGER:        '#3b82f6',
  LIGHT_COMMERCIAL: '#f59e0b',
  BUS:              '#10b981',
}

const REGION_COLOURS: Record<string, string> = {
  NSW: '#3b82f6',
  VIC: '#8b5cf6',
  QLD: '#f59e0b',
  SA:  '#10b981',
  WA:  '#f43f5e',
}

const SCENARIO_BADGE: Record<string, string> = {
  UNMANAGED:      'bg-rose-500/20 text-rose-300 border border-rose-500/40',
  SMART_CHARGING: 'bg-blue-500/20 text-blue-300 border border-blue-500/40',
  V2G_OPTIMISED:  'bg-emerald-500/20 text-emerald-300 border border-emerald-500/40',
  DSO_CONTROL:    'bg-amber-500/20 text-amber-300 border border-amber-500/40',
}

const TECH_BADGE: Record<string, string> = {
  V2G: 'bg-violet-500/20 text-violet-300 border border-violet-500/40',
  V2H: 'bg-cyan-500/20 text-cyan-300 border border-cyan-500/40',
  V2B: 'bg-teal-500/20 text-teal-300 border border-teal-500/40',
}

// ---- KPI Card ------------------------------------------------------------------

function KpiCard({ label, value, sub, icon: Icon, colour }: {
  label: string; value: string; sub?: string; icon: React.ElementType; colour: string
}) {
  return (
    <div className="bg-gray-800 rounded-xl p-5 border border-gray-700 flex items-start gap-4">
      <div className={`p-3 rounded-lg ${colour}`}>
        <Icon className="w-5 h-5" />
      </div>
      <div>
        <p className="text-xs text-gray-400 uppercase tracking-wider mb-1">{label}</p>
        <p className="text-2xl font-bold text-white">{value}</p>
        {sub && <p className="text-xs text-gray-500 mt-0.5">{sub}</p>}
      </div>
    </div>
  )
}

// ---- Section Heading -----------------------------------------------------------

function SectionHeading({ title, sub }: { title: string; sub?: string }) {
  return (
    <div className="mb-4">
      <h2 className="text-lg font-semibold text-white">{title}</h2>
      {sub && <p className="text-sm text-gray-400 mt-0.5">{sub}</p>}
    </div>
  )
}

// ---- Gauge Cell ----------------------------------------------------------------

function GaugeCell({ value, max, colour }: { value: number; max: number; colour: string }) {
  const pct = Math.min(100, (value / max) * 100)
  return (
    <div className="flex items-center gap-2">
      <div className="flex-1 bg-gray-700 rounded-full h-2">
        <div
          className="h-2 rounded-full transition-all"
          style={{ width: `${pct}%`, backgroundColor: colour }}
        />
      </div>
      <span className="text-xs text-gray-300 w-10 text-right">{value.toFixed(1)}%</span>
    </div>
  )
}

// ---- Fleet Growth Chart --------------------------------------------------------

function FleetGrowthChart({ records }: { records: EFGFleetRecord[] }) {
  // Aggregate EV count by region+year across all vehicle classes
  const regionYearMap: Record<string, Record<number, number>> = {}
  for (const r of records) {
    if (!regionYearMap[r.region]) regionYearMap[r.region] = {}
    regionYearMap[r.region][r.year] = (regionYearMap[r.region][r.year] ?? 0) + r.ev_count
  }

  const regions = Object.keys(regionYearMap)
  const years = [2024, 2030]

  const chartData = years.map(yr => {
    const row: Record<string, unknown> = { year: String(yr) }
    for (const reg of regions) {
      row[reg] = Math.round((regionYearMap[reg]?.[yr] ?? 0) / 1000)
    }
    return row
  })

  return (
    <ResponsiveContainer width="100%" height={300}>
      <BarChart data={chartData} margin={{ top: 10, right: 20, left: 10, bottom: 5 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
        <XAxis dataKey="year" tick={{ fill: '#9ca3af', fontSize: 12 }} />
        <YAxis tick={{ fill: '#9ca3af', fontSize: 12 }} unit="k" />
        <Tooltip
          contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
          labelStyle={{ color: '#f9fafb' }}
          itemStyle={{ color: '#d1d5db' }}
          formatter={(v: unknown) => [`${v}k EVs`]}
        />
        <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 12 }} />
        {regions.map(reg => (
          <Bar key={reg} dataKey={reg} fill={REGION_COLOURS[reg] ?? '#6b7280'} radius={[3, 3, 0, 0]} />
        ))}
      </BarChart>
    </ResponsiveContainer>
  )
}

// ---- Vehicle Class Breakdown Chart ---------------------------------------------

function VehicleClassChart({ records }: { records: EFGFleetRecord[] }) {
  // 2030 data, aggregate by vehicle class across regions
  const vcMap: Record<string, number> = {}
  for (const r of records.filter(r => r.year === 2030)) {
    vcMap[r.vehicle_class] = (vcMap[r.vehicle_class] ?? 0) + r.ev_count
  }
  const chartData = Object.entries(vcMap).map(([vc, cnt]) => ({
    vehicle_class: vc,
    ev_count_k: Math.round(cnt / 1000),
    annual_gwh: records
      .filter(r => r.year === 2030 && r.vehicle_class === vc)
      .reduce((s, r) => s + r.total_annual_energy_gwh, 0)
      .toFixed(0),
  }))

  return (
    <ResponsiveContainer width="100%" height={260}>
      <BarChart data={chartData} margin={{ top: 10, right: 20, left: 10, bottom: 5 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
        <XAxis dataKey="vehicle_class" tick={{ fill: '#9ca3af', fontSize: 11 }} />
        <YAxis yAxisId="left" tick={{ fill: '#9ca3af', fontSize: 12 }} unit="k" />
        <YAxis yAxisId="right" orientation="right" tick={{ fill: '#9ca3af', fontSize: 12 }} unit=" GWh" />
        <Tooltip
          contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
          labelStyle={{ color: '#f9fafb' }}
          itemStyle={{ color: '#d1d5db' }}
        />
        <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 12 }} />
        <Bar yAxisId="left" dataKey="ev_count_k" name="EV Count (k)" fill="#3b82f6" radius={[3, 3, 0, 0]} />
        <Bar yAxisId="right" dataKey="annual_gwh" name="Annual Energy (GWh)" fill="#10b981" radius={[3, 3, 0, 0]} />
      </BarChart>
    </ResponsiveContainer>
  )
}

// ---- Charging Profile Chart ----------------------------------------------------

function ChargingProfileChart({ records }: { records: EFGChargingProfileRecord[] }) {
  const [selectedRegion, setSelectedRegion] = useState<string>('NSW')

  const regions = [...new Set(records.map(r => r.region))].sort()
  const scenarios = ['UNMANAGED', 'SMART_CHARGING', 'V2G_OPTIMISED', 'DSO_CONTROL']

  const filtered = records.filter(r => r.region === selectedRegion)

  // Build hour-indexed map per scenario
  const scenarioData: Record<string, Record<number, number>> = {}
  for (const s of scenarios) {
    scenarioData[s] = {}
    for (const r of filtered.filter(r => r.charging_scenario === s)) {
      scenarioData[s][r.hour] = r.avg_load_mw
    }
  }

  const chartData = Array.from({ length: 24 }, (_, h) => {
    const row: Record<string, unknown> = { hour: h }
    for (const s of scenarios) {
      row[s] = scenarioData[s][h] ?? null
    }
    return row
  })

  return (
    <div>
      <div className="flex gap-2 mb-4 flex-wrap">
        {regions.map(reg => (
          <button
            key={reg}
            onClick={() => setSelectedRegion(reg)}
            className={`px-3 py-1 rounded-lg text-xs font-medium transition-colors ${
              selectedRegion === reg
                ? 'bg-blue-600 text-white'
                : 'bg-gray-700 text-gray-300 hover:bg-gray-600'
            }`}
          >
            {reg}
          </button>
        ))}
      </div>
      <ResponsiveContainer width="100%" height={300}>
        <LineChart data={chartData} margin={{ top: 10, right: 20, left: 10, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis
            dataKey="hour"
            tick={{ fill: '#9ca3af', fontSize: 11 }}
            tickFormatter={(v) => `${v}:00`}
          />
          <YAxis tick={{ fill: '#9ca3af', fontSize: 12 }} unit=" MW" />
          <ReferenceLine y={0} stroke="#6b7280" strokeDasharray="4 2" />
          <Tooltip
            contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
            labelStyle={{ color: '#f9fafb' }}
            itemStyle={{ color: '#d1d5db' }}
            labelFormatter={(v) => `Hour ${v}:00`}
            formatter={(v: unknown) => [`${Number(v).toFixed(1)} MW`]}
          />
          <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 12 }} />
          {scenarios.map(s => (
            <Line
              key={s}
              type="monotone"
              dataKey={s}
              stroke={SCENARIO_COLOURS[s]}
              dot={false}
              strokeWidth={2}
              connectNulls
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

// ---- Network Impact Table ------------------------------------------------------

function NetworkImpactTable({ records }: { records: EFGNetworkImpactRecord[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-gray-700">
            <th className="text-left py-2 px-3 text-gray-400 font-medium">Distributor</th>
            <th className="text-left py-2 px-3 text-gray-400 font-medium">State</th>
            <th className="text-left py-2 px-3 text-gray-400 font-medium w-36">LV Transformer Overload</th>
            <th className="text-left py-2 px-3 text-gray-400 font-medium w-36">Feeder Headroom</th>
            <th className="text-right py-2 px-3 text-gray-400 font-medium">Upgrade Cost ($M)</th>
            <th className="text-right py-2 px-3 text-gray-400 font-medium">Trigger EV %</th>
            <th className="text-right py-2 px-3 text-gray-400 font-medium">Managed Charging Deferral</th>
            <th className="text-right py-2 px-3 text-gray-400 font-medium">2030 EV Forecast %</th>
          </tr>
        </thead>
        <tbody>
          {records.map((r) => (
            <tr key={r.distributor} className="border-b border-gray-700/50 hover:bg-gray-700/30 transition-colors">
              <td className="py-3 px-3 text-white font-medium">{r.distributor}</td>
              <td className="py-3 px-3">
                <span className="bg-gray-700 text-gray-300 text-xs px-2 py-0.5 rounded">{r.state}</span>
              </td>
              <td className="py-3 px-3">
                <GaugeCell value={r.lv_transformer_overload_pct} max={50} colour="#f43f5e" />
              </td>
              <td className="py-3 px-3">
                <GaugeCell value={r.feeder_capacity_headroom_pct} max={100} colour="#10b981" />
              </td>
              <td className="py-3 px-3 text-right text-gray-300">${r.required_network_upgrade_m.toFixed(0)}M</td>
              <td className="py-3 px-3 text-right text-gray-300">{r.ev_penetration_trigger_pct.toFixed(1)}%</td>
              <td className="py-3 px-3">
                <GaugeCell value={r.managed_charging_deferral_pct} max={100} colour="#3b82f6" />
              </td>
              <td className="py-3 px-3 text-right text-gray-300">{r.forecast_ev_penetration_2030_pct.toFixed(1)}%</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// ---- V2G Services Chart --------------------------------------------------------

function V2GServicesChart({ records }: { records: EFGV2GServiceRecord[] }) {
  const [selectedTech, setSelectedTech] = useState<string>('V2G')
  const techs = [...new Set(records.map(r => r.technology))].sort()
  const filtered = records.filter(r => r.technology === selectedTech)

  const chartData = filtered.map(r => ({
    service: r.service.replace('_', ' '),
    'Annual Revenue': r.annual_revenue_per_vehicle,
    'Battery Degradation': r.battery_degradation_cost_per_yr,
    'Net Benefit': r.net_benefit_per_vehicle,
  }))

  return (
    <div>
      <div className="flex gap-2 mb-4 flex-wrap">
        {techs.map(tech => (
          <button
            key={tech}
            onClick={() => setSelectedTech(tech)}
            className={`px-3 py-1 rounded-lg text-xs font-medium transition-colors ${
              selectedTech === tech
                ? 'bg-violet-600 text-white'
                : 'bg-gray-700 text-gray-300 hover:bg-gray-600'
            }`}
          >
            {tech}
          </button>
        ))}
      </div>
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={chartData} margin={{ top: 10, right: 20, left: 10, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis dataKey="service" tick={{ fill: '#9ca3af', fontSize: 11 }} />
          <YAxis tick={{ fill: '#9ca3af', fontSize: 12 }} unit=" $" />
          <Tooltip
            contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
            labelStyle={{ color: '#f9fafb' }}
            itemStyle={{ color: '#d1d5db' }}
            formatter={(v: unknown) => [`$${Number(v).toFixed(0)}/vehicle/yr`]}
          />
          <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 12 }} />
          <Bar dataKey="Annual Revenue" fill="#10b981" radius={[3, 3, 0, 0]} />
          <Bar dataKey="Battery Degradation" fill="#f43f5e" radius={[3, 3, 0, 0]} />
          <Bar dataKey="Net Benefit" fill="#3b82f6" radius={[3, 3, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>

      {/* V2G Service Summary Table */}
      <div className="mt-6 overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-700">
              <th className="text-left py-2 px-3 text-gray-400 font-medium">Technology</th>
              <th className="text-left py-2 px-3 text-gray-400 font-medium">Service</th>
              <th className="text-right py-2 px-3 text-gray-400 font-medium">Fleet Size</th>
              <th className="text-right py-2 px-3 text-gray-400 font-medium">Avg Capacity (MW)</th>
              <th className="text-right py-2 px-3 text-gray-400 font-medium">Market Size ($M)</th>
            </tr>
          </thead>
          <tbody>
            {records.map((r, i) => (
              <tr key={i} className="border-b border-gray-700/50 hover:bg-gray-700/30 transition-colors">
                <td className="py-2 px-3">
                  <span className={`text-xs px-2 py-0.5 rounded ${TECH_BADGE[r.technology] ?? 'bg-gray-700 text-gray-300'}`}>
                    {r.technology}
                  </span>
                </td>
                <td className="py-2 px-3 text-gray-300">{r.service.replace(/_/g, ' ')}</td>
                <td className="py-2 px-3 text-right text-gray-300">{r.fleet_size_vehicles.toLocaleString()}</td>
                <td className="py-2 px-3 text-right text-gray-300">{r.avg_capacity_mw.toFixed(1)}</td>
                <td className="py-2 px-3 text-right text-white font-medium">${r.market_size_m.toFixed(0)}M</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ---- Emissions Chart -----------------------------------------------------------

function EmissionsChart({ records }: { records: EFGEmissionRecord[] }) {
  const [selectedRegion, setSelectedRegion] = useState<string>('NSW')

  const regions = [...new Set(records.map(r => r.region))].sort()
  const years = [...new Set(records.map(r => r.year))].sort((a, b) => a - b)

  // Net abatement by year for selected region
  const filtered = records.filter(r => r.region === selectedRegion)
  const byYear = years.map(yr => {
    const rec = filtered.find(r => r.year === yr)
    return {
      year: String(yr),
      net_abatement: rec?.net_abatement_mt_co2 ?? 0,
      ev_count_k: rec ? Math.round(rec.ev_count_thousands) : 0,
      grid_intensity: rec ? rec.grid_emission_intensity_kg_co2_per_mwh.toFixed(3) : '0',
    }
  })

  // All regions in 2030 for comparison bar
  const allRegion2030 = regions.map(reg => {
    const rec = records.find(r => r.region === reg && r.year === 2030)
    return {
      region: reg,
      net_abatement: rec?.net_abatement_mt_co2 ?? 0,
    }
  })

  return (
    <div className="space-y-6">
      <div>
        <div className="flex gap-2 mb-4 flex-wrap">
          {regions.map(reg => (
            <button
              key={reg}
              onClick={() => setSelectedRegion(reg)}
              className={`px-3 py-1 rounded-lg text-xs font-medium transition-colors ${
                selectedRegion === reg
                  ? 'bg-emerald-600 text-white'
                  : 'bg-gray-700 text-gray-300 hover:bg-gray-600'
              }`}
            >
              {reg}
            </button>
          ))}
        </div>
        <p className="text-xs text-gray-500 mb-3">Net CO2 abatement (Mt) over time — {selectedRegion}</p>
        <ResponsiveContainer width="100%" height={260}>
          <LineChart data={byYear} margin={{ top: 10, right: 20, left: 10, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="year" tick={{ fill: '#9ca3af', fontSize: 12 }} />
            <YAxis tick={{ fill: '#9ca3af', fontSize: 12 }} unit=" Mt" />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
              labelStyle={{ color: '#f9fafb' }}
              itemStyle={{ color: '#d1d5db' }}
              formatter={((v: unknown, name: string) => {
                if (name === 'net_abatement') return [`${Number(v).toFixed(2)} Mt CO2`, name]
                if (name === 'ev_count_k') return [`${v}k EVs`, name]
                return [v, name]
              }) as any}
            />
            <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 12 }} />
            <Line
              type="monotone"
              dataKey="net_abatement"
              name="Net Abatement (Mt CO2)"
              stroke="#10b981"
              strokeWidth={2}
              dot={{ r: 4, fill: '#10b981' }}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>

      <div>
        <p className="text-xs text-gray-500 mb-3">Net CO2 abatement by region — 2030</p>
        <ResponsiveContainer width="100%" height={220}>
          <BarChart data={allRegion2030} margin={{ top: 10, right: 20, left: 10, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="region" tick={{ fill: '#9ca3af', fontSize: 12 }} />
            <YAxis tick={{ fill: '#9ca3af', fontSize: 12 }} unit=" Mt" />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
              labelStyle={{ color: '#f9fafb' }}
              itemStyle={{ color: '#d1d5db' }}
              formatter={(v: unknown) => [`${Number(v).toFixed(2)} Mt CO2`]}
            />
            <Bar dataKey="net_abatement" name="Net Abatement" radius={[3, 3, 0, 0]}>
              {allRegion2030.map((entry) => (
                <rect key={entry.region} fill={REGION_COLOURS[entry.region] ?? '#6b7280'} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}

// ---- Main Page -----------------------------------------------------------------

export default function EVFleetGridImpactAnalytics() {
  const [data, setData] = useState<EFGDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getEVFleetGridImpactDashboard()
      .then(setData)
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500" />
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="flex items-center justify-center h-64 text-red-400">
        Failed to load EV Fleet Grid Impact data: {error}
      </div>
    )
  }

  const { summary } = data
  const total2024 = Number(summary.total_ev_count_2024).toLocaleString()
  const total2030 = (Number(summary.total_ev_count_2030_forecast) / 1_000_000).toFixed(2)
  const energy2030 = Number(summary.total_annual_energy_2030_gwh).toLocaleString()
  const managedBenefit = Number(summary.managed_charging_benefit_gw).toFixed(1)
  const v2gMarket = Number(summary.v2g_market_size_m).toLocaleString()
  const abatement2030 = Number(summary.net_abatement_2030_mt).toFixed(1)

  return (
    <div className="p-6 space-y-8 text-white">
      {/* Page Header */}
      <div>
        <div className="flex items-center gap-3 mb-1">
          <Car className="w-6 h-6 text-blue-400" />
          <h1 className="text-2xl font-bold text-white">EV Fleet Grid Impact Analytics</h1>
        </div>
        <p className="text-sm text-gray-400">
          Fleet growth, charging profiles, network impacts, V2G services and emissions — Sprint 76a
        </p>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-4">
        <KpiCard
          label="EV Count 2024"
          value={total2024}
          sub="Registered EVs"
          icon={Car}
          colour="bg-blue-500/20 text-blue-400"
        />
        <KpiCard
          label="EV Forecast 2030"
          value={`${total2030}M`}
          sub="Fleet projection"
          icon={TrendingUp}
          colour="bg-violet-500/20 text-violet-400"
        />
        <KpiCard
          label="Annual Energy 2030"
          value={`${energy2030} GWh`}
          sub="Total EV consumption"
          icon={Zap}
          colour="bg-amber-500/20 text-amber-400"
        />
        <KpiCard
          label="Managed Charging Benefit"
          value={`${managedBenefit} GW`}
          sub="Peak demand reduction"
          icon={Activity}
          colour="bg-emerald-500/20 text-emerald-400"
        />
        <KpiCard
          label="V2G Market Size"
          value={`$${v2gMarket}M`}
          sub="Annual services revenue"
          icon={DollarSign}
          colour="bg-rose-500/20 text-rose-400"
        />
        <KpiCard
          label="Net Abatement 2030"
          value={`${abatement2030} Mt`}
          sub="CO2 avoided vs ICE"
          icon={Leaf}
          colour="bg-teal-500/20 text-teal-400"
        />
      </div>

      {/* Fleet Growth Section */}
      <div className="bg-gray-800 rounded-xl p-6 border border-gray-700">
        <SectionHeading
          title="EV Fleet Growth by Region"
          sub="Total registered EV count (thousands) — 2024 vs 2030 forecast across all vehicle classes"
        />
        <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
          <div>
            <p className="text-xs text-gray-500 mb-3">Fleet count by region (2024 vs 2030)</p>
            <FleetGrowthChart records={data.fleet} />
          </div>
          <div>
            <p className="text-xs text-gray-500 mb-3">Vehicle class breakdown — 2030</p>
            <VehicleClassChart records={data.fleet} />
          </div>
        </div>
      </div>

      {/* Charging Profile Section */}
      <div className="bg-gray-800 rounded-xl p-6 border border-gray-700">
        <SectionHeading
          title="Charging Profile Comparison"
          sub="Average load (MW) by hour of day for four charging scenarios — select region to explore"
        />
        <div className="flex gap-4 mb-4 flex-wrap">
          {Object.entries(SCENARIO_BADGE).map(([sc, cls]) => (
            <span key={sc} className={`text-xs px-2 py-1 rounded ${cls}`}>
              {sc.replace(/_/g, ' ')}
            </span>
          ))}
        </div>
        <ChargingProfileChart records={data.charging_profiles} />
      </div>

      {/* Network Impact Section */}
      <div className="bg-gray-800 rounded-xl p-6 border border-gray-700">
        <SectionHeading
          title="Distribution Network Impact"
          sub="LV transformer overload risk, feeder headroom and upgrade cost estimates by distributor"
        />
        <NetworkImpactTable records={data.network_impacts} />
      </div>

      {/* V2G Services Section */}
      <div className="bg-gray-800 rounded-xl p-6 border border-gray-700">
        <SectionHeading
          title="V2G Services Economics"
          sub="Annual revenue, battery degradation cost and net benefit per vehicle by service type"
        />
        <V2GServicesChart records={data.v2g_services} />
      </div>

      {/* Emissions Section */}
      <div className="bg-gray-800 rounded-xl p-6 border border-gray-700">
        <SectionHeading
          title="Emissions Abatement Trajectory"
          sub="Net CO2 abatement (Mt) compared to equivalent petrol fleet — by region and year"
        />
        <EmissionsChart records={data.emissions} />
      </div>
    </div>
  )
}
