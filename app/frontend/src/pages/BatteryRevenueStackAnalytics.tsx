import { useEffect, useState } from 'react'
import { Battery } from 'lucide-react'
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
  getBatteryRevenueStackDashboard,
  BRSODashboard,
  BRSOAssetRecord,
  BRSOOptimisationRecord,
} from '../api/client'

export default function BatteryRevenueStackAnalytics() {
  const [data, setData] = useState<BRSODashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getBatteryRevenueStackDashboard()
      .then(setData)
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 bg-gray-900 text-gray-400">
        <Battery size={24} className="animate-pulse mr-2 text-green-400" />
        <span>Loading Battery Revenue Stack data...</span>
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="flex items-center justify-center h-64 bg-gray-900 text-red-400">
        <Battery size={24} className="mr-2" />
        <span>Error loading data: {error ?? 'Unknown error'}</span>
      </div>
    )
  }

  const summary = data.summary

  // ---------- KPI cards ----------
  const kpis = [
    { label: 'Fleet Capacity (MW)', value: `${summary.total_fleet_capacity_mw?.toLocaleString()}`, color: 'text-green-400' },
    { label: 'Avg FCAS Share %', value: `${Number(summary.avg_fcas_share_pct).toFixed(1)}%`, color: 'text-blue-400' },
    { label: 'Best IRR', value: `${Number(summary.best_irr_pct).toFixed(1)}%`, color: 'text-amber-400' },
    { label: 'Total Annual Revenue ($M)', value: `$${Number(summary.total_annual_revenue_m).toFixed(2)}M`, color: 'text-purple-400' },
  ]

  // ---------- Revenue stack per asset (aggregate across months) ----------
  const revenueByAsset: Record<string, { fcas: number; arb: number; network: number; vpp: number }> = {}
  for (const r of data.revenue_records) {
    if (!revenueByAsset[r.asset_id]) {
      revenueByAsset[r.asset_id] = { fcas: 0, arb: 0, network: 0, vpp: 0 }
    }
    revenueByAsset[r.asset_id].fcas += r.fcas_revenue_m
    revenueByAsset[r.asset_id].arb += r.wholesale_revenue_m
    revenueByAsset[r.asset_id].network += r.network_support_revenue_m
    revenueByAsset[r.asset_id].vpp += r.vpp_aggregation_m
  }
  const revenueStackData = Object.entries(revenueByAsset).map(([id, v]) => ({
    asset: id,
    FCAS: +v.fcas.toFixed(3),
    Arbitrage: +v.arb.toFixed(3),
    Network: +v.network.toFixed(3),
    VPP: +v.vpp.toFixed(3),
  }))

  // ---------- Scenario optimisation comparison (all assets, by scenario) ----------
  const scenarioMap: Record<string, { revenue: number; irr: number; count: number }> = {}
  for (const o of data.optimisations) {
    if (!scenarioMap[o.scenario]) scenarioMap[o.scenario] = { revenue: 0, irr: 0, count: 0 }
    scenarioMap[o.scenario].revenue += o.total_revenue_m_pa
    scenarioMap[o.scenario].irr += o.irr_pct
    scenarioMap[o.scenario].count += 1
  }
  const scenarioData = Object.entries(scenarioMap).map(([scen, v]) => ({
    scenario: scen,
    'Avg Revenue ($M)': +(v.revenue / v.count).toFixed(3),
    'Avg IRR (%)': +(v.irr / v.count).toFixed(2),
  }))

  // ---------- Revenue projection lines 2024-2029 ----------
  const projAssets = [...new Set(data.projections.map(p => p.asset_id))]
  const projByYear: Record<number, Record<string, number>> = {}
  for (const p of data.projections) {
    if (!projByYear[p.year]) projByYear[p.year] = { year: p.year }
    projByYear[p.year][p.asset_id] = p.projected_revenue_m
  }
  const projectionData = Object.values(projByYear).sort((a, b) => Number(a.year) - Number(b.year))
  const projColors = ['#34d399', '#60a5fa', '#f59e0b', '#a78bfa', '#fb7185']

  // ---------- Dispatch mode hours by asset ----------
  const dispatchByAsset: Record<string, Record<string, number>> = {}
  for (const d of data.dispatch_records) {
    if (!dispatchByAsset[d.asset_id]) dispatchByAsset[d.asset_id] = { Charging: 0, Discharging: 0, 'FCAS Enabled': 0, Standby: 0 }
    dispatchByAsset[d.asset_id][d.dispatch_mode] = (dispatchByAsset[d.asset_id][d.dispatch_mode] || 0) + 1
  }
  const dispatchData = Object.entries(dispatchByAsset).map(([id, v]) => ({
    asset: id, ...v,
  }))

  // ---------- Best strategy per asset ----------
  const bestByAsset: Record<string, BRSOOptimisationRecord> = {}
  for (const o of data.optimisations) {
    if (o.optimal_strategy) bestByAsset[o.asset_id] = o
  }
  const bestStrategies = Object.values(bestByAsset)

  return (
    <div className="p-6 bg-gray-900 min-h-screen text-gray-100">
      {/* Header */}
      <div className="flex items-center gap-3 mb-6">
        <Battery size={28} className="text-green-400" />
        <div>
          <h1 className="text-2xl font-bold text-white">Battery Revenue Stack Optimisation</h1>
          <p className="text-sm text-gray-400 mt-0.5">Multi-revenue stream optimisation analytics for grid-scale BESS</p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        {kpis.map(kpi => (
          <div key={kpi.label} className="bg-gray-800 rounded-lg p-4 border border-gray-700">
            <p className="text-xs text-gray-400 uppercase tracking-wide mb-1">{kpi.label}</p>
            <p className={`text-2xl font-bold ${kpi.color}`}>{kpi.value}</p>
          </div>
        ))}
      </div>

      {/* Row 1: Revenue stack + Scenario comparison */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6 mb-6">
        <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
          <h2 className="text-sm font-semibold text-gray-300 mb-3">Revenue Stack Breakdown per Asset ($M, Jan–Jun 2024)</h2>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={revenueStackData} margin={{ top: 4, right: 16, left: 0, bottom: 30 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="asset" tick={{ fill: '#9ca3af', fontSize: 10 }} angle={-30} textAnchor="end" />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 10 }} />
              <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }} />
              <Legend wrapperStyle={{ color: '#d1d5db', fontSize: 12 }} />
              <Bar dataKey="FCAS" stackId="a" fill="#34d399" />
              <Bar dataKey="Arbitrage" stackId="a" fill="#60a5fa" />
              <Bar dataKey="Network" stackId="a" fill="#f59e0b" />
              <Bar dataKey="VPP" stackId="a" fill="#a78bfa" />
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
          <h2 className="text-sm font-semibold text-gray-300 mb-3">Scenario Optimisation — Avg Revenue & IRR by Strategy</h2>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={scenarioData} margin={{ top: 4, right: 16, left: 0, bottom: 50 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="scenario" tick={{ fill: '#9ca3af', fontSize: 9 }} angle={-35} textAnchor="end" />
              <YAxis yAxisId="left" tick={{ fill: '#9ca3af', fontSize: 10 }} />
              <YAxis yAxisId="right" orientation="right" tick={{ fill: '#9ca3af', fontSize: 10 }} />
              <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }} />
              <Legend wrapperStyle={{ color: '#d1d5db', fontSize: 12 }} />
              <Bar yAxisId="left" dataKey="Avg Revenue ($M)" fill="#34d399" />
              <Bar yAxisId="right" dataKey="Avg IRR (%)" fill="#fb7185" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Row 2: Projection lines + Dispatch modes */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6 mb-6">
        <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
          <h2 className="text-sm font-semibold text-gray-300 mb-3">Asset Revenue Projection 2024–2029 ($M)</h2>
          <ResponsiveContainer width="100%" height={260}>
            <LineChart data={projectionData} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="year" tick={{ fill: '#9ca3af', fontSize: 10 }} />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 10 }} />
              <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }} />
              <Legend wrapperStyle={{ color: '#d1d5db', fontSize: 11 }} />
              {projAssets.map((aid, i) => (
                <Line
                  key={aid}
                  type="monotone"
                  dataKey={aid}
                  stroke={projColors[i % projColors.length]}
                  strokeWidth={2}
                  dot={{ r: 3 }}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>

        <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
          <h2 className="text-sm font-semibold text-gray-300 mb-3">Dispatch Mode Intervals by Asset</h2>
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={dispatchData} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="asset" tick={{ fill: '#9ca3af', fontSize: 10 }} />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 10 }} />
              <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }} />
              <Legend wrapperStyle={{ color: '#d1d5db', fontSize: 12 }} />
              <Bar dataKey="Charging" stackId="m" fill="#60a5fa" />
              <Bar dataKey="Discharging" stackId="m" fill="#34d399" />
              <Bar dataKey="FCAS Enabled" stackId="m" fill="#f59e0b" />
              <Bar dataKey="Standby" stackId="m" fill="#6b7280" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Table 1: Asset overview */}
      <div className="bg-gray-800 rounded-lg p-4 border border-gray-700 mb-6">
        <h2 className="text-sm font-semibold text-gray-300 mb-3">Asset Overview — Revenue Streams & Cycles</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-xs text-left">
            <thead>
              <tr className="border-b border-gray-700 text-gray-400 uppercase tracking-wide">
                <th className="pb-2 pr-3">Asset</th>
                <th className="pb-2 pr-3">Region</th>
                <th className="pb-2 pr-3">Tech</th>
                <th className="pb-2 pr-3">Cap (MW)</th>
                <th className="pb-2 pr-3">Duration (h)</th>
                <th className="pb-2 pr-3">Primary</th>
                <th className="pb-2 pr-3">Secondary</th>
                <th className="pb-2 pr-3">Cycles/yr</th>
                <th className="pb-2 pr-3">RTE %</th>
                <th className="pb-2">Life (yr)</th>
              </tr>
            </thead>
            <tbody>
              {data.assets.map((a: BRSOAssetRecord) => (
                <tr key={a.asset_id} className="border-b border-gray-700/50 hover:bg-gray-700/30 transition-colors">
                  <td className="py-1.5 pr-3 font-medium text-white">{a.asset_name}</td>
                  <td className="py-1.5 pr-3 text-gray-300">{a.region}</td>
                  <td className="py-1.5 pr-3">
                    <span className={`px-1.5 py-0.5 rounded text-xs ${
                      a.technology === 'LFP' ? 'bg-green-900/50 text-green-300' :
                      a.technology === 'NMC' ? 'bg-blue-900/50 text-blue-300' :
                      a.technology === 'Vanadium Flow' ? 'bg-purple-900/50 text-purple-300' :
                      'bg-amber-900/50 text-amber-300'
                    }`}>{a.technology}</span>
                  </td>
                  <td className="py-1.5 pr-3 text-gray-300">{a.capacity_mw}</td>
                  <td className="py-1.5 pr-3 text-gray-300">{a.duration_hours}</td>
                  <td className="py-1.5 pr-3 text-green-300">{a.primary_revenue_stream}</td>
                  <td className="py-1.5 pr-3 text-blue-300">{a.secondary_revenue_stream}</td>
                  <td className="py-1.5 pr-3 text-gray-300">{a.cycles_pa}</td>
                  <td className="py-1.5 pr-3 text-gray-300">{a.round_trip_efficiency_pct.toFixed(1)}%</td>
                  <td className="py-1.5 text-gray-300">{a.remaining_life_years.toFixed(1)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Table 2: Best strategy per asset */}
      <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
        <h2 className="text-sm font-semibold text-gray-300 mb-3">Optimal Strategy per Asset</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-xs text-left">
            <thead>
              <tr className="border-b border-gray-700 text-gray-400 uppercase tracking-wide">
                <th className="pb-2 pr-3">Asset</th>
                <th className="pb-2 pr-3">Optimal Scenario</th>
                <th className="pb-2 pr-3">Revenue $M/yr</th>
                <th className="pb-2 pr-3">FCAS %</th>
                <th className="pb-2 pr-3">Arb %</th>
                <th className="pb-2 pr-3">Network %</th>
                <th className="pb-2 pr-3">Cycles/yr</th>
                <th className="pb-2 pr-3">LCOE $/MWh</th>
                <th className="pb-2">IRR %</th>
              </tr>
            </thead>
            <tbody>
              {bestStrategies.map((o: BRSOOptimisationRecord) => (
                <tr key={o.opt_id} className="border-b border-gray-700/50 hover:bg-gray-700/30 transition-colors">
                  <td className="py-1.5 pr-3 font-medium text-white">{o.asset_id}</td>
                  <td className="py-1.5 pr-3">
                    <span className="px-2 py-0.5 rounded bg-green-900/60 text-green-300 text-xs">{o.scenario}</span>
                  </td>
                  <td className="py-1.5 pr-3 text-green-300">${o.total_revenue_m_pa.toFixed(3)}</td>
                  <td className="py-1.5 pr-3 text-blue-300">{o.fcas_share_pct.toFixed(1)}%</td>
                  <td className="py-1.5 pr-3 text-amber-300">{o.arb_share_pct.toFixed(1)}%</td>
                  <td className="py-1.5 pr-3 text-purple-300">{o.network_share_pct.toFixed(1)}%</td>
                  <td className="py-1.5 pr-3 text-gray-300">{o.cycles_pa}</td>
                  <td className="py-1.5 pr-3 text-gray-300">${o.lcoe_dolpermwh.toFixed(1)}</td>
                  <td className="py-1.5 font-semibold text-green-400">{o.irr_pct.toFixed(2)}%</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
