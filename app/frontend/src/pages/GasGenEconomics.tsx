import { useEffect, useState } from 'react'
import { Activity } from 'lucide-react'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import { api } from '../api/client'
import type {
  GasGenEconomicsDashboard,
  GasGeneratorRecord,
  SparkSpreadRecord,
} from '../api/client'

// ── KPI Card ──────────────────────────────────────────────────────────────

interface KpiCardProps {
  label: string
  value: string
  sub?: string
  valueClass?: string
}

function KpiCard({ label, value, sub, valueClass }: KpiCardProps) {
  return (
    <div className="bg-gray-800 rounded-lg p-4 flex flex-col gap-1">
      <span className="text-xs text-gray-400 uppercase tracking-wide">{label}</span>
      <span className={`text-2xl font-bold ${valueClass ?? 'text-white'}`}>{value}</span>
      {sub && <span className="text-xs text-gray-500">{sub}</span>}
    </div>
  )
}

// ── Spark Spread Chart ─────────────────────────────────────────────────────

const REGION_COLOURS: Record<string, string> = {
  NSW1: '#60a5fa',
  QLD1: '#34d399',
  VIC1: '#a78bfa',
  SA1:  '#fb923c',
}

interface SparkChartRow {
  month: string
  NSW1?: number
  QLD1?: number
  VIC1?: number
  SA1?:  number
}

function buildSparkChartData(records: SparkSpreadRecord[]): SparkChartRow[] {
  const byMonth: Record<string, SparkChartRow> = {}
  records.forEach(r => {
    if (!byMonth[r.month]) byMonth[r.month] = { month: r.month }
    ;(byMonth[r.month] as unknown as Record<string, unknown>)[r.region] = r.spark_spread_aud_mwh
  })
  return Object.values(byMonth).sort((a, b) => a.month.localeCompare(b.month))
}

interface SparkSpreadChartProps {
  records: SparkSpreadRecord[]
}

function SparkSpreadChart({ records }: SparkSpreadChartProps) {
  const data = buildSparkChartData(records)
  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <h2 className="text-sm font-semibold text-gray-200 uppercase tracking-wide mb-4">
        12-Month Spark Spread Trend by Region ($/MWh)
      </h2>
      <ResponsiveContainer width="100%" height={280}>
        <LineChart data={data} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis
            dataKey="month"
            tick={{ fontSize: 11, fill: '#9ca3af' }}
            tickFormatter={v => v.slice(2)}
          />
          <YAxis tick={{ fontSize: 11, fill: '#9ca3af' }} unit=" $" />
          <Tooltip
            contentStyle={{ backgroundColor: '#1f2937', border: 'none', borderRadius: 6 }}
            labelStyle={{ color: '#e5e7eb' }}
            itemStyle={{ color: '#d1d5db' }}
            formatter={(val: number) => [`$${val.toFixed(1)}/MWh`]}
          />
          <Legend wrapperStyle={{ fontSize: 12, color: '#9ca3af' }} />
          {Object.entries(REGION_COLOURS).map(([region, colour]) => (
            <Line
              key={region}
              type="monotone"
              dataKey={region}
              stroke={colour}
              strokeWidth={2}
              dot={false}
              name={region}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

// ── Technology Badge ───────────────────────────────────────────────────────

function TechBadge({ tech }: { tech: string }) {
  const map: Record<string, string> = {
    CCGT:      'bg-blue-700 text-blue-100',
    OCGT:      'bg-orange-700 text-orange-100',
    COGEN:     'bg-purple-700 text-purple-100',
    GAS_STEAM: 'bg-red-700 text-red-100',
  }
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-semibold ${map[tech] ?? 'bg-gray-700 text-gray-200'}`}>
      {tech}
    </span>
  )
}

// ── Contract Badge ─────────────────────────────────────────────────────────

function ContractBadge({ type }: { type: string }) {
  const map: Record<string, string> = {
    FIRM_GSA:    'bg-green-700 text-green-100',
    SPOT:        'bg-yellow-700 text-yellow-100',
    INTERRUPTIBLE: 'bg-orange-700 text-orange-100',
  }
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-semibold ${map[type] ?? 'bg-gray-700 text-gray-200'}`}>
      {type.replace('_', ' ')}
    </span>
  )
}

// ── Generators Table ───────────────────────────────────────────────────────

interface GeneratorsTableProps {
  generators: GasGeneratorRecord[]
}

function GeneratorsTable({ generators }: GeneratorsTableProps) {
  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <h2 className="text-sm font-semibold text-gray-200 uppercase tracking-wide mb-4">
        Gas Generator Fleet — SRMC & Economics
      </h2>
      <div className="overflow-x-auto">
        <table className="w-full text-xs text-gray-300">
          <thead>
            <tr className="text-gray-400 border-b border-gray-700">
              <th className="text-left py-2 pr-3 font-medium">Name</th>
              <th className="text-left py-2 pr-3 font-medium">Owner</th>
              <th className="text-left py-2 pr-3 font-medium">State</th>
              <th className="text-left py-2 pr-3 font-medium">Technology</th>
              <th className="text-right py-2 pr-3 font-medium">Cap (MW)</th>
              <th className="text-right py-2 pr-3 font-medium">Heat Rate</th>
              <th className="text-right py-2 pr-3 font-medium">Gas $/GJ</th>
              <th className="text-right py-2 pr-3 font-medium">Fuel $/MWh</th>
              <th className="text-right py-2 pr-3 font-medium">SRMC $/MWh</th>
              <th className="text-right py-2 pr-3 font-medium">CF %</th>
              <th className="text-left py-2 font-medium">Contract</th>
            </tr>
          </thead>
          <tbody>
            {generators.map(g => (
              <tr key={g.generator_id} className="border-b border-gray-700 hover:bg-gray-750">
                <td className="py-2 pr-3 text-white font-medium whitespace-nowrap">{g.name}</td>
                <td className="py-2 pr-3 whitespace-nowrap">{g.owner}</td>
                <td className="py-2 pr-3">{g.state}</td>
                <td className="py-2 pr-3"><TechBadge tech={g.technology} /></td>
                <td className="py-2 pr-3 text-right">{g.registered_capacity_mw.toFixed(0)}</td>
                <td className="py-2 pr-3 text-right">{g.heat_rate_gj_mwh.toFixed(1)}</td>
                <td className="py-2 pr-3 text-right">${g.gas_price_gj.toFixed(2)}</td>
                <td className="py-2 pr-3 text-right">${g.fuel_cost_aud_mwh.toFixed(1)}</td>
                <td className="py-2 pr-3 text-right font-semibold text-amber-300">
                  ${g.short_run_marginal_cost_aud_mwh.toFixed(1)}
                </td>
                <td className="py-2 pr-3 text-right">{g.capacity_factor_pct.toFixed(0)}%</td>
                <td className="py-2"><ContractBadge type={g.gas_contract_type} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── Main Page ──────────────────────────────────────────────────────────────

export default function GasGenEconomics() {
  const [dash, setDash] = useState<GasGenEconomicsDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    api.getGasGenDashboard()
      .then(setDash)
      .catch(e => setError(String(e)))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-gray-400 dark:text-gray-500">
        Loading gas generation economics…
      </div>
    )
  }
  if (error || !dash) {
    return (
      <div className="flex items-center justify-center h-64 text-red-400">
        {error ?? 'Failed to load data'}
      </div>
    )
  }

  const sparkColour =
    dash.avg_spark_spread_aud_mwh > 20
      ? 'text-green-400'
      : dash.avg_spark_spread_aud_mwh >= 5
      ? 'text-amber-400'
      : 'text-red-400'

  return (
    <div className="p-6 bg-gray-900 min-h-screen text-gray-100">
      {/* Header */}
      <div className="flex items-center gap-3 mb-6">
        <Activity className="text-orange-400" size={26} />
        <div>
          <h1 className="text-xl font-bold text-white">
            Gas-Fired Generation Economics Analytics
          </h1>
          <p className="text-xs text-gray-400 mt-0.5">
            Gas supply contracts, heat rates, SRMC, spark spreads & peaker economics — NEM
          </p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
        <KpiCard
          label="Total Gas Capacity"
          value={`${dash.total_gas_capacity_mw.toLocaleString()} MW`}
          sub={`${dash.ccgt_count} CCGT + ${dash.ocgt_count} OCGT/Steam`}
        />
        <KpiCard
          label="CCGT / OCGT Units"
          value={`${dash.ccgt_count} / ${dash.ocgt_count}`}
          sub="Combined + Open Cycle"
        />
        <KpiCard
          label="Avg Heat Rate"
          value={`${dash.avg_heat_rate_gj_mwh.toFixed(2)} GJ/MWh`}
          sub={`Avg gas price $${dash.avg_gas_price_aud_gj.toFixed(2)}/GJ`}
        />
        <KpiCard
          label="Avg Spark Spread"
          value={`$${dash.avg_spark_spread_aud_mwh.toFixed(1)}/MWh`}
          sub="Latest month, 4 regions"
          valueClass={sparkColour}
        />
      </div>

      {/* Spark Spread Chart */}
      <div className="mb-6">
        <SparkSpreadChart records={dash.spark_spreads} />
      </div>

      {/* Generators Table */}
      <GeneratorsTable generators={dash.generators} />
    </div>
  )
}
