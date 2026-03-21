import { useEffect, useState } from 'react'
import {
  LineChart,
  Line,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ScatterChart,
  Scatter,
  ZAxis,
} from 'recharts'
import { FileText } from 'lucide-react'
import { api, getPPAStructuringDashboard } from '../api/client'
import type {
  PPASDashboard,
  PPASContractRecord,
  PPASPricingModelRecord,
  PPASRiskRecord,
  PPASBuyerProfileRecord,
  PPASSettlementRecord,
} from '../api/client'

// ---------------------------------------------------------------------------
// Colour / badge helpers
// ---------------------------------------------------------------------------

const TECH_COLORS: Record<string, string> = {
  SOLAR: '#f59e0b',
  WIND: '#22c55e',
  HYBRID: '#a855f7',
  HYDRO: '#3b82f6',
}

function techBadge(tech: string) {
  const map: Record<string, string> = {
    SOLAR: 'bg-amber-900/30 text-amber-300',
    WIND: 'bg-green-900/30 text-green-300',
    HYBRID: 'bg-purple-900/30 text-purple-300',
    HYDRO: 'bg-blue-900/30 text-blue-300',
  }
  return map[tech] ?? 'bg-gray-700 text-gray-300'
}

function structureBadge(structure: string) {
  const map: Record<string, string> = {
    FIRMED: 'bg-indigo-900/30 text-indigo-300',
    SHAPED: 'bg-teal-900/30 text-teal-300',
    PAY_AS_PRODUCED: 'bg-sky-900/30 text-sky-300',
    BASELOAD: 'bg-slate-700 text-slate-300',
    FLOOR_PRICE: 'bg-rose-900/30 text-rose-300',
  }
  return map[structure] ?? 'bg-gray-700 text-gray-300'
}

function severityBadge(severity: string) {
  const map: Record<string, string> = {
    CRITICAL: 'bg-red-900/50 text-red-200',
    HIGH: 'bg-orange-900/40 text-orange-300',
    MEDIUM: 'bg-yellow-900/40 text-yellow-300',
    LOW: 'bg-green-900/30 text-green-300',
  }
  return map[severity] ?? 'bg-gray-700 text-gray-300'
}

function creditRatingBadge(rating: string) {
  const map: Record<string, string> = {
    AAA: 'bg-green-900/40 text-green-200',
    AA: 'bg-green-900/30 text-green-300',
    A: 'bg-teal-900/30 text-teal-300',
    BBB: 'bg-yellow-900/30 text-yellow-300',
    BB: 'bg-orange-900/30 text-orange-300',
    NR: 'bg-gray-700 text-gray-400',
  }
  return map[rating] ?? 'bg-gray-700 text-gray-300'
}

function priceSensitivityBadge(sensitivity: string) {
  const map: Record<string, string> = {
    HIGH: 'bg-red-900/30 text-red-300',
    MEDIUM: 'bg-yellow-900/30 text-yellow-300',
    LOW: 'bg-green-900/30 text-green-300',
  }
  return map[sensitivity] ?? 'bg-gray-700 text-gray-300'
}

// ---------------------------------------------------------------------------
// KPI Card
// ---------------------------------------------------------------------------

function KpiCard({
  label,
  value,
  sub,
}: {
  label: string
  value: string
  sub?: string
}) {
  return (
    <div className="bg-gray-800 rounded-lg border border-gray-700 p-4">
      <div className="text-xs text-gray-400 font-medium uppercase tracking-wide mb-1">
        {label}
      </div>
      <div className="text-2xl font-bold text-gray-100">{value}</div>
      {sub && <div className="text-xs text-gray-500 mt-1">{sub}</div>}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Pricing Model Line Chart
// ---------------------------------------------------------------------------

function PricingModelChart({ data }: { data: PPASPricingModelRecord[] }) {
  const scenario = 'Base Case'
  const years = [2025, 2028, 2030, 2035]
  const techs = ['SOLAR', 'WIND', 'HYBRID']

  // Average fair value across regions per year per tech (Base Case only)
  const filtered = data.filter((d) => d.scenario === scenario && techs.includes(d.technology))

  const chartData = years.map((yr) => {
    const row: Record<string, number | string> = { year: String(yr) }
    for (const tech of techs) {
      const subset = filtered.filter((d) => d.technology === tech && d.year === yr)
      if (subset.length > 0) {
        row[tech] = Math.round(
          (subset.reduce((s, d) => s + d.fair_value_aud_per_mwh, 0) / subset.length) * 100,
        ) / 100
      }
    }
    return row
  })

  return (
    <div className="bg-gray-800 rounded-lg border border-gray-700 p-4">
      <h3 className="text-sm font-semibold text-gray-300 mb-3">
        PPA Fair Value by Year — Base Case (AUD/MWh)
      </h3>
      <ResponsiveContainer width="100%" height={260}>
        <LineChart data={chartData} margin={{ top: 4, right: 24, bottom: 4, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis dataKey="year" stroke="#9ca3af" tick={{ fill: '#9ca3af', fontSize: 12 }} />
          <YAxis stroke="#9ca3af" tick={{ fill: '#9ca3af', fontSize: 12 }} unit=" $" />
          <Tooltip
            contentStyle={{
              backgroundColor: '#1f2937',
              border: '1px solid #374151',
              borderRadius: '6px',
              color: '#f9fafb',
            }}
            formatter={(value: number, name: string) => [`$${value} /MWh`, name]}
          />
          <Legend wrapperStyle={{ color: '#d1d5db' }} />
          {techs.map((tech) => (
            <Line
              key={tech}
              type="monotone"
              dataKey={tech}
              stroke={TECH_COLORS[tech]}
              strokeWidth={2}
              dot={{ r: 4, fill: TECH_COLORS[tech] }}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Risk Matrix (Scatter)
// ---------------------------------------------------------------------------

function RiskMatrix({ risks }: { risks: PPASRiskRecord[] }) {
  const scatterData = risks.map((r) => ({
    x: r.probability_pct,
    y: r.financial_impact_aud_m,
    z: r.residual_risk_score * 10,
    label: r.risk_type,
    severity: r.severity,
  }))

  return (
    <div className="bg-gray-800 rounded-lg border border-gray-700 p-4">
      <h3 className="text-sm font-semibold text-gray-300 mb-3">
        Risk Matrix — Probability vs Financial Impact
      </h3>
      <ResponsiveContainer width="100%" height={260}>
        <ScatterChart margin={{ top: 8, right: 24, bottom: 8, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis
            dataKey="x"
            name="Probability"
            unit="%"
            stroke="#9ca3af"
            tick={{ fill: '#9ca3af', fontSize: 11 }}
            label={{ value: 'Probability (%)', position: 'insideBottom', offset: -4, fill: '#6b7280', fontSize: 11 }}
          />
          <YAxis
            dataKey="y"
            name="Impact"
            unit="M"
            stroke="#9ca3af"
            tick={{ fill: '#9ca3af', fontSize: 11 }}
            label={{ value: 'Impact (A$M)', angle: -90, position: 'insideLeft', fill: '#6b7280', fontSize: 11 }}
          />
          <ZAxis dataKey="z" range={[60, 400]} />
          <Tooltip
            cursor={{ strokeDasharray: '3 3' }}
            contentStyle={{
              backgroundColor: '#1f2937',
              border: '1px solid #374151',
              borderRadius: '6px',
              color: '#f9fafb',
            }}
            formatter={(value: number, name: string) => {
              if (name === 'Probability') return [`${value}%`, name]
              if (name === 'Impact') return [`A$${value}M`, name]
              return [value, name]
            }}
          />
          <Scatter
            data={scatterData}
            fill="#f59e0b"
            opacity={0.8}
          />
        </ScatterChart>
      </ResponsiveContainer>
      <div className="mt-3 flex flex-wrap gap-2">
        {risks.map((r) => (
          <span
            key={r.risk_type}
            className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium ${severityBadge(r.severity)}`}
          >
            {r.risk_type} — {r.severity}
          </span>
        ))}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Settlement Bar Chart
// ---------------------------------------------------------------------------

function SettlementChart({
  settlements,
  contractId,
}: {
  settlements: PPASSettlementRecord[]
  contractId: string
}) {
  const filtered = settlements
    .filter((s) => s.contract_id === contractId)
    .sort((a, b) => a.settlement_month.localeCompare(b.settlement_month))

  const chartData = filtered.map((s) => ({
    month: s.settlement_month.slice(5), // MM
    net_payment: s.net_payment_aud_k,
    metered: s.metered_mwh,
    contracted: s.contracted_mwh,
  }))

  return (
    <div className="bg-gray-800 rounded-lg border border-gray-700 p-4">
      <h3 className="text-sm font-semibold text-gray-300 mb-1">
        Settlement Analysis — {contractId}
      </h3>
      <p className="text-xs text-gray-500 mb-3">Net payment (A$k) by settlement month, 2024</p>
      <ResponsiveContainer width="100%" height={240}>
        <BarChart data={chartData} margin={{ top: 4, right: 16, bottom: 4, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis dataKey="month" stroke="#9ca3af" tick={{ fill: '#9ca3af', fontSize: 11 }} />
          <YAxis stroke="#9ca3af" tick={{ fill: '#9ca3af', fontSize: 11 }} unit="k" />
          <Tooltip
            contentStyle={{
              backgroundColor: '#1f2937',
              border: '1px solid #374151',
              borderRadius: '6px',
              color: '#f9fafb',
            }}
            formatter={(value: number) => [`A$${value.toFixed(1)}k`, 'Net Payment']}
          />
          <Bar dataKey="net_payment" fill="#22c55e" radius={[2, 2, 0, 0]} name="Net Payment (A$k)" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Contract Table
// ---------------------------------------------------------------------------

function ContractTable({ contracts }: { contracts: PPASContractRecord[] }) {
  return (
    <div className="bg-gray-800 rounded-lg border border-gray-700 p-4">
      <h3 className="text-sm font-semibold text-gray-300 mb-3">Active PPA Contracts</h3>
      <div className="overflow-x-auto">
        <table className="min-w-full text-xs text-gray-300">
          <thead>
            <tr className="border-b border-gray-700">
              <th className="text-left py-2 px-2 text-gray-400 font-medium">Contract</th>
              <th className="text-left py-2 px-2 text-gray-400 font-medium">Buyer</th>
              <th className="text-left py-2 px-2 text-gray-400 font-medium">Seller</th>
              <th className="text-left py-2 px-2 text-gray-400 font-medium">Tech</th>
              <th className="text-left py-2 px-2 text-gray-400 font-medium">Region</th>
              <th className="text-left py-2 px-2 text-gray-400 font-medium">Structure</th>
              <th className="text-right py-2 px-2 text-gray-400 font-medium">Strike ($/MWh)</th>
              <th className="text-right py-2 px-2 text-gray-400 font-medium">Volume (GWh/yr)</th>
              <th className="text-right py-2 px-2 text-gray-400 font-medium">Start</th>
              <th className="text-right py-2 px-2 text-gray-400 font-medium">Term (yr)</th>
            </tr>
          </thead>
          <tbody>
            {contracts.map((c) => (
              <tr
                key={c.contract_id}
                className="border-b border-gray-700/50 hover:bg-gray-700/30 transition-colors"
              >
                <td className="py-2 px-2 font-mono text-gray-200">{c.contract_id}</td>
                <td className="py-2 px-2 text-gray-300">{c.buyer}</td>
                <td className="py-2 px-2 text-gray-400">{c.seller}</td>
                <td className="py-2 px-2">
                  <span
                    className={`inline-block px-2 py-0.5 rounded text-xs font-medium ${techBadge(c.technology)}`}
                  >
                    {c.technology}
                  </span>
                </td>
                <td className="py-2 px-2 text-gray-400">{c.region}</td>
                <td className="py-2 px-2">
                  <span
                    className={`inline-block px-2 py-0.5 rounded text-xs font-medium ${structureBadge(c.structure)}`}
                  >
                    {c.structure.replace(/_/g, ' ')}
                  </span>
                </td>
                <td className="py-2 px-2 text-right font-mono text-amber-300">
                  ${c.strike_price_aud_per_mwh.toFixed(2)}
                </td>
                <td className="py-2 px-2 text-right font-mono">
                  {(c.volume_mwh_per_year / 1000).toFixed(1)}
                </td>
                <td className="py-2 px-2 text-right text-gray-400">{c.start_year}</td>
                <td className="py-2 px-2 text-right text-gray-400">{c.contract_term_years}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Risk Detail Table
// ---------------------------------------------------------------------------

function RiskTable({ risks }: { risks: PPASRiskRecord[] }) {
  return (
    <div className="bg-gray-800 rounded-lg border border-gray-700 p-4">
      <h3 className="text-sm font-semibold text-gray-300 mb-3">Risk Register</h3>
      <div className="overflow-x-auto">
        <table className="min-w-full text-xs text-gray-300">
          <thead>
            <tr className="border-b border-gray-700">
              <th className="text-left py-2 px-2 text-gray-400 font-medium">Risk Type</th>
              <th className="text-left py-2 px-2 text-gray-400 font-medium">Severity</th>
              <th className="text-right py-2 px-2 text-gray-400 font-medium">Probability (%)</th>
              <th className="text-right py-2 px-2 text-gray-400 font-medium">Impact (A$M)</th>
              <th className="text-right py-2 px-2 text-gray-400 font-medium">Residual Score</th>
              <th className="text-left py-2 px-2 text-gray-400 font-medium">Mitigation</th>
            </tr>
          </thead>
          <tbody>
            {risks.map((r) => (
              <tr
                key={r.risk_type}
                className="border-b border-gray-700/50 hover:bg-gray-700/30 transition-colors"
              >
                <td className="py-2 px-2 font-semibold text-gray-200">{r.risk_type}</td>
                <td className="py-2 px-2">
                  <span
                    className={`inline-block px-2 py-0.5 rounded text-xs font-medium ${severityBadge(r.severity)}`}
                  >
                    {r.severity}
                  </span>
                </td>
                <td className="py-2 px-2 text-right font-mono">{r.probability_pct.toFixed(1)}%</td>
                <td className="py-2 px-2 text-right font-mono text-red-300">
                  A${r.financial_impact_aud_m.toFixed(1)}M
                </td>
                <td className="py-2 px-2 text-right font-mono">
                  <span
                    className={
                      r.residual_risk_score >= 4.5
                        ? 'text-red-300'
                        : r.residual_risk_score >= 3.0
                        ? 'text-yellow-300'
                        : 'text-green-300'
                    }
                  >
                    {r.residual_risk_score.toFixed(1)}
                  </span>
                </td>
                <td className="py-2 px-2 text-gray-400 max-w-xs truncate">{r.mitigation}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Buyer Profiles Table
// ---------------------------------------------------------------------------

function BuyerProfilesTable({ profiles }: { profiles: PPASBuyerProfileRecord[] }) {
  return (
    <div className="bg-gray-800 rounded-lg border border-gray-700 p-4">
      <h3 className="text-sm font-semibold text-gray-300 mb-3">Buyer Profiles</h3>
      <div className="overflow-x-auto">
        <table className="min-w-full text-xs text-gray-300">
          <thead>
            <tr className="border-b border-gray-700">
              <th className="text-left py-2 px-2 text-gray-400 font-medium">Type</th>
              <th className="text-right py-2 px-2 text-gray-400 font-medium">Typical Vol (GWh)</th>
              <th className="text-left py-2 px-2 text-gray-400 font-medium">Price Sensitivity</th>
              <th className="text-left py-2 px-2 text-gray-400 font-medium">Preferred Structure</th>
              <th className="text-left py-2 px-2 text-gray-400 font-medium">Credit Rating</th>
              <th className="text-right py-2 px-2 text-gray-400 font-medium">RE Target (%)</th>
              <th className="text-right py-2 px-2 text-gray-400 font-medium">Avg Term (yr)</th>
            </tr>
          </thead>
          <tbody>
            {profiles.map((p) => (
              <tr
                key={p.buyer_type}
                className="border-b border-gray-700/50 hover:bg-gray-700/30 transition-colors"
              >
                <td className="py-2 px-2 font-semibold text-gray-200">{p.buyer_type}</td>
                <td className="py-2 px-2 text-right font-mono">{p.typical_volume_gwh.toFixed(0)}</td>
                <td className="py-2 px-2">
                  <span
                    className={`inline-block px-2 py-0.5 rounded text-xs font-medium ${priceSensitivityBadge(p.price_sensitivity)}`}
                  >
                    {p.price_sensitivity}
                  </span>
                </td>
                <td className="py-2 px-2">
                  <span
                    className={`inline-block px-2 py-0.5 rounded text-xs font-medium ${structureBadge(p.preferred_structure)}`}
                  >
                    {p.preferred_structure.replace(/_/g, ' ')}
                  </span>
                </td>
                <td className="py-2 px-2">
                  <span
                    className={`inline-block px-2 py-0.5 rounded text-xs font-bold font-mono ${creditRatingBadge(p.credit_rating)}`}
                  >
                    {p.credit_rating}
                  </span>
                </td>
                <td className="py-2 px-2 text-right font-mono text-green-300">
                  {p.renewable_target_pct.toFixed(0)}%
                </td>
                <td className="py-2 px-2 text-right font-mono">{p.avg_term_years.toFixed(0)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------

export default function PPAStructuringAnalytics() {
  const [data, setData] = useState<PPASDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getPPAStructuringDashboard()
      .then((d) => {
        setData(d)
        setLoading(false)
      })
      .catch((err: any) => {
        setError(err?.message ?? 'Failed to load PPA structuring data')
        setLoading(false)
      })
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-gray-400">
        Loading PPA Structuring Analytics…
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="flex items-center justify-center h-64 text-red-400">
        {error ?? 'No data available'}
      </div>
    )
  }

  const summary = data.summary as Record<string, unknown>
  const firstContractId = data.contracts[0]?.contract_id ?? ''

  return (
    <div className="min-h-screen bg-gray-900 text-gray-100 p-6">
      {/* Header */}
      <div className="flex items-center gap-3 mb-6">
        <FileText className="text-indigo-400" size={28} />
        <div>
          <h1 className="text-2xl font-bold text-gray-100">PPA Structuring Analytics</h1>
          <p className="text-sm text-gray-400 mt-0.5">
            Contract structuring, pricing models, risk analytics and settlement — Australian NEM
          </p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
        <KpiCard
          label="Total Contracts"
          value={String(summary.total_contracts ?? '—')}
          sub="Active PPA portfolio"
        />
        <KpiCard
          label="Avg Strike Price"
          value={`$${(summary.avg_strike_price_aud_mwh as number)?.toFixed(2) ?? '—'}`}
          sub="AUD/MWh across portfolio"
        />
        <KpiCard
          label="Total Contracted"
          value={`${(summary.total_contracted_gwh_per_year as number)?.toFixed(2) ?? '—'} TWh/yr`}
          sub="Aggregated annual volume"
        />
        <KpiCard
          label="Avg Contract Term"
          value={`${(summary.avg_contract_term_years as number)?.toFixed(1) ?? '—'} yrs`}
          sub={`Most common: ${summary.most_common_structure ?? '—'}`}
        />
      </div>

      {/* Charts row: Pricing + Risk Matrix */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-6">
        <PricingModelChart data={data.pricing_models} />
        <RiskMatrix risks={data.risks} />
      </div>

      {/* Settlement Analysis */}
      <div className="mb-6">
        <SettlementChart settlements={data.settlements} contractId={firstContractId} />
      </div>

      {/* Contract Table */}
      <div className="mb-6">
        <ContractTable contracts={data.contracts} />
      </div>

      {/* Risk Table + Buyer Profiles */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-6">
        <RiskTable risks={data.risks} />
        <BuyerProfilesTable profiles={data.buyer_profiles} />
      </div>

      {/* Footer summary */}
      <div className="text-xs text-gray-600 text-right">
        {summary.scenarios_modelled as number} pricing scenarios modelled &mdash; Sprint 92a
      </div>
    </div>
  )
}
