import { useEffect, useState } from 'react'
import { Shield } from 'lucide-react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import { api } from '../api/client'
import type {
  ConsumerProtectionDashboard,
  RetailOfferRecord,
  ConsumerComplaintRecord,
  SwitchingRateRecord,
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

// ── Offer Type Badge ───────────────────────────────────────────────────────

function OfferTypeBadge({ type }: { type: string }) {
  const map: Record<string, string> = {
    DMO:          'bg-gray-600 text-gray-100',
    STANDING:     'bg-yellow-700 text-yellow-100',
    MARKET_VFT:   'bg-blue-700 text-blue-100',
    MARKET_FIXED: 'bg-green-700 text-green-100',
    BASIC_PLAN:   'bg-purple-700 text-purple-100',
  }
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-semibold ${map[type] ?? 'bg-gray-700 text-gray-200'}`}>
      {type.replace('_', ' ')}
    </span>
  )
}

// ── Trigger Badge ──────────────────────────────────────────────────────────

function TriggerBadge({ trigger }: { trigger: string }) {
  const map: Record<string, string> = {
    PRICE:     'bg-red-800 text-red-100',
    SERVICE:   'bg-blue-800 text-blue-100',
    DOORKNOCK: 'bg-orange-800 text-orange-100',
    DIGITAL:   'bg-teal-800 text-teal-100',
    OTHER:     'bg-gray-700 text-gray-200',
  }
  return (
    <span className={`px-2 py-0.5 rounded text-xs font-semibold ${map[trigger] ?? 'bg-gray-700 text-gray-200'}`}>
      {trigger}
    </span>
  )
}

// ── Complaints Stacked Bar Chart ───────────────────────────────────────────

interface ComplaintChartRow {
  quarter: string
  BILLING?: number
  DISCONNECTION?: number
  CONTRACT?: number
}

function buildComplaintData(
  complaints: ConsumerComplaintRecord[],
  state: string,
): ComplaintChartRow[] {
  const byQtr: Record<string, ComplaintChartRow> = {}
  complaints
    .filter(c => c.state === state && ['BILLING', 'DISCONNECTION', 'CONTRACT'].includes(c.category))
    .forEach(c => {
      if (!byQtr[c.quarter]) byQtr[c.quarter] = { quarter: c.quarter }
      ;(byQtr[c.quarter] as unknown as Record<string, unknown>)[c.category] = c.complaint_count
    })
  return Object.values(byQtr).sort((a, b) => a.quarter.localeCompare(b.quarter))
}

interface ComplaintChartProps {
  complaints: ConsumerComplaintRecord[]
  state: string
}

function ComplaintsChart({ complaints, state }: ComplaintChartProps) {
  const data = buildComplaintData(complaints, state)
  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <h2 className="text-sm font-semibold text-gray-200 uppercase tracking-wide mb-4">
        Complaints by Quarter — {state} (Stacked by Category)
      </h2>
      <ResponsiveContainer width="100%" height={280}>
        <BarChart data={data} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis dataKey="quarter" tick={{ fontSize: 11, fill: '#9ca3af' }} />
          <YAxis tick={{ fontSize: 11, fill: '#9ca3af' }} />
          <Tooltip
            contentStyle={{ backgroundColor: '#1f2937', border: 'none', borderRadius: 6 }}
            labelStyle={{ color: '#e5e7eb' }}
            itemStyle={{ color: '#d1d5db' }}
          />
          <Legend wrapperStyle={{ fontSize: 12, color: '#9ca3af' }} />
          <Bar dataKey="BILLING"      stackId="a" fill="#ef4444" name="Billing" />
          <Bar dataKey="DISCONNECTION" stackId="a" fill="#f97316" name="Disconnection" />
          <Bar dataKey="CONTRACT"     stackId="a" fill="#eab308" name="Contract" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

// ── Retail Offers Table ────────────────────────────────────────────────────

const STATES = ['ALL', 'NSW', 'VIC', 'QLD', 'SA']

interface RetailOffersTableProps {
  offers: RetailOfferRecord[]
}

function RetailOffersTable({ offers }: RetailOffersTableProps) {
  const [stateFilter, setStateFilter] = useState<string>('ALL')

  const filtered = stateFilter === 'ALL'
    ? offers
    : offers.filter(o => o.state === stateFilter)

  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <div className="flex items-center justify-between mb-4 flex-wrap gap-2">
        <h2 className="text-sm font-semibold text-gray-200 uppercase tracking-wide">
          Retail Offers — DMO vs Market Comparison
        </h2>
        <div className="flex items-center gap-2">
          <span className="text-xs text-gray-400">State:</span>
          <select
            value={stateFilter}
            onChange={e => setStateFilter(e.target.value)}
            className="bg-gray-700 text-gray-200 text-xs rounded px-2 py-1 border border-gray-600"
          >
            {STATES.map(s => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
        </div>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs text-gray-300">
          <thead>
            <tr className="text-gray-400 border-b border-gray-700">
              <th className="text-left py-2 pr-3 font-medium">Retailer</th>
              <th className="text-left py-2 pr-3 font-medium">State</th>
              <th className="text-left py-2 pr-3 font-medium">Type</th>
              <th className="text-right py-2 pr-3 font-medium">Annual Bill</th>
              <th className="text-right py-2 pr-3 font-medium">Usage c/kWh</th>
              <th className="text-right py-2 pr-3 font-medium">vs DMO %</th>
              <th className="text-center py-2 pr-3 font-medium">Cond. Disc.</th>
              <th className="text-right py-2 font-medium">Contract (mo)</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map(o => {
              const dmoColour =
                o.peak_vs_dmo_pct < 0 ? 'text-green-400' :
                o.peak_vs_dmo_pct > 0 ? 'text-red-400' : 'text-gray-400'
              return (
                <tr key={o.offer_id} className="border-b border-gray-700 hover:bg-gray-750">
                  <td className="py-2 pr-3 text-white font-medium whitespace-nowrap">{o.retailer}</td>
                  <td className="py-2 pr-3">{o.state}</td>
                  <td className="py-2 pr-3"><OfferTypeBadge type={o.offer_type} /></td>
                  <td className="py-2 pr-3 text-right">${o.annual_bill_aud.toLocaleString()}</td>
                  <td className="py-2 pr-3 text-right">{o.usage_rate_c_kwh.toFixed(2)}</td>
                  <td className={`py-2 pr-3 text-right font-semibold ${dmoColour}`}>
                    {o.peak_vs_dmo_pct === 0 ? '—' : `${o.peak_vs_dmo_pct > 0 ? '+' : ''}${o.peak_vs_dmo_pct.toFixed(1)}%`}
                  </td>
                  <td className="py-2 pr-3 text-center">
                    {o.conditional_discounts ? (
                      <span className="text-green-400 font-bold">✓</span>
                    ) : (
                      <span className="text-red-400 font-bold">✗</span>
                    )}
                  </td>
                  <td className="py-2 text-right">
                    {o.contract_length_months === 0 ? 'No lock-in' : `${o.contract_length_months}`}
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

// ── Switching Rates Table ──────────────────────────────────────────────────

interface SwitchingTableProps {
  records: SwitchingRateRecord[]
}

function SwitchingTable({ records }: SwitchingTableProps) {
  return (
    <div className="bg-gray-800 rounded-lg p-4">
      <h2 className="text-sm font-semibold text-gray-200 uppercase tracking-wide mb-4">
        Customer Switching Rates by Quarter
      </h2>
      <div className="overflow-x-auto">
        <table className="w-full text-xs text-gray-300">
          <thead>
            <tr className="text-gray-400 border-b border-gray-700">
              <th className="text-left py-2 pr-3 font-medium">Quarter</th>
              <th className="text-left py-2 pr-3 font-medium">State</th>
              <th className="text-right py-2 pr-3 font-medium">Total Switches</th>
              <th className="text-right py-2 pr-3 font-medium">per 1,000 Customers</th>
              <th className="text-right py-2 pr-3 font-medium">Inbound</th>
              <th className="text-right py-2 pr-3 font-medium">Outbound</th>
              <th className="text-left py-2 font-medium">Trigger</th>
            </tr>
          </thead>
          <tbody>
            {records.map(r => (
              <tr key={r.record_id} className="border-b border-gray-700 hover:bg-gray-750">
                <td className="py-2 pr-3 font-medium text-white">{r.quarter}</td>
                <td className="py-2 pr-3">{r.state}</td>
                <td className="py-2 pr-3 text-right">{r.total_switches.toLocaleString()}</td>
                <td className="py-2 pr-3 text-right text-amber-300 font-semibold">
                  {r.switches_per_1000_customers.toFixed(1)}
                </td>
                <td className="py-2 pr-3 text-right text-green-400">{r.inbound_switches.toLocaleString()}</td>
                <td className="py-2 pr-3 text-right text-red-400">{r.outbound_switches.toLocaleString()}</td>
                <td className="py-2"><TriggerBadge trigger={r.churn_triggered_by} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── Main Page ──────────────────────────────────────────────────────────────

const CHART_STATES = ['NSW', 'VIC', 'QLD', 'SA']

export default function ConsumerProtection() {
  const [dash, setDash] = useState<ConsumerProtectionDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [chartState, setChartState] = useState<string>('NSW')

  useEffect(() => {
    api.getConsumerProtectionDashboard()
      .then(setDash)
      .catch(e => setError(String(e)))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-gray-400 dark:text-gray-500">
        Loading consumer protection analytics...
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

  return (
    <div className="p-6 bg-gray-900 min-h-screen text-gray-100">
      {/* Header */}
      <div className="flex items-center gap-3 mb-6">
        <Shield className="text-blue-400" size={26} />
        <div>
          <h1 className="text-xl font-bold text-white">
            Consumer Protection &amp; Retail Market Analytics
          </h1>
          <p className="text-xs text-gray-400 mt-0.5">
            Default market offer (DMO) vs standing offer vs market offer, complaint volumes, switching rates, energy ombudsman cases
          </p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
        <KpiCard
          label="Avg DMO Annual Bill"
          value={`$${dash.avg_dmo_annual_bill_aud.toLocaleString()}`}
          sub="Weighted avg across NSW, VIC, QLD, SA"
        />
        <KpiCard
          label="Avg Market Offer Saving"
          value={`${dash.avg_market_offer_saving_pct.toFixed(1)}%`}
          sub="Below DMO reference price"
          valueClass="text-green-400"
        />
        <KpiCard
          label="Total Complaints YTD"
          value={dash.total_complaints_ytd.toLocaleString()}
          sub={`Ombudsman cases: ${dash.ombudsman_cases_ytd.toLocaleString()}`}
          valueClass="text-red-400"
        />
        <KpiCard
          label="Switching Rate per 1,000"
          value={`${dash.avg_switching_rate_per_1000.toFixed(1)}`}
          sub={`Hardship customers: ${dash.hardship_customers_pct.toFixed(1)}%`}
          valueClass="text-amber-400"
        />
      </div>

      {/* Complaints Chart with state filter */}
      <div className="mb-6">
        <div className="flex items-center gap-2 mb-3">
          {CHART_STATES.map(s => (
            <button
              key={s}
              onClick={() => setChartState(s)}
              className={`px-3 py-1 rounded text-xs font-semibold transition-colors ${
                chartState === s
                  ? 'bg-blue-600 text-white'
                  : 'bg-gray-700 text-gray-300 hover:bg-gray-600'
              }`}
            >
              {s}
            </button>
          ))}
        </div>
        <ComplaintsChart complaints={dash.complaints} state={chartState} />
      </div>

      {/* Retail Offers Table */}
      <div className="mb-6">
        <RetailOffersTable offers={dash.retail_offers} />
      </div>

      {/* Switching Rates Table */}
      <SwitchingTable records={dash.switching_rates} />
    </div>
  )
}
