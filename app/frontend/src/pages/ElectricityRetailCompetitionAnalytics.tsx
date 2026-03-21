import { useEffect, useState } from 'react'
import { ShoppingCart, Users, ArrowLeftRight, DollarSign, HeartHandshake } from 'lucide-react'
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
  ScatterChart,
  Scatter,
  ZAxis,
  RadarChart,
  Radar,
  PolarGrid,
  PolarAngleAxis,
  PolarRadiusAxis,
} from 'recharts'
import { getERCADashboard } from '../api/client'
import type { ERCARetailCompDashboard } from '../api/client'

// ---------------------------------------------------------------------------
// Colour palettes
// ---------------------------------------------------------------------------
const RETAILER_COLOURS: Record<string, string> = {
  Origin: '#3b82f6',
  AGL: '#f59e0b',
  EnergyAustralia: '#6366f1',
  Alinta: '#10b981',
  'Red Energy': '#ef4444',
  Momentum: '#8b5cf6',
  Powershop: '#ec4899',
  '1st Energy': '#14b8a6',
}

const REGION_COLOURS: Record<string, string> = {
  NSW: '#3b82f6',
  QLD: '#f59e0b',
  VIC: '#6366f1',
  SA: '#10b981',
  TAS: '#8b5cf6',
}

const PLAN_COLOURS: Record<string, string> = {
  STANDING_OFFER: '#ef4444',
  MARKET_OFFER: '#3b82f6',
  VPP_PLAN: '#10b981',
  GREEN_PLAN: '#f59e0b',
}

// ---------------------------------------------------------------------------
// KPI Card
// ---------------------------------------------------------------------------
function KpiCard({
  title,
  value,
  sub,
  icon: Icon,
  color,
}: {
  title: string
  value: string
  sub?: string
  icon: React.ElementType
  color: string
}) {
  return (
    <div className="bg-gray-800 rounded-2xl p-6 flex items-start gap-4">
      <div className={`p-2 rounded-lg ${color}`}>
        <Icon size={22} className="text-white" />
      </div>
      <div>
        <p className="text-xs text-gray-400 mb-1">{title}</p>
        <p className="text-2xl font-bold text-white">{value}</p>
        {sub && <p className="text-xs text-gray-500 mt-1">{sub}</p>}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Chart Card wrapper
// ---------------------------------------------------------------------------
function ChartCard({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="bg-gray-800 rounded-2xl p-6">
      <h3 className="text-sm font-semibold text-gray-200 mb-4">{title}</h3>
      {children}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Badge helpers
// ---------------------------------------------------------------------------
function churnBadge(rate: number) {
  if (rate < 15) return 'bg-green-900 text-green-300'
  if (rate < 25) return 'bg-yellow-900 text-yellow-300'
  return 'bg-red-900 text-red-300'
}

function affordabilityBadge(idx: number) {
  if (idx >= 7) return 'bg-green-900 text-green-300'
  if (idx >= 4) return 'bg-yellow-900 text-yellow-300'
  return 'bg-red-900 text-red-300'
}

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------
export default function ElectricityRetailCompetitionAnalytics() {
  const [data, setData] = useState<ERCARetailCompDashboard | null>(null)

  useEffect(() => {
    getERCADashboard().then(setData).catch(console.error)
  }, [])

  if (!data) {
    return <div className="min-h-screen bg-gray-900 p-8 text-white">Loading ERCA dashboard...</div>
  }

  // Aggregate KPI values
  const totalCustomers = data.retailers.reduce((s, r) => s + r.customer_count_k, 0)
  const avgSwitchingRate =
    data.switching.length > 0
      ? (data.switching.reduce((s, r) => s + r.switching_rate_pct, 0) / data.switching.length).toFixed(1)
      : '0.0'
  const avgPriceDispersion =
    data.price_dispersion.length > 0
      ? Math.round(
          data.price_dispersion.reduce((s, r) => s + (r.max_annual_aud - r.min_annual_aud), 0) /
            data.price_dispersion.length,
        )
      : 0
  const totalHardshipCustomers = data.hardship.reduce((s, r) => s + r.hardship_customers_k, 0)

  // Market share by retailer (aggregate across regions)
  const retailerAgg: Record<string, { share: number; customers: number }> = {}
  for (const r of data.retailers) {
    if (!retailerAgg[r.retailer_name]) retailerAgg[r.retailer_name] = { share: 0, customers: 0 }
    retailerAgg[r.retailer_name].share += r.market_share_pct
    retailerAgg[r.retailer_name].customers += r.customer_count_k
  }
  const marketShareData = Object.entries(retailerAgg)
    .map(([name, v]) => ({ name, share: Math.round(v.share * 10) / 10, customers: v.customers }))
    .sort((a, b) => b.share - a.share)

  // Switching rates over time by region
  const regions = [...new Set(data.switching.map((s) => s.region))]
  const quarters = [...new Set(data.switching.map((s) => s.quarter))]
  const switchingByQuarter = quarters.map((q) => {
    const row: Record<string, string | number> = { quarter: q }
    for (const reg of regions) {
      const rec = data.switching.find((s) => s.quarter === q && s.region === reg)
      row[reg] = rec ? rec.switching_rate_pct : 0
    }
    return row
  })

  // Price dispersion scatter: median vs plan_count coloured by plan_type
  const priceScatterData = data.price_dispersion.map((p) => ({
    region: p.region,
    plan_type: p.plan_type,
    median: p.median_annual_aud,
    spread: p.max_annual_aud - p.min_annual_aud,
    count: p.plan_count,
  }))

  // Radar chart: hardship metrics per retailer (normalised 0-10)
  const radarData = data.hardship.map((h) => ({
    retailer: h.retailer_name,
    affordability: h.affordability_index,
    payment_success: h.payment_plan_success_pct / 10,
    low_disconnection: Math.max(0, 10 - h.disconnection_rate_per_10k / 5),
    low_debt: Math.max(0, 10 - h.debt_level_avg_aud / 200),
  }))

  return (
    <div className="min-h-screen bg-gray-900 text-white p-8 space-y-8">
      {/* Header */}
      <header className="flex items-center gap-3">
        <ShoppingCart className="w-8 h-8 text-blue-400" />
        <div>
          <h1 className="text-3xl font-bold">Electricity Retail Competition Analytics</h1>
          <p className="text-sm text-gray-400 mt-1">
            Sprint 164a ERCA — Retailer market share, switching, price dispersion &amp; hardship
          </p>
        </div>
      </header>

      {/* KPI Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <KpiCard
          title="Total Retail Customers"
          value={`${(totalCustomers / 1000).toFixed(1)}M`}
          sub="across 5 NEM regions"
          icon={Users}
          color="bg-blue-600"
        />
        <KpiCard
          title="Avg Switching Rate"
          value={`${avgSwitchingRate}%`}
          sub="quarterly average"
          icon={ArrowLeftRight}
          color="bg-amber-600"
        />
        <KpiCard
          title="Avg Price Spread"
          value={`$${avgPriceDispersion}`}
          sub="annual AUD (max - min)"
          icon={DollarSign}
          color="bg-indigo-600"
        />
        <KpiCard
          title="Hardship Customers"
          value={`${totalHardshipCustomers.toFixed(1)}k`}
          sub="across all retailers"
          icon={HeartHandshake}
          color="bg-rose-600"
        />
      </div>

      {/* Row 1: Market Share Bar + Switching Line */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <ChartCard title="Retailer Market Share (sum across regions)">
          <ResponsiveContainer width="100%" height={340}>
            <BarChart data={marketShareData} layout="vertical" margin={{ left: 100 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis type="number" tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <YAxis dataKey="name" type="category" tick={{ fill: '#d1d5db', fontSize: 11 }} width={95} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: 'none', borderRadius: 8, color: '#f3f4f6' }}
              />
              <Bar dataKey="share" fill="#3b82f6" radius={[0, 4, 4, 0]} name="Market Share %" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Quarterly Switching Rates by Region">
          <ResponsiveContainer width="100%" height={340}>
            <LineChart data={switchingByQuarter}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="quarter" tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: 'none', borderRadius: 8, color: '#f3f4f6' }}
              />
              <Legend wrapperStyle={{ fontSize: 11, color: '#d1d5db' }} />
              {regions.map((reg) => (
                <Line
                  key={reg}
                  type="monotone"
                  dataKey={reg}
                  stroke={REGION_COLOURS[reg] || '#6b7280'}
                  strokeWidth={2}
                  dot={{ r: 3 }}
                  name={reg}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      {/* Row 2: Price Dispersion Scatter + Hardship Radar */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <ChartCard title="Price Dispersion — Median Annual Cost vs Spread">
          <ResponsiveContainer width="100%" height={360}>
            <ScatterChart margin={{ left: 10, right: 20, top: 10, bottom: 10 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                dataKey="median"
                name="Median Annual AUD"
                type="number"
                tick={{ fill: '#9ca3af', fontSize: 11 }}
              />
              <YAxis
                dataKey="spread"
                name="Price Spread AUD"
                type="number"
                tick={{ fill: '#9ca3af', fontSize: 11 }}
              />
              <ZAxis dataKey="count" range={[40, 400]} name="Plan Count" />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: 'none', borderRadius: 8, color: '#f3f4f6' }}
                cursor={{ strokeDasharray: '3 3' }}
              />
              <Legend wrapperStyle={{ fontSize: 11 }} />
              {Object.keys(PLAN_COLOURS).map((pt) => (
                <Scatter
                  key={pt}
                  name={pt.replace(/_/g, ' ')}
                  data={priceScatterData.filter((d) => d.plan_type === pt)}
                  fill={PLAN_COLOURS[pt]}
                />
              ))}
            </ScatterChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Retailer Hardship Performance Radar">
          <ResponsiveContainer width="100%" height={360}>
            <RadarChart outerRadius={120} data={radarData}>
              <PolarGrid stroke="#4b5563" />
              <PolarAngleAxis dataKey="retailer" tick={{ fill: '#d1d5db', fontSize: 10 }} />
              <PolarRadiusAxis tick={{ fill: '#6b7280', fontSize: 9 }} domain={[0, 10]} />
              <Radar name="Affordability" dataKey="affordability" stroke="#3b82f6" fill="#3b82f6" fillOpacity={0.25} />
              <Radar
                name="Payment Success"
                dataKey="payment_success"
                stroke="#10b981"
                fill="#10b981"
                fillOpacity={0.2}
              />
              <Radar
                name="Low Disconnection"
                dataKey="low_disconnection"
                stroke="#f59e0b"
                fill="#f59e0b"
                fillOpacity={0.15}
              />
              <Legend wrapperStyle={{ fontSize: 11 }} />
            </RadarChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      {/* Table 1: Retailer Overview */}
      <ChartCard title="Retailer Market Overview">
        <div className="overflow-x-auto">
          <table className="w-full text-sm text-left">
            <thead>
              <tr className="text-gray-400 border-b border-gray-700">
                <th className="py-2 px-3">Retailer</th>
                <th className="py-2 px-3">Region</th>
                <th className="py-2 px-3 text-right">Share %</th>
                <th className="py-2 px-3 text-right">Customers (k)</th>
                <th className="py-2 px-3 text-right">Churn %</th>
                <th className="py-2 px-3 text-right">Avg Price (c/kWh)</th>
                <th className="py-2 px-3 text-right">Complaints /10k</th>
              </tr>
            </thead>
            <tbody>
              {data.retailers.map((r, i) => (
                <tr key={i} className="border-b border-gray-800 hover:bg-gray-750">
                  <td className="py-2 px-3 font-medium" style={{ color: RETAILER_COLOURS[r.retailer_name] || '#d1d5db' }}>
                    {r.retailer_name}
                  </td>
                  <td className="py-2 px-3 text-gray-300">{r.region}</td>
                  <td className="py-2 px-3 text-right">{r.market_share_pct}%</td>
                  <td className="py-2 px-3 text-right">{r.customer_count_k}</td>
                  <td className="py-2 px-3 text-right">
                    <span className={`px-2 py-0.5 rounded text-xs ${churnBadge(r.churn_rate_pct)}`}>
                      {r.churn_rate_pct}%
                    </span>
                  </td>
                  <td className="py-2 px-3 text-right">{r.avg_price_c_kwh}</td>
                  <td className="py-2 px-3 text-right">{r.complaint_rate_per_10k}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </ChartCard>

      {/* Table 2: Hardship Program Performance */}
      <ChartCard title="Hardship Program Performance by Retailer">
        <div className="overflow-x-auto">
          <table className="w-full text-sm text-left">
            <thead>
              <tr className="text-gray-400 border-b border-gray-700">
                <th className="py-2 px-3">Retailer</th>
                <th className="py-2 px-3 text-right">Hardship Customers (k)</th>
                <th className="py-2 px-3 text-right">Avg Debt ($)</th>
                <th className="py-2 px-3 text-right">Payment Plan Success %</th>
                <th className="py-2 px-3 text-right">Disconnections /10k</th>
                <th className="py-2 px-3 text-right">Affordability Index</th>
              </tr>
            </thead>
            <tbody>
              {data.hardship.map((h, i) => (
                <tr key={i} className="border-b border-gray-800 hover:bg-gray-750">
                  <td className="py-2 px-3 font-medium" style={{ color: RETAILER_COLOURS[h.retailer_name] || '#d1d5db' }}>
                    {h.retailer_name}
                  </td>
                  <td className="py-2 px-3 text-right">{h.hardship_customers_k}</td>
                  <td className="py-2 px-3 text-right">${h.debt_level_avg_aud}</td>
                  <td className="py-2 px-3 text-right">{h.payment_plan_success_pct}%</td>
                  <td className="py-2 px-3 text-right">{h.disconnection_rate_per_10k}</td>
                  <td className="py-2 px-3 text-right">
                    <span className={`px-2 py-0.5 rounded text-xs ${affordabilityBadge(h.affordability_index)}`}>
                      {h.affordability_index}/10
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </ChartCard>

      {/* Footer timestamp */}
      <p className="text-xs text-gray-600 text-right">Data as at {data.timestamp}</p>
    </div>
  )
}
