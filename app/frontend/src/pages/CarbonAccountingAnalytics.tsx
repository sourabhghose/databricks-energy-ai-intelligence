import React, { useEffect, useState } from 'react'
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
} from 'recharts'
import {
  Leaf,
  RefreshCw,
  AlertTriangle,
  TrendingDown,
  Award,
  Building2,
  CheckCircle,
  XCircle,
} from 'lucide-react'
import {
  getCarbonAccountingDashboard,
  ECADashboard,
  ECAEmissionFactorRecord,
  ECACorporateRecord,
  ECAMethodologyRecord,
  ECARECQualityRecord,
  ECAGridImpactRecord,
} from '../api/client'

// ---------------------------------------------------------------------------
// Badge helpers
// ---------------------------------------------------------------------------

const APPROACH_BADGE: Record<string, string> = {
  LOCATION_BASED:  'bg-orange-600 text-white',
  MARKET_BASED:    'bg-green-600 text-white',
  DUAL_REPORTING:  'bg-blue-600 text-white',
}

const ADDITIONALITY_BADGE: Record<string, string> = {
  HIGH:   'bg-green-600 text-white',
  MEDIUM: 'bg-amber-600 text-white',
  LOW:    'bg-red-600 text-white',
}

const SECTOR_BADGE: Record<string, string> = {
  MINING:        'bg-orange-700 text-white',
  MANUFACTURING: 'bg-yellow-700 text-white',
  TECH:          'bg-blue-600 text-white',
  FINANCIAL:     'bg-indigo-600 text-white',
  RETAIL:        'bg-pink-600 text-white',
  PROPERTY:      'bg-teal-600 text-white',
  TRANSPORT:     'bg-sky-600 text-white',
}

const REGION_COLORS: Record<string, string> = {
  NSW: '#60a5fa',
  VIC: '#a78bfa',
  QLD: '#fb923c',
  SA:  '#34d399',
  TAS: '#f472b6',
}

function Badge({ label, colorClass }: { label: string; colorClass: string }) {
  return (
    <span className={`inline-block rounded px-2 py-0.5 text-xs font-semibold ${colorClass}`}>
      {label}
    </span>
  )
}

function BoolIcon({ value }: { value: boolean }) {
  return value
    ? <CheckCircle size={14} className="text-green-400 inline" />
    : <XCircle    size={14} className="text-red-500 inline" />
}

// ---------------------------------------------------------------------------
// KPI Card
// ---------------------------------------------------------------------------

function KPICard({
  label,
  value,
  sub,
  color,
  Icon,
}: {
  label: string
  value: string
  sub?: string
  color: string
  Icon: React.ElementType
}) {
  return (
    <div className="rounded-lg bg-gray-800 border border-gray-700 p-4 flex flex-col gap-1">
      <div className="flex items-center gap-2 text-gray-400 text-xs">
        <Icon size={14} className={color} />
        {label}
      </div>
      <p className={`text-2xl font-bold ${color}`}>{value}</p>
      {sub && <p className="text-gray-500 text-xs">{sub}</p>}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Section: Emission Factors Line Chart
// ---------------------------------------------------------------------------

function EmissionFactorsSection({ data }: { data: ECAEmissionFactorRecord[] }) {
  const [selectedRegion, setSelectedRegion] = useState<string>('NSW')
  const regions = Array.from(new Set(data.map(d => d.region))).sort()

  const chartData = Array.from({ length: 6 }, (_, i) => {
    const year = 2019 + i
    const row = data.find(d => d.region === selectedRegion && d.year === year)
    return {
      year: String(year),
      'Location-Based': row ? row.location_based_kg_per_mwh : null,
      'Market-Based Residual': row ? row.market_based_residual_mix_kg_per_mwh : null,
      'Combined Margin': row ? row.combined_margin_kg_per_mwh : null,
    }
  })

  return (
    <section className="rounded-xl bg-gray-800 border border-gray-700 p-6">
      <div className="flex flex-wrap items-center justify-between gap-3 mb-4">
        <h2 className="text-lg font-semibold text-white">Emission Factors by Region &amp; Year</h2>
        <div className="flex gap-2 flex-wrap">
          {regions.map(r => (
            <button
              key={r}
              onClick={() => setSelectedRegion(r)}
              className={`px-3 py-1 rounded border text-xs font-semibold transition-colors ${
                selectedRegion === r
                  ? 'text-white'
                  : 'text-gray-400 bg-transparent hover:bg-gray-700'
              }`}
              style={{
                borderColor: REGION_COLORS[r] ?? '#6b7280',
                backgroundColor: selectedRegion === r ? (REGION_COLORS[r] ?? '#6b7280') : undefined,
              }}
            >
              {r}
            </button>
          ))}
        </div>
      </div>
      <p className="text-gray-400 text-xs mb-4">
        kg CO2-e per MWh — location-based uses physical grid mix; market-based residual reflects
        the grid after corporate REC claims have been retired. Lower residual mix indicates higher
        REC uptake in the region.
      </p>
      <ResponsiveContainer width="100%" height={300}>
        <LineChart data={chartData} margin={{ top: 5, right: 20, left: 0, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis dataKey="year" stroke="#9ca3af" tick={{ fontSize: 11 }} />
          <YAxis stroke="#9ca3af" tick={{ fontSize: 11 }} tickFormatter={v => `${v}`} />
          <Tooltip
            contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
            labelStyle={{ color: '#f9fafb' }}
            formatter={(v: number) => [`${v.toFixed(3)} kg/MWh`]}
          />
          <Legend wrapperStyle={{ fontSize: 12 }} />
          <Line type="monotone" dataKey="Location-Based"       stroke="#f97316" strokeWidth={2} dot={{ r: 3 }} />
          <Line type="monotone" dataKey="Market-Based Residual" stroke="#34d399" strokeWidth={2} dot={{ r: 3 }} />
          <Line type="monotone" dataKey="Combined Margin"       stroke="#60a5fa" strokeWidth={2} strokeDasharray="5 5" dot={{ r: 3 }} />
        </LineChart>
      </ResponsiveContainer>

      <div className="mt-6 overflow-x-auto">
        <table className="w-full text-xs text-gray-300">
          <thead>
            <tr className="border-b border-gray-700 text-gray-400">
              <th className="text-left py-2 pr-3">Region</th>
              <th className="text-right py-2 pr-3">Year</th>
              <th className="text-right py-2 pr-3">Location (kg/MWh)</th>
              <th className="text-right py-2 pr-3">Market Residual</th>
              <th className="text-right py-2 pr-3">Op Margin</th>
              <th className="text-right py-2 pr-3">Build Margin</th>
              <th className="text-left py-2">Source</th>
            </tr>
          </thead>
          <tbody>
            {data
              .filter(d => d.region === selectedRegion)
              .map((d, i) => (
                <tr key={i} className="border-b border-gray-700/50 hover:bg-gray-700/20">
                  <td className="py-1.5 pr-3 font-medium" style={{ color: REGION_COLORS[d.region] }}>{d.region}</td>
                  <td className="py-1.5 pr-3 text-right">{d.year}</td>
                  <td className="py-1.5 pr-3 text-right text-orange-400">{d.location_based_kg_per_mwh.toFixed(3)}</td>
                  <td className="py-1.5 pr-3 text-right text-green-400">{d.market_based_residual_mix_kg_per_mwh.toFixed(3)}</td>
                  <td className="py-1.5 pr-3 text-right">{d.operating_margin_kg_per_mwh.toFixed(3)}</td>
                  <td className="py-1.5 pr-3 text-right">{d.build_margin_kg_per_mwh.toFixed(3)}</td>
                  <td className="py-1.5 text-gray-400 text-xs">{d.source.replace(/_/g, ' ')}</td>
                </tr>
              ))}
          </tbody>
        </table>
      </div>
    </section>
  )
}

// ---------------------------------------------------------------------------
// Section: Corporate Scope 2 Table
// ---------------------------------------------------------------------------

function CorporateSection({ data }: { data: ECACorporateRecord[] }) {
  const [sortKey, setSortKey] = useState<keyof ECACorporateRecord>('scope2_location_based_kt_co2')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')

  const sorted = [...data].sort((a, b) => {
    const av = a[sortKey] as number | null
    const bv = b[sortKey] as number | null
    const af = av ?? 0
    const bf = bv ?? 0
    return sortDir === 'desc' ? bf - af : af - bf
  })

  const toggleSort = (key: keyof ECACorporateRecord) => {
    if (sortKey === key) setSortDir(d => (d === 'desc' ? 'asc' : 'desc'))
    else { setSortKey(key); setSortDir('desc') }
  }

  const Th = ({ label, col }: { label: string; col: keyof ECACorporateRecord }) => (
    <th
      className="text-right py-2 pr-3 cursor-pointer hover:text-white select-none"
      onClick={() => toggleSort(col)}
    >
      {label} {sortKey === col ? (sortDir === 'desc' ? '▼' : '▲') : ''}
    </th>
  )

  return (
    <section className="rounded-xl bg-gray-800 border border-gray-700 p-6">
      <h2 className="text-lg font-semibold text-white mb-2">Corporate Scope 2 Emissions &amp; REC Coverage</h2>
      <p className="text-gray-400 text-xs mb-4">
        Large Australian corporations reporting under GHG Protocol dual reporting. Market-based
        figures reflect GreenPower, LGC retirements and PPAs. Click column headers to sort.
      </p>
      <div className="overflow-x-auto">
        <table className="w-full text-xs text-gray-300">
          <thead>
            <tr className="border-b border-gray-700 text-gray-400">
              <th className="text-left py-2 pr-3">Company</th>
              <th className="text-left py-2 pr-3">Sector</th>
              <th className="text-left py-2 pr-3">State</th>
              <Th label="Consumption (GWh)" col="electricity_consumption_gwh" />
              <Th label="Loc-Based (kt)" col="scope2_location_based_kt_co2" />
              <Th label="Mkt-Based (kt)" col="scope2_market_based_kt_co2" />
              <Th label="Reduction %" col="scope2_reduction_pct" />
              <Th label="REC Cov %" col="rec_coverage_pct" />
              <Th label="Renew %" col="renewable_energy_pct" />
              <th className="text-right py-2 pr-3">Net-Zero</th>
              <Th label="Savings ($M)" col="annual_cost_savings_m" />
            </tr>
          </thead>
          <tbody>
            {sorted.map((r, i) => (
              <tr key={i} className="border-b border-gray-700/50 hover:bg-gray-700/20">
                <td className="py-1.5 pr-3 font-medium text-white whitespace-nowrap">{r.company}</td>
                <td className="py-1.5 pr-3">
                  <Badge label={r.sector} colorClass={SECTOR_BADGE[r.sector] ?? 'bg-gray-600 text-white'} />
                </td>
                <td className="py-1.5 pr-3 text-gray-400">{r.state}</td>
                <td className="py-1.5 pr-3 text-right">{r.electricity_consumption_gwh.toLocaleString()}</td>
                <td className="py-1.5 pr-3 text-right text-orange-400">{r.scope2_location_based_kt_co2.toLocaleString()}</td>
                <td className="py-1.5 pr-3 text-right text-green-400">{r.scope2_market_based_kt_co2.toLocaleString()}</td>
                <td className="py-1.5 pr-3 text-right">
                  <span className={r.scope2_reduction_pct >= 60 ? 'text-green-400' : r.scope2_reduction_pct >= 40 ? 'text-amber-400' : 'text-red-400'}>
                    {r.scope2_reduction_pct.toFixed(1)}%
                  </span>
                </td>
                <td className="py-1.5 pr-3 text-right">
                  <span className={r.rec_coverage_pct >= 80 ? 'text-green-400' : r.rec_coverage_pct >= 50 ? 'text-amber-400' : 'text-red-400'}>
                    {r.rec_coverage_pct.toFixed(1)}%
                  </span>
                </td>
                <td className="py-1.5 pr-3 text-right text-blue-400">{r.renewable_energy_pct.toFixed(1)}%</td>
                <td className="py-1.5 pr-3 text-right text-purple-400">
                  {r.net_zero_target_year ?? <span className="text-gray-500">—</span>}
                </td>
                <td className="py-1.5 pr-3 text-right text-emerald-400">${r.annual_cost_savings_m.toFixed(1)}M</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  )
}

// ---------------------------------------------------------------------------
// Section: Methodology Comparison
// ---------------------------------------------------------------------------

function MethodologySection({ data }: { data: ECAMethodologyRecord[] }) {
  return (
    <section className="rounded-xl bg-gray-800 border border-gray-700 p-6">
      <h2 className="text-lg font-semibold text-white mb-2">Carbon Accounting Methodology Comparison</h2>
      <p className="text-gray-400 text-xs mb-4">
        Scope 2 reporting standards compared by approach, temporal &amp; spatial matching requirements,
        additionality criteria, and corporate adoption rates in Australia.
      </p>
      <div className="overflow-x-auto">
        <table className="w-full text-xs text-gray-300">
          <thead>
            <tr className="border-b border-gray-700 text-gray-400">
              <th className="text-left py-2 pr-3">Standard</th>
              <th className="text-left py-2 pr-3">Scope 2 Approach</th>
              <th className="text-left py-2 pr-3 max-w-xs">REC Quality Criteria</th>
              <th className="text-center py-2 pr-3">Additionality</th>
              <th className="text-center py-2 pr-3">Temporal Match</th>
              <th className="text-center py-2 pr-3">Spatial Match</th>
              <th className="text-right py-2">Adoption %</th>
            </tr>
          </thead>
          <tbody>
            {data.map((m, i) => (
              <tr key={i} className="border-b border-gray-700/50 hover:bg-gray-700/20 align-top">
                <td className="py-2 pr-3 font-semibold text-white whitespace-nowrap">{m.standard.replace(/_/g, ' ')}</td>
                <td className="py-2 pr-3">
                  <Badge
                    label={m.scope2_approach.replace(/_/g, ' ')}
                    colorClass={APPROACH_BADGE[m.scope2_approach] ?? 'bg-gray-600 text-white'}
                  />
                </td>
                <td className="py-2 pr-3 text-gray-400 text-xs max-w-xs leading-relaxed">{m.rec_quality_criteria}</td>
                <td className="py-2 pr-3 text-center">
                  <BoolIcon value={m.additionality_required} />
                </td>
                <td className="py-2 pr-3 text-center">
                  <span className={
                    m.temporal_matching === 'HOURLY' ? 'text-green-400' :
                    m.temporal_matching === 'MONTHLY' ? 'text-amber-400' :
                    'text-gray-400'
                  }>
                    {m.temporal_matching}
                  </span>
                </td>
                <td className="py-2 pr-3 text-center">
                  <span className={
                    m.spatial_matching === 'LOCAL' ? 'text-green-400' :
                    m.spatial_matching === 'REGIONAL' ? 'text-amber-400' :
                    'text-gray-400'
                  }>
                    {m.spatial_matching}
                  </span>
                </td>
                <td className="py-2 text-right">
                  <span className={m.adoption_pct >= 70 ? 'text-green-400' : m.adoption_pct >= 40 ? 'text-amber-400' : 'text-red-400'}>
                    {m.adoption_pct.toFixed(1)}%
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  )
}

// ---------------------------------------------------------------------------
// Section: REC Quality
// ---------------------------------------------------------------------------

function RECQualitySection({ data }: { data: ECARECQualityRecord[] }) {
  const sorted = [...data].sort((a, b) => b.credibility_score - a.credibility_score)

  return (
    <section className="rounded-xl bg-gray-800 border border-gray-700 p-6">
      <h2 className="text-lg font-semibold text-white mb-2">Renewable Energy Certificate Quality Assessment</h2>
      <p className="text-gray-400 text-xs mb-4">
        Credibility scoring for REC types used in Scope 2 market-based accounting. Higher credibility
        scores indicate stronger chain-of-custody, additionality, and geographic/temporal matching.
      </p>
      <div className="overflow-x-auto">
        <table className="w-full text-xs text-gray-300">
          <thead>
            <tr className="border-b border-gray-700 text-gray-400">
              <th className="text-left py-2 pr-3">Certificate Type</th>
              <th className="text-center py-2 pr-3">Additionality</th>
              <th className="text-center py-2 pr-3">Vintage Match</th>
              <th className="text-center py-2 pr-3">Geo Match</th>
              <th className="text-center py-2 pr-3">Chain of Custody</th>
              <th className="text-right py-2 pr-3">Market Price ($/cert)</th>
              <th className="text-right py-2">Credibility Score</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((r, i) => (
              <tr key={i} className="border-b border-gray-700/50 hover:bg-gray-700/20">
                <td className="py-1.5 pr-3 font-semibold text-white">{r.rec_type.replace(/_/g, ' ')}</td>
                <td className="py-1.5 pr-3 text-center">
                  <Badge
                    label={r.additionality_level}
                    colorClass={ADDITIONALITY_BADGE[r.additionality_level] ?? 'bg-gray-600 text-white'}
                  />
                </td>
                <td className="py-1.5 pr-3 text-center"><BoolIcon value={r.vintage_restrictions} /></td>
                <td className="py-1.5 pr-3 text-center"><BoolIcon value={r.geographic_match_required} /></td>
                <td className="py-1.5 pr-3 text-center"><BoolIcon value={r.chain_of_custody_required} /></td>
                <td className="py-1.5 pr-3 text-right text-yellow-400">${r.market_price.toFixed(2)}</td>
                <td className="py-1.5 text-right">
                  <span className={
                    r.credibility_score >= 9 ? 'text-green-400 font-bold' :
                    r.credibility_score >= 7 ? 'text-amber-400' :
                    'text-red-400'
                  }>
                    {r.credibility_score.toFixed(1)} / 10
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  )
}

// ---------------------------------------------------------------------------
// Section: Grid Impact Bar Chart
// ---------------------------------------------------------------------------

function GridImpactSection({ data }: { data: ECAGridImpactRecord[] }) {
  const [selectedRegion, setSelectedRegion] = useState<string>('NSW')
  const regions = Array.from(new Set(data.map(d => d.region))).sort()

  const chartData = data
    .filter(d => d.region === selectedRegion)
    .map(d => ({
      quarter: d.quarter,
      'Residual Mix %': d.residual_mix_after_claims_pct,
      'Over-Claiming Risk %': d.over_claiming_risk_pct,
      'Hourly Match Benefit %': d.hour_matching_benefit_pct,
    }))

  const addData = data
    .filter(d => d.region === selectedRegion)
    .map(d => ({
      quarter: d.quarter,
      'Corporate REC Demand (TWh)': d.corporate_rec_demand_twh,
      'Additionality Value ($M)': d.additionality_value_m,
    }))

  return (
    <section className="rounded-xl bg-gray-800 border border-gray-700 p-6">
      <div className="flex flex-wrap items-center justify-between gap-3 mb-4">
        <h2 className="text-lg font-semibold text-white">Grid Impact of Corporate REC Demand</h2>
        <div className="flex gap-2 flex-wrap">
          {regions.map(r => (
            <button
              key={r}
              onClick={() => setSelectedRegion(r)}
              className={`px-3 py-1 rounded border text-xs font-semibold transition-colors ${
                selectedRegion === r
                  ? 'text-white border-transparent'
                  : 'text-gray-400 border-gray-600 hover:bg-gray-700'
              }`}
              style={{
                borderColor: REGION_COLORS[r] ?? '#6b7280',
                backgroundColor: selectedRegion === r ? (REGION_COLORS[r] ?? '#6b7280') : undefined,
              }}
            >
              {r}
            </button>
          ))}
        </div>
      </div>
      <p className="text-gray-400 text-xs mb-4">
        Quarterly analysis of the residual mix intensity after corporate REC retirements, over-claiming
        risk from double-counting, and additional system value from 24/7 hourly matching approaches.
      </p>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div>
          <p className="text-gray-400 text-xs mb-2 font-medium">Emissions Metrics (%)</p>
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={chartData} margin={{ top: 5, right: 10, left: 0, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="quarter" stroke="#9ca3af" tick={{ fontSize: 11 }} />
              <YAxis stroke="#9ca3af" tick={{ fontSize: 11 }} tickFormatter={v => `${v}%`} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                labelStyle={{ color: '#f9fafb' }}
                formatter={(v: number) => [`${v.toFixed(1)}%`]}
              />
              <Legend wrapperStyle={{ fontSize: 11 }} />
              <Bar dataKey="Residual Mix %" fill="#f97316" radius={[3, 3, 0, 0]} />
              <Bar dataKey="Over-Claiming Risk %" fill="#ef4444" radius={[3, 3, 0, 0]} />
              <Bar dataKey="Hourly Match Benefit %" fill="#34d399" radius={[3, 3, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div>
          <p className="text-gray-400 text-xs mb-2 font-medium">Additionality &amp; Demand</p>
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={addData} margin={{ top: 5, right: 10, left: 0, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="quarter" stroke="#9ca3af" tick={{ fontSize: 11 }} />
              <YAxis stroke="#9ca3af" tick={{ fontSize: 11 }} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 6 }}
                labelStyle={{ color: '#f9fafb' }}
              />
              <Legend wrapperStyle={{ fontSize: 11 }} />
              <Bar dataKey="Corporate REC Demand (TWh)" fill="#60a5fa" radius={[3, 3, 0, 0]} />
              <Bar dataKey="Additionality Value ($M)" fill="#a78bfa" radius={[3, 3, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="mt-5 overflow-x-auto">
        <table className="w-full text-xs text-gray-300">
          <thead>
            <tr className="border-b border-gray-700 text-gray-400">
              <th className="text-left py-2 pr-3">Region</th>
              <th className="text-left py-2 pr-3">Quarter</th>
              <th className="text-right py-2 pr-3">REC Demand (TWh)</th>
              <th className="text-right py-2 pr-3">Residual Mix %</th>
              <th className="text-right py-2 pr-3">Over-Claiming %</th>
              <th className="text-right py-2 pr-3">Hourly Benefit %</th>
              <th className="text-right py-2">Additionality ($M)</th>
            </tr>
          </thead>
          <tbody>
            {data
              .filter(d => d.region === selectedRegion)
              .map((d, i) => (
                <tr key={i} className="border-b border-gray-700/50 hover:bg-gray-700/20">
                  <td className="py-1.5 pr-3 font-medium" style={{ color: REGION_COLORS[d.region] }}>{d.region}</td>
                  <td className="py-1.5 pr-3">{d.quarter}</td>
                  <td className="py-1.5 pr-3 text-right text-blue-400">{d.corporate_rec_demand_twh.toFixed(2)}</td>
                  <td className="py-1.5 pr-3 text-right text-orange-400">{d.residual_mix_after_claims_pct.toFixed(1)}%</td>
                  <td className="py-1.5 pr-3 text-right text-red-400">{d.over_claiming_risk_pct.toFixed(1)}%</td>
                  <td className="py-1.5 pr-3 text-right text-green-400">{d.hour_matching_benefit_pct.toFixed(1)}%</td>
                  <td className="py-1.5 text-right text-purple-400">${d.additionality_value_m.toFixed(1)}M</td>
                </tr>
              ))}
          </tbody>
        </table>
      </div>
    </section>
  )
}

// ---------------------------------------------------------------------------
// Main Page Component
// ---------------------------------------------------------------------------

export default function CarbonAccountingAnalytics() {
  const [data, setData] = useState<ECADashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const fetchData = () => {
    setLoading(true)
    setError(null)
    getCarbonAccountingDashboard()
      .then(d => { setData(d); setLoading(false) })
      .catch(e => { setError(String(e)); setLoading(false) })
  }

  useEffect(() => { fetchData() }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-gray-400">
        <RefreshCw size={20} className="animate-spin mr-2" />
        Loading carbon accounting data...
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="flex items-center justify-center h-64 text-red-400">
        <AlertTriangle size={20} className="mr-2" />
        {error ?? 'Failed to load data.'}
      </div>
    )
  }

  const summary = data.summary as Record<string, number>

  return (
    <div className="space-y-6 p-6 text-white">
      {/* Header */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div>
          <h1 className="text-2xl font-bold text-white flex items-center gap-2">
            <Leaf size={24} className="text-green-400" />
            Electricity Market Carbon Accounting &amp; Scope 2 Analytics
          </h1>
          <p className="text-gray-400 text-sm mt-1">
            Market-based vs location-based Scope 2 emissions, carbon accounting methodologies,
            REC quality assessment, and corporate decarbonisation pathways — Australia 2019–2024
          </p>
        </div>
        <button
          onClick={fetchData}
          className="flex items-center gap-2 px-4 py-2 rounded-lg bg-gray-700 hover:bg-gray-600 text-gray-300 text-sm transition-colors"
        >
          <RefreshCw size={14} />
          Refresh
        </button>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-3">
        <KPICard
          label="Avg Location-Based 2024"
          value={`${summary.avg_location_based_kg_mwh_2024} kg/MWh`}
          sub="NEM grid average"
          color="text-orange-400"
          Icon={TrendingDown}
        />
        <KPICard
          label="Avg Market Residual 2024"
          value={`${summary.avg_market_based_residual_kg_mwh_2024} kg/MWh`}
          sub="After REC retirements"
          color="text-green-400"
          Icon={Leaf}
        />
        <KPICard
          label="Corporate REC Coverage"
          value={`${summary.corporate_rec_coverage_avg_pct}%`}
          sub="Large corp average"
          color="text-blue-400"
          Icon={Award}
        />
        <KPICard
          label="Over-Claiming Risk"
          value={`${summary.over_claiming_risk_pct}%`}
          sub="Double-counting risk"
          color="text-red-400"
          Icon={AlertTriangle}
        />
        <KPICard
          label="Additionality Value"
          value={`$${summary.additionality_value_m}M`}
          sub="New investment unlocked"
          color="text-purple-400"
          Icon={Building2}
        />
        <KPICard
          label="Net-Zero Committed"
          value={`${summary.net_zero_target_count} cos`}
          sub="Of 15 tracked"
          color="text-emerald-400"
          Icon={CheckCircle}
        />
      </div>

      {/* Sections */}
      <EmissionFactorsSection data={data.emission_factors} />
      <CorporateSection data={data.corporate} />
      <MethodologySection data={data.methodologies} />
      <RECQualitySection data={data.rec_quality} />
      <GridImpactSection data={data.grid_impact} />
    </div>
  )
}
