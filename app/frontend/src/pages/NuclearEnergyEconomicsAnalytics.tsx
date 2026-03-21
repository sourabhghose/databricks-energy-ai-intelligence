import { useEffect, useState } from 'react'
import { Atom } from 'lucide-react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
  RadarChart, Radar, PolarGrid, PolarAngleAxis, PolarRadiusAxis,
} from 'recharts'
import {
  getNuclearEnergyEconomicsDashboard,
  NEEPDashboard,
  NEEPReactorTechRecord,
  NEEPSiteAssessmentRecord,
  NEEPScenarioRecord,
  NEEPStakeholderSentimentRecord,
} from '../api/client'

// ---- KPI Card ----
function KpiCard({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="bg-gray-800 rounded-xl p-5 flex flex-col gap-1 border border-gray-700">
      <span className="text-gray-400 text-sm">{label}</span>
      <span className="text-2xl font-bold text-white">{value}</span>
      {sub && <span className="text-gray-500 text-xs">{sub}</span>}
    </div>
  )
}

// ---- Colour helpers ----
const TECH_COLORS: Record<string, string> = {
  SMR: '#60a5fa',
  'Gen III+': '#34d399',
  'Gen IV': '#f59e0b',
  Fusion: '#a78bfa',
}
const SENTIMENT_COLORS = { support: '#34d399', neutral: '#fbbf24', oppose: '#f87171' }
const SCENARIO_COLORS = { capacity: '#60a5fa', cost: '#f87171', co2: '#34d399' }

export default function NuclearEnergyEconomicsAnalytics() {
  const [data, setData] = useState<NEEPDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getNuclearEnergyEconomicsDashboard()
      .then(d => { setData(d); setLoading(false) })
      .catch(e => { setError(String(e)); setLoading(false) })
  }, [])

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-900 flex items-center justify-center">
        <div className="text-white text-xl animate-pulse">Loading Nuclear Energy Economics data...</div>
      </div>
    )
  }
  if (error || !data) {
    return (
      <div className="min-h-screen bg-gray-900 flex items-center justify-center">
        <div className="text-red-400 text-xl">Error: {error || 'No data'}</div>
      </div>
    )
  }

  const { summary, reactor_technologies, site_assessments, cost_benchmarks, policy_timeline, stakeholder_sentiment, scenarios } = data

  // ---- Chart: LCOE by reactor type and country (grouped bar) ----
  const lcoeByCountry: Record<string, Record<string, number>> = {}
  cost_benchmarks.forEach(r => {
    if (!lcoeByCountry[r.reactor_type]) lcoeByCountry[r.reactor_type] = {}
    lcoeByCountry[r.reactor_type][r.country] = r.total_lcoe_dolpermwh
  })
  const countries = [...new Set(cost_benchmarks.map(r => r.country))]
  const lcoeChartData = Object.entries(lcoeByCountry).map(([rt, vals]) => ({
    reactor_type: rt.length > 12 ? rt.slice(0, 12) + '…' : rt,
    ...vals,
  }))

  // ---- Chart: Scenario comparison ----
  const scenarioChartData = scenarios
    .filter((s: NEEPScenarioRecord) => s.first_power_year > 0)
    .map((s: NEEPScenarioRecord) => ({
      name: s.scenario_name.length > 18 ? s.scenario_name.slice(0, 18) + '…' : s.scenario_name,
      'Capacity (GW)': s.total_capacity_gw,
      'Cost ($bn ÷10)': +(s.total_cost_bn / 10).toFixed(1),
      'CO₂ Abatement (Mt/yr)': s.co2_abatement_mt_per_year,
    }))

  // ---- Chart: Site suitability by state ----
  const siteByState: Record<string, { total: number; count: number }> = {}
  site_assessments.forEach((s: NEEPSiteAssessmentRecord) => {
    if (!siteByState[s.state]) siteByState[s.state] = { total: 0, count: 0 }
    siteByState[s.state].total += s.suitability_score
    siteByState[s.state].count += 1
  })
  const siteRadarData = Object.entries(siteByState).map(([state, v]) => ({
    state,
    'Avg Suitability': +(v.total / v.count).toFixed(1),
  }))

  // ---- Chart: Policy milestones by year ----
  const milestonesByYear: Record<string, number> = {}
  policy_timeline.forEach(m => {
    const yr = m.target_date.slice(0, 4)
    milestonesByYear[yr] = (milestonesByYear[yr] || 0) + 1
  })
  const policyChartData = Object.entries(milestonesByYear)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([year, count]) => ({ year, Milestones: count }))

  // ---- Chart: Stakeholder sentiment (stacked) ----
  const sentimentByGroup: Record<string, { support: number; neutral: number; oppose: number; count: number }> = {}
  stakeholder_sentiment.forEach((s: NEEPStakeholderSentimentRecord) => {
    if (!sentimentByGroup[s.stakeholder_group])
      sentimentByGroup[s.stakeholder_group] = { support: 0, neutral: 0, oppose: 0, count: 0 }
    sentimentByGroup[s.stakeholder_group].support += s.support_pct
    sentimentByGroup[s.stakeholder_group].neutral += s.neutral_pct
    sentimentByGroup[s.stakeholder_group].oppose += s.oppose_pct
    sentimentByGroup[s.stakeholder_group].count += 1
  })
  const sentimentChartData = Object.entries(sentimentByGroup).map(([grp, v]) => ({
    group: grp.length > 14 ? grp.slice(0, 14) + '…' : grp,
    Support: +(v.support / v.count).toFixed(1),
    Neutral: +(v.neutral / v.count).toFixed(1),
    Oppose: +(v.oppose / v.count).toFixed(1),
  }))

  // ---- Country bar colours ----
  const countryColors = ['#60a5fa', '#34d399', '#f59e0b', '#a78bfa']

  return (
    <div className="min-h-screen bg-gray-900 text-white p-6">
      {/* Header */}
      <div className="flex items-center gap-3 mb-8">
        <div className="bg-blue-600 p-3 rounded-xl">
          <Atom className="w-8 h-8 text-white" />
        </div>
        <div>
          <h1 className="text-3xl font-bold text-white">Nuclear Energy Economics Policy Analytics</h1>
          <p className="text-gray-400 text-sm mt-1">
            Reactor technologies, site assessments, cost benchmarks, policy timeline &amp; stakeholder sentiment
          </p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        <KpiCard
          label="Total Potential Capacity"
          value={`${summary.total_potential_capacity_gw} GW`}
          sub="Across assessed sites"
        />
        <KpiCard
          label="Viable Sites"
          value={String(summary.num_viable_sites)}
          sub="Suitability score >= 50"
        />
        <KpiCard
          label="Median LCOE"
          value={`$${summary.median_lcoe_dolpermwh}/MWh`}
          sub="Across cost benchmarks"
        />
        <KpiCard
          label="Policy Milestones"
          value={String(summary.num_policy_milestones)}
          sub={`Earliest power: ${summary.earliest_possible_power_year}`}
        />
      </div>

      {/* Row 1: LCOE comparison + Scenario comparison */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6 mb-6">
        {/* LCOE by Reactor Type and Country */}
        <div className="bg-gray-800 rounded-xl p-5 border border-gray-700">
          <h2 className="text-lg font-semibold mb-4 text-blue-300">LCOE Comparison by Reactor Type &amp; Country ($/MWh)</h2>
          <ResponsiveContainer width="100%" height={320}>
            <BarChart data={lcoeChartData} margin={{ top: 5, right: 10, left: 0, bottom: 60 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="reactor_type" tick={{ fill: '#9ca3af', fontSize: 11 }} angle={-35} textAnchor="end" interval={0} />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} label={{ value: '$/MWh', angle: -90, position: 'insideLeft', fill: '#9ca3af', fontSize: 11 }} />
              <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', color: '#fff' }} />
              <Legend wrapperStyle={{ color: '#9ca3af', paddingTop: 8 }} />
              {countries.map((c, i) => (
                <Bar key={c} dataKey={c} fill={countryColors[i % countryColors.length]} />
              ))}
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Scenario Comparison */}
        <div className="bg-gray-800 rounded-xl p-5 border border-gray-700">
          <h2 className="text-lg font-semibold mb-4 text-green-300">Deployment Scenario Comparison</h2>
          <ResponsiveContainer width="100%" height={320}>
            <BarChart data={scenarioChartData} margin={{ top: 5, right: 10, left: 0, bottom: 60 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="name" tick={{ fill: '#9ca3af', fontSize: 11 }} {...({ angle: -30 } as any)} textAnchor="end" interval={0} />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', color: '#fff' }} />
              <Legend wrapperStyle={{ color: '#9ca3af', paddingTop: 8 }} />
              <Bar dataKey="Capacity (GW)" fill={SCENARIO_COLORS.capacity} />
              <Bar dataKey="Cost ($bn ÷10)" fill={SCENARIO_COLORS.cost} />
              <Bar dataKey="CO₂ Abatement (Mt/yr)" fill={SCENARIO_COLORS.co2} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Row 2: Site suitability + Policy timeline */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6 mb-6">
        {/* Site Suitability by State (Radar) */}
        <div className="bg-gray-800 rounded-xl p-5 border border-gray-700">
          <h2 className="text-lg font-semibold mb-4 text-yellow-300">Average Site Suitability Score by State</h2>
          <ResponsiveContainer width="100%" height={300}>
            <RadarChart data={siteRadarData} margin={{ top: 10, right: 30, bottom: 10, left: 30 }}>
              <PolarGrid stroke="#374151" />
              <PolarAngleAxis dataKey="state" tick={{ fill: '#9ca3af', fontSize: 12 }} />
              <PolarRadiusAxis angle={30} domain={[0, 100]} tick={{ fill: '#6b7280', fontSize: 10 }} />
              <Radar name="Suitability" dataKey="Avg Suitability" stroke="#f59e0b" fill="#f59e0b" fillOpacity={0.35} />
              <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', color: '#fff' }} />
              <Legend wrapperStyle={{ color: '#9ca3af' }} />
            </RadarChart>
          </ResponsiveContainer>
        </div>

        {/* Policy Milestones by Year */}
        <div className="bg-gray-800 rounded-xl p-5 border border-gray-700">
          <h2 className="text-lg font-semibold mb-4 text-purple-300">Policy Milestones by Year</h2>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={policyChartData} margin={{ top: 5, right: 10, left: 0, bottom: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="year" tick={{ fill: '#9ca3af', fontSize: 12 }} />
              <YAxis allowDecimals={false} tick={{ fill: '#9ca3af', fontSize: 12 }} />
              <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', color: '#fff' }} />
              <Bar dataKey="Milestones" fill="#a78bfa" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Row 3: Stakeholder Sentiment (full width stacked bar) */}
      <div className="bg-gray-800 rounded-xl p-5 border border-gray-700 mb-6">
        <h2 className="text-lg font-semibold mb-4 text-pink-300">Stakeholder Sentiment by Group (avg across regions)</h2>
        <ResponsiveContainer width="100%" height={280}>
          <BarChart data={sentimentChartData} margin={{ top: 5, right: 20, left: 0, bottom: 40 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="group" tick={{ fill: '#9ca3af', fontSize: 12 }} angle={-20} textAnchor="end" interval={0} />
            <YAxis tick={{ fill: '#9ca3af', fontSize: 12 }} unit="%" />
            <Tooltip contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', color: '#fff' }} formatter={(v: number) => `${v}%`} />
            <Legend wrapperStyle={{ color: '#9ca3af', paddingTop: 8 }} />
            <Bar dataKey="Support" stackId="a" fill={SENTIMENT_COLORS.support} />
            <Bar dataKey="Neutral" stackId="a" fill={SENTIMENT_COLORS.neutral} />
            <Bar dataKey="Oppose" stackId="a" fill={SENTIMENT_COLORS.oppose} radius={[4, 4, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Row 4: Reactor Technology Table */}
      <div className="bg-gray-800 rounded-xl p-5 border border-gray-700">
        <h2 className="text-lg font-semibold mb-4 text-blue-300">Reactor Technologies — Key Specifications</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-gray-400 border-b border-gray-700">
                <th className="text-left py-2 pr-4">Developer</th>
                <th className="text-left py-2 pr-4">Technology</th>
                <th className="text-right py-2 pr-4">Capacity (MW)</th>
                <th className="text-right py-2 pr-4">LCOE ($/MWh)</th>
                <th className="text-right py-2 pr-4">Build (yrs)</th>
                <th className="text-right py-2 pr-4">Capacity Factor</th>
                <th className="text-left py-2 pr-4">Country</th>
                <th className="text-left py-2">Status</th>
              </tr>
            </thead>
            <tbody>
              {reactor_technologies.map((r: NEEPReactorTechRecord) => (
                <tr key={r.reactor_id} className="border-b border-gray-700 hover:bg-gray-750 transition-colors">
                  <td className="py-2 pr-4 text-white font-medium">{r.developer}</td>
                  <td className="py-2 pr-4">
                    <span
                      className="px-2 py-0.5 rounded text-xs font-semibold"
                      style={{ backgroundColor: TECH_COLORS[r.technology] + '33', color: TECH_COLORS[r.technology] }}
                    >
                      {r.technology}
                    </span>
                  </td>
                  <td className="py-2 pr-4 text-right text-gray-300">{r.capacity_mw.toLocaleString()}</td>
                  <td className="py-2 pr-4 text-right text-yellow-300">${r.lcoe_dolpermwh}</td>
                  <td className="py-2 pr-4 text-right text-gray-300">{r.construction_years}</td>
                  <td className="py-2 pr-4 text-right text-green-300">{r.capacity_factor_pct}%</td>
                  <td className="py-2 pr-4 text-gray-300">{r.country_of_origin}</td>
                  <td className="py-2 text-gray-400 text-xs">{r.regulatory_status}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
