import { useEffect, useState } from 'react'
import { PieChart } from 'lucide-react'
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
  Cell,
} from 'recharts'
import {
  getElectricityMarketCompetitionConcentrationDashboard,
  EMCCDashboard,
} from '../api/client'

const REGION_COLORS: Record<string, string> = {
  NSW1: '#6366f1',
  QLD1: '#f59e0b',
  VIC1: '#22c55e',
  SA1: '#ef4444',
  TAS1: '#22d3ee',
}

const SEGMENT_COLORS: Record<string, string> = {
  Generation: '#6366f1',
  Retail: '#f59e0b',
  'Commercial Contracts': '#22c55e',
  FCAS: '#22d3ee',
}

const EVENT_TYPE_COLORS: Record<string, string> = {
  'Pivotal Supplier': '#ef4444',
  'Economic Withholding': '#f97316',
  'Physical Withholding': '#dc2626',
  'Oligopolistic Coordination': '#9333ea',
  Rebidding: '#f59e0b',
  'Near-VoLL': '#22c55e',
}

const HHI_BAND = (score: number): string => {
  if (score >= 2500) return 'bg-red-900 text-red-200'
  if (score >= 1500) return 'bg-amber-900 text-amber-200'
  return 'bg-green-900 text-green-200'
}

export default function ElectricityMarketCompetitionConcentrationAnalytics() {
  const [data, setData] = useState<EMCCDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [selectedSegment, setSelectedSegment] = useState<string>('Generation')

  useEffect(() => {
    getElectricityMarketCompetitionConcentrationDashboard()
      .then(setData)
      .catch((e) => setError(e.message ?? 'Failed to load data'))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 bg-gray-900 text-gray-300">
        <span className="text-lg">Loading Electricity Market Competition and Concentration Analytics...</span>
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

  const {
    summary,
    hhi_records,
    participants,
    market_power_events,
    retail_competition,
    competition_trends,
  } = data

  const segments = ['Generation', 'Retail', 'Commercial Contracts', 'FCAS']
  const regions = ['NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1']

  // KPI cards
  const kpis = [
    {
      label: 'Avg Generation HHI',
      value: summary.avg_generation_hhi.toLocaleString(),
      sub: summary.avg_generation_hhi >= 2500 ? 'Highly Concentrated' : summary.avg_generation_hhi >= 1500 ? 'Moderately Concentrated' : 'Competitive',
      color: summary.avg_generation_hhi >= 2500 ? 'text-red-400' : 'text-amber-400',
    },
    {
      label: 'Avg Retail HHI',
      value: summary.avg_retail_hhi.toLocaleString(),
      sub: `Most concentrated: ${summary.most_concentrated_region}`,
      color: 'text-purple-400',
    },
    {
      label: 'Market Power Events YTD',
      value: summary.market_power_events_ytd.toString(),
      sub: `Largest participant: ${summary.largest_participant_share_pct}% share`,
      color: 'text-red-400',
    },
    {
      label: 'Market Contestability',
      value: summary.market_contestability_trend,
      sub: 'Structural competition trend',
      color: summary.market_contestability_trend === 'Declining' ? 'text-red-400' : 'text-green-400',
    },
  ]

  // ---- Chart 1: HHI Trend by Region (filtered by segment) ----
  const filteredHHI = hhi_records.filter((r) => r.market_segment === selectedSegment)
  const quarters = [...new Set(filteredHHI.map((r) => r.year_quarter))].sort()
  const hhiTrendData = quarters.map((yq) => {
    const row: Record<string, string | number> = { quarter: yq }
    for (const region of regions) {
      const rec = filteredHHI.find((r) => r.year_quarter === yq && r.region === region)
      if (rec) row[region] = rec.hhi_score
    }
    return row
  })

  // ---- Chart 2: Participant Market Share (horizontal bar, top 10 by generation MW) ----
  const topParticipants = [...participants]
    .filter((p) => p.total_generation_mw > 0)
    .sort((a, b) => b.total_generation_mw - a.total_generation_mw)
    .slice(0, 10)
    .map((p) => ({
      name: p.participant_name.length > 22 ? p.participant_name.slice(0, 22) + '…' : p.participant_name,
      total_generation_mw: p.total_generation_mw,
      market_share_generation_pct: p.market_share_generation_pct,
      entity_type: p.entity_type,
    }))

  // ---- Chart 3: Retail Competition grouped bar ----
  const retailChartData = retail_competition
    .sort((a, b) => b.market_share_residential_pct - a.market_share_residential_pct)
    .map((r) => ({
      name: r.retailer.length > 18 ? r.retailer.slice(0, 18) + '…' : r.retailer,
      market_share_pct: r.market_share_residential_pct,
      churn_rate_pct: r.churn_rate_pct,
      state: r.state,
    }))

  // ---- Chart 4: Market Power Events by region and type ----
  const mpByRegion: Record<string, Record<string, number>> = {}
  for (const ev of market_power_events) {
    if (!mpByRegion[ev.region]) mpByRegion[ev.region] = {}
    mpByRegion[ev.region][ev.event_type] = (mpByRegion[ev.region][ev.event_type] ?? 0) + 1
  }
  const mpChartData = Object.keys(mpByRegion).map((region) => {
    const row: Record<string, string | number> = { region }
    for (const et of Object.keys(EVENT_TYPE_COLORS)) {
      row[et] = mpByRegion[region][et] ?? 0
    }
    row['referrals'] = market_power_events.filter(
      (e) => e.region === region && e.regulatory_referral,
    ).length
    return row
  })

  // ---- Chart 5: HHI Heatmap (table) — generation vs retail by region × year ----
  const trendYears = [...new Set(competition_trends.map((t) => t.year))].sort()
  const trendRegions = [...new Set(competition_trends.map((t) => t.region))]

  // ---- Chart 6: Competition Trend LineChart ----
  const contestabilityChartData = trendYears.map((yr) => {
    const row: Record<string, string | number> = { year: yr.toString() }
    for (const region of trendRegions) {
      const rec = competition_trends.find((t) => t.year === yr && t.region === region)
      if (rec) row[region] = rec.market_contestability_score
    }
    return row
  })

  return (
    <div className="p-6 bg-gray-900 min-h-screen text-gray-100">
      {/* Header */}
      <div className="flex items-center gap-3 mb-6">
        <PieChart className="w-8 h-8 text-purple-400" />
        <div>
          <h1 className="text-2xl font-bold text-white">
            Electricity Market Competition and Concentration Analytics
          </h1>
          <p className="text-gray-400 text-sm">
            HHI by region, generator ownership concentration, retail market competition, market power
            episodes, M&A activity and ACCC monitoring
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

      {/* Segment Filter */}
      <div className="flex flex-wrap gap-4 mb-6">
        <div className="flex items-center gap-2">
          <label className="text-gray-400 text-sm">Market Segment (HHI Trend):</label>
          <select
            className="bg-gray-800 text-gray-200 rounded-lg px-3 py-1 border border-gray-600 text-sm"
            value={selectedSegment}
            onChange={(e) => setSelectedSegment(e.target.value)}
          >
            {segments.map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
        </div>
      </div>

      {/* Chart 1: HHI Trend by Region */}
      <div className="bg-gray-800 rounded-xl p-5 border border-gray-700 mb-6">
        <h2 className="text-lg font-semibold text-white mb-1">HHI Trend by Region — {selectedSegment}</h2>
        <p className="text-gray-400 text-xs mb-4">
          Herfindahl-Hirschman Index over time. Above 2,500 = highly concentrated; 1,500–2,500 = moderate;
          below 1,500 = competitive.
        </p>
        <div className="flex flex-wrap gap-3 mb-3">
          {regions.map((r) => (
            <span key={r} className="flex items-center gap-1 text-xs text-gray-300">
              <span
                className="inline-block w-3 h-3 rounded-sm"
                style={{ backgroundColor: REGION_COLORS[r] ?? '#9ca3af' }}
              />
              {r}
            </span>
          ))}
        </div>
        <ResponsiveContainer width="100%" height={280}>
          <LineChart data={hhiTrendData} margin={{ right: 20 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="quarter" stroke="#9ca3af" tick={{ fontSize: 11 }} />
            <YAxis stroke="#9ca3af" tick={{ fontSize: 11 }} domain={[0, 'auto']} />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }}
              formatter={(v: number) => [v.toLocaleString(), 'HHI']}
            />
            <Legend />
            {regions.map((region) => (
              <Line
                key={region}
                type="monotone"
                dataKey={region}
                stroke={REGION_COLORS[region] ?? '#9ca3af'}
                strokeWidth={2}
                dot={false}
                connectNulls
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 2: Participant Market Share */}
      <div className="bg-gray-800 rounded-xl p-5 border border-gray-700 mb-6">
        <h2 className="text-lg font-semibold text-white mb-1">Generation Capacity — Top 10 Participants</h2>
        <p className="text-gray-400 text-xs mb-4">
          Total installed generation capacity (MW) sorted descending. AGL, Origin, EnergyAustralia dominate
          the big-3 with ~47% combined share.
        </p>
        <ResponsiveContainer width="100%" height={340}>
          <BarChart layout="vertical" data={topParticipants} margin={{ left: 160 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" horizontal={false} />
            <XAxis type="number" stroke="#9ca3af" tick={{ fontSize: 11 }} tickFormatter={(v) => `${v.toLocaleString()} MW`} />
            <YAxis type="category" dataKey="name" stroke="#9ca3af" tick={{ fontSize: 10 }} width={160} />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }}
              formatter={(v: number, _name: string, props: { payload?: { market_share_generation_pct?: number; entity_type?: string } }) => [
                `${v.toLocaleString()} MW (${props.payload?.market_share_generation_pct ?? 0}% share) | ${props.payload?.entity_type ?? ''}`,
                'Capacity',
              ]}
            />
            <Bar dataKey="total_generation_mw" name="Generation Capacity (MW)">
              {topParticipants.map((_entry, idx) => (
                <Cell key={idx} fill={['#6366f1', '#f59e0b', '#22c55e', '#ef4444', '#22d3ee', '#f97316', '#a855f7', '#ec4899', '#14b8a6', '#84cc16'][idx % 10]} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 3: Retail Competition */}
      <div className="bg-gray-800 rounded-xl p-5 border border-gray-700 mb-6">
        <h2 className="text-lg font-semibold text-white mb-1">Retail Competition — Market Share vs Churn Rate</h2>
        <p className="text-gray-400 text-xs mb-4">
          Residential market share (%) alongside customer churn rate (%) by retailer. Higher churn
          typically signals stronger competition and customer switching activity.
        </p>
        <div className="flex flex-wrap gap-3 mb-3">
          <span className="flex items-center gap-1 text-xs text-gray-300">
            <span className="inline-block w-3 h-3 rounded-sm bg-purple-500" /> Market Share %
          </span>
          <span className="flex items-center gap-1 text-xs text-gray-300">
            <span className="inline-block w-3 h-3 rounded-sm bg-amber-500" /> Churn Rate %
          </span>
        </div>
        <ResponsiveContainer width="100%" height={320}>
          <BarChart data={retailChartData} margin={{ bottom: 80 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="name" stroke="#9ca3af" tick={{ fontSize: 9, angle: -40, textAnchor: 'end' } as any} interval={0} />
            <YAxis stroke="#9ca3af" tick={{ fontSize: 11 }} />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }}
            />
            <Legend verticalAlign="top" />
            <Bar dataKey="market_share_pct" name="Market Share (%)" fill="#a855f7" />
            <Bar dataKey="churn_rate_pct" name="Churn Rate (%)" fill="#f59e0b" />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 4: Market Power Events */}
      <div className="bg-gray-800 rounded-xl p-5 border border-gray-700 mb-6">
        <h2 className="text-lg font-semibold text-white mb-1">Market Power Events by Region and Type</h2>
        <p className="text-gray-400 text-xs mb-4">
          Count of market power episodes by region, broken down by event type. Regulatory referrals
          indicated as overlay.
        </p>
        <div className="flex flex-wrap gap-2 mb-3">
          {Object.entries(EVENT_TYPE_COLORS).map(([et, col]) => (
            <span key={et} className="flex items-center gap-1 text-xs text-gray-300">
              <span className="inline-block w-3 h-3 rounded-sm" style={{ backgroundColor: col }} />
              {et}
            </span>
          ))}
        </div>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={mpChartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="region" stroke="#9ca3af" tick={{ fontSize: 12 }} />
            <YAxis stroke="#9ca3af" tick={{ fontSize: 11 }} />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }}
            />
            <Legend />
            {Object.keys(EVENT_TYPE_COLORS).map((et) => (
              <Bar key={et} dataKey={et} stackId="events" fill={EVENT_TYPE_COLORS[et]} />
            ))}
            <Bar dataKey="referrals" name="Regulatory Referrals" fill="#ffffff" opacity={0.5} />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 5: HHI Heatmap Table */}
      <div className="bg-gray-800 rounded-xl p-5 border border-gray-700 mb-6">
        <h2 className="text-lg font-semibold text-white mb-1">HHI Heatmap — Generation vs Retail by Region and Year</h2>
        <p className="text-gray-400 text-xs mb-4">
          Colour coding: red = highly concentrated (above 2,500), amber = moderately concentrated
          (1,500–2,500), green = competitive (below 1,500). Format: Generation / Retail.
        </p>
        <div className="overflow-x-auto">
          <table className="w-full text-xs text-gray-300">
            <thead>
              <tr className="border-b border-gray-700">
                <th className="text-left p-2 text-gray-400">Region</th>
                {trendYears.map((yr) => (
                  <th key={yr} className="text-center p-2 text-gray-400">{yr}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {trendRegions.map((region) => (
                <tr key={region} className="border-b border-gray-700">
                  <td className="p-2 font-medium text-white">{region}</td>
                  {trendYears.map((yr) => {
                    const rec = competition_trends.find((t) => t.year === yr && t.region === region)
                    if (!rec) return <td key={yr} className="p-2 text-center text-gray-600">—</td>
                    return (
                      <td key={yr} className="p-2 text-center">
                        <div className={`rounded px-1 py-0.5 mb-0.5 ${HHI_BAND(rec.generation_hhi)}`}>
                          {rec.generation_hhi.toLocaleString()}
                        </div>
                        <div className={`rounded px-1 py-0.5 ${HHI_BAND(rec.retail_hhi)}`}>
                          {rec.retail_hhi.toLocaleString()}
                        </div>
                      </td>
                    )
                  })}
                </tr>
              ))}
            </tbody>
          </table>
          <div className="flex gap-4 mt-3">
            <span className="flex items-center gap-1 text-xs"><span className="inline-block w-3 h-3 rounded bg-red-900" /> Highly Concentrated (above 2,500)</span>
            <span className="flex items-center gap-1 text-xs"><span className="inline-block w-3 h-3 rounded bg-amber-900" /> Moderate (1,500–2,500)</span>
            <span className="flex items-center gap-1 text-xs"><span className="inline-block w-3 h-3 rounded bg-green-900" /> Competitive (below 1,500)</span>
          </div>
        </div>
      </div>

      {/* Chart 6: Competition Trend — Market Contestability Score */}
      <div className="bg-gray-800 rounded-xl p-5 border border-gray-700 mb-6">
        <h2 className="text-lg font-semibold text-white mb-1">Market Contestability Score by Region (2019–2024)</h2>
        <p className="text-gray-400 text-xs mb-4">
          Composite score (1–10) reflecting ease of entry, number of effective competitors, and
          structural competition. Declining trend reflects ongoing consolidation.
        </p>
        <div className="flex flex-wrap gap-3 mb-3">
          {trendRegions.map((r) => (
            <span key={r} className="flex items-center gap-1 text-xs text-gray-300">
              <span
                className="inline-block w-3 h-3 rounded-sm"
                style={{ backgroundColor: REGION_COLORS[r] ?? '#9ca3af' }}
              />
              {r}
            </span>
          ))}
        </div>
        <ResponsiveContainer width="100%" height={280}>
          <LineChart data={contestabilityChartData} margin={{ right: 20 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="year" stroke="#9ca3af" tick={{ fontSize: 11 }} />
            <YAxis stroke="#9ca3af" tick={{ fontSize: 11 }} domain={[0, 10]} />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' }}
              formatter={(v: number) => [v.toFixed(2), 'Contestability Score']}
            />
            <Legend />
            {trendRegions.map((region) => (
              <Line
                key={region}
                type="monotone"
                dataKey={region}
                stroke={REGION_COLORS[region] ?? '#9ca3af'}
                strokeWidth={2}
                dot={{ r: 3 }}
                connectNulls
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Segment Legend */}
      <div className="bg-gray-800 rounded-xl p-4 border border-gray-700">
        <h2 className="text-sm font-semibold text-white mb-2">HHI Segment Reference</h2>
        <div className="flex flex-wrap gap-4">
          {segments.map((seg) => (
            <span key={seg} className="flex items-center gap-1 text-xs text-gray-300">
              <span
                className="inline-block w-3 h-3 rounded-sm"
                style={{ backgroundColor: SEGMENT_COLORS[seg] ?? '#9ca3af' }}
              />
              {seg}
            </span>
          ))}
        </div>
      </div>
    </div>
  )
}
