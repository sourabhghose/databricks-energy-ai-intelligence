// Sprint 142b — CBAMX: Carbon Border Adjustment Mechanism Analytics
// Australian energy-intensive exports: CBAM exposure, trade flows, abatement,
// policy landscape and financial trajectory 2025-2030.

import { useEffect, useState } from 'react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  LineChart,
  Line,
  Cell,
} from 'recharts'
import { Globe2 } from 'lucide-react'
import {
  getCarbonBorderAdjustmentXDashboard,
  CBAMXDashboard,
} from '../api/client'

// ── colour palettes ──────────────────────────────────────────────────────────
const RISK_COLOURS: Record<string, string> = {
  High:   '#ef4444',
  Medium: '#f59e0b',
  Low:    '#22c55e',
}

const DEST_COLOURS: Record<string, string> = {
  EU:           '#6366f1',
  UK:           '#3b82f6',
  USA:          '#f59e0b',
  Japan:        '#10b981',
  'South Korea':'#a78bfa',
  Canada:       '#f97316',
}

const SECTOR_LINE_COLOURS = ['#6366f1', '#22c55e', '#f59e0b', '#ef4444']

const STATUS_COLOURS: Record<string, string> = {
  Implemented:         '#22c55e',
  'Phase-In':          '#3b82f6',
  Proposed:            '#f59e0b',
  'Under Consultation':'#a78bfa',
  Rejected:            '#ef4444',
}

// ── helpers ──────────────────────────────────────────────────────────────────
function fmt1(n: number) { return n.toFixed(1) }
function fmt2(n: number) { return n.toFixed(2) }

// ── KPI card ─────────────────────────────────────────────────────────────────
function KPICard({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="bg-gray-800 rounded-xl p-5 border border-gray-700">
      <p className="text-xs text-gray-400 uppercase tracking-wider mb-1">{label}</p>
      <p className="text-2xl font-bold text-white">{value}</p>
      {sub && <p className="text-xs text-gray-500 mt-1">{sub}</p>}
    </div>
  )
}

// ── tooltip style ─────────────────────────────────────────────────────────────
const tooltipStyle = {
  contentStyle: { backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '8px' },
  labelStyle:   { color: '#f3f4f6' },
  itemStyle:    { color: '#d1d5db' },
}

// ── main component ────────────────────────────────────────────────────────────
export default function CarbonBorderAdjustmentXAnalytics() {
  const [data, setData]       = useState<CBAMXDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState<string | null>(null)

  useEffect(() => {
    getCarbonBorderAdjustmentXDashboard()
      .then(d  => { setData(d); setLoading(false) })
      .catch(e => { setError(String(e)); setLoading(false) })
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full bg-gray-900">
        <p className="text-gray-400 animate-pulse">Loading Carbon Border Adjustment Analytics…</p>
      </div>
    )
  }
  if (error || !data) {
    return (
      <div className="flex items-center justify-center h-full bg-gray-900">
        <p className="text-red-400">Failed to load data: {error}</p>
      </div>
    )
  }

  const { sectors, trade_flows, abatement, policies, financial_impact, summary } = data

  // ── Chart 1: sector carbon intensity coloured by competitiveness risk ─────
  // Build a risk lookup from trade_flows (EU destination = most stringent)
  const sectorRisk: Record<string, string> = {}
  trade_flows
    .filter(tf => tf.destination_country === 'EU')
    .forEach(tf => { sectorRisk[tf.sector_id] = tf.competitiveness_risk })

  const chart1Data = sectors.map(s => ({
    name:  s.sector_name.split(' ').slice(0, 2).join(' '),
    value: s.carbon_intensity_tco2_per_t,
    risk:  sectorRisk[s.sector_id] ?? 'Low',
  }))

  // ── Chart 2: stacked bar — trade flow cbam_cost by destination for top 5 sectors ─
  const top5Ids = ['S01', 'S02', 'S03', 'S05', 'S08']
  const top5Names: Record<string, string> = {}
  sectors.forEach(s => { if (top5Ids.includes(s.sector_id)) top5Names[s.sector_id] = s.sector_name.split('/')[0].split(' ').slice(0, 2).join(' ') })

  // pivot: sector × destination → cbam_cost
  const chart2Map: Record<string, Record<string, number>> = {}
  top5Ids.forEach(id => {
    chart2Map[id] = { sector: top5Names[id] ?? id } as unknown as Record<string, number>
    Object.keys(DEST_COLOURS).forEach(d => { chart2Map[id][d] = 0 })
  })
  trade_flows.forEach(tf => {
    if (top5Ids.includes(tf.sector_id)) {
      chart2Map[tf.sector_id][tf.destination_country] =
        (chart2Map[tf.sector_id][tf.destination_country] ?? 0) + tf.cbam_cost_m_aud
    }
  })
  const chart2Data = top5Ids.map(id => chart2Map[id])

  // ── Chart 3: abatement emission_reduction_pct vs capex for each abate sector ─
  // Group by sector, show each measure as a bar pair
  const abate4Ids = ['S01', 'S02', 'S03', 'S05']
  const abate4Names: Record<string, string> = {}
  sectors.forEach(s => { if (abate4Ids.includes(s.sector_id)) abate4Names[s.sector_id] = s.sector_name.split('/')[0].split(' ').slice(0, 2).join(' ') })

  const chart3Data = abatement
    .filter(a => abate4Ids.includes(a.sector_id))
    .map(a => ({
      name:                a.abatement_measure.length > 12 ? a.abatement_measure.slice(0, 12) + '…' : a.abatement_measure,
      sector:              abate4Names[a.sector_id] ?? a.sector_id,
      emission_reduction:  a.emission_reduction_pct,
      capex:               a.capex_m_aud,
      feasibility:         a.feasibility,
    }))

  // ── Chart 4: line — financial cbam_cost 2025-2030 for top 4 sectors ─────
  const fin4Ids = ['S01', 'S02', 'S03', 'S08']
  const fin4Names: Record<string, string> = {}
  sectors.forEach(s => { if (fin4Ids.includes(s.sector_id)) fin4Names[s.sector_id] = s.sector_name.split('/')[0].split(' ').slice(0, 2).join(' ') })

  const finByYear: Record<number, Record<string, number>> = {}
  ;[2025, 2026, 2027, 2028, 2029, 2030].forEach(yr => { finByYear[yr] = { year: yr } })
  financial_impact
    .filter(fi => fin4Ids.includes(fi.sector_id))
    .forEach(fi => { finByYear[fi.year][fi.sector_id] = fi.cbam_cost_m_aud })
  const chart4Data = Object.values(finByYear).sort((a, b) => (a.year as number) - (b.year as number))

  // ── Chart 5: bar — policy carbon_price_benchmark by jurisdiction coloured by status ─
  const chart5Data = policies.map(p => ({
    name:   p.policy_name.length > 22 ? p.policy_name.slice(0, 22) + '…' : p.policy_name,
    value:  p.carbon_price_benchmark_aud,
    status: p.status,
    jur:    p.jurisdiction,
  }))

  return (
    <div className="p-6 bg-gray-900 min-h-full text-gray-100">

      {/* Header */}
      <div className="flex items-center gap-3 mb-6">
        <div className="p-2 bg-blue-600 rounded-lg">
          <Globe2 size={24} className="text-white" />
        </div>
        <div>
          <h1 className="text-2xl font-bold text-white">Carbon Border Adjustment Mechanism Analytics</h1>
          <p className="text-sm text-gray-400">
            Australian energy-intensive export sectors — EU/UK CBAM exposure, abatement pathways and financial trajectory 2025–2030
          </p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
        <KPICard
          label="Exports At Risk"
          value={`$${fmt2(summary.total_exports_at_risk_b_aud)}B AUD`}
          sub="EU/UK CBAM applicable sectors"
        />
        <KPICard
          label="Total CBAM Cost"
          value={`$${fmt1(summary.total_cbam_cost_m_aud)}M`}
          sub="Aggregate potential cost (AUD)"
        />
        <KPICard
          label="Most Exposed Sector"
          value={summary.most_exposed_sector.split('/')[0].split(' ').slice(0, 2).join(' ')}
          sub="Highest potential CBAM cost"
        />
        <KPICard
          label="Total Jobs At Risk"
          value={summary.total_jobs_at_risk.toLocaleString()}
          sub={`Policy readiness score: ${fmt1(summary.policy_readiness_score)}/10`}
        />
      </div>

      {/* Charts grid */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">

        {/* Chart 1: Sector Carbon Intensity coloured by EU Competitiveness Risk */}
        <div className="bg-gray-800 rounded-xl p-5 border border-gray-700">
          <h2 className="text-sm font-semibold text-gray-200 mb-4">
            Sector Carbon Intensity (tCO₂/t) — Coloured by EU Competitiveness Risk
          </h2>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={chart1Data} margin={{ top: 5, right: 10, left: 10, bottom: 70 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                dataKey="name"
                tick={{ fill: '#9ca3af', fontSize: 9 }}
                angle={-40}
                textAnchor="end"
                interval={0}
              />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} label={{ value: 'tCO₂/t', angle: -90, position: 'insideLeft', fill: '#6b7280', fontSize: 10 }} />
              <Tooltip
                {...tooltipStyle}
                formatter={(val: number, _name: string, props: { payload?: { risk: string } }) => [
                  `${val} tCO₂/t  (Risk: ${props.payload?.risk ?? ''})`,
                  'Carbon Intensity',
                ]}
              />
              <Bar dataKey="value" name="Carbon Intensity (tCO₂/t)" radius={[4, 4, 0, 0]}>
                {chart1Data.map((entry, idx) => (
                  <Cell key={idx} fill={RISK_COLOURS[entry.risk] ?? '#94a3b8'} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
          <div className="flex flex-wrap gap-3 mt-3">
            {Object.entries(RISK_COLOURS).map(([k, v]) => (
              <span key={k} className="flex items-center gap-1 text-xs text-gray-400">
                <span className="inline-block w-3 h-3 rounded-sm" style={{ backgroundColor: v }} />
                {k} Risk
              </span>
            ))}
          </div>
        </div>

        {/* Chart 2: Stacked Bar — Trade Flow CBAM Cost by Destination */}
        <div className="bg-gray-800 rounded-xl p-5 border border-gray-700">
          <h2 className="text-sm font-semibold text-gray-200 mb-4">
            CBAM Cost by Destination (Top 5 Sectors, $M AUD)
          </h2>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={chart2Data} margin={{ top: 5, right: 10, left: 10, bottom: 40 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                dataKey="sector"
                tick={{ fill: '#9ca3af', fontSize: 9 }}
                angle={-25}
                textAnchor="end"
                interval={0}
              />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <Tooltip {...tooltipStyle} />
              <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 11 }} />
              {Object.keys(DEST_COLOURS).map(dest => (
                <Bar
                  key={dest}
                  dataKey={dest}
                  stackId="a"
                  fill={DEST_COLOURS[dest]}
                  name={dest}
                />
              ))}
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Chart 3: Abatement Emission Reduction vs Capex */}
        <div className="bg-gray-800 rounded-xl p-5 border border-gray-700">
          <h2 className="text-sm font-semibold text-gray-200 mb-4">
            Abatement: Emission Reduction (%) vs Capex ($M) by Measure (4 sectors)
          </h2>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={chart3Data} margin={{ top: 5, right: 10, left: 10, bottom: 55 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                dataKey="name"
                tick={{ fill: '#9ca3af', fontSize: 9 }}
                angle={-35}
                textAnchor="end"
                interval={0}
              />
              <YAxis yAxisId="left"  tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <YAxis yAxisId="right" orientation="right" tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <Tooltip {...tooltipStyle} />
              <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 11 }} />
              <Bar yAxisId="left"  dataKey="emission_reduction" name="Emission Reduction (%)" fill="#22c55e" radius={[4, 4, 0, 0]} />
              <Bar yAxisId="right" dataKey="capex"              name="Capex ($M AUD)"          fill="#6366f1" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Chart 4: Line — CBAM Cost Trajectory 2025-2030 (top 4 sectors) */}
        <div className="bg-gray-800 rounded-xl p-5 border border-gray-700">
          <h2 className="text-sm font-semibold text-gray-200 mb-4">
            Financial Impact: CBAM Cost Trajectory 2025–2030 ($M AUD)
          </h2>
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={chart4Data} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="year" tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <Tooltip {...tooltipStyle} />
              <Legend wrapperStyle={{ color: '#9ca3af', fontSize: 11 }} />
              {fin4Ids.map((sid, i) => (
                <Line
                  key={sid}
                  type="monotone"
                  dataKey={sid}
                  stroke={SECTOR_LINE_COLOURS[i]}
                  strokeWidth={2}
                  dot={{ r: 4, fill: SECTOR_LINE_COLOURS[i] }}
                  name={fin4Names[sid] ?? sid}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Chart 5: Policy Carbon Price Benchmark coloured by Status */}
        <div className="bg-gray-800 rounded-xl p-5 border border-gray-700 lg:col-span-2">
          <h2 className="text-sm font-semibold text-gray-200 mb-4">
            Policy Carbon Price Benchmark by Jurisdiction ($/AUD) — Coloured by Status
          </h2>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={chart5Data} margin={{ top: 5, right: 10, left: 10, bottom: 80 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis
                dataKey="name"
                tick={{ fill: '#9ca3af', fontSize: 9 }}
                angle={-40}
                textAnchor="end"
                interval={0}
              />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} label={{ value: '$/AUD', angle: -90, position: 'insideLeft', fill: '#6b7280', fontSize: 10 }} />
              <Tooltip
                {...tooltipStyle}
                formatter={(val: number, _name: string, props: { payload?: { jur: string; status: string } }) => [
                  `$${val} AUD  (${props.payload?.jur ?? ''} · ${props.payload?.status ?? ''})`,
                  'Carbon Price Benchmark',
                ]}
              />
              <Bar dataKey="value" name="Carbon Price Benchmark" radius={[4, 4, 0, 0]}>
                {chart5Data.map((entry, idx) => (
                  <Cell key={idx} fill={STATUS_COLOURS[entry.status] ?? '#94a3b8'} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
          <div className="flex flex-wrap gap-3 mt-3">
            {Object.entries(STATUS_COLOURS).map(([k, v]) => (
              <span key={k} className="flex items-center gap-1 text-xs text-gray-400">
                <span className="inline-block w-3 h-3 rounded-sm" style={{ backgroundColor: v }} />
                {k}
              </span>
            ))}
          </div>
        </div>

      </div>
    </div>
  )
}
