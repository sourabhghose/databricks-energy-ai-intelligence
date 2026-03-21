import React, { useEffect, useState } from 'react'
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
  ScatterChart,
  Scatter,
  Cell,
} from 'recharts'
import { AlertTriangle } from 'lucide-react'
import {
  getEnergyMarketCreditRiskDashboard,
  EMCRXDashboard,
} from '../api/client'

// ---------------------------------------------------------------------------
// Colour maps
// ---------------------------------------------------------------------------

const CREDIT_RATING_COLOURS: Record<string, string> = {
  'AA':   '#10b981',
  'A+':   '#34d399',
  'A':    '#3b82f6',
  'A-':   '#60a5fa',
  'BBB+': '#f59e0b',
  'BBB':  '#fbbf24',
  'BBB-': '#ef4444',
  'BB+':  '#dc2626',
}

const REGION_COLOURS: Record<string, string> = {
  NSW1: '#3b82f6',
  QLD1: '#f59e0b',
  VIC1: '#8b5cf6',
  SA1:  '#10b981',
  TAS1: '#06b6d4',
}

const EVENT_TYPE_COLOURS: Record<string, string> = {
  'Technical Default':    '#ef4444',
  'Payment Default':      '#dc2626',
  'Withdrawal':           '#f59e0b',
  'Suspension':           '#8b5cf6',
  'Margin Call Failure':  '#3b82f6',
}

const TREND_COLOURS: Record<string, string> = {
  'Improving':    '#10b981',
  'Stable':       '#3b82f6',
  'Deteriorating':'#ef4444',
}

const SCENARIO_COLOURS: Record<string, string> = {
  'Price Spike $15,000/MWh': '#ef4444',
  'Generator Exit':          '#f59e0b',
  'Retailer Collapse':       '#8b5cf6',
  'Interconnector Failure':  '#3b82f6',
}

// ---------------------------------------------------------------------------
// KPI card
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
    <div className="bg-white dark:bg-gray-800 rounded-xl p-5 shadow flex flex-col gap-1">
      <div className="flex items-center gap-2 text-gray-500 dark:text-gray-400 text-sm font-medium">
        <AlertTriangle size={16} />
        {label}
      </div>
      <div className="text-2xl font-bold text-gray-900 dark:text-white">{value}</div>
      {sub && <div className="text-xs text-gray-500 dark:text-gray-400">{sub}</div>}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

export default function EnergyMarketCreditRiskAnalytics() {
  const [data, setData] = useState<EMCRXDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getEnergyMarketCreditRiskDashboard()
      .then(setData)
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-gray-500 dark:text-gray-400">
        Loading Energy Market Credit Risk Analytics...
      </div>
    )
  }
  if (error || !data) {
    return (
      <div className="flex items-center justify-center h-64 text-red-500">
        Error: {error ?? 'No data'}
      </div>
    )
  }

  const summary = data.summary as Record<string, unknown>

  // ── Chart 1: Bar — net_market_exposure_m by participant coloured by credit_rating ──
  const chart1Data = data.participants
    .slice()
    .sort((a, b) => b.net_market_exposure_m - a.net_market_exposure_m)
    .map((p) => ({
      name: p.participant_name.split(' ')[0],
      full_name: p.participant_name,
      net_market_exposure_m: p.net_market_exposure_m,
      credit_rating: p.credit_rating,
    }))

  // ── Chart 2: Line — total_market_exposure_m by quarter for 5 regions ──
  const quartersSet = Array.from(new Set(data.exposures.map((e) => e.quarter))).sort()
  const regions = ['NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1']
  const exposureByQuarterRegion: Record<string, Record<string, number>> = {}
  for (const e of data.exposures) {
    if (!exposureByQuarterRegion[e.quarter]) exposureByQuarterRegion[e.quarter] = {}
    exposureByQuarterRegion[e.quarter][e.region] = e.total_market_exposure_m
  }
  const chart2Data = quartersSet.map((q) => ({
    quarter: q,
    ...(exposureByQuarterRegion[q] ?? {}),
  }))

  // ── Chart 3: Bar — default_amount_m by year coloured by event_type ──
  const yearsSet = Array.from(new Set(data.default_history.map((d) => d.year))).sort()
  const eventTypes = ['Technical Default', 'Payment Default', 'Withdrawal', 'Suspension', 'Margin Call Failure']
  const defaultByYearEvent: Record<number, Record<string, number>> = {}
  for (const d of data.default_history) {
    if (!defaultByYearEvent[d.year]) defaultByYearEvent[d.year] = {}
    defaultByYearEvent[d.year][d.event_type] =
      (defaultByYearEvent[d.year][d.event_type] ?? 0) + d.default_amount_m
  }
  const chart3Data = yearsSet.map((yr) => ({
    year: String(yr),
    ...(defaultByYearEvent[yr] ?? {}),
  }))

  // ── Chart 4: Bar — breach_count by metric_name coloured by trend ──
  const metricMap: Record<string, { breach_count: number; trend: string }> = {}
  for (const m of data.prudential_metrics) {
    if (!metricMap[m.metric_name]) {
      metricMap[m.metric_name] = { breach_count: 0, trend: m.trend }
    }
    metricMap[m.metric_name].breach_count += m.breach_count
  }
  const chart4Data = Object.entries(metricMap).map(([metric_name, v]) => ({
    metric_name,
    breach_count: v.breach_count,
    trend: v.trend,
  }))

  // ── Chart 5: Scatter — systemic_risk_score vs potential_default_m dot sized by total_exposure_m ──
  const chart5Data = data.stress_tests.map((s) => ({
    systemic_risk_score: s.systemic_risk_score,
    potential_default_m: s.potential_default_m,
    total_exposure_m: s.total_exposure_m,
    scenario: s.scenario,
    region: s.region,
  }))

  return (
    <div className="p-6 space-y-8">
      {/* Header */}
      <div className="flex items-center gap-3">
        <AlertTriangle size={28} className="text-orange-600 dark:text-orange-400" />
        <div>
          <h1 className="text-2xl font-bold text-gray-900 dark:text-white">
            Energy Market Participant Credit Risk Analytics
          </h1>
          <p className="text-sm text-gray-500 dark:text-gray-400">
            Sprint 128c — EMCRX | Prudential Requirements, Exposure, Default History &amp; Stress Testing
          </p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KpiCard
          label="Total Market Exposure"
          value={`$${Number(summary.total_market_exposure_m ?? 0).toLocaleString('en-AU', { maximumFractionDigits: 0 })}M`}
          sub="Aggregate across all quarters and regions"
        />
        <KpiCard
          label="Avg Collateral Coverage"
          value={`${Number(summary.avg_collateral_coverage_pct ?? 0).toFixed(1)}%`}
          sub="Average collateral coverage across all exposure records"
        />
        <KpiCard
          label="Highest Risk Participant"
          value={String(summary.highest_risk_participant ?? '—')}
          sub="Participant with highest net market exposure"
        />
        <KpiCard
          label="Stress Test Max Default"
          value={`$${Number(summary.stress_test_max_default_m ?? 0).toFixed(1)}M`}
          sub="Maximum potential default under stress scenarios"
        />
      </div>

      {/* Chart 1: Bar — net_market_exposure_m by participant */}
      <div className="bg-white dark:bg-gray-800 rounded-xl p-5 shadow">
        <h2 className="text-lg font-semibold text-gray-800 dark:text-white mb-4">
          Chart 1: Net Market Exposure by Participant (coloured by Credit Rating)
        </h2>
        <ResponsiveContainer width="100%" height={320}>
          <BarChart data={chart1Data} margin={{ top: 10, right: 20, left: 10, bottom: 60 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="name" tick={{ fontSize: 11 }} angle={-35} textAnchor="end" />
            <YAxis unit=" $M" tick={{ fontSize: 11 }} />
            <Tooltip
              formatter={(v: number) => [`$${v.toFixed(2)}M`, 'Net Exposure']}
              labelFormatter={(_label, payload) =>
                payload?.[0]?.payload?.full_name ?? _label
              }
            />
            <Bar dataKey="net_market_exposure_m" name="Net Market Exposure ($M)">
              {chart1Data.map((entry) => (
                <Cell
                  key={entry.full_name}
                  fill={CREDIT_RATING_COLOURS[entry.credit_rating] ?? '#6b7280'}
                />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
        <div className="flex flex-wrap gap-2 mt-2">
          {Object.entries(CREDIT_RATING_COLOURS).map(([rating, colour]) => (
            <span key={rating} className="flex items-center gap-1 text-xs text-gray-600 dark:text-gray-300">
              <span className="inline-block w-3 h-3 rounded-sm" style={{ background: colour }} />
              {rating}
            </span>
          ))}
        </div>
      </div>

      {/* Chart 2: Line — total_market_exposure_m by quarter for 5 regions */}
      <div className="bg-white dark:bg-gray-800 rounded-xl p-5 shadow">
        <h2 className="text-lg font-semibold text-gray-800 dark:text-white mb-4">
          Chart 2: Total Market Exposure by Quarter per Region ($M)
        </h2>
        <ResponsiveContainer width="100%" height={320}>
          <LineChart data={chart2Data} margin={{ top: 10, right: 20, left: 10, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="quarter" tick={{ fontSize: 11 }} />
            <YAxis unit=" $M" tick={{ fontSize: 11 }} />
            <Tooltip formatter={(v: number) => `$${v.toFixed(2)}M`} />
            <Legend />
            {regions.map((region) => (
              <Line
                key={region}
                type="monotone"
                dataKey={region}
                stroke={REGION_COLOURS[region] ?? '#6b7280'}
                dot={false}
                strokeWidth={2}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 3: Bar — default_amount_m by year coloured by event_type */}
      <div className="bg-white dark:bg-gray-800 rounded-xl p-5 shadow">
        <h2 className="text-lg font-semibold text-gray-800 dark:text-white mb-4">
          Chart 3: Default Amount by Year and Event Type ($M)
        </h2>
        <ResponsiveContainer width="100%" height={320}>
          <BarChart data={chart3Data} margin={{ top: 10, right: 20, left: 10, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="year" tick={{ fontSize: 11 }} />
            <YAxis unit=" $M" tick={{ fontSize: 11 }} />
            <Tooltip formatter={(v: number) => `$${v.toFixed(2)}M`} />
            <Legend />
            {eventTypes.map((et) => (
              <Bar
                key={et}
                dataKey={et}
                stackId="a"
                fill={EVENT_TYPE_COLOURS[et] ?? '#6b7280'}
              />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 4: Bar — breach_count by metric_name coloured by trend */}
      <div className="bg-white dark:bg-gray-800 rounded-xl p-5 shadow">
        <h2 className="text-lg font-semibold text-gray-800 dark:text-white mb-4">
          Chart 4: Total Breach Count by Prudential Metric (coloured by Trend)
        </h2>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={chart4Data} margin={{ top: 10, right: 20, left: 10, bottom: 60 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="metric_name" tick={{ fontSize: 10 }} angle={-20} textAnchor="end" />
            <YAxis tick={{ fontSize: 11 }} />
            <Tooltip formatter={(v: number) => [v, 'Breach Count']} />
            <Bar dataKey="breach_count" name="Breach Count">
              {chart4Data.map((entry) => (
                <Cell
                  key={entry.metric_name}
                  fill={TREND_COLOURS[entry.trend] ?? '#6b7280'}
                />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
        <div className="flex flex-wrap gap-3 mt-2">
          {Object.entries(TREND_COLOURS).map(([trend, colour]) => (
            <span key={trend} className="flex items-center gap-1 text-xs text-gray-600 dark:text-gray-300">
              <span className="inline-block w-3 h-3 rounded-sm" style={{ background: colour }} />
              {trend}
            </span>
          ))}
        </div>
      </div>

      {/* Chart 5: Scatter — systemic_risk_score vs potential_default_m */}
      <div className="bg-white dark:bg-gray-800 rounded-xl p-5 shadow">
        <h2 className="text-lg font-semibold text-gray-800 dark:text-white mb-4">
          Chart 5: Stress Test — Systemic Risk Score vs Potential Default ($M)
          <span className="text-sm font-normal text-gray-500 dark:text-gray-400 ml-2">
            (dot size = total exposure, colour = scenario)
          </span>
        </h2>
        <ResponsiveContainer width="100%" height={360}>
          <ScatterChart margin={{ top: 10, right: 20, left: 10, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis
              dataKey="systemic_risk_score"
              name="Systemic Risk Score"
              unit=""
              tick={{ fontSize: 11 }}
              label={{ value: 'Systemic Risk Score (0-100)', position: 'insideBottom', offset: -5, fontSize: 11 }}
            />
            <YAxis
              dataKey="potential_default_m"
              name="Potential Default"
              unit=" $M"
              tick={{ fontSize: 11 }}
            />
            <Tooltip
              cursor={{ strokeDasharray: '3 3' }}
              content={({ payload }) => {
                if (!payload?.length) return null
                const d = payload[0].payload
                return (
                  <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-600 rounded p-2 text-xs shadow">
                    <p className="font-semibold">{d.scenario}</p>
                    <p>Region: {d.region}</p>
                    <p>Systemic Risk: {d.systemic_risk_score.toFixed(1)}</p>
                    <p>Potential Default: ${d.potential_default_m.toFixed(2)}M</p>
                    <p>Total Exposure: ${d.total_exposure_m.toFixed(2)}M</p>
                  </div>
                )
              }}
            />
            <Scatter
              data={chart5Data}
              shape={((props: Record<string, unknown>) => {
                const cx = Number(props.cx ?? 0)
                const cy = Number(props.cy ?? 0)
                const payload = props.payload as typeof chart5Data[0]
                const r = Math.max(4, Math.min(20, (payload.total_exposure_m / 5000) * 20))
                const fill = SCENARIO_COLOURS[payload.scenario] ?? '#6b7280'
                return <circle cx={cx} cy={cy} r={r} fill={fill} fillOpacity={0.7} stroke={fill} strokeWidth={1} />
              }) as any}
            />
          </ScatterChart>
        </ResponsiveContainer>
        <div className="flex flex-wrap gap-3 mt-2">
          {Object.entries(SCENARIO_COLOURS).map(([scenario, colour]) => (
            <span key={scenario} className="flex items-center gap-1 text-xs text-gray-600 dark:text-gray-300">
              <span className="inline-block w-3 h-3 rounded-full" style={{ background: colour }} />
              {scenario}
            </span>
          ))}
        </div>
      </div>

      {/* Summary dl grid */}
      <div className="bg-white dark:bg-gray-800 rounded-xl p-5 shadow">
        <h2 className="text-lg font-semibold text-gray-800 dark:text-white mb-4">
          Summary
        </h2>
        <dl className="grid grid-cols-2 md:grid-cols-3 gap-x-8 gap-y-4">
          <div>
            <dt className="text-xs text-gray-500 dark:text-gray-400 font-medium uppercase tracking-wide">
              Total Market Exposure
            </dt>
            <dd className="text-base font-semibold text-gray-900 dark:text-white mt-0.5">
              ${Number(summary.total_market_exposure_m ?? 0).toLocaleString('en-AU', { maximumFractionDigits: 0 })}M
            </dd>
          </div>
          <div>
            <dt className="text-xs text-gray-500 dark:text-gray-400 font-medium uppercase tracking-wide">
              Avg Collateral Coverage
            </dt>
            <dd className="text-base font-semibold text-gray-900 dark:text-white mt-0.5">
              {Number(summary.avg_collateral_coverage_pct ?? 0).toFixed(1)}%
            </dd>
          </div>
          <div>
            <dt className="text-xs text-gray-500 dark:text-gray-400 font-medium uppercase tracking-wide">
              Highest Risk Participant
            </dt>
            <dd className="text-base font-semibold text-gray-900 dark:text-white mt-0.5">
              {String(summary.highest_risk_participant ?? '—')}
            </dd>
          </div>
          <div>
            <dt className="text-xs text-gray-500 dark:text-gray-400 font-medium uppercase tracking-wide">
              Stress Test Max Default
            </dt>
            <dd className="text-base font-semibold text-gray-900 dark:text-white mt-0.5">
              ${Number(summary.stress_test_max_default_m ?? 0).toFixed(1)}M
            </dd>
          </div>
          <div>
            <dt className="text-xs text-gray-500 dark:text-gray-400 font-medium uppercase tracking-wide">
              Breach Events YTD
            </dt>
            <dd className="text-base font-semibold text-gray-900 dark:text-white mt-0.5">
              {Number(summary.breach_events_ytd ?? 0).toLocaleString()}
            </dd>
          </div>
          <div>
            <dt className="text-xs text-gray-500 dark:text-gray-400 font-medium uppercase tracking-wide">
              Participants Monitored
            </dt>
            <dd className="text-base font-semibold text-gray-900 dark:text-white mt-0.5">
              {data.participants.length}
            </dd>
          </div>
        </dl>
      </div>
    </div>
  )
}
