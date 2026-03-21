import React, { useEffect, useState } from 'react'
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
import { Radio } from 'lucide-react'
import {
  getAemoMarketOperationsDashboard,
  AMODADashboard,
} from '../api/client'

// ---------------------------------------------------------------------------
// Colour maps
// ---------------------------------------------------------------------------

const REGION_COLOURS: Record<string, string> = {
  NSW1: '#3b82f6',
  QLD1: '#f59e0b',
  VIC1: '#8b5cf6',
  SA1:  '#10b981',
  TAS1: '#06b6d4',
}

const NOTICE_COLOURS: Record<string, string> = {
  'MARKET NOTICE':             '#3b82f6',
  'LRC':                       '#f59e0b',
  'LACK_OF_RESERVE':           '#ef4444',
  'RECLASSIFIED_CONTINGENCY':  '#8b5cf6',
  'INTERVENTION':              '#f97316',
}

const REGIONS = ['NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1']

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
        <Radio size={16} />
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

export default function AemoMarketOperationsAnalytics() {
  const [data, setData] = useState<AMODADashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getAemoMarketOperationsDashboard()
      .then(setData)
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-gray-500 dark:text-gray-400">
        Loading AEMO Market Operations & Dispatch Analytics...
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

  // ── Chart 1: Stacked bar — renewable vs non-renewable dispatched GWh by month (all regions)
  const dispatchByMonth: Record<string, Record<string, number>> = {}
  for (const rec of data.dispatch) {
    if (!dispatchByMonth[rec.month]) dispatchByMonth[rec.month] = { month: rec.month } as unknown as Record<string, number>
    dispatchByMonth[rec.month][`${rec.region}_renewable`] =
      (dispatchByMonth[rec.month][`${rec.region}_renewable`] ?? 0) + rec.renewable_dispatched_gwh
    dispatchByMonth[rec.month][`${rec.region}_nonrenewable`] =
      (dispatchByMonth[rec.month][`${rec.region}_nonrenewable`] ?? 0) +
      (rec.total_dispatched_gwh - rec.renewable_dispatched_gwh)
  }
  const dispatchChartData = Object.values(dispatchByMonth).sort((a, b) =>
    String(a.month).localeCompare(String(b.month))
  )

  // ── Chart 2: Bar — total notice count by notice_type
  const noticeByType: Record<string, { notice_type: string; total_count: number }> = {}
  for (const rec of data.notices) {
    if (!noticeByType[rec.notice_type]) {
      noticeByType[rec.notice_type] = { notice_type: rec.notice_type, total_count: 0 }
    }
    noticeByType[rec.notice_type].total_count += rec.count
  }
  const noticeChartData = Object.values(noticeByType)

  // ── Chart 3: Stacked bar — total_energy_payments_m by quarter for 5 regions
  const settlementByQuarter: Record<string, Record<string, number>> = {}
  for (const rec of data.settlement) {
    if (!settlementByQuarter[rec.quarter]) settlementByQuarter[rec.quarter] = { quarter: rec.quarter } as unknown as Record<string, number>
    settlementByQuarter[rec.quarter][rec.region] =
      (settlementByQuarter[rec.quarter][rec.region] ?? 0) + rec.total_energy_payments_m
  }
  const settlementChartData = Object.values(settlementByQuarter).sort((a, b) =>
    String(a.quarter).localeCompare(String(b.quarter))
  )

  // ── Chart 4: Line — predispatch_mae_pct by month for 5 regions
  const predispatchByMonth: Record<string, Record<string, number>> = {}
  for (const rec of data.predispatch) {
    if (!predispatchByMonth[rec.month]) predispatchByMonth[rec.month] = { month: rec.month } as unknown as Record<string, number>
    predispatchByMonth[rec.month][rec.region] = rec.predispatch_mae_pct
  }
  const predispatchChartData = Object.values(predispatchByMonth).sort((a, b) =>
    String(a.month).localeCompare(String(b.month))
  )

  // ── Chart 5: Bar — events_count by metric coloured by regulatory_action_taken
  const eventsByMetric: Record<string, { metric: string; events_with_action: number; events_no_action: number }> = {}
  for (const rec of data.system_normal) {
    if (!eventsByMetric[rec.metric]) {
      eventsByMetric[rec.metric] = { metric: rec.metric, events_with_action: 0, events_no_action: 0 }
    }
    if (rec.regulatory_action_taken) {
      eventsByMetric[rec.metric].events_with_action += rec.events_count
    } else {
      eventsByMetric[rec.metric].events_no_action += rec.events_count
    }
  }
  const eventsChartData = Object.values(eventsByMetric).map((d) => ({
    ...d,
    metric_label: d.metric.length > 22 ? d.metric.slice(0, 20) + '…' : d.metric,
  }))

  return (
    <div className="p-6 space-y-8">
      {/* Header */}
      <div className="flex items-center gap-3">
        <Radio size={28} className="text-blue-600 dark:text-blue-400" />
        <div>
          <h1 className="text-2xl font-bold text-gray-900 dark:text-white">
            AEMO Market Operations &amp; Dispatch Analytics
          </h1>
          <p className="text-sm text-gray-500 dark:text-gray-400">
            Sprint 126c — AMODA | Dispatch, Notices, Settlement, Pre-Dispatch &amp; System Normal
          </p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KpiCard
          label="Total Dispatched"
          value={`${Number(summary.total_dispatched_twh ?? 0).toLocaleString(undefined, { maximumFractionDigits: 1 })} TWh`}
          sub="All regions, 2024"
        />
        <KpiCard
          label="Avg Renewable Share"
          value={`${Number(summary.avg_renewable_share_pct ?? 0).toFixed(1)}%`}
          sub="Renewable dispatch share"
        />
        <KpiCard
          label="Total Market Notices"
          value={Number(summary.total_notices ?? 0).toLocaleString()}
          sub="All notice types, 6 months"
        />
        <KpiCard
          label="Avg Pre-Dispatch Accuracy"
          value={`${Number(summary.avg_predispatch_accuracy_pct ?? 0).toFixed(1)}%`}
          sub="Price forecast accuracy"
        />
      </div>

      {/* Chart 1: Stacked bar — dispatch by month (renewable vs non-renewable per region) */}
      <div className="bg-white dark:bg-gray-800 rounded-xl p-5 shadow">
        <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
          Monthly Dispatch: Renewable vs Non-Renewable by Region (GWh)
        </h2>
        <ResponsiveContainer width="100%" height={320}>
          <BarChart data={dispatchChartData} margin={{ top: 8, right: 24, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="month" tick={{ fontSize: 11 }} />
            <YAxis tick={{ fontSize: 11 }} />
            <Tooltip />
            <Legend />
            {REGIONS.map((r) => (
              <Bar key={`${r}_renewable`} dataKey={`${r}_renewable`} stackId={r} name={`${r} Renewable`} fill={REGION_COLOURS[r]} />
            ))}
            {REGIONS.map((r) => (
              <Bar key={`${r}_nonrenewable`} dataKey={`${r}_nonrenewable`} stackId={r} name={`${r} Non-Renewable`} fill={REGION_COLOURS[r]} opacity={0.4} />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 2: Bar — notice count by notice_type */}
      <div className="bg-white dark:bg-gray-800 rounded-xl p-5 shadow">
        <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
          Market Notice Count by Notice Type
        </h2>
        <ResponsiveContainer width="100%" height={280}>
          <BarChart data={noticeChartData} margin={{ top: 8, right: 24, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="notice_type" tick={{ fontSize: 11 }} />
            <YAxis tick={{ fontSize: 11 }} />
            <Tooltip />
            <Bar dataKey="total_count" name="Total Count">
              {noticeChartData.map((entry) => (
                <rect key={entry.notice_type} fill={NOTICE_COLOURS[entry.notice_type] ?? '#6b7280'} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 3: Stacked bar — energy payments by quarter per region */}
      <div className="bg-white dark:bg-gray-800 rounded-xl p-5 shadow">
        <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
          Total Energy Payments by Quarter &amp; Region ($M)
        </h2>
        <ResponsiveContainer width="100%" height={320}>
          <BarChart data={settlementChartData} margin={{ top: 8, right: 24, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="quarter" tick={{ fontSize: 11 }} />
            <YAxis tick={{ fontSize: 11 }} />
            <Tooltip />
            <Legend />
            {REGIONS.map((r) => (
              <Bar key={r} dataKey={r} stackId="a" name={r} fill={REGION_COLOURS[r]} />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 4: Line — predispatch MAE % by month for 5 regions */}
      <div className="bg-white dark:bg-gray-800 rounded-xl p-5 shadow">
        <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
          Pre-Dispatch MAE % by Month &amp; Region
        </h2>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={predispatchChartData} margin={{ top: 8, right: 24, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="month" tick={{ fontSize: 11 }} />
            <YAxis tick={{ fontSize: 11 }} unit="%" />
            <Tooltip />
            <Legend />
            {REGIONS.map((r) => (
              <Line
                key={r}
                type="monotone"
                dataKey={r}
                name={r}
                stroke={REGION_COLOURS[r]}
                dot={false}
                strokeWidth={2}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 5: Bar — system normal events by metric coloured by regulatory action */}
      <div className="bg-white dark:bg-gray-800 rounded-xl p-5 shadow">
        <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
          System Normal Events by Metric (Regulatory Action Taken vs Not)
        </h2>
        <ResponsiveContainer width="100%" height={280}>
          <BarChart data={eventsChartData} margin={{ top: 8, right: 24, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="metric_label" tick={{ fontSize: 11 }} />
            <YAxis tick={{ fontSize: 11 }} />
            <Tooltip />
            <Legend />
            <Bar dataKey="events_with_action" name="Regulatory Action Taken" fill="#ef4444" stackId="a" />
            <Bar dataKey="events_no_action" name="No Regulatory Action" fill="#6b7280" stackId="a" />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Summary dl grid */}
      <div className="bg-white dark:bg-gray-800 rounded-xl p-5 shadow">
        <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
          Dashboard Summary
        </h2>
        <dl className="grid grid-cols-2 md:grid-cols-3 gap-x-8 gap-y-4">
          {Object.entries(summary).map(([k, v]) => (
            <div key={k}>
              <dt className="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wide">
                {k.replace(/_/g, ' ')}
              </dt>
              <dd className="text-base font-semibold text-gray-900 dark:text-white mt-1">
                {typeof v === 'number' ? v.toLocaleString(undefined, { maximumFractionDigits: 2 }) : String(v)}
              </dd>
            </div>
          ))}
        </dl>
      </div>
    </div>
  )
}
