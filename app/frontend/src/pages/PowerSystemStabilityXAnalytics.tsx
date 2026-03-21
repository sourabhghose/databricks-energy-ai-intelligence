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
import { Shield } from 'lucide-react'
import {
  getPowerSystemStabilityXDashboard,
  PSSAXDashboard,
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

const EVENT_COLOURS: Record<string, string> = {
  'N-1 Contingency':         '#3b82f6',
  'N-2 Contingency':         '#8b5cf6',
  'Severe Weather':           '#f59e0b',
  'Cyber Attack':             '#ef4444',
  'Bushfire':                 '#f97316',
}

const SCHEME_COLOURS: Record<string, string> = {
  'Distance Protection':                '#3b82f6',
  'Differential Protection':            '#10b981',
  'Rate of Change of Frequency':        '#f59e0b',
  'Under-Frequency Load Shedding':      '#8b5cf6',
  'Overcurrent Protection':             '#f97316',
}

const REGIONS = ['NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1']
const SCR_YEARS = [2020, 2021, 2022, 2023, 2024]

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
        <Shield size={16} />
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

export default function PowerSystemStabilityXAnalytics() {
  const [data, setData] = useState<PSSAXDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getPowerSystemStabilityXDashboard()
      .then(setData)
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-gray-500 dark:text-gray-400">
        Loading Power System Stability & Resilience Analytics...
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

  // ── Chart 1: Line — total_inertia_mws by month for 5 regions ────────────
  const inertiaByMonth: Record<string, Record<string, number>> = {}
  for (const rec of data.inertia) {
    if (!inertiaByMonth[rec.month]) inertiaByMonth[rec.month] = { month: rec.month } as unknown as Record<string, number>
    inertiaByMonth[rec.month][rec.region] = rec.total_inertia_mws
  }
  const inertiaChartData = Object.values(inertiaByMonth).sort((a, b) =>
    String(a.month).localeCompare(String(b.month))
  )

  // ── Chart 2: Bar — short_circuit_ratio by region x year (grouped) ────────
  const scrByRegion: Record<string, Record<string, number>> = {}
  for (const rec of data.scr) {
    if (!scrByRegion[rec.region]) scrByRegion[rec.region] = { region: rec.region } as unknown as Record<string, number>
    scrByRegion[rec.region][String(rec.year)] = rec.short_circuit_ratio
  }
  const scrChartData = REGIONS.map((r) => scrByRegion[r] ?? { region: r })

  // ── Chart 3: Bar — unserved_energy_mwh by event_type ─────────────────────
  const useByEvent: Record<string, { event_type: string; unserved_energy_mwh: number; avg_resilience: number }> = {}
  for (const rec of data.resilience) {
    if (!useByEvent[rec.event_type]) {
      useByEvent[rec.event_type] = {
        event_type: rec.event_type,
        unserved_energy_mwh: 0,
        avg_resilience: 0,
      }
    }
    useByEvent[rec.event_type].unserved_energy_mwh += rec.unserved_energy_mwh
  }
  // compute avg resilience per event type
  const countByEvent: Record<string, number> = {}
  for (const rec of data.resilience) {
    countByEvent[rec.event_type] = (countByEvent[rec.event_type] ?? 0) + 1
    useByEvent[rec.event_type].avg_resilience =
      (useByEvent[rec.event_type].avg_resilience ?? 0) + rec.resilience_score
  }
  for (const et of Object.keys(useByEvent)) {
    useByEvent[et].avg_resilience = parseFloat(
      (useByEvent[et].avg_resilience / (countByEvent[et] ?? 1)).toFixed(1)
    )
    useByEvent[et].unserved_energy_mwh = parseFloat(
      useByEvent[et].unserved_energy_mwh.toFixed(1)
    )
  }
  const useChartData = Object.values(useByEvent)

  // ── Chart 4: Line — static_voltage_stability_margin_pct by month ─────────
  const voltageByMonth: Record<string, Record<string, number>> = {}
  for (const rec of data.voltage) {
    if (!voltageByMonth[rec.month]) voltageByMonth[rec.month] = { month: rec.month } as unknown as Record<string, number>
    voltageByMonth[rec.month][rec.region] = rec.static_voltage_stability_margin_pct
  }
  const voltageChartData = Object.values(voltageByMonth).sort((a, b) =>
    String(a.month).localeCompare(String(b.month))
  )

  // ── Chart 5: Bar — response_time_ms by protection_scheme ─────────────────
  const protByScheme: Record<string, { protection_scheme: string; avg_response_ms: number; upgrade_needed_count: number }> = {}
  const countByScheme: Record<string, number> = {}
  for (const rec of data.protection) {
    const key = rec.protection_scheme
    if (!protByScheme[key]) {
      protByScheme[key] = {
        protection_scheme: key,
        avg_response_ms: 0,
        upgrade_needed_count: 0,
      }
      countByScheme[key] = 0
    }
    protByScheme[key].avg_response_ms += rec.response_time_ms
    countByScheme[key] += 1
    if (rec.upgrade_needed) protByScheme[key].upgrade_needed_count += 1
  }
  const protChartData = Object.values(protByScheme).map((p) => ({
    ...p,
    avg_response_ms: parseFloat((p.avg_response_ms / (countByScheme[p.protection_scheme] ?? 1)).toFixed(1)),
    scheme_label: p.protection_scheme.length > 22 ? p.protection_scheme.slice(0, 20) + '…' : p.protection_scheme,
  }))

  return (
    <div className="p-6 space-y-8">
      {/* Header */}
      <div className="flex items-center gap-3">
        <Shield size={28} className="text-blue-600 dark:text-blue-400" />
        <div>
          <h1 className="text-2xl font-bold text-gray-900 dark:text-white">
            Power System Stability &amp; Resilience Analytics
          </h1>
          <p className="text-sm text-gray-500 dark:text-gray-400">
            Sprint 125c — PSSAX | Inertia, Voltage Stability, SCR, Resilience & Protection
          </p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KpiCard
          label="Avg Total Inertia"
          value={`${Number(summary.avg_total_inertia_mws ?? 0).toLocaleString()} MWs`}
          sub="System-wide average across all regions & months"
        />
        <KpiCard
          label="Lowest SCR Region"
          value={String(summary.lowest_scr_region ?? '—')}
          sub="Region with weakest short-circuit ratio in 2024"
        />
        <KpiCard
          label="Total USE"
          value={`${Number(summary.total_unserved_energy_mwh ?? 0).toLocaleString()} MWh`}
          sub="Unserved energy across all events & regions"
        />
        <KpiCard
          label="Avg Resilience Score"
          value={`${Number(summary.avg_resilience_score ?? 0).toFixed(1)} / 100`}
          sub="Average resilience score across all event types"
        />
      </div>

      {/* Chart 1: Inertia by Month */}
      <div className="bg-white dark:bg-gray-800 rounded-xl p-5 shadow">
        <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
          Total Inertia (MWs) by Month — 5 Regions
        </h2>
        <ResponsiveContainer width="100%" height={320}>
          <LineChart data={inertiaChartData} margin={{ top: 4, right: 24, bottom: 4, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="month" tick={{ fontSize: 11 }} />
            <YAxis tick={{ fontSize: 11 }} />
            <Tooltip />
            <Legend />
            {REGIONS.map((r) => (
              <Line
                key={r}
                type="monotone"
                dataKey={r}
                stroke={REGION_COLOURS[r]}
                dot={false}
                strokeWidth={2}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 2: SCR by Region x Year */}
      <div className="bg-white dark:bg-gray-800 rounded-xl p-5 shadow">
        <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
          Short-Circuit Ratio (SCR) by Region &amp; Year
        </h2>
        <ResponsiveContainer width="100%" height={320}>
          <BarChart data={scrChartData} margin={{ top: 4, right: 24, bottom: 4, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="region" tick={{ fontSize: 12 }} />
            <YAxis tick={{ fontSize: 11 }} />
            <Tooltip />
            <Legend />
            {SCR_YEARS.map((yr, idx) => {
              const colours = ['#3b82f6', '#f59e0b', '#10b981', '#8b5cf6', '#f97316']
              return (
                <Bar
                  key={yr}
                  dataKey={String(yr)}
                  fill={colours[idx % colours.length]}
                  name={String(yr)}
                />
              )
            })}
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 3: Unserved Energy by Event Type */}
      <div className="bg-white dark:bg-gray-800 rounded-xl p-5 shadow">
        <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
          Unserved Energy (MWh) by Event Type
        </h2>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={useChartData} margin={{ top: 4, right: 24, bottom: 60, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis
              dataKey="event_type"
              tick={{ fontSize: 11 }}
              angle={-20}
              textAnchor="end"
              interval={0}
            />
            <YAxis tick={{ fontSize: 11 }} />
            <Tooltip
              formatter={(value: unknown, name: string) => {
                if (name === 'unserved_energy_mwh') return [`${Number(value).toFixed(1)} MWh`, 'USE']
                if (name === 'avg_resilience') return [`${Number(value).toFixed(1)}`, 'Avg Resilience Score']
                return [String(value), name]
              }}
            />
            <Legend />
            <Bar
              dataKey="unserved_energy_mwh"
              name="USE (MWh)"
              fill="#ef4444"
            />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 4: Voltage Stability Margin by Month */}
      <div className="bg-white dark:bg-gray-800 rounded-xl p-5 shadow">
        <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
          Static Voltage Stability Margin (%) by Month — 5 Regions
        </h2>
        <ResponsiveContainer width="100%" height={320}>
          <LineChart data={voltageChartData} margin={{ top: 4, right: 24, bottom: 4, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="month" tick={{ fontSize: 11 }} />
            <YAxis tick={{ fontSize: 11 }} />
            <Tooltip />
            <Legend />
            {REGIONS.map((r) => (
              <Line
                key={r}
                type="monotone"
                dataKey={r}
                stroke={REGION_COLOURS[r]}
                dot={false}
                strokeWidth={2}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 5: Protection System Response Time */}
      <div className="bg-white dark:bg-gray-800 rounded-xl p-5 shadow">
        <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
          Avg Response Time (ms) by Protection Scheme — Upgrade Flag
        </h2>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={protChartData} margin={{ top: 4, right: 24, bottom: 60, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis
              dataKey="scheme_label"
              tick={{ fontSize: 11 }}
              angle={-20}
              textAnchor="end"
              interval={0}
            />
            <YAxis tick={{ fontSize: 11 }} />
            <Tooltip
              formatter={(value: unknown, name: string) => {
                if (name === 'avg_response_ms') return [`${Number(value).toFixed(1)} ms`, 'Avg Response Time']
                if (name === 'upgrade_needed_count') return [String(value), 'Regions Needing Upgrade']
                return [String(value), name]
              }}
            />
            <Legend />
            <Bar dataKey="avg_response_ms" name="Avg Response (ms)" fill="#3b82f6" />
            <Bar dataKey="upgrade_needed_count" name="Upgrade Needed (regions)" fill="#ef4444" />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Summary DL grid */}
      <div className="bg-white dark:bg-gray-800 rounded-xl p-5 shadow">
        <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
          Dashboard Summary
        </h2>
        <dl className="grid grid-cols-2 md:grid-cols-3 gap-x-8 gap-y-4">
          <div>
            <dt className="text-xs text-gray-500 dark:text-gray-400 font-medium uppercase tracking-wide">
              Avg Total Inertia (MWs)
            </dt>
            <dd className="text-base font-semibold text-gray-900 dark:text-white mt-1">
              {Number(summary.avg_total_inertia_mws ?? 0).toLocaleString()}
            </dd>
          </div>
          <div>
            <dt className="text-xs text-gray-500 dark:text-gray-400 font-medium uppercase tracking-wide">
              Lowest SCR Region (2024)
            </dt>
            <dd className="text-base font-semibold text-gray-900 dark:text-white mt-1">
              {String(summary.lowest_scr_region ?? '—')}
            </dd>
          </div>
          <div>
            <dt className="text-xs text-gray-500 dark:text-gray-400 font-medium uppercase tracking-wide">
              Total Unserved Energy (MWh)
            </dt>
            <dd className="text-base font-semibold text-gray-900 dark:text-white mt-1">
              {Number(summary.total_unserved_energy_mwh ?? 0).toLocaleString()}
            </dd>
          </div>
          <div>
            <dt className="text-xs text-gray-500 dark:text-gray-400 font-medium uppercase tracking-wide">
              Avg Resilience Score
            </dt>
            <dd className="text-base font-semibold text-gray-900 dark:text-white mt-1">
              {Number(summary.avg_resilience_score ?? 0).toFixed(1)} / 100
            </dd>
          </div>
          <div>
            <dt className="text-xs text-gray-500 dark:text-gray-400 font-medium uppercase tracking-wide">
              IBR Penetration (%)
            </dt>
            <dd className="text-base font-semibold text-gray-900 dark:text-white mt-1">
              {Number(summary.ibr_penetration_pct ?? 0).toFixed(1)}%
            </dd>
          </div>
        </dl>
      </div>
    </div>
  )
}
