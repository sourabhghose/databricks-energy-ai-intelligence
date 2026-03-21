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
  Cell,
} from 'recharts'
import { Monitor } from 'lucide-react'
import {
  getGridModernisationDigitalTwinDashboard,
  GMDTDashboard,
} from '../api/client'

// ---------------------------------------------------------------------------
// Colour maps
// ---------------------------------------------------------------------------

const TECH_COLOURS: Record<string, string> = {
  'Digital Twin':           '#3b82f6',
  'AI/ML Analytics':        '#8b5cf6',
  'IoT Sensors':            '#10b981',
  'Advanced SCADA':         '#f59e0b',
  'Drone Inspection':       '#ef4444',
  'Predictive Maintenance': '#06b6d4',
}

const NB_COLOURS: Record<string, string> = {
  'Ausgrid':           '#3b82f6',
  'AusNet':            '#8b5cf6',
  'SA Power Networks': '#10b981',
  'TransGrid':         '#f59e0b',
  'ElectraNet':        '#ef4444',
}

const VENDOR_COLOURS: Record<string, string> = {
  'GE Vernova':     '#3b82f6',
  'Siemens':        '#8b5cf6',
  'ABB':            '#10b981',
  'Microsoft Azure':'#f59e0b',
  'AWS':            '#ef4444',
}

const YEAR_COLOURS: Record<number, string> = {
  2020: '#3b82f6',
  2021: '#8b5cf6',
  2022: '#10b981',
  2023: '#f59e0b',
  2024: '#ef4444',
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
        <Monitor size={16} />
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

export default function GridModernisationDigitalTwinAnalytics() {
  const [data, setData] = useState<GMDTDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getGridModernisationDigitalTwinDashboard()
      .then(setData)
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-gray-500 dark:text-gray-400">
        Loading Grid Modernisation &amp; Digital Twin Analytics...
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

  // ── Chart 1: Bar — investment_m by initiative_name coloured by technology (top 15) ──
  const chart1Data = [...data.initiatives]
    .sort((a, b) => b.investment_m - a.investment_m)
    .slice(0, 15)
    .map((init) => ({
      name: init.initiative_name.length > 30 ? init.initiative_name.slice(0, 28) + '…' : init.initiative_name,
      investment_m: init.investment_m,
      technology: init.technology,
    }))

  // ── Chart 2: Bar — deployed_count_k by sensor_type sorted desc ──────────
  const sensorMap: Record<string, number> = {}
  for (const s of data.sensors) {
    sensorMap[s.sensor_type] = (sensorMap[s.sensor_type] ?? 0) + s.deployed_count_k
  }
  const chart2Data = Object.entries(sensorMap)
    .map(([sensor_type, deployed_count_k]) => ({ sensor_type, deployed_count_k }))
    .sort((a, b) => b.deployed_count_k - a.deployed_count_k)

  // ── Chart 3: Line — prediction_accuracy_pct by year for 5 network businesses ──
  const years = [2020, 2021, 2022, 2023, 2024]
  const nbList = ['Ausgrid', 'AusNet', 'SA Power Networks', 'TransGrid', 'ElectraNet']
  const fdByNbYear: Record<string, Record<number, number>> = {}
  for (const fd of data.fault_detection) {
    if (!fdByNbYear[fd.network_business]) fdByNbYear[fd.network_business] = {}
    fdByNbYear[fd.network_business][fd.year] = fd.prediction_accuracy_pct
  }
  const chart3Data = years.map((year) => {
    const row: Record<string, unknown> = { year: String(year) }
    for (const nb of nbList) {
      row[nb] = fdByNbYear[nb]?.[year] ?? null
    }
    return row
  })

  // ── Chart 4: Bar — incidents_prevented by threat_category coloured by year (stacked) ──
  const threatList = ['Ransomware', 'SCADA Intrusion', 'Supply Chain Attack', 'Insider Threat', 'DDoS']
  const csMap: Record<string, Record<number, number>> = {}
  for (const cs of data.cyber_security) {
    if (!csMap[cs.threat_category]) csMap[cs.threat_category] = {}
    csMap[cs.threat_category][cs.year] = (csMap[cs.threat_category][cs.year] ?? 0) + cs.incidents_prevented
  }
  const chart4Data = threatList.map((threat) => {
    const row: Record<string, unknown> = { threat_category: threat }
    for (const yr of years) {
      row[String(yr)] = csMap[threat]?.[yr] ?? 0
    }
    return row
  })

  // ── Chart 5: Bar — data_volume_tb_per_day by platform_type coloured by vendor ──
  const platformTypes = ['Cloud SCADA', 'Edge Computing', 'Data Lake', 'AI Training Platform', 'Cybersecurity SOC']
  const vendors = ['GE Vernova', 'Siemens', 'ABB', 'Microsoft Azure', 'AWS']
  const dpMap: Record<string, Record<string, number>> = {}
  for (const dp of data.data_platforms) {
    if (!dpMap[dp.platform_type]) dpMap[dp.platform_type] = {}
    dpMap[dp.platform_type][dp.vendor] = (dpMap[dp.platform_type][dp.vendor] ?? 0) + dp.data_volume_tb_per_day
  }
  const chart5Data = platformTypes.map((pt) => {
    const row: Record<string, unknown> = { platform_type: pt }
    for (const v of vendors) {
      row[v] = dpMap[pt]?.[v] ?? 0
    }
    return row
  })

  return (
    <div className="p-6 space-y-8">
      {/* Header */}
      <div className="flex items-center gap-3">
        <Monitor size={28} className="text-blue-600 dark:text-blue-400" />
        <div>
          <h1 className="text-2xl font-bold text-gray-900 dark:text-white">
            Grid Modernisation &amp; Digital Twin Analytics
          </h1>
          <p className="text-sm text-gray-500 dark:text-gray-400">
            Sprint 128a — GMDT | Initiatives, Sensors, Fault Detection, Cyber Security &amp; Data Platforms
          </p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KpiCard
          label="Total Investment"
          value={`$${Number(summary.total_modernisation_investment_b ?? 0).toFixed(2)}B`}
          sub="Across all modernisation initiatives"
        />
        <KpiCard
          label="Avg Reliability Improvement"
          value={`${Number(summary.avg_reliability_improvement_pct ?? 0).toFixed(2)}%`}
          sub="Average across all initiatives"
        />
        <KpiCard
          label="Total Predicted Faults"
          value={String(summary.total_predicted_faults ?? 0)}
          sub="Faults detected before failure (2020–2024)"
        />
        <KpiCard
          label="Cyber Incidents Prevented"
          value={String(summary.cyber_incidents_prevented ?? 0)}
          sub="Across all threat categories &amp; regions"
        />
      </div>

      {/* Chart 1: Bar — investment_m by initiative_name coloured by technology (top 15) */}
      <div className="bg-white dark:bg-gray-800 rounded-xl p-5 shadow">
        <h2 className="text-lg font-semibold text-gray-800 dark:text-white mb-4">
          Chart 1: Top 15 Initiatives by Investment ($M) — Coloured by Technology
        </h2>
        <ResponsiveContainer width="100%" height={360}>
          <BarChart data={chart1Data} layout="vertical" margin={{ top: 5, right: 30, left: 10, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis type="number" unit=" $M" tick={{ fontSize: 11 }} />
            <YAxis type="category" dataKey="name" tick={{ fontSize: 10 }} width={180} />
            <Tooltip formatter={(v: number) => `$${v.toFixed(2)}M`} />
            <Bar dataKey="investment_m" name="Investment ($M)">
              {chart1Data.map((entry) => (
                <Cell key={entry.name} fill={TECH_COLOURS[entry.technology] ?? '#6b7280'} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
        <div className="flex flex-wrap gap-3 mt-3">
          {Object.entries(TECH_COLOURS).map(([tech, colour]) => (
            <span key={tech} className="flex items-center gap-1 text-xs text-gray-600 dark:text-gray-300">
              <span className="inline-block w-3 h-3 rounded-sm" style={{ backgroundColor: colour }} />
              {tech}
            </span>
          ))}
        </div>
      </div>

      {/* Chart 2: Bar — deployed_count_k by sensor_type sorted desc */}
      <div className="bg-white dark:bg-gray-800 rounded-xl p-5 shadow">
        <h2 className="text-lg font-semibold text-gray-800 dark:text-white mb-4">
          Chart 2: Total Deployed Sensors by Type (000s) — Sorted Descending
        </h2>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={chart2Data} margin={{ top: 10, right: 20, left: 10, bottom: 40 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="sensor_type" tick={{ fontSize: 11, angle: -25, textAnchor: 'end' } as any} />
            <YAxis unit="k" tick={{ fontSize: 11 }} />
            <Tooltip formatter={(v: number) => `${v.toLocaleString()}k units`} />
            <Bar dataKey="deployed_count_k" name="Deployed Count (000s)" fill="#3b82f6" />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 3: Line — prediction_accuracy_pct by year for 5 network businesses */}
      <div className="bg-white dark:bg-gray-800 rounded-xl p-5 shadow">
        <h2 className="text-lg font-semibold text-gray-800 dark:text-white mb-4">
          Chart 3: Fault Prediction Accuracy (%) by Year — Per Network Business
        </h2>
        <ResponsiveContainer width="100%" height={320}>
          <LineChart data={chart3Data} margin={{ top: 10, right: 20, left: 10, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="year" tick={{ fontSize: 11 }} />
            <YAxis unit="%" domain={[50, 100]} tick={{ fontSize: 11 }} />
            <Tooltip formatter={(v: number) => `${v.toFixed(2)}%`} />
            <Legend />
            {nbList.map((nb) => (
              <Line
                key={nb}
                type="monotone"
                dataKey={nb}
                stroke={NB_COLOURS[nb] ?? '#6b7280'}
                strokeWidth={2}
                dot={{ r: 4 }}
                connectNulls
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 4: Bar — incidents_prevented by threat_category coloured by year (stacked) */}
      <div className="bg-white dark:bg-gray-800 rounded-xl p-5 shadow">
        <h2 className="text-lg font-semibold text-gray-800 dark:text-white mb-4">
          Chart 4: Cyber Incidents Prevented by Threat Category — Stacked by Year
        </h2>
        <ResponsiveContainer width="100%" height={320}>
          <BarChart data={chart4Data} margin={{ top: 10, right: 20, left: 10, bottom: 40 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="threat_category" tick={{ fontSize: 11, angle: -20, textAnchor: 'end' } as any} />
            <YAxis tick={{ fontSize: 11 }} />
            <Tooltip />
            <Legend />
            {years.map((yr) => (
              <Bar key={yr} dataKey={String(yr)} stackId="a" fill={YEAR_COLOURS[yr] ?? '#6b7280'} name={String(yr)} />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 5: Bar — data_volume_tb_per_day by platform_type coloured by vendor */}
      <div className="bg-white dark:bg-gray-800 rounded-xl p-5 shadow">
        <h2 className="text-lg font-semibold text-gray-800 dark:text-white mb-4">
          Chart 5: Data Volume (TB/day) by Platform Type — Coloured by Vendor
        </h2>
        <ResponsiveContainer width="100%" height={320}>
          <BarChart data={chart5Data} margin={{ top: 10, right: 20, left: 10, bottom: 40 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="platform_type" tick={{ fontSize: 11, angle: -20, textAnchor: 'end' } as any} />
            <YAxis unit=" TB" tick={{ fontSize: 11 }} />
            <Tooltip formatter={(v: number) => `${v.toFixed(3)} TB/day`} />
            <Legend />
            {vendors.map((v) => (
              <Bar key={v} dataKey={v} fill={VENDOR_COLOURS[v] ?? '#6b7280'} name={v} />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Summary Detail */}
      <div className="bg-white dark:bg-gray-800 rounded-xl p-5 shadow">
        <h2 className="text-lg font-semibold text-gray-800 dark:text-white mb-4">
          Summary
        </h2>
        <dl className="grid grid-cols-2 md:grid-cols-3 gap-x-8 gap-y-4">
          <div>
            <dt className="text-xs text-gray-500 dark:text-gray-400">Total Modernisation Investment</dt>
            <dd className="text-base font-semibold text-gray-900 dark:text-white">
              ${Number(summary.total_modernisation_investment_b ?? 0).toFixed(2)}B
            </dd>
          </div>
          <div>
            <dt className="text-xs text-gray-500 dark:text-gray-400">Avg Reliability Improvement</dt>
            <dd className="text-base font-semibold text-gray-900 dark:text-white">
              {Number(summary.avg_reliability_improvement_pct ?? 0).toFixed(2)}%
            </dd>
          </div>
          <div>
            <dt className="text-xs text-gray-500 dark:text-gray-400">Total Predicted Faults (2020–2024)</dt>
            <dd className="text-base font-semibold text-gray-900 dark:text-white">
              {Number(summary.total_predicted_faults ?? 0).toLocaleString()}
            </dd>
          </div>
          <div>
            <dt className="text-xs text-gray-500 dark:text-gray-400">Top Technology</dt>
            <dd className="text-base font-semibold text-gray-900 dark:text-white">
              {String(summary.top_technology ?? '—')}
            </dd>
          </div>
          <div>
            <dt className="text-xs text-gray-500 dark:text-gray-400">Cyber Incidents Prevented</dt>
            <dd className="text-base font-semibold text-gray-900 dark:text-white">
              {Number(summary.cyber_incidents_prevented ?? 0).toLocaleString()}
            </dd>
          </div>
          <div>
            <dt className="text-xs text-gray-500 dark:text-gray-400">Total Initiatives</dt>
            <dd className="text-base font-semibold text-gray-900 dark:text-white">
              {data.initiatives.length}
            </dd>
          </div>
        </dl>
      </div>
    </div>
  )
}
