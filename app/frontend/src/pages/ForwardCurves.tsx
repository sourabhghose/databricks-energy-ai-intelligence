import { useEffect, useState } from 'react'
import { TrendingUp, RefreshCw, Download } from 'lucide-react'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import { curvesApi } from '../api/client'
import type { ForwardCurvePoint } from '../api/client'

const NEM_REGIONS = ['NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1'] as const
const PROFILES = ['FLAT', 'PEAK', 'OFF_PEAK'] as const
const REGION_COLOURS: Record<string, string> = {
  NSW1: '#3b82f6',
  QLD1: '#f59e0b',
  VIC1: '#14b8a6',
  SA1: '#ef4444',
  TAS1: '#8b5cf6',
}

export default function ForwardCurves() {
  const [region, setRegion] = useState<string>('NSW1')
  const [profile, setProfile] = useState<string>('FLAT')
  const [termPoints, setTermPoints] = useState<ForwardCurvePoint[]>([])
  const [compareData, setCompareData] = useState<Record<string, ForwardCurvePoint[]>>({})
  const [historyData, setHistoryData] = useState<Array<{ curve_date: string; points: ForwardCurvePoint[] }>>([])
  const [loading, setLoading] = useState(true)
  const [snapshotMsg, setSnapshotMsg] = useState<string | null>(null)

  const fetchData = async () => {
    setLoading(true)
    try {
      const [term, compare, history] = await Promise.all([
        curvesApi.getTermStructure(region, profile),
        curvesApi.getComparison(NEM_REGIONS as unknown as string[], profile),
        curvesApi.getHistory(region, profile, 5),
      ])
      setTermPoints(term.points || [])
      setCompareData(compare.curves || {})
      setHistoryData(history.snapshots || [])
    } catch (e) {
      console.error('Failed to fetch curve data:', e)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { fetchData() }, [region, profile])

  const handleSnapshot = async () => {
    try {
      setSnapshotMsg('Saving...')
      const res = await curvesApi.createSnapshot()
      setSnapshotMsg(`Saved ${res.rows_inserted} rows`)
      setTimeout(() => setSnapshotMsg(null), 3000)
    } catch {
      setSnapshotMsg('Snapshot failed')
      setTimeout(() => setSnapshotMsg(null), 3000)
    }
  }

  // Term structure chart data
  const termChartData = termPoints.map(p => ({
    month: p.month,
    price: p.price_mwh,
    source: p.source,
  }))

  // Comparison chart data — merge all regions into one array
  const comparisonData: Record<string, any>[] = []
  const months = termPoints.map(p => p.month)
  months.forEach((month, i) => {
    const row: Record<string, any> = { month }
    for (const r of NEM_REGIONS) {
      const pts = compareData[r]
      if (pts && pts[i]) {
        row[r] = pts[i].price_mwh
      }
    }
    comparisonData.push(row)
  })

  // History chart data — overlay curves from different dates
  const historyChartData: Record<string, any>[] = []
  if (historyData.length > 0 && historyData[0].points.length > 0) {
    historyData[0].points.forEach((pt, i) => {
      const row: Record<string, any> = { month: pt.month }
      historyData.forEach((snap, si) => {
        if (snap.points[i]) {
          row[snap.curve_date] = snap.points[i].price_mwh
        }
      })
      historyChartData.push(row)
    })
  }

  const historyColors = ['#3b82f6', '#f59e0b', '#14b8a6', '#ef4444', '#8b5cf6']

  // KPIs from term structure
  const avgPrice = termPoints.length > 0
    ? (termPoints.reduce((s, p) => s + p.price_mwh, 0) / termPoints.length)
    : 0
  const maxPrice = termPoints.length > 0
    ? Math.max(...termPoints.map(p => p.price_mwh))
    : 0
  const minPrice = termPoints.length > 0
    ? Math.min(...termPoints.map(p => p.price_mwh))
    : 0

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500" />
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Header + Controls */}
      <div className="flex items-center justify-between flex-wrap gap-4">
        <div className="flex items-center gap-3">
          <TrendingUp className="w-6 h-6 text-blue-400" />
          <h1 className="text-xl font-bold text-white">Forward Curve Construction</h1>
        </div>
        <div className="flex items-center gap-3">
          <select
            value={region}
            onChange={e => setRegion(e.target.value)}
            className="text-sm bg-gray-700 text-gray-200 border border-gray-600 rounded px-3 py-1.5"
          >
            {NEM_REGIONS.map(r => <option key={r} value={r}>{r}</option>)}
          </select>
          <select
            value={profile}
            onChange={e => setProfile(e.target.value)}
            className="text-sm bg-gray-700 text-gray-200 border border-gray-600 rounded px-3 py-1.5"
          >
            {PROFILES.map(p => <option key={p} value={p}>{p}</option>)}
          </select>
          <button
            onClick={fetchData}
            className="flex items-center gap-1.5 text-sm bg-gray-700 hover:bg-gray-600 text-gray-200 border border-gray-600 rounded px-3 py-1.5"
          >
            <RefreshCw className="w-3.5 h-3.5" /> Refresh
          </button>
          <button
            onClick={handleSnapshot}
            className="flex items-center gap-1.5 text-sm bg-blue-600 hover:bg-blue-500 text-white rounded px-3 py-1.5"
          >
            <Download className="w-3.5 h-3.5" /> Save Snapshot
          </button>
          {snapshotMsg && <span className="text-xs text-green-400">{snapshotMsg}</span>}
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-3 gap-4">
        <div className="bg-gray-800 rounded-lg p-4">
          <span className="text-xs text-gray-400 uppercase tracking-wide">Avg Forward Price</span>
          <div className="text-2xl font-bold text-white">${avgPrice.toFixed(2)}<span className="text-sm text-gray-400">/MWh</span></div>
          <span className="text-xs text-gray-500">{profile} profile, {region}</span>
        </div>
        <div className="bg-gray-800 rounded-lg p-4">
          <span className="text-xs text-gray-400 uppercase tracking-wide">Max Monthly Price</span>
          <div className="text-2xl font-bold text-red-400">${maxPrice.toFixed(2)}<span className="text-sm text-gray-400">/MWh</span></div>
        </div>
        <div className="bg-gray-800 rounded-lg p-4">
          <span className="text-xs text-gray-400 uppercase tracking-wide">Min Monthly Price</span>
          <div className="text-2xl font-bold text-green-400">${minPrice.toFixed(2)}<span className="text-sm text-gray-400">/MWh</span></div>
        </div>
      </div>

      {/* Term Structure Chart */}
      <div className="bg-gray-800 rounded-lg p-4">
        <h2 className="text-sm font-semibold text-gray-200 uppercase tracking-wide mb-4">
          Term Structure — {region} {profile}
        </h2>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={termChartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="month" tick={{ fill: '#9ca3af', fontSize: 11 }} />
            <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} label={{ value: '$/MWh', angle: -90, position: 'insideLeft', fill: '#9ca3af' }} />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
              labelStyle={{ color: '#d1d5db' }}
              formatter={(value: number) => [`$${value.toFixed(2)}/MWh`, 'Price']}
            />
            <Line type="monotone" dataKey="price" stroke="#3b82f6" strokeWidth={2} dot={{ r: 3 }} activeDot={{ r: 5 }} />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Regional Comparison Chart */}
      <div className="bg-gray-800 rounded-lg p-4">
        <h2 className="text-sm font-semibold text-gray-200 uppercase tracking-wide mb-4">
          Regional Comparison — {profile}
        </h2>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={comparisonData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="month" tick={{ fill: '#9ca3af', fontSize: 11 }} />
            <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
              labelStyle={{ color: '#d1d5db' }}
              formatter={(value: number) => [`$${value.toFixed(2)}/MWh`]}
            />
            <Legend />
            {NEM_REGIONS.map(r => (
              <Line key={r} type="monotone" dataKey={r} stroke={REGION_COLOURS[r]} strokeWidth={2} dot={false} />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Curve History Chart */}
      {historyChartData.length > 0 && (
        <div className="bg-gray-800 rounded-lg p-4">
          <h2 className="text-sm font-semibold text-gray-200 uppercase tracking-wide mb-4">
            Curve History — {region} {profile}
          </h2>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={historyChartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="month" tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
                labelStyle={{ color: '#d1d5db' }}
                formatter={(value: number) => [`$${value.toFixed(2)}/MWh`]}
              />
              <Legend />
              {historyData.map((snap, i) => (
                <Line
                  key={snap.curve_date}
                  type="monotone"
                  dataKey={snap.curve_date}
                  stroke={historyColors[i % historyColors.length]}
                  strokeWidth={i === 0 ? 2 : 1}
                  strokeDasharray={i === 0 ? undefined : '5 5'}
                  dot={false}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Data Table */}
      <div className="bg-gray-800 rounded-lg p-4">
        <h2 className="text-sm font-semibold text-gray-200 uppercase tracking-wide mb-4">
          Forward Curve Data
        </h2>
        <div className="overflow-x-auto">
          <table className="w-full text-xs text-gray-300">
            <thead>
              <tr className="border-b border-gray-700">
                <th className="text-left py-2 pr-3 text-gray-400 font-medium">Month</th>
                <th className="text-right py-2 pr-3 text-gray-400 font-medium">Price ($/MWh)</th>
                <th className="text-left py-2 pr-3 text-gray-400 font-medium">Quarter</th>
                <th className="text-left py-2 text-gray-400 font-medium">Source</th>
              </tr>
            </thead>
            <tbody>
              {termPoints.map(pt => (
                <tr key={pt.month} className="border-b border-gray-700/50 hover:bg-gray-750">
                  <td className="py-1.5 pr-3 font-mono">{pt.month}</td>
                  <td className="py-1.5 pr-3 text-right font-mono">${pt.price_mwh.toFixed(2)}</td>
                  <td className="py-1.5 pr-3">{pt.quarter}</td>
                  <td className="py-1.5">
                    <span className={`inline-block px-1.5 py-0.5 rounded text-[10px] font-semibold ${
                      pt.source === 'SHAPED' || pt.source === 'ASX_BOOTSTRAP'
                        ? 'bg-blue-900 text-blue-300'
                        : pt.source === 'EXTRAPOLATED'
                        ? 'bg-yellow-900 text-yellow-300'
                        : 'bg-gray-700 text-gray-400'
                    }`}>
                      {pt.source}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
