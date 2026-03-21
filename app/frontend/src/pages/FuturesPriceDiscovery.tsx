import { useEffect, useState } from 'react'
import {
  LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer, ReferenceLine, Cell,
} from 'recharts'
import { TrendingUp, DollarSign, BarChart2, Activity } from 'lucide-react'
import { getFuturesPriceDiscoveryDashboard, FuturesPriceDiscoveryDashboard } from '../api/client'

const REGION_COLORS: Record<string, string> = {
  NSW: '#3b82f6',
  VIC: '#10b981',
  QLD: '#f59e0b',
  SA:  '#ef4444',
  TAS: '#8b5cf6',
}

const CURVE_SHAPE_STYLES: Record<string, { label: string; bg: string; text: string }> = {
  CONTANGO:      { label: 'Contango',      bg: 'bg-green-900',  text: 'text-green-300'  },
  BACKWARDATION: { label: 'Backwardation', bg: 'bg-red-900',    text: 'text-red-300'    },
  FLAT:          { label: 'Flat',          bg: 'bg-gray-700',   text: 'text-gray-300'   },
  KINKED:        { label: 'Kinked',        bg: 'bg-amber-900',  text: 'text-amber-300'  },
}

export default function FuturesPriceDiscovery() {
  const [data, setData] = useState<FuturesPriceDiscoveryDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    getFuturesPriceDiscoveryDashboard()
      .then(setData)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-gray-400">
        Loading futures price discovery data...
      </div>
    )
  }
  if (error || !data) {
    return (
      <div className="flex items-center justify-center h-64 text-red-400">
        Error: {error ?? 'No data'}
      </div>
    )
  }

  // ---- KPI calculations ----
  const mostLiquid = [...data.term_structures].sort(
    (a, b) => b.open_interest_lots - a.open_interest_lots
  )[0]

  const largestBasis = [...data.basis_records].sort(
    (a, b) => Math.abs(b.basis_aud) - Math.abs(a.basis_aud)
  )[0]

  const steepestSlope = [...data.curve_shapes].sort(
    (a, b) => Math.abs(b.annual_slope_pct) - Math.abs(a.annual_slope_pct)
  )[0]

  const highestVol = [...data.term_structures].sort(
    (a, b) => b.implied_vol_pct - a.implied_vol_pct
  )[0]

  // ---- Term structure forward curve data ----
  const quarters = ['2025-Q1', '2025-Q2', '2025-Q3', '2025-Q4']
  const termStructureChartData = quarters.map(q => {
    const row: Record<string, string | number> = { quarter: q }
    const regions = ['NSW', 'VIC', 'QLD', 'SA', 'TAS']
    regions.forEach(reg => {
      const rec = data.term_structures.find(
        r => r.region === reg && r.contract_month === q
      )
      if (rec) row[reg] = rec.settlement_price_aud_mwh
    })
    return row
  })

  // ---- Basis analysis chart data ----
  const basisMonths = ['Jan', 'Feb', 'Mar', 'Apr']
  const basisRegions = ['NSW', 'VIC', 'QLD', 'SA']
  const basisChartData = basisMonths.map(mo => {
    const row: Record<string, string | number> = { month: mo }
    basisRegions.forEach(reg => {
      const rec = data.basis_records.find(r => r.region === reg && r.month === mo)
      if (rec) {
        row[`${reg}_futures`] = rec.futures_price
        row[`${reg}_spot`] = rec.spot_price
        row[`${reg}_basis`] = rec.basis_aud
      }
    })
    return row
  })

  // ---- Carry cost breakdown ----
  const carryChartData = data.carry_records.map(r => ({
    label: `${r.region} ${r.near_contract.replace('2025-', '')}→${r.far_contract.replace('2025-', '')}`,
    region: r.region,
    storage_premium: r.storage_premium_aud,
    risk_premium: r.risk_premium_aud,
    convenience_yield: r.convenience_yield_pct,
  }))

  // ---- Latest curve shapes (unique regions) ----
  const latestShapes: Record<string, typeof data.curve_shapes[0]> = {}
  data.curve_shapes.forEach(s => {
    if (!latestShapes[s.region] || s.snapshot_date > latestShapes[s.region].snapshot_date) {
      latestShapes[s.region] = s
    }
  })
  const curveShapeRows = Object.values(latestShapes)

  return (
    <div className="p-6 space-y-6 bg-gray-950 min-h-screen text-gray-100">
      {/* Header */}
      <div className="flex items-center gap-3">
        <TrendingUp className="text-blue-400 w-7 h-7" />
        <div>
          <h1 className="text-2xl font-bold text-white">
            ASX Energy Futures — Price Discovery &amp; Term Structure Analytics
          </h1>
          <p className="text-gray-400 text-sm mt-0.5">
            Forward curve, basis, carry costs and curve shape across NEM regions
          </p>
        </div>
      </div>

      {/* KPI cards */}
      <div className="grid grid-cols-2 xl:grid-cols-4 gap-4">
        {/* Most liquid */}
        <div className="bg-gray-900 rounded-xl p-4 border border-gray-800">
          <div className="flex items-center gap-2 mb-2">
            <Activity className="text-blue-400 w-4 h-4" />
            <span className="text-xs text-gray-400 uppercase tracking-wide">Most Liquid Contract</span>
          </div>
          <p className="text-2xl font-bold text-blue-300">
            {mostLiquid.region} {mostLiquid.contract_month}
          </p>
          <p className="text-gray-400 text-sm">
            {mostLiquid.open_interest_lots.toLocaleString()} lots OI
          </p>
          <p className="text-gray-500 text-xs mt-1">{mostLiquid.product} product</p>
        </div>

        {/* Largest basis */}
        <div className="bg-gray-900 rounded-xl p-4 border border-gray-800">
          <div className="flex items-center gap-2 mb-2">
            <DollarSign className="text-amber-400 w-4 h-4" />
            <span className="text-xs text-gray-400 uppercase tracking-wide">Largest Basis</span>
          </div>
          <p className="text-2xl font-bold text-amber-300">
            ${Math.abs(largestBasis.basis_aud).toFixed(2)}/MWh
          </p>
          <p className="text-gray-400 text-sm">
            {largestBasis.region} — {largestBasis.month}
          </p>
          <p className="text-gray-500 text-xs mt-1">{largestBasis.convergence_trend}</p>
        </div>

        {/* Steepest curve slope */}
        <div className="bg-gray-900 rounded-xl p-4 border border-gray-800">
          <div className="flex items-center gap-2 mb-2">
            <TrendingUp className="text-green-400 w-4 h-4" />
            <span className="text-xs text-gray-400 uppercase tracking-wide">Steepest Curve Slope</span>
          </div>
          <p className="text-2xl font-bold text-green-300">
            {steepestSlope.annual_slope_pct.toFixed(1)}%
          </p>
          <p className="text-gray-400 text-sm">{steepestSlope.region} annual slope</p>
          <p className="text-gray-500 text-xs mt-1">
            Inflection: {steepestSlope.inflection_quarter}
          </p>
        </div>

        {/* Highest implied vol */}
        <div className="bg-gray-900 rounded-xl p-4 border border-gray-800">
          <div className="flex items-center gap-2 mb-2">
            <BarChart2 className="text-purple-400 w-4 h-4" />
            <span className="text-xs text-gray-400 uppercase tracking-wide">Highest Implied Vol</span>
          </div>
          <p className="text-2xl font-bold text-purple-300">
            {highestVol.implied_vol_pct.toFixed(1)}%
          </p>
          <p className="text-gray-400 text-sm">
            {highestVol.region} {highestVol.contract_month}
          </p>
          <p className="text-gray-500 text-xs mt-1">{highestVol.days_to_expiry}d to expiry</p>
        </div>
      </div>

      {/* Term structure forward curve */}
      <div className="bg-gray-900 rounded-xl p-5 border border-gray-800">
        <h2 className="text-lg font-semibold text-white mb-4">
          Forward Curve by Region — Quarterly Term Structure (BASE, AUD/MWh)
        </h2>
        <ResponsiveContainer width="100%" height={320}>
          <LineChart data={termStructureChartData} margin={{ top: 10, right: 20, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="quarter" stroke="#9ca3af" tick={{ fill: '#9ca3af', fontSize: 12 }} />
            <YAxis
              stroke="#9ca3af"
              tick={{ fill: '#9ca3af', fontSize: 12 }}
              domain={['auto', 'auto']}
              tickFormatter={v => `$${v}`}
            />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
              labelStyle={{ color: '#e5e7eb' }}
              formatter={(v: number) => [`$${v.toFixed(2)}/MWh`]}
            />
            <Legend wrapperStyle={{ color: '#9ca3af' }} />
            {['NSW', 'VIC', 'QLD', 'SA', 'TAS'].map(reg => (
              <Line
                key={reg}
                type="monotone"
                dataKey={reg}
                stroke={REGION_COLORS[reg]}
                strokeWidth={2}
                dot={{ r: 4, fill: REGION_COLORS[reg] }}
                activeDot={{ r: 6 }}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Basis analysis */}
      <div className="bg-gray-900 rounded-xl p-5 border border-gray-800">
        <h2 className="text-lg font-semibold text-white mb-1">
          Basis Analysis — Futures vs Spot by Region (AUD/MWh)
        </h2>
        <p className="text-gray-400 text-xs mb-4">
          Solid lines = futures price. Dashed reference lines show approximate spot level.
        </p>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={basisChartData} margin={{ top: 10, right: 20, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="month" stroke="#9ca3af" tick={{ fill: '#9ca3af', fontSize: 12 }} />
            <YAxis
              stroke="#9ca3af"
              tick={{ fill: '#9ca3af', fontSize: 12 }}
              domain={['auto', 'auto']}
              tickFormatter={v => `$${v}`}
            />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
              labelStyle={{ color: '#e5e7eb' }}
              formatter={(v: number, name: string) => [`$${v.toFixed(2)}/MWh`, name]}
            />
            <Legend wrapperStyle={{ color: '#9ca3af' }} />
            {basisRegions.map(reg => (
              <Line
                key={`${reg}_futures`}
                type="monotone"
                dataKey={`${reg}_futures`}
                name={`${reg} Futures`}
                stroke={REGION_COLORS[reg]}
                strokeWidth={2}
                dot={{ r: 3 }}
              />
            ))}
            {/* spot reference lines */}
            <ReferenceLine y={89.5} stroke={REGION_COLORS['NSW']} strokeDasharray="4 4" label={{ value: 'NSW Spot', fill: REGION_COLORS['NSW'], fontSize: 10 }} />
            <ReferenceLine y={84.2} stroke={REGION_COLORS['VIC']} strokeDasharray="4 4" label={{ value: 'VIC Spot', fill: REGION_COLORS['VIC'], fontSize: 10 }} />
            <ReferenceLine y={93.0} stroke={REGION_COLORS['QLD']} strokeDasharray="4 4" label={{ value: 'QLD Spot', fill: REGION_COLORS['QLD'], fontSize: 10 }} />
            <ReferenceLine y={102.5} stroke={REGION_COLORS['SA']} strokeDasharray="4 4" label={{ value: 'SA Spot', fill: REGION_COLORS['SA'], fontSize: 10 }} />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Carry cost breakdown */}
      <div className="bg-gray-900 rounded-xl p-5 border border-gray-800">
        <h2 className="text-lg font-semibold text-white mb-1">
          Carry Cost Breakdown — Storage Premium vs Risk Premium (AUD/MWh)
        </h2>
        <p className="text-gray-400 text-xs mb-4">
          Near-to-far quarterly spread decomposed into storage premium, risk premium, and convenience yield.
        </p>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={carryChartData} margin={{ top: 10, right: 20, left: 0, bottom: 60 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis
              dataKey="label"
              stroke="#9ca3af"
              tick={{ fill: '#9ca3af', fontSize: 10, angle: -35, textAnchor: 'end' } as any}
            />
            <YAxis stroke="#9ca3af" tick={{ fill: '#9ca3af', fontSize: 12 }} tickFormatter={v => `$${v}`} />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: 8 }}
              labelStyle={{ color: '#e5e7eb' }}
              formatter={(v: number, name: string) => [`${v.toFixed(2)}`, name]}
            />
            <Legend wrapperStyle={{ color: '#9ca3af' }} />
            <Bar dataKey="storage_premium" name="Storage Premium (AUD)" fill="#3b82f6" radius={[3, 3, 0, 0]}>
              {carryChartData.map((entry, index) => (
                <Cell key={`sp-${index}`} fill={REGION_COLORS[entry.region] ?? '#3b82f6'} opacity={0.85} />
              ))}
            </Bar>
            <Bar dataKey="risk_premium" name="Risk Premium (AUD)" fill="#10b981" radius={[3, 3, 0, 0]} opacity={0.7} />
            <Bar dataKey="convenience_yield" name="Conv. Yield (%)" fill="#f59e0b" radius={[3, 3, 0, 0]} opacity={0.6} />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Curve shape summary table */}
      <div className="bg-gray-900 rounded-xl p-5 border border-gray-800">
        <h2 className="text-lg font-semibold text-white mb-4">
          Forward Curve Shape Summary
        </h2>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-700">
                <th className="text-left py-2 px-3 text-gray-400 font-medium">Region</th>
                <th className="text-left py-2 px-3 text-gray-400 font-medium">Shape</th>
                <th className="text-right py-2 px-3 text-gray-400 font-medium">Q1 (AUD/MWh)</th>
                <th className="text-right py-2 px-3 text-gray-400 font-medium">Q2 (AUD/MWh)</th>
                <th className="text-right py-2 px-3 text-gray-400 font-medium">Q3 (AUD/MWh)</th>
                <th className="text-right py-2 px-3 text-gray-400 font-medium">Q4 (AUD/MWh)</th>
                <th className="text-right py-2 px-3 text-gray-400 font-medium">Annual Slope</th>
                <th className="text-right py-2 px-3 text-gray-400 font-medium">Inflection</th>
              </tr>
            </thead>
            <tbody>
              {curveShapeRows.map(row => {
                const shapeStyle = CURVE_SHAPE_STYLES[row.curve_shape] ?? CURVE_SHAPE_STYLES['FLAT']
                return (
                  <tr key={row.region} className="border-b border-gray-800 hover:bg-gray-800/50 transition-colors">
                    <td className="py-3 px-3">
                      <div className="flex items-center gap-2">
                        <div
                          className="w-2.5 h-2.5 rounded-full"
                          style={{ backgroundColor: REGION_COLORS[row.region] ?? '#9ca3af' }}
                        />
                        <span className="font-semibold text-white">{row.region}</span>
                      </div>
                    </td>
                    <td className="py-3 px-3">
                      <span
                        className={`px-2 py-0.5 rounded text-xs font-semibold ${shapeStyle.bg} ${shapeStyle.text}`}
                      >
                        {shapeStyle.label}
                      </span>
                    </td>
                    <td className="py-3 px-3 text-right text-gray-200">${row.q1_price.toFixed(2)}</td>
                    <td className="py-3 px-3 text-right text-gray-200">${row.q2_price.toFixed(2)}</td>
                    <td className="py-3 px-3 text-right text-gray-200">${row.q3_price.toFixed(2)}</td>
                    <td className="py-3 px-3 text-right text-gray-200">${row.q4_price.toFixed(2)}</td>
                    <td className="py-3 px-3 text-right">
                      <span
                        className={
                          row.annual_slope_pct >= 0 ? 'text-green-400' : 'text-red-400'
                        }
                      >
                        {row.annual_slope_pct >= 0 ? '+' : ''}{row.annual_slope_pct.toFixed(1)}%
                      </span>
                    </td>
                    <td className="py-3 px-3 text-right text-gray-400">{row.inflection_quarter}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
