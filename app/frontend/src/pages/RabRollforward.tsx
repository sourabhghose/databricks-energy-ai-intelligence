// RAB Roll-Forward
import { useEffect, useState } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from 'recharts'
import { Building2, TrendingUp, TrendingDown, DollarSign, type LucideIcon } from 'lucide-react'
import { api } from '../api/client'

interface KpiCardProps {
  label: string; value: string; sub?: string
  Icon: LucideIcon; color: string
}
function KpiCard({ label, value, sub, Icon, color }: KpiCardProps) {
  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5 flex items-start gap-4">
      <div className={`p-2.5 rounded-lg ${color}`}><Icon size={20} className="text-white" /></div>
      <div className="min-w-0">
        <p className="text-xs text-gray-500 dark:text-gray-400 mb-0.5">{label}</p>
        <p className="text-2xl font-bold text-gray-900 dark:text-gray-100">{value}</p>
        {sub && <p className="text-xs text-gray-400 dark:text-gray-500 mt-0.5">{sub}</p>}
      </div>
    </div>
  )
}

const DNSPS = ['AusNet Services', 'Ergon Energy', 'Energex']

export default function RabRollforward() {
  const [selectedDnsp, setSelectedDnsp] = useState('AusNet Services')
  const [summary, setSummary] = useState<Record<string, any>>({})
  const [rollforward, setRollforward] = useState<any[]>([])
  const [waccSensitivity, setWaccSensitivity] = useState<any[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    Promise.all([
      api.getRabSummary({ dnsp: selectedDnsp }),
      api.getRabRollforward({ dnsp: selectedDnsp }),
      api.getRabWaccSensitivity({ dnsp: selectedDnsp }),
    ]).then(([s, r, w]) => {
      setSummary(s ?? {})
      setRollforward(Array.isArray(r?.rollforward) ? r.rollforward : Array.isArray(r) ? r : [])
      setWaccSensitivity(Array.isArray(w?.scenarios) ? w.scenarios : Array.isArray(w) ? w : [])
      setLoading(false)
    }).catch(() => setLoading(false))
  }, [selectedDnsp])

  if (loading) return <div className="p-8 text-gray-500 dark:text-gray-400">Loading...</div>

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900 dark:text-gray-100">RAB Roll-Forward</h1>
          <p className="text-sm text-gray-500 dark:text-gray-400">Regulatory Asset Base movements, capex additions and WACC sensitivity</p>
        </div>
        <div className="flex items-center gap-3">
          <select
            value={selectedDnsp}
            onChange={e => setSelectedDnsp(e.target.value)}
            className="text-xs border border-gray-200 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-300 rounded-lg px-3 py-1.5 focus:outline-none"
          >
            {DNSPS.map(d => <option key={d} value={d}>{d}</option>)}
          </select>
          <span className="text-xs px-2 py-1 rounded-full bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-400">Synthetic</span>
        </div>
      </div>

      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <KpiCard label="Total RAB" value={`$${((summary.total_rab_m_aud ?? 0) / 1000).toFixed(2)}B`} sub="Opening RAB this year" Icon={Building2} color="bg-blue-500" />
        <KpiCard label="YTD Capex Additions" value={`$${(summary.ytd_capex_additions_m_aud ?? 0).toFixed(0)}M`} sub="Net capex added" Icon={TrendingUp} color="bg-green-500" />
        <KpiCard label="Depreciation" value={`$${(summary.depreciation_m_aud ?? 0).toFixed(0)}M`} sub="YTD regulatory depreciation" Icon={TrendingDown} color="bg-orange-500" />
        <KpiCard label="Closing RAB Forecast" value={`$${((summary.closing_rab_forecast_m_aud ?? 0) / 1000).toFixed(2)}B`} sub="End-of-year estimate" Icon={DollarSign} color="bg-purple-500" />
      </div>

      <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5">
        <h2 className="text-sm font-semibold text-gray-800 dark:text-gray-100 mb-4">RAB Roll-Forward — 5 Year Waterfall ($M)</h2>
        {rollforward.length > 0 ? (
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={rollforward} margin={{ bottom: 10 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.3} />
              <XAxis dataKey="year" tick={{ fontSize: 11, fill: '#9CA3AF' }} />
              <YAxis tick={{ fontSize: 11, fill: '#9CA3AF' }} unit=" M" />
              <Tooltip contentStyle={{ backgroundColor: '#1F2937', border: '1px solid #374151', borderRadius: 8 }} labelStyle={{ color: '#F9FAFB' }} itemStyle={{ color: '#D1D5DB' }} />
              <Legend wrapperStyle={{ fontSize: 11, color: '#9CA3AF' }} />
              <Bar dataKey="opening_rab" fill="#6B7280" name="Opening RAB ($M)" radius={[3, 3, 0, 0]} />
              <Bar dataKey="capex_additions" fill="#3B82F6" name="Capex Additions ($M)" radius={[3, 3, 0, 0]} />
              <Bar dataKey="depreciation" fill="#EF4444" name="Depreciation ($M)" radius={[3, 3, 0, 0]} />
              <Bar dataKey="closing_rab" fill="#22C55E" name="Closing RAB ($M)" radius={[3, 3, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        ) : (
          <p className="text-xs text-gray-400 dark:text-gray-500">No roll-forward data available</p>
        )}
      </div>

      <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5">
        <h2 className="text-sm font-semibold text-gray-800 dark:text-gray-100 mb-4">WACC Sensitivity Scenarios</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="text-left text-gray-500 dark:text-gray-400 border-b border-gray-100 dark:border-gray-700">
                <th className="pb-2 pr-4">Scenario</th>
                <th className="pb-2 pr-4">WACC %</th>
                <th className="pb-2 pr-4">Allowed Revenue ($M)</th>
                <th className="pb-2">RAB Impact ($M)</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50 dark:divide-gray-700">
              {waccSensitivity.map((s, i) => (
                <tr key={i} className="text-gray-700 dark:text-gray-300">
                  <td className="py-1.5 pr-4 font-medium">WACC {(s.wacc_pct ?? 0).toFixed(0)}% Scenario</td>
                  <td className="py-1.5 pr-4">{(s.wacc_pct ?? 0).toFixed(2)}%</td>
                  <td className="py-1.5 pr-4">${(s.allowed_revenue_m_aud ?? 0).toFixed(0)}M</td>
                  <td className="py-1.5">{(s.delta_vs_base_m_aud ?? 0) >= 0 ? '+' : ''}{(s.delta_vs_base_m_aud ?? 0).toFixed(0)}M</td>
                </tr>
              ))}
              {waccSensitivity.length === 0 && (
                <tr><td colSpan={4} className="py-4 text-center text-gray-400 dark:text-gray-500">No sensitivity data</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
