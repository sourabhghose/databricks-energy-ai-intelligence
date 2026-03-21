import { useState, useEffect, useCallback } from 'react'
import { AlertTriangle, Activity, TrendingDown, DollarSign, ChevronUp, ChevronDown, RefreshCw } from 'lucide-react'
import { networkAssetsApi, NetworkAsset, AssetHealth, FailurePrediction } from '../api/client'

type SortField = 'health_index' | 'failure_probability_12m' | 'replacement_cost_aud'
type SortDir = 'asc' | 'desc'

function healthColor(index: number): string {
  if (index < 30) return 'bg-red-500'
  if (index < 60) return 'bg-amber-500'
  return 'bg-green-500'
}

function healthTextColor(index: number): string {
  if (index < 30) return 'text-red-400'
  if (index < 60) return 'text-amber-400'
  return 'text-green-400'
}

function formatCostM(aud: number): string {
  return `$${(aud / 1_000_000).toFixed(1)}M`
}

function formatPct(p: number): string {
  return `${(p * 100).toFixed(1)}%`
}

function parseRiskFactors(val: unknown): string[] {
  if (Array.isArray(val)) return val
  if (typeof val === 'string') {
    try { const parsed = JSON.parse(val.replace(/'/g, '"')); if (Array.isArray(parsed)) return parsed } catch { /* ignore */ }
    return val.split(',').map(s => s.trim()).filter(Boolean)
  }
  return []
}

interface HealthBar {
  value: number
}

function HealthBar({ value }: HealthBar) {
  return (
    <div className="flex items-center gap-2">
      <div className="flex-1 bg-gray-700 rounded-full h-2">
        <div
          className={`h-2 rounded-full ${healthColor(value)}`}
          style={{ width: `${Math.min(100, Math.max(0, value))}%` }}
        />
      </div>
      <span className={`text-xs font-medium w-8 text-right ${healthTextColor(value)}`}>
        {value.toFixed(0)}
      </span>
    </div>
  )
}

interface SortHeaderProps {
  label: string
  field: SortField
  currentField: SortField
  currentDir: SortDir
  onSort: (f: SortField) => void
}

function SortHeader({ label, field, currentField, currentDir, onSort }: SortHeaderProps) {
  const active = currentField === field
  return (
    <th
      className="px-4 py-3 text-left text-xs font-medium text-gray-400 uppercase tracking-wider cursor-pointer hover:text-gray-200 select-none"
      onClick={() => onSort(field)}
    >
      <div className="flex items-center gap-1">
        {label}
        <span className="flex flex-col">
          <ChevronUp
            size={10}
            className={active && currentDir === 'asc' ? 'text-blue-400' : 'text-gray-600'}
          />
          <ChevronDown
            size={10}
            className={active && currentDir === 'desc' ? 'text-blue-400' : 'text-gray-600'}
          />
        </span>
      </div>
    </th>
  )
}

// ── Tab 1: Health Ranking ────────────────────────────────────────────────────

function HealthRankingTab() {
  const [data, setData] = useState<AssetHealth[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [sortField, setSortField] = useState<SortField>('health_index')
  const [sortDir, setSortDir] = useState<SortDir>('asc')

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await networkAssetsApi.healthRanking()
      setData(res.ranking ?? [])
    } catch (e: any) {
      setError(e?.message ?? 'Failed to load health ranking')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { load() }, [load])

  function handleSort(field: SortField) {
    if (sortField === field) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortField(field)
      setSortDir('asc')
    }
  }

  const sorted = [...data].sort((a, b) => {
    const av = (a as any)[sortField] ?? 0
    const bv = (b as any)[sortField] ?? 0
    return sortDir === 'asc' ? av - bv : bv - av
  })

  if (loading) {
    return (
      <div className="flex items-center justify-center h-48 text-gray-400">
        <RefreshCw size={20} className="animate-spin mr-2" /> Loading health ranking…
      </div>
    )
  }

  if (error) {
    return (
      <div className="bg-red-900/30 border border-red-700 rounded-lg p-4 text-red-400 text-sm">
        {error}
      </div>
    )
  }

  return (
    <div className="bg-gray-800 rounded-lg border border-gray-700 overflow-hidden">
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-700">
          <thead className="bg-gray-900">
            <tr>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-400 uppercase tracking-wider">Asset</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-400 uppercase tracking-wider">Type</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-400 uppercase tracking-wider">Region</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-400 uppercase tracking-wider">DNSP</th>
              <SortHeader label="Health Index" field="health_index" currentField={sortField} currentDir={sortDir} onSort={handleSort} />
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-400 uppercase tracking-wider">Risk Factors</th>
              <SortHeader label="Replacement Cost" field="replacement_cost_aud" currentField={sortField} currentDir={sortDir} onSort={handleSort} />
              <SortHeader label="Fail Prob 12m" field="failure_probability_12m" currentField={sortField} currentDir={sortDir} onSort={handleSort} />
            </tr>
          </thead>
          <tbody className="bg-gray-800 divide-y divide-gray-700">
            {sorted.length === 0 && (
              <tr>
                <td colSpan={8} className="px-4 py-8 text-center text-gray-500 text-sm">No data available</td>
              </tr>
            )}
            {sorted.map((asset, i) => (
              <tr key={asset.asset_id ?? i} className="hover:bg-gray-750 transition-colors">
                <td className="px-4 py-3 text-sm font-medium text-gray-100 whitespace-nowrap">{asset.asset_name}</td>
                <td className="px-4 py-3 text-sm text-gray-300 whitespace-nowrap">{asset.asset_type}</td>
                <td className="px-4 py-3 text-sm text-gray-300 whitespace-nowrap">{asset.region}</td>
                <td className="px-4 py-3 text-sm text-gray-300 whitespace-nowrap">{asset.dnsp}</td>
                <td className="px-4 py-3 min-w-[140px]">
                  <HealthBar value={asset.health_index ?? 0} />
                </td>
                <td className="px-4 py-3 text-sm text-gray-400 max-w-[200px]">
                  <div className="flex flex-wrap gap-1">
                    {(Array.isArray(asset.risk_factors) ? asset.risk_factors : parseRiskFactors(asset.risk_factors)).map((rf: string, ri: number) => (
                      <span key={ri} className="bg-gray-700 text-gray-300 text-xs px-2 py-0.5 rounded-full">{rf}</span>
                    ))}
                  </div>
                </td>
                <td className="px-4 py-3 text-sm text-gray-100 font-mono whitespace-nowrap">
                  {formatCostM(asset.replacement_cost_aud ?? 0)}
                </td>
                <td className={`px-4 py-3 text-sm font-medium whitespace-nowrap ${healthTextColor(100 - (asset.failure_probability_12m ?? 0) * 100)}`}>
                  {formatPct(asset.failure_probability_12m ?? 0)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── Tab 2: Failure Risk Panel ────────────────────────────────────────────────

function FailureRiskTab() {
  const [data, setData] = useState<FailurePrediction[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await networkAssetsApi.failureRisk()
      setData(res.high_risk_assets ?? [])
    } catch (e: any) {
      setError(e?.message ?? 'Failed to load failure risk data')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { load() }, [load])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-48 text-gray-400">
        <RefreshCw size={20} className="animate-spin mr-2" /> Loading failure risk…
      </div>
    )
  }

  if (error) {
    return (
      <div className="bg-red-900/30 border border-red-700 rounded-lg p-4 text-red-400 text-sm">
        {error}
      </div>
    )
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2 text-amber-400 text-sm font-medium">
        <AlertTriangle size={16} />
        <span>{data.length} asset{data.length !== 1 ? 's' : ''} with &gt;50% failure probability in next 12 months</span>
      </div>
      {data.length === 0 && (
        <div className="bg-gray-800 rounded-lg border border-gray-700 p-8 text-center text-gray-500 text-sm">
          No high-risk assets detected
        </div>
      )}
      <div className="grid gap-4">
        {data.map((asset, i) => {
          const pct = (asset.failure_probability_12m ?? 0) * 100
          const severity = pct >= 80 ? 'red' : 'amber'
          return (
            <div
              key={asset.asset_id ?? i}
              className={`bg-gray-800 rounded-lg border p-4 ${severity === 'red' ? 'border-red-700' : 'border-amber-700'}`}
            >
              <div className="flex items-start justify-between gap-4">
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 flex-wrap">
                    <span className="text-gray-100 font-semibold">{asset.asset_name}</span>
                    <span
                      className={`text-xs font-bold px-2 py-0.5 rounded-full ${
                        severity === 'red'
                          ? 'bg-red-900/60 text-red-300 border border-red-700'
                          : 'bg-amber-900/60 text-amber-300 border border-amber-700'
                      }`}
                    >
                      {pct.toFixed(1)}% FAIL RISK
                    </span>
                  </div>
                  <div className="mt-1 flex flex-wrap gap-x-4 gap-y-1 text-sm text-gray-400">
                    <span>{asset.asset_type}</span>
                    <span>{asset.region}</span>
                    {asset.dnsp && <span>{asset.dnsp}</span>}
                    {asset.age_years != null && <span>Age: {asset.age_years} yrs</span>}
                    {asset.replacement_cost_aud != null && (
                      <span className="text-gray-300">Replacement: {formatCostM(asset.replacement_cost_aud)}</span>
                    )}
                  </div>
                  {asset.predicted_failure_date && (
                    <div className="mt-1 text-xs text-red-400">
                      Predicted failure: {asset.predicted_failure_date}
                    </div>
                  )}
                  {(() => { const rf = Array.isArray(asset.risk_factors) ? asset.risk_factors : parseRiskFactors(asset.risk_factors); return rf.length > 0 ? (
                    <div className="mt-2 flex flex-wrap gap-1">
                      {rf.map((r: string, ri: number) => (
                        <span key={ri} className="bg-gray-700 text-gray-300 text-xs px-2 py-0.5 rounded-full">{r}</span>
                      ))}
                    </div>
                  ) : null })()}
                </div>
                <div className="flex-shrink-0 text-right">
                  <div className="w-16 h-16 relative flex items-center justify-center">
                    <svg className="w-16 h-16 -rotate-90" viewBox="0 0 64 64">
                      <circle cx="32" cy="32" r="28" fill="none" stroke="#374151" strokeWidth="6" />
                      <circle
                        cx="32" cy="32" r="28"
                        fill="none"
                        stroke={severity === 'red' ? '#ef4444' : '#f59e0b'}
                        strokeWidth="6"
                        strokeDasharray={`${2 * Math.PI * 28 * pct / 100} ${2 * Math.PI * 28}`}
                      />
                    </svg>
                    <span className={`absolute text-xs font-bold ${severity === 'red' ? 'text-red-400' : 'text-amber-400'}`}>
                      {pct.toFixed(0)}%
                    </span>
                  </div>
                </div>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}

// ── Tab 3: Condition Trends ──────────────────────────────────────────────────

interface ConditionTrend {
  asset_type: string
  avg_health: number
  min_health: number
  max_health: number
  asset_count: number
}

function ConditionTrendsTab() {
  const [data, setData] = useState<ConditionTrend[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await networkAssetsApi.conditionTrends()
      setData(res.trends ?? [])
    } catch (e: any) {
      setError(e?.message ?? 'Failed to load condition trends')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { load() }, [load])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-48 text-gray-400">
        <RefreshCw size={20} className="animate-spin mr-2" /> Loading condition trends…
      </div>
    )
  }

  if (error) {
    return (
      <div className="bg-red-900/30 border border-red-700 rounded-lg p-4 text-red-400 text-sm">
        {error}
      </div>
    )
  }

  return (
    <div className="space-y-4">
      {data.length === 0 && (
        <div className="bg-gray-800 rounded-lg border border-gray-700 p-8 text-center text-gray-500 text-sm">
          No condition trend data available
        </div>
      )}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
        {data.map((trend, i) => (
          <div key={i} className="bg-gray-800 rounded-lg border border-gray-700 p-4">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-gray-100 font-semibold text-sm">{trend.asset_type}</h3>
              <span className="text-xs text-gray-500 bg-gray-700 px-2 py-0.5 rounded-full">
                {trend.asset_count} assets
              </span>
            </div>
            <div className="mb-3">
              <div className="flex items-end gap-1 mb-1">
                <span className={`text-2xl font-bold ${healthTextColor(trend.avg_health)}`}>
                  {trend.avg_health.toFixed(0)}
                </span>
                <span className="text-gray-500 text-sm mb-1">avg health</span>
              </div>
              <HealthBar value={trend.avg_health} />
            </div>
            <div className="grid grid-cols-2 gap-2 text-xs">
              <div className="bg-gray-700/50 rounded p-2">
                <div className="text-gray-500 mb-0.5">Min</div>
                <div className={`font-semibold ${healthTextColor(trend.min_health)}`}>
                  {trend.min_health.toFixed(0)}
                </div>
                <div className="mt-1 bg-gray-600 rounded-full h-1">
                  <div
                    className={`h-1 rounded-full ${healthColor(trend.min_health)}`}
                    style={{ width: `${Math.min(100, trend.min_health)}%` }}
                  />
                </div>
              </div>
              <div className="bg-gray-700/50 rounded p-2">
                <div className="text-gray-500 mb-0.5">Max</div>
                <div className={`font-semibold ${healthTextColor(trend.max_health)}`}>
                  {trend.max_health.toFixed(0)}
                </div>
                <div className="mt-1 bg-gray-600 rounded-full h-1">
                  <div
                    className={`h-1 rounded-full ${healthColor(trend.max_health)}`}
                    style={{ width: `${Math.min(100, trend.max_health)}%` }}
                  />
                </div>
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

// ── Tab 4: Replacement Priority ──────────────────────────────────────────────

interface ReplacementItem {
  asset_id?: string
  asset_name: string
  asset_type?: string
  region: string
  health_index: number
  failure_probability_12m: number
  replacement_cost_aud: number
  risk_weighted_cost_m: number
}

function ReplacementPriorityTab() {
  const [data, setData] = useState<ReplacementItem[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await networkAssetsApi.replacementPriority()
      setData((res.priority ?? []) as unknown as ReplacementItem[])
    } catch (e: any) {
      setError(e?.message ?? 'Failed to load replacement priority')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { load() }, [load])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-48 text-gray-400">
        <RefreshCw size={20} className="animate-spin mr-2" /> Loading replacement priority…
      </div>
    )
  }

  if (error) {
    return (
      <div className="bg-red-900/30 border border-red-700 rounded-lg p-4 text-red-400 text-sm">
        {error}
      </div>
    )
  }

  const totalCost = data.reduce((s, d) => s + (d.replacement_cost_aud ?? 0), 0)
  const totalRiskCost = data.reduce((s, d) => s + (d.risk_weighted_cost_m ?? 0), 0)

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-2 gap-4">
        <div className="bg-gray-800 rounded-lg border border-gray-700 p-4">
          <div className="text-gray-400 text-xs uppercase tracking-wider mb-1">Total Replacement Cost</div>
          <div className="text-2xl font-bold text-gray-100">{formatCostM(totalCost)}</div>
          <div className="text-gray-500 text-xs mt-1">{data.length} priority assets</div>
        </div>
        <div className="bg-gray-800 rounded-lg border border-gray-700 p-4">
          <div className="text-gray-400 text-xs uppercase tracking-wider mb-1">Total Risk-Weighted Cost</div>
          <div className="text-2xl font-bold text-red-400">${totalRiskCost.toFixed(1)}M</div>
          <div className="text-gray-500 text-xs mt-1">Probability-adjusted</div>
        </div>
      </div>

      <div className="bg-gray-800 rounded-lg border border-gray-700 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-700">
            <thead className="bg-gray-900">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-400 uppercase tracking-wider">Rank</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-400 uppercase tracking-wider">Asset</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-400 uppercase tracking-wider">Region</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-400 uppercase tracking-wider">Health</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-400 uppercase tracking-wider">Fail Prob 12m</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-400 uppercase tracking-wider">Replacement Cost</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-400 uppercase tracking-wider">Risk-Weighted Cost</th>
              </tr>
            </thead>
            <tbody className="bg-gray-800 divide-y divide-gray-700">
              {data.length === 0 && (
                <tr>
                  <td colSpan={7} className="px-4 py-8 text-center text-gray-500 text-sm">No replacement priority data</td>
                </tr>
              )}
              {data.map((item, i) => (
                <tr key={item.asset_id ?? i} className="hover:bg-gray-750 transition-colors">
                  <td className="px-4 py-3 text-sm font-bold text-gray-500">
                    #{i + 1}
                  </td>
                  <td className="px-4 py-3">
                    <div className="text-sm font-medium text-gray-100">{item.asset_name}</div>
                    {item.asset_type && (
                      <div className="text-xs text-gray-500">{item.asset_type}</div>
                    )}
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-300 whitespace-nowrap">{item.region}</td>
                  <td className="px-4 py-3 min-w-[120px]">
                    <HealthBar value={item.health_index ?? 0} />
                  </td>
                  <td className="px-4 py-3 text-sm whitespace-nowrap">
                    <span
                      className={`font-medium ${
                        (item.failure_probability_12m ?? 0) >= 0.8
                          ? 'text-red-400'
                          : (item.failure_probability_12m ?? 0) >= 0.5
                          ? 'text-amber-400'
                          : 'text-gray-300'
                      }`}
                    >
                      {formatPct(item.failure_probability_12m ?? 0)}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-100 font-mono whitespace-nowrap">
                    {formatCostM(item.replacement_cost_aud ?? 0)}
                  </td>
                  <td className="px-4 py-3 text-sm text-red-400 font-mono font-semibold whitespace-nowrap">
                    ${(item.risk_weighted_cost_m ?? 0).toFixed(2)}M
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

// ── Main Page ────────────────────────────────────────────────────────────────

type TabId = 'health' | 'risk' | 'trends' | 'priority'

interface Tab {
  id: TabId
  label: string
  icon: React.ReactNode
}

const TABS: Tab[] = [
  { id: 'health', label: 'Health Ranking', icon: <Activity size={14} /> },
  { id: 'risk', label: 'Failure Risk', icon: <AlertTriangle size={14} /> },
  { id: 'trends', label: 'Condition Trends', icon: <TrendingDown size={14} /> },
  { id: 'priority', label: 'Replacement Priority', icon: <DollarSign size={14} /> },
]

export default function NetworkAssets() {
  const [activeTab, setActiveTab] = useState<TabId>('health')

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-100">Asset Health &amp; Maintenance</h1>
          <p className="text-gray-400 text-sm mt-1">Predictive maintenance and asset condition monitoring</p>
        </div>
      </div>

      {/* Tab Bar */}
      <div className="flex gap-2 flex-wrap">
        {TABS.map(tab => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`flex items-center gap-1.5 px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
              activeTab === tab.id
                ? 'bg-blue-600 text-white'
                : 'bg-gray-700 text-gray-300 hover:bg-gray-600 hover:text-gray-100'
            }`}
          >
            {tab.icon}
            {tab.label}
          </button>
        ))}
      </div>

      {/* Tab Content */}
      {activeTab === 'health' && <HealthRankingTab />}
      {activeTab === 'risk' && <FailureRiskTab />}
      {activeTab === 'trends' && <ConditionTrendsTab />}
      {activeTab === 'priority' && <ReplacementPriorityTab />}
    </div>
  )
}
