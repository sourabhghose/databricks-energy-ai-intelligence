import React, { useState, useEffect, useCallback } from 'react'
import { Settings as SettingsIcon, Key, Database, Server, Save, RefreshCw } from 'lucide-react'
import { api, UserPreferences, ApiKeyInfo, DataSourceConfig, SystemConfig } from '../api/client'

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type TabId = 'preferences' | 'api-keys' | 'data-sources' | 'system-info'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function StatusBadge({ status }: { status: string }) {
  const colourMap: Record<string, string> = {
    connected: 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300',
    degraded: 'bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300',
    disconnected: 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300',
  }
  const cls = colourMap[status] ?? 'bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300'
  return (
    <span className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-medium ${cls}`}>
      <span className="w-1.5 h-1.5 rounded-full bg-current" />
      {status}
    </span>
  )
}

function ActiveBadge({ active }: { active: boolean }) {
  return active ? (
    <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300">
      <span className="w-1.5 h-1.5 rounded-full bg-current" />
      Active
    </span>
  ) : (
    <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300">
      <span className="w-1.5 h-1.5 rounded-full bg-current" />
      Expired
    </span>
  )
}

function formatDate(iso?: string | null): string {
  if (!iso) return '—'
  try {
    return new Date(iso).toLocaleDateString('en-AU', {
      day: '2-digit',
      month: 'short',
      year: 'numeric',
    })
  } catch {
    return iso
  }
}

function formatDateTime(iso?: string | null): string {
  if (!iso) return '—'
  try {
    return new Date(iso).toLocaleString('en-AU', {
      day: '2-digit',
      month: 'short',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      timeZoneName: 'short',
    })
  } catch {
    return iso
  }
}

// ---------------------------------------------------------------------------
// Tab 1: User Preferences
// ---------------------------------------------------------------------------

const NEM_REGIONS = ['NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1']

interface PreferencesTabProps {
  prefs: UserPreferences
  saving: boolean
  saveError: string | null
  saveSuccess: boolean
  onChange: (updated: UserPreferences) => void
  onSave: () => void
}

function PreferencesTab({ prefs, saving, saveError, saveSuccess, onChange, onSave }: PreferencesTabProps) {
  function set<K extends keyof UserPreferences>(key: K, value: UserPreferences[K]) {
    onChange({ ...prefs, [key]: value })
  }

  function toggleWatchlist(region: string) {
    const current = prefs.regions_watchlist
    const updated = current.includes(region)
      ? current.filter(r => r !== region)
      : [...current, region]
    set('regions_watchlist', updated)
  }

  return (
    <div className="space-y-6">
      {/* Default Region */}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
        <div>
          <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
            Default Region
          </label>
          <select
            value={prefs.default_region}
            onChange={e => set('default_region', e.target.value)}
            className="w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-amber-500"
          >
            {NEM_REGIONS.map(r => (
              <option key={r} value={r}>{r}</option>
            ))}
          </select>
        </div>

        {/* Default Forecast Horizon */}
        <div>
          <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
            Default Forecast Horizon
          </label>
          <select
            value={prefs.default_horizon}
            onChange={e => set('default_horizon', e.target.value)}
            className="w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-amber-500"
          >
            {['1h', '4h', '24h', '7d'].map(h => (
              <option key={h} value={h}>{h}</option>
            ))}
          </select>
        </div>
      </div>

      {/* Theme */}
      <div>
        <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
          Theme
        </label>
        <div className="flex gap-4">
          {(['light', 'dark', 'system'] as const).map(t => (
            <label key={t} className="flex items-center gap-2 cursor-pointer">
              <input
                type="radio"
                name="theme"
                value={t}
                checked={prefs.theme === t}
                onChange={() => set('theme', t)}
                className="text-amber-500 focus:ring-amber-500"
              />
              <span className="text-sm text-gray-700 dark:text-gray-300 capitalize">{t}</span>
            </label>
          ))}
        </div>
      </div>

      {/* Alert Thresholds */}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
        <div>
          <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
            Price Alert Threshold
          </label>
          <div className="flex items-center gap-2">
            <input
              type="number"
              min={0}
              step={10}
              value={prefs.price_alert_threshold}
              onChange={e => set('price_alert_threshold', parseFloat(e.target.value) || 0)}
              className="flex-1 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-amber-500"
            />
            <span className="text-xs text-gray-500 dark:text-gray-400 whitespace-nowrap">$/MWh</span>
          </div>
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
            Demand Alert Threshold
          </label>
          <div className="flex items-center gap-2">
            <input
              type="number"
              min={0}
              step={100}
              value={prefs.demand_alert_threshold}
              onChange={e => set('demand_alert_threshold', parseFloat(e.target.value) || 0)}
              className="flex-1 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-amber-500"
            />
            <span className="text-xs text-gray-500 dark:text-gray-400 whitespace-nowrap">MW</span>
          </div>
        </div>
      </div>

      {/* Auto-Refresh */}
      <div>
        <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
          Auto-Refresh Interval
        </label>
        <select
          value={prefs.auto_refresh_seconds}
          onChange={e => set('auto_refresh_seconds', parseInt(e.target.value, 10))}
          className="w-48 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-amber-500"
        >
          {[15, 30, 60, 120].map(s => (
            <option key={s} value={s}>{s}s</option>
          ))}
        </select>
      </div>

      {/* Regions Watchlist */}
      <div>
        <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
          Regions Watchlist
        </label>
        <div className="flex flex-wrap gap-3">
          {NEM_REGIONS.map(r => (
            <label key={r} className="flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={prefs.regions_watchlist.includes(r)}
                onChange={() => toggleWatchlist(r)}
                className="rounded text-amber-500 focus:ring-amber-500"
              />
              <span className="text-sm text-gray-700 dark:text-gray-300">{r}</span>
            </label>
          ))}
        </div>
      </div>

      {/* Notification Email */}
      <div>
        <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
          Notification Email
        </label>
        <input
          type="email"
          placeholder="admin@example.com"
          value={prefs.notification_email ?? ''}
          onChange={e => set('notification_email', e.target.value || undefined)}
          className="w-full max-w-sm rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-amber-500"
        />
      </div>

      {/* Data Export Format */}
      <div>
        <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
          Data Export Format
        </label>
        <div className="flex gap-4">
          {(['csv', 'json', 'parquet'] as const).map(fmt => (
            <label key={fmt} className="flex items-center gap-2 cursor-pointer">
              <input
                type="radio"
                name="export_format"
                value={fmt}
                checked={prefs.data_export_format === fmt}
                onChange={() => set('data_export_format', fmt)}
                className="text-amber-500 focus:ring-amber-500"
              />
              <span className="text-sm text-gray-700 dark:text-gray-300 uppercase">{fmt}</span>
            </label>
          ))}
        </div>
      </div>

      {/* Save button */}
      <div className="flex items-center gap-3 pt-2 border-t border-gray-200 dark:border-gray-700">
        <button
          onClick={onSave}
          disabled={saving}
          className="flex items-center gap-2 px-4 py-2 rounded-md bg-amber-500 hover:bg-amber-600 text-white text-sm font-medium transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {saving ? (
            <RefreshCw size={14} className="animate-spin" />
          ) : (
            <Save size={14} />
          )}
          {saving ? 'Saving…' : 'Save Preferences'}
        </button>
        {saveSuccess && (
          <span className="text-sm text-green-600 dark:text-green-400 font-medium">Saved successfully</span>
        )}
        {saveError && (
          <span className="text-sm text-red-600 dark:text-red-400">{saveError}</span>
        )}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Tab 2: API Keys
// ---------------------------------------------------------------------------

interface ApiKeysTabProps {
  keys: ApiKeyInfo[]
  loading: boolean
  error: string | null
  onRefresh: () => void
}

function ApiKeysTab({ keys, loading, error, onRefresh }: ApiKeysTabProps) {
  const [revokedIds, setRevokedIds] = useState<Set<string>>(new Set())
  const [showNewKeyModal, setShowNewKeyModal] = useState(false)
  const [newKeyName, setNewKeyName] = useState('')
  const [newKeyGenerated, setNewKeyGenerated] = useState(false)

  function handleRevoke(keyId: string) {
    setRevokedIds(prev => new Set([...prev, keyId]))
  }

  function handleGenerateKey() {
    if (newKeyName.trim()) {
      setNewKeyGenerated(true)
    }
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <p className="text-sm text-gray-600 dark:text-gray-400">
          API key secrets are never displayed after creation. Only the first 8 characters (prefix) are shown.
        </p>
        <div className="flex gap-2">
          <button
            onClick={onRefresh}
            disabled={loading}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-md border border-gray-300 dark:border-gray-600 text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors disabled:opacity-50"
          >
            <RefreshCw size={13} className={loading ? 'animate-spin' : ''} />
            Refresh
          </button>
          <button
            onClick={() => { setShowNewKeyModal(true); setNewKeyGenerated(false); setNewKeyName('') }}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-md bg-amber-500 hover:bg-amber-600 text-white text-sm font-medium transition-colors"
          >
            <Key size={13} />
            Generate New Key
          </button>
        </div>
      </div>

      {error && (
        <div className="rounded-md bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 p-3 text-sm text-red-700 dark:text-red-300">
          {error}
        </div>
      )}

      {loading && keys.length === 0 ? (
        <div className="py-12 text-center text-gray-500 dark:text-gray-400 text-sm">Loading API keys…</div>
      ) : (
        <div className="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
          <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700 text-sm">
            <thead className="bg-gray-50 dark:bg-gray-800">
              <tr>
                {['Name', 'Key (masked)', 'Created', 'Last Used', 'Expires', 'Permissions', 'Req. Today', 'Rate Limit', 'Status', 'Actions'].map(h => (
                  <th key={h} className="px-3 py-2.5 text-left text-xs font-semibold text-gray-600 dark:text-gray-400 uppercase tracking-wide whitespace-nowrap">
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100 dark:divide-gray-800 bg-white dark:bg-gray-900">
              {keys.map(k => {
                const revoked = revokedIds.has(k.key_id)
                return (
                  <tr key={k.key_id} className={revoked ? 'opacity-40' : ''}>
                    <td className="px-3 py-2.5 font-medium text-gray-900 dark:text-gray-100 whitespace-nowrap">
                      {k.name}
                    </td>
                    <td className="px-3 py-2.5 font-mono text-gray-600 dark:text-gray-400 whitespace-nowrap">
                      {k.key_prefix}••••••••••••••••
                    </td>
                    <td className="px-3 py-2.5 text-gray-600 dark:text-gray-400 whitespace-nowrap">
                      {formatDate(k.created_at)}
                    </td>
                    <td className="px-3 py-2.5 text-gray-600 dark:text-gray-400 whitespace-nowrap">
                      {formatDate(k.last_used_at)}
                    </td>
                    <td className="px-3 py-2.5 text-gray-600 dark:text-gray-400 whitespace-nowrap">
                      {k.expires_at ? formatDate(k.expires_at) : 'Never'}
                    </td>
                    <td className="px-3 py-2.5 whitespace-nowrap">
                      <div className="flex flex-wrap gap-1">
                        {k.permissions.map(p => (
                          <span key={p} className="px-1.5 py-0.5 rounded text-xs bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 font-mono">
                            {p}
                          </span>
                        ))}
                      </div>
                    </td>
                    <td className="px-3 py-2.5 text-gray-600 dark:text-gray-400 text-right tabular-nums">
                      {k.request_count_today.toLocaleString()}
                    </td>
                    <td className="px-3 py-2.5 text-gray-600 dark:text-gray-400 whitespace-nowrap">
                      {k.rate_limit_per_min}/min
                    </td>
                    <td className="px-3 py-2.5">
                      <ActiveBadge active={k.is_active && !revoked} />
                    </td>
                    <td className="px-3 py-2.5 whitespace-nowrap">
                      <div className="flex gap-2">
                        <button
                          title="Key value is masked for security"
                          disabled
                          className="px-2 py-1 rounded text-xs border border-gray-200 dark:border-gray-700 text-gray-400 dark:text-gray-600 cursor-not-allowed"
                        >
                          ****
                        </button>
                        <button
                          onClick={() => handleRevoke(k.key_id)}
                          disabled={revoked || !k.is_active}
                          className="px-2 py-1 rounded text-xs border border-red-200 dark:border-red-800 text-red-600 dark:text-red-400 hover:bg-red-50 dark:hover:bg-red-900/20 transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
                        >
                          Revoke
                        </button>
                      </div>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}

      {/* Generate New Key Modal */}
      {showNewKeyModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
          <div className="bg-white dark:bg-gray-900 rounded-xl shadow-xl p-6 w-full max-w-md mx-4 border border-gray-200 dark:border-gray-700">
            <h3 className="text-base font-semibold text-gray-900 dark:text-gray-100 mb-4">Generate New API Key</h3>
            {newKeyGenerated ? (
              <div className="space-y-4">
                <p className="text-sm text-green-700 dark:text-green-300 bg-green-50 dark:bg-green-900/20 rounded-md p-3 border border-green-200 dark:border-green-800">
                  Key generated successfully. Copy it now — it will not be shown again.
                </p>
                <div className="font-mono text-xs bg-gray-100 dark:bg-gray-800 rounded-md p-3 text-gray-800 dark:text-gray-200 break-all">
                  ec_new_{newKeyName.toLowerCase().replace(/\s+/g, '_')}_••••••••••••••••••••••••
                </div>
                <div className="flex justify-end">
                  <button
                    onClick={() => setShowNewKeyModal(false)}
                    className="px-4 py-2 rounded-md bg-amber-500 hover:bg-amber-600 text-white text-sm font-medium transition-colors"
                  >
                    Done
                  </button>
                </div>
              </div>
            ) : (
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Key Name</label>
                  <input
                    type="text"
                    placeholder="e.g. CI/CD Pipeline Key"
                    value={newKeyName}
                    onChange={e => setNewKeyName(e.target.value)}
                    className="w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-amber-500"
                  />
                </div>
                <div className="flex justify-end gap-2">
                  <button
                    onClick={() => setShowNewKeyModal(false)}
                    className="px-4 py-2 rounded-md border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-300 text-sm hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors"
                  >
                    Cancel
                  </button>
                  <button
                    onClick={handleGenerateKey}
                    disabled={!newKeyName.trim()}
                    className="px-4 py-2 rounded-md bg-amber-500 hover:bg-amber-600 text-white text-sm font-medium transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    Generate
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Tab 3: Data Sources
// ---------------------------------------------------------------------------

interface DataSourcesTabProps {
  sources: DataSourceConfig[]
  loading: boolean
  error: string | null
  onRefresh: () => void
}

function DataSourcesTab({ sources, loading, error, onRefresh }: DataSourcesTabProps) {
  const [syncedIds, setSyncedIds] = useState<Set<string>>(new Set())

  function handleSyncNow(sourceId: string) {
    setSyncedIds(prev => new Set([...prev, sourceId]))
    // Clear the visual feedback after 3 seconds
    setTimeout(() => {
      setSyncedIds(prev => {
        const next = new Set(prev)
        next.delete(sourceId)
        return next
      })
    }, 3000)
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <p className="text-sm text-gray-600 dark:text-gray-400">
          {sources.length} data sources configured. Last updated info reflects the most recent sync cycle.
        </p>
        <button
          onClick={onRefresh}
          disabled={loading}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded-md border border-gray-300 dark:border-gray-600 text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors disabled:opacity-50"
        >
          <RefreshCw size={13} className={loading ? 'animate-spin' : ''} />
          Refresh
        </button>
      </div>

      {error && (
        <div className="rounded-md bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 p-3 text-sm text-red-700 dark:text-red-300">
          {error}
        </div>
      )}

      {loading && sources.length === 0 ? (
        <div className="py-12 text-center text-gray-500 dark:text-gray-400 text-sm">Loading data sources…</div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {sources.map(src => (
            <div
              key={src.source_id}
              className="rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 p-4 flex flex-col gap-3"
            >
              {/* Header */}
              <div className="flex items-start justify-between gap-2">
                <div className="flex items-center gap-2">
                  <Database size={16} className="text-amber-500 shrink-0 mt-0.5" />
                  <span className="font-semibold text-sm text-gray-900 dark:text-gray-100 leading-snug">
                    {src.name}
                  </span>
                </div>
                <StatusBadge status={src.status} />
              </div>

              {/* Endpoint */}
              <div className="text-xs font-mono text-gray-500 dark:text-gray-500 truncate" title={src.endpoint_url}>
                {src.endpoint_url}
              </div>

              {/* Stats grid */}
              <div className="grid grid-cols-3 gap-2 text-xs">
                <div>
                  <span className="block text-gray-500 dark:text-gray-400 mb-0.5">Last Sync</span>
                  <span className="font-medium text-gray-800 dark:text-gray-200">
                    {formatDateTime(src.last_sync)}
                  </span>
                </div>
                <div>
                  <span className="block text-gray-500 dark:text-gray-400 mb-0.5">Interval</span>
                  <span className="font-medium text-gray-800 dark:text-gray-200">
                    {src.sync_interval_minutes}m
                  </span>
                </div>
                <div>
                  <span className="block text-gray-500 dark:text-gray-400 mb-0.5">Records Today</span>
                  <span className="font-medium text-gray-800 dark:text-gray-200">
                    {src.records_synced_today.toLocaleString()}
                  </span>
                </div>
              </div>

              {/* Actions */}
              <div className="flex gap-2 pt-1">
                <button
                  onClick={() => handleSyncNow(src.source_id)}
                  className="flex items-center gap-1.5 px-3 py-1.5 rounded-md bg-amber-500 hover:bg-amber-600 text-white text-xs font-medium transition-colors"
                >
                  {syncedIds.has(src.source_id) ? (
                    <>
                      <RefreshCw size={11} className="animate-spin" />
                      Syncing…
                    </>
                  ) : (
                    <>
                      <RefreshCw size={11} />
                      Sync Now
                    </>
                  )}
                </button>
                <button
                  disabled
                  className="px-3 py-1.5 rounded-md border border-gray-200 dark:border-gray-700 text-gray-400 dark:text-gray-600 text-xs cursor-not-allowed"
                  title="Configuration UI coming in a future sprint"
                >
                  Configure
                </button>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Tab 4: System Info
// ---------------------------------------------------------------------------

interface SystemInfoTabProps {
  config: SystemConfig | null
  loading: boolean
  error: string | null
  onRefresh: () => void
}

function SystemInfoTab({ config, loading, error, onRefresh }: SystemInfoTabProps) {
  const envColour: Record<string, string> = {
    development: 'bg-blue-100 text-blue-800 dark:bg-blue-900/40 dark:text-blue-300',
    staging: 'bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300',
    production: 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300',
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <p className="text-sm text-gray-600 dark:text-gray-400">
          Read-only platform configuration and runtime statistics.
        </p>
        <button
          onClick={onRefresh}
          disabled={loading}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded-md border border-gray-300 dark:border-gray-600 text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors disabled:opacity-50"
        >
          <RefreshCw size={13} className={loading ? 'animate-spin' : ''} />
          Refresh
        </button>
      </div>

      {error && (
        <div className="rounded-md bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 p-3 text-sm text-red-700 dark:text-red-300">
          {error}
        </div>
      )}

      {loading && !config ? (
        <div className="py-12 text-center text-gray-500 dark:text-gray-400 text-sm">Loading system config…</div>
      ) : config ? (
        <div className="rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 divide-y divide-gray-100 dark:divide-gray-800">
          {/* Mock Mode */}
          <div className="flex items-center justify-between px-4 py-3">
            <span className="text-sm font-medium text-gray-700 dark:text-gray-300">Mock Mode</span>
            <span className={`inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-semibold ${config.mock_mode ? 'bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300' : 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300'}`}>
              {config.mock_mode ? 'ON' : 'OFF'}
            </span>
          </div>

          {/* Environment */}
          <div className="flex items-center justify-between px-4 py-3">
            <span className="text-sm font-medium text-gray-700 dark:text-gray-300">Environment</span>
            <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-semibold ${envColour[config.environment] ?? 'bg-gray-100 text-gray-700'}`}>
              {config.environment}
            </span>
          </div>

          {/* Databricks Workspace */}
          <div className="flex items-center justify-between px-4 py-3">
            <span className="text-sm font-medium text-gray-700 dark:text-gray-300">Databricks Workspace</span>
            <span className="text-sm font-mono text-gray-600 dark:text-gray-400">{config.databricks_workspace}</span>
          </div>

          {/* Unity Catalog */}
          <div className="flex items-center justify-between px-4 py-3">
            <span className="text-sm font-medium text-gray-700 dark:text-gray-300">Unity Catalog</span>
            <span className="text-sm font-mono text-gray-600 dark:text-gray-400">{config.unity_catalog}</span>
          </div>

          {/* MLflow Experiment */}
          <div className="flex items-center justify-between px-4 py-3">
            <span className="text-sm font-medium text-gray-700 dark:text-gray-300">MLflow Experiment</span>
            <span className="text-sm font-mono text-gray-600 dark:text-gray-400">{config.mlflow_experiment}</span>
          </div>

          {/* API Version */}
          <div className="flex items-center justify-between px-4 py-3">
            <span className="text-sm font-medium text-gray-700 dark:text-gray-300">API Version</span>
            <span className="text-sm text-gray-600 dark:text-gray-400">Sprint {config.api_version}</span>
          </div>

          {/* Frontend Version */}
          <div className="flex items-center justify-between px-4 py-3">
            <span className="text-sm font-medium text-gray-700 dark:text-gray-300">Frontend Version</span>
            <span className="text-sm text-gray-600 dark:text-gray-400">Sprint {config.frontend_version}</span>
          </div>

          {/* Uptime */}
          <div className="flex items-center justify-between px-4 py-3">
            <span className="text-sm font-medium text-gray-700 dark:text-gray-300">Backend Uptime</span>
            <span className="text-sm text-gray-600 dark:text-gray-400">
              {config.backend_uptime_hours < 1
                ? `${Math.round(config.backend_uptime_hours * 60)} minutes`
                : `${config.backend_uptime_hours.toFixed(1)} hours`}
            </span>
          </div>

          {/* API Requests Today */}
          <div className="flex items-center justify-between px-4 py-3">
            <span className="text-sm font-medium text-gray-700 dark:text-gray-300">API Requests Today</span>
            <span className="text-sm font-semibold text-gray-800 dark:text-gray-200">
              {config.total_api_requests_today.toLocaleString()}
            </span>
          </div>

          {/* Cache Hit Rate */}
          <div className="px-4 py-3">
            <div className="flex items-center justify-between mb-2">
              <span className="text-sm font-medium text-gray-700 dark:text-gray-300">Cache Hit Rate</span>
              <span className="text-sm font-semibold text-gray-800 dark:text-gray-200">
                {config.cache_hit_rate_pct.toFixed(1)}%
              </span>
            </div>
            <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-2">
              <div
                className="bg-amber-500 h-2 rounded-full transition-all duration-500"
                style={{ width: `${Math.min(100, config.cache_hit_rate_pct)}%` }}
              />
            </div>
          </div>
        </div>
      ) : null}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main Settings page
// ---------------------------------------------------------------------------

export default function Settings() {
  const [activeTab, setActiveTab] = useState<TabId>('preferences')

  // Preferences state
  const [prefs, setPrefs] = useState<UserPreferences | null>(null)
  const [prefsLoading, setPrefsLoading] = useState(true)
  const [prefsError, setPrefsError] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)
  const [saveError, setSaveError] = useState<string | null>(null)
  const [saveSuccess, setSaveSuccess] = useState(false)

  // API keys state
  const [apiKeys, setApiKeys] = useState<ApiKeyInfo[]>([])
  const [keysLoading, setKeysLoading] = useState(false)
  const [keysError, setKeysError] = useState<string | null>(null)

  // Data sources state
  const [sources, setSources] = useState<DataSourceConfig[]>([])
  const [sourcesLoading, setSourcesLoading] = useState(false)
  const [sourcesError, setSourcesError] = useState<string | null>(null)

  // System config state
  const [sysConfig, setSysConfig] = useState<SystemConfig | null>(null)
  const [sysLoading, setSysLoading] = useState(false)
  const [sysError, setSysError] = useState<string | null>(null)

  // Load preferences on mount
  useEffect(() => {
    loadPreferences()
  }, [])

  // Load tab-specific data when tab changes
  useEffect(() => {
    if (activeTab === 'api-keys' && apiKeys.length === 0) {
      loadApiKeys()
    } else if (activeTab === 'data-sources' && sources.length === 0) {
      loadDataSources()
    } else if (activeTab === 'system-info' && !sysConfig) {
      loadSystemConfig()
    }
  }, [activeTab]) // eslint-disable-line react-hooks/exhaustive-deps

  const loadPreferences = useCallback(async () => {
    setPrefsLoading(true)
    setPrefsError(null)
    try {
      const data = await api.getAdminPreferences()
      setPrefs(data)
    } catch (e) {
      setPrefsError(e instanceof Error ? e.message : 'Failed to load preferences')
    } finally {
      setPrefsLoading(false)
    }
  }, [])

  const loadApiKeys = useCallback(async () => {
    setKeysLoading(true)
    setKeysError(null)
    try {
      const data = await api.getApiKeys()
      setApiKeys(data)
    } catch (e) {
      setKeysError(e instanceof Error ? e.message : 'Failed to load API keys')
    } finally {
      setKeysLoading(false)
    }
  }, [])

  const loadDataSources = useCallback(async () => {
    setSourcesLoading(true)
    setSourcesError(null)
    try {
      const data = await api.getDataSources()
      setSources(data)
    } catch (e) {
      setSourcesError(e instanceof Error ? e.message : 'Failed to load data sources')
    } finally {
      setSourcesLoading(false)
    }
  }, [])

  const loadSystemConfig = useCallback(async () => {
    setSysLoading(true)
    setSysError(null)
    try {
      const data = await api.getSystemConfig()
      setSysConfig(data)
    } catch (e) {
      setSysError(e instanceof Error ? e.message : 'Failed to load system config')
    } finally {
      setSysLoading(false)
    }
  }, [])

  const handleSavePreferences = useCallback(async () => {
    if (!prefs) return
    setSaving(true)
    setSaveError(null)
    setSaveSuccess(false)
    try {
      const updated = await api.updateAdminPreferences(prefs)
      setPrefs(updated)
      setSaveSuccess(true)
      setTimeout(() => setSaveSuccess(false), 3000)
    } catch (e) {
      setSaveError(e instanceof Error ? e.message : 'Failed to save preferences')
    } finally {
      setSaving(false)
    }
  }, [prefs])

  const TABS: { id: TabId; label: string; Icon: React.ElementType }[] = [
    { id: 'preferences', label: 'User Preferences', Icon: SettingsIcon },
    { id: 'api-keys', label: 'API Keys', Icon: Key },
    { id: 'data-sources', label: 'Data Sources', Icon: Database },
    { id: 'system-info', label: 'System Info', Icon: Server },
  ]

  return (
    <div className="p-6 max-w-6xl mx-auto">
      {/* Page header */}
      <div className="flex items-center gap-3 mb-6">
        <SettingsIcon className="text-amber-500" size={24} />
        <div>
          <h1 className="text-xl font-bold text-gray-900 dark:text-gray-100">Settings & Administration</h1>
          <p className="text-sm text-gray-500 dark:text-gray-400">
            Manage platform preferences, API access, data sources, and system configuration.
          </p>
        </div>
      </div>

      {/* Tab navigation */}
      <div className="border-b border-gray-200 dark:border-gray-700 mb-6">
        <nav className="flex gap-1 -mb-px" aria-label="Settings tabs">
          {TABS.map(({ id, label, Icon }) => (
            <button
              key={id}
              onClick={() => setActiveTab(id)}
              className={[
                'flex items-center gap-2 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors whitespace-nowrap',
                activeTab === id
                  ? 'border-amber-500 text-amber-600 dark:text-amber-400'
                  : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300 hover:border-gray-300 dark:hover:border-gray-600',
              ].join(' ')}
            >
              <Icon size={15} />
              {label}
            </button>
          ))}
        </nav>
      </div>

      {/* Tab content */}
      <div className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-6">
        {activeTab === 'preferences' && (
          <>
            {prefsLoading && !prefs ? (
              <div className="py-12 text-center text-gray-500 dark:text-gray-400 text-sm">Loading preferences…</div>
            ) : prefsError ? (
              <div className="rounded-md bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 p-3 text-sm text-red-700 dark:text-red-300">
                {prefsError}
              </div>
            ) : prefs ? (
              <PreferencesTab
                prefs={prefs}
                saving={saving}
                saveError={saveError}
                saveSuccess={saveSuccess}
                onChange={setPrefs}
                onSave={handleSavePreferences}
              />
            ) : null}
          </>
        )}

        {activeTab === 'api-keys' && (
          <ApiKeysTab
            keys={apiKeys}
            loading={keysLoading}
            error={keysError}
            onRefresh={loadApiKeys}
          />
        )}

        {activeTab === 'data-sources' && (
          <DataSourcesTab
            sources={sources}
            loading={sourcesLoading}
            error={sourcesError}
            onRefresh={loadDataSources}
          />
        )}

        {activeTab === 'system-info' && (
          <SystemInfoTab
            config={sysConfig}
            loading={sysLoading}
            error={sysError}
            onRefresh={loadSystemConfig}
          />
        )}
      </div>
    </div>
  )
}
