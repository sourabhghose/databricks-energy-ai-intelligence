import { useEffect, useState } from 'react'
import { Info } from 'lucide-react'

/**
 * Shown instead of illustrative numbers when the backend has no real data source for a dashboard.
 * The backend explains why in the HTTP 503 `detail`; we fetch it so the message stays in one place.
 */
export default function DataUnavailable({ title, endpoint }: { title: string; endpoint: string }) {
  const [detail, setDetail] = useState<string>('Checking data availability…')

  useEffect(() => {
    fetch(endpoint)
      .then(async r => {
        const body = await r.json().catch(() => ({}))
        setDetail(r.ok ? 'Data is available — reload the page.' : String(body?.detail ?? `HTTP ${r.status}`))
      })
      .catch(e => setDetail(String(e)))
  }, [endpoint])

  return (
    <div className="p-6 space-y-4">
      <h1 className="text-xl font-bold text-gray-900 dark:text-gray-100">{title}</h1>
      <div className="flex items-start gap-3 rounded-xl border border-amber-300 dark:border-amber-700 bg-amber-50 dark:bg-amber-900/20 p-4">
        <Info className="w-5 h-5 mt-0.5 text-amber-600 dark:text-amber-400 shrink-0" />
        <div>
          <p className="text-sm font-semibold text-amber-800 dark:text-amber-300">Not available in this build</p>
          <p className="text-sm text-amber-900/80 dark:text-amber-200/80 mt-1">{detail}</p>
        </div>
      </div>
    </div>
  )
}
