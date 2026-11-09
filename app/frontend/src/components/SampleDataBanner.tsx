import { useEffect, useState } from 'react'
import { useLocation } from 'react-router-dom'
import { AlertTriangle } from 'lucide-react'
import { SAMPLE_DATA_EVENT } from '../api/client'

/** Amber banner shown while the current page has loaded any endpoint that serves illustrative data. */
export default function SampleDataBanner() {
  const location = useLocation()
  const [paths, setPaths] = useState<string[]>([])

  useEffect(() => { setPaths([]) }, [location.pathname])
  useEffect(() => {
    const onSample = (e: Event) => {
      const path = (e as CustomEvent<string>).detail
      setPaths(prev => (prev.includes(path) ? prev : [...prev, path]))
    }
    window.addEventListener(SAMPLE_DATA_EVENT, onSample)
    return () => window.removeEventListener(SAMPLE_DATA_EVENT, onSample)
  }, [])

  if (paths.length === 0) return null
  return (
    <div className="flex items-center gap-2 px-6 py-1.5 text-xs bg-amber-50 dark:bg-amber-900/30 text-amber-800 dark:text-amber-200 border-b border-amber-200 dark:border-amber-800">
      <AlertTriangle className="w-3.5 h-3.5 shrink-0" />
      <span>Sample data: this page shows illustrative values, not live market or model outputs ({paths.join(', ')}).</span>
    </div>
  )
}
