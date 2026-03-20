// AIO Submission Pack
import { useEffect, useState } from 'react'
import { CheckCircle, AlertCircle, XCircle, Download, FileText, Sparkles, Copy, type LucideIcon } from 'lucide-react'
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

const FALLBACK_SECTIONS = [
  { section_id: 'S1', section_name: 'Network Overview', status: 'Complete', completion_pct: 100 },
  { section_id: 'S2', section_name: 'Reliability & Quality', status: 'Complete', completion_pct: 100 },
  { section_id: 'S3', section_name: 'Customer Service Standards', status: 'In Progress', completion_pct: 72 },
  { section_id: 'S4', section_name: 'STPIS Performance', status: 'In Progress', completion_pct: 60 },
  { section_id: 'S5', section_name: 'Capital Expenditure', status: 'Complete', completion_pct: 100 },
  { section_id: 'S6', section_name: 'Operating Expenditure', status: 'Incomplete', completion_pct: 35 },
  { section_id: 'S7', section_name: 'Demand Forecast & DER', status: 'In Progress', completion_pct: 55 },
  { section_id: 'S8', section_name: 'Network Capability Statement', status: 'Not Started', completion_pct: 0 },
]

const FALLBACK_VALIDATION = [
  { issue_id: 'V001', section: 'S6', severity: 'Critical', field: 'opex_total_aud', message: 'Operating expenditure total does not reconcile with audited financial statements. Variance of $2.1M detected.', action: 'Reconcile with Finance team before submission' },
  { issue_id: 'V002', section: 'S8', severity: 'Critical', field: 'network_capability_data', message: 'Section S8 (Network Capability Statement) has not been started. Mandatory AER requirement.', action: 'Initiate BSP headroom analysis immediately' },
  { issue_id: 'V003', section: 'S4', severity: 'Warning', field: 'stpis_s_factor', message: 'SAIFI calculation uses unverified outage event data from OMS. Independent verification recommended.', action: 'Cross-check OMS data with field reports' },
  { issue_id: 'V004', section: 'S3', severity: 'Warning', field: 'gsl_payments', message: 'Guaranteed Service Level payment count differs from customer billing records by 14 events.', action: 'Audit GSL payment register against billing system' },
  { issue_id: 'V005', section: 'S7', severity: 'Warning', field: 'der_penetration_pct', message: 'DER penetration forecast assumes 8% annual growth — above AER approved assumption of 6.5%.', action: 'Update DER growth assumption to AER-approved rate' },
  { issue_id: 'V006', section: 'S1', severity: 'Info', field: 'network_length_km', message: 'Network length increased by 42km since last AIO. Update asset register footnote.', action: 'Add footnote referencing new subdivision connections' },
]

const DNSP_OPTIONS = [
  'AusNet Services', 'Ergon Energy', 'Energex',
  'Ausgrid', 'Essential Energy', 'SA Power Networks',
]
const SECTION_OPTIONS = [
  'Capital Expenditure', 'Operating Expenditure', 'Reliability Performance',
  'Connection Standards', 'Demand Management', 'Price & Revenue',
]
const YEAR_OPTIONS = [2024, 2025, 2026]

export default function AioSubmissionPack() {
  const [sections, setSections] = useState<any[]>([])
  const [validation, setValidation] = useState<any[]>([])
  const [loading, setLoading] = useState(true)

  // AI Draft Generator state
  const [draftDnsp, setDraftDnsp] = useState('AusNet Services')
  const [draftSection, setDraftSection] = useState('Capital Expenditure')
  const [draftYear, setDraftYear] = useState(2026)
  const [draftLoading, setDraftLoading] = useState(false)
  const [draftResult, setDraftResult] = useState<Record<string, any> | null>(null)
  const [copied, setCopied] = useState(false)

  useEffect(() => {
    Promise.all([
      api.getAioSections(),
      api.getAioValidation(),
    ]).then(([s, v]) => {
      setSections(Array.isArray(s?.items) ? s.items : Array.isArray(s) ? s : FALLBACK_SECTIONS)
      setValidation(Array.isArray(v?.items) ? v.items : Array.isArray(v) ? v : FALLBACK_VALIDATION)
      setLoading(false)
    }).catch(() => {
      setSections(FALLBACK_SECTIONS)
      setValidation(FALLBACK_VALIDATION)
      setLoading(false)
    })
  }, [])

  if (loading) return <div className="p-8 text-gray-500 dark:text-gray-400">Loading...</div>

  const criticalCount = validation.filter(v => v.severity === 'Critical').length
  const warningCount = validation.filter(v => v.severity === 'Warning').length
  const completeCount = sections.filter(s => s.status === 'Complete').length
  const readyToSubmit = criticalCount === 0 && completeCount === sections.length

  const sectionIcon = (status: string) => {
    if (status === 'Complete') return <CheckCircle size={16} className="text-green-500 flex-shrink-0" />
    if (status === 'In Progress') return <AlertCircle size={16} className="text-amber-500 flex-shrink-0" />
    if (status === 'Incomplete') return <AlertCircle size={16} className="text-orange-500 flex-shrink-0" />
    return <XCircle size={16} className="text-red-500 flex-shrink-0" />
  }

  const severityBadge = (severity: string) => {
    if (severity === 'Critical') return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400'
    if (severity === 'Warning') return 'bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-400'
    return 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400'
  }

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900 dark:text-gray-100">AIO Submission Pack</h1>
          <p className="text-sm text-gray-500 dark:text-gray-400">Pre-submission validation, section checklist and AER template export</p>
        </div>
        <span className="text-xs px-2 py-1 rounded-full bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-400">Synthetic</span>
      </div>

      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <KpiCard label="Critical Issues" value={String(criticalCount)} sub="Block submission" Icon={XCircle} color={criticalCount > 0 ? 'bg-red-500' : 'bg-green-500'} />
        <KpiCard label="Warnings" value={String(warningCount)} sub="Review before submitting" Icon={AlertCircle} color="bg-amber-500" />
        <KpiCard label="Sections Complete" value={`${completeCount}/${sections.length}`} sub="AIO submission sections" Icon={CheckCircle} color={completeCount === sections.length ? 'bg-green-500' : 'bg-blue-500'} />
        <KpiCard label="Submission Status" value={readyToSubmit ? 'Ready' : 'Blocked'} sub={readyToSubmit ? 'Clear to submit' : 'Resolve critical issues'} Icon={FileText} color={readyToSubmit ? 'bg-green-500' : 'bg-red-500'} />
      </div>

      {/* Export button */}
      <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-sm font-semibold text-gray-800 dark:text-gray-100">AER Template Export</h2>
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">Generate AER-compliant Excel workbook with all AIO data pre-populated</p>
          </div>
          <button
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm rounded-lg font-medium transition-colors cursor-not-allowed opacity-75"
            disabled
            title="Export functionality — connect to backend"
          >
            <Download size={15} />
            Export to AER Template
          </button>
        </div>
        {criticalCount > 0 && (
          <div className="mt-3 flex items-center gap-2 text-xs text-red-600 dark:text-red-400">
            <XCircle size={13} />
            Export blocked: {criticalCount} critical validation issue{criticalCount > 1 ? 's' : ''} must be resolved first
          </div>
        )}
      </div>

      {/* Section checklist */}
      <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5">
        <h2 className="text-sm font-semibold text-gray-800 dark:text-gray-100 mb-4">Section Completion Checklist</h2>
        <div className="space-y-2">
          {sections.map((sec, i) => (
            <div
              key={i}
              className="flex items-center gap-3 p-3 rounded-lg bg-gray-50 dark:bg-gray-700/30 border border-gray-100 dark:border-gray-700/50"
            >
              {sectionIcon(sec.status)}
              <span className="font-mono text-xs font-medium text-gray-500 dark:text-gray-400 w-8">{sec.section_id}</span>
              <span className="flex-1 text-sm text-gray-800 dark:text-gray-200">{sec.section_name}</span>
              <div className="flex items-center gap-2 w-32">
                <div className="flex-1 bg-gray-200 dark:bg-gray-600 rounded-full h-1.5">
                  <div
                    className={`h-1.5 rounded-full ${sec.completion_pct === 100 ? 'bg-green-500' : sec.completion_pct > 50 ? 'bg-amber-500' : 'bg-red-500'}`}
                    style={{ width: `${sec.completion_pct ?? 0}%` }}
                  />
                </div>
                <span className="text-xs text-gray-500 dark:text-gray-400 w-8 text-right">{sec.completion_pct ?? 0}%</span>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Validation issues */}
      <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5">
        <h2 className="text-sm font-semibold text-gray-800 dark:text-gray-100 mb-4">Validation Issues ({validation.length})</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-100 dark:border-gray-700">
                <th className="text-left text-xs font-medium text-gray-500 dark:text-gray-400 pb-2 pr-4">ID</th>
                <th className="text-left text-xs font-medium text-gray-500 dark:text-gray-400 pb-2 pr-4">Section</th>
                <th className="text-left text-xs font-medium text-gray-500 dark:text-gray-400 pb-2 pr-4">Severity</th>
                <th className="text-left text-xs font-medium text-gray-500 dark:text-gray-400 pb-2 pr-4">Issue</th>
                <th className="text-left text-xs font-medium text-gray-500 dark:text-gray-400 pb-2">Recommended Action</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50 dark:divide-gray-700/50">
              {validation.map((v, i) => (
                <tr key={i} className="hover:bg-gray-50 dark:hover:bg-gray-700/30">
                  <td className="py-2.5 pr-4 text-gray-900 dark:text-gray-100 font-mono text-xs">{v.issue_id}</td>
                  <td className="py-2.5 pr-4">
                    <span className="px-2 py-0.5 text-xs rounded bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300 font-mono">{v.section}</span>
                  </td>
                  <td className="py-2.5 pr-4">
                    <span className={`px-2 py-0.5 text-xs rounded-full ${severityBadge(v.severity)}`}>{v.severity}</span>
                  </td>
                  <td className="py-2.5 pr-4 text-gray-700 dark:text-gray-300 text-xs max-w-sm">{v.message}</td>
                  <td className="py-2.5 text-gray-500 dark:text-gray-400 text-xs">{v.action}</td>
                </tr>
              ))}
              {validation.length === 0 && (
                <tr><td colSpan={5} className="py-6 text-center text-green-600 dark:text-green-400">No validation issues — submission pack is clean</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* ── AI Draft Generator ────────────────────────────────────── */}
      <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5">
        <div className="flex items-center gap-2 mb-1">
          <Sparkles size={16} className="text-blue-500" />
          <h2 className="text-sm font-semibold text-gray-800 dark:text-gray-100">AI Draft Generator</h2>
        </div>
        <p className="text-xs text-gray-500 dark:text-gray-400 mb-4">Generate regulatory-grade AIO narrative drafts using Claude Sonnet 4.5 via Databricks FMAPI</p>

        <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 mb-4">
          <div>
            <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">DNSP</label>
            <select
              value={draftDnsp}
              onChange={(e) => setDraftDnsp(e.target.value)}
              className="w-full text-sm border border-gray-200 dark:border-gray-600 rounded-lg px-3 py-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              {DNSP_OPTIONS.map((d) => <option key={d}>{d}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Section</label>
            <select
              value={draftSection}
              onChange={(e) => setDraftSection(e.target.value)}
              className="w-full text-sm border border-gray-200 dark:border-gray-600 rounded-lg px-3 py-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              {SECTION_OPTIONS.map((s) => <option key={s}>{s}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Year</label>
            <select
              value={draftYear}
              onChange={(e) => setDraftYear(Number(e.target.value))}
              className="w-full text-sm border border-gray-200 dark:border-gray-600 rounded-lg px-3 py-2 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              {YEAR_OPTIONS.map((y) => <option key={y}>{y}</option>)}
            </select>
          </div>
        </div>

        <button
          onClick={async () => {
            setDraftLoading(true)
            setDraftResult(null)
            setCopied(false)
            try {
              const result = await api.generateAioDraft({ section: draftSection, dnsp: draftDnsp, year: draftYear })
              setDraftResult(result)
            } catch {
              setDraftResult({ draft_text: 'Failed to generate draft. Please try again.', is_fallback: true, word_count: 0 })
            } finally {
              setDraftLoading(false)
            }
          }}
          disabled={draftLoading}
          className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:opacity-60 text-white text-sm rounded-lg font-medium transition-colors"
        >
          <Sparkles size={15} />
          {draftLoading ? 'Generating…' : 'Generate Draft with Claude'}
        </button>

        {draftLoading && (
          <div className="mt-4 flex items-center gap-3 text-sm text-gray-500 dark:text-gray-400">
            <svg className="animate-spin h-4 w-4 text-blue-500" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v4a4 4 0 00-4 4H4z" />
            </svg>
            Claude is drafting your regulatory narrative…
          </div>
        )}

        {draftResult && !draftLoading && (
          <div className="mt-4 space-y-3">
            {draftResult.is_fallback && (
              <div className="flex items-center gap-2 p-3 rounded-lg bg-yellow-50 dark:bg-yellow-900/10 border border-yellow-200 dark:border-yellow-700/50 text-xs text-yellow-700 dark:text-yellow-400">
                <AlertCircle size={13} className="flex-shrink-0" />
                Live AI unavailable — showing template draft
              </div>
            )}
            <div className="relative">
              <textarea
                rows={8}
                value={draftResult.draft_text ?? ''}
                onChange={(e) => setDraftResult({ ...draftResult, draft_text: e.target.value, word_count: e.target.value.split(/\s+/).filter(Boolean).length })}
                className="w-full text-sm border border-gray-200 dark:border-gray-600 rounded-lg px-3 py-2.5 bg-gray-50 dark:bg-gray-700/50 text-gray-800 dark:text-gray-200 focus:outline-none focus:ring-2 focus:ring-blue-500 resize-y"
              />
            </div>
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <span className="text-xs px-2 py-0.5 rounded-full bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400">
                  {draftResult.word_count ?? 0} words
                </span>
                <span className="text-xs px-2 py-0.5 rounded-full bg-blue-50 dark:bg-blue-900/20 text-blue-600 dark:text-blue-400 border border-blue-100 dark:border-blue-800/40">
                  Generated by Claude Sonnet 4.5 · Databricks FMAPI
                </span>
              </div>
              <button
                onClick={() => {
                  navigator.clipboard.writeText(draftResult.draft_text ?? '')
                  setCopied(true)
                  setTimeout(() => setCopied(false), 2000)
                }}
                className="flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-lg border border-gray-200 dark:border-gray-600 text-gray-600 dark:text-gray-400 hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors"
              >
                <Copy size={12} />
                {copied ? 'Copied!' : 'Copy'}
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
