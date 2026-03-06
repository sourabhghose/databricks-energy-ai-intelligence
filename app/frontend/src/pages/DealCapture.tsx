import React, { useState, useEffect, useCallback, useRef } from 'react'
import { Plus, Upload, Sparkles, Send } from 'lucide-react'
import {
    dealApi,
    Counterparty,
    Portfolio,
    TradeCreateRequest,
} from '../api/client'

const TRADE_TYPES = ['SPOT', 'FORWARD', 'SWAP', 'FUTURE', 'OPTION', 'PPA', 'REC']
const REGIONS = ['NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1']
const PROFILES = ['FLAT', 'PEAK', 'OFF_PEAK', 'SUPER_PEAK']

function fmt(v: number, d = 0) {
    return v.toLocaleString('en-AU', { minimumFractionDigits: d, maximumFractionDigits: d })
}

export default function DealCapture() {
    const [counterparties, setCounterparties] = useState<Counterparty[]>([])
    const [portfolios, setPortfolios] = useState<Portfolio[]>([])
    const [submitting, setSubmitting] = useState(false)
    const [result, setResult] = useState<{ ok: boolean; msg: string } | null>(null)
    const [importResult, setImportResult] = useState<{ imported: number; errors: number } | null>(null)
    const [aiText, setAiText] = useState('')
    const [aiParsing, setAiParsing] = useState(false)
    const fileRef = useRef<HTMLInputElement>(null)

    const [form, setForm] = useState<TradeCreateRequest>({
        trade_type: 'SWAP',
        region: 'NSW1',
        buy_sell: 'BUY',
        volume_mw: 50,
        price: 75,
        start_date: '2026-07-01',
        end_date: '2026-09-30',
        profile: 'FLAT',
        status: 'DRAFT',
        counterparty_id: '',
        portfolio_id: '',
        notes: '',
    })

    useEffect(() => {
        dealApi.getCounterparties().then(r => setCounterparties(r.counterparties)).catch(() => {})
        dealApi.getPortfolios().then(r => setPortfolios(r.portfolios)).catch(() => {})
    }, [])

    const setField = useCallback((key: string, val: string | number) => {
        setForm(f => ({ ...f, [key]: val }))
    }, [])

    const handleSubmit = useCallback(async () => {
        setSubmitting(true)
        setResult(null)
        try {
            const res = await dealApi.createTrade(form)
            setResult({ ok: true, msg: `Trade ${res.trade_id.slice(0, 8)}... created with ${res.legs_created} legs` })
        } catch (e: any) {
            setResult({ ok: false, msg: e.message || 'Failed to create trade' })
        } finally {
            setSubmitting(false)
        }
    }, [form])

    const handleBulkImport = useCallback(async () => {
        const file = fileRef.current?.files?.[0]
        if (!file) return
        setImportResult(null)
        try {
            const res = await dealApi.bulkImportTrades(file)
            setImportResult(res)
        } catch (e: any) {
            setImportResult({ imported: 0, errors: -1 })
        }
    }, [])

    const handleAiParse = useCallback(async () => {
        if (!aiText.trim()) return
        setAiParsing(true)
        try {
            // Send to copilot for NL parsing — the backend will parse trade description
            const res = await fetch('/api/copilot/chat', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    messages: [{ role: 'user', content: `Parse this trade description and return ONLY a JSON object with fields: trade_type, region, buy_sell, volume_mw, price, start_date, end_date, profile. Description: "${aiText}"` }],
                    stream: false,
                }),
            })
            const data = await res.json()
            const content = data.choices?.[0]?.message?.content || data.content || ''
            // Try to extract JSON from the response
            const jsonMatch = content.match(/\{[\s\S]*\}/)
            if (jsonMatch) {
                const parsed = JSON.parse(jsonMatch[0])
                setForm(f => ({
                    ...f,
                    trade_type: parsed.trade_type?.toUpperCase() || f.trade_type,
                    region: parsed.region?.toUpperCase() || f.region,
                    buy_sell: parsed.buy_sell?.toUpperCase() || f.buy_sell,
                    volume_mw: parsed.volume_mw || f.volume_mw,
                    price: parsed.price || f.price,
                    start_date: parsed.start_date || f.start_date,
                    end_date: parsed.end_date || f.end_date,
                    profile: parsed.profile?.toUpperCase() || f.profile,
                }))
                setResult({ ok: true, msg: 'AI parsed trade — review and submit' })
            } else {
                setResult({ ok: false, msg: 'Could not parse AI response into trade fields' })
            }
        } catch {
            setResult({ ok: false, msg: 'AI parsing failed' })
        } finally {
            setAiParsing(false)
        }
    }, [aiText])

    return (
        <div className="p-6 max-w-6xl mx-auto space-y-6">
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-2xl font-bold text-gray-100">Deal Capture</h1>
                    <p className="text-sm text-gray-400 mt-1">Enter new trades, import CSV, or use AI-assisted entry</p>
                </div>
            </div>

            {/* AI-Assisted Entry */}
            <div className="bg-gray-800 rounded-lg p-4">
                <div className="flex items-center gap-2 mb-3">
                    <Sparkles size={18} className="text-blue-400" />
                    <span className="text-sm font-medium text-gray-200">AI-Assisted Entry</span>
                </div>
                <div className="flex gap-2">
                    <input
                        type="text"
                        className="flex-1 bg-gray-900 text-gray-100 rounded px-3 py-2 text-sm border border-gray-700 focus:border-blue-500 focus:outline-none"
                        placeholder='e.g. "50MW peak swap VIC Q3 2026 at $85"'
                        value={aiText}
                        onChange={e => setAiText(e.target.value)}
                        onKeyDown={e => e.key === 'Enter' && handleAiParse()}
                    />
                    <button
                        className="px-4 py-2 bg-blue-600 text-white rounded text-sm hover:bg-blue-500 disabled:opacity-50 flex items-center gap-1"
                        onClick={handleAiParse}
                        disabled={aiParsing || !aiText.trim()}
                    >
                        {aiParsing ? 'Parsing...' : <><Send size={14} /> Parse</>}
                    </button>
                </div>
            </div>

            {/* Trade Entry Form */}
            <div className="bg-gray-800 rounded-lg p-6">
                <h2 className="text-lg font-semibold text-gray-100 mb-4">New Trade</h2>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                    <div>
                        <label className="block text-xs text-gray-400 mb-1">Contract Type</label>
                        <select className="w-full bg-gray-900 text-gray-100 rounded px-3 py-2 text-sm border border-gray-700"
                            value={form.trade_type} onChange={e => setField('trade_type', e.target.value)}>
                            {TRADE_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
                        </select>
                    </div>
                    <div>
                        <label className="block text-xs text-gray-400 mb-1">Region</label>
                        <select className="w-full bg-gray-900 text-gray-100 rounded px-3 py-2 text-sm border border-gray-700"
                            value={form.region} onChange={e => setField('region', e.target.value)}>
                            {REGIONS.map(r => <option key={r} value={r}>{r}</option>)}
                        </select>
                    </div>
                    <div>
                        <label className="block text-xs text-gray-400 mb-1">Direction</label>
                        <div className="flex gap-1">
                            {['BUY', 'SELL'].map(d => (
                                <button key={d}
                                    className={`flex-1 py-2 rounded text-sm font-medium ${form.buy_sell === d
                                        ? d === 'BUY' ? 'bg-emerald-600 text-white' : 'bg-red-600 text-white'
                                        : 'bg-gray-700 text-gray-300'}`}
                                    onClick={() => setField('buy_sell', d)}>{d}</button>
                            ))}
                        </div>
                    </div>
                    <div>
                        <label className="block text-xs text-gray-400 mb-1">Profile</label>
                        <select className="w-full bg-gray-900 text-gray-100 rounded px-3 py-2 text-sm border border-gray-700"
                            value={form.profile} onChange={e => setField('profile', e.target.value)}>
                            {PROFILES.map(p => <option key={p} value={p}>{p.replace('_', ' ')}</option>)}
                        </select>
                    </div>
                    <div>
                        <label className="block text-xs text-gray-400 mb-1">Volume (MW)</label>
                        <input type="number" className="w-full bg-gray-900 text-gray-100 rounded px-3 py-2 text-sm border border-gray-700"
                            value={form.volume_mw} onChange={e => setField('volume_mw', parseFloat(e.target.value) || 0)} />
                    </div>
                    <div>
                        <label className="block text-xs text-gray-400 mb-1">Price ($/MWh)</label>
                        <input type="number" step="0.01" className="w-full bg-gray-900 text-gray-100 rounded px-3 py-2 text-sm border border-gray-700"
                            value={form.price} onChange={e => setField('price', parseFloat(e.target.value) || 0)} />
                    </div>
                    <div>
                        <label className="block text-xs text-gray-400 mb-1">Start Date</label>
                        <input type="date" className="w-full bg-gray-900 text-gray-100 rounded px-3 py-2 text-sm border border-gray-700"
                            value={form.start_date} onChange={e => setField('start_date', e.target.value)} />
                    </div>
                    <div>
                        <label className="block text-xs text-gray-400 mb-1">End Date</label>
                        <input type="date" className="w-full bg-gray-900 text-gray-100 rounded px-3 py-2 text-sm border border-gray-700"
                            value={form.end_date} onChange={e => setField('end_date', e.target.value)} />
                    </div>
                    <div>
                        <label className="block text-xs text-gray-400 mb-1">Counterparty</label>
                        <select className="w-full bg-gray-900 text-gray-100 rounded px-3 py-2 text-sm border border-gray-700"
                            value={form.counterparty_id || ''} onChange={e => setField('counterparty_id', e.target.value)}>
                            <option value="">-- None --</option>
                            {counterparties.map(c => <option key={c.counterparty_id} value={c.counterparty_id}>{c.name}</option>)}
                        </select>
                    </div>
                    <div>
                        <label className="block text-xs text-gray-400 mb-1">Portfolio</label>
                        <select className="w-full bg-gray-900 text-gray-100 rounded px-3 py-2 text-sm border border-gray-700"
                            value={form.portfolio_id || ''} onChange={e => setField('portfolio_id', e.target.value)}>
                            <option value="">-- None --</option>
                            {portfolios.map(p => <option key={p.portfolio_id} value={p.portfolio_id}>{p.name}</option>)}
                        </select>
                    </div>
                    <div className="col-span-2">
                        <label className="block text-xs text-gray-400 mb-1">Notes</label>
                        <input type="text" className="w-full bg-gray-900 text-gray-100 rounded px-3 py-2 text-sm border border-gray-700"
                            value={form.notes || ''} onChange={e => setField('notes', e.target.value)} placeholder="Optional notes" />
                    </div>
                </div>
                <div className="flex items-center gap-4 mt-6">
                    <button
                        className="px-6 py-2 bg-blue-600 text-white rounded font-medium hover:bg-blue-500 disabled:opacity-50 flex items-center gap-2"
                        onClick={handleSubmit}
                        disabled={submitting}
                    >
                        <Plus size={16} />
                        {submitting ? 'Creating...' : 'Create Trade'}
                    </button>
                    {result && (
                        <span className={`text-sm ${result.ok ? 'text-emerald-400' : 'text-red-400'}`}>
                            {result.msg}
                        </span>
                    )}
                </div>
            </div>

            {/* Bulk CSV Import */}
            <div className="bg-gray-800 rounded-lg p-4">
                <div className="flex items-center gap-2 mb-3">
                    <Upload size={18} className="text-gray-400" />
                    <span className="text-sm font-medium text-gray-200">Bulk CSV Import</span>
                </div>
                <p className="text-xs text-gray-500 mb-2">
                    Columns: trade_type, region, buy_sell, volume_mw, price, start_date, end_date, profile, counterparty_id, portfolio_id, notes
                </p>
                <div className="flex gap-2 items-center">
                    <input type="file" accept=".csv" ref={fileRef}
                        className="text-sm text-gray-400 file:mr-4 file:py-1.5 file:px-4 file:rounded file:border-0 file:text-sm file:bg-gray-700 file:text-gray-200 hover:file:bg-gray-600" />
                    <button className="px-4 py-1.5 bg-gray-700 text-gray-200 rounded text-sm hover:bg-gray-600"
                        onClick={handleBulkImport}>
                        Import
                    </button>
                </div>
                {importResult && (
                    <p className="text-sm mt-2">
                        <span className="text-emerald-400">{importResult.imported} imported</span>
                        {importResult.errors > 0 && <span className="text-red-400 ml-2">{importResult.errors} errors</span>}
                    </p>
                )}
            </div>

            {/* Trade Summary Preview */}
            <div className="bg-gray-800 rounded-lg p-4">
                <h3 className="text-sm font-medium text-gray-300 mb-2">Trade Preview</h3>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
                    <div className="bg-gray-900 rounded p-3">
                        <span className="text-gray-500">Type</span>
                        <p className="text-gray-100 font-medium">{form.trade_type}</p>
                    </div>
                    <div className="bg-gray-900 rounded p-3">
                        <span className="text-gray-500">Direction</span>
                        <p className={form.buy_sell === 'BUY' ? 'text-emerald-400 font-medium' : 'text-red-400 font-medium'}>
                            {form.buy_sell} {fmt(form.volume_mw)} MW @ ${fmt(form.price, 2)}/MWh
                        </p>
                    </div>
                    <div className="bg-gray-900 rounded p-3">
                        <span className="text-gray-500">Region / Profile</span>
                        <p className="text-gray-100 font-medium">{form.region} / {form.profile}</p>
                    </div>
                    <div className="bg-gray-900 rounded p-3">
                        <span className="text-gray-500">Period</span>
                        <p className="text-gray-100 font-medium">{form.start_date} to {form.end_date}</p>
                    </div>
                </div>
            </div>
        </div>
    )
}
