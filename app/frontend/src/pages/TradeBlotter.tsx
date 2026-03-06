import React, { useState, useEffect, useCallback } from 'react'
import { RefreshCw, ChevronDown, ChevronRight, X, Edit2 } from 'lucide-react'
import { dealApi, Trade, TradeLeg, TradeAmendment } from '../api/client'

const REGIONS = ['', 'NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1']
const TRADE_TYPES = ['', 'SPOT', 'FORWARD', 'SWAP', 'FUTURE', 'OPTION', 'PPA', 'REC']
const STATUSES = ['', 'DRAFT', 'CONFIRMED', 'SETTLED', 'CANCELLED']

function fmt(v: number, d = 0) {
    return v.toLocaleString('en-AU', { minimumFractionDigits: d, maximumFractionDigits: d })
}

interface AmendModalProps {
    trade: Trade
    onClose: () => void
    onSaved: () => void
}

function AmendModal({ trade, onClose, onSaved }: AmendModalProps) {
    const [volume, setVolume] = useState(trade.volume_mw)
    const [price, setPrice] = useState(trade.price)
    const [status, setStatus] = useState(trade.status)
    const [notes, setNotes] = useState(trade.notes || '')
    const [saving, setSaving] = useState(false)
    const [error, setError] = useState('')

    const handleSave = useCallback(async () => {
        setSaving(true)
        setError('')
        try {
            const updates: Record<string, any> = { amended_by: 'user' }
            if (volume !== trade.volume_mw) updates.volume_mw = volume
            if (price !== trade.price) updates.price = price
            if (status !== trade.status) updates.status = status
            if (notes !== (trade.notes || '')) updates.notes = notes
            await dealApi.updateTrade(trade.trade_id, updates)
            onSaved()
            onClose()
        } catch (e: any) {
            setError(e.message || 'Failed to save')
        } finally {
            setSaving(false)
        }
    }, [volume, price, status, notes, trade, onClose, onSaved])

    return (
        <div className="fixed inset-0 bg-black/60 z-50 flex items-center justify-center" onClick={onClose}>
            <div className="bg-gray-800 rounded-lg p-6 w-full max-w-md" onClick={e => e.stopPropagation()}>
                <div className="flex items-center justify-between mb-4">
                    <h3 className="text-lg font-semibold text-gray-100">Amend Trade</h3>
                    <button onClick={onClose} className="text-gray-400 hover:text-gray-200"><X size={18} /></button>
                </div>
                <p className="text-xs text-gray-500 mb-4">
                    {trade.trade_type} {trade.buy_sell} {trade.region} — ID: {trade.trade_id.slice(0, 8)}...
                </p>
                <div className="space-y-3">
                    <div>
                        <label className="block text-xs text-gray-400 mb-1">Volume (MW)</label>
                        <input type="number" className="w-full bg-gray-900 text-gray-100 rounded px-3 py-2 text-sm border border-gray-700"
                            value={volume} onChange={e => setVolume(parseFloat(e.target.value) || 0)} />
                    </div>
                    <div>
                        <label className="block text-xs text-gray-400 mb-1">Price ($/MWh)</label>
                        <input type="number" step="0.01" className="w-full bg-gray-900 text-gray-100 rounded px-3 py-2 text-sm border border-gray-700"
                            value={price} onChange={e => setPrice(parseFloat(e.target.value) || 0)} />
                    </div>
                    <div>
                        <label className="block text-xs text-gray-400 mb-1">Status</label>
                        <select className="w-full bg-gray-900 text-gray-100 rounded px-3 py-2 text-sm border border-gray-700"
                            value={status} onChange={e => setStatus(e.target.value)}>
                            {STATUSES.filter(Boolean).map(s => <option key={s} value={s}>{s}</option>)}
                        </select>
                    </div>
                    <div>
                        <label className="block text-xs text-gray-400 mb-1">Notes</label>
                        <input type="text" className="w-full bg-gray-900 text-gray-100 rounded px-3 py-2 text-sm border border-gray-700"
                            value={notes} onChange={e => setNotes(e.target.value)} />
                    </div>
                </div>
                {error && <p className="text-red-400 text-sm mt-2">{error}</p>}
                <div className="flex gap-2 mt-4">
                    <button className="flex-1 py-2 bg-blue-600 text-white rounded text-sm hover:bg-blue-500 disabled:opacity-50"
                        onClick={handleSave} disabled={saving}>
                        {saving ? 'Saving...' : 'Save Changes'}
                    </button>
                    <button className="px-4 py-2 bg-gray-700 text-gray-300 rounded text-sm hover:bg-gray-600" onClick={onClose}>
                        Cancel
                    </button>
                </div>
            </div>
        </div>
    )
}

interface ExpandedRowProps {
    trade: Trade
}

function ExpandedRow({ trade }: ExpandedRowProps) {
    const [legs, setLegs] = useState<TradeLeg[]>([])
    const [amendments, setAmendments] = useState<TradeAmendment[]>([])

    useEffect(() => {
        dealApi.getTradeLegs(trade.trade_id).then(r => setLegs(r.legs)).catch(() => {})
        dealApi.getTradeAmendments(trade.trade_id).then(r => setAmendments(r.amendments)).catch(() => {})
    }, [trade.trade_id])

    return (
        <tr>
            <td colSpan={10} className="p-4 bg-gray-900/50">
                <div className="grid grid-cols-2 gap-4">
                    <div>
                        <h4 className="text-xs font-semibold text-gray-400 mb-2">Settlement Legs ({legs.length})</h4>
                        {legs.length > 0 ? (
                            <div className="max-h-40 overflow-y-auto">
                                <table className="w-full text-xs">
                                    <thead>
                                        <tr className="text-gray-500">
                                            <th className="text-left py-1">Date</th>
                                            <th className="text-right py-1">MW</th>
                                            <th className="text-right py-1">$/MWh</th>
                                            <th className="text-right py-1">Factor</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {legs.slice(0, 20).map(l => (
                                            <tr key={l.leg_id} className="text-gray-400 border-t border-gray-800">
                                                <td className="py-1">{l.settlement_date}</td>
                                                <td className="py-1 text-right">{fmt(l.volume_mw, 1)}</td>
                                                <td className="py-1 text-right">${fmt(l.price, 2)}</td>
                                                <td className="py-1 text-right">{l.profile_factor.toFixed(2)}</td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                                {legs.length > 20 && <p className="text-xs text-gray-600 mt-1">...and {legs.length - 20} more</p>}
                            </div>
                        ) : <p className="text-xs text-gray-600">No legs generated</p>}
                    </div>
                    <div>
                        <h4 className="text-xs font-semibold text-gray-400 mb-2">Amendment History ({amendments.length})</h4>
                        {amendments.length > 0 ? (
                            <div className="max-h-40 overflow-y-auto space-y-1">
                                {amendments.map(a => (
                                    <div key={a.amendment_id} className="text-xs bg-gray-800 rounded p-2">
                                        <span className="text-blue-400">{a.field_changed}</span>
                                        <span className="text-gray-500"> changed from </span>
                                        <span className="text-red-400">{a.old_value || '(empty)'}</span>
                                        <span className="text-gray-500"> to </span>
                                        <span className="text-emerald-400">{a.new_value}</span>
                                        <span className="text-gray-600 ml-2">by {a.amended_by} at {a.amended_at}</span>
                                    </div>
                                ))}
                            </div>
                        ) : <p className="text-xs text-gray-600">No amendments</p>}
                    </div>
                </div>
            </td>
        </tr>
    )
}

export default function TradeBlotter() {
    const [trades, setTrades] = useState<Trade[]>([])
    const [total, setTotal] = useState(0)
    const [loading, setLoading] = useState(false)
    const [expandedId, setExpandedId] = useState<string | null>(null)
    const [amendTrade, setAmendTrade] = useState<Trade | null>(null)

    // Filters
    const [filterRegion, setFilterRegion] = useState('')
    const [filterType, setFilterType] = useState('')
    const [filterStatus, setFilterStatus] = useState('')

    const loadTrades = useCallback(async () => {
        setLoading(true)
        try {
            const params: Record<string, string> = {}
            if (filterRegion) params.region = filterRegion
            if (filterType) params.trade_type = filterType
            if (filterStatus) params.status = filterStatus
            const res = await dealApi.getTrades(params as any)
            setTrades(res.trades)
            setTotal(res.total)
        } catch {
            setTrades([])
        } finally {
            setLoading(false)
        }
    }, [filterRegion, filterType, filterStatus])

    useEffect(() => { loadTrades() }, [loadTrades])

    const handleCancel = useCallback(async (trade: Trade) => {
        if (!confirm(`Cancel trade ${trade.trade_id.slice(0, 8)}...?`)) return
        try {
            await dealApi.deleteTrade(trade.trade_id)
            loadTrades()
        } catch {}
    }, [loadTrades])

    return (
        <div className="p-6 max-w-7xl mx-auto space-y-4">
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-2xl font-bold text-gray-100">Trade Blotter</h1>
                    <p className="text-sm text-gray-400 mt-1">{total} trades across all portfolios</p>
                </div>
                <button className="px-3 py-1.5 bg-gray-700 text-gray-200 rounded text-sm hover:bg-gray-600 flex items-center gap-1"
                    onClick={loadTrades} disabled={loading}>
                    <RefreshCw size={14} className={loading ? 'animate-spin' : ''} /> Refresh
                </button>
            </div>

            {/* Filters */}
            <div className="flex gap-3 items-center">
                <select className="bg-gray-800 text-gray-100 rounded px-3 py-1.5 text-sm border border-gray-700"
                    value={filterRegion} onChange={e => setFilterRegion(e.target.value)}>
                    <option value="">All Regions</option>
                    {REGIONS.filter(Boolean).map(r => <option key={r} value={r}>{r}</option>)}
                </select>
                <select className="bg-gray-800 text-gray-100 rounded px-3 py-1.5 text-sm border border-gray-700"
                    value={filterType} onChange={e => setFilterType(e.target.value)}>
                    <option value="">All Types</option>
                    {TRADE_TYPES.filter(Boolean).map(t => <option key={t} value={t}>{t}</option>)}
                </select>
                <select className="bg-gray-800 text-gray-100 rounded px-3 py-1.5 text-sm border border-gray-700"
                    value={filterStatus} onChange={e => setFilterStatus(e.target.value)}>
                    <option value="">All Statuses</option>
                    {STATUSES.filter(Boolean).map(s => <option key={s} value={s}>{s}</option>)}
                </select>
            </div>

            {/* Trade Table */}
            <div className="bg-gray-800 rounded-lg overflow-hidden">
                <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                        <thead>
                            <tr className="text-gray-400 border-b border-gray-700 bg-gray-800">
                                <th className="w-8 py-2 px-2"></th>
                                <th className="text-left py-2 px-2">Type</th>
                                <th className="text-left py-2 px-2">Region</th>
                                <th className="text-left py-2 px-2">B/S</th>
                                <th className="text-right py-2 px-2">MW</th>
                                <th className="text-right py-2 px-2">$/MWh</th>
                                <th className="text-left py-2 px-2">Profile</th>
                                <th className="text-left py-2 px-2">Period</th>
                                <th className="text-left py-2 px-2">Status</th>
                                <th className="text-left py-2 px-2">Actions</th>
                            </tr>
                        </thead>
                        <tbody>
                            {trades.map(t => (
                                <React.Fragment key={t.trade_id}>
                                    <tr className="border-b border-gray-700/50 hover:bg-gray-700/30 cursor-pointer"
                                        onClick={() => setExpandedId(expandedId === t.trade_id ? null : t.trade_id)}>
                                        <td className="py-2 px-2 text-gray-500">
                                            {expandedId === t.trade_id ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                                        </td>
                                        <td className="py-2 px-2">
                                            <span className="px-2 py-0.5 rounded text-xs bg-gray-700 text-gray-200">{t.trade_type}</span>
                                        </td>
                                        <td className="py-2 px-2 text-gray-200">{t.region}</td>
                                        <td className={`py-2 px-2 font-medium ${t.buy_sell === 'BUY' ? 'text-emerald-400' : 'text-red-400'}`}>
                                            {t.buy_sell}
                                        </td>
                                        <td className="py-2 px-2 text-right text-gray-200">{fmt(t.volume_mw, 1)}</td>
                                        <td className="py-2 px-2 text-right text-gray-200">${fmt(t.price, 2)}</td>
                                        <td className="py-2 px-2 text-gray-400">{t.profile}</td>
                                        <td className="py-2 px-2 text-gray-400 text-xs">{t.start_date} — {t.end_date}</td>
                                        <td className="py-2 px-2">
                                            <span className={`px-2 py-0.5 rounded text-xs ${
                                                t.status === 'CONFIRMED' ? 'bg-emerald-900/50 text-emerald-400' :
                                                t.status === 'CANCELLED' ? 'bg-red-900/50 text-red-400' :
                                                t.status === 'SETTLED' ? 'bg-blue-900/50 text-blue-400' :
                                                'bg-gray-700 text-gray-400'
                                            }`}>{t.status}</span>
                                        </td>
                                        <td className="py-2 px-2" onClick={e => e.stopPropagation()}>
                                            {t.status !== 'CANCELLED' && (
                                                <div className="flex gap-1">
                                                    <button className="px-2 py-1 bg-gray-700 text-gray-300 rounded text-xs hover:bg-gray-600"
                                                        onClick={() => setAmendTrade(t)}>
                                                        <Edit2 size={12} />
                                                    </button>
                                                    <button className="px-2 py-1 bg-red-900/50 text-red-400 rounded text-xs hover:bg-red-900/80"
                                                        onClick={() => handleCancel(t)}>
                                                        <X size={12} />
                                                    </button>
                                                </div>
                                            )}
                                        </td>
                                    </tr>
                                    {expandedId === t.trade_id && <ExpandedRow trade={t} />}
                                </React.Fragment>
                            ))}
                            {trades.length === 0 && !loading && (
                                <tr>
                                    <td colSpan={10} className="py-8 text-center text-gray-500">
                                        No trades found. Use Deal Capture to create trades.
                                    </td>
                                </tr>
                            )}
                        </tbody>
                    </table>
                </div>
            </div>

            {/* Amend Modal */}
            {amendTrade && (
                <AmendModal trade={amendTrade} onClose={() => setAmendTrade(null)} onSaved={loadTrades} />
            )}
        </div>
    )
}
