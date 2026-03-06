import React, { useState, useEffect, useCallback } from 'react'
import {
    BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
    ResponsiveContainer, Cell,
} from 'recharts'
import { Briefcase, TrendingUp, TrendingDown, Grid } from 'lucide-react'
import {
    dealApi, Portfolio as PortfolioType, PortfolioPosition, PnLEntry, ExposureCell, Trade,
} from '../api/client'

function fmt(v: number, d = 0) {
    return v.toLocaleString('en-AU', { minimumFractionDigits: d, maximumFractionDigits: d })
}

function fmtAud(v: number) {
    const abs = Math.abs(v)
    const sign = v < 0 ? '-' : ''
    if (abs >= 1_000_000) return `${sign}$${fmt(abs / 1_000_000, 2)}M`
    if (abs >= 1_000) return `${sign}$${fmt(abs / 1_000, 1)}k`
    return `${sign}$${fmt(abs, 0)}`
}

const REGION_COLORS: Record<string, string> = {
    NSW1: '#3B82F6', QLD1: '#F59E0B', VIC1: '#8B5CF6', SA1: '#EF4444', TAS1: '#10B981',
}

export default function PortfolioPage() {
    const [portfolios, setPortfolios] = useState<PortfolioType[]>([])
    const [selectedId, setSelectedId] = useState('')
    const [positions, setPositions] = useState<PortfolioPosition[]>([])
    const [pnl, setPnl] = useState<PnLEntry[]>([])
    const [exposure, setExposure] = useState<ExposureCell[]>([])
    const [trades, setTrades] = useState<Trade[]>([])
    const [loading, setLoading] = useState(false)

    useEffect(() => {
        dealApi.getPortfolios().then(r => {
            setPortfolios(r.portfolios)
            if (r.portfolios.length > 0) setSelectedId(r.portfolios[0].portfolio_id)
        }).catch(() => {})
    }, [])

    useEffect(() => {
        if (!selectedId) return
        setLoading(true)
        Promise.all([
            dealApi.getPortfolioPosition(selectedId).then(r => setPositions(r.positions)).catch(() => setPositions([])),
            dealApi.getPortfolioPnL(selectedId).then(r => setPnl(r.pnl)).catch(() => setPnl([])),
            dealApi.getExposureHeatmap(selectedId).then(r => setExposure(r.exposure)).catch(() => setExposure([])),
            dealApi.getTrades({ portfolio_id: selectedId, limit: 200 }).then(r => setTrades(r.trades)).catch(() => setTrades([])),
        ]).finally(() => setLoading(false))
    }, [selectedId])

    const totalPnl = pnl.reduce((s, p) => s + p.total, 0)
    const totalNetMw = pnl.reduce((s, p) => s + p.net_mw, 0)

    // Build exposure grid
    const months = Array.from(new Set(exposure.map(e => e.month))).sort()
    const regions = Array.from(new Set(exposure.map(e => e.region)))
    const expMap: Record<string, Record<string, number>> = {}
    exposure.forEach(e => {
        if (!expMap[e.region]) expMap[e.region] = {}
        expMap[e.region][e.month] = e.net_mw
    })

    return (
        <div className="p-6 max-w-7xl mx-auto space-y-6">
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-2xl font-bold text-gray-100">Portfolio Management</h1>
                    <p className="text-sm text-gray-400 mt-1">Position summary, P&L, exposure heatmap, and trade blotter</p>
                </div>
                <select
                    className="bg-gray-800 text-gray-100 rounded px-3 py-2 text-sm border border-gray-700"
                    value={selectedId}
                    onChange={e => setSelectedId(e.target.value)}
                >
                    {portfolios.map(p => (
                        <option key={p.portfolio_id} value={p.portfolio_id}>{p.name}</option>
                    ))}
                </select>
            </div>

            {loading && <p className="text-sm text-gray-500">Loading portfolio data...</p>}

            {/* KPI Cards */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <div className="bg-gray-800 rounded-lg p-4">
                    <p className="text-xs text-gray-400">Total P&L</p>
                    <p className={`text-2xl font-bold mt-1 ${totalPnl >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                        {fmtAud(totalPnl)}
                    </p>
                </div>
                <div className="bg-gray-800 rounded-lg p-4">
                    <p className="text-xs text-gray-400">Net Position</p>
                    <p className={`text-2xl font-bold mt-1 ${totalNetMw >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                        {fmt(totalNetMw, 1)} MW
                    </p>
                </div>
                <div className="bg-gray-800 rounded-lg p-4">
                    <p className="text-xs text-gray-400">Active Trades</p>
                    <p className="text-2xl font-bold mt-1 text-gray-100">{trades.filter(t => t.status !== 'CANCELLED').length}</p>
                </div>
                <div className="bg-gray-800 rounded-lg p-4">
                    <p className="text-xs text-gray-400">Regions</p>
                    <p className="text-2xl font-bold mt-1 text-gray-100">{new Set(trades.map(t => t.region)).size}</p>
                </div>
            </div>

            {/* Position Summary */}
            {positions.length > 0 && (
                <div className="bg-gray-800 rounded-lg p-4">
                    <h2 className="text-sm font-semibold text-gray-200 mb-3">Position Summary — Net MW by Region & Quarter</h2>
                    <div className="overflow-x-auto">
                        <table className="w-full text-sm">
                            <thead>
                                <tr className="text-gray-400 border-b border-gray-700">
                                    <th className="text-left py-2 px-3">Region</th>
                                    <th className="text-left py-2 px-3">Quarter</th>
                                    <th className="text-right py-2 px-3">Net MW</th>
                                    <th className="text-right py-2 px-3">Gross MW</th>
                                    <th className="text-right py-2 px-3">Trades</th>
                                </tr>
                            </thead>
                            <tbody>
                                {positions.map((p, i) => (
                                    <tr key={i} className="border-b border-gray-700/50 hover:bg-gray-700/30">
                                        <td className="py-2 px-3 text-gray-200">{p.region}</td>
                                        <td className="py-2 px-3 text-gray-300">{p.quarter}</td>
                                        <td className={`py-2 px-3 text-right font-medium ${p.net_mw >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                                            {fmt(p.net_mw, 1)}
                                        </td>
                                        <td className="py-2 px-3 text-right text-gray-300">{fmt(p.gross_mw, 1)}</td>
                                        <td className="py-2 px-3 text-right text-gray-400">{p.trade_count}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                </div>
            )}

            {/* P&L Bar Chart */}
            {pnl.length > 0 && (
                <div className="bg-gray-800 rounded-lg p-4">
                    <h2 className="text-sm font-semibold text-gray-200 mb-3">P&L by Region</h2>
                    <ResponsiveContainer width="100%" height={280}>
                        <BarChart data={pnl}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                            <XAxis dataKey="region" stroke="#9CA3AF" tick={{ fontSize: 12 }} />
                            <YAxis stroke="#9CA3AF" tick={{ fontSize: 12 }} tickFormatter={v => fmtAud(v)} />
                            <Tooltip
                                contentStyle={{ backgroundColor: '#1F2937', border: 'none', borderRadius: 8 }}
                                labelStyle={{ color: '#E5E7EB' }}
                                formatter={(v: number) => fmtAud(v)}
                            />
                            <Legend />
                            <Bar dataKey="realized" name="Realized" fill="#10B981" radius={[4, 4, 0, 0]} />
                            <Bar dataKey="unrealized" name="Unrealized" fill="#6366F1" radius={[4, 4, 0, 0]} />
                        </BarChart>
                    </ResponsiveContainer>
                </div>
            )}

            {/* Exposure Heatmap */}
            {exposure.length > 0 && (
                <div className="bg-gray-800 rounded-lg p-4">
                    <h2 className="text-sm font-semibold text-gray-200 mb-3">Exposure Heatmap — Net MW</h2>
                    <div className="overflow-x-auto">
                        <table className="w-full text-sm">
                            <thead>
                                <tr className="text-gray-400 border-b border-gray-700">
                                    <th className="text-left py-2 px-2">Region</th>
                                    {months.map(m => <th key={m} className="text-center py-2 px-2">{m}</th>)}
                                </tr>
                            </thead>
                            <tbody>
                                {regions.map(region => (
                                    <tr key={region} className="border-b border-gray-700/50">
                                        <td className="py-2 px-2 text-gray-200 font-medium">{region}</td>
                                        {months.map(m => {
                                            const val = expMap[region]?.[m] || 0
                                            const maxAbs = Math.max(...exposure.map(e => Math.abs(e.net_mw)), 1)
                                            const intensity = Math.min(Math.abs(val) / maxAbs, 1)
                                            const bg = val > 0
                                                ? `rgba(16, 185, 129, ${intensity * 0.6})`
                                                : val < 0
                                                ? `rgba(239, 68, 68, ${intensity * 0.6})`
                                                : 'transparent'
                                            return (
                                                <td key={m} className="py-2 px-2 text-center" style={{ backgroundColor: bg }}>
                                                    <span className={val === 0 ? 'text-gray-600' : 'text-gray-100'}>
                                                        {fmt(val, 0)}
                                                    </span>
                                                </td>
                                            )
                                        })}
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                </div>
            )}

            {/* Trade Blotter */}
            <div className="bg-gray-800 rounded-lg p-4">
                <h2 className="text-sm font-semibold text-gray-200 mb-3">Trade Blotter ({trades.length} trades)</h2>
                <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                        <thead>
                            <tr className="text-gray-400 border-b border-gray-700">
                                <th className="text-left py-2 px-2">Type</th>
                                <th className="text-left py-2 px-2">Region</th>
                                <th className="text-left py-2 px-2">B/S</th>
                                <th className="text-right py-2 px-2">MW</th>
                                <th className="text-right py-2 px-2">$/MWh</th>
                                <th className="text-left py-2 px-2">Profile</th>
                                <th className="text-left py-2 px-2">Period</th>
                                <th className="text-left py-2 px-2">Status</th>
                                <th className="text-left py-2 px-2">Counterparty</th>
                            </tr>
                        </thead>
                        <tbody>
                            {trades.map(t => (
                                <tr key={t.trade_id} className="border-b border-gray-700/50 hover:bg-gray-700/30">
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
                                    <td className="py-2 px-2 text-gray-400">{t.counterparty_name || '—'}</td>
                                </tr>
                            ))}
                            {trades.length === 0 && (
                                <tr>
                                    <td colSpan={9} className="py-8 text-center text-gray-500">
                                        No trades in this portfolio. Use Deal Capture to add trades.
                                    </td>
                                </tr>
                            )}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    )
}
