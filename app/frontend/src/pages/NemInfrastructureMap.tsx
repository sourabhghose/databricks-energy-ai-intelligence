import React, { useState, useEffect, useCallback, useMemo } from 'react'
import { MapContainer, TileLayer, CircleMarker, Polyline, Popup, useMap } from 'react-leaflet'
import { Filter, Layers, X, ChevronRight, Zap, RefreshCw } from 'lucide-react'
import 'leaflet/dist/leaflet.css'
import { mapApi, FacilityLocation, MapLayer } from '../api/client'

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
const AUSTRALIA_CENTER: [number, number] = [-25.5, 134.5]
const DEFAULT_ZOOM = 5

const FUEL_COLORS: Record<string, string> = {
  wind: '#22c55e',
  solar_utility: '#eab308',
  solar_rooftop: '#fbbf24',
  coal_black: '#1e293b',
  coal_brown: '#78350f',
  gas_ccgt: '#3b82f6',
  gas_ocgt: '#60a5fa',
  gas_steam: '#93c5fd',
  gas_recip: '#93c5fd',
  hydro: '#06b6d4',
  pumped_hydro: '#0891b2',
  battery: '#a855f7',
  bioenergy: '#84cc16',
  distillate: '#f97316',
  region_centroid: '#6b7280',
  gas_hub: '#ef4444',
  rez: '#f97316',
  isp_transmission: '#ec4899',
  unknown: '#9ca3af',
}

const FUEL_LABELS: Record<string, string> = {
  wind: 'Wind',
  solar_utility: 'Solar',
  coal_black: 'Coal (Black)',
  coal_brown: 'Coal (Brown)',
  gas_ccgt: 'Gas CCGT',
  gas_ocgt: 'Gas OCGT',
  gas_steam: 'Gas Steam',
  hydro: 'Hydro',
  pumped_hydro: 'Pumped Hydro',
  battery: 'Battery',
  bioenergy: 'Bioenergy',
  distillate: 'Distillate',
  gas_hub: 'Gas Hub',
  rez: 'REZ',
  isp_transmission: 'ISP Project',
  region_centroid: 'Region',
}

const LAYER_TYPES = ['generator', 'rez', 'isp', 'gas_hub']

const INTERCONNECTORS: { id: string; from: string; to: string; label: string }[] = [
  { id: 'QNI', from: 'REGION_QLD1', to: 'REGION_NSW1', label: 'QNI' },
  { id: 'VIC-NSW', from: 'REGION_VIC1', to: 'REGION_NSW1', label: 'VIC-NSW' },
  { id: 'VIC-SA', from: 'REGION_VIC1', to: 'REGION_SA1', label: 'Heywood' },
  { id: 'TAS-VIC', from: 'REGION_TAS1', to: 'REGION_VIC1', label: 'Basslink' },
]

// ---------------------------------------------------------------------------
// Radius helper — scale circle by capacity
// ---------------------------------------------------------------------------
function markerRadius(capacity_mw: number, layer_type: string): number {
  if (layer_type === 'gas_hub') return 8
  if (layer_type === 'region') return 10
  if (layer_type === 'rez') return 10
  if (layer_type === 'isp') return 8
  if (capacity_mw <= 0) return 3
  if (capacity_mw < 50) return 4
  if (capacity_mw < 200) return 6
  if (capacity_mw < 500) return 8
  return 10
}

// ---------------------------------------------------------------------------
// Map auto-bounds component
// ---------------------------------------------------------------------------
function MapBounds({ facilities }: { facilities: FacilityLocation[] }) {
  const map = useMap()
  useEffect(() => {
    if (facilities.length === 0) return
    const lats = facilities.map(f => f.lat)
    const lngs = facilities.map(f => f.lng)
    const bounds: [[number, number], [number, number]] = [
      [Math.min(...lats) - 1, Math.min(...lngs) - 1],
      [Math.max(...lats) + 1, Math.max(...lngs) + 1],
    ]
    map.fitBounds(bounds, { padding: [30, 30], maxZoom: 8 })
  }, [facilities, map])
  return null
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------
export default function NemInfrastructureMap() {
  const [facilities, setFacilities] = useState<FacilityLocation[]>([])
  const [layers, setLayers] = useState<MapLayer[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  // Filters
  const [filterRegion, setFilterRegion] = useState<string>('')
  const [filterFuelType, setFilterFuelType] = useState<string>('')
  const [filterMinCapacity, setFilterMinCapacity] = useState<number>(0)
  const [showFilters, setShowFilters] = useState(false)

  // Layer toggles
  const [enabledLayers, setEnabledLayers] = useState<Set<string>>(new Set(LAYER_TYPES))
  const [showInterconnectors, setShowInterconnectors] = useState(true)

  // Detail panel
  const [selectedFacility, setSelectedFacility] = useState<FacilityLocation | null>(null)
  const [detailData, setDetailData] = useState<{ facility: FacilityLocation; generation: { timestamp: string; mw: number }[] } | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)

  // ---------------------------------------------------------------------------
  // Data fetch
  // ---------------------------------------------------------------------------
  const fetchData = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const [facRes, layerRes] = await Promise.all([
        mapApi.facilities({ fuel_type: filterFuelType || undefined, region: filterRegion || undefined, min_capacity_mw: filterMinCapacity }),
        mapApi.layers(),
      ])
      setFacilities(facRes.facilities)
      setLayers(layerRes.layers)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to load map data')
    } finally {
      setLoading(false)
    }
  }, [filterRegion, filterFuelType, filterMinCapacity])

  useEffect(() => { fetchData() }, [fetchData])

  // ---------------------------------------------------------------------------
  // Facility detail
  // ---------------------------------------------------------------------------
  const openDetail = useCallback(async (fac: FacilityLocation) => {
    setSelectedFacility(fac)
    setDetailLoading(true)
    try {
      const data = await mapApi.facilityDetail(fac.duid)
      setDetailData(data)
    } catch {
      setDetailData(null)
    } finally {
      setDetailLoading(false)
    }
  }, [])

  // ---------------------------------------------------------------------------
  // Filtered facilities
  // ---------------------------------------------------------------------------
  const visibleFacilities = useMemo(
    () => facilities.filter(f => enabledLayers.has(f.layer_type)),
    [facilities, enabledLayers]
  )

  // Build region centroid lookup for interconnector lines
  const regionCoords = useMemo(() => {
    const map: Record<string, [number, number]> = {}
    for (const f of facilities) {
      if (f.layer_type === 'region') {
        map[f.duid] = [f.lat, f.lng]
      }
    }
    return map
  }, [facilities])

  // ---------------------------------------------------------------------------
  // Layer toggle
  // ---------------------------------------------------------------------------
  const toggleLayer = (lt: string) => {
    setEnabledLayers(prev => {
      const next = new Set(prev)
      if (next.has(lt)) next.delete(lt)
      else next.add(lt)
      return next
    })
  }

  // ---------------------------------------------------------------------------
  // Legend data
  // ---------------------------------------------------------------------------
  const legendItems = useMemo(() => {
    const map: Record<string, { count: number; mw: number }> = {}
    for (const f of visibleFacilities) {
      if (f.layer_type !== 'generator') continue
      if (!map[f.fuel_type]) map[f.fuel_type] = { count: 0, mw: 0 }
      map[f.fuel_type].count++
      map[f.fuel_type].mw += f.capacity_mw
    }
    return Object.entries(map)
      .sort((a, b) => b[1].mw - a[1].mw)
      .map(([ft, { count, mw }]) => ({ fuel_type: ft, count, mw }))
  }, [visibleFacilities])

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------
  return (
    <div className="flex h-[calc(100vh-4rem)] relative">
      {/* Left sidebar — layer controls + legend */}
      <div className="w-72 bg-gray-900 border-r border-gray-700 flex flex-col overflow-y-auto shrink-0">
        {/* Header */}
        <div className="p-4 border-b border-gray-700">
          <h2 className="text-lg font-bold text-white flex items-center gap-2">
            <Zap size={18} className="text-yellow-400" />
            NEM Infrastructure Map
          </h2>
          <p className="text-xs text-gray-400 mt-1">
            {visibleFacilities.length.toLocaleString()} facilities shown
          </p>
        </div>

        {/* Layer toggles */}
        <div className="p-4 border-b border-gray-700 space-y-2">
          <div className="flex items-center gap-2 text-sm font-semibold text-gray-300 mb-2">
            <Layers size={14} />
            Layers
          </div>
          {[
            { key: 'generator', label: 'Generators', color: '#22c55e' },
            { key: 'rez', label: 'REZ Zones', color: '#f97316' },
            { key: 'isp', label: 'ISP Projects', color: '#ec4899' },
            { key: 'gas_hub', label: 'Gas Hubs', color: '#ef4444' },
          ].map(({ key, label, color }) => (
            <label key={key} className="flex items-center gap-2 text-sm text-gray-300 cursor-pointer hover:text-white">
              <input
                type="checkbox"
                checked={enabledLayers.has(key)}
                onChange={() => toggleLayer(key)}
                className="rounded border-gray-600 bg-gray-800 text-blue-500 focus:ring-blue-500"
              />
              <span className="w-3 h-3 rounded-full inline-block" style={{ backgroundColor: color }} />
              {label}
            </label>
          ))}
          <label className="flex items-center gap-2 text-sm text-gray-300 cursor-pointer hover:text-white">
            <input
              type="checkbox"
              checked={showInterconnectors}
              onChange={() => setShowInterconnectors(v => !v)}
              className="rounded border-gray-600 bg-gray-800 text-blue-500 focus:ring-blue-500"
            />
            <span className="w-3 h-3 rounded-full inline-block border-2 border-gray-400" />
            Interconnectors
          </label>
        </div>

        {/* Filters */}
        <div className="p-4 border-b border-gray-700 space-y-3">
          <button
            onClick={() => setShowFilters(v => !v)}
            className="flex items-center gap-2 text-sm font-semibold text-gray-300 hover:text-white w-full"
          >
            <Filter size={14} />
            Filters
            <ChevronRight size={14} className={`ml-auto transition-transform ${showFilters ? 'rotate-90' : ''}`} />
          </button>
          {showFilters && (
            <div className="space-y-2">
              <select
                value={filterRegion}
                onChange={e => setFilterRegion(e.target.value)}
                className="w-full bg-gray-800 border border-gray-600 rounded px-2 py-1.5 text-sm text-gray-200"
              >
                <option value="">All Regions</option>
                <option value="NSW1">NSW</option>
                <option value="QLD1">QLD</option>
                <option value="VIC1">VIC</option>
                <option value="SA1">SA</option>
                <option value="TAS1">TAS</option>
              </select>
              <select
                value={filterFuelType}
                onChange={e => setFilterFuelType(e.target.value)}
                className="w-full bg-gray-800 border border-gray-600 rounded px-2 py-1.5 text-sm text-gray-200"
              >
                <option value="">All Fuel Types</option>
                {Object.entries(FUEL_LABELS).map(([k, v]) => (
                  <option key={k} value={k}>{v}</option>
                ))}
              </select>
              <div>
                <label className="text-xs text-gray-400">Min Capacity (MW)</label>
                <input
                  type="number"
                  min={0}
                  value={filterMinCapacity}
                  onChange={e => setFilterMinCapacity(Number(e.target.value) || 0)}
                  className="w-full bg-gray-800 border border-gray-600 rounded px-2 py-1.5 text-sm text-gray-200 mt-1"
                />
              </div>
              <button
                onClick={fetchData}
                className="w-full flex items-center justify-center gap-1.5 bg-blue-600 hover:bg-blue-700 text-white rounded px-3 py-1.5 text-sm font-medium"
              >
                <RefreshCw size={12} />
                Apply
              </button>
            </div>
          )}
        </div>

        {/* Legend */}
        <div className="p-4 flex-1">
          <div className="text-sm font-semibold text-gray-300 mb-2">Legend</div>
          <div className="space-y-1.5">
            {legendItems.map(({ fuel_type, count, mw }) => (
              <div key={fuel_type} className="flex items-center gap-2 text-xs text-gray-400">
                <span className="w-2.5 h-2.5 rounded-full shrink-0" style={{ backgroundColor: FUEL_COLORS[fuel_type] || '#9ca3af' }} />
                <span className="truncate flex-1">{FUEL_LABELS[fuel_type] || fuel_type}</span>
                <span className="tabular-nums text-gray-500">{count}</span>
                <span className="tabular-nums text-gray-500 w-16 text-right">{mw >= 1000 ? `${(mw / 1000).toFixed(1)}GW` : `${Math.round(mw)}MW`}</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Map */}
      <div className="flex-1 relative">
        {loading && (
          <div className="absolute inset-0 bg-gray-900/60 z-[1000] flex items-center justify-center">
            <div className="flex items-center gap-2 text-white">
              <RefreshCw size={18} className="animate-spin" />
              Loading map data...
            </div>
          </div>
        )}
        {error && (
          <div className="absolute top-4 left-1/2 -translate-x-1/2 z-[1000] bg-red-900/90 text-red-200 px-4 py-2 rounded-lg text-sm">
            {error}
          </div>
        )}

        <MapContainer
          center={AUSTRALIA_CENTER}
          zoom={DEFAULT_ZOOM}
          style={{ height: '100%', width: '100%' }}
          className="z-0"
        >
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />

          {/* Interconnector lines */}
          {showInterconnectors && INTERCONNECTORS.map(ic => {
            const from = regionCoords[ic.from]
            const to = regionCoords[ic.to]
            if (!from || !to) return null
            return (
              <Polyline
                key={ic.id}
                positions={[from, to]}
                pathOptions={{ color: '#9ca3af', weight: 2, dashArray: '8 4', opacity: 0.7 }}
              >
                <Popup>
                  <div className="text-sm font-medium">{ic.label}</div>
                </Popup>
              </Polyline>
            )
          })}

          {/* Facility markers */}
          {visibleFacilities.map(fac => (
            <CircleMarker
              key={fac.duid}
              center={[fac.lat, fac.lng]}
              radius={markerRadius(fac.capacity_mw, fac.layer_type)}
              pathOptions={{
                fillColor: FUEL_COLORS[fac.fuel_type] || '#9ca3af',
                color: fac.layer_type === 'rez' ? '#f97316' : fac.layer_type === 'isp' ? '#ec4899' : '#374151',
                weight: fac.layer_type === 'rez' || fac.layer_type === 'isp' ? 2 : 1,
                fillOpacity: 0.8,
                opacity: 0.9,
              }}
              eventHandlers={{
                click: () => openDetail(fac),
              }}
            >
              <Popup>
                <div className="min-w-[180px]">
                  <div className="font-semibold text-sm">{fac.station_name}</div>
                  <div className="text-xs text-gray-500 mt-1">
                    {FUEL_LABELS[fac.fuel_type] || fac.fuel_type} &middot; {fac.region_id}
                  </div>
                  {fac.capacity_mw > 0 && (
                    <div className="text-xs mt-0.5">{fac.capacity_mw.toLocaleString()} MW</div>
                  )}
                  <div className="text-xs text-gray-400 mt-0.5">DUID: {fac.duid}</div>
                  <button
                    onClick={() => openDetail(fac)}
                    className="mt-2 text-xs text-blue-600 hover:text-blue-800 font-medium"
                  >
                    View Details &rarr;
                  </button>
                </div>
              </Popup>
            </CircleMarker>
          ))}

          <MapBounds facilities={visibleFacilities} />
        </MapContainer>
      </div>

      {/* Detail panel */}
      {selectedFacility && (
        <div className="w-80 bg-gray-900 border-l border-gray-700 flex flex-col overflow-y-auto shrink-0">
          <div className="p-4 border-b border-gray-700 flex items-start justify-between">
            <div>
              <h3 className="font-bold text-white text-sm">{selectedFacility.station_name}</h3>
              <p className="text-xs text-gray-400 mt-0.5">{selectedFacility.duid}</p>
            </div>
            <button onClick={() => { setSelectedFacility(null); setDetailData(null) }} className="text-gray-400 hover:text-white">
              <X size={16} />
            </button>
          </div>

          <div className="p-4 space-y-3">
            {/* Info grid */}
            <div className="grid grid-cols-2 gap-2">
              {[
                ['Fuel Type', FUEL_LABELS[selectedFacility.fuel_type] || selectedFacility.fuel_type],
                ['Region', selectedFacility.region_id],
                ['State', selectedFacility.state],
                ['Capacity', selectedFacility.capacity_mw > 0 ? `${selectedFacility.capacity_mw.toLocaleString()} MW` : 'N/A'],
                ['Status', selectedFacility.status],
                ['Layer', selectedFacility.layer_type],
              ].map(([label, value]) => (
                <div key={label as string} className="bg-gray-800 rounded p-2">
                  <div className="text-xs text-gray-500">{label}</div>
                  <div className="text-sm text-white font-medium truncate">{value}</div>
                </div>
              ))}
            </div>

            {/* Coordinates */}
            <div className="bg-gray-800 rounded p-2">
              <div className="text-xs text-gray-500">Coordinates</div>
              <div className="text-sm text-white font-mono">
                {selectedFacility.lat.toFixed(4)}, {selectedFacility.lng.toFixed(4)}
              </div>
            </div>

            {/* Generation chart (simple bar display) */}
            {detailLoading && (
              <div className="flex items-center gap-2 text-gray-400 text-sm py-4">
                <RefreshCw size={14} className="animate-spin" />
                Loading generation data...
              </div>
            )}
            {detailData && detailData.generation.length > 0 && (
              <div>
                <div className="text-xs text-gray-500 mb-2">Recent Generation (last 24h)</div>
                <div className="h-24 flex items-end gap-px">
                  {detailData.generation.slice(0, 48).map((g, i) => {
                    const maxMw = Math.max(...detailData.generation.slice(0, 48).map(x => x.mw), 1)
                    const h = (g.mw / maxMw) * 100
                    return (
                      <div
                        key={i}
                        className="flex-1 rounded-t"
                        style={{
                          height: `${h}%`,
                          backgroundColor: FUEL_COLORS[selectedFacility.fuel_type] || '#3b82f6',
                          minWidth: 2,
                        }}
                        title={`${g.timestamp}: ${g.mw.toFixed(1)} MW`}
                      />
                    )
                  })}
                </div>
              </div>
            )}
            {detailData && detailData.generation.length === 0 && !detailLoading && (
              <div className="text-xs text-gray-500 py-2">No generation data available for this facility.</div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
