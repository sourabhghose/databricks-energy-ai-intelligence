import React, { useState, useCallback } from 'react'
import { TrendingUp, TrendingDown, Zap, Wind, Sun, Thermometer, RefreshCw } from 'lucide-react'
import { api, ScenarioComparison, ScenarioInput } from '../api/client'

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface Preset {
  id: string
  name: string
  description: string
  icon: string
  parameters: Partial<Omit<ScenarioInput, 'region' | 'base_temperature_c'>>
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const REGIONS = ['NSW1', 'QLD1', 'VIC1', 'SA1', 'TAS1']

const DEFAULT_INPUT: ScenarioInput = {
  region: 'NSW1',
  base_temperature_c: 25.0,
  temperature_delta_c: 0.0,
  gas_price_multiplier: 1.0,
  wind_output_multiplier: 1.0,
  solar_output_multiplier: 1.0,
  demand_multiplier: 1.0,
  coal_outage_mw: 0.0,
}

function fmt(value: number, decimals = 1): string {
  return value.toFixed(decimals)
}

function fmtPrice(value: number): string {
  return `$${value.toFixed(2)}`
}

function fmtChange(value: number, suffix = ''): string {
  const sign = value >= 0 ? '+' : ''
  return `${sign}${value.toFixed(1)}${suffix}`
}

function confidenceBadge(confidence: string): string {
  if (confidence === 'HIGH') return 'bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300'
  if (confidence === 'MEDIUM') return 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900 dark:text-yellow-300'
  return 'bg-red-100 text-red-700 dark:bg-red-900 dark:text-red-300'
}

// ---------------------------------------------------------------------------
// Preset icon component
// ---------------------------------------------------------------------------

function PresetIcon({ icon }: { icon: string }) {
  const cls = 'w-5 h-5'
  switch (icon) {
    case 'sun':         return <Sun className={cls} />
    case 'thermometer': return <Thermometer className={cls} />
    case 'wind':        return <Wind className={cls} />
    case 'zap':         return <Zap className={cls} />
    case 'leaf':        return <TrendingDown className={cls} />
    default:            return <TrendingUp className={cls} />
  }
}

// ---------------------------------------------------------------------------
// Slider row component
// ---------------------------------------------------------------------------

interface SliderProps {
  label: string
  value: number
  min: number
  max: number
  step: number
  unit: string
  onChange: (v: number) => void
  formatDisplay?: (v: number) => string
}

function SliderRow({ label, value, min, max, step, unit, onChange, formatDisplay }: SliderProps) {
  const display = formatDisplay ? formatDisplay(value) : `${value.toFixed(2)}${unit}`
  return (
    <div className="flex flex-col gap-1">
      <div className="flex justify-between items-center">
        <span className="text-sm font-medium text-gray-700 dark:text-gray-300">{label}</span>
        <span className="text-sm font-mono font-semibold text-blue-600 dark:text-blue-400 min-w-[80px] text-right">
          {display}
        </span>
      </div>
      <input
        type="range"
        min={min}
        max={max}
        step={step}
        value={value}
        onChange={e => onChange(parseFloat(e.target.value))}
        className="w-full h-2 bg-gray-200 dark:bg-gray-700 rounded-lg appearance-none cursor-pointer accent-blue-600"
      />
      <div className="flex justify-between text-xs text-gray-400 dark:text-gray-500">
        <span>{min}{unit}</span>
        <span>{max}{unit}</span>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Impact card component
// ---------------------------------------------------------------------------

interface ImpactCardProps {
  title: string
  baseValue: string
  scenarioValue: string
  change: number
  changeSuffix: string
  higherIsBad?: boolean
}

function ImpactCard({ title, baseValue, scenarioValue, change, changeSuffix, higherIsBad = true }: ImpactCardProps) {
  const isUp = change > 0
  const isNeutral = Math.abs(change) < 0.01
  const isBad = higherIsBad ? isUp : !isUp
  const colorCls = isNeutral
    ? 'text-gray-500 dark:text-gray-400'
    : isBad
      ? 'text-red-600 dark:text-red-400'
      : 'text-green-600 dark:text-green-400'
  const ArrowIcon = isUp ? TrendingUp : TrendingDown

  return (
    <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-4">
      <p className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-3">{title}</p>
      <div className="flex items-end justify-between">
        <div>
          <p className="text-xs text-gray-400 dark:text-gray-500 mb-0.5">Base</p>
          <p className="text-lg font-bold text-gray-800 dark:text-gray-100">{baseValue}</p>
        </div>
        <div className={`flex flex-col items-center ${colorCls}`}>
          {!isNeutral && <ArrowIcon size={28} />}
          <span className="text-sm font-semibold">{fmtChange(change, changeSuffix)}</span>
        </div>
        <div className="text-right">
          <p className="text-xs text-gray-400 dark:text-gray-500 mb-0.5">Scenario</p>
          <p className={`text-lg font-bold ${colorCls}`}>{scenarioValue}</p>
        </div>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

export default function ScenarioAnalysis() {
  const [inputs, setInputs] = useState<ScenarioInput>({ ...DEFAULT_INPUT })
  const [result, setResult] = useState<ScenarioComparison | null>(null)
  const [presets, setPresets] = useState<Preset[]>([])
  const [loading, setLoading] = useState(false)
  const [presetsLoading, setPresetsLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [activePreset, setActivePreset] = useState<string | null>(null)

  // Load presets on mount
  React.useEffect(() => {
    setPresetsLoading(true)
    api.getScenarioPresets()
      .then(data => setPresets(data as unknown as Preset[]))
      .catch(() => {/* silently ignore preset load failure */})
      .finally(() => setPresetsLoading(false))
  }, [])

  const handlePresetClick = useCallback((preset: Preset) => {
    setActivePreset(preset.id)
    setInputs(prev => ({
      ...prev,
      temperature_delta_c: preset.parameters.temperature_delta_c ?? 0,
      gas_price_multiplier: preset.parameters.gas_price_multiplier ?? 1.0,
      wind_output_multiplier: preset.parameters.wind_output_multiplier ?? 1.0,
      solar_output_multiplier: preset.parameters.solar_output_multiplier ?? 1.0,
      demand_multiplier: preset.parameters.demand_multiplier ?? 1.0,
      coal_outage_mw: preset.parameters.coal_outage_mw ?? 0,
    }))
    setResult(null)
  }, [])

  const handleReset = useCallback(() => {
    setInputs({ ...DEFAULT_INPUT })
    setResult(null)
    setActivePreset(null)
    setError(null)
  }, [])

  const handleRunScenario = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const data = await api.runScenario(inputs)
      setResult(data)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to run scenario')
    } finally {
      setLoading(false)
    }
  }, [inputs])

  const updateInput = useCallback(<K extends keyof ScenarioInput>(key: K, value: ScenarioInput[K]) => {
    setInputs(prev => ({ ...prev, [key]: value }))
    setActivePreset(null)
  }, [])

  return (
    <div className="p-6 space-y-6 max-w-5xl mx-auto">

      {/* Header */}
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div className="flex items-center gap-3">
          <h1 className="text-2xl font-bold text-gray-900 dark:text-white">Scenario Analysis</h1>
          <span className="px-2 py-0.5 text-xs font-semibold rounded-full bg-purple-100 text-purple-700 dark:bg-purple-900 dark:text-purple-300 border border-purple-200 dark:border-purple-700">
            What-If
          </span>
        </div>

        <div className="flex items-center gap-3">
          {/* Region selector */}
          <select
            value={inputs.region}
            onChange={e => updateInput('region', e.target.value)}
            className="px-3 py-1.5 text-sm border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-800 text-gray-800 dark:text-gray-200 focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            {REGIONS.map(r => (
              <option key={r} value={r}>{r}</option>
            ))}
          </select>

          {/* Reset */}
          <button
            onClick={handleReset}
            className="flex items-center gap-1.5 px-3 py-1.5 text-sm border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors"
          >
            <RefreshCw size={14} />
            Reset
          </button>
        </div>
      </div>

      {/* Preset Scenarios */}
      <section>
        <h2 className="text-sm font-semibold text-gray-600 dark:text-gray-400 uppercase tracking-wide mb-3">
          Preset Scenarios
        </h2>
        {presetsLoading ? (
          <div className="text-sm text-gray-400 dark:text-gray-500">Loading presets...</div>
        ) : (
          <div className="flex gap-3 overflow-x-auto pb-2">
            {presets.map(preset => (
              <button
                key={preset.id}
                onClick={() => handlePresetClick(preset)}
                className={[
                  'flex-shrink-0 flex flex-col items-start gap-1 px-4 py-3 rounded-xl border text-left transition-all w-44',
                  activePreset === preset.id
                    ? 'border-blue-500 bg-blue-50 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300'
                    : 'border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-300 hover:border-blue-300 dark:hover:border-blue-600 hover:bg-blue-50/50 dark:hover:bg-blue-900/10',
                ].join(' ')}
              >
                <div className="flex items-center gap-2 font-semibold text-sm">
                  <PresetIcon icon={preset.icon} />
                  {preset.name}
                </div>
                <p className="text-xs text-gray-500 dark:text-gray-400 leading-snug">{preset.description}</p>
              </button>
            ))}
          </div>
        )}
      </section>

      {/* Parameter Sliders + Run button */}
      <section className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-5">
        <h2 className="text-sm font-semibold text-gray-600 dark:text-gray-400 uppercase tracking-wide mb-4">
          Parameters
        </h2>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <SliderRow
            label="Temperature Delta"
            value={inputs.temperature_delta_c}
            min={-15}
            max={15}
            step={0.5}
            unit="°C"
            onChange={v => updateInput('temperature_delta_c', v)}
            formatDisplay={v => `${v >= 0 ? '+' : ''}${v.toFixed(1)}°C`}
          />

          <SliderRow
            label="Gas Price"
            value={inputs.gas_price_multiplier}
            min={0.5}
            max={2.5}
            step={0.05}
            unit="×"
            onChange={v => updateInput('gas_price_multiplier', v)}
            formatDisplay={v => `${v.toFixed(2)}×`}
          />

          <SliderRow
            label="Wind Output"
            value={inputs.wind_output_multiplier}
            min={0}
            max={1.5}
            step={0.05}
            unit="%"
            onChange={v => updateInput('wind_output_multiplier', v)}
            formatDisplay={v => `${(v * 100).toFixed(0)}%`}
          />

          <SliderRow
            label="Solar Output"
            value={inputs.solar_output_multiplier}
            min={0}
            max={1.5}
            step={0.05}
            unit="%"
            onChange={v => updateInput('solar_output_multiplier', v)}
            formatDisplay={v => `${(v * 100).toFixed(0)}%`}
          />

          <SliderRow
            label="Demand"
            value={inputs.demand_multiplier}
            min={0.7}
            max={1.3}
            step={0.01}
            unit="×"
            onChange={v => updateInput('demand_multiplier', v)}
            formatDisplay={v => `${v.toFixed(2)}×`}
          />

          <SliderRow
            label="Coal Outage"
            value={inputs.coal_outage_mw}
            min={0}
            max={3000}
            step={50}
            unit=" MW"
            onChange={v => updateInput('coal_outage_mw', v)}
            formatDisplay={v => `${v.toFixed(0)} MW`}
          />
        </div>

        <div className="mt-6 flex items-center gap-4">
          <button
            onClick={handleRunScenario}
            disabled={loading}
            className="flex items-center gap-2 px-6 py-2.5 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white text-sm font-semibold rounded-lg transition-colors"
          >
            {loading ? (
              <>
                <RefreshCw size={16} className="animate-spin" />
                Running...
              </>
            ) : (
              <>
                <Zap size={16} />
                Run Scenario
              </>
            )}
          </button>

          {result && (
            <span className="text-xs text-gray-400 dark:text-gray-500">
              Last run: {new Date(result.timestamp).toLocaleTimeString()}
            </span>
          )}
        </div>

        {error && (
          <div className="mt-3 px-3 py-2 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg text-sm text-red-700 dark:text-red-400">
            {error}
          </div>
        )}
      </section>

      {/* Results Panel */}
      {result && (
        <>
          <section>
            <div className="flex items-center justify-between mb-3">
              <h2 className="text-sm font-semibold text-gray-600 dark:text-gray-400 uppercase tracking-wide">
                Scenario Results — {result.result.region}
              </h2>
              <span className={`px-2 py-0.5 text-xs font-semibold rounded-full ${confidenceBadge(result.result.confidence)}`}>
                {result.result.confidence} Confidence
              </span>
            </div>

            {/* Impact cards grid */}
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
              <ImpactCard
                title="Wholesale Price (AUD/MWh)"
                baseValue={fmtPrice(result.result.base_price_aud_mwh)}
                scenarioValue={fmtPrice(result.result.scenario_price_aud_mwh)}
                change={result.result.price_change_aud_mwh}
                changeSuffix=" $/MWh"
                higherIsBad={true}
              />

              <ImpactCard
                title="Demand (MW)"
                baseValue={`${fmt(result.result.base_demand_mw, 0)} MW`}
                scenarioValue={`${fmt(result.result.scenario_demand_mw, 0)} MW`}
                change={result.result.demand_change_mw}
                changeSuffix=" MW"
                higherIsBad={false}
              />

              <ImpactCard
                title="Renewable %"
                baseValue={`${fmt(result.result.base_renewable_pct)}%`}
                scenarioValue={`${fmt(result.result.scenario_renewable_pct)}%`}
                change={result.result.scenario_renewable_pct - result.result.base_renewable_pct}
                changeSuffix="%"
                higherIsBad={false}
              />
            </div>

            {/* Price change pct banner */}
            <div className={`mt-4 rounded-xl p-4 flex items-center justify-between ${
              result.result.price_change_pct > 0
                ? 'bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800'
                : result.result.price_change_pct < 0
                  ? 'bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800'
                  : 'bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700'
            }`}>
              <div>
                <p className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-0.5">
                  Price Impact
                </p>
                <p className={`text-3xl font-bold ${
                  result.result.price_change_pct > 0
                    ? 'text-red-600 dark:text-red-400'
                    : result.result.price_change_pct < 0
                      ? 'text-green-600 dark:text-green-400'
                      : 'text-gray-600 dark:text-gray-300'
                }`}>
                  {fmtChange(result.result.price_change_pct, '%')}
                </p>
              </div>
              <div className="text-right">
                <p className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-0.5">
                  Marginal Generator
                </p>
                <p className="text-sm font-semibold text-gray-700 dark:text-gray-200">
                  {result.result.marginal_generator_base}
                </p>
                {result.result.marginal_generator_base !== result.result.marginal_generator_scenario && (
                  <p className="text-xs text-gray-500 dark:text-gray-400 flex items-center justify-end gap-1 mt-0.5">
                    <span>→</span>
                    <span className="font-medium text-orange-600 dark:text-orange-400">
                      {result.result.marginal_generator_scenario}
                    </span>
                  </p>
                )}
              </div>
            </div>

            {/* Key drivers */}
            <div className="mt-4 bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-4">
              <p className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-3">
                Key Drivers
              </p>
              <div className="flex flex-col gap-2">
                {result.result.key_drivers.map((driver, idx) => {
                  const isPositive = driver.toLowerCase().includes('surge') ||
                    driver.toLowerCase().includes('shortfall') ||
                    driver.toLowerCase().includes('outage') ||
                    driver.toLowerCase().includes('elevated') ||
                    driver.toLowerCase().includes('+')
                  const isNegative = driver.toLowerCase().includes('boost') ||
                    driver.toLowerCase().includes('reduced') ||
                    driver.toLowerCase().includes('-')
                  const badgeCls = isPositive
                    ? 'bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-300'
                    : isNegative
                      ? 'bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-300'
                      : 'bg-gray-100 text-gray-600 dark:bg-gray-700 dark:text-gray-300'
                  return (
                    <div key={idx} className={`inline-flex items-start gap-2 px-3 py-1.5 rounded-lg text-xs font-medium ${badgeCls}`}>
                      <span>{driver}</span>
                    </div>
                  )
                })}
              </div>
            </div>
          </section>

          {/* Sensitivity Table */}
          <section>
            <h2 className="text-sm font-semibold text-gray-600 dark:text-gray-400 uppercase tracking-wide mb-3">
              Sensitivity Table
            </h2>
            <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
              <table className="w-full text-sm">
                <thead>
                  <tr className="bg-gray-50 dark:bg-gray-900 border-b border-gray-200 dark:border-gray-700">
                    <th className="px-4 py-3 text-left font-semibold text-gray-600 dark:text-gray-400 text-xs uppercase tracking-wide">
                      Parameter
                    </th>
                    <th className="px-4 py-3 text-center font-semibold text-blue-600 dark:text-blue-400 text-xs uppercase tracking-wide">
                      Low
                    </th>
                    <th className="px-4 py-3 text-center font-semibold text-gray-600 dark:text-gray-400 text-xs uppercase tracking-wide">
                      Base
                    </th>
                    <th className="px-4 py-3 text-center font-semibold text-orange-600 dark:text-orange-400 text-xs uppercase tracking-wide">
                      High
                    </th>
                    <th className="px-4 py-3 text-center font-semibold text-gray-600 dark:text-gray-400 text-xs uppercase tracking-wide">
                      Range
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {result.sensitivity_table.map((row, idx) => {
                    const low = row['low_price'] as number
                    const base = row['base_price'] as number
                    const high = row['high_price'] as number
                    const range = Math.abs(high - low)
                    const maxRange = Math.max(
                      ...result.sensitivity_table.map(r => Math.abs((r['high_price'] as number) - (r['low_price'] as number)))
                    )
                    const barWidth = maxRange > 0 ? (range / maxRange) * 100 : 0
                    return (
                      <tr
                        key={idx}
                        className="border-b border-gray-100 dark:border-gray-700 last:border-0 hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors"
                      >
                        <td className="px-4 py-3 font-medium text-gray-700 dark:text-gray-300">
                          {String(row['parameter'])}
                        </td>
                        <td className="px-4 py-3 text-center font-mono text-blue-700 dark:text-blue-300">
                          {fmtPrice(low)}
                        </td>
                        <td className="px-4 py-3 text-center font-mono text-gray-600 dark:text-gray-400">
                          {fmtPrice(base)}
                        </td>
                        <td className="px-4 py-3 text-center font-mono text-orange-700 dark:text-orange-300">
                          {fmtPrice(high)}
                        </td>
                        <td className="px-4 py-3">
                          <div className="flex items-center gap-2">
                            <div className="flex-1 bg-gray-100 dark:bg-gray-700 rounded-full h-2">
                              <div
                                className="h-2 rounded-full bg-gradient-to-r from-blue-500 to-orange-500"
                                style={{ width: `${barWidth}%` }}
                              />
                            </div>
                            <span className="text-xs font-mono text-gray-500 dark:text-gray-400 w-16 text-right">
                              {fmtPrice(range)}
                            </span>
                          </div>
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
            <p className="text-xs text-gray-400 dark:text-gray-500 mt-2">
              Range bar shows relative sensitivity. Longer bar = higher price impact for that parameter.
            </p>
          </section>
        </>
      )}

      {/* Empty state */}
      {!result && !loading && (
        <div className="flex flex-col items-center justify-center py-16 text-center">
          <Zap size={40} className="text-gray-300 dark:text-gray-600 mb-3" />
          <p className="text-gray-500 dark:text-gray-400 text-sm font-medium">
            Adjust the parameters above and click <strong>Run Scenario</strong> to see the estimated price impact.
          </p>
          <p className="text-gray-400 dark:text-gray-500 text-xs mt-1">
            Or select a preset scenario to pre-populate the sliders.
          </p>
        </div>
      )}
    </div>
  )
}
