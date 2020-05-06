import {
  SizedConfig,
  Margins,
  Scale,
  Table,
  ColumnType,
  LineLayerConfig,
} from '../types'

import {
  DEFAULT_RANGE_PADDING,
  LAYER_DEFAULTS,
  CONFIG_DEFAULTS,
} from '../constants'

import * as transforms from '../layerTransforms'
import {getTicks} from './getTicks'
import {getTimeFormatter} from '../utils/getTimeFormatter'
import {assert} from './assert'
import {getTextMetrics} from './getTextMetrics'
import {getMargins} from './getMargins'
import {extentOfExtents} from './extrema'
import {flatMap} from './flatMap'
import {identityMerge} from './identityMerge'
import {MemoizedFunctionCache} from './MemoizedFunctionCache'
import {defaultNumberFormatter} from './defaultNumberFormatter'
import {getLinearScale} from './getLinearScale'

const X_DOMAIN_AESTHETICS = ['x', 'xMin', 'xMax']
const Y_DOMAIN_AESTHETICS = ['y', 'yMin', 'yMax']
const DEFAULT_X_DOMAIN: [number, number] = [0, 1]
const DEFAULT_Y_DOMAIN: [number, number] = [0, 1]
const DEFAULT_FORMATTER = x => String(x)

export class PlotEnv {
  private _config: SizedConfig | null = null
  private _xDomain: number[] | null = null
  private _yDomain: number[] | null = null
  private fns = new MemoizedFunctionCache()

  public get config(): SizedConfig | null {
    return this._config
  }

  public set config(config: SizedConfig) {
    const prevConfig = this._config

    this._config = mergeConfigs(config, prevConfig)

    if (areUncontrolledDomainsStale(this._config, prevConfig)) {
      this._xDomain = null
      this._yDomain = null
    }
  }

  public get margins(): Margins {
    const {
      yTicks,
      config: {xAxisLabel, yAxisLabel, tickFont},
    } = this

    const getMarginsMemoized = this.fns.get('margins', getMargins)

    return getMarginsMemoized(
      this.config.showAxes,
      xAxisLabel,
      yAxisLabel,
      yTicks,
      this.yTickFormatter,
      tickFont
    )
  }

  public get innerWidth(): number {
    const {width} = this.config
    const {margins} = this

    return width - margins.left - margins.right
  }

  public get innerHeight(): number {
    const {height} = this.config
    const {margins} = this

    return height - margins.top - margins.bottom
  }

  public get xTicks(): number[] {
    const getTicksMemoized = this.fns.get('xTicks', getTicks)

    return getTicksMemoized(
      this.xDomain,
      this.config.width,
      'horizontal',
      this.getColumnTypeForAesthetics(['x', 'xMin', 'xMax']),
      this.charMetrics
    )
  }

  public get yTicks(): number[] {
    const getTicksMemoized = this.fns.get('yTicks', getTicks)

    return getTicksMemoized(
      this.yDomain,
      this.config.height,
      'vertical',
      this.getColumnTypeForAesthetics(['y', 'yMin', 'yMax']),
      this.charMetrics
    )
  }

  public get xScale(): Scale<number, number> {
    const getXScaleMemoized = this.fns.get('xScale', getLinearScale)
    const {xDomain, rangePadding, innerWidth} = this

    return getXScaleMemoized(xDomain, [
      rangePadding,
      innerWidth - rangePadding * 2,
    ])
  }

  public get yScale(): Scale<number, number> {
    const getYScaleMemoized = this.fns.get('yScale', getLinearScale)
    const {yDomain, rangePadding, innerHeight} = this

    return getYScaleMemoized(yDomain, [
      innerHeight - rangePadding * 2,
      rangePadding,
    ])
  }

  public get xDomain(): number[] {
    if (this.isXControlled) {
      return this.config.xDomain
    }

    if (!this._xDomain) {
      this._xDomain = this.getXDomain()
    }

    return this._xDomain
  }

  public set xDomain(newXDomain: number[]) {
    if (this.isXControlled) {
      this.config.onSetXDomain(newXDomain)
    } else {
      this._xDomain = newXDomain
    }
  }

  public get yDomain(): number[] {
    if (this.isYControlled) {
      return this.config.yDomain
    }

    if (!this._yDomain) {
      this._yDomain = this.getYDomain()
    }

    return this._yDomain
  }

  public set yDomain(newYDomain: number[]) {
    if (this.isYControlled) {
      this.config.onSetYDomain(newYDomain)
    } else {
      this._yDomain = newYDomain
    }
  }

  public get xTickFormatter(): (tick: number) => string {
    const colKeys = this.getColumnKeysForAesthetics(X_DOMAIN_AESTHETICS)

    return this.getFormatterForColumn(colKeys[0])
  }

  public get yTickFormatter(): (tick: number) => string {
    const colKeys = this.getColumnKeysForAesthetics(Y_DOMAIN_AESTHETICS)

    return this.getFormatterForColumn(colKeys[0])
  }

  public getFormatterForColumn = (colKey: string): ((x: any) => string) => {
    const preferredFormatter = this.config.valueFormatters[colKey]

    if (preferredFormatter) {
      return preferredFormatter
    }

    const colType = this.getColumnTypeByKey(colKey)

    switch (colType) {
      case 'number':
        return defaultNumberFormatter
      case 'time':
        return this.timeFormatter
      default:
        return DEFAULT_FORMATTER
    }
  }

  public getTable = (layerIndex: number): Table => {
    const table = this.config.table
    const layerConfig = this.config.layers[layerIndex]
    const transformKey = `${layerIndex} ${layerConfig.type} table`

    switch (layerConfig.type) {
      case 'line': {
        const {fill} = layerConfig
        const transform = this.fns.get(transformKey, transforms.getLineTable)

        return transform(table, fill)
      }
      case 'scatter': {
        const {fill, symbol} = layerConfig
        const transform = this.fns.get(transformKey, transforms.getScatterTable)

        return transform(table, fill, symbol)
      }
      case 'histogram': {
        const {x, fill, binCount, position} = layerConfig

        const transform = this.fns.get(
          transformKey,
          transforms.getHistogramTable
        )

        return transform(
          table,
          x,
          this.config.xDomain,
          fill,
          binCount,
          position
        )
      }
      case 'heatmap': {
        const {x, y, binSize} = layerConfig
        const {width, height, xDomain, yDomain} = this.config
        const transform = this.fns.get(transformKey, transforms.getHeatmapTable)

        return transform(table, x, y, xDomain, yDomain, width, height, binSize)
      }
      default: {
        return this.config.table
      }
    }
  }

  public getScale = (layerIndex: number, aesthetic: string): Scale => {
    const {type: layerType, colors} = this.config.layers[layerIndex]
    const transformKey = `${layerIndex} ${layerType} scales`
    const table = this.getTable(layerIndex)

    switch (layerType) {
      case 'line': {
        const getter = this.fns.get(transformKey, transforms.getLineScales)

        return getter(table, colors)[aesthetic]
      }
      case 'scatter': {
        const getter = this.fns.get(transformKey, transforms.getScatterScales)

        return getter(table, colors)[aesthetic]
      }
      case 'histogram': {
        const getter = this.fns.get(transformKey, transforms.getHistogramScales)

        return getter(table, colors)[aesthetic]
      }
      case 'heatmap': {
        const getter = this.fns.get(transformKey, transforms.getHeatmapScales)

        return getter(table, colors)[aesthetic]
      }
      default: {
        throw new Error(`${aesthetic} scale for layer ${layerIndex} not found`)
      }
    }
  }

  public getMapping = (
    layerIndex: number,
    aesthetic: string
  ): string | null => {
    const layerConfig = this.config.layers[layerIndex]

    switch (layerConfig.type) {
      case 'line':
        return transforms.getLineMappings(layerConfig)[aesthetic]
      case 'scatter':
        return transforms.getScatterMappings(layerConfig)[aesthetic]
      case 'heatmap':
        return transforms.getHeatmapMappings()[aesthetic]
      case 'histogram':
        return transforms.getHistogramMappings(layerConfig.fill)[aesthetic]
      default:
        return null
    }
  }

  public resetDomains = (): void => {
    if (this.isXControlled) {
      this.config.onResetXDomain()
    } else {
      this._xDomain = this.getXDomain()
    }

    if (this.isYControlled) {
      this.config.onResetYDomain()
    } else {
      this._yDomain = this.getYDomain()
    }
  }

  private get charMetrics() {
    const getTextMetricsMemoized = this.fns.get('charMetrics', getTextMetrics)

    // https://stackoverflow.com/questions/3949422/which-letter-of-the-english-alphabet-takes-up-most-pixels
    return getTextMetricsMemoized(this.config.tickFont, 'W')
  }

  private get isXControlled() {
    return (
      this.config.xDomain &&
      this.config.onSetXDomain &&
      this.config.onResetXDomain
    )
  }

  private get isYControlled() {
    return (
      this.config.yDomain &&
      this.config.onSetYDomain &&
      this.config.onResetYDomain
    )
  }

  private get timeFormatter() {
    const getTimeFormatterMemoized = this.fns.get(
      'timeFormatter',
      getTimeFormatter
    )

    return getTimeFormatterMemoized(this.xDomain)
  }

  private getXDomain() {
    return this.getDomainForAesthetics(X_DOMAIN_AESTHETICS) || DEFAULT_X_DOMAIN
  }

  private getYDomain() {
    return this.getDomainForAesthetics(Y_DOMAIN_AESTHETICS) || DEFAULT_Y_DOMAIN
  }

  private getColumnTypeForAesthetics(aesthetics: string[]): ColumnType | null {
    const columnTypes: ColumnType[] = this.config.layers.reduce(
      (acc, _, layerIndex) => {
        const table = this.getTable(layerIndex)

        const colKeys = aesthetics
          .map(aes => this.getMapping(layerIndex, aes))
          .filter(d => !!d)

        return [...acc, ...colKeys.map(k => table.getColumnType(k))]
      },
      []
    )

    if (!columnTypes.length) {
      return null
    }

    assert(
      columnTypes.every(t => t === columnTypes[0]),
      `found multiple column types for aesthetics "${aesthetics}"`
    )

    return columnTypes[0]
  }

  private getColumnKeysForAesthetics(aesthetics: string[]): string[] {
    return flatMap((layer, layerIndex) => {
      const columnKeysForLayer = aesthetics.reduce((acc, aes) => {
        if (layer[aes]) {
          return [...acc, layer[aes]]
        }

        const derivedMapping = this.getMapping(layerIndex, aes)

        if (derivedMapping) {
          return [...acc, derivedMapping]
        }

        return acc
      }, [])

      return columnKeysForLayer
    }, this.config.layers)
  }

  private getDomainForAesthetics(aesthetics: string[]): number[] {
    // Collect column data arrays for all columns in the plot currently mapped
    // to any of the passed `aesthetics`
    const colData: number[][] = this.config.layers.reduce(
      (acc, _, layerIndex) => {
        const table = this.getTable(layerIndex)

        const colKeys = aesthetics
          .map(aes => this.getMapping(layerIndex, aes))
          .filter(d => !!d)

        return [...acc, ...colKeys.map(k => table.getColumn(k))]
      },
      []
    )

    const fnKey = `extentOfExtents ${aesthetics.join(', ')}`
    const fn = this.fns.get(fnKey, extentOfExtents)

    // Compute the domain of all of those columns (memoized, since doing so is
    // an expensive operation)
    const domain = fn(...colData)

    return domain
  }

  private getColumnTypeByKey(key: string): ColumnType {
    if (this.config.table.columnKeys.includes(key)) {
      return this.config.table.getColumnType(key)
    }

    for (let i = 0; i < this.config.layers.length; i++) {
      const table = this.getTable(i)

      if (table.columnKeys.includes(key)) {
        return table.getColumnType(key)
      }
    }

    return null
  }

  private get rangePadding(): number {
    const specifiedLineWidths = this.config.layers
      .filter(l => l.type === 'line')
      .map(l => (l as LineLayerConfig).lineWidth)

    return Math.max(DEFAULT_RANGE_PADDING, ...specifiedLineWidths)
  }
}

const applyLayerDefaults = (
  layers: SizedConfig['layers']
): SizedConfig['layers'] =>
  layers.map(layer =>
    LAYER_DEFAULTS[layer.type]
      ? {...LAYER_DEFAULTS[layer.type], ...layer}
      : layer
  )

const mergeConfigs = (
  config: SizedConfig,
  prevConfig: SizedConfig | null
): SizedConfig => ({
  ...identityMerge(prevConfig, {
    ...CONFIG_DEFAULTS,
    ...config,
    layers: applyLayerDefaults(config.layers),
    // Avoid passing the `table` to `identityMerge` since checking its identity
    // can be quite expensive if it is a large object
    table: null,
  }),
  table: config.table,
})

const areUncontrolledDomainsStale = (
  config: SizedConfig,
  prevConfig: SizedConfig
): boolean => {
  if (!prevConfig) {
    return true
  }

  if (config.table !== prevConfig.table) {
    return true
  }

  // TODO: can be stale via binCount

  const xyMappingsChanged = [
    ...X_DOMAIN_AESTHETICS,
    ...Y_DOMAIN_AESTHETICS,
  ].some(aes =>
    config.layers.some(
      (layer, layerIndex) => layer[aes] !== prevConfig.layers[layerIndex][aes]
    )
  )

  if (xyMappingsChanged) {
    return true
  }

  return false
}
