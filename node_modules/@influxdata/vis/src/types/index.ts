import {SymbolType} from '../utils/getSymbolScale'

export type NumericColumnData =
  | number[]
  | Int8Array
  | Int16Array
  | Int32Array
  | Uint8Array
  | Uint16Array
  | Uint32Array
  | Float32Array
  | Float64Array

export type ColumnData = number[] | string[] | boolean[]

export type ColumnType = 'number' | 'string' | 'time' | 'boolean'

export interface GetColumn {
  (columnKey: string): ColumnData
  (columnKey: string, type: 'number'): number[]
  (columnKey: string, type: 'time'): number[]
  (columnKey: string, type: 'string'): string[]
  (columnKey: string, type: 'boolean'): boolean[]
}

export interface Table {
  getColumn: GetColumn
  getColumnName: (columnKey: string) => string
  getColumnType: (columnKey: string) => ColumnType
  columnKeys: string[]
  length: number
  addColumn: (
    columnKey: string,
    type: ColumnType,
    data: ColumnData,
    name?: string
  ) => Table
}

export interface Scale<D = any, R = any> {
  (x: D): R
  invert?: (y: R) => D
}

export interface Margins {
  top: number
  right: number
  bottom: number
  left: number
}

export interface TooltipColumn {
  key: string
  name: string
  type: ColumnType
  values: string[]
  colors: string[] | null
}

export type TooltipData = TooltipColumn[]

export type LineInterpolation =
  | 'linear'
  | 'monotoneX'
  | 'monotoneY'
  | 'cubic'
  | 'step'
  | 'stepBefore'
  | 'stepAfter'
  | 'natural'

/*
  The tooltip for a line layer can operate in one of three modes:

  In the `x` mode, every y-value for the currently hovered x-value is displayed
  in the tooltip. The crosshair is a vertical line.

  In the `y` mode, every x-value for the currently hovered y-value is displayed
  in the tooltip. The crosshair is a horizontal line.

  In the `xy` mode, the single xy-point closest to the hovered mouse position
  is displayed in the tooltip. The series that it belongs to is highlighted.
  The crosshair is an intersecting pair of horizontal and vertical lines.
*/
export type LineHoverDimension = 'x' | 'y' | 'xy'

export interface LineLayerConfig {
  type: 'line'
  x: string
  y: string
  fill?: string[]
  colors?: string[]
  interpolation?: LineInterpolation
  hoverDimension?: LineHoverDimension | 'auto'
  lineWidth?: number
  maxTooltipRows?: number
}

export interface LineMappings {
  x: string
  y: string
  fill: string[]
}

export interface LineScales {
  fill: Scale<string, string>
}

export interface HeatmapLayerConfig {
  type: 'heatmap'
  x: string
  y: string
  colors?: string[]
  binSize?: number
}

export interface HeatmapScales {
  fill: Scale<number, string>
}

export interface HeatmapMappings {
  xMin: 'xMin'
  xMax: 'xMax'
  yMin: 'yMin'
  yMax: 'yMax'
  fill: 'count'
}

export interface HistogramMappings {
  xMin: 'xMin'
  xMax: 'xMax'
  yMin: 'yMin'
  yMax: 'yMax'
  fill: string[]
}

export interface HistogramScales {
  fill: Scale<string, string>
}

export interface ScatterLayerConfig {
  type: 'scatter'
  x: string
  y: string
  colors?: string[]
  fill?: string[]
  symbol?: string[]
}

export interface ScatterMappings {
  x: string
  y: string
  fill: string[]
  symbol: string[]
}

export interface ScatterScales {
  fill: Scale<string, string>
  symbol: Scale<string, SymbolType>
}

export type Mappings =
  | LineMappings
  | HistogramMappings
  | HeatmapMappings
  | ScatterMappings

export type Scales =
  | LineScales
  | HistogramScales
  | HeatmapScales
  | ScatterScales

export type LayerConfig =
  | LineLayerConfig
  | HistogramLayerConfig
  | HeatmapLayerConfig
  | ScatterLayerConfig

export type HistogramPosition = 'overlaid' | 'stacked'

export interface HistogramLayerConfig {
  type: 'histogram'
  x: string
  fill?: string[]
  colors?: string[]
  position?: HistogramPosition
  binCount?: number
}

export interface Config {
  table: Table
  layers: LayerConfig[]

  width?: number
  height?: number

  xAxisLabel?: string
  yAxisLabel?: string

  // The x domain of the plot can be explicitly set. If this option is passed,
  // then the component is operating in a "controlled" mode, where it always
  // uses the passed x domain. Any brush interaction with the plot that should
  // change the x domain will call the `onSetXDomain` option when the component
  // is in controlled mode. Double clicking the plot will call
  // `onResetXDomain`. If the `xDomain` option is not passed, then the
  // component is "uncontrolled". It will compute, set, and reset the `xDomain`
  // automatically.
  xDomain?: number[]
  onSetXDomain?: (xDomain: number[]) => void
  onResetXDomain?: () => void

  // See the `xDomain`, `onSetXDomain`, and `onResetXDomain` options
  yDomain?: number[]
  onSetYDomain?: (yDomain: number[]) => void
  onResetYDomain?: () => void

  valueFormatters?: {
    [colKey: string]: (value: any) => string
  }

  showAxes?: boolean

  axisColor?: string
  axisOpacity?: number
  gridColor?: string
  gridOpacity?: number

  tickFont?: string
  tickFontColor?: string

  legendFont?: string
  legendFontColor?: string
  legendFontBrightColor?: string
  legendBackgroundColor?: string
  legendBorder?: string
  legendCrosshairColor?: string
  legendColumns?: string[]
}

export type SizedConfig = Config & {width: number; height: number}
