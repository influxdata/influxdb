import {PlotAction} from 'src/minard/utils/plotEnvActions'

export const PLOT_PADDING = 20

export const TICK_PADDING_RIGHT = 8
export const TICK_PADDING_TOP = 5

// TODO: Measure text metrics instead
export const TICK_CHAR_WIDTH = 7
export const TICK_CHAR_HEIGHT = 10

export {Plot} from 'src/minard/components/Plot'

export {
  Histogram,
  Position as HistogramPosition,
  TooltipProps as HistogramTooltipProps,
} from 'src/minard/components/Histogram'

export {isNumeric} from 'src/minard/utils/isNumeric'

export type ColumnType = 'int' | 'uint' | 'float' | 'string' | 'time' | 'bool'

export type NumericColumnType = 'int' | 'uint' | 'float' | 'time'

export interface FloatColumn {
  data: number[]
  type: 'float'
}

export interface IntColumn {
  data: number[]
  type: 'int'
}

export interface UIntColumn {
  data: number[]
  type: 'uint'
}

export interface TimeColumn {
  data: number[]
  type: 'time'
}

export interface StringColumn {
  data: string[]
  type: 'string'
}

export interface BoolColumn {
  data: boolean[]
  type: 'bool'
}

export type NumericTableColumn =
  | FloatColumn
  | IntColumn
  | UIntColumn
  | TimeColumn

export type TableColumn =
  | FloatColumn
  | IntColumn
  | UIntColumn
  | TimeColumn
  | StringColumn
  | BoolColumn

export interface Table {
  length: number
  columns: {
    [columnName: string]: TableColumn
  }
}

export type LayerType = 'base' | 'histogram'

export interface Scale<D = number, R = number> {
  (x: D): R
  invert?: (y: R) => D
}

export interface BaseLayerMappings {}

export interface BaseLayerScales {
  x: Scale<number, number>
  y: Scale<number, number>
}

export interface BaseLayer {
  type: 'base'
  table: Table
  scales: BaseLayerScales
  mappings: {}
  xDomain: [number, number]
  yDomain: [number, number]
}

export interface HistogramTable extends Table {
  columns: {
    xMin: NumericTableColumn
    xMax: NumericTableColumn
    yMin: IntColumn
    yMax: IntColumn
    [fillColumn: string]: TableColumn
  }
  length: number
}

export interface HistogramMappings {
  xMin: 'xMin'
  xMax: 'xMax'
  yMin: 'yMin'
  yMax: 'yMax'
  fill: string[]
}

export interface HistogramScales {
  // x and y scale are from the `BaseLayer`
  fill: Scale<string, string>
}

export interface HistogramLayer {
  type: 'histogram'
  table: HistogramTable
  mappings: HistogramMappings
  scales: HistogramScales
  colors: string[]
}

export type Layer = BaseLayer | HistogramLayer

export interface Margins {
  top: number
  right: number
  bottom: number
  left: number
}

export interface PlotEnv {
  width: number
  height: number
  innerWidth: number
  innerHeight: number
  margins: Margins
  xTicks: number[]
  yTicks: number[]

  // If the domains have been explicitly passed in to the `Plot` component,
  // they will be stored here. Scales and child layers use the `xDomain` and
  // `yDomain` in the `baseLayer`, which are set from these domains if they
  // exist or computed from the extent of data otherwise
  xDomain: [number, number]
  yDomain: [number, number]

  baseLayer: BaseLayer
  layers: {[layerKey: string]: Layer}
  hoverX: number
  hoverY: number
  dispatch: (action: PlotAction) => void
}
