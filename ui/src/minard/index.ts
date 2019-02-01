import {PlotAction} from 'src/minard/utils/plotEnvActions'

export const PLOT_PADDING = 20

export const TICK_PADDING_RIGHT = 8
export const TICK_PADDING_TOP = 5

// TODO: Measure text metrics instead
export const TICK_CHAR_WIDTH = 7
export const TICK_CHAR_HEIGHT = 10

export const CATEGORY_10 = [
  '#1f77b4',
  '#ff7f0e',
  '#2ca02c',
  '#d62728',
  '#9467bd',
  '#8c564b',
  '#e377c2',
  '#7f7f7f',
  '#bcbd22',
  '#17becf',
]

export {Plot} from 'src/minard/components/Plot'

export {
  Histogram,
  Position as HistogramPosition,
  TooltipProps as HistogramTooltipProps,
} from 'src/minard/components/Histogram'

export interface Scale<D = number, R = number> {
  (x: D): R
  invert?: (y: R) => D
}

export interface AestheticDataMappings {
  [aestheticName: string]: string
}

export interface AestheticScaleMappings {
  [aestheticName: string]: Scale<any, any>
}

export interface Layer {
  table?: Table
  aesthetics: AestheticDataMappings
  scales: AestheticScaleMappings
  colors?: string[]
}

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
  defaults: Layer
  layers: {[layerKey: string]: Layer}
  xDomain: number[]
  yDomain: number[]
  xTicks: string[]
  yTicks: string[]
  margins: Margins
  hoverX: number
  hoverY: number
  dispatch: (action: PlotAction) => void
}

export enum ColumnType {
  Numeric = 'numeric',
  Categorical = 'categorical',
  Temporal = 'temporal',
  Boolean = 'bool',
}

export interface Table {
  columns: {[columnName: string]: any[]}
  columnTypes: {[columnName: string]: ColumnType}
}

// export enum InterpolationKind {
//   Linear = 'linear',
//   MonotoneX = 'monotoneX',
//   MonotoneY = 'monotoneY',
//   Cubic = 'cubic',
//   Step = 'step',
//   StepBefore = 'stepBefore',
//   StepAfter = 'stepAfter',
// }

// export interface LineProps {
//   x?: string
//   y?: string
//   stroke?: string
//   strokeWidth?: string
//   interpolate?: InterpolationKind
// }

// export enum AreaPositionKind {
//   Stack = 'stack',
//   Overlay = 'overlay',
// }

// export interface AreaProps {
//   x?: string
//   y?: string
//   position?: AreaPositionKind
// }

// export enum ShapeKind {
//   Point = 'point',
//   // Spade, Heart, Club, Triangle, Hexagon, etc.
// }

// export interface PointProps {
//   x?: string
//   y?: string
//   fill?: string
//   shape?: ShapeKind
//   radius?: number
//   alpha?: number
// }

// export interface ContinuousBarProps {
//   x0?: string
//   x1?: string
//   y?: string
//   fill?: string
// }

// export enum DiscreteBarPositionKind {
//   Stack = 'stack',
//   Dodge = 'dodge',
// }

// export interface DiscreteBarProps {
//   x?: string
//   y?: string
//   fill?: string
//   position?: DiscreteBarPositionKind
// }

// export interface StepLineProps {
//   x0?: string
//   x1?: string
//   y?: string
// }

// export interface StepAreaProps {
//   x0?: string
//   x1?: string
//   y?: string
//   position?: AreaPositionKind
// }

// export interface Bin2DProps {
//   x?: string
//   y?: string
//   binWidth?: number
//   binHeight?: number
// }
