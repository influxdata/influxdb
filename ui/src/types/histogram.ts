type UnixTime = number

export interface HistogramDatum {
  key: string
  time: UnixTime
  value: number
  group: string
}

export interface TimePeriod {
  start: UnixTime
  end: UnixTime
}

export type HistogramData = HistogramDatum[]

export type TooltipAnchor = 'left' | 'right'

export interface Margins {
  top: number
  right: number
  bottom: number
  left: number
}

export interface HoverData {
  data: HistogramData
  x: number
  y: number
  anchor: TooltipAnchor
}

export type ColorScale = (color: string, group: string) => string

export interface HistogramColor {
  group: string
  color: string
}

export interface BarGroup {
  key: string
  clip: {
    x: number
    y: number
    width: number
    height: number
  }
  bars: Array<{
    key: string
    group: string
    x: number
    y: number
    width: number
    height: number
    fill: string
  }>
  data: HistogramData
}
