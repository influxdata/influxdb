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
