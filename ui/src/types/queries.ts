export {Query, Dialect} from 'src/client'

export type CUSTOM_TIME_RANGE_LABEL = 'Custom Time Range'

export type TimeRangeLower =
  | 'now() - 5m'
  | 'now() - 15m'
  | 'now() - 1h'
  | 'now() - 6h'
  | 'now() - 12h'
  | 'now() - 24h'
  | 'now() - 2d'
  | 'now() - 7d'
  | 'now() - 30d'

export interface SelectedTimeRange {
  lower: TimeRangeLower
  upper: string | null
  seconds: number
  format?: string
  label: string
  duration: string
  type: 'selected'
}

export interface CustomTimeRange {
  lower: string
  upper: string
  label: CUSTOM_TIME_RANGE_LABEL | string
  type: 'custom'
}
export type TimeRange = SelectedTimeRange | CustomTimeRange

  lower: string
  upper?: string | null
  seconds?: number
  format?: string
  label?: string
  duration?: string
}
