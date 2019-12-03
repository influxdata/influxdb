export {Query, Dialect} from 'src/client'

export type CUSTOM_TIME_RANGE_LABEL = 'Custom Time Range'

export type SelectableTimeRangeLower =
  | 'now() - 5m'
  | 'now() - 15m'
  | 'now() - 1h'
  | 'now() - 6h'
  | 'now() - 12h'
  | 'now() - 24h'
  | 'now() - 2d'
  | 'now() - 7d'
  | 'now() - 30d'

export type TimeRange =
  | SelectableDurationTimeRange
  | DurationTimeRange
  | CustomTimeRange

export interface SelectableDurationTimeRange {
  lower: SelectableTimeRangeLower
  upper: null
  seconds: number
  format?: string
  label: string
  duration: string
  type: 'selectable-duration'
}

export interface DurationTimeRange {
  lower: string
  upper: null
  label: string
  type: 'duration'
}

export interface CustomTimeRange {
  lower: string
  upper: string
  label: CUSTOM_TIME_RANGE_LABEL | string
  type: 'custom'
}
