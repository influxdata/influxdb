import {
  TimeRange,
  CUSTOM_TIME_RANGE_LABEL,
  CustomTimeRange,
  SelectableDurationTimeRange,
  DurationTimeRange,
} from 'src/types'

export const TIME_RANGE_FORMAT = 'YYYY-MM-DD HH:mm'

export const pastHourTimeRange: SelectableDurationTimeRange = {
  seconds: 3600,
  lower: 'now() - 1h',
  upper: null,
  label: 'Past 1h',
  duration: '1h',
  type: 'selectable-duration',
}

export const pastThirtyDaysTimeRange: SelectableDurationTimeRange = {
  seconds: 2592000,
  lower: 'now() - 30d',
  upper: null,
  label: 'Past 30d',
  duration: '30d',
  type: 'selectable-duration',
}

export const pastFifteenMinTimeRange: SelectableDurationTimeRange = {
  seconds: 900,
  lower: 'now() - 15m',
  upper: null,
  label: 'Past 15m',
  duration: '15m',
  type: 'selectable-duration',
}

export const CUSTOM_TIME_RANGE: CustomTimeRange = {
  lower: '',
  upper: null,
  label: 'Custom Time Range' as CUSTOM_TIME_RANGE_LABEL,
  type: 'custom',
}

export const CHECK_TIME_RANGE: DurationTimeRange = {
  lower: '', // format: `now() - ${durationMultiple}`
  upper: null,
  label: 'Custom Duration',
  type: 'duration',
}

export const SELECTED_TIME_RANGES: SelectableDurationTimeRange[] = [
  {
    seconds: 300,
    lower: 'now() - 5m',
    upper: null,
    label: 'Past 5m',
    duration: '5m',
    type: 'selectable-duration',
  },
  pastFifteenMinTimeRange,
  pastHourTimeRange,
  {
    seconds: 21600,
    lower: 'now() - 6h',
    upper: null,
    label: 'Past 6h',
    duration: '6h',
    type: 'selectable-duration',
  },
  {
    seconds: 43200,
    lower: 'now() - 12h',
    upper: null,
    label: 'Past 12h',
    duration: '12h',
    type: 'selectable-duration',
  },
  {
    seconds: 86400,
    lower: 'now() - 24h',
    upper: null,
    label: 'Past 24h',
    duration: '24h',
    type: 'selectable-duration',
  },
  {
    seconds: 172800,
    lower: 'now() - 2d',
    upper: null,
    label: 'Past 2d',
    duration: '2d',
    type: 'selectable-duration',
  },
  {
    seconds: 604800,
    lower: 'now() - 7d',
    upper: null,
    label: 'Past 7d',
    duration: '7d',
    type: 'selectable-duration',
  },
  pastThirtyDaysTimeRange,
]

export const TIME_RANGES: TimeRange[] = [
  CUSTOM_TIME_RANGE,
  ...SELECTED_TIME_RANGES,
]

export const DEFAULT_TIME_RANGE: TimeRange = pastHourTimeRange
