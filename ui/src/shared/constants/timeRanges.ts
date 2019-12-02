import {
  TimeRange,
  CUSTOM_TIME_RANGE_LABEL,
  TimeRangeLower,
  SelectedTimeRange,
  CustomTimeRange,
  CheckTimeRange,
} from 'src/types'

export const TIME_RANGE_FORMAT = 'YYYY-MM-DD HH:mm'

export const pastHourTimeRange: SelectedTimeRange = {
  seconds: 3600,
  lower: 'now() - 1h' as TimeRangeLower,
  upper: null,
  label: 'Past 1h',
  duration: '1h',
  type: 'selected',
}

export const pastThirtyDaysTimeRange: SelectedTimeRange = {
  seconds: 2592000,
  lower: 'now() - 30d',
  upper: null,
  label: 'Past 30d',
  duration: '30d',
  type: 'selected',
}

export const pastFifteenMinTimeRange: SelectedTimeRange = {
  seconds: 900,
  lower: 'now() - 15m',
  upper: null,
  label: 'Past 15m',
  duration: '15m',
  type: 'selected',
}

export const CUSTOM_TIME_RANGE: CustomTimeRange = {
  lower: '',
  upper: null,
  label: 'Custom Time Range' as CUSTOM_TIME_RANGE_LABEL,
  type: 'custom',
}

export const CHECK_TIME_RANGE: CheckTimeRange = {
  lower: '', // format: `now() - ${durationMultiple}`
  upper: null,
  label: 'Auto-generated time range',
  type: 'check',
}

export const SELECTED_TIME_RANGES: SelectedTimeRange[] = [
  {
    seconds: 300,
    lower: 'now() - 5m',
    upper: null,
    label: 'Past 5m',
    duration: '5m',
    type: 'selected',
  },
  pastFifteenMinTimeRange,
  pastHourTimeRange,
  {
    seconds: 21600,
    lower: 'now() - 6h',
    upper: null,
    label: 'Past 6h',
    duration: '6h',
    type: 'selected',
  },
  {
    seconds: 43200,
    lower: 'now() - 12h',
    upper: null,
    label: 'Past 12h',
    duration: '12h',
    type: 'selected',
  },
  {
    seconds: 86400,
    lower: 'now() - 24h',
    upper: null,
    label: 'Past 24h',
    duration: '24h',
    type: 'selected',
  },
  {
    seconds: 172800,
    lower: 'now() - 2d',
    upper: null,
    label: 'Past 2d',
    duration: '2d',
    type: 'selected',
  },
  {
    seconds: 604800,
    lower: 'now() - 7d',
    upper: null,
    label: 'Past 7d',
    duration: '7d',
    type: 'selected',
  },
  pastThirtyDaysTimeRange,
]

export const TIME_RANGES: TimeRange[] = [
  CUSTOM_TIME_RANGE,
  ...SELECTED_TIME_RANGES,
]

export const DEFAULT_TIME_RANGE: TimeRange = pastHourTimeRange
