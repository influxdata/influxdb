import {TimeRange} from 'src/types'

export const TIME_RANGES: TimeRange[] = [
  {
    seconds: 300,
    lower: 'now() - 5m',
    upper: null,
    label: 'Past 5m',
  },
  {
    seconds: 900,
    lower: 'now() - 15m',
    upper: null,
    label: 'Past 15m',
  },
  {
    seconds: 3600,
    lower: 'now() - 1h',
    upper: null,
    label: 'Past 1h',
  },
  {
    seconds: 21600,
    lower: 'now() - 6h',
    upper: null,
    label: 'Past 6h',
  },
  {
    seconds: 43200,
    lower: 'now() - 12h',
    upper: null,
    label: 'Past 12h',
  },
  {
    seconds: 86400,
    lower: 'now() - 24h',
    upper: null,
    label: 'Past 24h',
  },
  {
    seconds: 172800,
    lower: 'now() - 2d',
    upper: null,
    label: 'Past 2d',
  },
  {
    seconds: 604800,
    lower: 'now() - 7d',
    upper: null,
    label: 'Past 7d',
  },
  {
    seconds: 2592000,
    lower: 'now() - 30d',
    upper: null,
    label: 'Past 30d',
  },
]

export const DEFAULT_TIME_RANGE: TimeRange = {
  upper: null,
  lower: 'now() - 15m',
  seconds: 900,
}

export const ABSOLUTE = 'absolute'
export const INVALID = 'invalid'
export const RELATIVE_LOWER = 'relative lower'
export const RELATIVE_UPPER = 'relative upper'
export const INFLUXQL = 'influxql'
