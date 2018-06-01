import {TimeRange} from 'src/types/query'

interface InputTimeRange {
  seconds?: number
  lower?: string
  upper?: string
}

interface OutputTimeRange {
  since: number
  until: number | null
}

export const millisecondTimeRange = ({
  seconds,
  lower,
  upper,
}: InputTimeRange): OutputTimeRange => {
  // Is this a relative time range?
  if (seconds) {
    return {since: Date.now() - seconds * 1000, until: null}
  }

  const since = Date.parse(lower)
  let until
  if (upper === 'now()') {
    until = Date.now()
  } else {
    until = Date.parse(upper)
  }
  return {since, until}
}

export const validTimeRange = (timeRange: TimeRange): boolean => {
  if (!timeRange || !timeRange.lower) {
    return false
  }
  return true
}
