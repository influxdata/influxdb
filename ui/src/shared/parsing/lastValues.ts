import _ from 'lodash'

interface Result {
  lastValues: number[]
  series: string[]
}

type SeriesValue = number | string

interface Series {
  name: string
  values: SeriesValue[][] | null
  columns: string[] | null
}

interface TimeSeriesResult {
  series: Series[]
}

export interface TimeSeriesResponse {
  response: {
    results: TimeSeriesResult[]
  }
}

export default function(
  timeSeriesResponse: TimeSeriesResponse[] | null
): Result {
  const values = _.get(
    timeSeriesResponse,
    ['0', 'response', 'results', '0', 'series', '0', 'values'],
    [['', '']]
  )

  const series = _.get(
    timeSeriesResponse,
    ['0', 'response', 'results', '0', 'series', '0', 'columns'],
    ['', '']
  ).slice(1)

  const lastValues = values[values.length - 1].slice(1) // remove time with slice 1

  return {lastValues, series}
}
