import _ from 'lodash'

interface Result {
  lastValues: any[]
  series: any[]
}

interface Series {
  value: any
  column: string
}

interface TimeSeriesResult {
  series: Series[]
}

export interface TimeSeriesResponse {
  response: {
    result: TimeSeriesResult[]
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
