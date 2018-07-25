import _ from 'lodash'
import {Data} from 'src/types/dygraphs'
import {TimeSeriesServerResponse} from 'src/types/series'

interface Result {
  lastValues: number[]
  series: string[]
}

export default function(
  timeSeriesResponse: TimeSeriesServerResponse[] | Data | null
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
