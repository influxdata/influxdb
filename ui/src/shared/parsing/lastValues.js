import _ from 'lodash'

export default function(timeSeriesResponse) {
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
