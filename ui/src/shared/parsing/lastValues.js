import _ from 'lodash'

export default function(timeSeriesResponse) {
  const values = _.get(
    timeSeriesResponse,
    ['0', 'response', 'results', '0', 'series', '0', 'values'],
    [['', '']]
  )
  const lastValues = values[values.length - 1]

  return lastValues
}
