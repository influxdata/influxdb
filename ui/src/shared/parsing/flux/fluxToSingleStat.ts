import _ from 'lodash'
import {FluxTable} from 'src/types'
import {parseTablesByTime} from 'src/shared/parsing/flux/parseTablesByTime'

export interface LastValues {
  values: number[]
  series: string[]
}

export default (tables: FluxTable[]): LastValues => {
  const {tablesByTime} = parseTablesByTime(tables)

  const lastValues = _.reduce(
    tablesByTime,
    (acc, table) => {
      const lastTime = _.last(Object.keys(table))
      const values = table[lastTime]
      _.forEach(values, (value, series) => {
        acc.series.push(series)
        acc.values.push(value)
      })
      return acc
    },
    {values: [], series: []}
  )

  return lastValues
}
