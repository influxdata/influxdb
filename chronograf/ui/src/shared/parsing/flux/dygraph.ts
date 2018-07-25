import _ from 'lodash'
import {FluxTable, DygraphValue} from 'src/types'

export const fluxTablesToDygraph = (data: FluxTable[]): DygraphValue[][] => {
  interface V {
    [time: string]: number[]
  }

  const valuesForTime: V = {}

  data.forEach(table => {
    const header = table.data[0]
    const timeColIndex = header.findIndex(col => col === '_time')

    table.data.slice(1).forEach(row => {
      valuesForTime[row[timeColIndex]] = Array(data.length).fill(null)
    })
  })

  data.forEach((table, i) => {
    const header = table.data[0]
    const timeColIndex = header.findIndex(col => col === '_time')
    const valueColIndex = header.findIndex(col => col === '_value')

    table.data.slice(1).forEach(row => {
      const time = row[timeColIndex]
      const value = row[valueColIndex]

      valuesForTime[time][i] = +value
    })
  })

  return _.sortBy(Object.entries(valuesForTime), ([time]) => time).map(
    ([time, values]) => [new Date(time), ...values]
  )
}
