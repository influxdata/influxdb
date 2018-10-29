// Libraries
import _ from 'lodash'

// Types
import {FluxTable} from 'src/types'
import {DygraphValue} from 'src/external/dygraph'

const COLUMN_BLACKLIST = new Set([
  '_time',
  'result',
  'table',
  '_start',
  '_stop',
  '',
])

const NUMERIC_DATATYPES = ['double', 'long', 'int', 'float']

export interface FluxTablesToDygraphResult {
  labels: string[]
  dygraphsData: DygraphValue[][]
  nonNumericColumns: string[]
}

export const fluxTablesToDygraph = (
  tables: FluxTable[]
): FluxTablesToDygraphResult => {
  const allColumnNames = []
  const nonNumericColumns = []

  const tablesByTime = tables.map(table => {
    const header = table.data[0]
    const columnNames: {[k: number]: string} = {}

    for (let i = 0; i < header.length; i++) {
      const columnName = header[i]
      const dataType = table.dataTypes[columnName]

      if (COLUMN_BLACKLIST.has(columnName)) {
        continue
      }

      if (table.groupKey[columnName]) {
        continue
      }

      if (!NUMERIC_DATATYPES.includes(dataType)) {
        nonNumericColumns.push(columnName)

        continue
      }

      const uniqueColumnName = Object.entries(table.groupKey).reduce(
        (acc, [k, v]) => `${acc}[${k}=${v}]`,
        columnName
      )

      columnNames[i] = uniqueColumnName
      allColumnNames.push(uniqueColumnName)
    }

    const timeIndex = header.indexOf('_time')

    if (timeIndex < 0) {
      throw new Error('Could not find time index in FluxTable')
    }

    const result = {}

    for (let i = 1; i < table.data.length; i++) {
      const row = table.data[i]
      const time = row[timeIndex]

      result[time] = Object.entries(columnNames).reduce(
        (acc, [valueIndex, columnName]) => ({
          ...acc,
          [columnName]: row[valueIndex],
        }),
        {}
      )
    }

    return result
  })

  const dygraphValuesByTime: {[k: string]: DygraphValue[]} = {}
  const DATE_INDEX = 0
  const DATE_INDEX_OFFSET = 1

  for (const table of tablesByTime) {
    for (const time of Object.keys(table)) {
      dygraphValuesByTime[time] = Array(
        allColumnNames.length + DATE_INDEX_OFFSET
      ).fill(null)
    }
  }

  for (const table of tablesByTime) {
    for (const [date, values] of Object.entries(table)) {
      dygraphValuesByTime[date][DATE_INDEX] = new Date(date)

      for (const [seriesName, value] of Object.entries(values)) {
        const i = allColumnNames.indexOf(seriesName) + DATE_INDEX_OFFSET
        dygraphValuesByTime[date][i] = Number(value)
      }
    }
  }

  const dygraphsData = _.sortBy(Object.values(dygraphValuesByTime), ([date]) =>
    Date.parse(date as string)
  )

  return {
    labels: ['time', ...allColumnNames],
    dygraphsData,
    nonNumericColumns: _.uniq(nonNumericColumns),
  }
}
