// Libraries
import _ from 'lodash'

// Utils
import {parseTablesByTime} from 'src/shared/parsing/flux/parseTablesByTime'

// Types
import {FluxTable} from 'src/types'
import {DygraphValue} from 'src/external/dygraph'

export interface FluxTablesToDygraphResult {
  labels: string[]
  dygraphsData: DygraphValue[][]
  nonNumericColumns: string[]
}

export const fluxTablesToDygraph = (
  tables: FluxTable[]
): FluxTablesToDygraphResult => {
  const {tablesByTime, nonNumericColumns, allColumnNames} = parseTablesByTime(
    tables
  )

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
