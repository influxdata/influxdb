// Libraries
import _ from 'lodash'

// Utils
import {
  spreadTables,
  SeriesDescription,
} from 'src/shared/parsing/flux/spreadTables'

// Types
import {FluxTable} from 'src/types'
import {DygraphValue} from 'src/external/dygraph'

export interface FluxTablesToDygraphResult {
  labels: string[]
  dygraphsData: DygraphValue[][]
  seriesDescriptions: SeriesDescription[]
}

export const fluxTablesToDygraph = (
  tables: FluxTable[]
): FluxTablesToDygraphResult => {
  const {table, seriesDescriptions} = spreadTables(tables)
  const labels = seriesDescriptions.map(d => d.key)

  labels.sort()

  const dygraphsData = Object.keys(table).map(time => [
    new Date(time),
    ...labels.map(label => table[time][label]),
  ])

  dygraphsData.sort((a, b) => (a[0] as any) - (b[0] as any))

  return {dygraphsData, labels: ['time', ...labels], seriesDescriptions}
}
