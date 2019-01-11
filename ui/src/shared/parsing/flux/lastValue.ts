import {spreadTables} from 'src/shared/parsing/flux/spreadTables'

import {FluxTable} from 'src/types'

export const lastValue = (tables: FluxTable[]): number => {
  if (tables.every(table => !table.data.length)) {
    return null
  }

  const {table, seriesDescriptions} = spreadTables(tables)
  const seriesKeys = seriesDescriptions.map(d => d.key)
  const times = Object.keys(table)

  times.sort()
  seriesKeys.sort()

  const lastTime = times[times.length - 1]
  const firstSeriesKey = seriesKeys[0]

  return table[lastTime][firstSeriesKey]
}
