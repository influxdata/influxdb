import {Table} from '../types'
import {getGroupKey} from '../utils/getGroupKey'

export const appendGroupingCol = (
  table: Table,
  groupingColKeys: string[],
  columnKey: string
): Table => {
  const groupKeysData = groupingColKeys.reduce(
    (acc, k) => ({
      ...acc,
      [k]: table.getColumn(k),
    }),
    {}
  )

  const data: string[] = []

  for (let i = 0; i < table.length; i++) {
    data.push(getGroupKey(groupingColKeys.map(k => groupKeysData[k][i])))
  }

  return table.addColumn(columnKey, 'string', data)
}
