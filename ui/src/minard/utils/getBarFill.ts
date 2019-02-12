import {Layer} from 'src/minard'
import {getGroupKey} from 'src/minard/utils/getGroupKey'

// Given a histogram `Layer` and the index of row in its table, this function
// will get the color for the bar that corresponds to that row.
//
// Since the color of a bar depends on the values in multiple columns of that
// bar's row, the process is to:
//
// 1. Get the value of each necessary column (the columns are specified by the
//    `fill` data-to-aesthetic mapping)
// 2. Turn that set of values into the hashable representation (the “group
//    key”) that the scale uses as a domain
// 3. Lookup the scale and get the color via this representation
export const getBarFill = (
  {scales, aesthetics, table}: Layer,
  i: number
): string => {
  const fillScale = scales.fill
  const values = aesthetics.fill.map(colKey => table.columns[colKey][i])
  const groupKey = getGroupKey(values)
  const fill = fillScale(groupKey)

  return fill
}
