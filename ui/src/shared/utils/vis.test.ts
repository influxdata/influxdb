// Funcs
import {parseYBounds} from 'src/shared/utils/vis'

describe('parseYBounds', () => {
  it('should return null when bounds is null', () => {
    expect(parseYBounds(null)).toEqual(null)
    expect(parseYBounds([null, null])).toEqual(null)
  })
  it('should return [0, 100] when the bounds are ["0", "100"]', () => {
    expect(parseYBounds(['0', '100'])).toEqual([0, 100])
  })
  it('should return [null, 100] when the bounds are [null, "100"]', () => {
    expect(parseYBounds([null, '100'])).toEqual([null, 100])
  })
  it('should return [-10, null] when the bounds are ["-10", null]', () => {
    expect(parseYBounds(['-10', null])).toEqual([-10, null])
  })
  it('should return [0.1, .6] when the bounds are ["0.1", "0.6"]', () => {
    expect(parseYBounds(['0.1', '0.6'])).toEqual([0.1, 0.6])
  })
})
// export interface Table {
//   getColumn: GetColumn
//   getColumnName: (columnKey: string) => string
//   getColumnType: (columnKey: string) => ColumnType
//   columnKeys: string[]
//   length: number
//   addColumn: (
//     columnKey: string,
//     type: ColumnType,
//     data: ColumnData,
//     name?: string
//   ) => Table
// }

 const results = ["result",
 "table",
 "_start",
 "_stop",
 "_time",
 "_value",
 "_field",
 "_measurement",
 "device",
 "fstype",
 "host",
 "mode",
 "path"]
