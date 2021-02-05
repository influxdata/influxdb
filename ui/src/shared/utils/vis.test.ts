// Funcs
import {
  defaultXColumn,
  defaultYColumn,
  parseYBounds,
} from 'src/shared/utils/vis'
import {Table} from '@influxdata/giraffe'

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

describe('getting default columns', () => {
  const table = ({
    getColumn() {
      return [0, 0, 1000000]
    },
    getColumnName: jest.fn(),
    getColumnType: columnKey => {
      if (['_start', '_stop', '_time'].includes(columnKey)) {
        return 'time'
      }

      if (columnKey === '_value') {
        return 'number'
      }

      return 'boolean'
    },
    addColumn: jest.fn(),
    columnKeys: [
      'result',
      'table',
      '_start',
      '_stop',
      '_field',
      '_measurement',
      '_value',
      'cpu',
      'host',
      '_time',
    ],
    length: 3,
  } as unknown) as Table

  it('returns _time for the default x column', () => {
    expect(defaultXColumn(table)).toBe('_time')
  })

  it('does something for the default y column', () => {
    expect(defaultYColumn(table)).toBe('_value')
  })
})
