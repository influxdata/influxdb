import {HistogramPosition, ColumnType} from 'src/minard'
import {bin} from 'src/minard/utils/bin'

const TABLE = {
  columns: {
    _value: [70, 56, 60, 100, 76, 0, 63, 48, 79, 67],
    _field: [
      'usage_guest',
      'usage_guest',
      'usage_guest',
      'usage_guest',
      'usage_guest',
      'usage_idle',
      'usage_idle',
      'usage_idle',
      'usage_idle',
      'usage_idle',
    ],
    cpu: [
      'cpu0',
      'cpu0',
      'cpu0',
      'cpu1',
      'cpu1',
      'cpu0',
      'cpu0',
      'cpu0',
      'cpu1',
      'cpu1',
    ],
  },
  columnTypes: {
    _value: ColumnType.Numeric,
    _field: ColumnType.Categorical,
    cpu: ColumnType.Categorical,
  },
}

describe('bin', () => {
  test('without grouping', () => {
    const actual = bin(TABLE, '_value', null, [], 5, HistogramPosition.Stacked)
    const expected = [
      {
        columnTypes: {
          xMax: 'numeric',
          xMin: 'numeric',
          yMax: 'numeric',
          yMin: 'numeric',
        },
        columns: {
          xMax: [20, 40, 60, 80, 100],
          xMin: [0, 20, 40, 60, 80],
          yMax: [1, 0, 2, 6, 1],
          yMin: [0, 0, 0, 0, 0],
        },
      },
      {fill: [], xMax: 'xMax', xMin: 'xMin', yMax: 'yMax', yMin: 'yMin'},
    ]

    expect(actual).toEqual(expected)
  })

  test('with grouping by _field and cpu', () => {
    const actual = bin(
      TABLE,
      '_value',
      null,
      ['_field'],
      5,
      HistogramPosition.Stacked
    )[0].columns

    const expected = {
      _field: [
        'usage_guest',
        'usage_guest',
        'usage_guest',
        'usage_guest',
        'usage_guest',
        'usage_idle',
        'usage_idle',
        'usage_idle',
        'usage_idle',
        'usage_idle',
      ],
      xMax: [20, 40, 60, 80, 100, 20, 40, 60, 80, 100],
      xMin: [0, 20, 40, 60, 80, 0, 20, 40, 60, 80],
      yMax: [0, 0, 1, 3, 1, 1, 0, 2, 6, 1],
      yMin: [0, 0, 0, 0, 0, 0, 0, 1, 3, 1],
    }

    expect(actual).toEqual(expected)
  })

  test('with grouping and overlaid positioning', () => {
    const actual = bin(
      TABLE,
      '_value',
      null,
      ['_field'],
      5,
      HistogramPosition.Overlaid
    )[0].columns

    const expected = {
      _field: [
        'usage_guest',
        'usage_guest',
        'usage_guest',
        'usage_guest',
        'usage_guest',
        'usage_idle',
        'usage_idle',
        'usage_idle',
        'usage_idle',
        'usage_idle',
      ],
      xMax: [20, 40, 60, 80, 100, 20, 40, 60, 80, 100],
      xMin: [0, 20, 40, 60, 80, 0, 20, 40, 60, 80],
      yMax: [0, 0, 1, 3, 1, 1, 0, 1, 3, 0],
      yMin: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    }

    expect(actual).toEqual(expected)
  })

  test('with an explicitly set xDomain', () => {
    const actual = bin(
      TABLE,
      '_value',
      [-200, 200],
      [],
      10,
      HistogramPosition.Stacked
    )[0].columns

    const expected = {
      xMax: [-160, -120, -80, -40, 0, 40, 80, 120, 160, 200],
      xMin: [-200, -160, -120, -80, -40, 0, 40, 80, 120, 160],
      yMax: [0, 0, 0, 0, 0, 1, 8, 1, 0, 0],
      yMin: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    }

    expect(actual).toEqual(expected)
  })
})
