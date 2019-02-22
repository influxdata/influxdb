import {HistogramPosition, Table} from 'src/minard'
import {bin} from 'src/minard/utils/bin'

const TABLE: Table = {
  columns: {
    _value: {
      data: [70, 56, 60, 100, 76, 0, 63, 48, 79, 67],
      type: 'int',
    },
    _field: {
      data: [
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
      type: 'string',
    },
    cpu: {
      data: [
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
      type: 'string',
    },
  },
  length: 10,
}

describe('bin', () => {
  test('without grouping', () => {
    const actual = bin(TABLE, '_value', null, [], 5, HistogramPosition.Stacked)
    const expected = [
      {
        columns: {
          xMin: {data: [0, 20, 40, 60, 80], type: 'int'},
          xMax: {data: [20, 40, 60, 80, 100], type: 'int'},
          yMin: {data: [0, 0, 0, 0, 0], type: 'int'},
          yMax: {data: [1, 0, 2, 6, 1], type: 'int'},
        },
        length: 5,
      },
      {xMin: 'xMin', xMax: 'xMax', yMin: 'yMin', yMax: 'yMax', fill: []},
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
      xMin: {data: [0, 20, 40, 60, 80, 0, 20, 40, 60, 80], type: 'int'},
      xMax: {data: [20, 40, 60, 80, 100, 20, 40, 60, 80, 100], type: 'int'},
      yMin: {data: [0, 0, 0, 0, 0, 0, 0, 1, 3, 1], type: 'int'},
      yMax: {data: [0, 0, 1, 3, 1, 1, 0, 2, 6, 1], type: 'int'},
      _field: {
        data: [
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
        type: 'string',
      },
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
      xMin: {data: [0, 20, 40, 60, 80, 0, 20, 40, 60, 80], type: 'int'},
      xMax: {data: [20, 40, 60, 80, 100, 20, 40, 60, 80, 100], type: 'int'},
      yMin: {data: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0], type: 'int'},
      yMax: {data: [0, 0, 1, 3, 1, 1, 0, 1, 3, 0], type: 'int'},
      _field: {
        data: [
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
        type: 'string',
      },
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
      xMin: {
        data: [-200, -160, -120, -80, -40, 0, 40, 80, 120, 160],
        type: 'int',
      },
      xMax: {
        data: [-160, -120, -80, -40, 0, 40, 80, 120, 160, 200],
        type: 'int',
      },
      yMin: {data: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0], type: 'int'},
      yMax: {data: [0, 0, 0, 0, 0, 1, 8, 1, 0, 0], type: 'int'},
    }

    expect(actual).toEqual(expected)
  })

  test('ignores values outside of xDomain', () => {
    const actual = bin(
      TABLE,
      '_value',
      [50, 80],
      [],
      3,
      HistogramPosition.Stacked
    )[0].columns

    const expected = {
      xMin: {data: [50, 60, 70], type: 'int'},
      xMax: {data: [60, 70, 80], type: 'int'},
      yMin: {data: [0, 0, 0], type: 'int'},
      yMax: {data: [1, 3, 3], type: 'int'},
    }

    expect(actual).toEqual(expected)
  })
})
