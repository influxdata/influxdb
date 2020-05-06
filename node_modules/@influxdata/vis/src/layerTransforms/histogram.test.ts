import {bin} from './histogram'
import {newTable} from '../utils/newTable'
import {X_MIN, X_MAX, Y_MIN, Y_MAX, COUNT} from '../constants/columnKeys'

const valueData = [70, 56, 60, 100, 76, 0, 63, 48, 79, 67]

const fieldData = [
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
]

const cpuData = [
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
]

const TABLE = newTable(10)
  .addColumn('_value', 'number', valueData)
  .addColumn('_field', 'string', fieldData)
  .addColumn('cpu', 'string', cpuData)

describe('bin', () => {
  test('without grouping', () => {
    const actual = bin(TABLE, '_value', null, [], 5, 'stacked')

    expect(actual.length).toEqual(5)
    expect(actual.getColumn(X_MIN, 'number')).toEqual([0, 20, 40, 60, 80])
    expect(actual.getColumn(X_MAX, 'number')).toEqual([20, 40, 60, 80, 100])
    expect(actual.getColumn(Y_MIN, 'number')).toEqual([0, 0, 0, 0, 0])
    expect(actual.getColumn(Y_MAX, 'number')).toEqual([1, 0, 2, 6, 1])
    expect(actual.getColumn(COUNT, 'number')).toEqual([1, 0, 2, 6, 1])
  })

  test('with grouping by _field and cpu', () => {
    const actual = bin(TABLE, '_value', null, ['_field'], 5, 'stacked')

    expect(actual.getColumn(X_MIN, 'number')).toEqual([
      0,
      20,
      40,
      60,
      80,
      0,
      20,
      40,
      60,
      80,
    ])

    expect(actual.getColumn(X_MAX, 'number')).toEqual([
      20,
      40,
      60,
      80,
      100,
      20,
      40,
      60,
      80,
      100,
    ])

    expect(actual.getColumn(Y_MIN, 'number')).toEqual([
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      1,
      3,
      1,
    ])

    expect(actual.getColumn(Y_MAX, 'number')).toEqual([
      0,
      0,
      1,
      3,
      1,
      1,
      0,
      2,
      6,
      1,
    ])

    expect(actual.getColumn(COUNT, 'number')).toEqual([
      0,
      0,
      1,
      3,
      1,
      1,
      0,
      1,
      3,
      0,
    ])

    expect(actual.getColumn('_field', 'string')).toEqual([
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
    ])
  })

  test('with grouping and overlaid positioning', () => {
    const actual = bin(TABLE, '_value', null, ['_field'], 5, 'overlaid')

    expect(actual.getColumn(X_MIN, 'number')).toEqual([
      0,
      20,
      40,
      60,
      80,
      0,
      20,
      40,
      60,
      80,
    ])

    expect(actual.getColumn(X_MAX, 'number')).toEqual([
      20,
      40,
      60,
      80,
      100,
      20,
      40,
      60,
      80,
      100,
    ])

    expect(actual.getColumn(Y_MIN, 'number')).toEqual([
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
    ])

    expect(actual.getColumn(Y_MAX, 'number')).toEqual([
      0,
      0,
      1,
      3,
      1,
      1,
      0,
      1,
      3,
      0,
    ])

    expect(actual.getColumn(COUNT, 'number')).toEqual([
      0,
      0,
      1,
      3,
      1,
      1,
      0,
      1,
      3,
      0,
    ])

    expect(actual.getColumn('_field', 'string')).toEqual([
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
    ])
  })

  test('with an explicitly set xDomain', () => {
    const actual = bin(TABLE, '_value', [-200, 200], [], 10, 'stacked')

    expect(actual.getColumn(X_MIN, 'number')).toEqual([
      -200,
      -160,
      -120,
      -80,
      -40,
      0,
      40,
      80,
      120,
      160,
    ])

    expect(actual.getColumn(X_MAX, 'number')).toEqual([
      -160,
      -120,
      -80,
      -40,
      0,
      40,
      80,
      120,
      160,
      200,
    ])

    expect(actual.getColumn(Y_MIN, 'number')).toEqual([
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
    ])

    expect(actual.getColumn(Y_MAX, 'number')).toEqual([
      0,
      0,
      0,
      0,
      0,
      1,
      8,
      1,
      0,
      0,
    ])

    expect(actual.getColumn(COUNT, 'number')).toEqual([
      0,
      0,
      0,
      0,
      0,
      1,
      8,
      1,
      0,
      0,
    ])
  })

  test('ignores values outside of xDomain', () => {
    const actual = bin(TABLE, '_value', [50, 80], [], 3, 'stacked')

    expect(actual.getColumn(X_MIN, 'number')).toEqual([50, 60, 70])
    expect(actual.getColumn(X_MAX, 'number')).toEqual([60, 70, 80])
    expect(actual.getColumn(Y_MIN, 'number')).toEqual([0, 0, 0])
    expect(actual.getColumn(Y_MAX, 'number')).toEqual([1, 3, 3])
    expect(actual.getColumn(COUNT, 'number')).toEqual([1, 3, 3])
  })
})
