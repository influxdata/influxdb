import {fluxTablesToDygraph} from 'src/shared/parsing/flux/dygraph'
import {parseResponse} from 'src/shared/parsing/flux/response'
import {
  SIMPLE,
  MISMATCHED,
  MULTI_VALUE_ROW,
  MIXED_DATATYPES,
} from 'src/shared/parsing/flux/constants'

describe('fluxTablesToDygraph', () => {
  it('can parse flux tables to dygraph series', () => {
    const fluxTables = parseResponse(SIMPLE)
    const actual = fluxTablesToDygraph(fluxTables)
    const expected = [
      [new Date('2018-09-10T22:34:29Z'), 0],
      [new Date('2018-09-10T22:34:39Z'), 10],
    ]

    expect(actual.dygraphsData).toEqual(expected)
  })

  it('can parse flux tables for series of mismatched periods', () => {
    const fluxTables = parseResponse(MISMATCHED)
    const actual = fluxTablesToDygraph(fluxTables)
    const expected = [
      [new Date('2018-06-04T17:12:25Z'), 1, null],
      [new Date('2018-06-04T17:12:35Z'), 2, null],
      [new Date('2018-06-05T17:12:25Z'), null, 10],
      [new Date('2018-06-05T17:12:35Z'), null, 11],
    ]

    expect(actual.dygraphsData).toEqual(expected)
  })

  it('can parse multiple values per row', () => {
    const fluxTables = parseResponse(MULTI_VALUE_ROW)
    const actual = fluxTablesToDygraph(fluxTables)
    const expected = {
      labels: [
        'time',
        'mean_usage_idle[_measurement=cpu]',
        'mean_usage_user[_measurement=cpu]',
        'mean_usage_idle[_measurement=mem]',
        'mean_usage_user[_measurement=mem]',
      ],
      dygraphsData: [
        [new Date('2018-09-10T16:54:37Z'), 85, 10, 8, 1],
        [new Date('2018-09-10T16:54:38Z'), 87, 7, 9, 2],
        [new Date('2018-09-10T16:54:39Z'), 89, 5, 10, 3],
      ],
      nonNumericColumns: [],
    }

    expect(actual).toEqual(expected)
  })

  it('filters out non-numeric series', () => {
    const fluxTables = parseResponse(MIXED_DATATYPES)
    const actual = fluxTablesToDygraph(fluxTables)
    const expected = {
      labels: [
        'time',
        'mean_usage_idle[_measurement=cpu]',
        'mean_usage_idle[_measurement=mem]',
      ],
      dygraphsData: [
        [new Date('2018-09-10T16:54:37Z'), 85, 8],
        [new Date('2018-09-10T16:54:39Z'), 89, 10],
      ],
      nonNumericColumns: ['my_fun_col'],
    }

    expect(actual).toEqual(expected)
  })
})
