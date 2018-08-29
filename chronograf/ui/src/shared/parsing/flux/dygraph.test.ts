import {fluxTablesToDygraph} from 'src/shared/parsing/flux/dygraph'
import {parseResponse} from 'src/shared/parsing/flux/response'
import {
  CSV_TO_DYGRAPH,
  CSV_TO_DYGRAPH_MISMATCHED,
} from 'src/shared/parsing/flux/constants'

describe('fluxTablesToDygraph', () => {
  it('can parse flux tables to dygraph series', () => {
    const fluxTables = parseResponse(CSV_TO_DYGRAPH)
    const actual = fluxTablesToDygraph(fluxTables)
    const expected = [
      [new Date('2018-06-01T22:27:41Z'), 1, 3, 5],
      [new Date('2018-06-01T22:27:42Z'), 2, 2, 1],
    ]

    expect(actual).toEqual(expected)
  })

  it('can parse flux tables for series of mismatched periods', () => {
    const fluxTables = parseResponse(CSV_TO_DYGRAPH_MISMATCHED)
    const actual = fluxTablesToDygraph(fluxTables)
    const expected = [
      [new Date('2018-06-04T17:12:25Z'), 1, null],
      [new Date('2018-06-04T17:12:35Z'), 2, null],
      [new Date('2018-06-05T17:12:25Z'), null, 10],
      [new Date('2018-06-05T17:12:35Z'), null, 11],
    ]

    expect(actual).toEqual(expected)
  })
})
