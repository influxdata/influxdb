import {parseDuration} from 'src/variables/utils/parseDuration'

const TEST_CASES = [
  ['1d', [{magnitude: 1, unit: 'd'}]],
  ['1mo10d', [{magnitude: 1, unit: 'mo'}, {magnitude: 10, unit: 'd'}]],
  ['1h15m', [{magnitude: 1, unit: 'h'}, {magnitude: 15, unit: 'm'}]],
  ['10y', [{magnitude: 10, unit: 'y'}]],
  ['2w', [{magnitude: 2, unit: 'w'}]],
  ['123410012ms', [{magnitude: 123410012, unit: 'ms'}]],
  ['30m', [{magnitude: 30, unit: 'm'}]],
  ['30m1ms', [{magnitude: 30, unit: 'm'}, {magnitude: 1, unit: 'ms'}]],
  ['999us', [{magnitude: 999, unit: 'us'}]],
  ['999µs', [{magnitude: 999, unit: 'µs'}]],
  ['999ns', [{magnitude: 999, unit: 'ns'}]],
]

describe('parseDuration', () => {
  test.each(TEST_CASES)(
    'can parse Flux duration literals',
    (input, expected) => {
      expect(parseDuration(input)).toEqual(expected)
    }
  )

  test('it throws an error when passed bad input', () => {
    expect(() => parseDuration('howdy')).toThrow()
  })
})
