import {
  parseDuration,
  durationToMilliseconds,
  areDurationsEqual,
  millisecondsToDuration,
} from 'src/shared/utils/duration'

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

describe('durationToMilliseconds', () => {
  expect(durationToMilliseconds([{magnitude: 2, unit: 'h'}])).toEqual(7200000)

  expect(
    durationToMilliseconds([
      {magnitude: 2, unit: 'h'},
      {magnitude: 5, unit: 'ms'},
    ])
  ).toEqual(7200005)
})

describe('areDurationsEqual', () => {
  test('tests if durations are equal', () => {
    expect(areDurationsEqual('1h', '3600s')).toBe(true)
    expect(areDurationsEqual('1h', '2s')).toBe(false)
    expect(areDurationsEqual('1h2s', '3602s')).toBe(true)
  })

  test('returns false when passed invalid durations', () => {
    expect(areDurationsEqual('1h', 'howdy')).toBe(false)
    expect(areDurationsEqual('howdy', '1h')).toBe(false)
    expect(areDurationsEqual('howdy', 'howdy')).toBe(false)
    expect(areDurationsEqual('howdy', 'hi')).toBe(false)
  })
})

describe('millisecondsToDuration', () => {
  test('can convert millisecond duration to duration ast', () => {
    expect(millisecondsToDuration(150_000)).toEqual('2m30s')
    expect(millisecondsToDuration(7_200_005)).toEqual('2h5ms')
    expect(millisecondsToDuration(9_000_000)).toEqual('2h30m')
    expect(millisecondsToDuration(2 / 1_000_000)).toEqual('2ns')
  })
})
