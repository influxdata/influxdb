import {buildQuery} from 'src/shared/utils/queryBuilder'

import {BuilderConfig} from 'src/types/v2/timeMachine'

describe('buildQuery', () => {
  test('single bucket, single measurement', () => {
    const config: BuilderConfig = {
      buckets: ['b0'],
      measurements: ['m0'],
      fields: [],
      functions: [],
    }

    const expected = `from(bucket: "b0")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "m0")`

    const actual = buildQuery(config, '1h')

    expect(actual).toEqual(expected)
  })

  test('single bucket, multiple measurements, multiple fields', () => {
    const config: BuilderConfig = {
      buckets: ['b0'],
      measurements: ['m0', 'm1'],
      fields: ['f0', 'f1'],
      functions: [],
    }

    const expected = `from(bucket: "b0")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "m0" or r._measurement == "m1")
  |> filter(fn: (r) => r._field == "f0" or r._field == "f1")`

    const actual = buildQuery(config, '1h')

    expect(actual).toEqual(expected)
  })

  test('multiple buckets, single measurement, multiple functions', () => {
    const config: BuilderConfig = {
      buckets: ['b0', 'b1'],
      measurements: ['m0'],
      fields: [],
      functions: [
        {name: 'foo', flux: '|> toFloat()\n  |> foo()'},
        {name: 'bar', flux: '|> bar()'},
      ],
    }

    const expected = `from(bucket: "b0")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "m0")
  |> toFloat()
  |> foo()

from(bucket: "b0")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "m0")
  |> bar()

from(bucket: "b1")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "m0")
  |> toFloat()
  |> foo()

from(bucket: "b1")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "m0")
  |> bar()`

    const actual = buildQuery(config, '1h')

    expect(actual).toEqual(expected)
  })
})
