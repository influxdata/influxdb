import {buildQuery} from 'src/shared/utils/queryBuilder'

import {BuilderConfig} from 'src/types/v2'

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
      functions: ['mean', 'median'],
    }

    const expected = `from(bucket: "b0")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "m0")
  |> mean()

from(bucket: "b0")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "m0")
  |> toFloat()
  |> median()

from(bucket: "b1")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "m0")
  |> mean()

from(bucket: "b1")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "m0")
  |> toFloat()
  |> median()`

    const actual = buildQuery(config, '1h')

    expect(actual).toEqual(expected)
  })
})
