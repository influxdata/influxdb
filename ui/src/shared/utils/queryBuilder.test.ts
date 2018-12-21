import {buildQuery} from 'src/shared/utils/queryBuilder'

import {BuilderConfig} from 'src/types/v2'

describe('buildQuery', () => {
  test('single tag', () => {
    const config: BuilderConfig = {
      buckets: ['b0'],
      tags: [{key: '_measurement', values: ['m0']}],
      functions: [],
    }

    const expected = `from(bucket: "b0")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "m0")`

    const actual = buildQuery(config, '1h')

    expect(actual).toEqual(expected)
  })

  test('multiple tags', () => {
    const config: BuilderConfig = {
      buckets: ['b0'],
      tags: [
        {key: '_measurement', values: ['m0', 'm1']},
        {key: '_field', values: ['f0', 'f1']},
      ],
      functions: [],
    }

    const expected = `from(bucket: "b0")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "m0" or r._measurement == "m1")
  |> filter(fn: (r) => r._field == "f0" or r._field == "f1")`

    const actual = buildQuery(config, '1h')

    expect(actual).toEqual(expected)
  })

  test('single tag, multiple functions', () => {
    const config: BuilderConfig = {
      buckets: ['b0'],
      tags: [{key: '_measurement', values: ['m0']}],
      functions: [{name: 'mean'}, {name: 'median'}],
    }

    const expected = `from(bucket: "b0")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "m0")
  |> window(every: 1m)
  |> mean()
  |> group(columns: ["_value", "_time", "_start", "_stop"], mode: "except")
  |> yield(name: "mean")

from(bucket: "b0")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "m0")
  |> window(every: 1m)
  |> toFloat()
  |> median()
  |> group(columns: ["_value", "_time", "_start", "_stop"], mode: "except")
  |> yield(name: "median")`

    const actual = buildQuery(config, '1h')

    expect(actual).toEqual(expected)
  })
})
