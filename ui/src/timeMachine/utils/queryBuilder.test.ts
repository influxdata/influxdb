import {buildQuery} from 'src/timeMachine/utils/queryBuilder'

import {BuilderConfig} from 'src/types'

describe('buildQuery', () => {
  test('single tag', () => {
    const config: BuilderConfig = {
      buckets: ['b0'],
      tags: [
        {key: '_measurement', values: ['m0'], aggregateFunctionType: 'filter'},
      ],
      functions: [{name: 'median'}],
      aggregateWindow: {period: 'auto', fillValues: true},
    }

    const expected = `from(bucket: "b0")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "m0")
  |> aggregateWindow(every: v.windowPeriod, fn: median, createEmpty: true)
  |> yield(name: "median")`

    const actual = buildQuery(config)
    expect(actual).toEqual(expected)
  })

  test('multiple tags', () => {
    const config: BuilderConfig = {
      buckets: ['b0'],
      tags: [
        {
          key: '_measurement',
          values: ['m0', 'm1'],
          aggregateFunctionType: 'filter',
        },
        {key: '_field', values: ['f0', 'f1'], aggregateFunctionType: 'filter'},
      ],
      functions: [{name: 'median'}],
      aggregateWindow: {period: 'auto', fillValues: false},
    }

    const expected = `from(bucket: "b0")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "m0" or r["_measurement"] == "m1")
  |> filter(fn: (r) => r["_field"] == "f0" or r["_field"] == "f1")
  |> aggregateWindow(every: v.windowPeriod, fn: median, createEmpty: false)
  |> yield(name: "median")`

    const actual = buildQuery(config)

    expect(actual).toEqual(expected)
  })

  test('single tag, multiple functions', () => {
    const config: BuilderConfig = {
      buckets: ['b0'],
      tags: [
        {key: '_measurement', values: ['m0'], aggregateFunctionType: 'filter'},
      ],
      functions: [{name: 'mean'}, {name: 'median'}],
      aggregateWindow: {period: 'auto', fillValues: true},
    }

    const expected = `from(bucket: "b0")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "m0")
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: true)
  |> yield(name: "mean")

from(bucket: "b0")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "m0")
  |> aggregateWindow(every: v.windowPeriod, fn: median, createEmpty: true)
  |> yield(name: "median")`

    const actual = buildQuery(config)

    expect(actual).toEqual(expected)
  })
})
