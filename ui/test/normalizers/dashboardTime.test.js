import normalizer from 'src/normalizers/dashboardTime'

const dashboardID = 1
const upper = null
const lower = 'now() - 15m'
const timeRange = {dashboardID, upper, lower}

describe('Normalizers.DashboardTime', () => {
  it('can filter out non-objects', () => {
    const ranges = [1, null, undefined, 'string', timeRange]

    const actual = normalizer(ranges)
    const expected = [timeRange]

    expect(actual).toEqual(expected)
  })

  it('can remove objects with missing keys', () => {
    const ranges = [
      {},
      {dashboardID, upper},
      {dashboardID, lower},
      {upper, lower},
      timeRange,
    ]

    const actual = normalizer(ranges)
    const expected = [timeRange]
    expect(actual).toEqual(expected)
  })

  it('can remove timeRanges with incorrect dashboardID', () => {
    const ranges = [{dashboardID: '1', upper, lower}, timeRange]

    const actual = normalizer(ranges)
    const expected = [timeRange]
    expect(actual).toEqual(expected)
  })

  it('can remove timeRange when is neither an upper or lower bound', () => {
    const noBounds = {dashboardID, upper: null, lower: null}
    const ranges = [timeRange, noBounds]

    const actual = normalizer(ranges)
    const expected = [timeRange]
    expect(actual).toEqual(expected)
  })

  it('can remove a timeRange when upper and lower bounds are of the wrong type', () => {
    const badTime = {dashboardID, upper: [], lower}
    const reallyBadTime = {dashboardID, upper, lower: {bad: 'time'}}
    const ranges = [timeRange, badTime, reallyBadTime]

    const actual = normalizer(ranges)
    const expected = [timeRange]
    expect(actual).toEqual(expected)
  })
})
