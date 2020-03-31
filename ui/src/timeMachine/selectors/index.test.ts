import {getStartTime, getEndTime} from 'src/timeMachine/selectors/index'
import moment from 'moment'

import {
  pastThirtyDaysTimeRange,
  pastHourTimeRange,
  pastFifteenMinTimeRange,
} from 'src/shared/constants/timeRanges'

const custom = 'custom' as 'custom'

describe('TimeMachine.Selectors.Index', () => {
  const thirty = moment()
    .subtract(30, 'days')
    .subtract(moment().isDST() ? 1 : 0, 'hours') // added to account for DST
    .valueOf()
  it(`getStartTime should return ${thirty} when lower is now() - 30d`, () => {
    expect(getStartTime(pastThirtyDaysTimeRange)).toBeGreaterThanOrEqual(thirty)
  })

  const hour = moment()
    .subtract(1, 'hours')
    .valueOf()
  it(`getStartTime should return ${hour} when lower is now() - 1h`, () => {
    expect(getStartTime(pastHourTimeRange)).toBeGreaterThanOrEqual(hour)
  })

  const fifteen = moment()
    .subtract(15, 'minutes')
    .valueOf()
  it(`getStartTime should return ${hour} when lower is now() - 1h`, () => {
    expect(getStartTime(pastFifteenMinTimeRange)).toBeGreaterThanOrEqual(
      fifteen
    )
  })

  const date = '2019-01-01'
  const newYears = moment(date).valueOf()
  it(`getStartTime should return ${newYears} when lower is ${date}`, () => {
    const timeRange = {
      type: custom,
      lower: date,
      upper: date,
    }
    expect(getStartTime(timeRange)).toEqual(newYears)
  })

  it(`getEndTime should return ${newYears} when lower is ${date}`, () => {
    const timeRange = {
      type: custom,
      lower: date,
      upper: date,
    }
    expect(getEndTime(timeRange)).toEqual(newYears)
  })

  const now = moment().valueOf()
  it(`getEndTime should return ${now} when upper is null and lower includes now()`, () => {
    expect(getEndTime(pastThirtyDaysTimeRange)).toBeGreaterThanOrEqual(now)
  })
})
