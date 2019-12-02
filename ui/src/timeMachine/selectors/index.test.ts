// Funcs
import {getStartTime, getEndTime} from 'src/timeMachine/selectors/index'
import moment from 'moment'

describe('TimeMachine.Selectors.Index', () => {
  const thirty = moment()
    .subtract(30, 'days')
    .valueOf()
  it(`getStartTime should return ${thirty} when lower is now() - 30d`, () => {
    const timeRange = {
      lower: 'now() - 30d',
      upper: null,
    }
    expect(getStartTime(timeRange)).toBeGreaterThanOrEqual(thirty)
  })
  const seven = moment()
    .subtract(7, 'days')
    .valueOf()
  it(`getStartTime should return ${seven} when lower is now() - 7d`, () => {
    const timeRange = {
      lower: 'now() - 7d',
      upper: null,
    }
    expect(getStartTime(timeRange)).toBeGreaterThanOrEqual(seven)
  })
  const two = moment()
    .subtract(2, 'days')
    .valueOf()
  it(`getStartTime should return ${two} when lower is now() - 2d`, () => {
    const timeRange = {
      lower: 'now() - 2d',
      upper: null,
    }
    expect(getStartTime(timeRange)).toBeGreaterThanOrEqual(two)
  })
  const twentyFour = moment()
    .subtract(24, 'hours')
    .valueOf()
  it(`getStartTime should return ${twentyFour} when lower is now() - 24h`, () => {
    const timeRange = {
      lower: 'now() - 24h',
      upper: null,
    }
    expect(getStartTime(timeRange)).toBeGreaterThanOrEqual(twentyFour)
  })
  const twelve = moment()
    .subtract(12, 'hours')
    .valueOf()
  it(`getStartTime should return ${twelve} when lower is now() - 12h`, () => {
    const timeRange = {
      lower: 'now() - 12h',
      upper: null,
    }
    expect(getStartTime(timeRange)).toBeGreaterThanOrEqual(twelve)
  })
  const six = moment()
    .subtract(6, 'hours')
    .valueOf()
  it(`getStartTime should return ${six} when lower is now() - 6h`, () => {
    const timeRange = {
      lower: 'now() - 6h',
      upper: null,
    }
    expect(getStartTime(timeRange)).toBeGreaterThanOrEqual(six)
  })
  const hour = moment()
    .subtract(1, 'hours')
    .valueOf()
  it(`getStartTime should return ${hour} when lower is now() - 1h`, () => {
    const timeRange = {
      lower: 'now() - 1h',
      upper: null,
    }
    expect(getStartTime(timeRange)).toBeGreaterThanOrEqual(hour)
  })
  const fifteen = moment()
    .subtract(15, 'minutes')
    .valueOf()
  it(`getStartTime should return ${fifteen} when lower is now() - 15m`, () => {
    const timeRange = {
      lower: 'now() - 15m',
      upper: null,
    }
    expect(getStartTime(timeRange)).toBeGreaterThanOrEqual(fifteen)
  })
  const five = moment()
    .subtract(5, 'minutes')
    .valueOf()
  it(`getStartTime should return ${five} when lower is now() - 5m`, () => {
    const timeRange = {
      lower: 'now() - 5m',
      upper: null,
    }
    expect(getStartTime(timeRange)).toBeGreaterThanOrEqual(five)
  })
  const date = 'January 1, 2019'
  const newYears = moment(date).valueOf()
  it(`getStartTime should return ${newYears} when lower is ${date}`, () => {
    const timeRange = {
      lower: date,
      upper: null,
    }
    expect(getStartTime(timeRange)).toEqual(newYears)
  })
  it(`getEndTime should return ${newYears} when lower is ${date}`, () => {
    const timeRange = {
      lower: 'now() - 30d',
      upper: date,
    }
    expect(getEndTime(timeRange)).toEqual(newYears)
  })
  it(`getEndTime should return null when upper is null`, () => {
    const timeRange = {
      lower: 'now() - 30d',
      upper: null,
    }
    expect(getEndTime(timeRange)).toEqual(null)
  })
})
